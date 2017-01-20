/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
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

#if 0
typedef int bool;

extern bool ProfilerStart(const char* fname);

extern void ProfilerStop();

extern void ProfilerFlush();
#endif

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>
#if FD_FILECONFIG_ENABLED
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#endif

#include <sys/time.h>

#if HAVE_SYS_RESOURCE_H
#include <sys/resource.h>
#endif

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#if HAVE_GRP_H
#include <grp.h>
#endif

u8_condition SetRLimit=_("SetRLimit");
u8_condition fd_ArgvConfig=_("Config (argv)");

fd_exception fd_UnknownError=_("Unknown error condition");
fd_exception fd_ConfigError=_("Configuration error");
fd_exception fd_OutOfMemory=_("Memory apparently exhausted");
fd_exception fd_ExitException=_("Unhandled exception at exit");
fd_exception fd_ReadOnlyConfig=_("Read-only config setting");

static u8_string logdir=NULL, sharedir=NULL, datadir=NULL;

static int trace_config=0;

/* Configuration handling */

struct FD_CONFIG_HANDLER *config_handlers=NULL;

static fdtype configuration_table;

#if FD_THREADS_ENABLED
static u8_mutex config_lookup_lock;
static u8_mutex config_register_lock;
static u8_mutex atexit_handlers_lock;
#endif

static struct FD_CONFIG_LOOKUPS *config_lookupfns=NULL;

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
  struct FD_CONFIG_LOOKUPS *entry=
    u8_alloc(struct FD_CONFIG_LOOKUPS);
  fd_lock_mutex(&config_lookup_lock);
  entry->fdcfg_lookup=fn;
  entry->fdcfg_lookup_data=ldata;
  entry->next=config_lookupfns;
  config_lookupfns=entry;
  fd_unlock_mutex(&config_lookup_lock);
}

static fdtype config_get(u8_string var)
{
  fdtype symbol=config_intern(var);
  fdtype probe=fd_get(configuration_table,symbol,FD_VOID);
  /* This lookups configuration information using various methods */
  if (FD_VOIDP(probe)) {
    fdtype value=FD_VOID;
    struct FD_CONFIG_LOOKUPS *scan=config_lookupfns;
    while (scan) {
      value=scan->fdcfg_lookup(symbol,scan->fdcfg_lookup_data);
      if (FD_VOIDP(value)) scan=scan->next; else break;}
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

int config_set(u8_string var,fdtype val)
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
      struct FD_BYTE_INPUT in;
      in.start=in.ptr=content+1; in.end=in.start+n_bytes;
      in.fillfn=NULL;
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
    if (FD_EQ(scan->var,symbol)) {
      fdtype val;
      val=scan->config_get_method(symbol,scan->data);
      return val;}
    else scan=scan->next;
  return config_get(var);
}

FD_EXPORT int fd_config_set(u8_string var,fdtype val)
{
  fdtype symbol=config_intern(var); int retval=0;
  struct FD_CONFIG_HANDLER *scan=config_handlers;
  while (scan)
    if (FD_EQ(scan->var,symbol)) {
      scan->flags=scan->flags|FD_CONFIG_ALREADY_MODIFIED;
      retval=scan->config_set_method(symbol,val,scan->data);
      if (trace_config)
        u8_log(LOG_WARN,"ConfigSet",
               "Using handler to configure %s (%s) with %q",
               var,FD_SYMBOL_NAME(symbol),val);
      break;}
    else scan=scan->next;
  if ((!(scan))&&(trace_config))
    u8_log(LOG_WARN,"ConfigSet","Configuring %s (%s) with %q",
           var,FD_SYMBOL_NAME(symbol),val);
  config_set(var,val);
  if (retval<0) {
    u8_string errsum=fd_errstring(NULL);
    u8_log(LOG_WARN,fd_ConfigError,"Config error %q=%q: %s",symbol,val,errsum);
    u8_free(errsum);}
  return retval;
}

FD_EXPORT int fd_config_default(u8_string var,fdtype val)
{
  fdtype symbol=config_intern(var); int retval=1;
  struct FD_CONFIG_HANDLER *scan=config_handlers;
  while (scan)
    if (FD_EQ(scan->var,symbol)) {
      if ((scan->flags)&(FD_CONFIG_ALREADY_MODIFIED)) return 0;
      scan->flags=scan->flags|FD_CONFIG_ALREADY_MODIFIED;
      retval=scan->config_set_method(symbol,val,scan->data);
      if (trace_config)
        u8_log(LOG_WARN,"ConfigSet",
               "Using handler to configure default %s (%s) with %q",
               var,FD_SYMBOL_NAME(symbol),val);
      break;}
    else scan=scan->next;
  if (fd_test(configuration_table,symbol,FD_VOID)) return 0;
  else {
    if ((!(scan))&&(trace_config))
      u8_log(LOG_WARN,"ConfigSet","Configuring %s (%s) with %q",
             var,FD_SYMBOL_NAME(symbol),val);
    retval=config_set(var,val);}
  if (retval<0) {
    u8_string errsum=fd_errstring(NULL);
    u8_log(LOG_WARN,fd_ConfigError,"Config error %q=%q: %s",symbol,val,errsum);
    u8_free(errsum);}
  return retval;
}

FD_EXPORT int fd_config_set_consed(u8_string var,fdtype val)
{
  int retval=fd_config_set(var,val);
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
  fd_lock_mutex(&config_register_lock);
  scan=config_handlers;
  while (scan)
    if (FD_EQ(scan->var,symbol)) {
      if (reuse) reuse(scan);
      if (doc) {
        /* We don't override a real doc with a NULL doc.
           Possibly not the right thing. */
        if (scan->doc) u8_free(scan->doc);
        scan->doc=u8_strdup(doc);}
      scan->config_get_method=getfn;
      scan->config_set_method=setfn;
      scan->data=data;
      break;}
    else scan=scan->next;
  if (scan==NULL) {
    current=config_get(var);
    scan=u8_alloc(struct FD_CONFIG_HANDLER);
    scan->var=symbol;
    if (doc) scan->doc=u8_strdup(doc); else scan->doc=NULL;
    scan->flags=0;
    scan->config_get_method=getfn;
    scan->config_set_method=setfn;
    scan->data=data;
    scan->next=config_handlers;
    config_handlers=scan;}
  fd_unlock_mutex(&config_register_lock);
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
  fd_lock_mutex(&config_register_lock); {
    scan=config_handlers;
    while (scan) {
      fdtype var=scan->var;
      if (with_docs) {
        fdtype doc=((scan->doc)?(fdstring(scan->doc)):(FD_EMPTY_LIST));
        fdtype pair=fd_conspair(var,doc); fd_incref(var);
        FD_ADD_TO_CHOICE(results,pair);}
      else {fd_incref(var); FD_ADD_TO_CHOICE(results,var);}
      scan=scan->next;}}
  fd_unlock_mutex(&config_register_lock);
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
    retval=fd_config_set_consed(namebuf,value);
    if (namebuf!=_namebuf) u8_free(namebuf);
    return retval;}
  else return -1;
}

/* This takes a string of the form var=value */
FD_EXPORT int fd_config_default_assignment(u8_string assignment)
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
    if (!(fd_test(configuration_table,config_intern(namebuf),FD_VOID)))
      retval=fd_config_set_consed(namebuf,value);
    if (namebuf!=_namebuf) u8_free(namebuf);
    return retval;}
  else return -1;
}

/* DEPRECATED!
   Use fd_handle_argv() instead

   This takes an argv, argc combination and processes the argv
   elements which are configs (var=value again) */
FD_EXPORT int fd_argv_config(int argc,char **argv)
{
  int i=0, n=0;
  u8_threadcheck();
  while (i<argc)
    if (strchr(argv[i],'=')) {
      char *carg=argv[i++], *eq=strchr(carg,'=');
      if ((eq>carg)&&(*(eq-1)=='\\')) continue;
      else {
        u8_string arg=u8_fromlibc(carg);
        int retval=fd_config_assignment(arg);
        u8_free(arg);
        u8_log(LOG_INFO,fd_ArgvConfig,"   %s",arg);
        if (retval<0) u8_clear_errors(0);
        else n++;}}
    else i++;
  return n;
}

/* Processing argc,argv */

fdtype *fd_argv=NULL;
int fd_argc=-1;

static void set_vector_length(fdtype vector,int len);
static fdtype exec_arg=FD_FALSE, lisp_argv=FD_FALSE, string_argv=FD_FALSE;
static fdtype raw_argv=FD_FALSE, config_argv=FD_FALSE;
static int init_argc=0;
static size_t app_argc;

/* This takes an argv, argc combination and processes the argv elements
   which are configs (var=value again) */
FD_EXPORT fdtype *fd_handle_argv(int argc,char **argv,
                                 unsigned int arg_mask,
                                 size_t *arglen_ptr)
{
  if (argc>0) {
    u8_string exe_name=u8_fromlibc(argv[0]);
    fdtype interp=fd_lispstring(exe_name);
    fd_config_set("INTERPRETER",interp);
    fd_config_set("EXE",interp);
    fd_decref(interp);}

  if (fd_argv!=NULL)  {
    if ((init_argc>0) && (argc != init_argc)) 
      u8_log(LOG_WARN,"InconsistentArgv/c",
             "Trying to reprocess argv with a different argc (%d) length != %d",
             argc,init_argc);
    if (arglen_ptr) *arglen_ptr=fd_argc;
    return fd_argv;}
  else if (argc<=0) {
    u8_log(LOG_CRIT,"fd_handle_arg(invalid argv)",
           _("The argc length %d is not valid (>0)"),argc);
    return NULL;}
  else if (argv==NULL) {
    u8_log(LOG_CRIT,"fd_handle_arg(invalid argv)",
           _("The argv argument cannot be NULL!"),argc);
    return NULL;}
  else {
    int i=0, n=0, config_i=0;
    fdtype string_args=fd_make_vector(argc-1,NULL), string_arg=FD_VOID;
    fdtype lisp_args=fd_make_vector(argc-1,NULL), lisp_arg=FD_VOID;
    fdtype config_args=fd_make_vector(argc-1,NULL);
    fdtype raw_args=fd_make_vector(argc,NULL);
    fdtype *return_args=(arglen_ptr) ? (u8_alloc_n(argc-1,fdtype)) : (NULL);
    fdtype *_fd_argv=u8_alloc_n(argc-1,fdtype);
    u8_threadcheck();
    init_argc=argc;
    while (i<argc) {
      char *carg=argv[i];
      u8_string arg=u8_fromlibc(carg), eq=strchr(arg,'=');
      FD_VECTOR_SET(raw_args,i,fdtype_string(arg));
      /* Don't include argv[0] in the arglists */
      if (i==0) {
        i++; u8_free(arg); continue;} 
      else if ( ( n < 32 ) && ( ( (arg_mask) & (1<<i)) !=0 ) ) {
        i++; u8_free(arg); continue;}
      else i++;
      if ((eq!=NULL) && (eq>arg) && (*(eq-1)!='\\')) {
        int retval=(arg!=NULL) ? (fd_config_assignment(arg)) : (-1);
        FD_VECTOR_SET(config_args,config_i,fdtype_string(arg)); config_i++;
        if (retval<0) {
          u8_log(LOGCRIT,"FailedConfig",
                 "Couldn't handle the config argument `%s`",
                 (arg==NULL) ? ((u8_string)carg) : (arg));
          u8_clear_errors(0);}
        else u8_log(LOG_INFO,fd_ArgvConfig,"   %s",arg);
        u8_free(arg);
        continue;}
      string_arg=fdtype_string(arg);
      /* Note that fd_parse_arg should always return at least a lisp
         string */
      lisp_arg=fd_parse_arg(arg);
      if (return_args) {
        return_args[n]=lisp_arg; fd_incref(lisp_arg);}
      _fd_argv[n]=lisp_arg; fd_incref(lisp_arg);
      FD_VECTOR_SET(lisp_args,n,lisp_arg);
      FD_VECTOR_SET(string_args,n,string_arg);
      u8_free(arg);
      n++;}
    set_vector_length(lisp_args,n);
    lisp_argv = lisp_args;
    set_vector_length(string_args,n);
    string_argv = string_args;
    set_vector_length(config_args,n);
    config_argv = config_args;
    raw_argv = raw_args;
    app_argc=n;
    fd_argv=_fd_argv;
    fd_argc=n;
    if (return_args) {
      *arglen_ptr=n;
      return return_args;}
    else return NULL;}
}

static void set_vector_length(fdtype vector,int len)
{
  if (FD_VECTORP(vector)) {
    struct FD_VECTOR *vec = (struct FD_VECTOR *) vector;
    if (len>=0) {
      vec->length=len;
      return;}}
  u8_log(LOGCRIT,"Internal/CmdArgInitVec","Not a vector! %q",
         vector);
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
        if (fd_config_set(FD_SYMBOL_NAME(FD_CAR(entry)),(FD_CADR(entry)))<0) {
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
        if (fd_config_default(FD_SYMBOL_NAME(FD_CAR(entry)),(FD_CADR(entry)))<0) {
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
      if (fd_config_default_assignment(buf)<0)
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
    return fd_reterr(fd_ReadOnlyConfig,"fd_config_set",
                     FD_SYMBOL_NAME(v),FD_VOID);
  else if (FD_STRINGP(v))
    return fd_reterr(fd_ReadOnlyConfig,"fd_config_set",
                     FD_STRDATA(v),FD_VOID);
  else return fd_reterr(fd_ReadOnlyConfig,"fd_config_set",NULL,FD_VOID);
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
  if (FD_FIXNUMP(v)) {
    *ptr=FD_FIX2INT(v);
    return 1;}
  else return
         fd_reterr(fd_TypeError,"fd_intconfig_set",u8_strdup(_("fixnum")),v);
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
    int intval=FD_FIX2INT(v);
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
    *ptr=FD_FIX2INT(v); return 1;}
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


/* RLIMIT configs */

#if HAVE_SYS_RESOURCE_H

struct NAMED_RLIMIT {
  u8_string name; int code;};

#ifdef RLIMIT_CPU
static struct NAMED_RLIMIT MAXCPU={"max cpu",RLIMIT_CPU};
#endif
#ifdef RLIMIT_RSS
static struct NAMED_RLIMIT MAXRSS={"max resident memory",RLIMIT_RSS};
#endif
#ifdef RLIMIT_CORE
static struct NAMED_RLIMIT MAXCORE={"max core file size",RLIMIT_CORE};
#endif
#ifdef RLIMIT_NPROC
static struct NAMED_RLIMIT MAXNPROC={"max number of simulaneous processes",RLIMIT_NPROC};
#endif
#ifdef RLIMIT_NOFILE
static struct NAMED_RLIMIT MAXFILES={"max number of open files",RLIMIT_NOFILE};
#endif
#ifdef RLIMIT_STACK
static struct NAMED_RLIMIT MAXSTACK={"max tack size",RLIMIT_STACK};
#endif

FD_EXPORT fdtype fd_config_rlimit_get(fdtype ignored,void *vptr)
{
  struct rlimit rlim;
  struct NAMED_RLIMIT *nrl=(struct NAMED_RLIMIT *)vptr;
  int retval=getrlimit(nrl->code,&rlim);
  if (retval<0) {
    u8_condition cond=u8_strerror(errno);
    errno=0;
    return fd_err(cond,"rlimit_get",u8_strdup(nrl->name),FD_VOID);}
  else if (rlim.rlim_cur==RLIM_INFINITY)
    return FD_FALSE;
  else return FD_INT((long long)(rlim.rlim_cur));
}

FD_EXPORT int fd_config_rlimit_set(fdtype ignored,fdtype v,void *vptr)
{
  struct rlimit rlim;
  struct NAMED_RLIMIT *nrl=(struct NAMED_RLIMIT *)vptr;
  int retval=getrlimit(nrl->code,&rlim); rlim_t setval;
  if ((FD_FIXNUMP(v))||(FD_BIGINTP(v))) {
    long lval=fd_getint(v);
    if (lval<0) {
      fd_incref(v);
      fd_seterr(fd_TypeError,"fd_config_rlimit_set",
                u8_strdup("resource limit (integer)"),v);
      return -1;}
    else setval=lval;}
  else if (FD_FALSEP(v)) setval=(RLIM_INFINITY);
  else if ((FD_STRINGP(v))&&
           ((strcasecmp(FD_STRDATA(v),"unlimited")==0)||
            (strcasecmp(FD_STRDATA(v),"nolimit")==0)||
            (strcasecmp(FD_STRDATA(v),"infinity")==0)||
            (strcasecmp(FD_STRDATA(v),"infinite")==0)||
            (strcasecmp(FD_STRDATA(v),"false")==0)||
            (strcasecmp(FD_STRDATA(v),"none")==0)))
    setval=(RLIM_INFINITY);
  else {
    fd_incref(v);
    fd_seterr(fd_TypeError,"fd_config_rlimit_set",
              u8_strdup("resource limit (integer)"),v);
    return -1;}
  if (retval<0) {
    u8_condition cond=u8_strerror(errno); errno=0;
    return fd_err(cond,"rlimit_get",u8_strdup(nrl->name),FD_VOID);}
  else if (setval>rlim.rlim_max) {
    /* Should be more informative */
    fd_seterr(_("RLIMIT too high"),"set_rlimit",u8_strdup(nrl->name),FD_VOID);
    return -1;}
  if (setval==rlim.rlim_cur)
    u8_log(LOG_WARN,SetRLimit,"Setting for %s did not need to change",nrl->name);
  else if (setval==RLIM_INFINITY)
    u8_log(LOG_WARN,SetRLimit,"Setting %s to unlimited from %d",nrl->name,rlim.rlim_cur);
  else u8_log(LOG_WARN,SetRLimit,"Setting %s to %lld from %lld",nrl->name,(long long)setval,rlim.rlim_cur);
  rlim.rlim_cur=setval;
  retval=setrlimit(nrl->code,&rlim);
  if (retval<0) {
    u8_condition cond=u8_strerror(errno); errno=0;
    return fd_err(cond,"rlimit_set",u8_strdup(nrl->name),FD_VOID);}
  else return 1;
}
#endif


/* Option objects */

static fd_exception WeirdOption=_("Weird option specification");

FD_EXPORT fdtype fd_getopt(fdtype opts,fdtype key,fdtype dflt)
{
  if (FD_VOIDP(opts))
    return fd_incref(dflt);
  else if (FD_EMPTY_CHOICEP(opts))
    return fd_incref(dflt);
  else if ((FD_CHOICEP(opts)) || (FD_ACHOICEP(opts))) {
    FD_DO_CHOICES(opt,opts) {
      fdtype value=fd_getopt(opt,key,FD_VOID);
      if (!(FD_VOIDP(value))) {
        FD_STOP_DO_CHOICES; return value;}}
    return fd_incref(dflt);}
  else if (FD_QCHOICEP(opts))
    return fd_getopt(FD_XQCHOICE(opts)->choice,key,dflt);
  else while (!(FD_VOIDP(opts))) {
      if (FD_PAIRP(opts)) {
        fdtype car=FD_CAR(opts);
        if (FD_SYMBOLP(car)) {
          if (FD_EQ(key,car)) return FD_TRUE;}
        else if (FD_PAIRP(car)) {
          if (FD_EQ(FD_CAR(car),key))
            return fd_incref(FD_CDR(car));
          else {
            fdtype value=fd_getopt(car,key,FD_VOID);
            if (!(FD_VOIDP(value))) return value;}}
        else if (FD_TABLEP(car)) {
          fdtype value=fd_get(car,key,FD_VOID);
          if (!(FD_VOIDP(value))) return value;}
        else if ((FD_FALSEP(car))||(FD_EMPTY_LISTP(car))) {}
        else return fd_err(WeirdOption,"fd_getopt",NULL,car);
        opts=FD_CDR(opts);}
      else if (FD_SYMBOLP(opts))
        if (FD_EQ(key,opts)) return FD_TRUE;
        else return fd_incref(dflt);
      else if (FD_TABLEP(opts))
        return fd_get(opts,key,dflt);
      else if ((FD_EMPTY_LISTP(opts))||(FD_FALSEP(opts)))
        return fd_incref(dflt);
      else return fd_err(WeirdOption,"fd_getopt",NULL,opts);}
  return fd_incref(dflt);
}

static int boolopt(fdtype opts,fdtype key)
{
  while (!(FD_VOIDP(opts))) {
    if (FD_PAIRP(opts)) {
      fdtype car=FD_CAR(opts);
      if (FD_SYMBOLP(car)) {
        if (FD_EQ(key,car)) return 1;}
      else if (FD_PAIRP(car)) {
        if (FD_EQ(FD_CAR(car),key)) {
          if (FD_FALSEP(FD_CDR(car))) return 0;
          else return 1;}}
      else if (FD_FALSEP(car)) {}
      else if (FD_TABLEP(car)) {
        fdtype value=fd_get(car,key,FD_VOID);
        if (FD_FALSEP(value)) return 0;
        else if (!(FD_VOIDP(value))) {
          fd_decref(value); return 1;}}
      else return fd_err(WeirdOption,"fd_getopt",NULL,car);
      opts=FD_CDR(opts);}
    else if (FD_SYMBOLP(opts))
      if (FD_EQ(key,opts)) return 1;
      else return 0;
    else if (FD_TABLEP(opts)) {
      fdtype value=fd_get(opts,key,FD_VOID);
      if (FD_FALSEP(value)) return 0;
      else if (FD_VOIDP(value)) return 0;
      else return 1;}
    else if ((FD_EMPTY_LISTP(opts))||(FD_FALSEP(opts)))
      return 0;
    else return fd_err(WeirdOption,"fd_getopt",NULL,opts);}
  return 0;
}

FD_EXPORT int fd_testopt(fdtype opts,fdtype key,fdtype val)
{
  if (FD_VOIDP(opts)) return 0;
  else if ((FD_CHOICEP(opts)) || (FD_ACHOICEP(opts))) {
    FD_DO_CHOICES(opt,opts)
      if (fd_testopt(opt,key,val)) {
        FD_STOP_DO_CHOICES; return 1;}
    return 0;}
  else if (FD_VOIDP(val))
    return boolopt(opts,key);
  else if (FD_QCHOICEP(opts))
    return fd_testopt(FD_XQCHOICE(opts)->choice,key,val);
  else if (FD_EMPTY_CHOICEP(opts))
    return 0;
  else while (!(FD_VOIDP(opts))) {
         if (FD_PAIRP(opts)) {
           fdtype car=FD_CAR(opts);
           if (FD_SYMBOLP(car)) {
             if ((FD_EQ(key,car)) && (FD_TRUEP(val)))
               return 1;}
           else if (FD_PAIRP(car)) {
             if (FD_EQ(FD_CAR(car),key)) {
               if (FD_EQUAL(val,FD_CDR(car)))
                 return 1;
               else return 0;}}
           else if (FD_TABLEP(car)) {
             int tv=fd_test(car,key,val);
             if (tv) return tv;}
           else if (FD_FALSEP(car)) {}
           else return fd_err(WeirdOption,"fd_getopt",NULL,car);
           opts=FD_CDR(opts);}
         else if (FD_SYMBOLP(opts))
           if (FD_EQ(key,opts))
             if (FD_TRUEP(val)) return 1;
             else return 0;
           else return 0;
         else if (FD_TABLEP(opts))
           return fd_test(opts,key,val);
         else if ((FD_EMPTY_LISTP(opts))||(FD_FALSEP(opts)))
           return 0;
         else return fd_err(WeirdOption,"fd_getopt",NULL,opts);}
  return 0;
}


/* Managing error data */

FD_EXPORT void fd_free_exception_xdata(void *ptr)
{
  fdtype v=(fdtype)ptr;
  fd_decref(v);
}

FD_EXPORT void fd_seterr
  (u8_condition c,u8_context cxt,u8_string details,fdtype irritant)
{
  u8_push_exception(c,cxt,details,(void *)irritant,fd_free_exception_xdata);
}

FD_EXPORT int fd_poperr
  (u8_condition *c,u8_context *cxt,u8_string *details,fdtype *irritant)
{
  u8_exception current=u8_current_exception;
  if (current==NULL) return 0;
  if (c) *c=current->u8x_cond;
  if (cxt) *cxt=current->u8x_context;
  if (details) {
    /* If we're hanging onto the details, clear it from
       the structure before popping. */
    *details=current->u8x_details;
    current->u8x_details=NULL;}
  if (irritant) {
    if ((current->u8x_xdata) &&
        (current->u8x_free_xdata==fd_free_exception_xdata)) {
      /* Likewise for the irritant */
      *irritant=(fdtype)(current->u8x_xdata);
      current->u8x_xdata=NULL;
      current->u8x_free_xdata=NULL;}
    else *irritant=FD_VOID;}
  u8_pop_exception();
  return 1;
}

FD_EXPORT fdtype fd_exception_xdata(u8_exception ex)
{
  if ((ex->u8x_xdata) &&
      (ex->u8x_free_xdata==fd_free_exception_xdata))
    return (fdtype)(ex->u8x_xdata);
  else return FD_VOID;
}

FD_EXPORT int fd_reterr
  (u8_condition c,u8_context cxt,u8_string details,fdtype irritant)
{
  fd_seterr(c,cxt,details,irritant);
  return -1;
}

FD_EXPORT int fd_interr(fdtype x)
{
  return -1;
}

FD_EXPORT fdtype fd_err
  (fd_exception ex,u8_context cxt,u8_string details,fdtype irritant)
{
  if (FD_CHECK_PTR(irritant)) {
    if (details)
      fd_seterr(ex,cxt,u8_strdup(details),fd_incref(irritant));
    else fd_seterr(ex,cxt,NULL,fd_incref(irritant));}
  else if (details)
    fd_seterr(ex,cxt,u8_strdup(details),FD_VOID);
  else fd_seterr(ex,cxt,NULL,FD_VOID);
  return FD_ERROR_VALUE;
}

FD_EXPORT void fd_push_error_context(u8_context cxt,fdtype data)
{
  u8_push_exception
    (NULL,cxt,NULL,(void *)data,fd_free_exception_xdata);
}

FD_EXPORT fdtype fd_type_error
  (u8_string type_name,u8_context cxt,fdtype irritant)
{
  u8_string msg=u8_mkstring(_("object is not a %m"),type_name);
  fd_seterr(fd_TypeError,cxt,msg,fd_incref(irritant));
  return FD_TYPE_ERROR;
}

FD_EXPORT void fd_set_type_error(u8_string type_name,fdtype irritant)
{
  u8_string msg=u8_mkstring(_("object is not a %m"),type_name);
  fd_seterr(fd_TypeError,NULL,msg,fd_incref(irritant));
}

FD_EXPORT
void fd_print_exception(U8_OUTPUT *out,u8_exception ex)
{
  u8_printf(out,";;(ERROR %m)",ex->u8x_cond);
  if (ex->u8x_details) u8_printf(out," %m",ex->u8x_details);
  if (ex->u8x_context) u8_printf(out," (%s)",ex->u8x_context);
  u8_printf(out,"\n");
  if (ex->u8x_xdata) {
    fdtype irritant=fd_exception_xdata(ex);
    u8_printf(out,";;\t%q\n",irritant);}
}

FD_EXPORT
void fd_log_exception(u8_exception ex)
{
  if (ex->u8x_xdata) {
    fdtype irritant=fd_exception_xdata(ex);
    u8_log(LOG_WARN,ex->u8x_cond,"%m (%m)\n\t%q",
           (U8ALT((ex->u8x_details),((U8S0())))),
           (U8ALT((ex->u8x_context),((U8S0())))),
           irritant);}
  else u8_log(LOG_WARN,ex->u8x_cond,"%m (%m)\n\t%q",
              (U8ALT((ex->u8x_details),((U8S0())))),
              (U8ALT((ex->u8x_context),((U8S0())))));
}

FD_EXPORT
fdtype fd_exception_backtrace(u8_exception ex)
{
  fdtype result=FD_EMPTY_LIST;
  u8_condition cond=NULL;  u8_string details=NULL; u8_context cxt=NULL;
  while (ex) {
    u8_condition c=ex->u8x_cond;
    u8_string d=ex->u8x_details;
    u8_context cx=ex->u8x_context;
    fdtype x=fd_exception_xdata(ex);
    if ((c!=cond)||
        ((d)&&(d!=details))||
        ((cx)&&(cx!=cxt))) {
      u8_string sum=
        (((d)&&cx)?(u8_mkstring("%s (%s) %s",c,cx,d)):
         (d)?(u8_mkstring("%s: %s",c,d)):
         (cx)?(u8_mkstring("%s (%s)",c,cx)):
         ((u8_string)u8_strdup(c)));
      result=fd_conspair(fd_make_string(NULL,-1,sum),result);
      u8_free(sum);}
    if (!((FD_NULLP(x))||(FD_VOIDP(x)))) {
      if (FD_VECTORP(x)) {
        int len=FD_VECTOR_LENGTH(x);
        fdtype applyvec=fd_init_vector(NULL,len+1,NULL);
        int i=0; while (i<len) {
          fdtype elt=FD_VECTOR_REF(x,i); fd_incref(elt);
          FD_VECTOR_SET(applyvec,i+1,elt);
          i++;}
        FD_VECTOR_SET(applyvec,0,fd_intern("=>"));
        result=fd_conspair(applyvec,result);}
      else {
        fd_incref(x); result=fd_conspair(x,result);}}
    ex=ex->u8x_prev;}
  return result;
}

FD_EXPORT
void sum_exception(U8_OUTPUT *out,u8_exception ex,u8_exception bg)
{
  if (!(ex)) {
    u8_printf(out,"what error?");
    return;}
  else if ((bg==NULL) || ((bg->u8x_cond) != (ex->u8x_cond)))
    u8_printf(out,"(%m)",ex->u8x_cond);
  if ((bg==NULL) || ((bg->u8x_context) != (ex->u8x_context)))
    u8_printf(out," <%s>",ex->u8x_context);
  if ((bg==NULL) || ((bg->u8x_details) != (ex->u8x_details)))
    u8_printf(out," %m",ex->u8x_details);
  if (ex->u8x_xdata) {
    fdtype irritant=fd_exception_xdata(ex);
    if ((bg==NULL) || (bg->u8x_xdata==NULL))
      u8_printf(out," -- %q",irritant);
    else {
      fdtype bgirritant=fd_exception_xdata(bg);
      if (!(FD_EQUAL(irritant,bgirritant)))
        u8_printf(out," -- %q",irritant);}}
}

FD_EXPORT
void fd_sum_exception(U8_OUTPUT *out,u8_exception ex)
{
  u8_exception root=u8_exception_root(ex);
  int stacklen=u8_exception_stacklen(ex);

  sum_exception(out,root,NULL);
  u8_printf(out," << %d calls << ",stacklen);
  sum_exception(out,ex,root);
}

FD_EXPORT u8_string fd_errstring(u8_exception ex)
{
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
  if (ex==NULL) ex=u8_current_exception;
  fd_sum_exception(&out,ex);
  return out.u8_outbuf;
}

FD_EXPORT fd_exception fd_retcode_to_exception(fdtype err)
{
  switch (err) {
  case FD_EOF: case FD_EOD: return fd_UnexpectedEOD;
  case FD_PARSE_ERROR: case FD_EOX: return fd_ParseError;
  case FD_TYPE_ERROR: return fd_TypeError;
  case FD_RANGE_ERROR: return fd_RangeError;
  case FD_OOM: return fd_OutOfMemory;
  case FD_DTYPE_ERROR: return fd_DTypeError;
  default: return fd_UnknownError;
  }
}

FD_EXPORT
int fd_clear_errors(int report)
{
  u8_exception ex=u8_erreify(), scan=ex; int n_errs=0;
  while (scan) {
    if (report) {
      u8_string sum=fd_errstring(scan);
      u8_logger(LOG_ERR,scan->u8x_cond,sum);
      u8_free(sum);}
    scan=scan->u8x_prev;
    n_errs++;}
  u8_free_exception(ex,1);
  return n_errs;
}

/* Thread Tables */

#if FD_USE_TLS
static u8_tld_key threadtable_key;
static fdtype get_threadtable()
{
  fdtype table=(fdtype)u8_tld_get(threadtable_key);
  if (table) return table;
  else {
    table=fd_empty_slotmap();
    u8_tld_set(threadtable_key,(void*)table);
    return table;}
}
FD_EXPORT void fd_reset_threadvars()
{
  fdtype table=(fdtype)u8_tld_get(threadtable_key);
  fdtype new_table=fd_empty_slotmap();
  u8_tld_set(threadtable_key,(void*)new_table);
  if (table) fd_decref(table);
}
#elif FD_THREADS_ENABLED
static fdtype __thread thread_table=FD_VOID;
static fdtype get_threadtable()
{
  if (FD_TABLEP(thread_table)) return thread_table;
  else return (thread_table=fd_empty_slotmap());
}
FD_EXPORT void fd_reset_threadvars()
{
  fdtype table=thread_table;
  thread_table=fd_empty_slotmap();
  fd_decref(table);
}
#else
static fdtype thread_table=FD_VOID;
static fdtype get_threadtable()
{
  if (FD_TABLEP(thread_table)) return thread_table;
  else return (thread_table=fd_empty_slotmap());
}
FD_EXPORT void fd_reset_threadvars()
{
  fdtype table=thread_table;
  thread_table=fd_empty_slotmap();
  fd_decref(table);
}
#endif

FD_EXPORT fdtype fd_thread_get(fdtype var)
{
  return fd_get(get_threadtable(),var,FD_VOID);
}

FD_EXPORT int fd_thread_set(fdtype var,fdtype val)
{
  return fd_store(get_threadtable(),var,val);
}

FD_EXPORT int fd_thread_add(fdtype var,fdtype val)
{
  return fd_add(get_threadtable(),var,val);
}

/* Request objects */

#if FD_USE_TLS
static u8_tld_key reqinfo_key;
static fdtype try_reqinfo()
{
  fdtype table=(fdtype)u8_tld_get(reqinfo_key);
  if (table) return table;
  else return FD_EMPTY_CHOICE;
}
static fdtype get_reqinfo()
{
  fdtype table=(fdtype)u8_tld_get(reqinfo_key);
  if ((table)&&(FD_TABLEP(table))) return table;
  else {
    fdtype newinfo=fd_empty_slotmap();
    fd_slotmap sm=FD_GET_CONS(newinfo,fd_slotmap_type,fd_slotmap);
    u8_write_lock(&(sm->rwlock)); sm->uselock=0;
    u8_tld_set(reqinfo_key,(void *)newinfo);
    return newinfo;}
}
static void set_reqinfo(fdtype table)
{
  u8_tld_set(reqinfo_key,(void *)table);
}
#else
#if FD_THREADS_ENABLED
static fdtype __thread reqinfo=FD_VOID;
#else
static fdtype reqinfo=FD_VOID;
#endif
static fdtype try_reqinfo()
{
  if ((reqinfo)&&(FD_TABLEP(reqinfo))) return reqinfo;
  else return FD_EMPTY_CHOICE;
}
static fdtype get_reqinfo()
{
  if ((reqinfo)&&(FD_TABLEP(reqinfo))) return reqinfo;
  else {
    fdtype newinfo=fd_empty_slotmap();
    fd_slotmap sm=FD_GET_CONS(newinfo,fd_slotmap_type,fd_slotmap);
    u8_write_lock(&(sm->rwlock)); sm->uselock=0;
    reqinfo=newinfo;
    return newinfo;}
}
static void set_reqinfo(fdtype table)
{
  reqinfo=table;
}
#endif

FD_EXPORT fdtype fd_req(fdtype var)
{
  return fd_get(try_reqinfo(),var,FD_VOID);
}

FD_EXPORT int fd_isreqlive()
{
  fdtype info=try_reqinfo();
  if (FD_TABLEP(info)) return 1; else return 0;
}

FD_EXPORT fdtype fd_req_get(fdtype var,fdtype dflt)
{
  fdtype info=try_reqinfo();
  if (FD_TABLEP(info)) return fd_get(info,var,dflt);
  else return fd_incref(dflt);
}

FD_EXPORT int fd_req_store(fdtype var,fdtype val)
{
  fdtype info=get_reqinfo();
  return fd_store(info,var,val);
}

FD_EXPORT int fd_req_test(fdtype var,fdtype val)
{
  return fd_test(try_reqinfo(),var,val);
}

FD_EXPORT int fd_req_add(fdtype var,fdtype val)
{
  return fd_add(get_reqinfo(),var,val);
}

FD_EXPORT int fd_req_drop(fdtype var,fdtype val)
{
  return fd_drop(try_reqinfo(),var,val);
}

FD_EXPORT int fd_req_push(fdtype var,fdtype val)
{
  fdtype info=get_reqinfo(), cur=fd_get(info,var,FD_EMPTY_LIST);
  fdtype new_pair=fd_conspair(val,cur);
  fd_store(info,var,new_pair);
  fd_incref(val); fd_decref(new_pair);
  return 1;
}

FD_EXPORT fdtype fd_req_call(fd_reqfn reqfn)
{
  fdtype info=get_reqinfo();
  return reqfn(info);
}

FD_EXPORT void fd_use_reqinfo(fdtype newinfo)
{
  fdtype curinfo=try_reqinfo();
  if (curinfo==newinfo) return;
  if ((FD_TRUEP(newinfo))&&(FD_TABLEP(curinfo))) return;
  if (FD_SLOTMAPP(curinfo)) {
    fd_slotmap sm=FD_GET_CONS(curinfo,fd_slotmap_type,fd_slotmap);
    if (sm->uselock==0) {
      sm->uselock=1;
      u8_rw_unlock(&(sm->rwlock));}}
  else if (FD_HASHTABLEP(curinfo)) {
    fd_hashtable ht=FD_GET_CONS(curinfo,fd_hashtable_type,fd_hashtable);
    if (ht->uselock==0) {
      ht->uselock=1;
      u8_rw_unlock(&(ht->rwlock));}}
  else {}
  if ((FD_FALSEP(newinfo))||
      (FD_VOIDP(newinfo))||
      (FD_EMPTY_CHOICEP(newinfo))) {
    fd_decref(curinfo);
    set_reqinfo(newinfo);
    return;}
  else if (FD_TRUEP(newinfo)) {
    fd_slotmap sm; newinfo=fd_empty_slotmap();
    sm=FD_GET_CONS(newinfo,fd_slotmap_type,fd_slotmap);
    u8_write_lock(&(sm->rwlock)); sm->uselock=0;}
  else if ((FD_SLOTMAPP(newinfo))||(FD_SLOTMAPP(curinfo)))
    fd_incref(newinfo);
  else {
    u8_log(LOG_CRIT,fd_TypeError,
           "USE_REQINFO arg isn't slotmap or table: %q",newinfo);
    fd_slotmap sm; newinfo=fd_empty_slotmap();
    sm=FD_GET_CONS(newinfo,fd_slotmap_type,fd_slotmap);
    u8_write_lock(&(sm->rwlock)); sm->uselock=0;}
  if (FD_SLOTMAPP(newinfo)) {
    fd_slotmap sm=FD_GET_CONS(newinfo,fd_slotmap_type,fd_slotmap);
    u8_write_lock(&(sm->rwlock));
    sm->uselock=0;}
  else if (FD_HASHTABLEP(newinfo)) {
    fd_hashtable ht=FD_GET_CONS(newinfo,fd_hashtable_type,fd_hashtable);
    u8_write_lock(&(ht->rwlock));
    ht->uselock=0;}
  set_reqinfo(newinfo);
  fd_decref(curinfo);
}

FD_EXPORT fdtype fd_push_reqinfo(fdtype newinfo)
{
  fdtype curinfo=try_reqinfo();
  if (curinfo==newinfo) return curinfo;
  if (FD_SLOTMAPP(curinfo)) {
    fd_slotmap sm=FD_GET_CONS(curinfo,fd_slotmap_type,fd_slotmap);
    if (sm->uselock==0) {
      sm->uselock=1;
      u8_rw_unlock(&(sm->rwlock));}}
  else if (FD_HASHTABLEP(curinfo)) {
    fd_hashtable ht=FD_GET_CONS(curinfo,fd_hashtable_type,fd_hashtable);
    if (ht->uselock==0) {
      ht->uselock=1;
      u8_rw_unlock(&(ht->rwlock));}}
  if ((FD_FALSEP(newinfo))||
      (FD_VOIDP(newinfo))||
      (FD_EMPTY_CHOICEP(newinfo))) {
    set_reqinfo(newinfo);
    return curinfo;}
  else if (FD_TRUEP(newinfo)) newinfo=fd_empty_slotmap();
  else if ((FD_SLOTMAPP(newinfo))||(FD_SLOTMAPP(curinfo)))
    fd_incref(newinfo);
  else {
    u8_log(LOG_CRIT,fd_TypeError,
           "PUSH_REQINFO arg isn't slotmap or table: %q",newinfo);
    newinfo=fd_empty_slotmap();}
  if (FD_SLOTMAPP(newinfo)) {
    fd_slotmap sm=FD_GET_CONS(newinfo,fd_slotmap_type,fd_slotmap);
    u8_write_lock(&(sm->rwlock));
    sm->uselock=0;}
  else if (FD_HASHTABLEP(newinfo)) {
    fd_hashtable ht=FD_GET_CONS(newinfo,fd_hashtable_type,fd_hashtable);
    u8_write_lock(&(ht->rwlock));
    ht->uselock=0;}
  set_reqinfo(newinfo);
  return curinfo;
}

/* Req logging */

/* Request objects */

#if FD_USE_TLS
static u8_tld_key reqlog_key;
static struct U8_OUTPUT *try_reqlog()
{
  struct U8_OUTPUT *table=(struct U8_OUTPUT *)u8_tld_get(reqlog_key);
  if (table) return table;
  else return NULL;
}
static struct U8_OUTPUT *get_reqlog()
{
  struct U8_OUTPUT *log=(struct U8_OUTPUT *)u8_tld_get(reqlog_key);
  if (log) return log;
  else {
    struct U8_OUTPUT *newlog=u8_open_output_string(1024);
    u8_tld_set(reqlog_key,(void *)newlog);
    return newlog;}
}
static void set_reqlog(struct U8_OUTPUT *stream)
{
  struct U8_OUTPUT *log=(struct U8_OUTPUT *)u8_tld_get(reqlog_key);
  if (stream==log) return;
  else if (log) u8_close_output(log);
  u8_tld_set(reqlog_key,(void *)stream);
}
#else
#if FD_THREADS_ENABLED
static struct U8_OUTPUT __thread *reqlog=NULL;
#else
static struct U8_OUTPUT *reqlog=FD_VOID;
#endif
static struct U8_OUTPUT *try_reqlog()
{
  return reqlog;
}
static struct U8_OUTPUT *get_reqlog()
{
  if (reqlog) return reqlog;
  else {
    struct U8_OUTPUT *newlog=u8_open_output_string(1024);
    reqlog=newlog;
    return newlog;}
}
static void set_reqlog(struct U8_OUTPUT *stream)
{
  if (reqlog==stream) return;
  else {
    if (reqlog) u8_close_output(reqlog);
    reqlog=stream;}
}
#endif


FD_EXPORT struct U8_OUTPUT *fd_reqlog(int force)
{
  if (force<0) {
    set_reqlog(NULL);
    return NULL;}
  else if (force) return get_reqlog();
  else return try_reqlog();
}

FD_EXPORT int fd_reqlogger
  (u8_condition c,u8_context cxt,u8_string message)
{
  struct U8_OUTPUT *out=get_reqlog();
  if (!(out)) return 0;
  if ((c)&&(cxt))
    u8_printf(out,"[%l*t] (%s) @%s %s",c,cxt,message);
  else if (c)
    u8_printf(out,"[%l*t] (%s) %s",c,message);
  else if (cxt)
    u8_printf(out,"[%l*t] @%s %s",cxt,message);
  else u8_printf(out,"[%l*t] %s",message);
  return 1;
}

/* Debugging support functions */

FD_EXPORT fd_ptr_type _fd_ptr_type(fdtype x)
{
  return FD_PTR_TYPE(x);
}

FD_EXPORT fdtype _fd_debug(fdtype x)
{
  return x;
}

/* Configuration for session and app id info */

static fdtype config_getappid(fdtype var,void *data)
{
  return fdtype_string(u8_appid());
}

static int config_setappid(fdtype var,fdtype val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_identify_application(FD_STRDATA(val));
    return 1;}
  else return -1;
}

static fdtype config_getpid(fdtype var,void *data)
{
  pid_t pid=getpid();
  return FD_INT(((unsigned long)pid));
}

static fdtype config_getppid(fdtype var,void *data)
{
  pid_t pid=getppid();
  return FD_INT(((unsigned long)pid));
}

static fdtype config_getsessionid(fdtype var,void *data)
{
  return fdtype_string(u8_sessionid());
}

static int config_setsessionid(fdtype var,fdtype val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_identify_session(FD_STRDATA(val));
    return 1;}
  else return -1;
}

static fdtype config_getutf8warn(fdtype var,void *data)
{
  if (u8_config_utf8warn(-1))
    return FD_TRUE;
  else return FD_FALSE;
}

static int config_setutf8warn(fdtype var,fdtype val,void *data)
{
  if (FD_TRUEP(val))
    if (u8_config_utf8warn(1)) return 0;
    else return 1;
  else if (u8_config_utf8warn(0)) return 1;
  else return 0;
}

static fdtype config_getutf8err(fdtype var,void *data)
{
  if (u8_config_utf8err(-1))
    return FD_TRUE;
  else return FD_FALSE;
}

static int config_setutf8err(fdtype var,fdtype val,void *data)
{
  if (FD_TRUEP(val))
    if (u8_config_utf8err(1)) return 0;
    else return 1;
  else if (u8_config_utf8err(0)) return 1;
  else return 0;
}

/* Google profiling control */

#if 0
static u8_string google_profile=NULL;

static int set_google_profile(fdtype var,fdtype val,void *data)
{
  if (FD_STRINGP(val)) {
    if (google_profile!=NULL) {
      ProfilerStop();
      u8_free(google_profile);
      google_profile=NULL;}
    ProfilerStart(FD_STRDATA(val));
    google_profile=u8_strdup(FD_STRDATA(val));
    return 1;}
  else if (FD_FALSEP(val)) {
    if (google_profile) {
      ProfilerStop();
      u8_free(google_profile);
      google_profile=NULL;
      return 1;}
    else return 0;}
  else return -1;
}

static fdtype get_google_profile(fdtype var,void *data)
{
  if (google_profile)
    return fdtype_string(google_profile);
  else return FD_FALSE;
}
#endif


/* Random seed initialization */

static fd_exception TimeFailed="call to time() failed";

static unsigned int randomseed=0x327b23c6;

static fdtype config_getrandomseed(fdtype var,void *data)
{
  if (randomseed<FD_MAX_FIXNUM) return FD_INT(randomseed);
  else return (fdtype)fd_ulong_to_bigint(randomseed);
}

static int config_setrandomseed(fdtype var,fdtype val,void *data)
{
  if (((FD_SYMBOLP(val)) && ((strcmp(FD_XSYMBOL_NAME(val),"TIME"))==0)) ||
      ((FD_STRINGP(val)) && ((strcmp(FD_STRDATA(val),"TIME"))==0))) {
    time_t tick=time(NULL);
    if (tick<0) {
      u8_graberr(-1,"time",NULL);
      fd_seterr(TimeFailed,"setrandomseed",NULL,FD_VOID);
      return -1;}
    else {
      randomseed=(unsigned int)tick;
      u8_randomize(randomseed);
      return 1;}}
  else if (FD_FIXNUMP(val)) {
    randomseed=FD_FIX2INT(val);
    u8_randomize(randomseed);
    return 1;}
  else if (FD_BIGINTP(val)) {
    randomseed=(unsigned int)(fd_bigint_to_long((fd_bigint)val));
    u8_randomize(randomseed);
    return 1;}
  else return -1;
}

/* LOGLEVEL */

static int loglevelconfig_set(fdtype var,fdtype val,void *data)
{
  if (FD_FIXNUMP(val)) {
    int *valp=(int *)data; *valp=FD_FIX2INT(val);
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

/* RUNBASE */

static u8_string runbase_config=NULL, runbase=NULL;

FD_EXPORT u8_string fd_runbase_filename(u8_string suffix)
{
  if (runbase==NULL) {
    if (runbase_config==NULL) {
      u8_string wd=u8_getcwd(), appid=u8_string_subst(u8_appid(),"/",":");
      runbase=u8_mkpath(wd,appid);
      u8_free(appid);}
    else if (u8_directoryp(runbase_config)) {
      /* If the runbase is a directory, create files using the appid. */
      u8_string appid=u8_string_subst(u8_appid(),"/",":");
      runbase=u8_mkpath(runbase_config,appid);
      u8_free(appid);}
  /* Otherwise, use the configured name as the prefix */
    else runbase=u8_strdup(runbase_config);}
  if (suffix==NULL)
    return u8_strdup(runbase);
  else return u8_string_append(runbase,suffix,NULL);
}

static fdtype config_getrunbase(fdtype var,void *data)
{
  if (runbase==NULL) return FD_FALSE;
  else return fdtype_string(runbase);
}

static int config_setrunbase(fdtype var,fdtype val,void *data)
{
  if (runbase)
    return fd_err("Runbase already set and used","config_set_runbase",runbase,FD_VOID);
  else if (FD_STRINGP(val)) {
    runbase_config=u8_strdup(FD_STRDATA(val));
    return 1;}
  else return fd_type_error(_("string"),"config_setrunbase",val);
}

/* fd_setapp */

FD_EXPORT void fd_setapp(u8_string spec,u8_string statedir)
{
  if (strchr(spec,'/')) {
    u8_string fullpath=
      ((spec[0]=='/')?((u8_string)(u8_strdup(spec))):(u8_abspath(spec,NULL)));
    u8_string base=u8_basename(spec,"*");
    u8_identify_application(base);
    if (statedir) runbase=u8_mkpath(statedir,base);
    else {
      u8_string dir=u8_dirname(fullpath);
      runbase=u8_mkpath(dir,base);
      u8_free(dir);}
    u8_free(base); u8_free(fullpath);}
  else {
    u8_byte *atpos=strchr(spec,'@');
    u8_string appid=((atpos)?(u8_slice(spec,atpos)):
                     ((u8_string)(u8_strdup(spec))));
    u8_identify_application(appid);
    if (statedir)
      runbase=u8_mkpath(statedir,appid);
    else {
      u8_string wd=u8_getcwd();
      runbase=u8_mkpath(wd,appid);
      u8_free(wd);}
    u8_free(appid);}
}

/* Accessing source file registry */

static void add_source_file(u8_string s,void *vp)
{
  fdtype *valp=(fdtype *)vp;
  fdtype val=*valp;
  FD_ADD_TO_CHOICE(val,fdtype_string(s));
  *valp=val;
}

static fdtype config_get_source_files(fdtype var,void *data)
{
  fdtype result=FD_EMPTY_CHOICE;
  u8_for_source_files(add_source_file,&result);
  return result;
}

static int config_add_source_file(fdtype var,fdtype val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_string stringval=u8_strdup(FD_STRDATA(val));
    u8_register_source_file(stringval);
    return 1;}
  else {
    fd_type_error(_("string"),"config_addsourcefile",val);
    return -1;}
}

/* Termination */

static struct FD_ATEXIT {
  fdtype handler; struct FD_ATEXIT *next;} *atexit_handlers=NULL;
static int n_atexit_handlers=0;

static fdtype config_atexit_get(fdtype var,void *data)
{
  struct FD_ATEXIT *scan; int i=0; fdtype result;
  fd_lock_mutex(&atexit_handlers_lock);
  result=fd_make_vector(n_atexit_handlers,NULL);
  scan=atexit_handlers; while (scan) {
    fdtype handler=scan->handler; fd_incref(handler);
    FD_VECTOR_SET(result,i,handler);
    scan=scan->next; i++;}
  fd_unlock_mutex(&atexit_handlers_lock);
  return result;
}

static int config_atexit_set(fdtype var,fdtype val,void *data)
{
  struct FD_ATEXIT *fresh=u8_malloc(sizeof(struct FD_ATEXIT));
  if (!(FD_APPLICABLEP(val))) {
    fd_type_error("applicable","config_atexit",val);
    return -1;}
  fd_lock_mutex(&atexit_handlers_lock);
  fresh->next=atexit_handlers; fresh->handler=val; fd_incref(val);
  n_atexit_handlers++; atexit_handlers=fresh;
  fd_unlock_mutex(&atexit_handlers_lock);
  return 1;
}

FD_EXPORT void fd_doexit(fdtype arg)
{
  struct FD_ATEXIT *scan, *tmp;
  if (fd_argv) {
    int i=0, n=fd_argc; while (i<n) {
      fdtype elt=fd_argv[i++]; fd_decref(elt);}
    u8_free(fd_argv);
    fd_argv=NULL;
    fd_argc=-1;}
  if (!(atexit_handlers)) {
    u8_log(LOG_DEBUG,"fd_doexit","No FramerD exit handlers!");
    return;}
  fd_lock_mutex(&atexit_handlers_lock);
  u8_log(LOG_NOTICE,"fd_doexit","Running %d FramerD exit handlers",
         n_atexit_handlers);
  scan=atexit_handlers; atexit_handlers=NULL;
  fd_unlock_mutex(&atexit_handlers_lock);
  while (scan) {
    fdtype handler=scan->handler, result=FD_VOID;
    u8_log(LOG_INFO,"fd_doexit","Running FramerD exit handler %q",handler);
    if ((FD_FUNCTIONP(handler))&&(FD_FUNCTION_ARITY(handler)))
      result=fd_apply(handler,1,&arg);
    else result=fd_apply(handler,0,NULL);
    if (FD_ABORTP(result)) {
      fd_clear_errors(1);}
    else fd_decref(result);
    fd_decref(handler); tmp=scan; scan=scan->next;
    u8_free(tmp);}
  fd_decref(exec_arg); exec_arg=FD_FALSE; 
  fd_decref(lisp_argv); lisp_argv=FD_FALSE;
  fd_decref(string_argv); string_argv=FD_FALSE;
  fd_decref(raw_argv); raw_argv=FD_FALSE;
  fd_decref(config_argv); config_argv=FD_FALSE;
}

static void doexit_atexit(){fd_doexit(FD_FALSE);}

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

/* UID/GID setting */

static int resolve_uid(fdtype val)
{
  if (FD_FIXNUMP(val)) return (gid_t)(FD_FIX2INT(val));
#if ((HAVE_GETPWNAM_R)||(HAVE_GETPWNAM))
  if (FD_STRINGP(val)) {
    struct passwd _uinfo, *uinfo; char buf[1024]; int retval;
#if HAVE_GETPWNAM_R
    retval=getpwnam_r(FD_STRDATA(val),&_uinfo,buf,1024,&uinfo);
#else
    uinfo=getpwnam(FD_STRDATA(val));
#endif
    if ((retval<0)||(uinfo==NULL)) {
      if (errno) u8_graberr(errno,"resolve_uid",NULL);
      fd_seterr("BadUser","resolve_uid",NULL,val);
      return (uid_t) -1;}
    else return uinfo->pw_uid;}
  else return fd_type_error("userid","resolve_uid",val);
#else
  return -1;
#endif
}

static int resolve_gid(fdtype val)
{
  if (FD_FIXNUMP(val)) return (gid_t)(FD_FIX2INT(val));
#if ((HAVE_GETGRNAM_R)||(HAVE_GETGRNAM))
  if (FD_STRINGP(val)) {
    struct group _ginfo, *ginfo; char buf[1024]; int retval;
#if HAVE_GETGRNAM_R
    retval=getgrnam_r(FD_STRDATA(val),&_ginfo,buf,1024,&ginfo);
#else
    ginfo=getgrnam(FD_STRDATA(val));
#endif
    if ((retval<0)||(ginfo==NULL)) {
      if (errno) u8_graberr(errno,"resolve_gid",NULL);
      fd_seterr("BadGroup","resolve_gid",NULL,val);
      return (uid_t) -1;}
    else return ginfo->gr_gid;}
  else return fd_type_error("groupid","resolve_gid",val);
#else
  return -1;
#endif
}

/* Determine which functions to use */

#if HAVE_GETEUID
#define GETUIDFN geteuid
#elif HAVE_GETUID
#define GETUIDFN getuid
#endif

#if HAVE_SETEUID
#define SETUIDFN seteuid
#elif HAVE_SETUID
#define SETUIDFN setuid
#endif

#if HAVE_GETEGID
#define GETGIDFN getegid
#elif HAVE_GETGID
#define GETGIDFN getgid
#endif

#if HAVE_SETEGID
#define SETGIDFN setegid
#elif HAVE_SETGID
#define SETGIDFN setgid
#endif

/* User config functions */

#if (HAVE_GETUID|HAVE_GETEUID)
static fdtype config_getuser(fdtype var,void *data)
{
  uid_t gid=GETUIDFN(); int ival=(int)gid;
  return FD_INT(ival);
}
#else
static fdtype config_getuser(fdtype var,void *data)
{
  return FD_FALSE;
}
#endif

#if ((HAVE_GETUID|HAVE_GETEUID)&((HAVE_SETUID|HAVE_SETEUID)))
static int config_setuser(fdtype var,fdtype val,void *data)
{
  uid_t cur_uid=GETUIDFN(); int uid=resolve_uid(val);
  if (uid<0) return -1;
  else if (cur_uid==uid) return 0;
  else {
    int rv=SETUIDFN(uid);
    if (rv<0) {
      u8_graberr(errno,"config_setuser",NULL);
      u8_log(LOG_CRIT,"Can't set user","Can't change user ID from %d",cur_uid);
      fd_seterr("CantSetUser","config_setuser",NULL,uid);
      return -1;}
    return 1;}
}
#else
static int config_setuser(fdtype var,fdtype val,void *data)
{
  u8_log(LOG_CRIT,"Can't set user","Can't change user ID in this OS");
  fd_seterr("SystemCantSetUser","config_setuser",NULL,val);
  return -1;
}
#endif

/* User config functions */

#if (HAVE_GETGID|HAVE_GETEGID)
static fdtype config_getgroup(fdtype var,void *data)
{
  gid_t gid=GETGIDFN(); int i=(int)gid;
  return FD_INT(i);
}
#else
static fdtype config_getgroup(fdtype var,void *data)
{
  return FD_FALSE;
}
#endif

#if (HAVE_SETGID|HAVE_SETEGID)
static int config_setgroup(fdtype var,fdtype val,void *data)
{
  gid_t cur_gid=GETGIDFN(); int gid=resolve_gid(val);
  if (gid<0) return -1;
  else if (cur_gid==gid) return 0;
  else {
    int rv=SETGIDFN(gid);
    if (rv<0) {
      u8_graberr(errno,"config_setgroup",NULL);
      u8_log(LOG_CRIT,"Can't set group","Can't change group ID from %d",cur_gid);
      fd_seterr("CantSetGroup","config_setgroup",NULL,gid);
      return -1;}
    return 1;}
}
#else
static int config_setgroup(fdtype var,fdtype val,void *data)
{
  u8_log(LOG_CRIT,"Can't set group","Can't change group ID in this OS");
  fd_seterr("SystemCantSetGroup","config_setgroup",NULL,val);
  return -1;
}
#endif

/* Initialization */

static int boot_config()
{
  u8_byte *config_string=(u8_byte *)u8_getenv("FD_BOOT_CONFIG");
  u8_byte *scan, *end; int count=0;
  if (config_string==NULL) config_string=u8_strdup(FD_BOOT_CONFIG);
  else config_string=u8_strdup(config_string);
  scan=config_string; end=strchr(scan,';');
  while (scan) {
    if (end==NULL) {
      fd_config_assignment(scan); count++;
      break;}
    *end='\0'; fd_config_assignment(scan); count++;
    scan=end+1; end=strchr(scan,';');}
  u8_free(config_string);
  return count;
}

/* Log functions */

static fdtype framerd_logfns=FD_EMPTY_CHOICE;
static fdtype framerd_logfn=FD_VOID;
static int using_fd_logger=0;
static int req_loglevel=-1, req_logonly=-1;
#if FD_THREADS_ENABLED
static u8_mutex log_lock;
#endif

#define MAX_LOGLEVEL 8
static char *loglevel_names[]=
  {"Emergency","Alert","Critical","Error","Warning","Notice","Info",
   "Debug","Detail","Deluge"};

U8_EXPORT int u8_default_logger(int loglevel,u8_condition c,u8_string message);

static int default_log_error(void);

static int fd_logger(int loglevel,u8_condition c,u8_string message)
{
  int local_log=(loglevel<0);
  int abs_loglevel=((loglevel<0)?(-loglevel):(loglevel));
  fdtype ll=FD_INT(loglevel);
  fdtype csym=((c==NULL)?(FD_FALSE):(fd_intern((u8_string)c)));
  struct U8_OUTPUT *reqout=((req_loglevel>=loglevel)?(try_reqlog()):(NULL));
  fdtype mstring=fd_make_string(NULL,-1,message);
  fdtype args[3]={ll,csym,mstring};
  fdtype logfns=fd_make_simple_choice(framerd_logfns);
  char *level=((abs_loglevel>MAX_LOGLEVEL)?(NULL):
               (loglevel_names[abs_loglevel]));
  if (reqout) {
    struct U8_XTIME xt;
    u8_local_xtime(&xt,-1,u8_nanosecond,0);
    if (local_log)
      u8_printf(reqout,"<logentry level='%d' scope='local'>",
                abs_loglevel);
    else u8_printf(reqout,"<logentry level='%d'>",abs_loglevel);
    if (level) u8_printf(reqout,"\n\t<level>%s</level>",level);
    u8_printf(reqout,"\n\t<datetime tick='%ld' nsecs='%d'>%Xlt</datetime>",
              xt.u8_tick,xt.u8_nsecs,&xt);
    if (c) u8_printf(reqout,"\n\t<condition>%s</condition>",c);
    u8_printf(reqout,"\n\t<message>\n%s\n\t</message>",message);
    u8_printf(reqout,"\n</logentry>\n");}
  if ((reqout!=NULL)&&
      (!(local_log))&&
      (req_logonly>=0)&&
      (abs_loglevel>=LOG_NOTIFY)&&
      (abs_loglevel>=req_logonly))
    return 0;
  if (FD_VOIDP(framerd_logfn)) u8_default_logger(loglevel,c,message);
  else {
    fdtype logfn=fd_incref(framerd_logfn);
    fdtype v=fd_apply(logfn,3,args);
    if (FD_ABORTP(v)) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,1024);
      u8_default_logger(loglevel,c,message);
      u8_default_logger(LOG_CRIT,"Log Error","FramerD log call failed");
      default_log_error();
      framerd_logfn=FD_VOID;
      fd_decref(logfn); fd_decref(logfn);}
    fd_decref(v);}
  {
    FD_DO_CHOICES(logfn,framerd_logfns) {
      fdtype v=fd_apply(logfn,3,args);
      if (FD_ABORTP(v)) {
        u8_default_logger(LOG_CRIT,"Log Error","FramerD log call failed");
        default_log_error();}
      fd_decref(v);}}
  fd_decref(mstring); fd_decref(ll); fd_decref(logfns);
  return 1;
}

static int default_log_error()
{
  u8_exception ex=u8_erreify(), scan=ex; int n_errs=0;
  while (scan) {
    u8_string sum=fd_errstring(scan);
    u8_logger(LOG_ERR,scan->u8x_cond,sum);
    u8_free(sum);
    scan=scan->u8x_prev;
    n_errs++;}
  u8_free_exception(ex,1);
  return n_errs;
}

static void use_fd_logger()
{
  if (using_fd_logger) return;
  else {
    u8_lock_mutex(&log_lock);
    if (!(using_fd_logger)) {
      u8_set_logfn(fd_logger);
      using_fd_logger=1;}
    u8_unlock_mutex(&log_lock);}
}

static fdtype config_get_logfn(fdtype var,void *data)
{
  if (FD_VOIDP(framerd_logfn)) return FD_FALSE;
  else return fd_incref(framerd_logfn);
}

static int config_set_logfn(fdtype var,fdtype val,void *data)
{
  if (FD_VOIDP(framerd_logfn)) {
    framerd_logfn=val;
    fd_incref(val);
    use_fd_logger();
    return 1;}
  else if (framerd_logfn==val)
    return 0;
  else {
    fdtype oldfn=framerd_logfn;
    framerd_logfn=val;
    fd_incref(val);
    fd_decref(oldfn);
    return 1;}
}

static fdtype config_get_logfns(fdtype var,void *data)
{
  return fd_incref(framerd_logfns);
}

static int config_add_logfn(fdtype var,fdtype val,void *data)
{
  fdtype arity=-1;
  if (FD_FUNCTIONP(val)) arity=FD_FUNCTION_ARITY(val);
  if (arity!=3) {
    fd_seterr(fd_TypeError,"config_add_logfn",u8_strdup("log function"),val);
    return -1;}
  use_fd_logger(); fd_incref(val);
  u8_lock_mutex(&log_lock);
  FD_ADD_TO_CHOICE(framerd_logfns,val);
  framerd_logfns=fd_simplify_choice(framerd_logfns);
  u8_unlock_mutex(&log_lock);
  return 1;
}

static fdtype config_get_reqloglevel(fdtype var,void *data)
{
  if ((using_fd_logger)&&(req_loglevel>=0))
    return FD_INT(req_loglevel);
  else return FD_FALSE;
}

static int config_set_reqloglevel(fdtype var,fdtype val,void *data)
{
  if (FD_FALSEP(val)) {
    if (req_loglevel>=0) {req_loglevel=-1; return 1;}
    else return 0;}
  else if (FD_FIXNUMP(val)) {
    int level=FD_FIX2INT(val);
    if (level==req_loglevel) return 0;
    else if (level>=0) use_fd_logger();
    else {}
    req_loglevel=level;
    return 1;}
  else if (req_loglevel>=0) return 0;
  else {
    use_fd_logger();
    req_loglevel=5;
    return 1;}
}

static fdtype config_get_reqlogonly(fdtype var,void *data)
{
  if ((using_fd_logger)&&(req_logonly>=0))
    return FD_INT(req_logonly);
  else return FD_FALSE;
}

static int config_set_reqlogonly(fdtype var,fdtype val,void *data)
{
  if (FD_FALSEP(val)) {
    if (req_logonly>=0) {req_logonly=-1; return 1;}
    else return 0;}
  else if (FD_FIXNUMP(val)) {
    int level=FD_FIX2INT(val);
    if (level==req_logonly) return 0;
    else if (level>=0) use_fd_logger();
    else {}
    req_logonly=level;
    return 1;}
  else if (req_logonly>=0) return 0;
  else {
    use_fd_logger();
    req_logonly=5;
    return 1;}
}

/* Getting module locations */

#define LOCAL_MODULES 1
#define LOCAL_SAFE_MODULES 2
#define INSTALLED_MODULES 3
#define INSTALLED_SAFE_MODULES 4
#define SHARED_MODULES 5
#define SHARED_SAFE_MODULES 6
#define BUILTIN_MODULES 7
#define BUILTIN_SAFE_MODULES 8
#define UNPACKAGE_DIR 9

static fdtype config_get_module_loc(fdtype var,void *which_arg)
{
#if (SIZEOF_LONG_LONG == SIZEOF_VOID_P)
  long long which = (long long) which_arg;
#else
  int which = (int) which_arg;
#endif
  switch (which) {
  case LOCAL_MODULES:
    return fdtype_string(FD_LOCAL_MODULE_DIR);
  case LOCAL_SAFE_MODULES:
    return fdtype_string(FD_LOCAL_SAFE_MODULE_DIR);
  case INSTALLED_MODULES:
    return fdtype_string(FD_INSTALLED_MODULE_DIR);
  case INSTALLED_SAFE_MODULES:
    return fdtype_string(FD_INSTALLED_SAFE_MODULE_DIR);
  case SHARED_MODULES:
    return fdtype_string(FD_SHARED_MODULE_DIR);
  case SHARED_SAFE_MODULES:
    return fdtype_string(FD_SHARED_SAFE_MODULE_DIR);
  case BUILTIN_MODULES:
    return fdtype_string(FD_BUILTIN_MODULE_DIR);
  case BUILTIN_SAFE_MODULES:
    return fdtype_string(FD_BUILTIN_SAFE_MODULE_DIR);
  case UNPACKAGE_DIR:
    return fdtype_string(FD_UNPACKAGE_DIR);
  default:
    return fd_err("Bad call","config_get_module_loc",NULL,FD_VOID);}
}

/* Converting signals to exceptions */

struct sigaction sigaction_catch, sigaction_exit, sigaction_default;
static sigset_t sigcatch_set, sigexit_set, sigdefault_set;

static sigset_t default_sigmask;
sigset_t *fd_default_sigmask=&default_sigmask;

struct sigaction *fd_sigaction_catch=&sigaction_catch;
struct sigaction *fd_sigaction_exit=&sigaction_catch;
struct sigaction *fd_sigaction_default=&sigaction_catch;

static void siginfo_raise(int signum,siginfo_t *info,void *stuff)
{
  u8_contour c=u8_dynamic_contour;
  u8_condition ex=u8_signal_name(signum);
  if (!(c)) {
    u8_log(LOG_CRIT,ex,"Unexpected signal");
    exit(1);}
  else {
    u8_raise(ex,c->u8c_label,NULL);}
}

static void siginfo_exit(int signum,siginfo_t *info,void *stuff)
{
  u8_contour c=u8_dynamic_contour;
  u8_condition ex=u8_signal_name(signum);
  if (!(c)) {
    u8_log(LOG_CRIT,ex,"Unexpected signal");}
  else {
    u8_raise(ex,c->u8c_label,NULL);}
  exit(1);
}

/* Signal handling configs */

static fdtype sigmask2dtype(sigset_t *mask)
{
  fdtype result=FD_EMPTY_CHOICE;
  int sig=1; while (sig<32) {
    if (sigismember(mask,sig)) {
      FD_ADD_TO_CHOICE(result,fd_intern(u8_signal_name(sig)));}
    sig++;}
  return result;
}

static int arg2signum(fdtype arg)
{
  int sig=-1;
  if (FD_FIXNUMP(arg)) 
    sig=FD_FIX2INT(arg);
  else if (FD_SYMBOLP(arg))
    sig=u8_name2signal(FD_SYMBOL_NAME(arg));
  else if (FD_STRINGP(arg))
    sig=u8_name2signal(FD_STRDATA(arg));
  else sig=-1;
  if ((sig>1)&&(sig<32))
    return sig;
  else {
    fd_seterr(fd_TypeError,"arg2signum",NULL,arg);
    return -1;}
}

static fdtype sigconfig_getfn(fdtype var,void *data)
{
  sigset_t *mask=(sigset_t *)data;
  return sigmask2dtype(mask);
}

static int sigconfig_setfn(fdtype var,fdtype val,
                           sigset_t *mask,
                           struct sigaction *action,
                           u8_string caller)
{
  int sig=arg2signum(val);
  if (sig<0) return sig;
  if (sigismember(mask,sig))
    return 0;
  else {
    sigaction(sig,action,NULL);
    sigaddset(mask,sig);
    if (mask!=&sigcatch_set) sigdelset(&sigcatch_set,sig);
    if (mask!=&sigexit_set) sigdelset(&sigexit_set,sig);
    if (mask!=&sigdefault_set) sigdelset(&sigdefault_set,sig);
    return 1;}
}

static int sigconfig_catch_setfn(fdtype var,fdtype val,void *data)
{
  sigset_t *mask=(sigset_t *)data;
  return sigconfig_setfn(var,val,mask,&sigaction_catch,
                         "sigconfig_catch_setfn");
}

static int sigconfig_exit_setfn(fdtype var,fdtype val,void *data)
{
  sigset_t *mask=(sigset_t *)data;
  return sigconfig_setfn(var,val,mask,&sigaction_exit,
                         "sigconfig_exit_setfn");
}

static int sigconfig_default_setfn(fdtype var,fdtype val,void *data)
{
  sigset_t *mask=(sigset_t *)data;
  return sigconfig_setfn(var,val,mask,&sigaction_default,
                         "sigconfig_default_setfn");
}

/* Initialization */

static void setup_logging();

void fd_init_support_c()
{
  u8_register_source_file(_FILEINFO);

  u8_register_textdomain("FramerD");

  atexit(doexit_atexit);

  /* atexit(report_errors_atexit); */

#if FD_USE_TLS
  u8_new_threadkey(&threadtable_key,NULL);
  u8_new_threadkey(&reqinfo_key,NULL);
  u8_new_threadkey(&reqlog_key,NULL);
#endif
  configuration_table=fd_make_hashtable(NULL,16);

#if FD_THREADS_ENABLED
  fd_init_mutex(&config_lookup_lock);
  fd_init_mutex(&config_register_lock);
  fd_init_mutex(&atexit_handlers_lock);
  fd_init_mutex(&log_lock);
#endif

  fd_register_config_lookup(getenv_config_lookup,NULL);

  boot_config();

  setup_logging();

  fd_register_config
    ("APPID",_("application ID used in messages and SESSIONID"),
     config_getappid,config_setappid,NULL);

  fd_register_config
    ("ARGV",
     _("the vector of args (before parsing) to the application (no configs)"),
     fd_lconfig_get,NULL,&raw_argv);
  fd_register_config
    ("RAWARGS",
     _("the vector of args (before parsing) to the application (no configs)"),
     fd_lconfig_get,NULL,&raw_argv);
  fd_register_config
    ("CMDARGS",_("the vector of parsed args to the application (no configs)"),
     fd_lconfig_get,NULL,&lisp_argv);
  fd_register_config
    ("ARGS",_("the vector of parsed args to the application (no configs)"),
     fd_lconfig_get,NULL,&lisp_argv);
  fd_register_config
    ("STRINGARGS",
     _("the vector of args (before parsing) to the application (no configs)"),
     fd_lconfig_get,NULL,&string_argv);
  fd_register_config
    ("CONFIGARGS",_("config arguments passed to the application (unparsed)"),
     fd_lconfig_get,NULL,&config_argv);
  
  fd_register_config
    ("SESSIONID",_("unique session identifier"),
     config_getsessionid,config_setsessionid,NULL);
  fd_register_config
    ("PID",_("system process ID (read-only)"),
     config_getpid,NULL,NULL);
  fd_register_config
    ("PPID",_("parent's process ID (read-only)"),
     config_getppid,NULL,NULL);

  fd_register_config
    ("UTF8WARN",_("warn on bad UTF-8 sequences"),
     config_getutf8warn,config_setutf8warn,NULL);
  fd_register_config
    ("UTF8ERR",_("fail (error) on bad UTF-8 sequences"),
     config_getutf8err,config_setutf8err,NULL);
  fd_register_config
    ("RANDOMSEED",_("random seed used for stochastic operations"),
     config_getrandomseed,config_setrandomseed,NULL);
  fd_register_config
    ("MERGECHOICES",
     _("Threshold at which to use an external hashset for merging"),
     fd_intconfig_get,fd_intconfig_set,
     &fd_mergesort_threshold);
  fd_register_config
    ("TRACECONFIG",_("whether to trace configuration"),
     fd_boolconfig_get,fd_boolconfig_set,&trace_config);

  fd_register_config
    ("RUNUSER",_("Set the user ID for this process"),
     config_getuser,config_setuser,NULL);
  fd_register_config
    ("RUNGROUP",_("Set the group ID for this process"),
     config_getgroup,config_setgroup,NULL);


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

  fd_register_config
    ("MAXCHARS",_("Max number of chars to show in strings"),
     fd_intconfig_get,fd_intconfig_set,
     &fd_unparse_maxchars);
  fd_register_config
    ("MAXELTS",
     _("Max number of elements to show in vectors/lists/choices, etc"),
     fd_intconfig_get,fd_intconfig_set,
     &fd_unparse_maxelts);
  fd_register_config
    ("NUMVEC:SHOWMAX",_("Max number of elements to show in numeric vectors"),
     fd_intconfig_get,fd_intconfig_set,
     &fd_numvec_showmax);
  fd_register_config
    ("PACKETFMT",
     _("How to dump packets to ASCII (16=hex,64=base64,dflt=ascii-ish)"),
     fd_intconfig_get,fd_intconfig_set,
     &fd_packet_outfmt);

  fd_register_config("RUNBASE",_("Path prefix for program state files"),
                     config_getrunbase,config_setrunbase,NULL);

#if HAVE_SYS_RESOURCE_H
#ifdef RLIMIT_CPU
  fd_register_config("MAXCPU",_("Max CPU execution time limit"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXCPU);
#endif
#ifdef RLIMIT_RSS
  fd_register_config("MAXRSS",_("Max resident set (RSS) size"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXRSS);
#endif
#ifdef RLIMIT_CORE
  fd_register_config("MAXCORE",_("Max core dump size"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXCORE);
#endif
#ifdef RLIMIT_NPROC
  fd_register_config("MAXNPROC",_("Max number of subprocesses"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXNPROC);
#endif
#ifdef RLIMIT_NOFILE
  fd_register_config("MAXFILES",_("Max number of open file descriptors"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXFILES);
#endif
#ifdef RLIMIT_STACK
  fd_register_config("MAXSTACK",_("Max stack depth"),
                     fd_config_rlimit_get,fd_config_rlimit_set,
                     (void *)&MAXSTACK);
#endif

#endif

#if 0
  fd_register_config("GOOGLEPROFILE",
                     _("File to store profile output from Google perftools"),
                     get_google_profile,set_google_profile,
                     NULL);
#endif

  if (!(logdir)) logdir=u8_strdup(FD_LOG_DIR);
  fd_register_config
    ("LOGDIR",_("Root FramerD logging directories"),
     fd_sconfig_get,fd_sconfig_set,&logdir);

  if (!(sharedir)) sharedir=u8_strdup(FD_SHARE_DIR);
  fd_register_config
    ("SHAREDIR",_("Shared config/data directory for FramerD"),
     fd_sconfig_get,fd_sconfig_set,&sharedir);

  if (!(datadir)) datadir=u8_strdup(FD_DATA_DIR);
  fd_register_config
    ("DATADIR",_("Data directory for FramerD"),
     fd_sconfig_get,fd_sconfig_set,&datadir);

#if FD_FILECONFIG_ENABLED
  fd_register_config
    ("CONFIGDATA",_("Directory for looking up config entries"),
     fd_sconfig_get,fd_sconfig_set,&configdata_path);
  fd_register_config_lookup(file_config_lookup,NULL);
#endif
  fd_register_config
    ("SOURCES",_("Registered source files"),
     config_get_source_files,config_add_source_file,
     &u8_log_show_procinfo);

  fd_register_config("ATEXIT",_("Procedures to call on exit"),
                     config_atexit_get,config_atexit_set,NULL);

  fd_register_config
    ("REQ:LOGLEVEL",_("whether to use FramerD per-request logging"),
     config_get_reqloglevel,config_set_reqloglevel,NULL);
  fd_register_config
    ("REQ:LOGONLY",_("only use per-request logging (when available) for loglevels >= this"),
     config_get_reqlogonly,config_set_reqlogonly,NULL);
  fd_register_config
    ("LOGFN",_("the default log function"),
     config_get_logfn,config_set_logfn,NULL);
  fd_register_config
    ("LOGFNS",_("additional log functions"),
     config_get_logfns,config_add_logfn,NULL);

  fd_register_config("LOCAL_MODULES",_("value of LOCAL_MODULES"),
                     config_get_module_loc,NULL,(void *) LOCAL_MODULES);
  fd_register_config("LOCAL_SAFE_MODULES",_("value of LOCAL_SAFE_MODULES"),
                     config_get_module_loc,NULL,(void *) LOCAL_SAFE_MODULES);
  fd_register_config("INSTALLED_MODULES",_("value of INSTALLED_MODULES"),
                     config_get_module_loc,NULL,(void *) INSTALLED_MODULES);
  fd_register_config("INSTALLED_SAFE_MODULES",_("value of INSTALLED_SAFE_MODULES"),
                     config_get_module_loc,NULL,(void *) INSTALLED_SAFE_MODULES);
  fd_register_config("SHARED_MODULES",_("value of SHARED_MODULES"),
                     config_get_module_loc,NULL,(void *) SHARED_MODULES);
  fd_register_config("SHARED_SAFE_MODULES",_("value of SHARED_SAFE_MODULES"),
                     config_get_module_loc,NULL,(void *) SHARED_SAFE_MODULES);
  fd_register_config("BUILTIN_MODULES",_("value of BUILTIN_MODULES"),
                     config_get_module_loc,NULL,(void *) BUILTIN_MODULES);
  fd_register_config("BUILTIN_SAFE_MODULES",_("value of BUILTIN_SAFE_MODULES"),
                     config_get_module_loc,NULL,(void *) BUILTIN_SAFE_MODULES);

  fd_register_config("UNPACKAGE_DIR",_("value of UNPACKAGE_DIR"),
                     config_get_module_loc,NULL,(void *) UNPACKAGE_DIR);

  fd_register_config
    ("LOGFNS",_("additional log functions"),
     config_get_logfns,config_add_logfn,NULL);

}

void setup_logging()
{

  u8_logprefix=u8_strdup(";; ");
  u8_logsuffix=u8_strdup("\n");
  u8_logindent=u8_strdup(";;     ");

  fd_register_config
    ("LOGLEVEL",_("Required priority for messages to be displayed"),
     fd_intconfig_get,loglevelconfig_set,
     &u8_loglevel);
  fd_register_config
    ("STDOUTLEVEL",
     _("Required priority for messages to be displayed on stdout"),
     fd_intconfig_get,loglevelconfig_set,
     &u8_stdout_loglevel);
  fd_register_config
    ("STDERRLEVEL",
     _("Required priority for messages to be displayed on stderr"),
     fd_intconfig_get,loglevelconfig_set,
     &u8_stderr_loglevel);

  /* Default logindent for FramerD */
  if (!(u8_logindent)) u8_logindent=u8_strdup("       ");
  fd_register_config
    ("LOGINDENT",_("String for indenting multi-line log messages"),
     fd_sconfig_get,fd_sconfig_set,&u8_logindent);
  fd_register_config
    ("LOGPREFIX",_("Prefix for log messages (default '[')"),
     fd_sconfig_get,fd_sconfig_set,&u8_logprefix);
  fd_register_config
    ("LOGSUFFIX",_("Suffix for log messages (default ']\\n')"),
     fd_sconfig_get,fd_sconfig_set,&u8_logsuffix);
  
  fd_register_config
    ("SHOWDATE",_("Whether to show the date in log messages"),
     fd_boolconfig_get,fd_boolconfig_set,
     &u8_log_show_date);
  fd_register_config
    ("LOGPROCINFO",_("Whether to show PID/appid info in messages"),
     fd_boolconfig_get,fd_boolconfig_set,
     &u8_log_show_procinfo);
  fd_register_config
    ("LOGTHREADINFO",_("Whether to show thread id info in messages"),
     fd_boolconfig_get,fd_boolconfig_set,
     &u8_log_show_threadinfo);
  fd_register_config
    ("SHOWELAPSED",_("Whether to show elapsed time in messages"),
     fd_boolconfig_get,fd_boolconfig_set,
     &u8_log_show_elapsed);

  /* Setup sigaction handler */

  memset(&sigaction_catch,0,sizeof(struct sigaction));
  memset(&sigaction_exit,0,sizeof(struct sigaction));
  memset(&sigaction_default,0,sizeof(struct sigaction));

  sigemptyset(&sigcatch_set);
  sigemptyset(&sigexit_set);
  sigemptyset(&sigdefault_set);

  /* Setup sigaction for converting signals to u8_raise (longjmp or exit) */
  sigaction_catch.sa_sigaction=siginfo_raise;
  sigaction_catch.sa_flags=SA_SIGINFO;
  sigemptyset(&(sigaction_catch.sa_mask));

  /* Setup sigaction for default action */
  sigaction_exit.sa_handler=SIG_DFL;
  sigemptyset(&(sigaction_exit.sa_mask));

  /* Setup sigaction for exit action */
  sigaction_exit.sa_sigaction=siginfo_exit;
  sigaction_exit.sa_flags=SA_SIGINFO;
  sigemptyset(&(sigaction_exit.sa_mask));

  /* Default exit actions */

  sigaddset(&(sigaction_exit.sa_mask),SIGSEGV);
  sigaction(SIGSEGV,&(sigaction_exit),NULL);
  
  sigaddset(&(sigaction_exit.sa_mask),SIGILL);
  sigaction(SIGILL,&(sigaction_exit),NULL);

  sigaddset(&(sigaction_exit.sa_mask),SIGFPE);
  sigaction(SIGFPE,&(sigaction_exit),NULL);

#ifdef SIGBUS
  sigaddset(&(sigaction_exit.sa_mask),SIGBUS);
  sigaction(SIGBUS,&(sigaction_exit),NULL);
#endif

  /* The default sigmask is masking everything but synchronous
     signals */
  sigfillset(&default_sigmask);
  sigdelset(&default_sigmask,SIGSEGV);
  sigdelset(&default_sigmask,SIGILL);
  sigdelset(&default_sigmask,SIGFPE);
#ifdef SIGBUS
  sigaddset(&default_sigmask,SIGBUS);
#endif

  fd_register_config
    ("SIGCATCH",_("Errors to catch and return as errors"),
     sigconfig_getfn,sigconfig_catch_setfn,
     &sigcatch_set);
  fd_register_config
    ("SIGEXIT",_("Errors to trigger exists"),
     sigconfig_getfn,sigconfig_exit_setfn,
     &sigexit_set);
  fd_register_config
    ("SIGDEFAULT",_("Errors to trigger exists"),
     sigconfig_getfn,sigconfig_default_setfn,
     &sigexit_set);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
