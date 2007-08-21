/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/numbers.h"

#include <libu8/u8pathfns.h>

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

u8_condition SetRLimit=_("SetRLimit");

fd_exception fd_UnknownError=_("Unknown error condition");
fd_exception fd_ConfigError=_("Configuration error");
fd_exception fd_OutOfMemory=_("Memory apparently exhausted");
fd_exception fd_ExitException=_("Unhandled exception at exit");

/* Configuration handling */

struct FD_CONFIG_HANDLER *config_handlers=NULL;

static fdtype global_config;

#if FD_USE_TLS
static u8_tld_key thread_config_var;
#elif FD_THREADS_ENABLED
static fdtype __thread thread_config=(fdtype)0;
#else
static fdtype thread_config=(fdtype)0;
#endif

#if FD_USE_TLS
#define get_thread_config() ((fdtype)(u8_tld_get(thread_config_var)))
FD_EXPORT void fd_set_thread_config(fdtype table)
{
  fdtype current=get_thread_config();
  if (FD_EMPTY_CHOICEP(table)) {
    if (current) fd_decref(current);
    u8_tld_set(thread_config_var,(void *)NULL);}
  else {
    if (current) fd_decref(current);
    fd_incref(table);
    u8_tld_set(thread_config_var,(void *)table);}
}
#else
#define get_thread_config() (thread_config)
FD_EXPORT void fd_set_thread_config(fdtype table)
{
  if (thread_config) fd_decref(thread_config);
  thread_config=fd_incref(table);
}
#endif

#if FD_THREADS_ENABLED
static u8_mutex config_lookup_lock;
#endif

static struct FD_CONFIG_LOOKUPS *config_lookupfns=NULL;

static fdtype config_intern(u8_string start)
{
  fdtype symbol; U8_OUTPUT nameout; u8_byte buf[64], *scan=start;
  U8_INIT_OUTPUT_BUF(&nameout,64,buf);
  while (*scan) {
    int c=u8_sgetc(&scan);
    if (c == '/') u8_putc(&nameout,c);
    else if (u8_ispunct(c)) {}
    else if (u8_isupper(c)) u8_putc(&nameout,c);
    else u8_putc(&nameout,u8_toupper(c));}
  if (nameout.u8_streaminfo&U8_STREAM_OWNS_BUF) {
    fdtype symbol=
      fd_make_symbol(nameout.u8_outbuf,nameout.u8_outptr-nameout.u8_outbuf);
    u8_close((u8_stream)&nameout);
    return symbol;}
  else return fd_make_symbol
	 (nameout.u8_outbuf,nameout.u8_outptr-nameout.u8_outbuf);
}

FD_EXPORT
void fd_register_config_lookup(fdtype (*fn)(fdtype))
{
  struct FD_CONFIG_LOOKUPS *entry=
    u8_alloc(struct FD_CONFIG_LOOKUPS);
  fd_lock_mutex(&config_lookup_lock);
  entry->lookup=fn; entry->next=config_lookupfns;
  config_lookupfns=entry;
  fd_unlock_mutex(&config_lookup_lock);
}

static fdtype config_get(u8_string var)
{
  fdtype symbol=config_intern(var), probe=FD_VOID;
  fdtype table=get_thread_config();
  if (table) {
    probe=fd_get(table,symbol,FD_VOID);
    if (!(FD_VOIDP(probe))) return probe;
    else probe=fd_get(global_config,symbol,FD_VOID);}
  else probe=fd_get(global_config,symbol,FD_VOID);
  if (FD_VOIDP(probe)) {
    fdtype value=FD_VOID;
    struct FD_CONFIG_LOOKUPS *scan=config_lookupfns;
    while (scan) {
      value=scan->lookup(symbol);
      if (FD_VOIDP(value)) scan=scan->next; else break;}
    if (table) fd_store(table,symbol,value);
    else fd_store(global_config,symbol,value);
    return value;}
  else return probe;
}

static fdtype getenv_config_lookup(fdtype symbol)
{
  U8_OUTPUT out; 
  char *getenv_result, *u8result; fdtype result;
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
  fdtype table=get_thread_config();
  if (table) return fd_store(table,symbol,val);
  else return fd_store(global_config,symbol,val);
}

/* File-based configuration */

#if FD_FILECONFIG_ENABLED
static u8_string configdata_path=NULL;

static fdtype file_config_lookup(fdtype symbol)
{
  u8_string path=
    ((configdata_path) ? (configdata_path) : ((u8_string)FD_CONFIG_FILE_PATH));
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

#if FD_THREADS_ENABLED
static u8_mutex config_lock;
#endif

FD_EXPORT fdtype fd_config_get(u8_string var)
{
  fdtype symbol=config_intern(var);
  struct FD_CONFIG_HANDLER *scan=config_handlers;
  while (scan)
    if (FD_EQ(scan->var,symbol)) {
      fdtype val;
      fd_lock_mutex(&config_lock);
      val=scan->config_get_method(symbol,scan->data);
      fd_unlock_mutex(&config_lock);
      return val;}
    else scan=scan->next;
  return config_get(var);
}

FD_EXPORT int fd_config_set(u8_string var,fdtype val)
{
  fdtype symbol=config_intern(var); int retval;
  struct FD_CONFIG_HANDLER *scan=config_handlers;
  while (scan)
    if (FD_EQ(scan->var,symbol)) {
      fd_lock_mutex(&config_lock);
      scan->flags=scan->flags|FD_CONFIG_HANDLER_INVOKED;
      retval=scan->config_set_method(symbol,val,scan->data);
      fd_unlock_mutex(&config_lock);
      break;}
    else scan=scan->next;
  if (scan==NULL) return config_set(var,val);
  else if (retval<0) {
    u8_string errsum=fd_errstring(NULL);
    u8_warn(fd_ConfigError,"Config error %q=%q: %s",symbol,val,errsum);
    u8_free(errsum);}
  return retval;
}

FD_EXPORT int fd_config_default(u8_string var,fdtype val)
{
  fdtype symbol=config_intern(var); int retval;
  struct FD_CONFIG_HANDLER *scan=config_handlers;
  while (scan)
    if (FD_EQ(scan->var,symbol)) {
      fd_lock_mutex(&config_lock);
      if ((scan->flags)&(FD_CONFIG_HANDLER_INVOKED)) {
	fd_unlock_mutex(&config_lock); break;}
      scan->flags=scan->flags|FD_CONFIG_HANDLER_INVOKED;
      retval=scan->config_set_method(symbol,val,scan->data);
      fd_unlock_mutex(&config_lock);
      break;}
    else scan=scan->next;
  if (scan==NULL) return config_set(var,val);
  else if (retval<0) {
    u8_string errsum=fd_errstring(NULL);
    u8_warn(fd_ConfigError,"Config error %q=%q: %s",symbol,val,errsum);
    u8_free(errsum);}
  return retval;
}

FD_EXPORT void fd_config_lock(int lock)
{
  if (lock) fd_lock_mutex(&config_lock);
  else fd_unlock_mutex(&config_lock);
}

FD_EXPORT int fd_register_config
  (u8_string var,u8_string doc,
   fdtype (*getfn)(fdtype,void *),
   int (*setfn)(fdtype,fdtype,void *),
   void *data)
{
  fdtype symbol=config_intern(var), current; int retval=0;
  struct FD_CONFIG_HANDLER *scan=config_handlers;
  while (scan)
    if (FD_EQ(scan->var,symbol)) {
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
    scan=u8_alloc(struct FD_CONFIG_HANDLER);
    scan->var=symbol;
    if (doc) scan->doc=u8_strdup(doc); else scan->doc=NULL;
    scan->flags=0; 
    scan->config_get_method=getfn;
    scan->config_set_method=setfn;
    scan->data=data;
    scan->next=config_handlers;
    config_handlers=scan;}
  current=config_get(var);
  if (!(FD_VOIDP(current)))
    retval=setfn(symbol,current,data);
  fd_decref(current);
  return retval;
}

FD_EXPORT int fd_config_set_consed(u8_string var,fdtype val)
{
  int retval=fd_config_set(var,val);
  if (retval<0) return retval;
  fd_decref(val);
  return retval;
}

FD_EXPORT int fd_config_assignment(u8_string assignment)
{
  u8_byte *equals;
  if (equals=strchr(assignment,'=')) {
    u8_byte _namebuf[64], *namebuf; u8_byte *scan=assignment;
    int c, namelen=equals-assignment, retval;
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

FD_EXPORT int fd_argv_config(int argc,char **argv)
{
  int i=0, n=0; while (i<argc)
    if (strchr(argv[i],'=')) {
      char *carg=argv[i++];
      u8_string arg=u8_fromlibc(carg);
      int retval=fd_config_assignment(arg);
      if (retval<0) u8_clear_errors(0);
      else n++;}
    else i++;
  return n;
}

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
      if ((FD_PAIRP(entry)) &&
	  (FD_SYMBOLP(FD_CAR(entry))) &&
	  (FD_PAIRP(FD_CDR(entry)))) {
	if (fd_config_set(FD_SYMBOL_NAME(FD_CAR(entry)),(FD_CADR(entry)))<0) {
	  fd_seterr(fd_ConfigError,"fd_read_config",NULL,entry);
	  return -1;}
	fd_decref(entry); n++;}
      else if (FD_ABORTP(entry))
	return fd_interr(entry);
      else {
	fd_seterr(fd_ConfigError,"fd_read_config",NULL,entry);
	return -1;}}
    else if ((u8_isspace(c)) || (u8_isctrl(c)))  {}
    else {
      u8_ungetc(in,c);
      buf=u8_gets(in);
      if (fd_config_assignment(buf)<0) 
	return fd_reterr(fd_ConfigError,"fd_read_config",buf,FD_VOID);
      else n++;
      u8_free(buf);}
  return n;
}

/* Utility functions for configuration variables which effect
   dtype pointers. */

FD_EXPORT fdtype fd_lconfig_get(fdtype ignored,void *lispp)
{
  fdtype *val=(fdtype *)lispp;
  return fd_incref(*val);
}
FD_EXPORT int fd_lconfig_set(fdtype ignored,fdtype v,void *lispp)
{
  fdtype *val=(fdtype *)lispp;
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
  *val=fd_init_pair(NULL,fd_incref(v),*val);
  return 1;
}
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

FD_EXPORT fdtype fd_intconfig_get(fdtype ignored,void *vptr)
{
  int *ptr=vptr;
  return FD_INT2DTYPE(*ptr);
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

FD_EXPORT fdtype fd_boolconfig_get(fdtype ignored,void *vptr)
{
  int *ptr=vptr;
  if (*ptr) return FD_TRUE; else return FD_FALSE;
}
FD_EXPORT int fd_boolconfig_set(fdtype ignored,fdtype v,void *vptr)
{
  int *ptr=vptr;
  if (FD_FALSEP(v)) {
    *ptr=0; return 1;}
  else {*ptr=1; return 1;}
}

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
  else if (FD_PTR_TYPEP(v,fd_double_type)) {
    *ptr=FD_FLONUM(v);}
  else if (FD_FIXNUMP(v)) {
    int intval=FD_FIX2INT(v);
    double dblval=(double)intval;
    *ptr=dblval;}
  else return fd_reterr(fd_TypeError,"fd_dblconfig_set",
			FD_SYMBOL_NAME(var),v);
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
  else return FD_INT2DTYPE((long long)(rlim.rlim_cur));
}

FD_EXPORT int fd_config_rlimit_set(fdtype ignored,fdtype v,void *vptr)
{
  struct rlimit rlim;
  struct NAMED_RLIMIT *nrl=(struct NAMED_RLIMIT *)vptr;
  rlim_t setval=((FD_FALSEP(v)) ? (RLIM_INFINITY) : (fd_getint(v)));
  int retval=getrlimit(nrl->code,&rlim);
  if (retval<0) {
    u8_condition cond=u8_strerror(errno); errno=0;
    return fd_err(cond,"rlimit_get",u8_strdup(nrl->name),FD_VOID);}
  else if (setval>rlim.rlim_max) {
    /* Should be more informative */
    fd_seterr(_("RLIMIT too high"),"set_rlimit",u8_strdup(nrl->name),FD_VOID);
    return -1;}
  if (setval==rlim.rlim_cur)
    u8_warn(SetRLimit,"Setting for %s did not need to change",nrl->name);
  else if (setval==RLIM_INFINITY)
    u8_warn(SetRLimit,"Setting %s to unlimited from %d",nrl->name,rlim.rlim_cur);
  else u8_warn(SetRLimit,"Setting %s to %lld from %lld",nrl->name,(long long)setval,rlim.rlim_cur);
  rlim.rlim_cur=setval;
  retval=setrlimit(nrl->code,&rlim);
  if (retval<0) {
    u8_condition cond=u8_strerror(errno); errno=0;
    return fd_err(cond,"rlimit_set",u8_strdup(nrl->name),FD_VOID);}
  else return 1;
}

#endif

/* Managing error data */

#if FD_USE_TLS
static u8_tld_key errdata_key;
#elif FD_THREADS_ENABLED
static __thread struct FD_ERRDATA *errdata=NULL;
#else
static struct FD_ERRDATA *errdata=NULL;
#endif

#if FD_USE_TLS
static void set_errdata(struct FD_ERRDATA *ed)
{
  u8_tld_set(errdata_key,(void *)ed);
}
static struct FD_ERRDATA * get_errdata()
{
  return (struct FD_ERRDATA *) u8_tld_get(errdata_key);
}
#else
static void set_errdata(struct FD_ERRDATA *ed)
{
  errdata=ed;
}
static struct FD_ERRDATA * get_errdata()
{
  return errdata;
}
#endif

static FD_ERRDATA *adopt_u8errors()
{
  /* This copies the u8 errors to fd errors (clearing them in the process).
     It tries to maintain the chronological/accmulative structure of the U8
     errors, which means building a chain copying the U8 errors and pointing
     the end of that chain to the top of the current fd error stack. */
  struct FD_ERRDATA *head=NULL, *tail=NULL, *current=get_errdata();
  u8_condition cond; u8_context cxt; u8_string details;
  while (u8_poperr(&cond,&cxt,&details)) {
    struct FD_ERRDATA *newdata=u8_alloc(struct FD_ERRDATA);
    newdata->cond=cond; newdata->cxt=cxt; newdata->details=details;
    newdata->irritant=FD_VOID;
    if (head==NULL) head=newdata;
    if (tail) {
      newdata->next=tail->next; tail->next=newdata;}
    else {tail=newdata; newdata->next=NULL;}}
  if (tail) {
    tail->next=current;
    set_errdata(head);
    return head;}
  else return current;
}

FD_EXPORT void fd_seterr
  (u8_condition c,u8_context cxt,u8_string details,fdtype irritant)
{
  struct FD_ERRDATA *current=adopt_u8errors();
  struct FD_ERRDATA *newdata=u8_alloc(struct FD_ERRDATA);
  newdata->cond=c;
  newdata->cxt=cxt;
  newdata->details=details;
  if (FD_CHECK_PTR(irritant))
    newdata->irritant=irritant;
  else newdata->irritant=FD_BADPTR;
  newdata->next=current;
  set_errdata(newdata);
}

FD_EXPORT int fd_geterr
  (u8_condition *c,u8_context *cxt,u8_string *details,fdtype *irritant)
{
  struct FD_ERRDATA *current=adopt_u8errors();
  if (current) {
    if (c) *c=current->cond;
    if (cxt) *cxt=current->cxt;
    if (details)
      if (current->details) *details=u8_strdup(current->details);
      else *details=NULL;
    if (irritant) *irritant=fd_incref(current->irritant);
    return 1;}
  else return 0;
}
FD_EXPORT int fd_poperr
  (u8_condition *c,u8_context *cxt,u8_string *details,fdtype *irritant)
{
  struct FD_ERRDATA *current=adopt_u8errors();
  if (current) {
    if (c) *c=current->cond;
    if (cxt) *cxt=current->cxt;
    if (details)
      if (current->details) *details=current->details;
      else {u8_free(current->details); *details=NULL;}
    if (irritant) *irritant=fd_incref(current->irritant);
    else fd_decref(current->irritant);
    set_errdata(current->next);
    u8_free(current);
    return 1;}
  else return 0;
}

FD_EXPORT int fd_errout(U8_OUTPUT *out,struct FD_ERRDATA *ed)
{
  if (ed==NULL) ed=adopt_u8errors();
  if (ed==NULL) return 0;
  else if (!(FD_VOIDP(ed->irritant)))
    if ((ed->cxt) && (ed->details))
      return u8_printf(out,"%m (%s): %q (%m)",
		       ed->cond,ed->cxt,ed->irritant,ed->details);
    else if (ed->cxt)
      return u8_printf(out,"%m (%s): %q",ed->cond,ed->cxt,ed->irritant);
    else if (ed->details)
      return u8_printf(out,"%m: %q (%m)",ed->cond,ed->irritant,ed->details);
    else return u8_printf(out,"%m: %q",ed->cond,ed->irritant);
  else if ((ed->cxt) && (ed->details))
    return u8_printf(out,"%m (%s): %m",
		     ed->cond,ed->cxt,ed->details);
  else if (ed->cxt)
    return u8_printf(out,"%m (%s)",ed->cond,ed->cxt);
  else if (ed->details)
    return u8_printf(out,"%m: %m",ed->cond,ed->details);
  else return u8_printf(out,"%m",ed->cond);
}
FD_EXPORT u8_string fd_errstring(struct FD_ERRDATA *ed)
{
  if (ed==NULL) ed=adopt_u8errors();
  if (ed==NULL) return NULL;
  else {
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
    fd_errout(&out,ed);
    return out.u8_outbuf;}
}

FD_EXPORT void fd_raise_error()
{
  struct FD_ERRDATA *current=adopt_u8errors();
  if (FD_VOIDP(current->irritant))
    u8_raise(current->cond,current->cxt,current->details);
  else {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,512);
    u8_printf(&out,"%m: %q",current->details,current->irritant);
    u8_raise(current->cond,current->cxt,out.u8_outbuf);}
}

FD_EXPORT int fd_reterr
  (u8_condition c,u8_context cxt,u8_string details,fdtype irritant)
{
  fd_seterr(c,cxt,details,irritant);
  return -1;
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
  
FD_EXPORT int fd_interr(fdtype x)
{
  if (FD_ERRORP(x)) {
    struct FD_EXCEPTION_OBJECT *xo=
      FD_GET_CONS(x,fd_error_type,struct FD_EXCEPTION_OBJECT *);
    fd_seterr(xo->data.cond,xo->data.cxt,
	      ((xo->data.details) ? (u8_strdup(xo->data.details)) : (NULL)),
	      fd_incref(xo->data.irritant));
    fd_decref(x);
    return -1;}
  else if (FD_TROUBLEP(x)) {
    fd_exception ex=fd_retcode_to_exception(x);
    if (ex) {
      fd_seterr(ex,NULL,NULL,FD_VOID);
      return -1;}
    else return 0;}
  else {
    fd_decref(x);
    return 0;}
}

FD_EXPORT fdtype fd_erreify()
{
  u8_condition c; u8_context context; u8_string details; fdtype irritant;
  if (fd_poperr(&c,&context,&details,&irritant)) {
    fdtype retval=fd_err(c,context,details,irritant);
    if (details) u8_free(details); fd_decref(irritant);
    return retval;}
  else return fd_err(fd_UnknownError,NULL,NULL,FD_VOID);
}

int fd_report_errors_atexit=1;

static int clear_fderrors(struct FD_ERRDATA *ed,int report)
{
  struct U8_OUTPUT out; u8_byte buf[128]; int retval=0;
  if (ed==NULL) return 0;
  if (ed->next) retval=1+clear_fderrors(ed->next,report);
  if (report) {
    U8_INIT_OUTPUT_BUF(&out,128,buf);
    u8_printf(&out,"[%d]%m",retval,"Clearing error ");
    fd_errout(&out,ed);
    u8_message(out.u8_outbuf);
    if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);}
  if (ed->details) u8_free(ed->details);
  u8_free(ed);
  return retval;
}

FD_EXPORT
int fd_clear_errors(int report)
{
  struct FD_ERRDATA *ed=adopt_u8errors();
  int n_errors=clear_fderrors(ed,report);
  set_errdata(NULL);
  return n_errors;
}

static void report_errors_atexit()
{
  if (fd_report_errors_atexit==0) return;
  fd_clear_errors(1);
}

FD_EXPORT
int fd_errorp(void)
{
  struct FD_ERRDATA *ed=adopt_u8errors();
  if (ed==NULL) return 0; else return 1;
}

/* Thread Tables */

#if FD_USE_TLS
static u8_tld_key threadtable_key;
static fdtype get_threadtable()
{
  fdtype table=(fdtype)u8_tld_get(threadtable_key);
  if (table) return table;
  else {
    table=fd_init_slotmap(NULL,0,NULL);
    u8_tld_set(threadtable_key,(void*)table);
    return table;}
}
#elif FD_THREADS_ENABLED
static fdtype __thread thread_table=FD_VOID;
static fdtype get_threadtable()
{
  if (FD_TABLEP(thread_table)) return thread_table;
  else return (thread_table=fd_init_slotmap(NULL,0,NULL));
}
#else
static fdtype thread_table=FD_VOID;
static fdtype get_threadtable()
{
  if (FD_TABLEP(thread_table)) return thread_table;
  else return (thread_table=fd_init_slotmap(NULL,0,NULL));
}
#endif

FD_EXPORT fdtype fd_thread_get(fdtype var)
{
  return fd_get(get_threadtable(),var,FD_VOID);
}

FD_EXPORT fdtype fd_thread_set(fdtype var,fdtype val)
{
  return fd_store(get_threadtable(),var,val);
}

/* Recording source file information */

static struct FD_SOURCE_FILE_RECORD *source_files=NULL;

FD_EXPORT void fd_register_source_file(u8_string s)
{
  struct FD_SOURCE_FILE_RECORD *rec=
    u8_alloc(struct FD_SOURCE_FILE_RECORD);
  rec->filename=s; rec->next=source_files;
  source_files=rec;
}
FD_EXPORT void fd_for_source_files(void (*f)(u8_string s,void *),void *data)
{
  struct FD_SOURCE_FILE_RECORD *scan=source_files;
  while (scan) {
    f(scan->filename,data); scan=scan->next;}
}

/* Debugging support functions */

FD_EXPORT fd_ptr_type _fd_ptr_type(fdtype x)
{
  return FD_PTR_TYPE(x);
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

static fd_exception ClockFailed="call to clock() failed";
static fd_exception TimeFailed="call to time() failed";

static unsigned int randomseed=0x327b23c6;

static fdtype config_getrandomseed(fdtype var,void *data)
{
  if (randomseed<FD_MAX_FIXNUM) return FD_INT2DTYPE(randomseed);
  else return (fdtype)fd_ulong_to_bigint(randomseed);
}

static int config_setrandomseed(fdtype var,fdtype val,void *data)
{
  if (((FD_SYMBOLP(val)) && ((strcmp(FD_SYMBOL_NAME(val),"TIME"))==0)) ||
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

/* RUNBASE */

static u8_string runbase_config=NULL, runbase=0;

FD_EXPORT u8_string fd_runbase_filename(u8_string suffix)
{
  if (runbase==NULL)
    if (runbase_config==NULL) {
      u8_string wd=u8_getcwd(), appid=u8_appid();
      runbase=u8_mkpath(wd,appid);}
    else if (u8_directoryp(runbase_config))
      runbase=u8_mkpath(runbase_config,u8_appid());
    else runbase=u8_strdup(runbase_config);
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

/* Initialization */

static int boot_config()
{
  u8_string config_string=u8_getenv("FD_BOOT_CONFIG"), scan, end; int count=0;
  if (config_string==NULL) config_string=u8_strdup(FD_BOOT_CONFIG);
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

void fd_init_support_c()
{
  fd_register_source_file(versionid);

  u8_register_textdomain("eframerd");

  atexit(report_errors_atexit);

#if FD_USE_TLS
  u8_new_threadkey(&thread_config_var,NULL);
  u8_new_threadkey(&errdata_key,NULL);
  u8_new_threadkey(&threadtable_key,NULL);
#endif
  global_config=fd_make_hashtable(NULL,16);

#if FD_THREADS_ENABLED
  fd_init_mutex(&config_lookup_lock);
  fd_init_mutex(&config_lock);
#endif

  fd_register_config_lookup(getenv_config_lookup);

  boot_config();

  fd_register_config("APPID",_("application ID used in messages and SESSIONID"),
		     config_getappid,config_setappid,NULL);
  fd_register_config("SESSIONID",_("unique session identifier"),
		     config_getsessionid,config_setsessionid,NULL);
  fd_register_config("UTF8WARN",_("warn on bad UTF-8 sequences"),
		     config_getutf8warn,config_setutf8warn,NULL);
  fd_register_config("RANDOMSEED",_("random seed used for stochastic operations"),
		     config_getrandomseed,config_setrandomseed,NULL);
  fd_register_config("MERGECHOICES",_("Threshold at which to use an external hashset for merging"),
		     fd_intconfig_get,fd_intconfig_set,
		     &fd_mergesort_threshold);
  fd_register_config("SHOWPROCINFO",_("Whether to show PID/appid info in messages"),
		     fd_boolconfig_get,fd_boolconfig_set,
		     &u8_show_procinfo);
  fd_register_config("SHOWELAPSED",_("Whether to show elapsed time in messages"),
		     fd_boolconfig_get,fd_boolconfig_set,
		     &u8_show_elapsed);
  fd_register_config("DISPLAYMAXCHARS",_("Max number of chars to show in strings"),
		     fd_intconfig_get,fd_intconfig_set,
		     &fd_unparse_maxchars);
  fd_register_config("DISPLAYMAXELTS",_("Max number of elements to show in vectors/lists/choices, etc"),
		     fd_intconfig_get,fd_intconfig_set,
		     &fd_unparse_maxelts);

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
  fd_register_config("GOOGLEPROFILE",_("File to store profile output from Google perftools"),
		     get_google_profile,set_google_profile,
		     NULL);
#endif

#if FD_FILECONFIG_ENABLED
  fd_register_config
    ("CONFIGDATA",_("Directory for looking up config entries"),
     fd_sconfig_get,fd_sconfig_set,&configdata_path);
  fd_register_config_lookup(file_config_lookup);
#endif
}


