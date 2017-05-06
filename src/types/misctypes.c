/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/cons.h"

/* UUID Types */

static fdtype uuid_symbol;

FD_EXPORT fdtype fd_cons_uuid
   (struct FD_UUID *ptr,
    struct U8_XTIME *xtime,long long nodeid,short clockid)
{
  if (ptr == NULL) ptr = u8_alloc(struct FD_UUID);
  FD_INIT_CONS(ptr,fd_uuid_type);
  u8_consuuid(xtime,nodeid,clockid,(u8_uuid)&(ptr->fd_uuid16));
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_fresh_uuid(struct FD_UUID *ptr)
{
  if (ptr == NULL) ptr = u8_alloc(struct FD_UUID);
  FD_INIT_CONS(ptr,fd_uuid_type);
  u8_getuuid((u8_uuid)&(ptr->fd_uuid16));
  return FDTYPE_CONS(ptr);
}

static fdtype copy_uuid(fdtype x,int deep)
{
  struct FD_UUID *uuid = fd_consptr(struct FD_UUID *,x,fd_uuid_type);
  struct FD_UUID *nuuid = u8_alloc(struct FD_UUID);
  FD_INIT_CONS(nuuid,fd_uuid_type);
  memcpy(nuuid->fd_uuid16,uuid->fd_uuid16,16);
  return FDTYPE_CONS(nuuid);
}

#define MU U8_MAYBE_UNUSED

static int uuid_dtype(struct FD_OUTBUF *out,fdtype x)
{
  int size = 0;
  struct FD_UUID *uuid = fd_consptr(struct FD_UUID *,x,fd_uuid_type);
  fd_write_byte(out,dt_compound);
  size = size+1+fd_write_dtype(out,uuid_symbol);
  fd_write_byte(out,dt_packet); fd_write_4bytes(out,16); size = size+5;
  fd_write_bytes(out,uuid->fd_uuid16,16); size = size+16;
  return size;
}

static fdtype uuid_dump(fdtype x,fd_compound_typeinfo MU e)
{
  struct FD_UUID *uuid = fd_consptr(struct FD_UUID *,x,fd_uuid_type);
  return fd_make_packet(NULL,16,uuid->fd_uuid16);
}

static fdtype uuid_restore(fdtype MU tag,fdtype x,fd_compound_typeinfo MU e)
{
  if (FD_PACKETP(x)) {
    struct FD_STRING *p = fd_consptr(struct FD_STRING *,x,fd_packet_type);
    if (p->fd_bytelen==16) {
      struct FD_UUID *uuid = u8_alloc(struct FD_UUID);
      FD_INIT_CONS(uuid,fd_uuid_type);
      memcpy(uuid->fd_uuid16,p->fd_bytes,16);
      return FDTYPE_CONS(uuid);}
    else return fd_err("Bad UUID packet","uuid_restore",
                       "UUID packet has wrong length",
                       x);}
  else return fd_err("Bad UUID rep","uuid_restore",
                     "UUID serialization isn't a packet",
                     x);
}


/* Exceptions */

FD_EXPORT fdtype fd_init_exception
   (struct FD_EXCEPTION_OBJECT *exo,u8_exception ex)
{
  if (exo == NULL) exo = u8_alloc(struct FD_EXCEPTION_OBJECT);
  FD_INIT_CONS(exo,fd_error_type); exo->fdex_u8ex = ex;
  return FDTYPE_CONS(exo);
}

FD_EXPORT fdtype fd_make_exception
  (fd_exception c,u8_context cxt,u8_string details,fdtype content)
{
  struct FD_EXCEPTION_OBJECT *exo = u8_alloc(struct FD_EXCEPTION_OBJECT);
  u8_exception ex; void *xdata; u8_exception_xdata_freefn freefn;
  if (FD_VOIDP(content)) {
    xdata = NULL; freefn = NULL;}
  else {
    xdata = (void *) content;
    freefn = fd_free_exception_xdata;}
  ex = u8_make_exception(c,cxt,details,xdata,freefn);
  FD_INIT_CONS(exo,fd_error_type); exo->fdex_u8ex = ex;
  return FDTYPE_CONS(exo);
}

static int dtype_exception(struct FD_OUTBUF *out,fdtype x)
{
  struct FD_EXCEPTION_OBJECT *exo = (struct FD_EXCEPTION_OBJECT *)x;
  if (exo->fdex_u8ex == NULL) {
    u8_log(LOG_CRIT,NULL,"Trying to serialize expired exception ");
    fd_write_byte(out,dt_void);
    return 1;}
  else {
    u8_exception ex = exo->fdex_u8ex;
    fdtype irritant = fd_exception_xdata(ex);
    int veclen = ((FD_VOIDP(irritant)) ? (3) : (4));
    fdtype vector = fd_init_vector(NULL,veclen,NULL);
    int n_bytes;
    FD_VECTOR_SET(vector,0,fd_intern((u8_string)(ex->u8x_cond)));
    if (ex->u8x_context) {
      FD_VECTOR_SET(vector,1,fd_intern((u8_string)(ex->u8x_context)));}
    else {FD_VECTOR_SET(vector,1,FD_FALSE);}
    if (ex->u8x_details) {
      FD_VECTOR_SET(vector,2,fdtype_string(ex->u8x_details));}
    else {FD_VECTOR_SET(vector,2,FD_FALSE);}
    if (!(FD_VOIDP(irritant)))
      FD_VECTOR_SET(vector,3,fd_incref(irritant));
    fd_write_byte(out,dt_exception);
    n_bytes = 1+fd_write_dtype(out,vector);
    fd_decref(vector);
    return n_bytes;}
}

static u8_exception copy_exception_helper(u8_exception ex,int flags)
{
  u8_exception newex; u8_string details = NULL; fdtype irritant;
  if (ex == NULL) return ex;
  if (ex->u8x_details) details = u8_strdup(ex->u8x_details);
  irritant = fd_exception_xdata(ex);
  if (FD_VOIDP(irritant))
    newex = u8_make_exception
      (ex->u8x_cond,ex->u8x_context,details,NULL,NULL);
  else if (flags)
    newex = u8_make_exception
      (ex->u8x_cond,ex->u8x_context,details,
       (void *)fd_copier(irritant,flags),fd_free_exception_xdata);
  else newex = u8_make_exception
         (ex->u8x_cond,ex->u8x_context,details,
          (void *)fd_incref(irritant),fd_free_exception_xdata);
  newex->u8x_prev = copy_exception_helper(ex->u8x_prev,flags);
  return newex;
}

static fdtype copy_exception(fdtype x,int deep)
{
  struct FD_EXCEPTION_OBJECT *xo=
    fd_consptr(struct FD_EXCEPTION_OBJECT *,x,fd_error_type);
  return fd_init_exception(NULL,copy_exception_helper(xo->fdex_u8ex,deep));
}


/* Timestamps */

static fdtype timestamp_symbol, timestamp0_symbol;

FD_EXPORT
/* fd_make_timestamp:
    Arguments: a pointer to a U8_XTIME struct and a memory pool
    Returns: a dtype pointer to a timestamp
 */
fdtype fd_make_timestamp(struct U8_XTIME *tm)
{
  struct FD_TIMESTAMP *tstamp = u8_alloc(struct FD_TIMESTAMP);
  memset(tstamp,0,sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(tstamp,fd_timestamp_type);
  if (tm)
    memcpy(&(tstamp->ts_u8xtime),tm,sizeof(struct U8_XTIME));
  else u8_now(&(tstamp->ts_u8xtime));
  return FDTYPE_CONS(tstamp);
}

FD_EXPORT
/* fd_time2timestamp
    Arguments: a pointer to a U8_XTIME struct and a memory pool
    Returns: a dtype pointer to a timestamp
 */
fdtype fd_time2timestamp(time_t moment)
{
  struct U8_XTIME xt;
  u8_init_xtime(&xt,moment,u8_second,0,0,0);
  return fd_make_timestamp(&xt);
}

static int reversible_time = 1;

static int unparse_timestamp(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_TIMESTAMP *tm=
    fd_consptr(struct FD_TIMESTAMP *,x,fd_timestamp_type);
  if (reversible_time) {
    u8_puts(out,"#T");
    u8_xtime_to_iso8601(out,&(tm->ts_u8xtime));
    return 1;}
  else {
    char xbuf[32];
    u8_puts(out,"#<TIMESTAMP 0x");
    u8_puts(out,u8_uitoa16((unsigned long long)x,xbuf));
    u8_puts(out," \"");
    u8_xtime_to_iso8601(out,&(tm->ts_u8xtime));
    u8_puts(out,"\">");
    return 1;}
}

static fdtype timestamp_parsefn(int n,fdtype *args,fd_compound_typeinfo e)
{
  struct FD_TIMESTAMP *tm = u8_alloc(struct FD_TIMESTAMP);
  u8_string timestring;
  memset(tm,0,sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(tm,fd_timestamp_type);
  if ((n==2) && (FD_STRINGP(args[1])))
    timestring = FD_STRDATA(args[1]);
  else if ((n==3) && (FD_STRINGP(args[2])))
    timestring = FD_STRDATA(args[2]);
  else return fd_err(fd_CantParseRecord,"TIMESTAMP",NULL,FD_VOID);
  u8_iso8601_to_xtime(timestring,&(tm->ts_u8xtime));
  return FDTYPE_CONS(tm);
}

static fdtype copy_timestamp(fdtype x,int deep)
{
  struct FD_TIMESTAMP *tm=
    fd_consptr(struct FD_TIMESTAMP *,x,fd_timestamp_type);
  struct FD_TIMESTAMP *newtm = u8_alloc(struct FD_TIMESTAMP);
  memset(newtm,0,sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(newtm,fd_timestamp_type);
  memcpy(&(newtm->ts_u8xtime),&(tm->ts_u8xtime),sizeof(struct U8_XTIME));
  return FDTYPE_CONS(newtm);
}

static int dtype_timestamp(struct FD_OUTBUF *out,fdtype x)
{
  struct FD_TIMESTAMP *xtm=
    fd_consptr(struct FD_TIMESTAMP *,x,fd_timestamp_type);
  int size = 1;
  fd_write_byte(out,dt_compound);
  size = size+fd_write_dtype(out,timestamp_symbol);
  if ((xtm->ts_u8xtime.u8_prec == u8_second) && (xtm->ts_u8xtime.u8_tzoff==0)) {
    fdtype xval = FD_INT(xtm->ts_u8xtime.u8_tick);
    size = size+fd_write_dtype(out,xval);}
  else {
    fdtype vec = fd_init_vector(NULL,4,NULL);
    int tzoff = xtm->ts_u8xtime.u8_tzoff;
    FD_VECTOR_SET(vec,0,FD_INT(xtm->ts_u8xtime.u8_tick));
    FD_VECTOR_SET(vec,1,FD_INT(xtm->ts_u8xtime.u8_nsecs));
    FD_VECTOR_SET(vec,2,FD_INT((int)xtm->ts_u8xtime.u8_prec));
    FD_VECTOR_SET(vec,3,FD_INT(tzoff));
    size = size+fd_write_dtype(out,vec);
    fd_decref(vec);}
  return size;
}

static fdtype timestamp_restore(fdtype tag,fdtype x,fd_compound_typeinfo e)
{
  if (FD_FIXNUMP(x)) {
    struct FD_TIMESTAMP *tm = u8_alloc(struct FD_TIMESTAMP);
    memset(tm,0,sizeof(struct FD_TIMESTAMP));
    FD_INIT_CONS(tm,fd_timestamp_type);
    u8_init_xtime(&(tm->ts_u8xtime),FD_FIX2INT(x),u8_second,0,0,0);
    return FDTYPE_CONS(tm);}
  else if (FD_BIGINTP(x)) {
    struct FD_TIMESTAMP *tm = u8_alloc(struct FD_TIMESTAMP);
    time_t tval = (time_t)(fd_bigint_to_long((fd_bigint)x));
    memset(tm,0,sizeof(struct FD_TIMESTAMP));
    FD_INIT_CONS(tm,fd_timestamp_type);
    u8_init_xtime(&(tm->ts_u8xtime),tval,u8_second,0,0,0);
    return FDTYPE_CONS(tm);}
  else if (FD_VECTORP(x)) {
    struct FD_TIMESTAMP *tm = u8_alloc(struct FD_TIMESTAMP);
    int secs = fd_getint(FD_VECTOR_REF(x,0));
    int nsecs = fd_getint(FD_VECTOR_REF(x,1));
    int iprec = fd_getint(FD_VECTOR_REF(x,2));
    int tzoff = fd_getint(FD_VECTOR_REF(x,3));
    memset(tm,0,sizeof(struct FD_TIMESTAMP));
    FD_INIT_CONS(tm,fd_timestamp_type);
    u8_init_xtime(&(tm->ts_u8xtime),secs,iprec,nsecs,tzoff,0);
    return FDTYPE_CONS(tm);}
  else return fd_err(fd_DTypeError,"bad timestamp compound",NULL,x);
}


/* Regexes */

fd_exception fd_RegexError=_("Regular expression error");

static int default_regex_flags = REG_EXTENDED|REG_NEWLINE;

FD_EXPORT fdtype fd_make_regex(u8_string src,int flags)
{
  struct FD_REGEX *ptr = u8_alloc(struct FD_REGEX); int retval;
  FD_INIT_FRESH_CONS(ptr,fd_regex_type);
  if (flags<0) flags = default_regex_flags;
  src = u8_strdup(src);
  retval = regcomp(&(ptr->fd_rxcompiled),src,flags);
  if (retval) {
    u8_byte buf[512];
    regerror(retval,&(ptr->fd_rxcompiled),buf,512);
    u8_free(ptr);
    return fd_err(fd_RegexError,"fd_make_regex",u8_strdup(buf),
                  fd_init_string(NULL,-1,src));}
  else {
    ptr->fd_rxflags = flags; ptr->fd_rxsrc = src;
    u8_init_mutex(&(ptr->fdrx_lock)); ptr->fd_rxactive = 1;
    return FDTYPE_CONS(ptr);}
}


/* Raw pointers */

FD_EXPORT fdtype fd_wrap_pointer(void *ptrval,
                                 fd_raw_recyclefn recycler,
                                 fdtype typespec,
                                 u8_string idstring)
{
  struct FD_RAWPTR *rawptr = u8_alloc(struct FD_RAWPTR);
  FD_INIT_CONS(rawptr,fd_rawptr_type);
  rawptr->ptrval = ptrval;
  rawptr->recycler = recycler;
  rawptr->raw_typespec = typespec; fd_incref(typespec);
  if (FD_SYMBOLP(typespec))
    rawptr->typestring = FD_SYMBOL_NAME(typespec);
  else if (FD_STRINGP(typespec))
    rawptr->typestring = FD_STRDATA(typespec);
  else rawptr->typestring = NULL;
  rawptr->idstring = idstring;
  return (fdtype) rawptr;
}


void fd_init_misctypes_c()
{
  fd_copiers[fd_error_type]=copy_exception;
  if (fd_dtype_writers[fd_error_type]==NULL)
    fd_dtype_writers[fd_error_type]=dtype_exception;

  fd_dtype_writers[fd_uuid_type]=uuid_dtype;
  fd_copiers[fd_uuid_type]=copy_uuid;

  fd_unparsers[fd_timestamp_type]=unparse_timestamp;

  uuid_symbol = fd_intern("UUID");
  {
    struct FD_COMPOUND_TYPEINFO *e = fd_register_compound(uuid_symbol,NULL,NULL);
    e->fd_compound_dumpfn = uuid_dump;
    e->fd_compound_restorefn = uuid_restore;}

  timestamp_symbol = fd_intern("TIMESTAMP");
  timestamp0_symbol = fd_intern("TIMESTAMP0");
  {
    struct FD_COMPOUND_TYPEINFO *e=
      fd_register_compound(timestamp_symbol,NULL,NULL);
    e->fd_compound_parser = timestamp_parsefn;
    e->fd_compound_dumpfn = NULL;
    e->fd_compound_restorefn = timestamp_restore;}
  {
    struct FD_COMPOUND_TYPEINFO *e=
      fd_register_compound(timestamp0_symbol,NULL,NULL);
    e->fd_compound_parser = timestamp_parsefn;
    e->fd_compound_dumpfn = NULL;
    e->fd_compound_restorefn = timestamp_restore;}
  fd_dtype_writers[fd_timestamp_type]=dtype_timestamp;

  fd_copiers[fd_timestamp_type]=copy_timestamp;


  u8_register_source_file(_FILEINFO);
}
