/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2018 beingmeta, inc.
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

static lispval uuid_symbol;

FD_EXPORT lispval fd_cons_uuid
   (struct FD_UUID *ptr,
    struct U8_XTIME *xtime,long long nodeid,short clockid)
{
  if (ptr == NULL) ptr = u8_alloc(struct FD_UUID);
  FD_INIT_CONS(ptr,fd_uuid_type);
  u8_consuuid(xtime,nodeid,clockid,(u8_uuid)&(ptr->uuid16));
  return LISP_CONS(ptr);
}

FD_EXPORT lispval fd_fresh_uuid(struct FD_UUID *ptr)
{
  if (ptr == NULL) ptr = u8_alloc(struct FD_UUID);
  FD_INIT_CONS(ptr,fd_uuid_type);
  u8_getuuid((u8_uuid)&(ptr->uuid16));
  return LISP_CONS(ptr);
}

static lispval copy_uuid(lispval x,int deep)
{
  struct FD_UUID *uuid = fd_consptr(struct FD_UUID *,x,fd_uuid_type);
  struct FD_UUID *nuuid = u8_alloc(struct FD_UUID);
  FD_INIT_CONS(nuuid,fd_uuid_type);
  memcpy(nuuid->uuid16,uuid->uuid16,16);
  return LISP_CONS(nuuid);
}

#define MU U8_MAYBE_UNUSED

static ssize_t uuid_dtype(struct FD_OUTBUF *out,lispval x)
{
  ssize_t size = 0;
  struct FD_UUID *uuid = fd_consptr(struct FD_UUID *,x,fd_uuid_type);
  fd_write_byte(out,dt_compound);
  size = size+1+fd_write_dtype(out,uuid_symbol);
  fd_write_byte(out,dt_packet); fd_write_4bytes(out,16); size = size+5;
  fd_write_bytes(out,uuid->uuid16,16); size = size+16;
  return size;
}

static int hash_uuid(lispval x,unsigned int (*fn)(lispval))
{
  struct FD_UUID *uuid = fd_consptr(struct FD_UUID *,x,fd_uuid_type);
  return fd_hash_bytes(uuid->uuid16,16);
}

static lispval uuid_dump(lispval x,fd_compound_typeinfo MU e)
{
  struct FD_UUID *uuid = fd_consptr(struct FD_UUID *,x,fd_uuid_type);
  return fd_make_packet(NULL,16,uuid->uuid16);
}

static lispval uuid_restore(lispval MU tag,lispval x,fd_compound_typeinfo MU e)
{
  if (PACKETP(x)) {
    struct FD_STRING *p = fd_consptr(struct FD_STRING *,x,fd_packet_type);
    if (p->str_bytelen==16) {
      struct FD_UUID *uuid = u8_alloc(struct FD_UUID);
      FD_INIT_CONS(uuid,fd_uuid_type);
      memcpy(uuid->uuid16,p->str_bytes,16);
      return LISP_CONS(uuid);}
    else return fd_err("Bad UUID packet","uuid_restore",
                       "UUID packet has wrong length",
                       x);}
  else return fd_err("Bad UUID rep","uuid_restore",
                     "UUID serialization isn't a packet",
                     x);
}


/* Timestamps */

static lispval timestamp_symbol, timestamp0_symbol;

FD_EXPORT
/* fd_make_timestamp:
    Arguments: a pointer to a U8_XTIME struct and a memory pool
    Returns: a dtype pointer to a timestamp
 */
lispval fd_make_timestamp(struct U8_XTIME *tm)
{
  struct FD_TIMESTAMP *tstamp = u8_alloc(struct FD_TIMESTAMP);
  memset(tstamp,0,sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(tstamp,fd_timestamp_type);
  if (tm)
    memcpy(&(tstamp->u8xtimeval),tm,sizeof(struct U8_XTIME));
  else u8_now(&(tstamp->u8xtimeval));
  return LISP_CONS(tstamp);
}

FD_EXPORT
/* fd_time2timestamp
    Arguments: a pointer to a U8_XTIME struct and a memory pool
    Returns: a dtype pointer to a timestamp
 */
lispval fd_time2timestamp(time_t moment)
{
  struct U8_XTIME xt;
  u8_init_xtime(&xt,moment,u8_second,0,0,0);
  return fd_make_timestamp(&xt);
}

static int reversible_time = 1;

static int unparse_timestamp(struct U8_OUTPUT *out,lispval x)
{
  struct FD_TIMESTAMP *tm=
    fd_consptr(struct FD_TIMESTAMP *,x,fd_timestamp_type);
  if (reversible_time) {
    u8_puts(out,"#T");
    u8_xtime_to_iso8601(out,&(tm->u8xtimeval));
    return 1;}
  else {
    char xbuf[32];
    u8_puts(out,"#<TIMESTAMP 0x");
    u8_puts(out,u8_uitoa16((unsigned long long)x,xbuf));
    u8_puts(out," \"");
    u8_xtime_to_iso8601(out,&(tm->u8xtimeval));
    u8_puts(out,"\">");
    return 1;}
}

static lispval timestamp_parsefn(int n,lispval *args,fd_compound_typeinfo e)
{
  struct FD_TIMESTAMP *tm = u8_alloc(struct FD_TIMESTAMP);
  u8_string timestring;
  memset(tm,0,sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(tm,fd_timestamp_type);
  if ((n==2) && (STRINGP(args[1])))
    timestring = CSTRING(args[1]);
  else if ((n==3) && (STRINGP(args[2])))
    timestring = CSTRING(args[2]);
  else return fd_err(fd_CantParseRecord,"TIMESTAMP",NULL,VOID);
  u8_iso8601_to_xtime(timestring,&(tm->u8xtimeval));
  return LISP_CONS(tm);
}

static lispval copy_timestamp(lispval x,int deep)
{
  struct FD_TIMESTAMP *tm=
    fd_consptr(struct FD_TIMESTAMP *,x,fd_timestamp_type);
  struct FD_TIMESTAMP *newtm = u8_alloc(struct FD_TIMESTAMP);
  memset(newtm,0,sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(newtm,fd_timestamp_type);
  memcpy(&(newtm->u8xtimeval),&(tm->u8xtimeval),sizeof(struct U8_XTIME));
  return LISP_CONS(newtm);
}

static ssize_t timestamp_dtype(struct FD_OUTBUF *out,lispval x)
{
  struct FD_TIMESTAMP *xtm=
    fd_consptr(struct FD_TIMESTAMP *,x,fd_timestamp_type);
  ssize_t size = 1;
  fd_write_byte(out,dt_compound);
  size = size+fd_write_dtype(out,timestamp_symbol);
  if ((xtm->u8xtimeval.u8_prec == u8_second) && (xtm->u8xtimeval.u8_tzoff==0)) {
    lispval xval = FD_INT(xtm->u8xtimeval.u8_tick);
    size = size+fd_write_dtype(out,xval);}
  else {
    lispval vec = fd_empty_vector(4);
    int tzoff = xtm->u8xtimeval.u8_tzoff;
    FD_VECTOR_SET(vec,0,FD_INT(xtm->u8xtimeval.u8_tick));
    FD_VECTOR_SET(vec,1,FD_INT(xtm->u8xtimeval.u8_nsecs));
    FD_VECTOR_SET(vec,2,FD_INT((int)xtm->u8xtimeval.u8_prec));
    FD_VECTOR_SET(vec,3,FD_INT(tzoff));
    size = size+fd_write_dtype(out,vec);
    fd_decref(vec);}
  return size;
}

static lispval timestamp_restore(lispval tag,lispval x,fd_compound_typeinfo e)
{
  if (FIXNUMP(x)) {
    struct FD_TIMESTAMP *tm = u8_alloc(struct FD_TIMESTAMP);
    memset(tm,0,sizeof(struct FD_TIMESTAMP));
    FD_INIT_CONS(tm,fd_timestamp_type);
    u8_init_xtime(&(tm->u8xtimeval),FIX2INT(x),u8_second,0,0,0);
    return LISP_CONS(tm);}
  else if (FD_BIGINTP(x)) {
    struct FD_TIMESTAMP *tm = u8_alloc(struct FD_TIMESTAMP);
    time_t tval = (time_t)(fd_bigint_to_long((fd_bigint)x));
    memset(tm,0,sizeof(struct FD_TIMESTAMP));
    FD_INIT_CONS(tm,fd_timestamp_type);
    u8_init_xtime(&(tm->u8xtimeval),tval,u8_second,0,0,0);
    return LISP_CONS(tm);}
  else if (VECTORP(x)) {
    struct FD_TIMESTAMP *tm = u8_alloc(struct FD_TIMESTAMP);
    int secs = fd_getint(VEC_REF(x,0));
    int nsecs = fd_getint(VEC_REF(x,1));
    int iprec = fd_getint(VEC_REF(x,2));
    int tzoff = fd_getint(VEC_REF(x,3));
    memset(tm,0,sizeof(struct FD_TIMESTAMP));
    FD_INIT_CONS(tm,fd_timestamp_type);
    u8_init_xtime(&(tm->u8xtimeval),secs,iprec,nsecs,tzoff,0);
    return LISP_CONS(tm);}
  else return fd_err(fd_DTypeError,"bad timestamp compound",NULL,x);
}


/* Regexes */

u8_condition fd_RegexError=_("Regular expression error");

static int default_regex_flags = REG_EXTENDED|REG_NEWLINE;

FD_EXPORT lispval fd_make_regex(u8_string src,int flags)
{
  struct FD_REGEX *ptr = u8_alloc(struct FD_REGEX); int retval;
  FD_INIT_FRESH_CONS(ptr,fd_regex_type);
  if (flags<0) flags = default_regex_flags;
  src = u8_strdup(src);
  retval = regcomp(&(ptr->rxcompiled),src,flags);
  if (retval) {
    u8_byte buf[512];
    regerror(retval,&(ptr->rxcompiled),buf,512);
    u8_free(ptr);
    return fd_err(fd_RegexError,"fd_make_regex",u8_strdup(buf),
                  fd_init_string(NULL,-1,src));}
  else {
    ptr->rxflags = flags; ptr->rxsrc = src;
    u8_init_mutex(&(ptr->rx_lock)); ptr->rxactive = 1;
    return LISP_CONS(ptr);}
}


/* Raw pointers */

FD_EXPORT lispval fd_wrap_pointer(void *ptrval,
                                  fd_raw_recyclefn recycler,
                                  lispval typespec,
                                  u8_string idstring)
{
  struct FD_RAWPTR *rawptr = u8_alloc(struct FD_RAWPTR);
  FD_INIT_CONS(rawptr,fd_rawptr_type);
  rawptr->ptrval = ptrval;
  rawptr->recycler = recycler;
  rawptr->raw_typespec = typespec; fd_incref(typespec);
  if (SYMBOLP(typespec))
    rawptr->typestring = SYM_NAME(typespec);
  else if (STRINGP(typespec))
    rawptr->typestring = CSTRING(typespec);
  else rawptr->typestring = NULL;
  rawptr->idstring = u8_strdup(idstring);
  return (lispval) rawptr;
}


void fd_init_misctypes_c()
{
  fd_hashfns[fd_uuid_type]=hash_uuid;
  fd_dtype_writers[fd_uuid_type]=uuid_dtype;
  fd_copiers[fd_uuid_type]=copy_uuid;

  fd_unparsers[fd_timestamp_type]=unparse_timestamp;

  uuid_symbol = fd_intern("UUID");
  {
    struct FD_COMPOUND_TYPEINFO *e = 
      fd_register_compound(uuid_symbol,NULL,NULL);
    e->compound_dumpfn = uuid_dump;
    e->compound_restorefn = uuid_restore;}

  timestamp_symbol = fd_intern("TIMESTAMP");
  timestamp0_symbol = fd_intern("TIMESTAMP0");
  {
    struct FD_COMPOUND_TYPEINFO *e=
      fd_register_compound(timestamp_symbol,NULL,NULL);
    e->compound_parser = timestamp_parsefn;
    e->compound_dumpfn = NULL;
    e->compound_restorefn = timestamp_restore;}
  {
    struct FD_COMPOUND_TYPEINFO *e=
      fd_register_compound(timestamp0_symbol,NULL,NULL);
    e->compound_parser = timestamp_parsefn;
    e->compound_dumpfn = NULL;
    e->compound_restorefn = timestamp_restore;}
  fd_dtype_writers[fd_timestamp_type]=timestamp_dtype;

  fd_copiers[fd_timestamp_type]=copy_timestamp;


  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
