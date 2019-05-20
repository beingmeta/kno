/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/cons.h"

/* UUID Types */

static lispval uuid_symbol;

KNO_EXPORT lispval kno_cons_uuid
   (struct KNO_UUID *ptr,
    struct U8_XTIME *xtime,long long nodeid,short clockid)
{
  if (ptr == NULL) ptr = u8_alloc(struct KNO_UUID);
  KNO_INIT_CONS(ptr,kno_uuid_type);
  u8_consuuid(xtime,nodeid,clockid,(u8_uuid)&(ptr->uuid16));
  return LISP_CONS(ptr);
}

KNO_EXPORT lispval kno_fresh_uuid(struct KNO_UUID *ptr)
{
  if (ptr == NULL) ptr = u8_alloc(struct KNO_UUID);
  KNO_INIT_CONS(ptr,kno_uuid_type);
  u8_getuuid((u8_uuid)&(ptr->uuid16));
  return LISP_CONS(ptr);
}

static lispval copy_uuid(lispval x,int deep)
{
  struct KNO_UUID *uuid = kno_consptr(struct KNO_UUID *,x,kno_uuid_type);
  struct KNO_UUID *nuuid = u8_alloc(struct KNO_UUID);
  KNO_INIT_CONS(nuuid,kno_uuid_type);
  memcpy(nuuid->uuid16,uuid->uuid16,16);
  return LISP_CONS(nuuid);
}

#define MU U8_MAYBE_UNUSED

static ssize_t uuid_dtype(struct KNO_OUTBUF *out,lispval x)
{
  ssize_t size = 0;
  struct KNO_UUID *uuid = kno_consptr(struct KNO_UUID *,x,kno_uuid_type);
  kno_write_byte(out,dt_compound);
  size = size+1+kno_write_dtype(out,uuid_symbol);
  kno_write_byte(out,dt_packet); kno_write_4bytes(out,16); size = size+5;
  kno_write_bytes(out,uuid->uuid16,16); size = size+16;
  return size;
}

static int hash_uuid(lispval x,unsigned int (*fn)(lispval))
{
  struct KNO_UUID *uuid = kno_consptr(struct KNO_UUID *,x,kno_uuid_type);
  return kno_hash_bytes(uuid->uuid16,16);
}

static lispval uuid_dump(lispval x,kno_compound_typeinfo MU e)
{
  struct KNO_UUID *uuid = kno_consptr(struct KNO_UUID *,x,kno_uuid_type);
  return kno_make_packet(NULL,16,uuid->uuid16);
}

static lispval uuid_restore(lispval MU tag,lispval x,kno_compound_typeinfo MU e)
{
  if (PACKETP(x)) {
    struct KNO_STRING *p = kno_consptr(struct KNO_STRING *,x,kno_packet_type);
    if (p->str_bytelen==16) {
      struct KNO_UUID *uuid = u8_alloc(struct KNO_UUID);
      KNO_INIT_CONS(uuid,kno_uuid_type);
      memcpy(uuid->uuid16,p->str_bytes,16);
      return LISP_CONS(uuid);}
    else return kno_err("Bad UUID packet","uuid_restore",
                       "UUID packet has wrong length",
                       x);}
  else return kno_err("Bad UUID rep","uuid_restore",
                     "UUID serialization isn't a packet",
                     x);
}


/* Timestamps */

static lispval timestamp_symbol, timestamp0_symbol;

KNO_EXPORT
/* kno_make_timestamp:
    Arguments: a pointer to a U8_XTIME struct and a memory pool
    Returns: a dtype pointer to a timestamp
 */
lispval kno_make_timestamp(struct U8_XTIME *tm)
{
  struct KNO_TIMESTAMP *tstamp = u8_alloc(struct KNO_TIMESTAMP);
  memset(tstamp,0,sizeof(struct KNO_TIMESTAMP));
  KNO_INIT_CONS(tstamp,kno_timestamp_type);
  if (tm)
    memcpy(&(tstamp->u8xtimeval),tm,sizeof(struct U8_XTIME));
  else u8_now(&(tstamp->u8xtimeval));
  return LISP_CONS(tstamp);
}

KNO_EXPORT
/* kno_time2timestamp
    Arguments: a pointer to a U8_XTIME struct and a memory pool
    Returns: a dtype pointer to a timestamp
 */
lispval kno_time2timestamp(time_t moment)
{
  struct U8_XTIME xt;
  u8_init_xtime(&xt,moment,u8_second,0,0,0);
  return kno_make_timestamp(&xt);
}

static int reversible_time = 1;

static int unparse_timestamp(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_TIMESTAMP *tm=
    kno_consptr(struct KNO_TIMESTAMP *,x,kno_timestamp_type);
  if (reversible_time) {
    u8_puts(out,"#T");
    u8_xtime_to_iso8601(out,&(tm->u8xtimeval));
    return 1;}
  else {
    char xbuf[32];
    u8_puts(out,"#<TIMESTAMP 0x");
    u8_puts(out,u8_uitoa16(KNO_LONGVAL(x),xbuf));
    u8_puts(out," \"");
    u8_xtime_to_iso8601(out,&(tm->u8xtimeval));
    u8_puts(out,"\">");
    return 1;}
}

static lispval timestamp_parsefn(int n,lispval *args,kno_compound_typeinfo e)
{
  struct KNO_TIMESTAMP *tm = u8_alloc(struct KNO_TIMESTAMP);
  u8_string timestring;
  memset(tm,0,sizeof(struct KNO_TIMESTAMP));
  KNO_INIT_CONS(tm,kno_timestamp_type);
  if ((n==2) && (STRINGP(args[1])))
    timestring = CSTRING(args[1]);
  else if ((n==3) && (STRINGP(args[2])))
    timestring = CSTRING(args[2]);
  else return kno_err(kno_CantParseRecord,"TIMESTAMP",NULL,VOID);
  u8_iso8601_to_xtime(timestring,&(tm->u8xtimeval));
  return LISP_CONS(tm);
}

static lispval copy_timestamp(lispval x,int deep)
{
  struct KNO_TIMESTAMP *tm=
    kno_consptr(struct KNO_TIMESTAMP *,x,kno_timestamp_type);
  struct KNO_TIMESTAMP *newtm = u8_alloc(struct KNO_TIMESTAMP);
  memset(newtm,0,sizeof(struct KNO_TIMESTAMP));
  KNO_INIT_CONS(newtm,kno_timestamp_type);
  memcpy(&(newtm->u8xtimeval),&(tm->u8xtimeval),sizeof(struct U8_XTIME));
  return LISP_CONS(newtm);
}

static ssize_t timestamp_dtype(struct KNO_OUTBUF *out,lispval x)
{
  struct KNO_TIMESTAMP *xtm=
    kno_consptr(struct KNO_TIMESTAMP *,x,kno_timestamp_type);
  ssize_t size = 1;
  kno_write_byte(out,dt_compound);
  size = size+kno_write_dtype(out,timestamp_symbol);
  if ((xtm->u8xtimeval.u8_prec == u8_second) && (xtm->u8xtimeval.u8_tzoff==0)) {
    lispval xval = KNO_INT(xtm->u8xtimeval.u8_tick);
    size = size+kno_write_dtype(out,xval);}
  else {
    lispval vec = kno_empty_vector(4);
    int tzoff = xtm->u8xtimeval.u8_tzoff;
    KNO_VECTOR_SET(vec,0,KNO_INT(xtm->u8xtimeval.u8_tick));
    KNO_VECTOR_SET(vec,1,KNO_INT(xtm->u8xtimeval.u8_nsecs));
    KNO_VECTOR_SET(vec,2,KNO_INT((int)xtm->u8xtimeval.u8_prec));
    KNO_VECTOR_SET(vec,3,KNO_INT(tzoff));
    size = size+kno_write_dtype(out,vec);
    kno_decref(vec);}
  return size;
}

static lispval timestamp_restore(lispval tag,lispval x,kno_compound_typeinfo e)
{
  if (FIXNUMP(x)) {
    struct KNO_TIMESTAMP *tm = u8_alloc(struct KNO_TIMESTAMP);
    memset(tm,0,sizeof(struct KNO_TIMESTAMP));
    KNO_INIT_CONS(tm,kno_timestamp_type);
    u8_init_xtime(&(tm->u8xtimeval),FIX2INT(x),u8_second,0,0,0);
    return LISP_CONS(tm);}
  else if (KNO_BIGINTP(x)) {
    struct KNO_TIMESTAMP *tm = u8_alloc(struct KNO_TIMESTAMP);
    time_t tval = (time_t)(kno_bigint_to_long((kno_bigint)x));
    memset(tm,0,sizeof(struct KNO_TIMESTAMP));
    KNO_INIT_CONS(tm,kno_timestamp_type);
    u8_init_xtime(&(tm->u8xtimeval),tval,u8_second,0,0,0);
    return LISP_CONS(tm);}
  else if (VECTORP(x)) {
    struct KNO_TIMESTAMP *tm = u8_alloc(struct KNO_TIMESTAMP);
    int secs = kno_getint(VEC_REF(x,0));
    int nsecs = kno_getint(VEC_REF(x,1));
    int iprec = kno_getint(VEC_REF(x,2));
    int tzoff = kno_getint(VEC_REF(x,3));
    memset(tm,0,sizeof(struct KNO_TIMESTAMP));
    KNO_INIT_CONS(tm,kno_timestamp_type);
    u8_init_xtime(&(tm->u8xtimeval),secs,iprec,nsecs,tzoff,0);
    return LISP_CONS(tm);}
  else return kno_err(kno_DTypeError,"bad timestamp compound",NULL,x);
}


/* Regexes */

u8_condition kno_RegexError=_("Regular expression error");

static int default_regex_flags = REG_EXTENDED|REG_NEWLINE;

KNO_EXPORT lispval kno_make_regex(u8_string src,int flags)
{
  struct KNO_REGEX *ptr = u8_alloc(struct KNO_REGEX); int retval;
  KNO_INIT_FRESH_CONS(ptr,kno_regex_type);
  if (flags<0) flags = default_regex_flags;
  src = u8_strdup(src);
  retval = regcomp(&(ptr->rxcompiled),src,flags);
  if (retval) {
    u8_byte buf[512];
    regerror(retval,&(ptr->rxcompiled),buf,512);
    u8_free(ptr);
    return kno_err(kno_RegexError,"kno_make_regex",u8_strdup(buf),
                  kno_init_string(NULL,-1,src));}
  else {
    ptr->rxflags = flags; ptr->rxsrc = src;
    u8_init_mutex(&(ptr->rx_lock)); ptr->rxactive = 1;
    return LISP_CONS(ptr);}
}


/* Raw pointers */

KNO_EXPORT lispval kno_wrap_pointer(void *ptrval,
                                  kno_raw_recyclefn recycler,
                                  lispval typespec,
                                  u8_string idstring)
{
  struct KNO_RAWPTR *rawptr = u8_alloc(struct KNO_RAWPTR);
  KNO_INIT_CONS(rawptr,kno_rawptr_type);
  rawptr->ptrval = ptrval;
  rawptr->recycler = recycler;
  rawptr->raw_typespec = typespec; kno_incref(typespec);
  if (SYMBOLP(typespec))
    rawptr->typestring = SYM_NAME(typespec);
  else if (STRINGP(typespec))
    rawptr->typestring = CSTRING(typespec);
  else rawptr->typestring = NULL;
  rawptr->idstring = u8_strdup(idstring);
  return (lispval) rawptr;
}


void kno_init_misctypes_c()
{
  kno_hashfns[kno_uuid_type]=hash_uuid;
  kno_dtype_writers[kno_uuid_type]=uuid_dtype;
  kno_copiers[kno_uuid_type]=copy_uuid;

  kno_unparsers[kno_timestamp_type]=unparse_timestamp;

  uuid_symbol = kno_intern("uuid");
  {
    struct KNO_COMPOUND_TYPEINFO *e = 
      kno_register_compound(uuid_symbol,NULL,NULL);
    e->compound_dumpfn = uuid_dump;
    e->compound_restorefn = uuid_restore;}

  timestamp_symbol = kno_intern("timestamp");
  timestamp0_symbol = kno_intern("timestamp0");
  {
    struct KNO_COMPOUND_TYPEINFO *e=
      kno_register_compound(timestamp_symbol,NULL,NULL);
    e->compound_parser = timestamp_parsefn;
    e->compound_dumpfn = NULL;
    e->compound_restorefn = timestamp_restore;}
  {
    struct KNO_COMPOUND_TYPEINFO *e=
      kno_register_compound(timestamp0_symbol,NULL,NULL);
    e->compound_parser = timestamp_parsefn;
    e->compound_dumpfn = NULL;
    e->compound_restorefn = timestamp_restore;}
  kno_dtype_writers[kno_timestamp_type]=timestamp_dtype;

  kno_copiers[kno_timestamp_type]=copy_timestamp;


  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
