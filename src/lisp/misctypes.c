/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2020 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/hash.h"
#include "kno/cons.h"
#include "kno/xtypes.h"

#include "libu8/u8printf.h"

/* UUID Types */

static lispval uuid_tag;

KNO_EXPORT lispval kno_cons_uuid
(struct KNO_UUID *ptr,
 struct U8_XTIME *xtime,long long nodeid,short clockid)
{
  if (ptr == NULL) ptr = u8_alloc(struct KNO_UUID);
  KNO_INIT_CONS(ptr,kno_uuid_type);
  u8_consuuid(xtime,nodeid,clockid,(u8_uuid)&(ptr->uuid16));
  U8_CLEAR_ERRNO();
  return LISP_CONS(ptr);
}

KNO_EXPORT lispval kno_fresh_uuid(struct KNO_UUID *ptr)
{
  if (ptr == NULL) ptr = u8_alloc(struct KNO_UUID);
  KNO_INIT_CONS(ptr,kno_uuid_type);
  u8_getuuid((u8_uuid)&(ptr->uuid16));
  U8_CLEAR_ERRNO();
  return LISP_CONS(ptr);
}

KNO_EXPORT lispval kno_make_uuid(struct KNO_UUID *ptr,u8_uuid uuid)
{
  if (ptr == NULL) ptr = u8_alloc(struct KNO_UUID);
  KNO_INIT_CONS(ptr,kno_uuid_type);
  memcpy(ptr->uuid16,uuid,16);
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
  size = size+1+kno_write_dtype(out,uuid_tag);
  kno_write_byte(out,dt_packet); kno_write_4bytes(out,16); size = size+5;
  kno_write_bytes(out,uuid->uuid16,16); size = size+16;
  return size;
}

static int hash_uuid(lispval x,unsigned int (*fn)(lispval))
{
  struct KNO_UUID *uuid = kno_consptr(struct KNO_UUID *,x,kno_uuid_type);
  return kno_hash_bytes(uuid->uuid16,16);
}

static lispval uuid_dump(lispval x,kno_typeinfo MU e)
{
  struct KNO_UUID *uuid = kno_consptr(struct KNO_UUID *,x,kno_uuid_type);
  return kno_make_packet(NULL,16,uuid->uuid16);
}

static lispval uuid_restore(lispval MU tag,lispval x,kno_typeinfo MU e)
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

static int unparse_uuid(u8_output out,lispval x)
{
  struct KNO_UUID *uuid = kno_consptr(struct KNO_UUID *,x,kno_uuid_type);
  char buf[37]; u8_uuidstring((u8_uuid)(&(uuid->uuid16)),buf);
  u8_printf(out,"#U%s",buf);
  return 1;
}


/* Base types */

static lispval basetype_tag;

static lispval basetype_restore(lispval MU tag,lispval x,kno_typeinfo MU e)
{
  if (FIXNUMP(x)) {
    int codeval = KNO_FIX2INT(x);
    if ( (codeval >= 0) && (codeval <= kno_max_xtype ) )
      return KNO_CTYPE(codeval);}
  return kno_incref(x);
}

static ssize_t write_basetype_dtype(struct KNO_OUTBUF *out,lispval x)
{
  int ival = KNO_IMMEDIATE_DATA(x);
  ssize_t size = 1;
  kno_write_byte(out,dt_compound);
  size = size+kno_write_dtype(out,basetype_tag);
  size = size+kno_write_byte(out,dt_fixnum);
  size = size+kno_write_4bytes(out,ival);
  return size;
}

static lispval basetype_dump(lispval x,kno_typeinfo ignored)
{
  int ival = KNO_IMMEDIATE_DATA(x);
  return KNO_INT(ival);
}

/* Timestamps */

static lispval timestamp_symbol, timestamp0_symbol;

KNO_EXPORT
/* kno_make_timestamp:
   Arguments: a pointer to a U8_XTIME struct and a memory pool
   Returns: a lisp pointer to a timestamp
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
   Returns: a lisp pointer to a timestamp
*/
lispval kno_time2timestamp(time_t moment)
{
  struct U8_XTIME xt;
  u8_init_xtime(&xt,moment,u8_second,0,0,0);
  return kno_make_timestamp(&xt);
}

static int unparse_timestamp(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_TIMESTAMP *tm=
    kno_consptr(struct KNO_TIMESTAMP *,x,kno_timestamp_type);
  u8_puts(out,"#T");
  u8_xtime_to_iso8601(out,&(tm->u8xtimeval));
  return 1;
}

static lispval timestamp_parsefn(int n,lispval *args,kno_typeinfo e)
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

static int hash_timestamp(lispval x,unsigned int (*fn)(lispval))
{
  struct KNO_TIMESTAMP *tm=
    kno_consptr(struct KNO_TIMESTAMP *,x,kno_timestamp_type);
  return hash_mult(tm->u8xtimeval.u8_tick,MYSTERIOUS_MODULUS);
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

static ssize_t timestamp_xtype(struct KNO_OUTBUF *out,lispval x,xtype_refs refs)
{
  struct KNO_TIMESTAMP *xtm=
    kno_consptr(struct KNO_TIMESTAMP *,x,kno_timestamp_type);
  kno_write_byte(out,xt_tagged);
  kno_write_byte(out,xt_timestamp);
  ssize_t size = 2;
  if ((xtm->u8xtimeval.u8_prec == u8_second) && (xtm->u8xtimeval.u8_tzoff==0)) {
    lispval xval = KNO_INT(xtm->u8xtimeval.u8_tick);
    size += kno_write_xtype(out,xval,refs);}
  else {
    lispval vec = kno_empty_vector(4);
    int tzoff = xtm->u8xtimeval.u8_tzoff;
    KNO_VECTOR_SET(vec,0,KNO_INT(xtm->u8xtimeval.u8_tick));
    KNO_VECTOR_SET(vec,1,KNO_INT(xtm->u8xtimeval.u8_nsecs));
    KNO_VECTOR_SET(vec,2,KNO_INT((int)xtm->u8xtimeval.u8_prec));
    KNO_VECTOR_SET(vec,3,KNO_INT(tzoff));
    size = size+kno_write_xtype(out,vec,refs);
    kno_decref(vec);}
  return size;
}

static lispval timestamp_restore(lispval tag,lispval x,kno_typeinfo e)
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
  else return kno_err(kno_ExternalFormatError,"bad timestamp compound",NULL,x);
}

/* Restoring 'lispvals' which are basically strings to parse */

static lispval lispval_symbol;

static lispval lispval_restore(lispval tag,lispval x,kno_typeinfo e)
{
  if (KNO_STRINGP(x)) {
    lispval v = kno_parse(KNO_CSTRING(x));
    if (KNO_ABORTP(v)) {
      kno_clear_errors(0);
      return kno_init_compound(NULL,lispval_symbol,KNO_COMPOUND_INCREF,1,x);}
    else return v;}
  else return kno_init_compound(NULL,lispval_symbol,KNO_COMPOUND_INCREF,1,x);
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
    lispval src_string = kno_init_string(NULL,-1,src);
    u8_free(ptr);
    kno_seterr(kno_RegexError,"kno_make_regex",buf,src_string);
    kno_decref(src_string);
    return KNO_ERROR;}
  else {
    ptr->rxsrc = src;
    ptr->rxflags = flags;
    u8_init_mutex(&(ptr->rx_lock));
    ptr->rxactive = 1;
    return LISP_CONS(ptr);}
}

static int hash_regex(lispval x,unsigned int (*fn)(lispval))
{
  struct KNO_REGEX *rx = kno_consptr(struct KNO_REGEX *,x,kno_regex_type);
  u8_string src = rx->rxsrc;
  return kno_hash_bytes(src,strlen(src));
}

static int unparse_regex(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_REGEX *rx = (struct KNO_REGEX *)x;
  u8_printf(out,"#/%s/%s%s%s%s",rx->rxsrc,
            (((rx->rxflags)&REG_EXTENDED)?"e":""),
            (((rx->rxflags)&REG_NEWLINE)?"m":""),
            (((rx->rxflags)&REG_ICASE)?"i":""),
            (((rx->rxflags)&REG_NOSUB)?"s":""));
  return 1;
}

static ssize_t write_regex_dtype(struct KNO_OUTBUF *out,lispval x)
{
  struct KNO_REGEX *rx = (kno_regex) x;
  unsigned char buf[100], *tagname="%regex";
  u8_string rxsrc = rx->rxsrc;
  int srclen = strlen(rxsrc), rxflags = rx->rxflags;
  struct KNO_OUTBUF tmp = { 0 };
  KNO_INIT_OUTBUF(&tmp,buf,100,0);
  kno_write_byte(&tmp,dt_compound);
  kno_write_byte(&tmp,dt_symbol);
  kno_write_4bytes(&tmp,6);
  kno_write_bytes(&tmp,tagname,6);
  kno_write_byte(&tmp,dt_vector);
  kno_write_4bytes(&tmp,2);
  kno_write_byte(&tmp,dt_string);
  kno_write_4bytes(&tmp,srclen);
  kno_write_bytes(&tmp,rxsrc,srclen);
  kno_write_byte(&tmp,dt_fixnum);
  kno_write_4bytes(&tmp,rxflags);
  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  kno_write_bytes(out,tmp.buffer,n_bytes);
  kno_close_outbuf(&tmp);
  return n_bytes;
}

static ssize_t write_regex_xtype(struct KNO_OUTBUF *out,lispval x,
				 xtype_refs refs)
{
  struct KNO_REGEX *rx = (kno_regex) x;
  u8_string rxsrc = rx->rxsrc;
  int srclen = strlen(rxsrc);
  int rxflags = rx->rxflags;
  struct KNO_OUTBUF tmp = { 0 };
  unsigned char buf[100];
  KNO_INIT_OUTBUF(&tmp,buf,100,0);
  kno_write_byte(&tmp,xt_tagged);
  kno_write_byte(&tmp,xt_regex);

  kno_write_byte(&tmp,xt_vector);
  kno_write_varint(&tmp,2);

  kno_write_byte(&tmp,xt_utf8);
  kno_write_varint(&tmp,srclen);
  kno_write_bytes(&tmp,rxsrc,srclen);

  kno_write_byte(&tmp,xt_posint);
  kno_write_varint(&tmp,rxflags);

  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  kno_write_bytes(out,tmp.buffer,n_bytes);
  kno_close_outbuf(&tmp);
  return n_bytes;
}

static lispval regex_restore(lispval U8_MAYBE_UNUSED tag,
			     lispval x,
			     kno_typeinfo U8_MAYBE_UNUSED e)
{
  if ( (VECTORP(x)) && (KNO_VECTOR_LENGTH(x) == 2) ) {
    lispval rxsrc = KNO_VECTOR_REF(x,0);
    lispval rxflags = KNO_VECTOR_REF(x,1);
    if ( (KNO_STRINGP(rxsrc)) || (KNO_FIXNUMP(rxflags)) )
      return kno_make_regex(KNO_CSTRING(rxsrc),KNO_FIX2INT(rxflags));
    else return kno_err("Bad Regex","regex_restore",NULL,x);}
  else return kno_err("Bad UUID rep","uuid_restore",NULL,x);
}

/* Raw pointers */

KNO_EXPORT lispval kno_wrap_pointer(void *ptrval,
				    ssize_t rawlen,
                                    kno_raw_recyclefn recycler,
                                    lispval typetag,
                                    u8_string idstring)
{
  struct KNO_RAWPTR *rawptr = u8_alloc(struct KNO_RAWPTR);
  KNO_INIT_CONS(rawptr,kno_rawptr_type);
  struct KNO_TYPEINFO *info = kno_use_typeinfo(typetag);
  rawptr->ptrval = ptrval;
  rawptr->rawlen = rawlen;
  rawptr->raw_recycler = recycler;
  if (CONSP(typetag))
    rawptr->typetag = kno_incref(info->typetag);
  else rawptr->typetag = kno_incref(typetag);
  rawptr->typeinfo = info;
  if (idstring)
    rawptr->idstring = u8_strdup(idstring);
  else rawptr->idstring = NULL;
  rawptr->raw_annotations=KNO_VOID;
  rawptr->raw_cleanup=KNO_EMPTY;
  return (lispval) rawptr;
}

KNO_EXPORT int kno_set_pointer(lispval rawptr,lispval typetag,
			       void *ptrval,ssize_t len,
			       u8_string idstring)
{
  if (! (KNO_TYPEP(rawptr,kno_rawptr_type)) )
    return kno_reterr("Not a raw pointer","kno_set_pointer",
		      (SYMBOLP(typetag))?(SYMBOL_NAME(typetag)):
		      (STRINGP(typetag))?(CSTRING(typetag)):(NULL),
		      rawptr);
  struct KNO_RAWPTR *raw = (kno_rawptr) rawptr;
  if (raw->typetag != typetag)
    return kno_reterr("Wrong raw pointer type","kno_set_pointer",
		      (SYMBOLP(typetag))?(SYMBOL_NAME(typetag)):
		      (STRINGP(typetag))?(CSTRING(typetag)):(NULL),
		      rawptr);
  void *cur = raw->ptrval;
  if (ptrval) {
    if (cur == ptrval) {}
    else {
      if ( (cur) && (raw->raw_recycler) )
	raw->raw_recycler(cur);
      raw->ptrval = ptrval;}}
  if (len >= 0) raw->rawlen=len;
  if (idstring) {
    if (raw->idstring) {
      u8_string curstring = raw->idstring;
      raw->idstring=NULL;
      u8_free(curstring);}
    raw->idstring=idstring;}
  return 1;
}

/* Rawptr get functions */

static lispval init_rawptr_annotations(struct KNO_RAWPTR *raw)
{
  lispval annotations = raw->annotations;
  if ( (annotations!=KNO_NULL) &&
       (KNO_CONSP(annotations)) &&
       (KNO_XXCONS_TYPEP((annotations),kno_coretable_type)) )
    return annotations;
  lispval consed = kno_make_slotmap(2,0,NULL);
  KNO_LOCK_PTR(raw);
  annotations = raw->annotations;
  if ( (annotations != KNO_NULL) &&
       (KNO_CONSP(annotations)) &&
       (KNO_XXCONS_TYPEP(annotations,kno_coretable_type)) ) {
    KNO_UNLOCK_PTR(raw);
    kno_decref(consed);
    return annotations;}
  else {
    raw->annotations=consed;
    KNO_UNLOCK_PTR(raw);
    return consed;}
}

static lispval rawptr_get(lispval x,lispval key,lispval dflt)
{
  struct KNO_RAWPTR *a = (kno_rawptr) x;
  if ( (a->typeinfo) && (a->typeinfo->type_tablefns) )
    if (a->typeinfo->type_tablefns->get)
      return (a->typeinfo->type_tablefns->get)(x,key,dflt);
    else return KNO_EMPTY;
  else if ( (a->annotations == KNO_NULL) || (a->annotations == KNO_EMPTY) )
    return KNO_EMPTY;
  else return kno_get(a->annotations,key,dflt);
}

static int rawptr_test(lispval x,lispval key,lispval val)
{
  struct KNO_RAWPTR *a = (kno_rawptr) x;
  if ( (a->typeinfo) && (a->typeinfo->type_tablefns) ) {
    if (a->typeinfo->type_tablefns->test)
      return (a->typeinfo->type_tablefns->test)(x,key,val);
    else return 0;}
  else if ( (a->annotations == KNO_NULL) || (a->annotations == KNO_EMPTY) )
    return 0;
  lispval annotations = a->annotations;
  if ( (KNO_CONSP(annotations)) &&
       (KNO_XXCONS_TYPEP(annotations,kno_coretable_type)) )
    return kno_test(annotations,key,val);
  else return 0;
}

static int rawptr_store(lispval x,lispval key,lispval val)
{
  kno_rawptr raw = (kno_rawptr) x;
  if ( (raw->typeinfo) && (raw->typeinfo->type_tablefns) ) {
    if (raw->typeinfo->type_tablefns->store)
      return (raw->typeinfo->type_tablefns->store)(x,key,val);
    else return 0;}
  lispval annotations = init_rawptr_annotations((kno_rawptr)x);
  return kno_store(annotations,key,val);
}

static int rawptr_add(lispval x,lispval key,lispval val)
{
  kno_rawptr raw = (kno_rawptr) x;
  if ( (raw->typeinfo) && (raw->typeinfo->type_tablefns) ) {
    if (raw->typeinfo->type_tablefns->add)
      return (raw->typeinfo->type_tablefns->add)(x,key,val);
    else return 0;}
  lispval annotations = init_rawptr_annotations((kno_rawptr)x);
  return kno_add(annotations,key,val);
}

static int rawptr_drop(lispval x,lispval key,lispval val)
{
  kno_rawptr raw = (kno_rawptr) x;
  if ( (raw->typeinfo) && (raw->typeinfo->type_tablefns) ) {
    if (raw->typeinfo->type_tablefns->drop)
      return (raw->typeinfo->type_tablefns->drop)(x,key,val);
    else return 0;}
  lispval annotations = init_rawptr_annotations((kno_rawptr)x);
  return kno_drop(annotations,key,val);
}

static lispval rawptr_getkeys(lispval x)
{
  struct KNO_RAWPTR *raw = (kno_rawptr) x;
  if ( (raw->typeinfo) && (raw->typeinfo->type_tablefns) ) {
    if (raw->typeinfo->type_tablefns->keys)
      return (raw->typeinfo->type_tablefns->keys)(x);
    else return 0;}
  if ( (raw->annotations == KNO_NULL) || (raw->annotations == KNO_EMPTY) )
    return KNO_EMPTY;
  lispval annotations = raw->annotations;
  if ( (KNO_CONSP(annotations)) &&
       (KNO_XXCONS_TYPEP((annotations),kno_coretable_type)) )
    return kno_getkeys(annotations);
  else return KNO_EMPTY;
}

static int rawptr_tablep(lispval x)
{
  if (KNO_TYPEP(x,kno_rawptr_type)) {
    struct KNO_RAWPTR *raw = (kno_rawptr) x;
    return (raw->typeinfo) &&
      ( (raw->typeinfo->type_tablefns!=NULL) ||
	(raw->typeinfo->type_istable) );}
  else return 0;
}

static struct KNO_TABLEFNS rawptr_tablefns =
  {
   rawptr_get, /* get */
   rawptr_store,/* store */
   rawptr_add, /* add */
   rawptr_drop, /* drop */
   rawptr_test, /* test */
   NULL, /* readonly */
   NULL, /* modified */
   NULL, /* finished */
   NULL, /*getsize */
   rawptr_getkeys, /* getkeys */
   NULL, /* keyvec */
   NULL, /* keyvals */
   rawptr_tablep};

struct KNO_TABLEFNS *kno_rawptr_tablefns = &rawptr_tablefns;

/* Initializations from this file */

void kno_init_misctypes_c()
{
  kno_unparsers[kno_uuid_type]=unparse_uuid;
  kno_hashfns[kno_uuid_type]=hash_uuid;
  kno_dtype_writers[kno_uuid_type]=uuid_dtype;
  kno_copiers[kno_uuid_type]=copy_uuid;

  kno_unparsers[kno_regex_type]=unparse_regex;
  kno_hashfns[kno_regex_type]=hash_regex;

  kno_unparsers[kno_timestamp_type]=unparse_timestamp;
  kno_hashfns[kno_timestamp_type]=hash_timestamp;

  kno_tablefns[kno_rawptr_type]=kno_rawptr_tablefns;

  uuid_tag = kno_intern("uuid");
  {
    struct KNO_TYPEINFO *e = kno_use_typeinfo(uuid_tag);
    e->type_dumpfn = uuid_dump;
    e->type_restorefn = uuid_restore;}

  timestamp_symbol = kno_intern("timestamp");
  timestamp0_symbol = kno_intern("timestamp0");
  {
    struct KNO_TYPEINFO *e = kno_use_typeinfo(timestamp_symbol);
    e->type_consfn = timestamp_parsefn;
    e->type_dumpfn = NULL;
    e->type_restorefn = timestamp_restore;}
  {
    struct KNO_TYPEINFO *e = kno_use_typeinfo(timestamp0_symbol);
    e->type_consfn = timestamp_parsefn;
    e->type_dumpfn = NULL;
    e->type_restorefn = timestamp_restore;}
  kno_dtype_writers[kno_timestamp_type]=timestamp_dtype;

  {
    struct KNO_TYPEINFO *e = kno_use_typeinfo(kno_timestamp_xtag);
    e->type_consfn = NULL;
    e->type_dumpfn = NULL;
    e->type_restorefn = timestamp_restore;}
  kno_xtype_writers[kno_timestamp_type]=timestamp_xtype;

  kno_copiers[kno_timestamp_type]=copy_timestamp;

  lispval_symbol = kno_intern("%lispval");
  {
    struct KNO_TYPEINFO *e = kno_use_typeinfo(lispval_symbol);
    e->type_consfn = NULL;
    e->type_dumpfn = NULL;
    e->type_restorefn = lispval_restore;}
  kno_dtype_writers[kno_timestamp_type]=timestamp_dtype;

  {
    struct KNO_TYPEINFO *info = kno_use_typeinfo(kno_intern("%regex"));
    info->type_restorefn = regex_restore;}
  {
    struct KNO_TYPEINFO *info = kno_use_typeinfo(kno_regex_xtag);
    info->type_restorefn = regex_restore;}
  kno_dtype_writers[kno_regex_type] = write_regex_dtype;
  kno_xtype_writers[kno_regex_type] = write_regex_xtype;

  basetype_tag = kno_intern("%kno:basetype");
  {struct KNO_TYPEINFO *info = kno_use_typeinfo(basetype_tag);
    info->type_restorefn = basetype_restore;}
  {struct KNO_TYPEINFO *info = kno_use_typeinfo(KNO_CTYPE(kno_ctype_type));
    kno_decref(info->type_usetag);
    info->type_usetag = basetype_tag;
    info->type_dumpfn = basetype_dump;}

  kno_dtype_writers[kno_ctype_type] = write_basetype_dtype;
  /*  kno_xtype_writers[kno_ctype_type] = write_basetype_xtype; */

  u8_register_source_file(_FILEINFO);
}

