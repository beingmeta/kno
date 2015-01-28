/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* mongodb.c
   This implements FramerD bindings to mongodb.
   Copyright (C) 2007-2013 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/eval.h"
#include "framerd/sequences.h"
#include "framerd/texttools.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>

#include <mongoc.h>
/* Initialization */

static int mongodb_initialized=0;

static bool bson_write_keyval(bson_t *out,char *key,int keylen,fdtype val)
{
  bool ok=true;
  if (FD_CONSP(val)) {
    fd_ptr_type ctype=FD_PPTR_TYPE(val);
    switch (ctype) {
    case fd_string_type:
      ok=bson_append_utf8(out,key,keylen,FD_STRDATA(in),FD_STRLEN(in));
      break;
    case fd_packet_type:
      ok=bson_append_binary(out,key,keylen,BSON_SUBTYPE_BINARY,
                            FD_PACKET_DATA(in),FD_PACKET_LENGTH(in));
      break;
    case fd_double_type: {
      double d=FD_FLONUM(val);
      ok=bson_append_double(out,key,keylen,d);
      break;}
    case fd_bigint_type: {
      fd_bigint b=FD_GET_CONS(val,fd_bigint_type,fd_bigint);
      if (fd_bigint_fits_in_wordp(b,32,1)) {
        long int b32=fd_bigint_to_long(b);
        ok=bson_append_int32(out,key,keylen,b32);}
      else if (fd_bigint_fits_in_wordp(b,65,1)) {
        long long int b64=fd_bigint_to_long_long(b);
        ok=bson_append_int64(out,key,keylen,b64);}
      else {
        u8_log(LOG_WARN,"Can't save bigint value %q",val);
        ok=bson_append_int32(out,key,keylen,0);}
      break;}
    case fd_timestamp_type: {
      struct FD_TIMESTAMP *fdt=FD_GET_CONS(val,fd_timestamp_type,FD_TIMESTAMP);
      ok=bson_append_time_t(out,key,keylen,fdt->xtime.u8_tick);
      break;}
    case fd_uuid_type: {
      struct FD_UUID *uuid=FD_GET_CONS(val,fd_uuid_type,struct FD_UUID);
      ok=bson_append_binary(out,key,keylen,BSON_SUBTYPE_UUID,uuid->uuid,16);
      break;}
    case fd_choice_type: case fd_achoice_type: {
      bson_t arr; char buf[16];
      ok=bson_append_array_begin(out,key,keylen,&arr);
      if (ok) {
        int i=0; FD_DO_CHOICES(v,val) {
          sprintf(buf,"%d",i++);
          ok=bson_append_value(out,buf,strlen(buf),v);
          if (!(ok)) FD_STOP_DO_CHOICES;}}
      bson_append_array_end(out,key,keylen,&arr);
      break;}
    case fd_vector_type: case fd_rail_type: {
      struct FD_VECTOR *vec=(struct FD_VECTOR *)val;
      bson_t arr; char buf[16];
      int i=0, lim=vec->length;
      fdtype *data=vec->data;
      ok=bson_append_array_begin(out,key,keylen,&arr);
      if (ok) while (i<lim) {
          fdtype v=data[i]; sprintf(buf,"%d",i++);
          ok=bson_append_value(out,buf,strlen(buf),v);
          if (!(ok)) FD_STOP_DO_CHOICES;}
      bson_append_array_end(out,key,keylen,&arr);
      break;}
    case fd_slotmap_type: case fd_hashtable_type: {
      bson_t doc; char buf[16];
      fdtype keys=fd_getkeys(val);
      ok=bson_append_document_begin(out,key,keylen,&doc);
      if (ok) {
        FD_DO_CHOICES(key,keys) {
          fdtype value=fd_get(val,key,FD_VOID);
          if (!(FD_VOIDP(value))) {
            ok=bson_append_dtype(&doc,key,value);
            fd_decref(value);
            if (!(ok)) FD_STOP_DO_CHOICES;}}}
      fd_decref(keys);
      bson_append_document_end(out,key,keylen,&doc);
      break;}
    default: break;}
    return ok;}
  else if (FD_FIXNUMP(val))
    return bson_append_int32(out,key,keylen,FD_FIX2INT(val));
  else if (FD_OIDP(val)) {
    unsigned char bytes[12];
    FD_OID addr=FD_OID_ADDR(val); bson_oid_t oid;
    unsigned int hi=FD_OID_HI(addr), lo=FD_OID_LO(addr);
    bytes[11]=bytes[10]=bytes[9]=bytes[8]=0;
    bytes[7]=((hi>>24)&0xFF); bytes[6]=((hi>>16)&0xFF);
    bytes[5]=((hi>>8)&0xFF); bytes[4]=(hi&0xFF);
    bytes[3]=((lo>>24)&0xFF); bytes[2]=((lo>>16)&0xFF);
    bytes[1]=((lo>>8)&0xFF); bytes[0]=(lo&0xFF);
    bson_oid_init_from_data(&oid,bytes);
    return bson_append_oid(out,key,keylen,&oid);}
  else if (FD_SYMBOLP(val)) {
    return bson_append_utf8(out,key,keylen,FD_SYMBOL_NAME(val),-1);
}
  else if (FD_CHARACTERP(val)) {
    int code=FD_CHARCODE(val);
    if (code<128) {
      char c=code;
      return bson_append_utf8(out,key,keylen,&c,1);}
    else {
      struct U8_OUTPUT vout; U8_INIT_STATIC_OUTPUT(&vout,16);
      u8_putc(&vout,code);
      return bson_append_utf8(out,key,keylen,vout.u8_outbuf,
                              vout.u8_outptr-vout.u8_outbuf);}}
  else switch (val) {
    case FD_TRUE: case FD_FALSE:
      return bson_append_bool(out,key,keylen,(val==FD_TRUE));
    default: return true;}
}

static bool bson_write_slotval(bson_t *out,fdtype key,fdtype val)
{
  struct U8_OUTPUT keyout;
  const char *keystring; int keylen; bool ok=true;
  U8_INIT_STATIC_OUTPUT(&keyout,256);
  if (FD_VOIDP(val)) continue;
  if (FD_SYMBOLP(key)) {
    keystring=FD_SYMBOL_NAME(key);
    keylen=strlen(keystring);}
  else if (FD_STRINGP(key)) {
    keystring=FD_STRDATA(key);
    keylen=FD_STRLEN(key);}
  else {
    keyout.u8_outptr=keyout.u8_outbuf;
    u8_putc(&keyout,':');
    fd_unparse(&keyout,key);
    keystring=keyout.u8_outbuf;
    keylen=keyout.u8_outptr-keyout.u8_outbuf;}
  ok=bson_write_keyval(out,keystring,keylen,val);
  u8_close(&keyout);
  return ok;
}

FD_EXPORT fdtype fd_bson_write(bson_t *out,fdtype in)
{
  struct U8_OUTPUT keyout; unsigned char buf[128];
  fdtype keys=fd_getkeys(in);
  U8_INIT_OUTPUT_BUF(&out,128,buf);
  {FD_DO_CHOICES(key,keys) {
      fdtype val=fd_get(in,key,FD_VOID);
      bson_write_slotval(out,key,val);
      fd_decref(val);}}
  fd_decref(keys);
  return FD_VOID;
}

FD_EXPORT int fd_init_mongodb()
{
  fdtype module;
  if (mongodb_initialized) return 0;
  mongodb_initialized=1;
  fd_init_fdscheme();

  module=fd_new_module("MONGODB",(0));

  fd_finish_module(module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
