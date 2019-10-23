/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2007-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"
#include "kno/cprims.h"

#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>
#include <libu8/u8rusage.h>
#include <libu8/u8crypto.h>

KNO_EXPORT int kno_init_crypto(void) KNO_LIBINIT_FN;

static long long int crypto_init = 0;

lispval (*unpacker)(unsigned char *,size_t len);

static lispval doencrypt(lispval data,lispval key,
                         u8_string ciphername,lispval iv,
                         int dtype)
{
  struct KNO_OUTBUF tmp = { 0 };
  const unsigned char *payload; size_t payload_len; int free_payload = 0;
  const unsigned char *ivdata; size_t iv_len;
  unsigned char *outbuf; size_t outlen;
  if (!(KNO_PACKETP(key)))
    return kno_type_error("packet/secret","doencrypt",key);
  if ((!(dtype))&& (KNO_STRINGP(data))) {
    payload = KNO_CSTRING(data); payload_len = KNO_STRLEN(data);}
  else if ((!(dtype))&&(KNO_PACKETP(data))) {
    payload = KNO_PACKET_DATA(data); payload_len = KNO_PACKET_LENGTH(data);}
  else {
    KNO_INIT_BYTE_OUTPUT(&tmp,512);
    kno_write_dtype(&tmp,data);
    payload = tmp.buffer;
    payload_len = tmp.bufwrite-tmp.buffer;
    free_payload = 1;}
  if (KNO_PACKETP(iv)) {
    ivdata = KNO_PACKET_DATA(iv);
    iv_len = KNO_PACKET_LENGTH(iv);}
  else {ivdata = NULL; iv_len = 0;}
  outbuf = u8_encrypt(payload,payload_len,(char *)ciphername,
                      KNO_PACKET_DATA(key),KNO_PACKET_LENGTH(key),
                      ivdata,iv_len,&outlen);
  if (outbuf) {
    if (free_payload) u8_free(payload);
    return kno_init_packet(NULL,outlen,outbuf);}
  else return KNO_ERROR_VALUE;
}

DEFPRIM4("encrypt",encrypt_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
         "`(ENCRYPT *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval encrypt_prim(lispval data,lispval key,lispval cipher,lispval iv)
{
  u8_string ciphername = NULL;
  if (KNO_SYMBOLP(cipher)) ciphername = KNO_SYMBOL_NAME(cipher);
  else if (KNO_STRINGP(cipher)) ciphername = KNO_CSTRING(cipher);
  else if (KNO_VOIDP(cipher)) ciphername = NULL;
  else return kno_type_error("ciphername","encrypt_prim",cipher);
  return doencrypt(data,key,ciphername,iv,0);
}
DEFPRIM4("encrypt-dtype",encrypt_dtype_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
         "`(ENCRYPT-DTYPE *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval encrypt_dtype_prim(lispval data,lispval key,lispval cipher,lispval iv)
{
  u8_string ciphername;
  if (KNO_SYMBOLP(cipher)) ciphername = KNO_SYMBOL_NAME(cipher);
  else if (KNO_STRINGP(cipher)) ciphername = KNO_CSTRING(cipher);
  else if (KNO_VOIDP(cipher)) ciphername = NULL;
  else return kno_type_error("ciphername","encrypt_dtype",cipher);
  return doencrypt(data,key,ciphername,iv,1);
}

DEFPRIM4("decrypt",decrypt_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
         "`(DECRYPT *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval decrypt_prim(lispval data,lispval key,lispval cipher,lispval iv)
{
  const unsigned char *payload; size_t payload_len;
  unsigned char *outbuf; size_t outlen;
  const unsigned char *ivdata; size_t iv_len;
  u8_string ciphername;
  if (!(KNO_PACKETP(key)))
    return kno_type_error("packet/secret","doencrypt",key);
  if (KNO_SYMBOLP(cipher)) ciphername = KNO_SYMBOL_NAME(cipher);
  else if (KNO_STRINGP(cipher)) ciphername = KNO_CSTRING(cipher);
  else if (KNO_VOIDP(cipher)) ciphername = NULL;
  else return kno_type_error("ciphername","decrypt_prim",cipher);
  if (KNO_PACKETP(iv)) {
    ivdata = KNO_PACKET_DATA(iv);
    iv_len = KNO_PACKET_LENGTH(iv);}
  else {ivdata = NULL; iv_len = 0;}
  payload = KNO_PACKET_DATA(data); payload_len = KNO_PACKET_LENGTH(data);
  outbuf = u8_decrypt(payload,payload_len,(char *)ciphername,
                      KNO_PACKET_DATA(key),KNO_PACKET_LENGTH(key),
                      ivdata,iv_len,&outlen);
  if (outbuf)
    return kno_init_packet(NULL,outlen,outbuf);
  else return KNO_ERROR_VALUE;
}

DEFPRIM4("decrypt->string",decrypt2string_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
         "`(DECRYPT->STRING *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval decrypt2string_prim(lispval data,lispval key,lispval cipher,lispval iv)
{
  const unsigned char *payload; size_t payload_len;
  unsigned char *outbuf; size_t outlen;
  const unsigned char *ivdata; size_t iv_len;
  u8_string ciphername;
  if (!(KNO_PACKETP(key))) return kno_type_error("packet/secret","doencrypt",key);
  if (KNO_SYMBOLP(cipher)) ciphername = KNO_SYMBOL_NAME(cipher);
  else if (KNO_STRINGP(cipher)) ciphername = KNO_CSTRING(cipher);
  else if (KNO_VOIDP(cipher)) ciphername = NULL;
  else return kno_type_error("ciphername","decrypt_prim",cipher);
  if (KNO_PACKETP(iv)) {
    ivdata = KNO_PACKET_DATA(iv);
    iv_len = KNO_PACKET_LENGTH(iv);}
  else {ivdata = NULL; iv_len = 0;}
  payload = KNO_PACKET_DATA(data); payload_len = KNO_PACKET_LENGTH(data);
  outbuf = u8_decrypt(payload,payload_len,ciphername,
                      KNO_PACKET_DATA(key),KNO_PACKET_LENGTH(key),
                      ivdata,iv_len,&outlen);
  if (outbuf)
    return kno_init_string(NULL,outlen,outbuf);
  else return KNO_ERROR_VALUE;
}

DEFPRIM4("decrypt->dtype",decrypt2dtype_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
         "`(DECRYPT->DTYPE *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval decrypt2dtype_prim(lispval data,lispval key,lispval cipher,lispval iv)
{
  const unsigned char *payload; size_t payload_len;
  unsigned char *outbuf; size_t outlen;
  const unsigned char *ivdata; size_t iv_len;
  u8_string ciphername;
  if (!(KNO_PACKETP(key))) return kno_type_error("packet/secret","doencrypt",key);
  if (KNO_SYMBOLP(cipher)) ciphername = KNO_SYMBOL_NAME(cipher);
  else if (KNO_STRINGP(cipher)) ciphername = KNO_CSTRING(cipher);
  else if (KNO_VOIDP(cipher)) ciphername = NULL;
  else return kno_type_error("ciphername","decrypt_prim",cipher);
  if (KNO_PACKETP(iv)) {
    ivdata = KNO_PACKET_DATA(iv);
    iv_len = KNO_PACKET_LENGTH(iv);}
  else {ivdata = NULL; iv_len = 0;}
  payload = KNO_PACKET_DATA(data); payload_len = KNO_PACKET_LENGTH(data);
  outbuf = u8_decrypt(payload,payload_len,ciphername,
                      KNO_PACKET_DATA(key),KNO_PACKET_LENGTH(key),
                      ivdata,iv_len,&outlen);
  if (outbuf) {
    struct KNO_INBUF in = { 0 };
    lispval result;
    KNO_INIT_BYTE_INPUT(&in,outbuf,outlen);
    result = kno_read_dtype(&in);
    u8_free(outbuf);
    return result;}
  else return KNO_ERROR_VALUE;
}

DEFPRIM1("random-packet",random_packet_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(RANDOM-PACKET *arg0*)` **undocumented**",
         kno_fixnum_type,KNO_VOID);
KNO_EXPORT lispval random_packet_prim(lispval arg)
{
  if (KNO_UINTP(arg))
    return kno_init_packet(NULL,KNO_FIX2INT(arg),
                           u8_random_vector(KNO_FIX2INT(arg)));
  else return kno_type_error("uint","random_packet_prim",arg);
}


DEFPRIM2("fill-packet",fill_packet_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
         "`(FILL-PACKET *arg0* [*arg1*])` **undocumented**",
         kno_fixnum_type,KNO_VOID,kno_any_type,KNO_VOID);
KNO_EXPORT lispval fill_packet_prim(lispval len,lispval init)
{
  lispval result; unsigned char *bytes;
  int byte_init, packet_len = KNO_FIX2INT(len);
  if ((KNO_VOIDP(init))||(KNO_FALSEP(init))||(KNO_DEFAULTP(init)))
    byte_init = 0;
  else if (KNO_FALSEP(init))
    byte_init = 1;
  else if (!(KNO_FIXNUMP(init)))
    return kno_type_error(_("Byte init value"),"fill_packet_prim",init);
  else if (!(KNO_UINTP(len)))
    return kno_type_error(_("uint len"),"fill_packet_prim",len);
  else {
    byte_init = KNO_FIX2INT(init);
    if ((byte_init<0)||(byte_init>=256))
      return kno_type_error(_("Byte init value"),"fill_packet_prim",init);}
  result = kno_init_packet(NULL,packet_len,NULL);
  bytes = (unsigned char *)KNO_PACKET_DATA(result);
  memset(bytes,byte_init,packet_len);
  return result;
}

static lispval crypto_module;

KNO_EXPORT int kno_init_crypto()
{
  if (crypto_init) return 0;

  crypto_init = u8_millitime();
  crypto_module = kno_new_cmodule("crypto",0,kno_init_crypto);

  u8_init_cryptofns();

  link_local_cprims();

  kno_finish_module(crypto_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}



static void link_local_cprims()
{
  KNO_LINK_PRIM("decrypt->dtype",decrypt2dtype_prim,4,crypto_module);
  KNO_LINK_PRIM("decrypt->string",decrypt2string_prim,4,crypto_module);
  KNO_LINK_PRIM("decrypt",decrypt_prim,4,crypto_module);
  KNO_LINK_PRIM("encrypt-dtype",encrypt_dtype_prim,4,crypto_module);
  KNO_LINK_PRIM("encrypt",encrypt_prim,4,crypto_module);
  KNO_LINK_PRIM("random-packet",random_packet_prim,1,crypto_module);
  KNO_LINK_PRIM("fill-packet",fill_packet_prim,2,crypto_module);
}
