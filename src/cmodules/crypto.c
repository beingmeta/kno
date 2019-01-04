/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2007-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"

#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>
#include <libu8/u8rusage.h>
#include <libu8/u8crypto.h>

FD_EXPORT int fd_init_crypto(void) FD_LIBINIT_FN;

static long long int crypto_init = 0;

lispval (*unpacker)(unsigned char *,size_t len);

static lispval doencrypt(lispval data,lispval key,
                        u8_string ciphername,lispval iv,
                        int dtype)
{
  struct FD_OUTBUF tmp = { 0 };
  const unsigned char *payload; size_t payload_len; int free_payload = 0;
  const unsigned char *ivdata; size_t iv_len;
  unsigned char *outbuf; size_t outlen;
  if (!(FD_PACKETP(key)))
    return fd_type_error("packet/secret","doencrypt",key);
  if ((!(dtype))&& (FD_STRINGP(data))) {
    payload = FD_CSTRING(data); payload_len = FD_STRLEN(data);}
  else if ((!(dtype))&&(FD_PACKETP(data))) {
    payload = FD_PACKET_DATA(data); payload_len = FD_PACKET_LENGTH(data);}
  else {
    FD_INIT_BYTE_OUTPUT(&tmp,512);
    fd_write_dtype(&tmp,data);
    payload = tmp.buffer;
    payload_len = tmp.bufwrite-tmp.buffer;
    free_payload = 1;}
  if (FD_PACKETP(iv)) {
    ivdata = FD_PACKET_DATA(iv);
    iv_len = FD_PACKET_LENGTH(iv);}
  else {ivdata = NULL; iv_len = 0;}
  outbuf = u8_encrypt(payload,payload_len,(char *)ciphername,
                    FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
                    ivdata,iv_len,&outlen);
  if (outbuf) {
    if (free_payload) u8_free(payload);
    return fd_init_packet(NULL,outlen,outbuf);}
  else return FD_ERROR_VALUE;
}

static lispval encrypt_prim(lispval data,lispval key,lispval cipher,lispval iv)
{
  u8_string ciphername = NULL;
  if (FD_SYMBOLP(cipher)) ciphername = FD_SYMBOL_NAME(cipher);
  else if (FD_STRINGP(cipher)) ciphername = FD_CSTRING(cipher);
  else if (FD_VOIDP(cipher)) ciphername = NULL;
  else return fd_type_error("ciphername","encrypt_prim",cipher);
  return doencrypt(data,key,ciphername,iv,0);
}
static lispval encrypt_dtype_prim(lispval data,lispval key,lispval cipher,lispval iv)
{
  u8_string ciphername;
  if (FD_SYMBOLP(cipher)) ciphername = FD_SYMBOL_NAME(cipher);
  else if (FD_STRINGP(cipher)) ciphername = FD_CSTRING(cipher);
  else if (FD_VOIDP(cipher)) ciphername = NULL;
  else return fd_type_error("ciphername","encrypt_dtype",cipher);
  return doencrypt(data,key,ciphername,iv,1);
}

static lispval decrypt_prim(lispval data,lispval key,lispval cipher,lispval iv)
{
  const unsigned char *payload; size_t payload_len;
  unsigned char *outbuf; size_t outlen;
  const unsigned char *ivdata; size_t iv_len;
  u8_string ciphername;
  if (!(FD_PACKETP(key)))
    return fd_type_error("packet/secret","doencrypt",key);
  if (FD_SYMBOLP(cipher)) ciphername = FD_SYMBOL_NAME(cipher);
  else if (FD_STRINGP(cipher)) ciphername = FD_CSTRING(cipher);
  else if (FD_VOIDP(cipher)) ciphername = NULL;
  else return fd_type_error("ciphername","decrypt_prim",cipher);
  if (FD_PACKETP(iv)) {
    ivdata = FD_PACKET_DATA(iv);
    iv_len = FD_PACKET_LENGTH(iv);}
  else {ivdata = NULL; iv_len = 0;}
  payload = FD_PACKET_DATA(data); payload_len = FD_PACKET_LENGTH(data);
  outbuf = u8_decrypt(payload,payload_len,(char *)ciphername,
                      FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
                      ivdata,iv_len,&outlen);
  if (outbuf)
    return fd_init_packet(NULL,outlen,outbuf);
  else return FD_ERROR_VALUE;
}

static lispval decrypt2string_prim(lispval data,lispval key,lispval cipher,lispval iv)
{
  const unsigned char *payload; size_t payload_len;
  unsigned char *outbuf; size_t outlen;
  const unsigned char *ivdata; size_t iv_len;
  u8_string ciphername;
  if (!(FD_PACKETP(key))) return fd_type_error("packet/secret","doencrypt",key);
  if (FD_SYMBOLP(cipher)) ciphername = FD_SYMBOL_NAME(cipher);
  else if (FD_STRINGP(cipher)) ciphername = FD_CSTRING(cipher);
  else if (FD_VOIDP(cipher)) ciphername = NULL;
  else return fd_type_error("ciphername","decrypt_prim",cipher);
  if (FD_PACKETP(iv)) {
    ivdata = FD_PACKET_DATA(iv);
    iv_len = FD_PACKET_LENGTH(iv);}
  else {ivdata = NULL; iv_len = 0;}
  payload = FD_PACKET_DATA(data); payload_len = FD_PACKET_LENGTH(data);
  outbuf = u8_decrypt(payload,payload_len,ciphername,
                    FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
                    ivdata,iv_len,&outlen);
  if (outbuf)
    return fd_init_string(NULL,outlen,outbuf);
  else return FD_ERROR_VALUE;
}

static lispval decrypt2dtype_prim(lispval data,lispval key,lispval cipher,lispval iv)
{
  const unsigned char *payload; size_t payload_len;
  unsigned char *outbuf; size_t outlen;
  const unsigned char *ivdata; size_t iv_len;
  u8_string ciphername;
  if (!(FD_PACKETP(key))) return fd_type_error("packet/secret","doencrypt",key);
  if (FD_SYMBOLP(cipher)) ciphername = FD_SYMBOL_NAME(cipher);
  else if (FD_STRINGP(cipher)) ciphername = FD_CSTRING(cipher);
  else if (FD_VOIDP(cipher)) ciphername = NULL;
  else return fd_type_error("ciphername","decrypt_prim",cipher);
  if (FD_PACKETP(iv)) {
    ivdata = FD_PACKET_DATA(iv);
    iv_len = FD_PACKET_LENGTH(iv);}
  else {ivdata = NULL; iv_len = 0;}
  payload = FD_PACKET_DATA(data); payload_len = FD_PACKET_LENGTH(data);
  outbuf = u8_decrypt(payload,payload_len,ciphername,
                    FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
                    ivdata,iv_len,&outlen);
  if (outbuf) {
    struct FD_INBUF in = { 0 };
    lispval result;
    FD_INIT_BYTE_INPUT(&in,outbuf,outlen);
    result = fd_read_dtype(&in);
    u8_free(outbuf);
    return result;}
  else return FD_ERROR_VALUE;
}

FD_EXPORT lispval random_packet_prim(lispval arg)
{
  if (FD_UINTP(arg))
    return fd_init_packet(NULL,FD_FIX2INT(arg),
                          u8_random_vector(FD_FIX2INT(arg)));
  else return fd_type_error("uint","random_packet_prim",arg);
}

FD_EXPORT lispval fill_packet_prim(lispval len,lispval init)
{
  lispval result; unsigned char *bytes;
  int byte_init, packet_len = FD_FIX2INT(len);
  if ((FD_VOIDP(init))||(FD_FALSEP(init))||(FD_DEFAULTP(init)))
    byte_init = 0;
  else if (FD_FALSEP(init))
    byte_init = 1;
  else if (!(FD_FIXNUMP(init)))
    return fd_type_error(_("Byte init value"),"fill_packet_prim",init);
  else if (!(FD_UINTP(len)))
    return fd_type_error(_("uint len"),"fill_packet_prim",len);
  else {
    byte_init = FD_FIX2INT(init);
    if ((byte_init<0)||(byte_init>=256))
      return fd_type_error(_("Byte init value"),"fill_packet_prim",init);}
  result = fd_init_packet(NULL,packet_len,NULL);
  bytes = (unsigned char *)FD_PACKET_DATA(result);
  memset(bytes,byte_init,packet_len);
  return result;
}

FD_EXPORT int fd_init_crypto()
{
  lispval crypto_module;
  if (crypto_init) return 0;

  crypto_init = u8_millitime();
  crypto_module = fd_new_cmodule("CRYPTO",(FD_MODULE_SAFE),fd_init_crypto);

  u8_init_cryptofns();

  fd_idefn(crypto_module,
           fd_make_cprim4x("ENCRYPT",encrypt_prim,2,
                           -1,VOID,-1,VOID,
                           -1,VOID,-1,VOID));
  fd_idefn(crypto_module,
           fd_make_cprim4x("ENCRYPT-DTYPE",encrypt_dtype_prim,2,
                           -1,VOID,-1,VOID,
                           -1,VOID,-1,VOID));

  fd_idefn(crypto_module,
           fd_make_cprim4x("DECRYPT",decrypt_prim,2,
                           -1,VOID,-1,VOID,
                           -1,VOID,-1,VOID));
  fd_idefn(crypto_module,
           fd_make_cprim4x("DECRYPT->STRING",decrypt2string_prim,2,
                           -1,VOID,-1,VOID,
                           -1,VOID,-1,VOID));
  fd_idefn(crypto_module,
           fd_make_cprim4x("DECRYPT->DTYPE",decrypt2dtype_prim,2,
                           -1,VOID,-1,VOID,
                           -1,VOID,-1,VOID));

  fd_idefn(crypto_module,
           fd_make_cprim1x("RANDOM-PACKET",random_packet_prim,1,
                           fd_fixnum_type,VOID));
  fd_idefn(crypto_module,
           fd_make_cprim2x("FILL-PACKET",fill_packet_prim,1,
                           fd_fixnum_type,VOID,-1,VOID));

  fd_finish_module(crypto_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
