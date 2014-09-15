/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2007-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"

#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>
#include <libu8/u8rusage.h>
#include <libu8/u8crypto.h>

FD_EXPORT int fd_init_crypto(void) FD_LIBINIT_FN;

static int crypto_init=0;

fdtype (*unpacker)(unsigned char *,size_t len);

static fdtype doencrypt(fdtype data,fdtype key,u8_string ciphername,int dtype)
{
  struct FD_BYTE_OUTPUT tmp;
  unsigned char *payload; size_t payload_len; int free_payload=0;
  unsigned char *outbuf; size_t outlen;
  if (!(FD_PACKETP(key))) return fd_type_error("packet/secret","doencrypt",key);
  if ((!(dtype))&& (FD_STRINGP(data))) {
    payload=FD_STRDATA(data); payload_len=FD_STRLEN(data);}
  else if ((!(dtype))&&(FD_PACKETP(data))) {
    payload=FD_PACKET_DATA(data); payload_len=FD_PACKET_LENGTH(data);}
  else {
    FD_INIT_BYTE_OUTPUT(&tmp,512);
    fd_write_dtype(&tmp,data);
    payload=tmp.start; payload_len=tmp.ptr-tmp.start;
    free_payload=1;}
  outbuf=u8_encrypt(payload,payload_len,ciphername,
                    FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
                    &outlen);
  if (outbuf) {
    if (free_payload) u8_free(payload);
    return fd_init_packet(NULL,outlen,outbuf);}
  else return FD_ERROR_VALUE;
}

static fdtype encrypt_prim(fdtype data,fdtype key,fdtype cipher)
{
  u8_string ciphername=NULL;
  if (FD_SYMBOLP(cipher)) ciphername=FD_SYMBOL_NAME(cipher);
  else if (FD_STRINGP(cipher)) ciphername=FD_STRDATA(cipher);
  else if (FD_VOIDP(cipher)) ciphername=NULL;
  else return fd_type_error("ciphername","encrypt_prim",cipher);
  return doencrypt(data,key,ciphername,0);
}
static fdtype encrypt_dtype_prim(fdtype data,fdtype key,fdtype cipher)
{
  u8_string ciphername;
  if (FD_SYMBOLP(cipher)) ciphername=FD_SYMBOL_NAME(cipher);
  else if (FD_STRINGP(cipher)) ciphername=FD_STRDATA(cipher);
  else if (FD_VOIDP(cipher)) ciphername=NULL;
  else return fd_type_error("ciphername","encrypt_dtype",cipher);
  return doencrypt(data,key,ciphername,1);
}

static fdtype decrypt_prim(fdtype data,fdtype key,fdtype cipher)
{
  unsigned char *payload; size_t payload_len;
  unsigned char *outbuf; size_t outlen;
  u8_string ciphername;
  if (!(FD_PACKETP(key)))
    return fd_type_error("packet/secret","doencrypt",key);
  if (FD_SYMBOLP(cipher)) ciphername=FD_SYMBOL_NAME(cipher);
  else if (FD_STRINGP(cipher)) ciphername=FD_STRDATA(cipher);
  else if (FD_VOIDP(cipher)) ciphername=NULL;
  else return fd_type_error("ciphername","decrypt_prim",cipher);
  payload=FD_PACKET_DATA(data); payload_len=FD_PACKET_LENGTH(data);
  outbuf=u8_decrypt(payload,payload_len,ciphername,
                    FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
                    &outlen);
  if (outbuf)
    return fd_init_packet(NULL,outlen,outbuf);
  else return FD_ERROR_VALUE;
}

static fdtype decrypt2string_prim(fdtype data,fdtype key,fdtype cipher)
{
  unsigned char *payload; size_t payload_len;
  unsigned char *outbuf; size_t outlen;
  u8_string ciphername;
  if (!(FD_PACKETP(key))) return fd_type_error("packet/secret","doencrypt",key);
  if (FD_SYMBOLP(cipher)) ciphername=FD_SYMBOL_NAME(cipher);
  else if (FD_STRINGP(cipher)) ciphername=FD_STRDATA(cipher);
  else if (FD_VOIDP(cipher)) ciphername=NULL;
  else return fd_type_error("ciphername","decrypt_prim",cipher);
  payload=FD_PACKET_DATA(data); payload_len=FD_PACKET_LENGTH(data);
  outbuf=u8_decrypt(payload,payload_len,ciphername,
                    FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
                    &outlen);
  if (outbuf)
    return fd_init_string(NULL,outlen,outbuf);
  else return FD_ERROR_VALUE;
}

static fdtype decrypt2dtype_prim(fdtype data,fdtype key,fdtype cipher)
{
  unsigned char *payload; size_t payload_len;
  unsigned char *outbuf; size_t outlen;
  u8_string ciphername;
  if (!(FD_PACKETP(key))) return fd_type_error("packet/secret","doencrypt",key);
  if (FD_SYMBOLP(cipher)) ciphername=FD_SYMBOL_NAME(cipher);
  else if (FD_STRINGP(cipher)) ciphername=FD_STRDATA(cipher);
  else if (FD_VOIDP(cipher)) ciphername=NULL;
  else return fd_type_error("ciphername","decrypt_prim",cipher);
  payload=FD_PACKET_DATA(data); payload_len=FD_PACKET_LENGTH(data);
  outbuf=u8_decrypt(payload,payload_len,ciphername,
                    FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
                    &outlen);
  if (outbuf) {
    struct FD_BYTE_INPUT in; fdtype result;
    FD_INIT_BYTE_INPUT(&in,outbuf,outlen);
    result=fd_read_dtype(&in);
    u8_free(outbuf);
    return result;}
  else return FD_ERROR_VALUE;
}

FD_EXPORT fdtype random_packet_prim(fdtype arg)
{
  return fd_init_packet(NULL,FD_FIX2INT(arg),
                        u8_random_vector(FD_FIX2INT(arg)));
}

FD_EXPORT int fd_init_crypto()
{
  fdtype crypto_module;
  if (crypto_init) return 0;

  crypto_init=1;
  crypto_module=fd_new_module("CRYPTO",(FD_MODULE_SAFE));

  u8_init_cryptofns();

  fd_idefn(crypto_module,
           fd_make_cprim3x("ENCRYPT",encrypt_prim,2,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_VOID));
  fd_idefn(crypto_module,
           fd_make_cprim3x("ENCRYPT-DTYPE",encrypt_dtype_prim,2,
                           -1,FD_VOID,-1,FD_VOID,-1,FD_VOID));

  fd_idefn(crypto_module,
           fd_make_cprim3x("DECRYPT",decrypt_prim,2,
                           -1,FD_VOID,-1,FD_VOID,-1,FD_VOID));
  fd_idefn(crypto_module,
           fd_make_cprim3x("DECRYPT->STRING",decrypt2string_prim,2,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_VOID));
  fd_idefn(crypto_module,
           fd_make_cprim3x("DECRYPT->DTYPE",decrypt2dtype_prim,2,
                           -1,FD_VOID,-1,FD_VOID,-1,FD_VOID));

  fd_idefn(crypto_module,
           fd_make_cprim1x
           ("RANDOM-PACKET",random_packet_prim,1,fd_fixnum_type,FD_VOID));

  fd_finish_module(crypto_module);
  fd_persist_module(crypto_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
