/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2007-2020 beingmeta, inc.
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
#include <libu8/u8convert.h>
#include <libu8/u8pathfns.h>

#include <sys/types.h>
#include <zlib.h>

static u8_condition zlibOutOfMemory=_("ZLIB out of memory");
static u8_condition zlibBufferError=_("ZLIB buffer error");
static u8_condition zlibBadErrorCode=_("ZLIB odd error code");
static u8_condition zlibDataError=_("Bad ZLIB input data");


KNO_DEFCPRIM("zlib/compress",zlib_compress_prim,
	     KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	     ""
	     "**undocumented**",
	     {"input_arg",kno_any_type,KNO_VOID},
	     {"level_arg",kno_any_type,KNO_VOID})
static lispval zlib_compress_prim(lispval input_arg,lispval level_arg)
{
  int level = ((KNO_UINTP(level_arg))?(KNO_FIX2INT(level_arg)):(9));
  const Bytef *input; uLongf input_len = 0;
  Bytef *output; uLongf output_len = 0;
  int retval;
  if ((level<0)||(level>9)) level = 9;
  if (KNO_STRINGP(input_arg)) {
    input = KNO_CSTRING(input_arg);
    input_len = KNO_STRLEN(input_arg);}
  else if (KNO_PACKETP(input_arg)) {
    input = KNO_PACKET_DATA(input_arg);
    input_len = KNO_PACKET_LENGTH(input_arg);}
  else return kno_type_error("string or packet","zip_prim",input_arg);
  output_len = input_len; output = u8_malloc(output_len);
  retval = compress2(output,&output_len,input,input_len,level);
  if (retval == Z_BUF_ERROR) {
    Bytef *newbuf = u8_malloc(input_len*2);
    if (!(newbuf)) {
      u8_free(output);
      return kno_err(u8_MallocFailed,"zip_prim",NULL,KNO_VOID);}
    u8_free(output); output = newbuf; output_len = input_len*2;
    retval = compress2(output,&output_len,input,input_len,9);}
  if (retval>=Z_OK)
    return kno_init_packet(NULL,output_len,output);
  else {
    u8_condition ex;
    switch (retval) {
    case Z_MEM_ERROR: ex = zlibOutOfMemory; break;
    case Z_BUF_ERROR: ex = zlibBufferError; break;
    default: ex = zlibBadErrorCode; break;}
    u8_free(output);
    return kno_err(ex,"zip_prim",NULL,KNO_VOID);}
}


KNO_DEFCPRIM("zlib/uncompress",zlib_uncompress_prim,
	     KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	     ""
	     "**undocumented**",
	     {"input_arg",kno_packet_type,KNO_VOID},
	     {"text",kno_any_type,KNO_FALSE},
	     {"init_factor",kno_any_type,KNO_FALSE})
static lispval zlib_uncompress_prim(lispval input_arg,lispval text,lispval init_factor)
{
  int init_grow = ((KNO_UINTP(init_factor))?(KNO_FIX2INT(init_factor)):(5));
  const Bytef *input; uLongf input_len = 0;
  Bytef *output; uLongf output_len = 0, buf_len = 0;
  int retval;
  if ((init_grow<1)||(init_grow>20)) init_grow = 5;
  input = KNO_PACKET_DATA(input_arg);
  input_len = KNO_PACKET_LENGTH(input_arg);
  output_len = buf_len = (input_len*init_grow);
  output = u8_malloc(buf_len);
  if (!(output)) return kno_err(u8_MallocFailed,"unzip_prim",NULL,KNO_VOID);
  while ((retval = uncompress(output,&output_len,input,input_len))<Z_OK) {
    if (retval == Z_BUF_ERROR) {
      Bytef *newbuf = u8_malloc(buf_len*2);
      if (!(newbuf)) {
        u8_free(output);
        return kno_err(u8_MallocFailed,"unzip_prim",NULL,KNO_VOID);}
      else {
        u8_free(output); output = newbuf;
        buf_len = buf_len*2; output_len = buf_len;}}
    else if (retval == Z_MEM_ERROR) {
      u8_free(output);
      return kno_err(zlibOutOfMemory,"unzip_prim",NULL,KNO_VOID);}
    else if (retval == Z_DATA_ERROR) {
      u8_free(output);
      return kno_err(zlibDataError,"unzip_prim",NULL,KNO_VOID);}
    else {
      u8_free(output);
      return kno_err(zlibBadErrorCode,"unzip_prim",NULL,KNO_VOID);}}
  if ((KNO_FALSEP(text))||(KNO_VOIDP(text)))
    return kno_init_packet(NULL,output_len,output);
  else if (KNO_TRUEP(text)) {
    if (u8_validate(output,output_len) == output_len) {
      if (buf_len>(output_len+1)) output[output_len]='\0';
      else {
        unsigned char *wnull = u8_realloc(output,output_len+1);
        if (!(wnull)) {
          u8_free(output);
          return kno_err(u8_MallocFailed,"unzip_prim",NULL,KNO_VOID);}
        else {
          if (wnull!=output) u8_free(output);
          wnull[output_len]='\0'; output = wnull;}}
      return kno_init_string(NULL,output_len,output);}
    else return kno_init_packet(NULL,output_len,output);}
  else if (KNO_STRINGP(text)) {
    u8_encoding enc = u8_get_encoding(KNO_CSTRING(text));
    if (!(enc)) {
      u8_free(output);
      return kno_type_error("text encoding","unzip_prim",text);}
    else {
      int retval = 0;
      /* The output (of compression) is now the input for conversion */
      const unsigned char *scan = output, *limit = output+output_len;
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,output_len);
      retval = u8_convert(enc,0,&out,&scan,limit);
      if (retval<0) {
        u8_free(output); u8_free(out.u8_outbuf);
        return kno_err("malencoded text","unzip_prim",NULL,KNO_VOID);}
      else {
        u8_free(output);
        return kno_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);}}}
  else return kno_type_error("text encoding/#t/#f","unzip_prim",text);
}

/* Initialization */

KNO_EXPORT int kno_init_zlib(void) KNO_LIBINIT_FN;

static long long int zlib_init = 0;

static lispval zlib_module;

KNO_EXPORT int kno_init_zlib()
{
  if (zlib_init) return 0;

  zlib_init = u8_millitime();
  zlib_module = kno_new_cmodule("zlib",0,kno_init_zlib);

  link_local_cprims();

  kno_finish_module(zlib_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}



static void link_local_cprims()
{
  KNO_LINK_CPRIM("zlib/uncompress",zlib_uncompress_prim,3,zlib_module);
  KNO_LINK_CPRIM("zlib/compress",zlib_compress_prim,2,zlib_module);
}
