/* -*- Mode: C; -*- */

/* Copyright (C) 2007-2012 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"

#include <libu8/libu8io.h>
#include <libu8/u8convert.h>
#include <libu8/u8pathfns.h>

#include <sys/types.h>
#include <zlib.h>

static u8_condition zlibOutOfMemory=_("ZLIB out of memory");
static u8_condition zlibBufferError=_("ZLIB buffer error");
static u8_condition zlibBadErrorCode=_("ZLIB odd error code");
static u8_condition zlibDataError=_("Bad ZLIB input data");

static fdtype zlib_compress_prim(fdtype input_arg,fdtype level_arg)
{
  int level=((FD_FIXNUMP(level_arg))?(FD_FIX2INT(level_arg)):(9));
  Bytef *input; size_t input_len=0;
  Bytef *output; size_t output_len=0;
  int retval;
  if ((level<0)||(level>9)) level=9;
  if (FD_STRINGP(input_arg)) {
    input=FD_STRDATA(input_arg);
    input_len=FD_STRLEN(input_arg);}
  else if (FD_PACKETP(input_arg)) {
    input=FD_PACKET_DATA(input_arg);
    input_len=FD_PACKET_LENGTH(input_arg);}
  else return fd_type_error("string or packet","zip_prim",input_arg);
  output_len=input_len; output=u8_malloc(output_len);
  retval=compress2(output,&output_len,input,input_len,level);
  if (retval == Z_BUF_ERROR) {
    Bytef *newbuf=u8_malloc(input_len*2);
    if (!(newbuf)) {
      u8_free(output);
      return fd_err(u8_MallocFailed,"zip_prim",NULL,FD_VOID);}
    u8_free(output); output=newbuf; output_len=input_len*2;
    retval=compress2(output,&output_len,input,input_len,9);}
  if (retval>=Z_OK) 
    return fd_init_packet(NULL,output_len,output);
  else {
    u8_condition ex;
    switch (retval) {
    case Z_MEM_ERROR: ex=zlibOutOfMemory; break;
    case Z_BUF_ERROR: ex=zlibBufferError; break;
    default: ex=zlibBadErrorCode; break;}
    u8_free(output);
    return fd_err(ex,"zip_prim",NULL,FD_VOID);}
}

static fdtype zlib_uncompress_prim(fdtype input_arg,fdtype text,fdtype init_factor)
{
  int init_grow=((FD_FIXNUMP(init_factor))?(FD_FIX2INT(init_factor)):(5));
  Bytef *input; size_t input_len=0;
  Bytef *output; size_t output_len=0, buf_len=0;
  int retval;
  if ((init_grow<1)||(init_grow>20)) init_grow=5;
  input=FD_PACKET_DATA(input_arg);
  input_len=FD_PACKET_LENGTH(input_arg);
  output_len=buf_len=(input_len*init_grow);
  output=u8_malloc(buf_len);
  if (!(output)) return fd_err(u8_MallocFailed,"unzip_prim",NULL,FD_VOID);
  while ((retval=uncompress(output,&output_len,input,input_len))<Z_OK) {
    if (retval == Z_BUF_ERROR) {
      Bytef *newbuf=u8_malloc(buf_len*2);
      if (!(newbuf)) {
	u8_free(output);
	return fd_err(u8_MallocFailed,"unzip_prim",NULL,FD_VOID);}
      else {
	u8_free(output); output=newbuf;
	buf_len=buf_len*2; output_len=buf_len;}}
    else if (retval==Z_MEM_ERROR) {
      u8_free(output);
      return fd_err(zlibOutOfMemory,"unzip_prim",NULL,FD_VOID);}
    else if (retval==Z_DATA_ERROR) {
      u8_free(output);
      return fd_err(zlibDataError,"unzip_prim",NULL,FD_VOID);}
    else {
      u8_free(output);
      return fd_err(zlibBadErrorCode,"unzip_prim",NULL,FD_VOID);}}
  if ((FD_FALSEP(text))||(FD_VOIDP(text)))
    return fd_init_packet(NULL,output_len,output);
  else if (FD_TRUEP(text)) {
    if (u8_validate(output,output_len)==output_len) {
      if (buf_len>(output_len+1)) output[output_len]='\0';
      else {
	unsigned char *wnull=u8_realloc(output,output_len+1);
	if (!(wnull)) {
	  u8_free(output);
	  return fd_err(u8_MallocFailed,"unzip_prim",NULL,FD_VOID);}
	else {
	  if (wnull!=output) u8_free(output);
	  wnull[output_len]='\0'; output=wnull;}}
      return fd_init_string(NULL,output_len,output);}
    else return fd_init_packet(NULL,output_len,output);}
  else if (FD_STRINGP(text)) {
    u8_encoding enc=u8_get_encoding(FD_STRDATA(text));
    if (!(enc)) {
      u8_free(output);
      return fd_type_error("text encoding","unzip_prim",text);}
    else {
      int retval=0;
      unsigned char *scan=output, *limit=output+output_len;
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,output_len);
      retval=u8_convert(enc,0,&out,&scan,limit);
      if (retval<0) {
	u8_free(output); u8_free(out.u8_outbuf);
	return fd_err("malencoded text","unzip_prim",NULL,FD_VOID);}
      else {
	u8_free(output);
	return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}}}
  else return fd_type_error("text encoding/#t/#f","unzip_prim",text);
}

/* Initialization */

FD_EXPORT int fd_init_zlib(void) FD_LIBINIT_FN;

static int zlib_init=0;

FD_EXPORT int fd_init_zlib()
{
  fdtype zlib_module;
  if (zlib_init) return 0;

  zlib_init=1;
  zlib_module=fd_new_module("ZLIB",(FD_MODULE_SAFE));
  
  fd_idefn(zlib_module,
	   fd_make_cprim2("ZLIB/COMPRESS",zlib_compress_prim,1));

  fd_idefn(zlib_module,
	   fd_make_cprim3x("ZLIB/UNCOMPRESS",zlib_uncompress_prim,1,
			   fd_packet_type,FD_VOID,-1,FD_FALSE,-1,FD_FALSE));

  fd_finish_module(zlib_module);
  fd_persist_module(zlib_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

