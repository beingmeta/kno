/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_BUFIO 1
#define FD_INLINE_STREAMIO 1
#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/streams.h"
#include "framerd/storage.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>
#include <libu8/u8printf.h>
#include <libu8/libu8io.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <zlib.h>
#if HAVE_SNAPPYC_H
#include <snappy-c.h>
#endif
#if HAVE_ZSTD_H
#include <zstd.h>
#endif

#ifndef FD_INIT_ZBUF_SIZE
#define FD_INIT_ZBUF_SIZE 24000
#endif

#ifndef FD_DEFAULT_ZSTDLEVEL
#define FD_DEFAULT_ZSTDLEVEL 17
#endif

/* Getting compression type from options */

static lispval compression_symbol, snappy_symbol, none_symbol, no_symbol;
static lispval libz_symbol, zlib_symbol, zlib9_symbol, zstd_symbol;

#define DEFAULT_COMPRESSION FD_ZLIB

FD_EXPORT
fd_compress_type fd_compression_type(lispval opts,fd_compress_type dflt)
{
  if (FD_SYMBOLP(opts)) {
    if (opts == snappy_symbol)
    return FD_SNAPPY;
  else if (opts == zstd_symbol)
    return FD_ZSTD;
  else if ( (opts == zlib_symbol) || (opts == libz_symbol) )
    return FD_ZLIB;
  else if ( (opts == zlib9_symbol) ||
	    (opts == FD_INT(9)) )
    return FD_ZLIB9;
  else if ( (opts == FDSYM_NO) || (opts == FD_INT(0)) )
    return FD_NOCOMPRESS;
  else if (opts == snappy_symbol)
    return FD_SNAPPY;
  else return dflt;}
  else if (fd_testopt(opts,compression_symbol,FD_FALSE))
    return FD_NOCOMPRESS;
  else if (fd_testopt(opts,compression_symbol,snappy_symbol))
    return FD_SNAPPY;
  else if ( (fd_testopt(opts,compression_symbol,FD_TRUE)) ||
	    (fd_testopt(opts,compression_symbol,FD_DEFAULT_VALUE)) ||
	    (fd_testopt(opts,compression_symbol,FDSYM_DEFAULT)) )
    return dflt;
  else if (fd_testopt(opts,compression_symbol,zstd_symbol))
    return FD_ZSTD;
  else if ( (fd_testopt(opts,compression_symbol,zlib_symbol)) ||
	    (fd_testopt(opts,compression_symbol,libz_symbol)) )
    return FD_ZLIB;
  else if ( (fd_testopt(opts,compression_symbol,zlib9_symbol)) ||
	    (fd_testopt(opts,compression_symbol,FD_INT(9))) )
    return FD_ZLIB9;
  else if ( (fd_testopt(opts,compression_symbol,FDSYM_NO)) ||
	    (fd_testopt(opts,compression_symbol,FD_INT(0))) )
    return FD_NOCOMPRESS;
  else if ( (fd_testopt(opts,compression_symbol,FD_TRUE)) ||
	    (fd_testopt(opts,compression_symbol,FD_DEFAULT_VALUE)) ||
	    (fd_testopt(opts,compression_symbol,FDSYM_DEFAULT)) ) {
    if (dflt)
      return dflt;
    else return DEFAULT_COMPRESSION;}
  else return dflt;
}

/* no compression */

static unsigned char *just_copy
(ssize_t *destlen,const unsigned char *source,size_t source_len)
{
  unsigned char *copy = u8_big_alloc(source_len);
  if (copy == NULL) {
    u8_seterr(u8_MallocFailed,"fd_compress",NULL);
    return NULL;}
  memcpy(copy,source,source_len);
  *destlen = source_len;
  return copy;
}

/* libz */

static unsigned char *do_zuncompress
(const unsigned char *bytes,size_t n_bytes,
 ssize_t *dbytes,unsigned char *init_dbuf)
{
  u8_condition error = NULL; int zerror;
  unsigned long csize = n_bytes, dsize, dsize_max;
  Bytef *cbuf = (Bytef *)bytes, *dbuf;
  if (init_dbuf == NULL) {
    dsize = dsize_max = csize*4;
    dbuf = u8_big_alloc(dsize_max);}
  else {
    dbuf = init_dbuf;
    dsize = dsize_max = *dbytes;}
  while ((zerror = uncompress(dbuf,&dsize,cbuf,csize)) < Z_OK)
    if (zerror == Z_MEM_ERROR) {
      error=_("ZLIB ran out of memory"); break;}
    else if (zerror == Z_BUF_ERROR) {
      /* We don't use realloc because there's not point in copying
	 the data and we hope the overhead of free/malloc beats
	 realloc when we're doubling the buffer. */
      if (dbuf!=init_dbuf) u8_big_free(dbuf);
      dbuf = u8_big_alloc(dsize_max*2);
      if (dbuf == NULL) {
	error=_("pool value uncompress ran out of memory");
	break;}
      dsize = dsize_max = dsize_max*2;}
    else if (zerror == Z_DATA_ERROR) {
      error=_("ZLIB uncompress data error"); break;}
    else {
      error=_("Bad ZLIB return code"); break;}
  if (error == NULL) {
    *dbytes = dsize;
    return dbuf;}
  else {
    fd_seterr2(error,"do_zuncompress");
    if (dbuf != init_dbuf) u8_big_free(dbuf);
    return NULL;}
}

static U8_MAYBE_UNUSED unsigned char *do_zcompress
   (const unsigned char *bytes,size_t n_bytes,
    ssize_t *cbytes,unsigned char *init_cbuf,
    int level)
{
  u8_condition error = NULL; int zerror;
  uLongf dsize = n_bytes, csize, csize_max;
  Bytef *dbuf = (Bytef *)bytes, *cbuf;
  if (init_cbuf == NULL) {
    csize = csize_max = dsize;
    cbuf = u8_big_alloc(csize_max);}
  else {
    cbuf = init_cbuf;
    csize = csize_max = *cbytes;}
  while ((zerror = compress2(cbuf,&csize,dbuf,dsize,level)) < Z_OK)
    if (zerror == Z_MEM_ERROR) {
      error=_("ZLIB ran out of memory"); break;}
    else if (zerror == Z_BUF_ERROR) {
      /* We don't use realloc because there's no point in copying
	 the data and we hope the overhead of free/malloc beats
	 realloc when we're doubling the buffer size. */
      if (cbuf!=init_cbuf) u8_big_free(cbuf);
      cbuf = u8_big_alloc(csize_max*2);
      if (cbuf == NULL) {
	error=_("pool value compression ran out of memory"); break;}
      csize = csize_max = csize_max*2;}
    else if (zerror == Z_DATA_ERROR) {
      error=_("ZLIB compress data error"); break;}
    else {
      error=_("Bad ZLIB return code"); break;}
  if (error == NULL) {
    *cbytes = csize;
    return cbuf;}
  else {
    fd_seterr2(error,"do_zcompress");
    return NULL;}
}

/* Snappy */

static unsigned char *do_snappy_compress
(ssize_t *destlen,const unsigned char *source,size_t source_len)
{
  size_t max_compressed_length = snappy_max_compressed_length(source_len);
  size_t compressed_length = max_compressed_length;
  unsigned char *zbuf = u8_big_alloc(compressed_length);
  snappy_status compress_rv=
    snappy_compress(source,source_len,zbuf,&compressed_length);
  if (compress_rv == SNAPPY_OK) {
    if ((compressed_length*2) < (max_compressed_length)) {
      unsigned char *new_zbuf = u8_big_realloc(zbuf,compressed_length);
      if (new_zbuf) zbuf = new_zbuf;}
    *destlen = compressed_length;
    return zbuf;}
  else {
    u8_big_free(zbuf);
    return NULL;}
}

static unsigned char *do_snappy_uncompress
(ssize_t *destlen,const unsigned char *source,size_t source_len)
{
  size_t uncompressed_size;
  snappy_status size_rv =
    snappy_uncompressed_length(source,source_len,&uncompressed_size);
  if (size_rv == SNAPPY_OK) {
    unsigned char *uncompressed = u8_big_alloc(uncompressed_size);
    snappy_status uncompress_rv=
      snappy_uncompress(source,source_len,uncompressed,&uncompressed_size);
    if (uncompress_rv == SNAPPY_OK) {
      *destlen = uncompressed_size;
      return uncompressed;}
    else {
      u8_big_free(uncompressed);
      u8_seterr("SnappyUncompressFailed","do_snappy_uncompress",NULL);}
      return NULL;}
  else {
    u8_seterr("SnappyUncompressFailed","do_snappy_uncompress",NULL);
    return NULL;}
}

/* Snappy */

#define zstd_error(code) (u8_fromlibc((char *)ZSTD_getErrorName(code)))

static unsigned char *do_zstd_compress
(ssize_t *destlen,const unsigned char *source,size_t source_len,
 int level,void *state)
{
  size_t max_compressed_length = ZSTD_compressBound(source_len);
  if (ZSTD_isError(max_compressed_length)) {
    u8_seterr("ZSTD_CompressError/size","do_zstd_compress",
	      zstd_error(max_compressed_length));
    return NULL;}
  unsigned char *zbuf = u8_big_alloc(max_compressed_length);
  size_t compressed_length=
    ZSTD_compress(zbuf,max_compressed_length,source,source_len,level);
  if (ZSTD_isError(compressed_length)) {
    u8_seterr("ZSTD_CompressError","do_zstd_compress",
	      zstd_error(compressed_length));
    u8_big_free(zbuf);
    return NULL;}
  else {
    if ((compressed_length*2) < (max_compressed_length)) {
      unsigned char *new_zbuf = u8_big_realloc(zbuf,compressed_length);
      if (new_zbuf) zbuf = new_zbuf;}
    *destlen = compressed_length;
    return zbuf;}
}

static unsigned char *do_zstd_uncompress
(ssize_t *destlen,const unsigned char *source,size_t source_len,void *state)
{
#if HAVE_ZSTD_GETFRAMECONTENTSIZE
  size_t alloc_size = ZSTD_getFrameContentSize(source,source_len);
  if (PRED_FALSE(alloc_size == ZSTD_CONTENTSIZE_UNKNOWN)) {
    u8_seterr("UnknownContentSize","do_zstd_uncompress",
	      zstd_error(alloc_size));
    return NULL;}
  else if (PRED_FALSE(alloc_size == ZSTD_CONTENTSIZE_ERROR)) {
    u8_seterr("ZSTD_ContentSizeError","do_zstd_uncompress",
	      zstd_error(alloc_size));
    return NULL;}
#else
  size_t alloc_size = ZSTD_getDecompressedSize(source,source_len);
#endif
  unsigned char *uncompressed = u8_big_alloc(alloc_size);
  size_t uncompressed_size =
    ZSTD_decompress(uncompressed,alloc_size,source,source_len);
  if (ZSTD_isError(uncompressed_size)) {
    u8_seterr("ZSTD_UncompressError","do_zstd_uncompress",
	      zstd_error(uncompressed_size));
    u8_big_free(uncompressed);
    return NULL;}
  else {
    if ( (uncompressed_size*2) < alloc_size) {
      unsigned char *new_data = u8_big_realloc(uncompressed,uncompressed_size);
      if (new_data) uncompressed = new_data;}
    *destlen = uncompressed_size;
    return uncompressed;}
}

/* Exported compression functions */

FD_EXPORT unsigned char *fd_compress
(fd_compress_type ctype,ssize_t *result_size,
 const unsigned char *source,size_t source_len,
 void *state)
{
  switch (ctype) {
  case FD_NOCOMPRESS:
    return just_copy(result_size,source,source_len);
  case FD_ZLIB:
    return do_zcompress(source,source_len,result_size,NULL,6);
  case FD_ZLIB9:
    return do_zcompress(source,source_len,result_size,NULL,9);
  case FD_SNAPPY:
    return do_snappy_compress(result_size,source,source_len);
  case FD_ZSTD:
    return do_zstd_compress(result_size,source,source_len,
			    FD_DEFAULT_ZSTDLEVEL,state);
  default:
    u8_seterr("BadCompressMethod","fd_compress",NULL);
    return NULL;}
}

FD_EXPORT unsigned char *fd_uncompress
(fd_compress_type ctype,ssize_t *result_size,
 const unsigned char *source,size_t source_len,
 void *state)
{
  switch (ctype) {
  case FD_NOCOMPRESS:
    return just_copy(result_size,source,source_len);
  case FD_ZLIB:
    return do_zuncompress(source,source_len,result_size,NULL);
  case FD_ZLIB9:
    return do_zuncompress(source,source_len,result_size,NULL);
  case FD_SNAPPY:
    return do_snappy_uncompress(result_size,source,source_len);
  case FD_ZSTD:
    return do_zstd_uncompress(result_size,source,source_len,state);
  default:
    u8_seterr("BadCompressMethod","fd_compress",NULL);
    return NULL;}
}

/* Initialization */

static time_t compress_c_initialized = 0;

FD_EXPORT void fd_init_compress_c()
{
  if (compress_c_initialized) return;
  else compress_c_initialized = time(NULL);

  compression_symbol = fd_intern("COMPRESSION");
  snappy_symbol = fd_intern("SNAPPY");
  zlib_symbol = fd_intern("ZLIB");
  zlib9_symbol = fd_intern("ZLIB9");
  libz_symbol = fd_intern("LIBZ");
  zstd_symbol = fd_intern("ZSTD");
  no_symbol = fd_intern("NO");
  none_symbol = fd_intern("NONE");

  u8_register_source_file(_FILEINFO);
}
