#include <zlib.h>

#if HAVE_SNAPPYC_H
#include <snappy-c.h>
#endif

#ifndef KNO_INIT_ZBUF_SIZE
#define KNO_INIT_ZBUF_SIZE 24000
#endif

/* Compression functions */

static U8_MAYBE_UNUSED unsigned char *do_zuncompress
   (const unsigned char *bytes,size_t n_bytes,
    ssize_t *dbytes,unsigned char *init_dbuf)
{
  u8_condition error = NULL; int zerror;
  unsigned long csize = n_bytes, dsize, dsize_max;
  Bytef *cbuf = (Bytef *)bytes, *dbuf;
  if (init_dbuf == NULL) {
    dsize = dsize_max = csize*4;
    dbuf = u8_malloc(dsize_max);}
  else {
    dbuf = init_dbuf; dsize = dsize_max = *dbytes;}
  while ((zerror = uncompress(dbuf,&dsize,cbuf,csize)) < Z_OK)
    if (zerror == Z_MEM_ERROR) {
      error=_("ZLIB ran out of memory"); break;}
    else if (zerror == Z_BUF_ERROR) {
      /* We don't use realloc because there's not point in copying
         the data and we hope the overhead of free/malloc beats
         realloc when we're doubling the buffer. */
      if (dbuf!=init_dbuf) u8_free(dbuf);
      dbuf = u8_malloc(dsize_max*2);
      if (dbuf == NULL) {
        error=_("pool value uncompress ran out of memory"); break;}
      dsize = dsize_max = dsize_max*2;}
    else if (zerror == Z_DATA_ERROR) {
      error=_("ZLIB uncompress data error"); break;}
    else {
      error=_("Bad ZLIB return code"); break;}
  if (error == NULL) {
    *dbytes = dsize;
    return dbuf;}
  else {
    kno_seterr2(error,"do_zuncompress");
    if (dbuf != init_dbuf) u8_free(dbuf);
    return NULL;}
}

static U8_MAYBE_UNUSED unsigned char *do_zcompress
   (unsigned char *bytes,size_t n_bytes,
    ssize_t *cbytes,unsigned char *init_cbuf,
    int level)
{
  u8_condition error = NULL; int zerror;
  uLongf dsize = n_bytes, csize, csize_max;
  Bytef *dbuf = (Bytef *)bytes, *cbuf;
  if (init_cbuf == NULL) {
    csize = csize_max = dsize;
    cbuf = u8_malloc(csize_max);}
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
      if (cbuf!=init_cbuf) u8_free(cbuf);
      cbuf = u8_malloc(csize_max*2);
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
    kno_seterr2(error,"do_zcompress");
    return NULL;}
}
