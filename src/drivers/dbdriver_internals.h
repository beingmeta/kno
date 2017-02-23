#include <zlib.h>

#define STREAM_UNLOCKED 0
#define STREAM_LOCKED 1

/* Getting chunk refs */

typedef long long int ll;
typedef unsigned long long ull;

static FD_CHUNK_REF read_chunk_ref(struct FD_STREAM *stream,
                                   fd_off_t base,fd_offset_type offtype,
                                   unsigned int offset);

static FD_CHUNK_REF get_chunk_ref(struct FD_STREAM *stream,
				  unsigned int *offsets,
				  fd_off_t base,fd_offset_type offtype,
				  unsigned int offset,
				  unsigned int stream_locked)
{
  FD_CHUNK_REF result;
  if (offsets) {
    switch (offtype) {
    case FD_B32: {
#if ((HAVE_MMAP) && (!(WORDS_BIGENDIAN)))
      unsigned int word1=fd_flip_word((offsets)[offset*2]);
      unsigned int word2=fd_flip_word((offsets)[offset*2+1]);
#else
      unsigned int word1=(offsets)[offset*2];
      unsigned int word2=(offsets)[offset*2+1];
#endif
      result.off=word1; result.size=word2;
      break;}
    case FD_B40: {
#if ((HAVE_MMAP) && (!(WORDS_BIGENDIAN)))
      unsigned int word1=fd_flip_word((offsets)[offset*2]);
      unsigned int word2=fd_flip_word((offsets)[offset*2+1]);
#else
      unsigned int word1=(offsets)[offset*2];
      unsigned int word2=(offsets)[offset*2+1];
#endif
      result.off=(((((ull)(word2))&(0xFF000000))<<8)|((ull)word1));
      result.size=((word2)&(0x00FFFFFF));
      break;}
    case FD_B64: {
#if ((HAVE_MMAP) && (!(WORDS_BIGENDIAN)))
      unsigned int word1=fd_flip_word((offsets)[offset*3]);
      unsigned int word2=fd_flip_word((offsets)[offset*3+1]);
      unsigned int word3=fd_flip_word((offsets)[offset*3+2]);
#else
      unsigned int word1=(offsets)[offset*3];
      unsigned int word2=(offsets)[offset*3+1];
      unsigned int word3=(offsets)[offset*3+2];
#endif
      result.off=(fd_off_t) ((((ll)word1)<<32)|(((ll)word2)));
      result.size=(size_t) word3;
      break;}
    default:
      u8_seterr(fd_InvalidOffsetType,"get_chunkref",NULL);
      result.off=-1; result.size=-1;} /* switch (offtype) */
  } /* if (offsets) */
  else if ((stream)&&(stream_locked)) 
    result=read_chunk_ref(stream,256,offtype,offset);
  else if (stream) {
    u8_lock_mutex(&(stream->stream_lock));
    result=read_chunk_ref(stream,256,offtype,offset);
    u8_unlock_mutex(&(stream->stream_lock));}
  else {
    u8_seterr("NoStream","get_chunkref",NULL);
    result.off=-1; result.size=-1;}
  return result;
}

static int chunk_ref_size(fd_offset_type offtype)
{
  switch (offtype) {
  case FD_B32: case FD_B40: return 8;
  case FD_B64: return 12;}
  return -1;
}

static FD_CHUNK_REF read_chunk_ref(struct FD_STREAM *stream,
                                   fd_off_t base,fd_offset_type offtype,
				   unsigned int offset)
{
  FD_CHUNK_REF result; int chunk_size = chunk_ref_size(offtype);
  fd_off_t ref_off = offset*chunk_size;
  if ( (fd_setpos(stream,base+ref_off)) < 0 ) {
    result.off=(fd_off_t)-1; result.size=(size_t)-1;}
  else {
    fd_inbuf in=fd_readbuf(stream);
    switch (offtype) {
    case FD_B32:
      result.off=fd_read_4bytes(in);
      result.size=fd_read_4bytes(in);
      break;
    case FD_B40: {
      unsigned int word1, word2;
      word1=fd_read_4bytes(in);
      word2=fd_read_4bytes(in);
      result.off=((((ll)((word2)&(0xFF000000)))<<8)|word1);
      result.size=(ll)((word2)&(0x00FFFFFF));
      break;}
    case FD_B64:
      result.off=fd_read_8bytes(in);
      result.size=fd_read_4bytes(in);
      break;
    default:
      u8_seterr("Invalid Offset type","read_chunk_ref",NULL);
      result.off=-1;
      result.size=-1;} /* switch (p->fdb_offtype) */
  }
  return result;
}

static int convert_FD_B40_ref(FD_CHUNK_REF ref,
                              unsigned int *word1,
                              unsigned int *word2)
{
  if (ref.size>=0x1000000) return -1;
  else if (ref.off<0x100000000LL) {
    *word1=(ref.off)&(0xFFFFFFFFLL);
    *word2=ref.size;}
  else {
    *word1=(ref.off)&(0xFFFFFFFFLL);
    *word2=ref.size|(((ref.off)>>8)&(0xFF000000LL));}
  return 0;
}

static unsigned char *read_chunk(fd_stream stream,
                                 unsigned char *mmap,
                                 fd_off_t off,uint size,
                                 uchar *usebuf,
                                 u8_mutex *unlock)
{
  uchar *buf = (usebuf) ? (usebuf) : (u8_malloc(size)) ;
  if (mmap) {
    memcpy(buf,mmap+off,size);
    if (unlock) u8_unlock_mutex(unlock);
    return buf;}
  else {
    fd_inbuf in=fd_start_read(stream,off);
    int bytes_read = (in) ? (fd_read_bytes(buf,in,size)) : (-1);
    if (unlock) u8_unlock_mutex(unlock);
    if (bytes_read<0) {
      if (usebuf==NULL) u8_free(buf);
      return NULL;}
    else return buf;}
}

/* Compression functions */

static U8_MAYBE_UNUSED unsigned char *do_zuncompress
   (unsigned char *bytes,int n_bytes,
    unsigned int *dbytes,
    unsigned char *init_dbuf)
{
  fd_exception error=NULL; int zerror;
  unsigned long csize=n_bytes, dsize, dsize_max;
  Bytef *cbuf=(Bytef *)bytes, *dbuf;
  if (init_dbuf==NULL) {
    dsize=dsize_max=csize*4; dbuf=u8_malloc(dsize_max);}
  else {
    dbuf=init_dbuf; dsize=dsize_max=*dbytes;}
  while ((zerror=uncompress(dbuf,&dsize,cbuf,csize)) < Z_OK)
    if (zerror == Z_MEM_ERROR) {
      error=_("ZLIB ran out of memory"); break;}
    else if (zerror == Z_BUF_ERROR) {
      /* We don't use realloc because there's not point in copying
         the data and we hope the overhead of free/malloc beats
         realloc when we're doubling the buffer. */
      if (dbuf!=init_dbuf) u8_free(dbuf);
      dbuf=u8_malloc(dsize_max*2);
      if (dbuf==NULL) {
        error=_("OIDPOOL uncompress ran out of memory"); break;}
      dsize=dsize_max=dsize_max*2;}
    else if (zerror == Z_DATA_ERROR) {
      error=_("ZLIB uncompress data error"); break;}
    else {
      error=_("Bad ZLIB return code"); break;}
  if (error==NULL) {
    *dbytes=dsize;
    return dbuf;}
  else {
    fd_seterr(error,"do_zuncompress",NULL,FD_VOID);
    return NULL;}
}

static U8_MAYBE_UNUSED unsigned char *do_zcompress
   (unsigned char *bytes,int n_bytes,
    int *cbytes,unsigned char *init_cbuf,
    int level)
{
  fd_exception error=NULL; int zerror;
  unsigned long dsize=n_bytes, csize, csize_max;
  Bytef *dbuf=(Bytef *)bytes, *cbuf;
  if (init_cbuf==NULL) {
    csize=csize_max=dsize; cbuf=u8_malloc(csize_max);}
  else {
    cbuf=init_cbuf; csize=csize_max=*cbytes;}
  while ((zerror=compress2(cbuf,&csize,dbuf,dsize,level)) < Z_OK)
    if (zerror == Z_MEM_ERROR) {
      error=_("ZLIB ran out of memory"); break;}
    else if (zerror == Z_BUF_ERROR) {
      /* We don't use realloc because there's no point in copying
         the data and we hope the overhead of free/malloc beats
         realloc when we're doubling the buffer size. */
      if (cbuf!=init_cbuf) u8_free(cbuf);
      cbuf=u8_malloc(csize_max*2);
      if (cbuf==NULL) {
        error=_("OIDPOOL compress ran out of memory"); break;}
      csize=csize_max=csize_max*2;}
    else if (zerror == Z_DATA_ERROR) {
      error=_("ZLIB compress data error"); break;}
    else {
      error=_("Bad ZLIB return code"); break;}
  if (error==NULL) {
    *cbytes=csize;
    return cbuf;}
  else {
    fd_seterr(error,"do_zcompress",NULL,FD_VOID);
    return NULL;}
}

