#include "fdb/dtype.h"
#include "fdb/fddb.h"
#include "fdb/dbfile.h"

#include <zlib.h>

#define FD_METADATA_POS_POS      0x20
#define FD_OIDPOOL_FETCHBUF_SIZE 4096

/* OIDPOOLs are the next generation of object pool data file.  While 
    previous formats have all stored OIDs for years and years, OIDPOOLs
    are supposed to be the best to date and somewhat paradigmatic.
   The design focuses on performance and extensibility.  With FD_HASHINDEX,
    it became clear that storing offset sizes helped with performance and
    with the use of compression, it becomes even more relevant.
   OIDPOOLs are also the first native files to support files >4GB and
    use three different offset models, described below.
*/

/* Layout of new OIDPOOL files
   [256 bytes of header]
   [offset table]
   ...data items...

   Header consists of

   0x00 XXXX     Magic number
   0x04 XXXX     Pool information bits/flags
   0x08 XXXX     Base OID of pool (8 bytes
        XXXX
   0x10 XXXX     Capacity of pool
   0x14 XXXX     Load of pool
   0x18 XXXX     file offset of the pool label (8 bytes)
        XXXX
   0x20 XXXX     file offset of pool metadata (8 bytes)
        XXXX
   0x28 XXXX     file offset of the schemas record (8 bytes)
        XXXX
   0x30 XXXX     pool creation time_t (8 bytes)
        XXXX
   0x38 XXXX     pool repack time_t (8 bytes)
        XXXX
   0x40 XXXX     pool modification time_t (8 bytes)
        XXXX
   0x48 XXXX     repack generation (8 bytes)
        XXXX

   The flags are divided as follows:
     MASK        INTERPRETATION
     0x0003      Structure of offsets table:
                    0= 4 bytes of position followed by 4 bytes of length (32B form)
                    1= 5 bytes of position followed by 3 bytes of length (40B form)
                    2= 8 bytes of position followed by 4 bytes of length (64B form)
                    3= reserved for future use
     0x001c      Compression function for blocks:
                    0= no compression
		    1= libz compression
		    2= libbz2 compression
                    3-7 reserved for future use
     0x0020      Set if this pool is intended to be read-only

   The offsets block starts at 0x100 and goes for either capacity*8 or capacity*16
    bytes.  The offset values are stored as pairs of big-endian binary representations.
    For the 32B and 64B forms, these are just straightforward integers of the same size.
    For the 40B form, which is designed to better use memory and cache, the high 8
    bits of the second word are taken as the high eight bits of a forty-byte offset.

*/

typedef long long int ll;

static FD_CHUNK_REF get_chunk_ref(struct FD_OIDPOOL *p,unsigned int offset)
{
  FD_CHUNK_REF result;
  if (p->offsets) {
    switch (p->offset_type) {
    case FD_B32:
      result.off=(p->offsets)[offset*2];
      result.size=(p->offsets)[offset*2+1];
      break;
    case FD_B40: {
      unsigned int word1=(p->offsets)[offset*2];
      unsigned int word2=(p->offsets)[offset*2];
      result.off=((((ll)((word2)&(0xFF000000)))<<8)|word1);
      result.size=(ll)((word2)&(0x00FFFFFF));
      break;}
    case fd_B64: 
      result.off=(off_t)
	((((ll)((p->offsets)[offset*4]))<<32)|
	 (((ll)(p->offsets)[offset*4+1])));
      result.size=(size_t)((p->offsets)[offset*4+3]);
      break;}
#if ((HAVE_MMAP) && (!(FD_WORDS_BIGENDIAN)))
    result.off=fd_flip_word8(result.off);
    result.size=fd_flip_word(result.size);
#endif
  }
  else {
    int error=0;
    fd_dtype_stream stream=&(p->stream);
    switch (p->offset_type) {
    case FD_B32:
      if (fd_setpos(stream,256+offset*8)<0) error=1;
      result.off=fd_dtsread_off_t(stream);
      result.size=fd_dtsread_4bytes(stream);
      break;
    case FD_B40: {
      unsigned int word1, word2;
      if (fd_setpos(stream,256+offset*8)<0) error=1;
      word1=fd_dtstread_4bytes(stream);
      word2=fd_dtsread_4bytes(stream);
      result.off=((((ll)((word2)&(0xFF000000)))<<8)|word1);
      result.size=(ll)((word2)&(0x00FFFFFF));
      break;}
    case FD_B64: 
      if (fd_setpos(stream,256+offset*8)<0) error=1;
      result.off=fd_dtsread_off8_t(stream);
      result.size=fd_dtsread_4bytes(stream);
      break;
    default:
      fd_warn("Invalid OIDPOOL","Invalid offset type for %s: 0x%x",
	      p->cid,p->offset_type);
      result.off=-1;
      result.size=-1;}
    if (error) {
      result.off=(off_t)-1; result.size=(size_t)-1;}}
  fd_unlock_mutex(&(p->lock));
  return result;
}

static unsigned char *read_chunk(fd_oidool p,off_t off,uint size,uchar *buf)
{
  if (p->mmap) {
    if (buf==NULL) {
      buf=u8_malloc(size); mallocd=1;}
    memcpy(buf,p->mmap+off,size);
    return buf;}
  else {
    fd_dtype_stream stream=&(p->stream); int bytes_read, mallocd=0;
    if (buf==NULL) {
      buf=u8_malloc(size); mallocd=1;}
    fd_lock_mutex(&(p->lock));
    fd_setpos(&(p->stream),off);
    bytes_read=fd_dtsread_bytes(stream,buf,(int)size);
    fd_unlock_mutex(&(p->lock));
    if (bytes_read<0) {
      if (mallocd) u8_free(buf);
      return NULL;}
    else return buf;}
}

/* Compression functions */

static unsigned char *do_zuncompress
   (unsigned char *bytes,int n_bytes,int *dbytes,unsigned char *init_dbuf)
{
  fd_exception error=NULL;
  int csize=n_bytes, dsize, dsize_max;
  Bytef *cbuf=(Bytef *)bytes, *dbuf;
  if (init_dbuf==NULL) {
    dsize=dsize_max=csize*4; dbuf=u8_malloc(dsize_max);}
  else {
    dbuf=init_dbuf; dsize=dsize_max=*dbytes;}
  while ((error=uncompress(dbuf,&dsize,cbuf,csize)) < Z_OK)
    if (error == Z_MEM_ERROR) {
      error=_("ZLIB ran out of memory"); break;}
    else if (error == Z_BUF_ERROR) {
      /* We don't use realloc because there's not point in copying
	 the data and we hope the overhead of free/malloc beats
	 realloc when we're doubling the buffer. */
      if (dbuf!=init_dbuf) u8_free(dbuf);
      dbuf=u8_malloc(dsize_max*2);
      if (dbuf==NULL) {
	error=_("OIDPOOL uncompress ran out of memory"); break;}
      dsize=dsize_max=dsize_max*2;}
    else if (error == Z_DATA_ERROR) {
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

static unsigned char *do_zcompress
   (unsigned char *bytes,int n_bytes,int *cbytes,unsigned char *init_cbuf)
{
  fd_exception error=NULL;
  int dsize=n_bytes, csize, csize_max;
  Bytef *dbuf=(Bytef *)bytes, *cbuf;
  if (init_cbuf==NULL) {
    csize=csize_max=dsize; cbuf=u8_malloc(csize_max);}
  else {
    cbuf=init_cbuf; csize=csize_max=*cbytes;}
  while ((error=compress(cbuf,&csize,dbuf,dsize)) < Z_OK)
    if (error == Z_MEM_ERROR) {
      error=_("ZLIB ran out of memory"); break;}
    else if (error == Z_BUF_ERROR) {
      /* We don't use realloc because there's not point in copying
	 the data and we hope the overhead of free/malloc beats
	 realloc when we're doubling the buffer size. */
      if (cbuf!=init_cbuf) u8_free(cbuf);
      cbuf=u8_malloc(csize_max*2);
      if (cbuf==NULL) {
	error=_("OIDPOOL compress ran out of memory"); break;}
      csize=csize_max=csize_max*2;}
    else if (error == Z_DATA_ERROR) {
      error=_("ZLIB compress data error"); break;}
    else {
      error=_("Bad ZLIB return code"); break;}
  if (error=NULL) {
    *cbytes=csize;
    return cbuf;}
  else {
    fd_seterr(error,"do_zcompress",NULL,FD_VOID);
    return NULL;}
}

/* Making and opening oidpools */

static fd_pool open_oidpool(u8_string fname,int read_only)
{
  struct FD_OIDPOOL *pool=u8_malloc(sizeof(struct FD_OIDPOOL));
  struct FD_DTYPE_STREAM *s=&(pool->stream);
  FD_OID base;
  unsigned int hi, lo, magicno, capacity, load, flags;
  off_t label_loc, md_loc, schemas_loc; fdtype label;
  u8_string rname=u8_realpath(fname,NULL);
  fd_dtstream_mode mode=
    ((read_only) ? (FD_DTSTREAM_READ) : (FD_DTSTREAM_MODIFY));
  fd_init_dtype_file_stream(stream,fname,mode,FD_FILEDB_BUFSIZE,NULL,NULL);
  /* See if it ended up read only */
  if ((((pool)->stream).flags)&FD_DTSTREAM_READ_ONLY) read_only=1;
  pool->stream.mallocd=0;
  magicno=fd_dtsread_4bytes(s); flags=fd_dstread_4bytes;
  pool->dbflags=flags;
  if ((read_only==0) && ((flags)&(FD_OIDPOOL_READONLY))) {
    /* If the pool is intrinsically read-only make it so. */
    read_only=1; fd_dtsclose(stream);
    fd_init_dtype_file_stream
      (stream,fname,FD_DTSTREAM_READ,FD_FILEDB_BUFSIZE,NULL,NULL);}
  pool->offset_type=(fd_offset_type)((flags)&(FD_OIDPOOL_OFFMODE));
  pool->compress_type=
    (fd_compression_type)(((flags)&(FD_OIDPOOL_COMPRESSION))>>3);
  /* Read POOL base etc. */
  hi=fd_dtsread_4bytes(s); lo=fd_dtsread_4bytes(s);
  FD_SET_OID_HI(base,hi); FD_SET_OID_LO(base,lo);
  capacity=fd_dtsread_4bytes(s); load=fd_dtsread_4bytes(s);
  fd_init_pool((fd_pool)pool,base,capacity,&file_pool_handler,fname,rname);
  p->load=load;
  u8_free(rname); /* Done with this */
  if (magicno==FD_FILE_POOL_TO_RECOVER) {
    u8_warn(fd_RecoveryRequired,"Recovering the file pool %s",fname);
    if (recover_oidpool(pool)<0) {
      fd_seterr(fd_MallocFailed,"open_file_pool",NULL,FD_VOID);
      return NULL;}}
  /* Get the label */
  label_loc=fd_dtsread_off_t(s);
  if (label_loc) {
    if (fd_setpos(s,label_loc)>0) {
      label=fd_dtsread_dtype(s);
      if (FD_STRINGP(label)) pool->label=u8_strdup(FD_STRDATA(label));
      else u8_warn(fd_BadFilePoolLabel,fd_dtype2string(label));
      fd_decref(label);}
    else {
      fd_seterr(fd_BadFilePoolLabel,"open_oidpool",
		u8_strdup("bad label loc"),
		FD_INT2DTYPE(label_loc));
      fd_dtsclose(&(pool->stream),1);
      u8_free(rname); u8_free(pool);
      return NULL;}}
  /* Skip the metadata field */
  fd_dtsread_off_t(stream);
  /* Offsets size is the malloc'd size (in unsigned ints) of the offsets.
     We don't feel this in until we actually need it. */
  pool->offsets=NULL; pool->offsets_size=0;
  pool->read_only=read_only;
  fd_init_mutex(&(pool->lock));
  update_modtime(pool);
  return (fd_pool)pool;
}

static void update_modtime(struct FD_FILE_POOL *fp)
{
  struct stat fileinfo;
  if ((fstat(fp->stream.fd,&fileinfo))<0)
    fp->modtime=(time_t)-1;
  else fp->modtime=fileinfo.st_mtime;
}


static int lock_oidpool(struct FD_OIDPOOL *fp,int use_mutex)
{
  if (FD_OIDPOOL_LOCKED(fp)) return 1;
  else if ((fp->stream.flags)&(FD_DTSTREAM_READ_ONLY)) return 0;
  else {
    struct FD_DTYPE_STREAM *s=&(fp->stream);
    struct stat fileinfo;
    if (use_mutex) fd_lock_mutex(&(fp->lock));
    /* Handle race condition by checking when locked */
    if (FD_FILE_POOL_LOCKED(fp)) {
      if (use_mutex) fd_unlock_mutex(&(fp->lock));
      return 1;}
    if (fd_dtslock(s)==0) {
      fd_unlock_mutex(&(fp->lock));
      return 0;}
    fstat(s->fd,&fileinfo);
    if (fileinfo.st_mtime>fp->modtime) {
      /* Make sure we're up to date. */
      if (fp->offsets) reload_file_pool_cache(fp,0);
      else {
	fd_reset_hashtable(&(fp->cache),-1,1);
	fd_reset_hashtable(&(fp->locks),32,1);}}
    if (use_mutex) fd_unlock_mutex(&(fp->lock));
    return 1;}
}

/* Methods */

static int oidpool_load(fd_pool p)
{
  fd_oidpool fp=(fd_oidpool)p;
  if (FD_OIDPOOL_LOCKED(fp)) return fp->load;
  else {
    int load;
    fd_lock_mutex(&(fp->lock));
    if (fd_setpos(&(fp->stream),16)<0) {
      fd_unlock_mutex(&(fp->lock));
      return -1;}
    load=fd_dtsread_4bytes(&(fp->stream));
    fp->load=load;
    fd_unlock_mutex(&(fp->lock));
    return load;}
}

static fdtype read_oid_value(fd_oidpool op,fd_byte_input in)
{
  int zip_code;
  zip_code=fd_dtsread_zint(in);
  if (FD_EXPECT_FALSE(zip_code>(op->n_schemas)))
    return fd_err(fd_InvalidSchemaRef,"oidpool_fetch",op->cid,FD_VOID);
  else if (zip_code==0)
    return fd_read_dtype(in,NULL);
  else {
    struct FD_SCHEMA_ENTRY *se=op->schemas[zip_code+1];
    int n_vals=fd_dtsread_zint(in), n_slotids=se->n_slotids;
    if (FD_EXPECT_TRUE(n_vals==n_slotids)) {
      fdtype *values=u8_malloc(sizeof(fdtype)*n_vals), *write=values;
      fdtype *limit=values+n_vals;
      while (write<limit) *write++=fd_read_dtype(in,NULL);
      return fd_make_schemap(NULL,n_vals,0,
			     se->schema,values,NULL);}
    else return fd_err(fd_SchemaInconsistency,cxt,op->cid,FD_VOID);}
}

static fdtype read_oid_value_at(fd_oidpool op,FD_CHUNK_REF ref)
{
  if ((op->compression==FD_NOCOMPRESS) && (op->mmap)) {
      FD_BYTE_INPUT in;
      FD_INIT_BYTE_INPUT(&in,op->mmap+ref.off,ref.size);
      return read_oid_value(op,&in);}
  else {
    unsigned char _buf[FD_OIDPOOL_FETCHBUF_SIZE], *buf; int free_buf=0;
    if (op->mmap) buf=op->mmap+ref.off;
    else if (ref.size>FD_OIDPOOL_FETCHBUF_SIZE) {
      buf=read_chunk(op,ref.off,ref.size,NULL); free_buf=1;}
    else buf=read_chunk(op,ref.off,ref.size,_buf);
    if (buf==NULL) return fd_erreify();
    else if (op->compression=FD_NOCOMPRESS)
      if (free_buf) {
	FD_BYTE_INPUT in;
	FD_INIT_BYTE_INPUT(&in,buf,off.size);
	fdtype result=read_oid_value(op,&in);
	u8-free(buf);
	return result;}
      else {
	FD_BYTE_INPUT in;
	FD_INIT_BYTE_INPUT(&in,buf,off.size);
	return read_oid_value(op,&in);}
    else {
      unsigned char _ubuf[FD_OIDPOOL_FETCHBUF_SIZE*3], *ubuf;
      int ubuf_size;
      if (ref.size>(FD_OIDPOOL_FETCHBUF_SIZE*3)) {
	ubuf=u8_malloc(3*ref.size); ubuf_size=3*ref.size;}
      else {
	ubuf=_ubuf; ubuf_size=(FD_OIDPOOL_FETCHBUF_SIZE*3);}
      switch (op->compression) {
      case FD_ZLIB:
	ubuf=do_zuncompress(); break;
      default:
	if (free_buf) u8_free(buf);
	if (ubuf!=_ubuff) u8_free(ubuf);
	return fd_err(_("Bad compress level"),"oidpool_fetch",op->cid,
		      FD_VOID);}
      if (ubuf==NULL) {
	if (free_buf) u8_free(buf);
	if (ubuf!=_ubuff) u8_free(ubuf);
	return fd_erreify();}
      else if ((free_buf) || (ubuf!=_ubuff)) {
	FD_BYTE_INPUT in; fdtype result;
	FD_INIT_BYTE_INPUT(&in,ubuf,ubuf_size);	  
	result=read_oid_value(&in,NULL);
	if (free_buf) u8_free(buf);
	if (ubuf!=_ubuff) u8_free(ubuf);
	return result;}
      else {
	FD_BYTE_INPUT in;
	FD_INIT_BYTE_INPUT(&in,ubuf,ubuf_size);	  
	return read_oid_value(&in,NULL);}}}
}

static fdtype oidpool_fetch(fd_pool p,fdtype oid)
{
  fdtype value; fd_oidpool op=(fd_oidpool)p;
  FD_OID addr=FD_OID_ADDR(oid);
  int offset=FD_OID_DIFFERENCE(addr,op->base);
  if (FD_EXPECT_FALSE(offset>=op->load)) {
    /* Double check by going to disk */
    if (offset>=(oidpool_load(op))) {
      fd_unlock_mutex(&(fp->lock));
      return fd_err(fd_UnallocatedOID,"file_pool_fetch",fp->cid,oid);}}
  else {
    FD_CHUNK_REF ref=get_chunk_ref(op,offset);
    if (ref.off<0) return fd_erreify();
    else if (ref.off==0)
      return FD_EMPTY_CHOICE;
    else return read_oid_value_at(op,ref);}
}

struct OIDPOOL_FETCH_SCHEDULE {
  unsigned int value_at; FD_CHUNK_REF location;};

static int compare_offsets(const void *x1,const void *x2)
{
  const struct OIDPOOL_FETCH_SCHEDULE *s1=x1, *s2=x2;
  if (s1->location.off<s2->location.off) return -1;
  else if (s1->location.off>s2->location.off) return 1;
  else return 0;
}

static fdtype *oidpool_fetchn(fd_pool p,int n,fdtype *oids)
{
  fd_oidpool op=(fd_oidpool)p; FD_OID base=p->base;
  fdtype *values=u8_malloc(sizeof(fdtype)*n);
  if (fp->offsets==NULL) {
    /* Don't bother being clever if you don't even have an offsets
       table.  This could be fixed later for small memory implementations. */
    int i=0; while (i<n) {
      values[i]=oidpool_fetch(p,oids[i]); i++;}
    return values;}
  else {
    struct FD_DTYPE_STREAM *stream=&(op->stream);
    struct OIDPOOL_FETCH_SCHEDULE *schedule=
      u8_malloc(sizeof(struct POOL_FETCH_SCHEDULE)*n);
    int i=0; while (i<n) {
      fdtype oid=oids[i]; FD_OID addr=FD_OID_ADDR(oid);
      unsigned int off=FD_OID_DIFFERENCE(addr,base);
      schedule[i].value_at=i;
      schedule[i].location=get_chunk_ref(op,off);
      if (schedule[i].location.off<0) {
	u8_free(schedule); u8_free(values);
	return NULL;}
      else i++;}
    /* Note that we sort even if we're mmaped in order to take
       advantage of page locality. */
    qsort(schedule,n,sizeof(struct OIDPOOL_FETCH_SCHEDULE),
	  compare_offsets);
    i=0; while (i<n) {
      fdtype value=read_oid_value_at(op,schedule[i].location);
      if (FD_ABORTP(value)) {
	int j=0; while (j<i) { fd_decref(values[j]); j++;}
	u8_free(schedule); u8_free(values);
	fd_reterr(value);
	return NULL;}
      else values[schedule[i].value_at]=value;
      i++;}
    return values;}
}

static fdtype oidpool_alloc(fd_pool p,int n)
{
  fdtype results=FD_EMPTY_CHOICE; int i=0;
  fd_oidpool op=(fd_oidpool)p;
  fd_lock_mutex(&(op->lock));
  if (!(FD_OIDPOOL_LOCKED(op))) lock_oidpool(op,0);
  if (op->load+n>=op->capacity) {
    fd_unlock_mutex(&(op->lock));
    return fd_err(fd_ExhaustedPool,"file_pool_alloc",p->cid,FD_VOID);}
  while (i < n) {
    FD_OID new_addr=FD_OID_PLUS(op->base,op->load);
    fdtype new_oid=fd_make_oid(new_addr);
    FD_ADD_TO_CHOICE(results,new_oid);
    op->load++; i++; op->n_locks++;}
  fd_unlock_mutex(&(op->lock));
  return fd_simplify_choice(results);
}

static int oidpool_lock(fd_pool p,fdtype oids)
{
  struct FD_OIDPOOL *fp=(struct FD_OIDPOOL *)p;
  int retval=lock_oidpool(fp,1);
  if (retval)
    fp->n_locks=fp->n_locks+FD_CHOICE_SIZE(oids);
  return retval;
}

static int oidpool_unlock(fd_pool p,fdtype oids)
{
  struct FD_OIDPOOL *fp=(struct FD_OIDPOOL *)p;
  if (fp->n_locks == 0) return 0;
  else if (!(FD_OIDPOOL_LOCKED(fp))) return 0;
  else if (FD_CHOICEP(oids))
    fp->n_locks=fp->n_locks-FD_CHOICE_SIZE(oids);
  else if (FD_EMPTY_CHOICEP(oids)) {}
  else fp->n_locks--;
  if (fp->n_locks == 0) {
    fd_dtsunlock(&(fp->stream));
    fd_reset_hashtable(&(fp->locks),0,1);}
  return 1;
}

static void file_pool_setcache(fd_pool p,int level)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  if (level == 2)
    if (fp->offsets) return;
    else {
      fd_dtype_stream s=&(fp->stream);
      unsigned int load, *offsets, *newmmap;
      fd_lock_mutex(&(fp->lock));
      if (fp->offsets) {
	fd_unlock_mutex(&(fp->lock));
	return;}
#if HAVE_MMAP
      newmmap=
	/* When allocating an offset buffer to read, we only have to make it as
	   big as the file pools load. */
	mmap(NULL,(4*fp->load)+24,PROT_READ,
	     MAP_SHARED|MAP_NORESERVE,s->fd,0);
      if ((newmmap==NULL) || (newmmap==((void *)-1))) {
	u8_warn(u8_strerror(errno),"file_pool_setcache:mmap %s",fp->cid);
	fp->offsets=NULL; fp->offsets_size=0; errno=0;}
      fp->offsets=offsets=newmmap+6;
      fp->offsets_size=fp->load;
#else
      fd_dts_start_read(s);
      fd_setpos(s,12);
      fp->load=load=fd_dtsread_4bytes(s);
      offsets=u8_malloc(sizeof(unsigned int)*load);
      fd_setpos(s,24);
      fd_dtsread_ints(s,load,offsets);
      fp->offsets=offsets; fp->offsets_size=load;
#endif
      fd_unlock_mutex(&(fp->lock));}
  else if (level < 2)
    if (fp->offsets == NULL) return;
    else {
      int retval;
      fd_lock_mutex(&(fp->lock));
#if HAVE_MMAP
      /* Since we were just reading, the buffer was only as big
	 as the load, not the capacity. */
      retval=munmap((fp->offsets)-6,4*fp->load+24);
      if (retval<0) {
	u8_warn(u8_strerror(errno),"file_pool_setcache:munmap %s",fp->cid);
	fp->offsets=NULL; errno=0;}
#else
      u8_free(fp->offsets);
#endif
      fp->offsets=NULL; fp->offsets_size=0;}
}

static void reload_file_pool_cache(struct FD_FILE_POOL *fp,int lock)
{
#if HAVE_MMAP
  /* This should grow the offsets if the load has changed. */
#else
  fd_dtype_stream s=&(fp->stream);
  /* Read new offsets table, compare it with the current, and
     only void those OIDs */
  unsigned int new_load, *offsets, *nscan, *oscan, *olim;
  if (lock) fd_lock_mutex(&(fp->lock));
  oscan=fp->offsets; olim=oscan+fp->offsets_size;
  fd_setpos(s,16); new_load=fd_dtsread_4bytes(s);
  nscan=offsets=u8_malloc(sizeof(unsigned int)*new_load);
  fd_setpos(s,24);
  fd_dtsread_ints(s,new_load,offsets);
  while (oscan < olim)
    if (*oscan == *nscan) {oscan++; nscan++;}
    else {
      FD_OID addr=FD_OID_PLUS(fp->base,(nscan-offsets));
      fdtype changed_oid=fd_make_oid(addr);
      fd_hashtable_op(&(fp->cache),fd_table_replace,changed_oid,FD_VOID);
      oscan++; nscan++;}
  u8_free(fp->offsets);
  fp->offsets=offsets; fp->load=fp->offsets_size=new_load;
  update_modtime(fp);
  if (lock) fd_unlock_mutex(&(fp->lock));
#endif
}

static void oidpool_close(fd_pool p)
{
  fd_oidpool op=(fd_oidpool)p;
  fd_lock_mutex(&(op->lock));
  if (op->offsets) {
#if HAVE_MMAP
    /* Since we were just reading, the buffer was only as big
       as the load, not the capacity. */
    int retval=munmap((op->offsets)-6,4*op->offsets_size+24);
    unsigned int *newmmap;
    if (retval<0) {
      u8_warn(u8_strerror(errno),"oidpool_close:munmap offsets %s",op->cid);
      errno=0;}
#else
    u8_free(op->offsets);
#endif 
    op->offsets=NULL; op->offsets_size=0;
    op->cache_level=-1;}
#if HAVE_MMAP
  if (op->mmap) {
    int retval=munmap(op->mmap,mmap_size);
    if (retval<0) {
      u8_warn(u8_strerror(errno),"oidpool_close:munmap %s",op->cid);
      errno=0;}}
#endif
  fd_dtsclose(&(op->stream),1);
  fd_unlock_mutex(&(op->lock));
}

static void oidpool_setbuf(fd_pool p,int bufsiz)
{
  fd_oidpool op=(fd_oidpool)p;
  fd_lock_mutex(&(op->lock));
  fd_dtsbufsize(&(op->stream),bufsiz);
  fd_unlock_mutex(&(op->lock));
}

static fdtype oidpool_metadata(fd_pool p,fdtype md)
{
  fd_oidpool op=(fd_oidpool)p;
  if (FD_VOIDP(md)) {
    fdtype metadata; off_t metadata_pos;
    fd_dtype_stream stream=&(op->stream);
    fd_lock_mutex(&(op->lock));
    fd_setpos(stream,FD_METADATA_POS_POS);
    metadata_pos=fd_dstread_off_t(stream);
    if (metadata_pos) {
      fd_setpos(stream,metadata_pos);
      metadata=fd_dtsread_dtype(stream);
      fd_unlock_mutex(&(op->lock));
      return metadata;}
    else {
      fd_unlock_mutex(&(op->lock));
      return FD_VOID;}}
  else if (op->read_only)
    return fd_err(fd_ReadOnlyPool,"oidpool_metadata",op->cid,FD_VOID);
  else {
    fd_dtype_stream stream=&(op->stream);
    off_t metadata_pos=fd_endpos(stream);
    fd_dtswrite_dtype(stream,metadata);
    fd_setpos(stream,FD_METADATA_POS_POS);
    fd_dtswrite_off_t(stream,metadata_pos);
    fd_dtsflush(stream); fsync(s->fd);
    return fd_incref(metadata);}
}

/* Module (file) Initialization */

static struct FD_POOL_HANDLER oidpool_handler={
  "oidpool", 1, sizeof(struct FD_OIDPOOL), 12,
   oidpool_close, /* close */
   oidpool_setcache, /* setcache */
   oidpool_setbuf, /* setbuf */
   oidpool_alloc, /* alloc */
   oidpool_fetch, /* fetch */
   oidpool_fetchn, /* fetchn */
   oidpool_load, /* getload */
   oidpool_lock, /* lock */
   oidpool_unlock, /* release */
   oidpool_storen, /* storen */
   oidpool_metadata, /* metadata */
   NULL}; /* sync */


FD_EXPORT void fd_init_oidpools_c()
{
  fd_register_source_file(versionid);

  fd_register_pool_opener
    (FD_FILE_POOL_MAGIC_NUMBER,
     open_std_file_pool,fd_read_pool_metadata,fd_write_pool_metadata);
  fd_register_pool_opener
    (FD_FILE_POOL_TO_RECOVER,
     open_std_file_pool,fd_read_pool_metadata,fd_write_pool_metadata);
}

