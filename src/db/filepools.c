/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_INLINE_DTYPEIO 1

#include "fdb/dtype.h"
#include "fdb/dbfile.h"

#include <libu8/pathfns.h>
#include <libu8/filefns.h>

#include <errno.h>
#include <sys/stat.h>

#if (HAVE_MMAP)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#define MMAP_FLAGS (MAP_SHARED|MAP_NORESERVE)
#endif

#if ((HAVE_MMAP) && (!(WORDS_BIGENDIAN)))
#define offget(offvec,offset) (fd_flip_word((offvec)[offset]))
#define set_offset(offvec,offset,v) (offvec)[offset]=(fd_flip_word(v))
#else
#define offget(offvec,offset) ((offvec)[offset])
#define set_offset(offvec,offset,v) (offvec)[offset]=(v)
#endif

fd_exception fd_MMAPError=_("MMAP Error");
fd_exception fd_MUNMAPError=_("MUNMAP Error");

static void update_modtime(struct FD_FILE_POOL *fp);
static void reload_filepool_cache(struct FD_FILE_POOL *fp,int lock);

static struct FD_POOL_HANDLER filepool_handler;

static fd_pool open_std_file_pool(u8_string fname,int read_only)
{
  struct FD_FILE_POOL *pool=u8_malloc(sizeof(struct FD_FILE_POOL));
  struct FD_DTYPE_STREAM *s=&(pool->stream);
  FD_OID base; unsigned int hi, lo, magicno, capacity, load;
  off_t label_loc; fdtype label;
  u8_string rname=u8_realpath(fname,NULL);
  fd_dtstream_mode mode=
    ((read_only) ? (FD_DTSTREAM_READ) : (FD_DTSTREAM_MODIFY));
  fd_init_dtype_file_stream(&(pool->stream),fname,mode,FD_FILEDB_BUFSIZE,NULL,NULL);
  /* See if it ended up read only */
  if (pool->stream.bits&FD_DTSTREAM_READ_ONLY) read_only=1;
  pool->stream.mallocd=0;
  magicno=fd_dtsread_4bytes(s);
  hi=fd_dtsread_4bytes(s); lo=fd_dtsread_4bytes(s);
  FD_SET_OID_HI(base,hi); FD_SET_OID_LO(base,lo);
  capacity=fd_dtsread_4bytes(s);
  fd_init_pool((fd_pool)pool,base,capacity,&filepool_handler,fname,rname);
  u8_free(rname);
  load=fd_dtsread_4bytes(s);
  label_loc=(off_t)fd_dtsread_4bytes(s);
  if (label_loc) {
    if (fd_setpos(s,label_loc)>0) {
      label=fd_dtsread_dtype(s);
      if (FD_STRINGP(label)) pool->label=u8_strdup(FD_STRDATA(label));
      else u8_warn(fd_BadFilePoolLabel,fd_dtype2string(label));
      fd_decref(label);}
    else {
      fd_seterr(fd_BadFilePoolLabel,"open_std_file_pool",
		u8_strdup("bad label loc"),
		FD_INT2DTYPE(label_loc));
      fd_dtsclose(&(pool->stream),1);
      u8_free(rname); u8_free(pool);
      return NULL;}}
  pool->load=load; pool->offsets=NULL; pool->offsets_size=0;
  pool->read_only=read_only;
  u8_init_mutex(&(pool->lock));
  update_modtime(pool);
  return (fd_pool)pool;
}

static int lock_file_pool(struct FD_FILE_POOL *fp,int use_mutex)
{
  if (FD_FILEPOOL_LOCKED(fp)) return 1;
  else if ((fp->stream.bits)&(FD_DTSTREAM_READ_ONLY)) return 0;
  else {
    struct FD_DTYPE_STREAM *s=&(fp->stream);
    struct stat fileinfo;
    if (use_mutex) u8_lock_mutex(&(fp->lock));
    /* Handle race condition by checking when locked */
    if (FD_FILEPOOL_LOCKED(fp)) {
      if (use_mutex) u8_unlock_mutex(&(fp->lock));
      return 1;}
    if (fd_dtslock(s)==0) {
      u8_unlock_mutex(&(fp->lock));
      return 0;}
    fstat(s->fd,&fileinfo);
    if (fileinfo.st_mtime>fp->modtime) {
      /* Make sure we're up to date. */
      if (fp->offsets) reload_filepool_cache(fp,0);
      else {
	fd_reset_hashtable(&(fp->cache),-1,1);
	fd_reset_hashtable(&(fp->locks),32,1);}}
    if (use_mutex) u8_unlock_mutex(&(fp->lock));
    return 1;}
}

static void update_modtime(struct FD_FILE_POOL *fp)
{
  struct stat fileinfo;
  if ((fstat(fp->stream.fd,&fileinfo))<0)
    fp->modtime=(time_t)-1;
  else fp->modtime=fileinfo.st_mtime;
}

static int file_pool_load(fd_pool p)
{
  fd_file_pool fp=(fd_file_pool)p;
  if (FD_FILEPOOL_LOCKED(fp)) return fp->load;
  else {
    int load;
    u8_lock_mutex(&(fp->lock));
    if (fd_setpos(&(fp->stream),16)<0) {
      u8_unlock_mutex(&(fp->lock));
      return -1;}
    load=fd_dtsread_4bytes(&(fp->stream));
    fp->load=load;
    u8_unlock_mutex(&(fp->lock));
    return load;}
}

static fdtype file_pool_fetch(fd_pool p,fdtype oid)
{
  fdtype value;
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  FD_OID addr=FD_OID_ADDR(oid);
  int offset=FD_OID_DIFFERENCE(addr,fp->base);
  off_t data_pos;
  u8_lock_mutex(&(fp->lock));
  if (fp->offsets) data_pos=offget(fp->offsets,offset);
  else {
    if (fd_setpos(&(fp->stream),24+4*offset)<0) {
      u8_unlock_mutex(&(fp->lock));
      return fd_erreify();}
    data_pos=fd_dtsread_4bytes(&(fp->stream));}
  if (data_pos == 0) value=FD_EMPTY_CHOICE;
  else {
    if (fd_setpos(&(fp->stream),data_pos)<0) {
      u8_unlock_mutex(&(fp->lock));
      return fd_erreify();}
    value=fd_dtsread_dtype(&(fp->stream));}
  u8_unlock_mutex(&(fp->lock));
  return value;
}

struct POOL_FETCH_SCHEDULE {
  unsigned int vpos; off_t filepos;};

static int compare_filepos(const void *x1,const void *x2)
{
  const struct POOL_FETCH_SCHEDULE *s1=x1, *s2=x2;
  if (s1->filepos<s2->filepos) return -1;
  else if (s1->filepos>s2->filepos) return 1;
  else return 0;
}

static fdtype *file_pool_fetchn(fd_pool p,int n,fdtype *oids)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p; FD_OID base=p->base;
  struct FD_DTYPE_STREAM *stream=&(fp->stream);
  struct POOL_FETCH_SCHEDULE *schedule=
    u8_malloc(sizeof(struct POOL_FETCH_SCHEDULE)*n);
  fdtype *result=u8_malloc(sizeof(fdtype)*n);
  int i=0;
  u8_lock_mutex(&(fp->lock));
  if (fp->offsets) {
    unsigned int *offsets=fp->offsets;
    int i=0; while (i < n) {
      fdtype oid=oids[i]; FD_OID addr=FD_OID_ADDR(oid);
      unsigned int off=FD_OID_DIFFERENCE(addr,base);
      schedule[i].vpos=i;
      schedule[i].filepos=offget(offsets,off);
      i++;}}
  else {
    int i=0; while (i < n) {
      fdtype oid=oids[i]; FD_OID addr=FD_OID_ADDR(oid);
      unsigned int off=FD_OID_DIFFERENCE(addr,base);
      schedule[i].vpos=i;
      if (fd_setpos(stream,24+4*off)<0) {
	u8_free(schedule);
	u8_free(result);
	u8_unlock_mutex(&(fp->lock));
	return NULL;}
      schedule[i].filepos=fd_dtsread_4bytes(stream);
      i++;}}
  qsort(schedule,n,sizeof(struct POOL_FETCH_SCHEDULE),
	compare_filepos);
  i=0; while (i < n) 
    if (schedule[i].filepos) {
      if (fd_setpos(stream,schedule[i].filepos)<0) {
	int j=0; while (j<i) {
	  fd_decref(result[schedule[j].vpos]); j++;}
	u8_free(schedule);
	u8_free(result);
	u8_unlock_mutex(&(fp->lock));
	return NULL;}
      result[schedule[i].vpos]=fd_dtsread_dtype(stream);
      i++;}
    else result[schedule[i++].vpos]=FD_EMPTY_CHOICE;
  u8_free(schedule);
  u8_unlock_mutex(&(fp->lock));  
  return result;
}

static int file_pool_storen(fd_pool p,int n,fdtype *oids,fdtype *values)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p; FD_OID base=p->base;
  unsigned int *offsets=u8_malloc(sizeof(unsigned int)*n);
  struct FD_DTYPE_STREAM *stream=&(fp->stream);
  off_t endpos; int i=0, retcode=n;
  u8_lock_mutex(&(fp->lock));
  endpos=fd_endpos(stream);
#if HAVE_MMAP
  if (fp->offsets) {
    /* Since we were just reading, the buffer was only as big
       as the load, not the capacity. */
    int retval=munmap((fp->offsets)-6,4*fp->offsets_size+24);
    unsigned int *newmmap;
    if (retval<0) {
      u8_warn(u8_strerror(errno),"file_pool_storen:munmap %s",fp->source);
      fp->offsets=NULL; errno=0;}
    else fp->offsets=NULL;
    newmmap=
      mmap(NULL,(4*fp->load)+24,
	   PROT_READ|PROT_WRITE,
	   MAP_SHARED,stream->fd,0);
    if ((newmmap==NULL) || (newmmap==((void *)-1))) {
      u8_warn(u8_strerror(errno),"file_pool_storen:mmap",fp->source);
      fp->offsets=NULL; errno=0;}
    else {
      fp->offsets=newmmap+6;
      fp->offsets_size=fp->load;}}
#endif
  while (i<n) {
    int delta=fd_dtswrite_dtype(stream,values[i]);
    FD_OID oid=FD_OID_ADDR(oids[i]);
    if (delta<0) {retcode=-1; break;}
    offsets[i]=endpos; endpos=endpos+delta;
    i++;}
  if (retcode<0) {}
  else if (fp->offsets) {
    int i=0; while (i<n) {
      FD_OID addr=FD_OID_ADDR(oids[i]);
      unsigned int reloff=FD_OID_DIFFERENCE(addr,base);
      set_offset(fp->offsets,reloff,offsets[i]);
      i++;}}
  else {
    int i=0; while (i<n) {
      FD_OID addr=FD_OID_ADDR(oids[i]);
      unsigned int reloff=FD_OID_DIFFERENCE(addr,base);
      if (fd_setpos(stream,24+4*reloff)<0) {
	retcode=-1; break;}
      fd_dtswrite_4bytes(stream,offsets[i]);
      i++;}}
  u8_free(offsets);
#if HAVE_MMAP
  if (fp->offsets) {
    int retval=munmap((fp->offsets)-6,4*fp->offsets_size+24);
    unsigned int *newmmap;
    if (retval<0) {
      u8_warn(u8_strerror(errno),"file_pool_storen:munmap %s",fp->source);
      fp->offsets=NULL; errno=0;}
    else {
      fp->offsets=NULL; fp->offsets_size=0;}
    newmmap=
      /* When allocating an offset buffer to read, we only have to make it as
	 big as the file pools load. */
      mmap(NULL,(4*fp->load)+24,
	   PROT_READ,MAP_SHARED|MAP_NORESERVE,stream->fd,0);
    if ((newmmap==NULL) || (newmmap==((void *)-1))) {
      u8_warn(u8_strerror(errno),"file_pool_storen:mmap %s",fp->source);
      fp->offsets=NULL; errno=0;}
    else {
      fp->offsets_size=fp->load;
      fp->offsets=newmmap+6;}}
#else
  if ((retcode>=0) && (fp->offsets)) {
    if (fd_setpos(stream,24)<0) retcode=-1;
    else fd_dtswrite_ints(stream,fp->offsets_size,offsets);}
#endif
  if (retcode>=0) {
    if (fd_setpos(stream,16)<0) retcode=-1;
    else {
      fd_dtswrite_4bytes(stream,fp->load);
      fd_dtsflush(stream);
      update_modtime(fp);
      fsync(stream->fd);}}
  u8_unlock_mutex(&(fp->lock));
  return retcode;
}

static fdtype file_pool_alloc(fd_pool p,int n)
{
  fdtype results=FD_EMPTY_CHOICE; int i=0;
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  u8_lock_mutex(&(fp->lock));
  if (!(FD_FILEPOOL_LOCKED(fp))) lock_file_pool(fp,0);
  if (fp->load+n>=fp->capacity) {
    u8_unlock_mutex(&(fp->lock));
    return fd_err(fd_ExhaustedPool,"file_pool_alloc",p->cid,FD_VOID);}
  while (i < n) {
    FD_OID new_addr=FD_OID_PLUS(fp->base,fp->load);
    fdtype new_oid=fd_make_oid(new_addr);
    FD_ADD_TO_CHOICE(results,new_oid);
    fp->load++; i++; fp->n_locks++;}
  u8_unlock_mutex(&(fp->lock));
  return results;
}

static int file_pool_lock(fd_pool p,fdtype oids)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  int retval=lock_file_pool(fp,1);
  if (retval)
    fp->n_locks=fp->n_locks+FD_CHOICE_SIZE(oids);
  return retval;
}

static int file_pool_unlock(fd_pool p,fdtype oids)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  if (fp->n_locks == 0) return 0;
  else if (!(FD_FILEPOOL_LOCKED(fp))) return 0;
  else if (FD_CHOICEP(oids))
    fp->n_locks=fp->n_locks-FD_CHOICE_SIZE(oids);
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
      u8_lock_mutex(&(fp->lock));
      if (fp->offsets) {
	u8_unlock_mutex(&(fp->lock));
	return;}
#if HAVE_MMAP
      newmmap=
	/* When allocating an offset buffer to read, we only have to make it as
	   big as the file pools load. */
	mmap(NULL,(4*fp->load)+24,PROT_READ,
	     MAP_SHARED|MAP_NORESERVE,s->fd,0);
      if ((newmmap==NULL) || (newmmap==((void *)-1))) {
	u8_warn(u8_strerror(errno),"file_pool_setcache:mmap %s",fp->source);
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
      u8_unlock_mutex(&(fp->lock));}
  else if (level < 2)
    if (fp->offsets == NULL) return;
    else {
      int retval;
      u8_lock_mutex(&(fp->lock));
#if HAVE_MMAP
      /* Since we were just reading, the buffer was only as big
	 as the load, not the capacity. */
      retval=munmap((fp->offsets)-6,4*fp->load+24);
      if (retval<0) {
	u8_warn(u8_strerror(errno),"file_pool_setcache:munmap %s",fp->source);
	fp->offsets=NULL; errno=0;}
#else
      u8_free(fp->offsets);
#endif
      fp->offsets=NULL; fp->offsets_size=0;}
}

static void reload_filepool_cache(struct FD_FILE_POOL *fp,int lock)
{
#if HAVE_MMAP
  /* This should grow the offsets if the load has changed. */
#else
  fd_dtype_stream s=&(fp->stream);
  /* Read new offsets table, compare it with the current, and
     only void those OIDs */
  unsigned int new_load, *offsets, *nscan, *oscan, *olim;
  if (lock) u8_lock_mutex(&(fp->lock));
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
  if (lock) u8_unlock_mutex(&(fp->lock));
#endif
}

static void file_pool_close(fd_pool p)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  u8_lock_mutex(&(fp->lock));
  fd_dtsclose(&(fp->stream),1);
  if (fp->offsets) {
#if HAVE_MMAP
    /* Since we were just reading, the buffer was only as big
       as the load, not the capacity. */
    int retval=munmap((fp->offsets)-6,4*fp->offsets_size+24);
    unsigned int *newmmap;
    if (retval<0) {
      u8_warn(u8_strerror(errno),"file_pool_storen:munmap %s",fp->source);
      errno=0;}
#else
    u8_free(fp->offsets);
#endif 
    fp->offsets=NULL; fp->offsets_size=0;
    fp->cache_level=-1;}
  u8_unlock_mutex(&(fp->lock));
}

static void file_pool_setbuf(fd_pool p,int bufsiz)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  u8_lock_mutex(&(fp->lock));
  fd_dtsbufsize(&(fp->stream),bufsiz);
  u8_unlock_mutex(&(fp->lock));
}

static fdtype file_pool_metadata(fd_pool p,fdtype md)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  if (FD_VOIDP(md))
    return fd_read_pool_metadata(&(fp->stream));
  else return fd_write_pool_metadata((&(fp->stream)),md);
}


/* The handler struct */

static struct FD_POOL_HANDLER filepool_handler={
  "filepool", 1, sizeof(struct FD_FILE_POOL), 12,
   file_pool_close, /* close */
   file_pool_setcache, /* setcache */
   file_pool_setbuf, /* setbuf */
   file_pool_alloc, /* alloc */
   file_pool_fetch, /* fetch */
   file_pool_fetchn, /* fetchn */
   file_pool_load, /* getload */
   file_pool_lock, /* lock */
   file_pool_unlock, /* release */
   file_pool_storen, /* storen */
   file_pool_metadata, /* metadata */
   NULL}; /* sync */

/* Module (file) Initialization */

FD_EXPORT fd_init_filepools_c()
{
  fd_register_source_file(versionid);

  fd_register_pool_opener
    (FD_FILE_POOL_MAGIC_NUMBER,
     open_std_file_pool,fd_read_pool_metadata,fd_write_pool_metadata);
}


/* The CVS log for this file
   $Log: filepools.c,v $
   Revision 1.47  2006/03/14 04:30:19  haase
   Signal error when a pool is exhausted

   Revision 1.46  2006/02/05 13:53:26  haase
   Added locking around file pool stores

   Revision 1.45  2006/01/31 13:47:23  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.44  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.43  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.42  2006/01/05 19:16:44  haase
   Fix file pool lock/unlock inconsistency

   Revision 1.41  2006/01/05 18:04:44  haase
   Made pool access check return values from fd_setpos

   Revision 1.40  2005/12/22 14:39:28  haase
   Renamed file pool opener to avoid conflict with generic filepool opener

   Revision 1.39  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.38  2005/05/30 00:03:54  haase
   Fixes to pool declaration, allowing the USE-POOL primitive to return multiple pools correctly when given a ; spearated list or a pool server which provides multiple pools

   Revision 1.37  2005/05/26 11:03:21  haase
   Fixed some bugs with read-only pools and indices

   Revision 1.36  2005/05/25 18:05:19  haase
   Check for -1 return value from mmap as well as NULL

   Revision 1.35  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.34  2005/05/17 18:41:27  haase
   Fixed bug in oid commitment and unlocking

   Revision 1.33  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.32  2005/04/12 20:42:45  haase
   Used #define FD_FILEDB_BUFSIZE to set default buffer size (initially 256K)

   Revision 1.31  2005/04/06 18:31:51  haase
   Fixed mmap error calls to produce warnings rather than raising errors and to use u8_strerror to get the condition name

   Revision 1.30  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.29  2005/03/28 19:19:36  haase
   Added metadata reading and writing and file pool/index creation

   Revision 1.28  2005/03/25 19:49:47  haase
   Removed base library for eframerd, deferring to libu8

   Revision 1.27  2005/03/07 19:37:33  haase
   Added fsyncs and mmap/munmamp return value checking

   Revision 1.26  2005/03/07 14:18:19  haase
   Moved lock counting into pool handlers and made swapout devoid the locks table

   Revision 1.25  2005/03/06 19:26:44  haase
   Plug some leaks and some failures to return values

   Revision 1.24  2005/03/06 18:28:21  haase
   Added timeprims

   Revision 1.23  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.22  2005/03/03 17:58:15  haase
   Moved stdio dependencies out of fddb and reorganized make structure

   Revision 1.21  2005/02/26 21:39:56  haase
   Moved lock increments into pools.c

   Revision 1.20  2005/02/25 19:45:24  haase
   Fixed commitment of cached file pools

   Revision 1.19  2005/02/25 19:23:32  haase
   Fixes to pool locking and commitment

   Revision 1.18  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
 
