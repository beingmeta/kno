/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_BUFIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/stream.h"
#include "framerd/drivers.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>

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

static void update_modtime(struct FD_FILE_POOL *fp);
static void reload_file_pool_cache(struct FD_FILE_POOL *fp,int lock);

static struct FD_POOL_HANDLER file_pool_handler;

static int recover_file_pool(struct FD_FILE_POOL *);

static fd_pool open_file_pool(u8_string fname,fddb_flags flags)
{
  struct FD_FILE_POOL *pool=u8_alloc(struct FD_FILE_POOL);
  struct FD_STREAM *s=&(pool->pool_stream);
  FD_OID base=FD_NULL_OID_INIT;
  unsigned int read_only=(U8_BITP(flags,FDB_READ_ONLY));
  unsigned int hi, lo, magicno, capacity, load;
  fd_off_t label_loc; fdtype label;
  u8_string rname=u8_realpath(fname,NULL);
  fd_stream_mode mode=
    ((read_only) ? (FD_STREAM_READ) : (FD_STREAM_MODIFY));
  fd_init_file_stream(&(pool->pool_stream),fname,mode,
                            fd_driver_bufsize);
  /* See if it ended up read only */
  if (s->stream_flags&FD_STREAM_READ_ONLY) read_only=1;
  s->stream_flags&=~FD_STREAM_IS_MALLOCD;
  magicno=fd_read_4bytes_at(s,0);
  hi=fd_read_4bytes_at(s,4); lo=fd_read_4bytes_at(s,8);
  FD_SET_OID_HI(base,hi); FD_SET_OID_LO(base,lo);
  capacity=fd_read_4bytes_at(s,12);
  fd_init_pool((fd_pool)pool,base,capacity,&file_pool_handler,fname,rname);
  u8_free(rname);
  if (magicno==FD_FILE_POOL_TO_RECOVER) {
    u8_log(LOG_WARN,fd_RecoveryRequired,"Recovering the file pool %s",fname);
    if (recover_file_pool(pool)<0) {
      fd_seterr(fd_MallocFailed,"open_file_pool",NULL,FD_VOID);
      return NULL;}}
  load=fd_read_4bytes_at(s,16);
  label_loc=(fd_off_t)fd_read_4bytes_at(s,20);
  if (label_loc) {
    if (fd_setpos(s,label_loc)>0) {
      label=fd_read_dtype(fd_readbuf(s));
      if (FD_STRINGP(label))
        pool->pool_label=u8_strdup(FD_STRDATA(label));
      else u8_log(LOG_WARN,fd_BadFilePoolLabel,fd_dtype2string(label));
      fd_decref(label);}
    else {
      fd_seterr(fd_BadFilePoolLabel,"open_file_pool",
                u8_strdup("bad label loc"),
                FD_INT(label_loc));
      fd_close_stream(&(pool->pool_stream),0);
      u8_free(rname); u8_free(pool);
      return NULL;}}
  pool->pool_load=load; pool->pool_offsets=NULL; pool->pool_offsets_size=0;
  if (read_only)
    U8_SETBITS(pool->pool_flags,FDB_READ_ONLY);
  else U8_CLEARBITS(pool->pool_flags,FDB_READ_ONLY);
  fd_init_mutex(&(pool->file_lock));
  if (!(U8_BITP(pool->pool_flags,FDB_UNREGISTERED)))
    fd_register_pool((fd_pool)pool);
  update_modtime(pool);
  return (fd_pool)pool;
}

static void update_modtime(struct FD_FILE_POOL *fp)
{
  struct stat fileinfo;
  if ((fstat(fp->pool_stream.stream_fileno,&fileinfo))<0)
    fp->pool_modtime=(time_t)-1;
  else fp->pool_modtime=fileinfo.st_mtime;
}

/* These assume that the pool itself is locked */
static int write_file_pool_load(fd_file_pool fp)
{
  if (FD_POOLFILE_LOCKEDP(fp)) {
    /* Update the load */
    long long load;
    fd_stream stream=&(fp->pool_stream);
    fd_lock_stream(stream);
    load=fd_read_4bytes_at(stream,16);
    if (load<0) {
      fd_unlock_stream(stream);
      return -1;}
    else if (load>fp->pool_capacity) {
      u8_seterr("InvalidLoad","write_file_pool_load",u8_strdup(fp->pool_idstring));
      fd_unlockfile(stream);
      fd_unlock_stream(stream);
      return -1;}
    else if (fp->pool_load>load) {
      int rv=fd_write_4bytes_at(stream,fp->pool_load,16);
      fd_unlock_stream(stream);
      if (rv<0) return rv;
      return 1;}
    else {
      fd_unlock_stream(stream);
      return 0;}}
  else return 0;
}

static int read_file_pool_load(fd_file_pool fp)
{
  long long load;
  fd_stream stream=&(fp->pool_stream);
  if (FD_POOLFILE_LOCKEDP(fp)) {
    return fp->pool_load;}
  else fd_lock_stream(stream);
  if (fd_lockfile(stream)<0) return -1;
  load=fd_read_4bytes_at(stream,16);
  if (load<0) {
    fd_unlockfile(stream);
    fd_unlock_stream(stream);
    return -1;}
  else if (load>fp->pool_capacity) {
    u8_seterr("InvalidLoad","read_file_pool_load",u8_strdup(fp->pool_idstring));
    fd_unlockfile(stream);
    fd_unlock_stream(stream);
    return -1;}
  fd_unlockfile(stream);
  fd_unlock_stream(stream);
  fp->pool_load=load;
  fd_unlock_pool(fp);
  return load;
}

static int file_pool_load(fd_pool p)
{
  fd_file_pool fp=(fd_file_pool)p;
  if (FD_POOLFILE_LOCKEDP(fp))
    return fp->pool_load;
  else {
    int pool_load;
    fd_lock_pool(fp);
    pool_load=read_file_pool_load(fp);
    fd_unlock_pool(fp);
    return pool_load;}
}

static int lock_file_pool(struct FD_FILE_POOL *fp,int use_mutex)
{
  if (FD_POOLFILE_LOCKEDP(fp)) return 1;
  else if ((fp->pool_stream.stream_flags)&(FD_STREAM_READ_ONLY)) return 0;
  else {
    struct FD_STREAM *s=&(fp->pool_stream);
    struct stat fileinfo;
    if (use_mutex) fd_lock_pool(fp);
    /* Handle race condition by checking when locked */
    if (FD_POOLFILE_LOCKEDP(fp)) {
      if (use_mutex) fd_unlock_pool(fp);
      return 1;}
    if (fd_lockfile(s)==0) {
      fd_unlock_pool(fp);
      return 0;}
    fstat(s->stream_fileno,&fileinfo);
    if (fileinfo.st_mtime>fp->pool_modtime) {
      /* Make sure we're up to date. */
      if (fp->pool_offsets) reload_file_pool_cache(fp,0);
      else {
        fd_reset_hashtable(&(fp->pool_cache),-1,1);
        fd_reset_hashtable(&(fp->pool_changes),32,1);}}
    read_file_pool_load(fp);
    if (use_mutex) fd_unlock_pool(fp);
    return 1;}
}

static fdtype file_pool_fetch(fd_pool p,fdtype oid)
{
  fdtype value;
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  FD_OID addr=FD_OID_ADDR(oid);
  int offset=FD_OID_DIFFERENCE(addr,fp->pool_base), stream_locked=0;
  fd_stream stream=&(fp->pool_stream);
  fd_off_t data_pos;
  fd_lock_pool(fp);
  if (FD_EXPECT_FALSE(offset>=fp->pool_load)) {
    fd_unlock_pool(fp);
    return fd_err(fd_UnallocatedOID,"file_pool_fetch",fp->pool_idstring,oid);}
  else if (fp->pool_offsets) data_pos=offget(fp->pool_offsets,offset);
  else {
    fd_lock_stream(stream);
    stream_locked=1;
    if (fd_setpos(stream,24+4*offset)<0) {
      fd_unlock_stream(stream);
      fd_unlock_pool(fp);
      return FD_ERROR_VALUE;}
    data_pos=fd_read_4bytes(fd_readbuf(stream));}
  if (data_pos == 0) value=FD_EMPTY_CHOICE;
  else if (FD_EXPECT_FALSE(data_pos<24+fp->pool_load*4)) {
    /* We got a data pointer into the file header.  This will
       happen in the (hopefully now non-existent) case where
       we've stored a >32 bit offset into a 32-bit sized location
       and it got truncated down. */
    fd_unlock_pool(fp);
    return fd_err(fd_CorruptedPool,"file_pool_fetch",fp->pool_idstring,FD_VOID);}
  else {
    if (!(stream_locked)) fd_lock_stream(stream);
    if (fd_setpos(&(fp->pool_stream),data_pos)<0) {
      fd_unlock_stream(stream);
      fd_unlock_pool(fp);
      return FD_ERROR_VALUE;}
    value=fd_read_dtype(fd_readbuf(stream));
    fd_unlock_stream(stream);}
  fd_unlock_pool(fp);
  return value;
}

struct POOL_FETCH_SCHEDULE {
  unsigned int vpos; fd_off_t filepos;};

static int compare_filepos(const void *x1,const void *x2)
{
  const struct POOL_FETCH_SCHEDULE *s1=x1, *s2=x2;
  if (s1->filepos<s2->filepos) return -1;
  else if (s1->filepos>s2->filepos) return 1;
  else return 0;
}

static fdtype *file_pool_fetchn(fd_pool p,int n,fdtype *oids)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p; FD_OID base=p->pool_base;
  struct FD_STREAM *stream=&(fp->pool_stream);
  struct POOL_FETCH_SCHEDULE *schedule=
    u8_alloc_n(n,struct POOL_FETCH_SCHEDULE);
  fdtype *result=u8_alloc_n(n,fdtype);
  int i=0, min_file_pos=24+fp->pool_capacity*4, load;
  fd_lock_pool(fp); load=fp->pool_load;
  if (fp->pool_offsets) {
    unsigned int *offsets=fp->pool_offsets;
    int i=0; while (i < n) {
      fdtype oid=oids[i]; FD_OID addr=FD_OID_ADDR(oid);
      unsigned int off=FD_OID_DIFFERENCE(addr,base), file_off;
      if (FD_EXPECT_FALSE(off>=load)) {
        u8_free(result); u8_free(schedule);
        fd_unlock_pool(fp);
        fd_seterr(fd_UnallocatedOID,"file_pool_fetchn",u8_strdup(fp->pool_idstring),oid);
        return NULL;}
      file_off=offget(offsets,off);
      schedule[i].vpos=i;
      if (file_off==0)
        schedule[i].filepos=file_off;
      else if (FD_EXPECT_FALSE(file_off<min_file_pos)) {
        /* As above, we have a data pointer into the header.
           This should never happen unless a file is corrupted. */
        u8_free(result); u8_free(schedule);
        fd_unlock_pool(fp);
        fd_seterr(fd_CorruptedPool,"file_pool_fetchn",u8_strdup(fp->pool_idstring),oid);
        return NULL;}
      else schedule[i].filepos=file_off;
      i++;}}
  else {
    int i=0; while (i < n) {
      fdtype oid=oids[i]; FD_OID addr=FD_OID_ADDR(oid);
      unsigned int off=FD_OID_DIFFERENCE(addr,base), file_off;
      schedule[i].vpos=i; fd_lock_stream(stream);
      if (fd_setpos(stream,24+4*off)<0) {
        u8_free(schedule);
        u8_free(result);
        fd_unlock_stream(stream);
        fd_unlock_pool(fp);
        return NULL;}
      file_off=fd_read_4bytes(fd_readbuf(stream));
      if (FD_EXPECT_FALSE(file_off==0))
        /* This is okay, just an allocated but unassigned OID. */
        schedule[i].filepos=file_off;
      else if (FD_EXPECT_FALSE(file_off<min_file_pos)) {
        /* As above, we have a data pointer into the header.
           This should never happen unless a file is corrupted. */
        u8_free(result); u8_free(schedule);
        fd_unlock_stream(stream);
        fd_unlock_pool(fp);
        fd_seterr(fd_CorruptedPool,"file_pool_fetchn",u8_strdup(fp->pool_idstring),oid);
        return NULL;}
      else schedule[i].filepos=file_off;
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
        fd_unlock_pool(fp);
        fd_unlock_stream(stream);
        return NULL;}
      result[schedule[i].vpos]=fd_read_dtype(fd_readbuf(stream));
      i++;}
    else result[schedule[i++].vpos]=FD_EMPTY_CHOICE;
  u8_free(schedule);
  fd_unlock_stream(stream);
  fd_unlock_pool(fp);
  return result;
}

static void write_file_pool_recovery_data
  (struct FD_FILE_POOL *fp,unsigned int *off);

static int file_pool_storen(fd_pool p,int n,fdtype *oids,fdtype *values)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p; FD_OID base=p->pool_base;
  /* This stores the offset where the DTYPE representation of each changed OID
     has been written, indexed by the OIDs position in *oids. */
  unsigned int *changed_offsets=u8_alloc_n(n,unsigned int);
  struct FD_STREAM *stream=&(fp->pool_stream);
  struct FD_OUTBUF *outstream=fd_writebuf(stream);
  /* Make sure that pos_limit fits into an int, in case fd_off_t is an int. */
  fd_off_t endpos, pos_limit=0xFFFFFFFF;
  int i=0, retcode=n, load;
  unsigned int *tmp_offsets=NULL, old_size=0;
  fd_lock_pool(fp); load=fp->pool_load;
  /* Get the endpos after the file pool structure is locked. */
  fd_lock_stream(stream);
  endpos=fd_endpos(stream);
  while (i<n) {
    FD_OID oid=FD_OID_ADDR(oids[i]);
    unsigned int oid_off=FD_OID_DIFFERENCE(oid,base);
    int delta=fd_write_dtype(fd_writebuf(stream),values[i]);
    if (FD_EXPECT_FALSE(oid_off>=load)) {
      fd_seterr(fd_UnallocatedOID,
                "file_pool_storen",u8_strdup(fp->pool_idstring),
                oids[i]);
      retcode=-1; break;}
    else if (FD_EXPECT_FALSE(delta<0)) {retcode=-1; break;}
    else if (FD_EXPECT_FALSE(((fd_off_t)(endpos+delta))>pos_limit)) {
      fd_seterr(fd_FileSizeOverflow,
                "file_pool_storen",u8_strdup(fp->pool_idstring),
                oids[i]);
      retcode=-1; break;}
    changed_offsets[i]=endpos; endpos=endpos+delta;
    i++;}
  /* Write recovery information which can be used to restore the
     offsets table and load. */
  if (retcode<0) {}
  else if ((fp->pool_offsets) && ((endpos+((fp->pool_load)*4))>=pos_limit)) {
    /* No space to write the recovery information! */
    fd_seterr(fd_FileSizeOverflow,
              "file_pool_storen",u8_strdup(fp->pool_idstring),
              FD_VOID);
    retcode=-1;}
  else if (fp->pool_offsets) {
    int i=0, load=fp->pool_load;
    unsigned int *old_offsets=fp->pool_offsets;
    old_size=fp->pool_offsets_size;
    tmp_offsets=u8_alloc_n(load,unsigned int);
    /* Initialize tmp_offsets from the current offsets */
    if (HAVE_MMAP) {
      /* If we're mmapped, the latest values are there. */
      while (i<old_size) {
        tmp_offsets[i]=offget(old_offsets,i); i++;}
      while (i<load) tmp_offsets[i++]=0;}
    else {
      /* Otherwise, we just copy them from the old offsets. */
      memcpy(tmp_offsets,old_offsets,sizeof(unsigned int)*old_size);
      memset(tmp_offsets+old_size,0,sizeof(unsigned int)*(load-old_size));}
    /* Write the changes */
    i=0; while (i<n) {
      FD_OID addr=FD_OID_ADDR(oids[i]);
      unsigned int oid_off=FD_OID_DIFFERENCE(addr,base);
      tmp_offsets[oid_off]=changed_offsets[i];
      i++;}
    u8_free(changed_offsets);
    /* Now write the new offset values to the end of the file. */
    write_file_pool_recovery_data(fp,tmp_offsets);
    /* Now write the real data */
    fd_setpos(stream,24);
    fd_write_ints(stream,fp->pool_load,tmp_offsets);}
  else {
    /* If we don't have an offsets cache, we don't bother
       with ACID and just write the changed offsets directly */
    int i=0; while (i<n) {
      FD_OID addr=FD_OID_ADDR(oids[i]);
      unsigned int reloff=FD_OID_DIFFERENCE(addr,base);
      if (fd_setpos(stream,24+4*reloff)<0) {
        retcode=-1; break;}
      fd_write_4bytes(outstream,changed_offsets[i]);
      i++;}
    u8_free(changed_offsets);}
  if (retcode>=0) {
    /* Now we update the load and do other cleanup.  */
    if (write_file_pool_load(fp)<0)
      u8_log(LOG_CRIT,"FileError","Can't update load for %s",fp->pool_idstring);
    update_modtime(fp);
    /* Now, we set the file's magic number back to something
       that doesn't require recovery and truncate away the saved
       recovery information. */
    if (fp->pool_offsets) {
      fd_off_t end=fd_endpos(stream); int retval;
      fd_setpos(stream,0);
      /* This was overwritten with FD_FILE_POOL_TO_RECOVER by
         fd_write_file_pool_recovery_data. */
      fd_write_4bytes(fd_writebuf(stream),FD_FILE_POOL_MAGIC_NUMBER);
      fd_flush_stream(stream); fsync(stream->stream_fileno);
      fd_endpos(stream); fd_movepos(stream,-(4*(fp->pool_capacity+1)));
      retval=ftruncate(stream->stream_fileno,end-(4*(fp->pool_capacity+1)));
      if (retval<0) {
        retcode=-1; u8_graberr(errno,"file_pool_storen",fp->pool_idstring);}}
    else fd_flush_stream(stream);
    /* Update the offsets, if you have any */
    if (fp->pool_offsets==NULL) {}
    else if (HAVE_MMAP) {
      int retval=munmap((fp->pool_offsets)-6,4*old_size+24);
      unsigned int *newmmap;
      if (retval<0) {
        u8_log(LOG_WARN,u8_strerror(errno),"file_pool_storen:munmap %s",fp->pool_idstring);
        fp->pool_offsets=NULL; errno=0;}
      newmmap=mmap(NULL,(4*fp->pool_load)+24,PROT_READ,
                   MAP_SHARED|MAP_NORESERVE,stream->stream_fileno,0);
      if ((newmmap==NULL) || (newmmap==((void *)-1))) {
        u8_log(LOG_WARN,u8_strerror(errno),"file_pool_storen:mmap %s",fp->pool_idstring);
        fp->pool_offsets=NULL; fp->pool_offsets_size=0; errno=0;}
      else {
        fp->pool_offsets=newmmap+6;
        fp->pool_offsets_size=fp->pool_load;}
      u8_free(tmp_offsets);}
    else {
      u8_free(fp->pool_offsets);
      fp->pool_offsets=tmp_offsets;
      fp->pool_offsets_size=fp->pool_load;}}
  /* Note that if we exited abnormally, the file is still intact. */
  fd_unlock_stream(stream);
  fd_unlock_pool(fp);
  return retcode;
}

static void write_file_pool_recovery_data
   (struct FD_FILE_POOL *fp,unsigned int *offsets)
{
  struct FD_STREAM *stream=&(fp->pool_stream);
  int i=0, load=fp->pool_load, len=fp->pool_capacity;
  fd_endpos(stream);
  fd_write_4bytes(fd_writebuf(stream),load);
  while (i<load) {
    fd_write_4bytes(fd_writebuf(stream),offsets[i]); i++;}
  while (i<len) {fd_write_4bytes(fd_writebuf(stream),0); i++;}
  fd_setpos(stream,0);
  fd_write_4bytes(fd_writebuf(stream),FD_FILE_POOL_TO_RECOVER);
  fd_flush_stream(stream);
}

static int recover_file_pool(struct FD_FILE_POOL *fp)
{
  /* This reads the offsets vector written at the end of the file
     during commitment. */
  int i=0, len=fp->pool_capacity, load; fd_off_t new_end, retval;
  unsigned int *offsets=u8_malloc(4*len);
  struct FD_STREAM *s=&(fp->pool_stream);
  struct FD_INBUF *instream=fd_readbuf(s);
  struct FD_OUTBUF *outstream;
  fd_lock_stream(s);
  fd_endpos(s);
  new_end=fd_movepos(s,-(4+4*len));
  load=fd_read_4bytes(instream);
  while (i<len) {
    offsets[i]=fd_read_4bytes(instream); i++;}
  fd_setpos(s,16);
  outstream=fd_writebuf(s);
  fd_write_4bytes(outstream,load);
  fd_setpos(s,24);
  i=0; while (i<len) {
    fd_write_4bytes(outstream,offsets[i]); i++;}
  fd_setpos(s,0);
  fd_write_4bytes(outstream,FD_FILE_POOL_MAGIC_NUMBER);
  fd_flush_stream(s); fp->pool_load=load;
  retval=ftruncate(s->stream_fileno,new_end);
  fd_unlock_stream(s);
  if (retval<0) return retval;
  else retval=fsync(s->stream_fileno);
  return retval;
}

static fdtype file_pool_alloc(fd_pool p,int n)
{
  fdtype results=FD_EMPTY_CHOICE; int i=0;
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  fd_lock_pool(fp);
  if (!(FD_POOLFILE_LOCKEDP(fp))) lock_file_pool(fp,0);
  if (fp->pool_load+n>=fp->pool_capacity) {
    fd_unlock_pool(fp);
    return fd_err(fd_ExhaustedPool,"file_pool_alloc",p->pool_idstring,FD_VOID);}
  while (i < n) {
    FD_OID new_addr=FD_OID_PLUS(fp->pool_base,fp->pool_load);
    fdtype new_oid=fd_make_oid(new_addr);
    FD_ADD_TO_CHOICE(results,new_oid);
    i++;}
  fp->pool_load+=n;
  fd_unlock_pool(fp);
  return results;
}

static int file_pool_lock(fd_pool p,fdtype oids)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  if (FD_POOLFILE_LOCKEDP(fp)) return 1;
  else {
    int retval=lock_file_pool(fp,1);
    return retval;}
}

static int file_pool_unlock(fd_pool p,fdtype oids)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  if (fp->pool_changes.table_n_keys == 0)
    fd_unlockfile(&(fp->pool_stream));
  return 1;
}

static void file_pool_setcache(fd_pool p,int level)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  if (level == 2)
    if (fp->pool_offsets) return;
    else {
      fd_stream s=&(fp->pool_stream);
      unsigned int *offsets, *newmmap;
      fd_lock_pool(fp);
      if (fp->pool_offsets) {
        fd_unlock_pool(fp);
        return;}
#if HAVE_MMAP
      newmmap=
        /* When allocating an offset buffer to read, we only have to make it as
           big as the file pools load. */
        mmap(NULL,(4*fp->pool_load)+24,PROT_READ,
             MAP_SHARED|MAP_NORESERVE,s->stream_fileno,0);
      if ((newmmap==NULL) || (newmmap==((void *)-1))) {
        u8_log(LOG_WARN,u8_strerror(errno),"file_pool_setcache:mmap %s",fp->pool_idstring);
        fp->pool_offsets=NULL; fp->pool_offsets_size=0; errno=0;}
      fp->pool_offsets=offsets=newmmap+6;
      fp->pool_offsets_size=fp->pool_load;
#else
      fd_inbuf ins=fd_readbuf(s);
      fd_setpos(s,12);
      fp->pool_load=load=fd_read_4bytes(ins);
      offsets=u8_alloc_n(load,unsigned int);
      fd_setpos(s,24);
      fd_read_ints(ins,load,offsets);
      fp->pool_offsets=offsets; fp->pool_offsets_size=load;
#endif
      fd_unlock_pool(fp);}
  else if (level < 2) {
    if (fp->pool_offsets == NULL) return;
    else {
      int retval;
      fd_lock_pool(fp);
#if HAVE_MMAP
      /* Since we were just reading, the buffer was only as big
         as the load, not the capacity. */
      retval=munmap((fp->pool_offsets)-6,4*fp->pool_load+24);
      if (retval<0) {
        u8_log(LOG_WARN,u8_strerror(errno),"file_pool_setcache:munmap %s",fp->pool_idstring);
        fp->pool_offsets=NULL; errno=0;}
#else
      u8_free(fp->pool_offsets);
#endif
      fp->pool_offsets=NULL; fp->pool_offsets_size=0;}}
}

static void reload_file_pool_cache(struct FD_FILE_POOL *fp,int lock)
{
#if HAVE_MMAP
  /* This should grow the offsets if the load has changed. */
#else
  fd_stream s=&(fp->pool_stream);
  fd_inbuf ins=fd_readbuf(s);
  /* Read new offsets table, compare it with the current, and
     only void those OIDs */
  unsigned int new_load, *offsets, *nscan, *oscan, *olim;
  if (lock) fd_lock_pool(fp);
  oscan=fp->pool_offsets; olim=oscan+fp->pool_offsets_size;
  fd_setpos(s,16); new_load=fd_read_4bytes(ins);
  nscan=offsets=u8_alloc_n(new_load,unsigned int);
  fd_setpos(s,24);
  fd_read_ints(ins,new_load,offsets);
  while (oscan < olim)
    if (*oscan == *nscan) {oscan++; nscan++;}
    else {
      FD_OID addr=FD_OID_PLUS(fp->pool_base,(nscan-offsets));
      fdtype changed_oid=fd_make_oid(addr);
      fd_hashtable_op(&(fp->pool_cache),fd_table_replace,changed_oid,FD_VOID);
      oscan++; nscan++;}
  u8_free(fp->pool_offsets);
  fp->pool_offsets=offsets; fp->pool_load=fp->pool_offsets_size=new_load;
  update_modtime(fp);
  if (lock) fd_unlock_pool(fp);
#endif
}

static void file_pool_close(fd_pool p)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  fd_lock_pool(fp);
  if (write_file_pool_load(fp)<0)
    u8_log(LOG_CRIT,"FileError","Can't update load for %s",fp->pool_idstring);
  fd_free_stream(&(fp->pool_stream),1);
  if (fp->pool_offsets) {
#if HAVE_MMAP
    /* Since we were just reading, the buffer was only as big
       as the load, not the capacity. */
    int retval=munmap((fp->pool_offsets)-6,4*fp->pool_offsets_size+24);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),"file_pool_close:munmap %s",fp->pool_idstring);
      errno=0;}
#else
    u8_free(fp->pool_offsets);
#endif
    fp->pool_offsets=NULL; fp->pool_offsets_size=0;
    fp->pool_cache_level=-1;}
  fd_unlock_pool(fp);
}

static void file_pool_setbuf(fd_pool p,int bufsiz)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  fd_lock_pool(fp);
  fd_stream_setbuf(&(fp->pool_stream),bufsiz);
  fd_unlock_pool(fp);
}

/* Making file pools */

FD_EXPORT
/* fd_make_file_pool:
    Arguments: a filename string, a magic number (usigned int), an FD_OID,
    a capacity, and a dtype pointer to a metadata description (a slotmap).
    Returns: -1 on error, 1 on success. */
int fd_make_file_pool
  (u8_string filename,unsigned int magicno,
   FD_OID base,unsigned int capacity,unsigned int load)
{
  int i, hi, lo;
  struct FD_STREAM _stream;
  struct FD_STREAM *stream=
    fd_init_file_stream(&_stream,filename,FD_STREAM_CREATE,8192);
  struct FD_OUTBUF *outstream=fd_writebuf(stream);
  if (stream==NULL) return -1;
  else if ((stream->stream_flags)&FD_STREAM_READ_ONLY) {
    fd_seterr3(fd_CantWrite,"fd_make_file_pool",u8_strdup(filename));
    fd_close_stream(stream,FD_STREAM_FREE);
    return -1;}
  stream->stream_flags&=~FD_STREAM_IS_MALLOCD;
  fd_setpos(stream,0);
  hi=FD_OID_HI(base); lo=FD_OID_LO(base);
  fd_write_4bytes(outstream,magicno);
  fd_write_4bytes(outstream,hi);
  fd_write_4bytes(outstream,lo);
  fd_write_4bytes(outstream,capacity);
  fd_write_4bytes(outstream,load); /* load */
  fd_write_4bytes(outstream,0); /* label pos */
  i=0; while (i<capacity) {fd_write_4bytes(outstream,0); i++;}
  /* Write an initially empty metadata block */
  fd_write_4bytes(outstream,0xFFFFFFFE);
  fd_write_4bytes(outstream,40);
  i=0; while (i<8) {fd_write_4bytes(outstream,0); i++;}
  fd_close_stream(stream,FD_STREAM_FREE);
  return 1;
}

static fd_pool filepool_create(u8_string spec,void *type_data,
                               fddb_flags flags,fdtype opts)
{
  fdtype base_oid=fd_getopt(opts,fd_intern("BASE"),FD_VOID);
  fdtype capacity=fd_getopt(opts,fd_intern("CAPACITY"),FD_VOID);
  fdtype load=fd_getopt(opts,fd_intern("LOAD"),FD_FIXZERO);
  unsigned int magic_number=(unsigned int)((unsigned long)type_data);
  int rv=0;
  if (!(FD_OIDP(base_oid))) {
    fd_seterr("Not a base oid","filepool_create",spec,base_oid);
    rv=-1;}
  if ((rv>=0)&&(!(FD_INTEGERP(capacity)))) {
    fd_seterr("Not a valid capacity","filepool_create",spec,capacity);
    rv=-1;}
  if ((rv>=0)&&(!(FD_INTEGERP(load)))) {
    fd_seterr("Not a valid load","filepool_create",spec,load);
    rv=-1;}
  if (rv<0) return NULL;
  else rv=fd_make_file_pool(spec,
                            magic_number,
                            FD_OID_ADDR(base_oid),
                            capacity,
                            load);
  if (rv>=0)
    return fd_open_pool(spec,flags);
  else return NULL;
}


/* The handler struct */

static struct FD_POOL_HANDLER file_pool_handler={
  "file_pool", 1, sizeof(struct FD_FILE_POOL), 12,
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
  NULL, /* swapout */
  NULL, /* metadata */
  NULL, /* sync */
  filepool_create /* create */};

/* Matching pool names */

static u8_string match_pool_name(u8_string spec,void *data)
{
  if ((u8_file_existsp(spec))&&
      (fd_match4bytes(spec,data)))
    return spec;
  else if (u8_has_suffix(spec,".pool",1))
    return NULL;
  else {
    u8_string variation=u8_mkstring("%s.pool",spec);
    if ((u8_file_existsp(variation))&&
        (fd_match4bytes(variation,data)))
      return variation;
    else {
      u8_free(variation);
      return NULL;}}
}

/* Module (file) Initialization */

FD_EXPORT void fd_init_file_pools_c()
{
  u8_register_source_file(_FILEINFO);

  fd_register_pool_type
    ("filepool",
     &file_pool_handler,
     open_file_pool,
     match_pool_name,
     (void *)FD_FILE_POOL_MAGIC_NUMBER);
  fd_register_pool_type
    ("damaged_filepool",
     &file_pool_handler,
     open_file_pool,
     match_pool_name,
     (void *)FD_FILE_POOL_TO_RECOVER);
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
