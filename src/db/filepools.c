/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_DTYPEIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/dbfile.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>

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
fd_exception fd_CorruptedPool=_("Corrupted file pool");
fd_exception fd_FileSizeOverflow=_("File pool overflowed file size");
fd_exception fd_RecoveryRequired=_("RECOVERY");

static void update_modtime(struct FD_FILE_POOL *fp);
static void reload_file_pool_cache(struct FD_FILE_POOL *fp,int lock);

static struct FD_POOL_HANDLER file_pool_handler;

static int recover_file_pool(struct FD_FILE_POOL *);

static fd_pool open_std_file_pool(u8_string fname,int read_only)
{
  struct FD_FILE_POOL *pool=u8_alloc(struct FD_FILE_POOL);
  struct FD_DTYPE_STREAM *s=&(pool->stream);
  FD_OID base=FD_NULL_OID_INIT;
  unsigned int hi, lo, magicno, capacity, load;
  fd_off_t label_loc; fdtype label;
  u8_string rname=u8_realpath(fname,NULL);
  fd_dtstream_mode mode=
    ((read_only) ? (FD_DTSTREAM_READ) : (FD_DTSTREAM_MODIFY));
  fd_init_dtype_file_stream(&(pool->stream),fname,mode,FD_FILEDB_BUFSIZE);
  /* See if it ended up read only */
  if ((((pool)->stream).fd_dts_flags)&FD_DTSTREAM_READ_ONLY) read_only=1;
  pool->stream.fd_mallocd=0;
  magicno=fd_dtsread_4bytes(s);
  hi=fd_dtsread_4bytes(s); lo=fd_dtsread_4bytes(s);
  FD_SET_OID_HI(base,hi); FD_SET_OID_LO(base,lo);
  capacity=fd_dtsread_4bytes(s);
  fd_init_pool((fd_pool)pool,base,capacity,&file_pool_handler,fname,rname);
  u8_free(rname);
  if (magicno==FD_FILE_POOL_TO_RECOVER) {
    u8_log(LOG_WARN,fd_RecoveryRequired,"Recovering the file pool %s",fname);
    if (recover_file_pool(pool)<0) {
      fd_seterr(fd_MallocFailed,"open_file_pool",NULL,FD_VOID);
      return NULL;}}
  load=fd_dtsread_4bytes(s);
  label_loc=(fd_off_t)fd_dtsread_4bytes(s);
  if (label_loc) {
    if (fd_setpos(s,label_loc)>0) {
      label=fd_dtsread_dtype(s);
      if (FD_STRINGP(label)) pool->label=u8_strdup(FD_STRDATA(label));
      else u8_log(LOG_WARN,fd_BadFilePoolLabel,fd_dtype2string(label));
      fd_decref(label);}
    else {
      fd_seterr(fd_BadFilePoolLabel,"open_std_file_pool",
                u8_strdup("bad label loc"),
                FD_INT(label_loc));
      fd_dtsclose(&(pool->stream),1);
      u8_free(rname); u8_free(pool);
      return NULL;}}
  pool->load=load; pool->offsets=NULL; pool->offsets_size=0;
  pool->read_only=read_only;
  fd_init_mutex(&(pool->fd_lock));
  update_modtime(pool);
  return (fd_pool)pool;
}

static int lock_file_pool(struct FD_FILE_POOL *fp,int use_mutex)
{
  if (FD_FILE_POOL_LOCKED(fp)) return 1;
  else if ((fp->stream.fd_dts_flags)&(FD_DTSTREAM_READ_ONLY)) return 0;
  else {
    struct FD_DTYPE_STREAM *s=&(fp->stream);
    struct stat fileinfo;
    if (use_mutex) fd_lock_struct(fp);
    /* Handle race condition by checking when locked */
    if (FD_FILE_POOL_LOCKED(fp)) {
      if (use_mutex) fd_unlock_struct(fp);
      return 1;}
    if (fd_dtslock(s)==0) {
      fd_unlock_struct(fp);
      return 0;}
    fstat(s->fd_fileno,&fileinfo);
    if (fileinfo.st_mtime>fp->modtime) {
      /* Make sure we're up to date. */
      if (fp->offsets) reload_file_pool_cache(fp,0);
      else {
        fd_reset_hashtable(&(fp->cache),-1,1);
        fd_reset_hashtable(&(fp->locks),32,1);}}
    if (use_mutex) fd_unlock_struct(fp);
    return 1;}
}

static void update_modtime(struct FD_FILE_POOL *fp)
{
  struct stat fileinfo;
  if ((fstat(fp->stream.fd_fileno,&fileinfo))<0)
    fp->modtime=(time_t)-1;
  else fp->modtime=fileinfo.st_mtime;
}

static int file_pool_load(fd_pool p)
{
  fd_file_pool fp=(fd_file_pool)p;
  if (FD_FILE_POOL_LOCKED(fp)) return fp->load;
  else {
    int load;
    fd_lock_struct(fp);
    if (fd_setpos(&(fp->stream),16)<0) {
      fd_unlock_struct(fp);
      return -1;}
    load=fd_dtsread_4bytes(&(fp->stream));
    fp->load=load;
    fd_unlock_struct(fp);
    return load;}
}

static fdtype file_pool_fetch(fd_pool p,fdtype oid)
{
  fdtype value;
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  FD_OID addr=FD_OID_ADDR(oid);
  int offset=FD_OID_DIFFERENCE(addr,fp->base);
  fd_off_t data_pos;
  fd_lock_struct(fp);
  if (FD_EXPECT_FALSE(offset>=fp->load)) {
    fd_unlock_struct(fp);
    return fd_err(fd_UnallocatedOID,"file_pool_fetch",fp->cid,oid);}
  else if (fp->offsets) data_pos=offget(fp->offsets,offset);
  else {
    if (fd_setpos(&(fp->stream),24+4*offset)<0) {
      fd_unlock_struct(fp);
      return FD_ERROR_VALUE;}
    data_pos=fd_dtsread_4bytes(&(fp->stream));}
  if (data_pos == 0) value=FD_EMPTY_CHOICE;
  else if (FD_EXPECT_FALSE(data_pos<24+fp->load*4)) {
    /* We got a data pointer into the file header.  This will
       happen in the (hopefully now non-existent) case where
       we've stored a >32 bit offset into a 32-bit sized location
       and it got truncated down. */
    fd_unlock_struct(fp);
    return fd_err(fd_CorruptedPool,"file_pool_fetch",fp->cid,FD_VOID);}
  else {
    if (fd_setpos(&(fp->stream),data_pos)<0) {
      fd_unlock_struct(fp);
      return FD_ERROR_VALUE;}
    value=fd_dtsread_dtype(&(fp->stream));}
  fd_unlock_struct(fp);
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
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p; FD_OID base=p->base;
  struct FD_DTYPE_STREAM *stream=&(fp->stream);
  struct POOL_FETCH_SCHEDULE *schedule=
    u8_alloc_n(n,struct POOL_FETCH_SCHEDULE);
  fdtype *result=u8_alloc_n(n,fdtype);
  int i=0, min_file_pos=24+fp->capacity*4, load;
  fd_lock_struct(fp); load=fp->load;
  if (fp->offsets) {
    unsigned int *offsets=fp->offsets;
    int i=0; while (i < n) {
      fdtype oid=oids[i]; FD_OID addr=FD_OID_ADDR(oid);
      unsigned int off=FD_OID_DIFFERENCE(addr,base), file_off;
      if (FD_EXPECT_FALSE(off>=load)) {
        u8_free(result); u8_free(schedule);
        fd_unlock_struct(fp);
        fd_seterr(fd_UnallocatedOID,"file_pool_fetchn",u8_strdup(fp->cid),oid);
        return NULL;}
      file_off=offget(offsets,off);
      schedule[i].vpos=i;
      if (file_off==0)
        schedule[i].filepos=file_off;
      else if (FD_EXPECT_FALSE(file_off<min_file_pos)) {
        /* As above, we have a data pointer into the header.
           This should never happen unless a file is corrupted. */
        u8_free(result); u8_free(schedule);
        fd_unlock_struct(fp);
        fd_seterr(fd_CorruptedPool,"file_pool_fetchn",u8_strdup(fp->cid),oid);
        return NULL;}
      else schedule[i].filepos=file_off;
      i++;}}
  else {
    int i=0; while (i < n) {
      fdtype oid=oids[i]; FD_OID addr=FD_OID_ADDR(oid);
      unsigned int off=FD_OID_DIFFERENCE(addr,base), file_off;
      schedule[i].vpos=i;
      if (fd_setpos(stream,24+4*off)<0) {
        u8_free(schedule);
        u8_free(result);
        fd_unlock_struct(fp);
        return NULL;}
      file_off=fd_dtsread_4bytes(stream);
      if (FD_EXPECT_FALSE(file_off==0))
        /* This is okay, just an allocated but unassigned OID. */
        schedule[i].filepos=file_off;
      else if (FD_EXPECT_FALSE(file_off<min_file_pos)) {
        /* As above, we have a data pointer into the header.
           This should never happen unless a file is corrupted. */
        u8_free(result); u8_free(schedule);
        fd_unlock_struct(fp);
        fd_seterr(fd_CorruptedPool,"file_pool_fetchn",u8_strdup(fp->cid),oid);
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
        fd_unlock_struct(fp);
        return NULL;}
      result[schedule[i].vpos]=fd_dtsread_dtype(stream);
      i++;}
    else result[schedule[i++].vpos]=FD_EMPTY_CHOICE;
  u8_free(schedule);
  fd_unlock_struct(fp);
  return result;
}

static void write_file_pool_recovery_data
  (struct FD_FILE_POOL *fp,unsigned int *off);

static int file_pool_storen(fd_pool p,int n,fdtype *oids,fdtype *values)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p; FD_OID base=p->base;
  /* This stores the offset where the DTYPE representation of each changed OID
     has been written, indexed by the OIDs position in *oids. */
  unsigned int *changed_offsets=u8_alloc_n(n,unsigned int);
  struct FD_DTYPE_STREAM *stream=&(fp->stream);
  /* Make sure that pos_limit fits into an int, in case fd_off_t is an int. */
  fd_off_t endpos, pos_limit=0xFFFFFFFF;
  int i=0, retcode=n, load;
  unsigned int *tmp_offsets=NULL, old_size=0;
  fd_lock_struct(fp); load=fp->load;
  /* Get the endpos after the file pool structure is locked. */
  endpos=fd_endpos(stream);
  while (i<n) {
    FD_OID oid=FD_OID_ADDR(oids[i]);
    unsigned int oid_off=FD_OID_DIFFERENCE(oid,base);
    int delta=fd_dtswrite_dtype(stream,values[i]);
    if (FD_EXPECT_FALSE(oid_off>=load)) {
      fd_seterr(fd_UnallocatedOID,
                "file_pool_storen",u8_strdup(fp->cid),
                oids[i]);
      retcode=-1; break;}
    else if (FD_EXPECT_FALSE(delta<0)) {retcode=-1; break;}
    else if (FD_EXPECT_FALSE(((fd_off_t)(endpos+delta))>pos_limit)) {
      fd_seterr(fd_FileSizeOverflow,
                "file_pool_storen",u8_strdup(fp->cid),
                oids[i]);
      retcode=-1; break;}
    changed_offsets[i]=endpos; endpos=endpos+delta;
    i++;}
  /* Write recovery information which can be used to restore the
     offsets table and load. */
  if (retcode<0) {}
  else if ((fp->offsets) && ((endpos+((fp->load)*4))>=pos_limit)) {
    /* No space to write the recovery information! */
    fd_seterr(fd_FileSizeOverflow,
              "file_pool_storen",u8_strdup(fp->cid),
              FD_VOID);
    retcode=-1;}
  else if (fp->offsets) {
    int i=0, load=fp->load;
    unsigned int *old_offsets=fp->offsets;
    old_size=fp->offsets_size;
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
    fd_dtswrite_ints(stream,fp->load,tmp_offsets);}
  else {
    /* If we don't have an offsets cache, we don't bother
       with ACID and just write the changed offsets directly */
    int i=0; while (i<n) {
      FD_OID addr=FD_OID_ADDR(oids[i]);
      unsigned int reloff=FD_OID_DIFFERENCE(addr,base);
      if (fd_setpos(stream,24+4*reloff)<0) {
        retcode=-1; break;}
      fd_dtswrite_4bytes(stream,changed_offsets[i]);
      i++;}
    u8_free(changed_offsets);}
  if (retcode>=0) {
    /* Now we update the load and do other cleanup.  */
    fd_setpos(stream,16);
    fd_dtswrite_4bytes(stream,fp->load);
    update_modtime(fp);
    /* Now, we set the file's magic number back to something
       that doesn't require recovery and truncate away the saved
       recovery information. */
    if (fp->offsets) {
      fd_off_t end=fd_endpos(stream); int retval;
      fd_setpos(stream,0);
      /* This was overwritten with FD_FILE_POOL_TO_RECOVER by
         fd_write_file_pool_recovery_data. */
      fd_dtswrite_4bytes(stream,FD_FILE_POOL_MAGIC_NUMBER);
      fd_dtsflush(stream); fsync(stream->fd_fileno);
      fd_movepos(stream,-(4*(fp->capacity+1)));
      retval=ftruncate(stream->fd_fileno,end-(4*(fp->capacity+1)));
      if (retval<0) {
        retcode=-1; u8_graberr(errno,"file_pool_storen",fp->cid);}}
    else fd_dtsflush(stream);
    /* Update the offsets, if you have any */
    if (fp->offsets==NULL) {}
    else if (HAVE_MMAP) {
      int retval=munmap((fp->offsets)-6,4*old_size+24);
      unsigned int *newmmap;
      if (retval<0) {
        u8_log(LOG_WARN,u8_strerror(errno),"file_pool_storen:munmap %s",fp->cid);
        fp->offsets=NULL; errno=0;}
      newmmap=mmap(NULL,(4*fp->load)+24,PROT_READ,
                   MAP_SHARED|MAP_NORESERVE,stream->fd_fileno,0);
      if ((newmmap==NULL) || (newmmap==((void *)-1))) {
        u8_log(LOG_WARN,u8_strerror(errno),"file_pool_storen:mmap %s",fp->cid);
        fp->offsets=NULL; fp->offsets_size=0; errno=0;}
      else {
        fp->offsets=newmmap+6;
        fp->offsets_size=fp->load;}
      u8_free(tmp_offsets);}
    else {
      u8_free(fp->offsets);
      fp->offsets=tmp_offsets;
      fp->offsets_size=fp->load;}}
  /* Note that if we exited abnormally, the file is still intact. */
  fd_unlock_struct(fp);
  return retcode;
}

static void write_file_pool_recovery_data
   (struct FD_FILE_POOL *fp,unsigned int *offsets)
{
  struct FD_DTYPE_STREAM *stream=&(fp->stream);
  int i=0, load=fp->load, len=fp->capacity;
  fd_endpos(stream);
  fd_dtswrite_4bytes(stream,load);
  while (i<load) {
    fd_dtswrite_4bytes(stream,offsets[i]); i++;}
  while (i<len) {fd_dtswrite_4bytes(stream,0); i++;}
  fd_setpos(stream,0);
  fd_dtswrite_4bytes(stream,FD_FILE_POOL_TO_RECOVER);
  fd_dtsflush(stream);
}

static int recover_file_pool(struct FD_FILE_POOL *fp)
{
  /* This reads the offsets vector written at the end of the file
     during commitment. */
  int i=0, len=fp->capacity, load; fd_off_t new_end, retval;
  unsigned int *offsets=u8_malloc(4*len);
  struct FD_DTYPE_STREAM *s=&(fp->stream);
  fd_endpos(s); new_end=fd_movepos(s,-(4+4*len));
  load=fd_dtsread_4bytes(s);
  while (i<len) {
    offsets[i]=fd_dtsread_4bytes(s); i++;}
  fd_setpos(s,16);
  fd_dtswrite_4bytes(s,load);
  fd_setpos(s,24);
  i=0; while (i<len) {
    fd_dtswrite_4bytes(s,offsets[i]); i++;}
  fd_setpos(s,0);
  fd_dtswrite_4bytes(s,FD_FILE_POOL_MAGIC_NUMBER);
  fd_dtsflush(s); fp->load=load;
  retval=ftruncate(s->fd_fileno,new_end);
  if (retval<0) return retval;
  else retval=fsync(s->fd_fileno);
  return retval;
}

static fdtype file_pool_alloc(fd_pool p,int n)
{
  fdtype results=FD_EMPTY_CHOICE; int i=0;
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  fd_lock_struct(fp);
  if (!(FD_FILE_POOL_LOCKED(fp))) lock_file_pool(fp,0);
  if (fp->load+n>=fp->capacity) {
    fd_unlock_struct(fp);
    return fd_err(fd_ExhaustedPool,"file_pool_alloc",p->cid,FD_VOID);}
  while (i < n) {
    FD_OID new_addr=FD_OID_PLUS(fp->base,fp->load);
    fdtype new_oid=fd_make_oid(new_addr);
    FD_ADD_TO_CHOICE(results,new_oid);
    fp->load++; i++; fp->n_locks++;}
  fd_unlock_struct(fp);
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
  else if (!(FD_FILE_POOL_LOCKED(fp))) return 0;
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
      unsigned int *offsets, *newmmap;
      fd_lock_struct(fp);
      if (fp->offsets) {
        fd_unlock_struct(fp);
        return;}
#if HAVE_MMAP
      newmmap=
        /* When allocating an offset buffer to read, we only have to make it as
           big as the file pools load. */
        mmap(NULL,(4*fp->load)+24,PROT_READ,
             MAP_SHARED|MAP_NORESERVE,s->fd_fileno,0);
      if ((newmmap==NULL) || (newmmap==((void *)-1))) {
        u8_log(LOG_WARN,u8_strerror(errno),"file_pool_setcache:mmap %s",fp->cid);
        fp->offsets=NULL; fp->offsets_size=0; errno=0;}
      fp->offsets=offsets=newmmap+6;
      fp->offsets_size=fp->load;
#else
      fd_dts_start_read(s);
      fd_setpos(s,12);
      fp->load=load=fd_dtsread_4bytes(s);
      offsets=u8_alloc_n(load,unsigned int);
      fd_setpos(s,24);
      fd_dtsread_ints(s,load,offsets);
      fp->offsets=offsets; fp->offsets_size=load;
#endif
      fd_unlock_struct(fp);}
  else if (level < 2) {
    if (fp->offsets == NULL) return;
    else {
      int retval;
      fd_lock_struct(fp);
#if HAVE_MMAP
      /* Since we were just reading, the buffer was only as big
         as the load, not the capacity. */
      retval=munmap((fp->offsets)-6,4*fp->load+24);
      if (retval<0) {
        u8_log(LOG_WARN,u8_strerror(errno),"file_pool_setcache:munmap %s",fp->cid);
        fp->offsets=NULL; errno=0;}
#else
      u8_free(fp->offsets);
#endif
      fp->offsets=NULL; fp->offsets_size=0;}}
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
  if (lock) fd_lock_struct(fp);
  oscan=fp->offsets; olim=oscan+fp->offsets_size;
  fd_setpos(s,16); new_load=fd_dtsread_4bytes(s);
  nscan=offsets=u8_alloc_n(new_load,unsigned int);
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
  if (lock) fd_unlock_struct(fp);
#endif
}

static void file_pool_close(fd_pool p)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  fd_lock_struct(fp);
  fd_dtsclose(&(fp->stream),1);
  if (fp->offsets) {
#if HAVE_MMAP
    /* Since we were just reading, the buffer was only as big
       as the load, not the capacity. */
    int retval=munmap((fp->offsets)-6,4*fp->offsets_size+24);
    if (retval<0) {
      u8_log(LOG_WARN,u8_strerror(errno),"file_pool_close:munmap %s",fp->cid);
      errno=0;}
#else
    u8_free(fp->offsets);
#endif
    fp->offsets=NULL; fp->offsets_size=0;
    fp->cache_level=-1;}
  fd_unlock_struct(fp);
}

static void file_pool_setbuf(fd_pool p,int bufsiz)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  fd_lock_struct(fp);
  fd_dtsbufsize(&(fp->stream),bufsiz);
  fd_unlock_struct(fp);
}

static fdtype file_pool_metadata(fd_pool p,fdtype md)
{
  struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
  if (FD_VOIDP(md))
    return fd_read_pool_metadata(&(fp->stream));
  else return fd_write_pool_metadata((&(fp->stream)),md);
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
  file_pool_metadata, /* metadata */
  NULL}; /* sync */

/* Module (file) Initialization */

FD_EXPORT void fd_init_file_pools_c()
{
  u8_register_source_file(_FILEINFO);

  fd_register_pool_opener
    (FD_FILE_POOL_MAGIC_NUMBER,
     open_std_file_pool,fd_read_pool_metadata,fd_write_pool_metadata);
  fd_register_pool_opener
    (FD_FILE_POOL_TO_RECOVER,
     open_std_file_pool,fd_read_pool_metadata,fd_write_pool_metadata);
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
