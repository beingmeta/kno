/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"
#define KNO_INLINE_POOLS 1
#define KNO_INLINE_BUFIO 1

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/numbers.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/streams.h"
#include "kno/drivers.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>

#include "headers/filepool.h"

#include <errno.h>
#include <sys/stat.h>

#if (KNO_USE_MMAP)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#define MMAP_FLAGS (MAP_SHARED|MAP_NORESERVE)
#endif

#if ((KNO_USE_MMAP) && (!(WORDS_BIGENDIAN)))
#define offget(offvec,offset) (kno_flip_word((offvec)[offset]))
#define set_offset(offvec,offset,v) (offvec)[offset]=(kno_flip_word(v))
#else
#define offget(offvec,offset) ((offvec)[offset])
#define set_offset(offvec,offset,v) (offvec)[offset]=(v)
#endif

static void update_modtime(struct KNO_FILE_POOL *fp);
static void reload_file_pool_cache(struct KNO_FILE_POOL *fp,int lock);

static struct KNO_POOL_HANDLER file_pool_handler;

static int recover_file_pool(struct KNO_FILE_POOL *);

static kno_pool open_file_pool(u8_string fname,kno_storage_flags flags,lispval opts)
{
  struct KNO_FILE_POOL *pool = u8_alloc(struct KNO_FILE_POOL);
  int read_only = U8_BITP(flags,KNO_STORAGE_READ_ONLY);

  if ( (read_only == 0) && (u8_file_writablep(fname)) ) {
    if (kno_check_rollback("open_file_pool",fname)<0) {
      /* If we can't apply the rollback, open the file read-only */
      u8_log(LOG_WARN,"RollbackFailed",
             "Opening filepool %s as read-only due to failed rollback",
             fname);
      kno_clear_errors(1);
      read_only=1;}}
  else read_only=1;

  KNO_OID base = KNO_NULL_OID_INIT;
  unsigned int hi, lo, magicno, capacity, load;
  kno_off_t label_loc; lispval label;
  u8_string realpath = u8_realpath(fname,NULL);
  u8_string abspath = u8_abspath(fname,NULL);
  kno_stream_mode mode=
    ((read_only) ? (KNO_FILE_READ) : (KNO_FILE_MODIFY));
  kno_stream s = kno_init_file_stream
    (&(pool->pool_stream),fname,mode,
     ( (read_only) ? (KNO_STREAM_READ_ONLY) : (0) ) |
     KNO_STREAM_CAN_SEEK|
     KNO_STREAM_NEEDS_LOCK,
     kno_driver_bufsize);

  if (s==NULL) {
    u8_seterr(kno_FileNotFound,"open_file_pool",u8dup(fname));
    u8_free(realpath);
    u8_free(abspath);
    return NULL;}

  s->stream_flags &= ~KNO_STREAM_IS_CONSED;
  magicno = kno_read_4bytes_at(s,0,KNO_ISLOCKED);
  hi = kno_read_4bytes_at(s,4,KNO_ISLOCKED);
  lo = kno_read_4bytes_at(s,8,KNO_ISLOCKED);
  KNO_SET_OID_HI(base,hi); KNO_SET_OID_LO(base,lo);
  capacity = kno_read_4bytes_at(s,12,KNO_ISLOCKED);

  kno_init_pool((kno_pool)pool,base,capacity,&file_pool_handler,
               fname,abspath,realpath,flags,VOID,opts);
  u8_free(realpath);
  u8_free(abspath);

  if (magicno == KNO_FILE_POOL_TO_RECOVER) {
    u8_logf(LOG_WARN,kno_RecoveryRequired,"Recovering the file pool %s",fname);
    if (recover_file_pool(pool)<0) {
      kno_seterr(kno_MallocFailed,"open_file_pool",NULL,VOID);
      return NULL;}}

  load = kno_read_4bytes_at(s,16,KNO_ISLOCKED);
  if (load > capacity) {
    u8_logf(LOG_CRIT,"LoadOverFlow",
            "The filepool %s specifies a load (%u) > its capacity (%u)",
            fname,load,capacity);
    pool->pool_load=load=capacity;}

  label_loc = (kno_off_t)kno_read_4bytes_at(s,20,KNO_ISLOCKED);
  if (label_loc) {
    if (kno_setpos(s,label_loc)>0) {
      label = kno_read_dtype(kno_readbuf(s));
      if (STRINGP(label))
        pool->pool_label = u8_strdup(CSTRING(label));
      else u8_logf(LOG_WARN,kno_BadFilePoolLabel,kno_lisp2string(label));
      kno_decref(label);}
    else {
      kno_seterr(kno_BadFilePoolLabel,"open_file_pool","bad label loc",
                KNO_INT(label_loc));
      kno_close_stream(&(pool->pool_stream),0);
      u8_free(pool);
      return NULL;}}
  pool->pool_load = load;
  pool->pool_offdata = NULL;
  pool->pool_offdata_size = 0;
  if (read_only)
    U8_SETBITS(pool->pool_flags,KNO_STORAGE_READ_ONLY);
  else U8_CLEARBITS(pool->pool_flags,KNO_STORAGE_READ_ONLY);
  kno_register_pool((kno_pool)pool);
  update_modtime(pool);
  return (kno_pool)pool;
}

static void update_modtime(struct KNO_FILE_POOL *fp)
{
  struct stat fileinfo;
  if ((fstat(fp->pool_stream.stream_fileno,&fileinfo))<0)
    fp->pool_mtime = (time_t)-1;
  else fp->pool_mtime = fileinfo.st_mtime;
}

/* These assume that the pool itself is locked */
static int write_file_pool_load(kno_file_pool fp)
{
  long long load;
  kno_stream stream = &(fp->pool_stream);
  load = kno_read_4bytes_at(stream,16,KNO_ISLOCKED);
  if (load<0) {
    return -1;}
  else if (load>fp->pool_capacity) {
    u8_seterr("InvalidLoad","write_file_pool_load",u8_strdup(fp->poolid));
    return -1;}
  else if (fp->pool_load>load) {
    int rv = kno_write_4bytes_at(stream,fp->pool_load,16);
    if (rv<0) return rv;
    return 1;}
  else {
    return 0;}
}

static int read_file_pool_load(kno_file_pool fp)
{
  long long load;
  kno_stream stream = &(fp->pool_stream);
  if (KNO_POOLSTREAM_LOCKEDP(fp)) {
    return fp->pool_load;}
  else if (fp->pool_load == fp->pool_capacity)
    return fp->pool_load;
  else if (kno_streamctl(stream,kno_stream_lockfile,NULL)<0)
    return -1;
  load = kno_read_4bytes_at(stream,16,KNO_ISLOCKED);
  if (load<0) {
    kno_streamctl(stream,kno_stream_unlockfile,NULL);
    return -1;}
  else if (load>fp->pool_capacity) {
    u8_seterr("InvalidLoad","read_file_pool_load",u8_strdup(fp->poolid));
    kno_streamctl(stream,kno_stream_unlockfile,NULL);
    return -1;}
  kno_streamctl(stream,kno_stream_unlockfile,NULL);
  fp->pool_load = load;
  return load;
}

static int file_pool_load(kno_pool p)
{
  kno_file_pool fp = (kno_file_pool)p;
  if (KNO_POOLSTREAM_LOCKEDP(fp))
    return fp->pool_load;
  else {
    int pool_load;
    kno_lock_pool_struct(p,1);
    kno_lock_stream(&(fp->pool_stream));
    pool_load = read_file_pool_load(fp);
    kno_unlock_stream(&(fp->pool_stream));
    kno_unlock_pool_struct(p);
    return pool_load;}
}

static int lock_file_pool(struct KNO_FILE_POOL *fp,int use_mutex)
{
  if (KNO_POOLSTREAM_LOCKEDP(fp)) return 1;
  else if ((fp->pool_flags)&(KNO_STORAGE_READ_ONLY))
    return 0;
  else {
    struct KNO_STREAM *s = &(fp->pool_stream);
    struct stat fileinfo;
    if (use_mutex) kno_lock_pool_struct((kno_pool)fp,1);
    /* Handle race condition by checking when locked */
    if (KNO_POOLSTREAM_LOCKEDP(fp)) {
      if (use_mutex) kno_unlock_pool_struct((kno_pool)fp);
      return 1;}
    if (kno_streamctl(s,kno_stream_lockfile,NULL)==0) {
      if (use_mutex) kno_unlock_pool_struct((kno_pool)fp);
      return 0;}
    fstat(s->stream_fileno,&fileinfo);
    if (fileinfo.st_mtime>fp->pool_mtime) {
      /* Make sure we're up to date. */
      if (fp->pool_offdata) reload_file_pool_cache(fp,0);
      else {
        kno_reset_hashtable(&(fp->pool_cache),-1,1);
        kno_reset_hashtable(&(fp->pool_changes),32,1);}}
    read_file_pool_load(fp);
    if (use_mutex) kno_unlock_pool_struct((kno_pool)fp);
    return 1;}
}

static lispval file_pool_fetch(kno_pool p,lispval oid)
{
  lispval value;
  struct KNO_FILE_POOL *fp = (struct KNO_FILE_POOL *)p;
  KNO_OID addr = KNO_OID_ADDR(oid);
  int offset = KNO_OID_DIFFERENCE(addr,fp->pool_base), stream_locked = 0;
  kno_stream stream = &(fp->pool_stream);
  kno_off_t data_pos;
  kno_lock_pool_struct((kno_pool)fp,0);
  if (PRED_FALSE(offset>=fp->pool_load)) {
    kno_unlock_pool_struct((kno_pool)fp);
    if ( (p->pool_flags) & (KNO_POOL_ADJUNCT) )
      return KNO_EMPTY;
    else return KNO_UNALLOCATED_OID;}
  else if (fp->pool_offdata)
    data_pos = offget(fp->pool_offdata,offset);
  else {
    kno_lock_stream(stream); stream_locked = 1;
    if (kno_setpos(stream,24+4*offset)<0) {
      kno_unlock_stream(stream);
      kno_unlock_pool_struct((kno_pool)fp);
      return KNO_ERROR;}
    data_pos = kno_read_4bytes(kno_readbuf(stream));}
  if (data_pos == 0) value = EMPTY;
  else if (PRED_FALSE(data_pos<24+fp->pool_load*4)) {
    /* We got a data pointer into the file header.  This will
       happen in the (hopefully now non-existent) case where
       we've stored a >32 bit offset into a 32-bit sized location
       and it got truncated down. */
    kno_unlock_pool_struct((kno_pool)fp);
    if (stream_locked) kno_unlock_stream(stream);
    return kno_err(kno_CorruptedPool,"file_pool_fetch",fp->poolid,VOID);}
  else {
    if (!(stream_locked)) {
      kno_lock_stream(stream); stream_locked = 1;}
    if (kno_setpos(&(fp->pool_stream),data_pos)<0) {
      kno_unlock_stream(stream);
      kno_unlock_pool_struct((kno_pool)fp);
      return KNO_ERROR;}
    value = kno_read_dtype(kno_readbuf(stream));}
  if (stream_locked) kno_unlock_stream(stream);
  kno_unlock_pool_struct((kno_pool)fp);
  return value;
}

struct POOL_FETCH_SCHEDULE {
  unsigned int vpos; kno_off_t filepos;};

static int compare_filepos(const void *x1,const void *x2)
{
  const struct POOL_FETCH_SCHEDULE *s1 = x1, *s2 = x2;
  if (s1->filepos<s2->filepos) return -1;
  else if (s1->filepos>s2->filepos) return 1;
  else return 0;
}

static lispval *file_pool_fetchn(kno_pool p,int n,lispval *oids)
{
  struct KNO_FILE_POOL *fp = (struct KNO_FILE_POOL *)p; KNO_OID base = p->pool_base;
  struct KNO_STREAM *stream = &(fp->pool_stream);
  struct POOL_FETCH_SCHEDULE *schedule=
    u8_big_alloc_n(n,struct POOL_FETCH_SCHEDULE);
  lispval *result = u8_big_alloc_n(n,lispval);
  int i = 0, min_file_pos = 24+fp->pool_capacity*4, load;
  kno_lock_pool_struct(p,0);
  load = fp->pool_load;
  if (fp->pool_offdata) {
    unsigned int *offsets = fp->pool_offdata;
    int i = 0; while (i < n) {
      lispval oid = oids[i]; KNO_OID addr = KNO_OID_ADDR(oid);
      unsigned int off = KNO_OID_DIFFERENCE(addr,base), file_off;
      if (PRED_FALSE(off>=load)) {
        u8_big_free(result);
        u8_big_free(schedule);
        kno_unlock_pool_struct(p);
        kno_seterr(kno_UnallocatedOID,"file_pool_fetchn",fp->poolid,oid);
        return NULL;}
      file_off = offget(offsets,off);
      schedule[i].vpos = i;
      if (file_off==0)
        schedule[i].filepos = file_off;
      else if (PRED_FALSE(file_off<min_file_pos)) {
        /* As above, we have a data pointer into the header.
           This should never happen unless a file is corrupted. */
        u8_big_free(result);
        u8_big_free(schedule);
        kno_unlock_pool_struct(p);
        kno_seterr(kno_CorruptedPool,"file_pool_fetchn",fp->poolid,oid);
        return NULL;}
      else schedule[i].filepos = file_off;
      i++;}
    kno_lock_stream(stream);}
  else {
    int i = 0; kno_lock_stream(stream);
    while (i < n) {
      lispval oid = oids[i]; KNO_OID addr = KNO_OID_ADDR(oid);
      unsigned int off = KNO_OID_DIFFERENCE(addr,base), file_off;
      schedule[i].vpos = i;
      if (kno_setpos(stream,24+4*off)<0) {
        u8_big_free(schedule);
        u8_big_free(result);
        kno_unlock_stream(stream);
        kno_unlock_pool_struct(p);
        return NULL;}
      file_off = kno_read_4bytes(kno_readbuf(stream));
      if (PRED_FALSE(file_off==0))
        /* This is okay, just an allocated but unassigned OID. */
        schedule[i].filepos = file_off;
      else if (PRED_FALSE(file_off<min_file_pos)) {
        /* As above, we have a data pointer into the header.
           This should never happen unless a file is corrupted. */
        u8_big_free(result);
        u8_big_free(schedule);
        kno_unlock_stream(stream);
        kno_unlock_pool_struct(p);
        kno_seterr(kno_CorruptedPool,"file_pool_fetchn",fp->poolid,oid);
        return NULL;}
      else schedule[i].filepos = file_off;
      i++;}}
  qsort(schedule,n,sizeof(struct POOL_FETCH_SCHEDULE),
        compare_filepos);
  i = 0; while (i < n)
           if (schedule[i].filepos) {
             if (kno_setpos(stream,schedule[i].filepos)<0) {
               int j = 0; while (j<i) {
                 kno_decref(result[schedule[j].vpos]); j++;}
               u8_big_free(schedule);
               u8_big_free(result);
               kno_unlock_pool_struct(p);
               kno_unlock_stream(stream);
               return NULL;}
             result[schedule[i].vpos]=kno_read_dtype(kno_readbuf(stream));
             i++;}
           else result[schedule[i++].vpos]=EMPTY;
  u8_big_free(schedule);
  kno_unlock_stream(stream);
  kno_unlock_pool_struct(p);
  return result;
}

static int file_pool_storen(kno_pool p,int n,lispval *oids,lispval *values)
{
  KNO_OID base = p->pool_base;
  int isadjunct = (p->pool_flags) & (KNO_POOL_ADJUNCT);
  struct KNO_FILE_POOL *fp = (struct KNO_FILE_POOL *)p;
  /* This stores the offset where the DTYPE representation of each changed OID
     has been written, indexed by the OIDs position in *oids. */
  unsigned int *changed_offsets = u8_big_alloc_n(n,unsigned int);
  struct KNO_STREAM *stream = &(fp->pool_stream);
  struct KNO_OUTBUF *outstream = kno_writebuf(stream);
  /* Make sure that pos_limit fits into an int, in case kno_off_t is an int. */
  kno_off_t endpos, pos_limit = 0xFFFFFFFF;
  unsigned int *tmp_offsets = NULL, old_size = 0;
  int i = 0, retcode = n, load;
  double started = u8_elapsed_time();
  kno_lock_pool_struct(p,1);
  load = fp->pool_load;
  /* Get the endpos after the file pool structure is locked. */
  kno_lock_stream(stream);
  endpos = kno_endpos(stream);
  while (i<n) {
    KNO_OID oid = KNO_OID_ADDR(oids[i]);
    unsigned int oid_off = KNO_OID_DIFFERENCE(oid,base);
    int delta = kno_write_dtype(kno_writebuf(stream),values[i]);
    if (PRED_FALSE((!isadjunct) && (oid_off>=load))) {
      kno_seterr(kno_UnallocatedOID,
                "file_pool_storen",fp->poolid,
                oids[i]);
      retcode = -1; break;}
    else if (PRED_FALSE(delta<0)) {retcode = -1; break;}
    else if (PRED_FALSE(((kno_off_t)(endpos+delta))>pos_limit)) {
      kno_seterr(kno_PoolFileSizeOverflow,
                "file_pool_storen",fp->poolid,
                oids[i]);
      retcode = -1; break;}
    if ( (isadjunct) && (oid_off >= load) ) load = oid_off+1;
    changed_offsets[i]=endpos;
    endpos = endpos+delta;
    i++;}
  fp->pool_load = load;
  if (retcode<0) {}
  else if (fp->pool_offdata) {
    int i = 0;
    unsigned int *old_offsets = fp->pool_offdata;
    old_size = fp->pool_offdata_size;
    tmp_offsets = u8_big_alloc_n(load,unsigned int);
    /* Initialize tmp_offsets from the current offsets */
    if (KNO_USE_MMAP) {
      /* If we're mmapped, the latest values are there. */
      while (i<old_size) {
        tmp_offsets[i]=offget(old_offsets,i); i++;}
      while (i<load) tmp_offsets[i++]=0;}
    else {
      /* Otherwise, we just copy them from the old offsets. */
      memcpy(tmp_offsets,old_offsets,sizeof(unsigned int)*old_size);
      memset(tmp_offsets+old_size,0,sizeof(unsigned int)*(load-old_size));}
    /* Write the changes */
    i = 0; while (i<n) {
      KNO_OID addr = KNO_OID_ADDR(oids[i]);
      unsigned int oid_off = KNO_OID_DIFFERENCE(addr,base);
      tmp_offsets[oid_off]=changed_offsets[i];
      i++;}
    u8_big_free(changed_offsets);
    /* Now write the real data */
    kno_setpos(stream,24);
    kno_write_ints(stream,fp->pool_load,tmp_offsets);}
  else {
    /* If we don't have an offsets cache, we don't bother
       with ACID and just write the changed offsets directly */
    int i = 0; while (i<n) {
      KNO_OID addr = KNO_OID_ADDR(oids[i]);
      unsigned int reloff = KNO_OID_DIFFERENCE(addr,base);
      if (kno_setpos(stream,24+4*reloff)<0) {
        retcode = -1; break;}
      kno_write_4bytes(outstream,changed_offsets[i]);
      i++;}
    u8_big_free(changed_offsets);}
  if (retcode>=0) {
    /* Now we update the load and do other cleanup.  */
    if (write_file_pool_load(fp)<0)
      u8_logf(LOG_CRIT,"FileError","Can't update load for %s",fp->poolid);
    update_modtime(fp);
    /* Now, we set the file's magic number back to something
       that doesn't require recovery and truncate away the saved
       recovery information. */
    if (fp->pool_offdata) {
      kno_off_t end = kno_endpos(stream); int retval;
      kno_setpos(stream,0);
      kno_flush_stream(stream);
      fsync(stream->stream_fileno);
      kno_endpos(stream);
      kno_movepos(stream,-(4*(fp->pool_capacity+1)));
      retval = ftruncate(stream->stream_fileno,end-(4*(fp->pool_capacity+1)));
      if (retval<0) {
        retcode = -1; u8_graberr(errno,"file_pool_storen",fp->poolid);}}
    else kno_flush_stream(stream);
    /* Update the offsets, if you have any */
    if (fp->pool_offdata == NULL) {}
    else if (KNO_USE_MMAP) {
      int retval = munmap((fp->pool_offdata)-6,4*old_size+24);
      unsigned int *newmmap;
      if (retval<0) {
        u8_logf(LOG_WARN,u8_strerror(errno),"file_pool_storen:munmap %s",fp->poolid);
        fp->pool_offdata = NULL; errno = 0;}
      newmmap = mmap(NULL,(4*fp->pool_load)+24,PROT_READ,
                     MAP_SHARED|MAP_NORESERVE,stream->stream_fileno,0);
      if ((newmmap == NULL) || (newmmap == MAP_FAILED)) {
        u8_logf(LOG_WARN,u8_strerror(errno),"file_pool_storen:mmap %s",fp->poolid);
        fp->pool_offdata = NULL; fp->pool_offdata_size = 0; errno = 0;}
      else {
        fp->pool_offdata = newmmap+6;
        fp->pool_offdata_size = fp->pool_load;}
      u8_big_free(tmp_offsets);}
    else {
      u8_big_free(fp->pool_offdata);
      fp->pool_offdata = tmp_offsets;
      fp->pool_offdata_size = fp->pool_load;}}
  /* Note that if we exited abnormally, the file is still intact. */
  kno_unlock_stream(stream);
  kno_unlock_pool_struct(p);
  u8_logf(LOG_NOTICE,"FilePoolStore",
          _("Stored %d oid values in oidpool %s in %f seconds"),
          n,p->poolid,u8_elapsed_time()-started);
  return retcode;
}

static int file_pool_commit(kno_pool p,kno_commit_phase phase,
                            struct KNO_POOL_COMMITS *commits)
{
  switch (phase) {
  case kno_commit_start: {
    u8_string source = p->pool_source;
    struct KNO_FILE_POOL *fp = (kno_file_pool) p;
    if (!(KNO_POOLSTREAM_LOCKEDP(fp))) {
      int locked = kno_streamctl(&(fp->pool_stream),kno_stream_lockfile,NULL);
      if (locked <= 0) {
        u8_seterr("LockFailed","file_pool_commit",u8_strdup(source));
        return -1;}
      else {
        commits->commit_stream = &(fp->pool_stream);}}
    return kno_write_rollback("filepool_commit",p->poolid,source,
                             (24+(4*p->pool_capacity)));}
  case kno_commit_write: {
    return file_pool_storen(p,commits->commit_count,
                            commits->commit_oids,
                            commits->commit_vals);}
  case kno_commit_sync:
    return 0;
  case kno_commit_rollback: {
    u8_string source = p->pool_source;
    u8_string rollback_file = u8_mkstring("%s.rollback",source);
    if (u8_file_existsp(rollback_file)) {
      ssize_t rv= kno_apply_head(rollback_file,source);
      u8_free(rollback_file);
      if (rv<0) return -1; else return 1;}
    else {
      u8_logf(LOG_CRIT,"NoRollbackFile",
              "The rollback file %s for %s doesn't exist",
              rollback_file,p->poolid);
      u8_free(rollback_file);
      return -1;}}
  case kno_commit_cleanup: {
    u8_string source = p->pool_source;
    u8_string rollback_file = u8_mkstring("%s.rollback",source);
    if (commits->commit_stream) {
      int unlocked = kno_streamctl(commits->commit_stream,kno_stream_unlockfile,NULL);
      if (unlocked<=0) {
        int saved_errno = errno; errno=0;
        u8_logf(LOG_WARN,"CantUnlock",
                "Can't unlock stream for %s errno=%d:%s",
                source,saved_errno,u8_strerror(saved_errno));}}
    if (u8_file_existsp(rollback_file)) {
      int rv = u8_removefile(rollback_file);
      u8_free(rollback_file);
      return rv;}
    else {
      u8_logf(LOG_WARN,"Rollback file %s was deleted",rollback_file);
      u8_free(rollback_file);
      return -1;}}
  default: {
    u8_logf(LOG_INFO,"NoPhasedCommit",
            "The pool %s doesn't support phased commits",
            p->poolid);
    return 0;}
  }
}

static int recover_file_pool(struct KNO_FILE_POOL *fp)
{
  /* This reads the offsets vector written at the end of the file
     during commitment. */
  int i = 0, len = fp->pool_capacity, load; kno_off_t new_end, retval;
  unsigned int *offsets = u8_big_alloc(4*len);
  struct KNO_STREAM *s = &(fp->pool_stream);
  struct KNO_INBUF *instream = kno_readbuf(s);
  struct KNO_OUTBUF *outstream;
  kno_lock_stream(s);
  kno_endpos(s);
  new_end = kno_movepos(s,-(4+4*len));
  load = kno_read_4bytes(instream);
  while (i<len) {
    offsets[i]=kno_read_4bytes(instream); i++;}
  kno_setpos(s,16);
  outstream = kno_writebuf(s);
  kno_write_4bytes(outstream,load);
  kno_setpos(s,24);
  i = 0; while (i<len) {
    kno_write_4bytes(outstream,offsets[i]); i++;}
  kno_setpos(s,0);
  kno_write_4bytes(outstream,KNO_FILE_POOL_MAGIC_NUMBER);
  kno_flush_stream(s); fp->pool_load = load;
  retval = ftruncate(s->stream_fileno,new_end);
  kno_unlock_stream(s);
  if (retval<0) return retval;
  else retval = fsync(s->stream_fileno);
  return retval;
}

static lispval file_pool_alloc(kno_pool p,int n)
{
  lispval results = EMPTY; int i = 0;
  struct KNO_FILE_POOL *fp = (struct KNO_FILE_POOL *)p;
  KNO_OID base=fp->pool_base;
  unsigned int start;
  kno_lock_pool_struct(p,1);
  if (!(KNO_POOLSTREAM_LOCKEDP(fp))) lock_file_pool(fp,0);
  if ( (fp->pool_load+n) > fp->pool_capacity ) {
    kno_unlock_pool_struct(p);
    return kno_err(kno_ExhaustedPool,"file_pool_alloc",p->poolid,VOID);}
  start=fp->pool_load; fp->pool_load+=n;
  kno_unlock_pool_struct(p);
  while (i < n) {
    KNO_OID new_addr = KNO_OID_PLUS(base,start+i);
    lispval new_oid = kno_make_oid(new_addr);
    CHOICE_ADD(results,new_oid);
    i++;}
  return results;
}

static int file_pool_lock(kno_pool p,lispval oids)
{
  struct KNO_FILE_POOL *fp = (struct KNO_FILE_POOL *)p;
  if (KNO_POOLSTREAM_LOCKEDP(fp)) return 1;
  else return lock_file_pool(fp,1);
}

static int file_pool_unlock(kno_pool p,lispval oids)
{
  struct KNO_FILE_POOL *fp = (struct KNO_FILE_POOL *)p;
  kno_lock_pool_struct(p,1);
  if (fp->pool_changes.table_n_keys == 0)
    kno_streamctl(&(fp->pool_stream),kno_stream_unlockfile,NULL);
  kno_unlock_pool_struct(p);
  return 1;
}

static void file_pool_setcache(kno_pool p,int level)
{
  struct KNO_FILE_POOL *fp = (struct KNO_FILE_POOL *)p;
  if (level == 2)
    if (fp->pool_offdata) return;
    else {
      kno_stream s = &(fp->pool_stream);
      unsigned int *offsets, *newmmap;
      kno_lock_pool_struct(p,1);
      if (fp->pool_offdata) {
        kno_unlock_pool_struct(p);
        return;}
#if KNO_USE_MMAP
      newmmap=
        /* When allocating an offset buffer to read, we only have to make it as
           big as the file pools load. */
        mmap(NULL,(4*fp->pool_load)+24,PROT_READ,
             MAP_SHARED|MAP_NORESERVE,s->stream_fileno,0);
      if ((newmmap == NULL) || (newmmap == MAP_FAILED)) {
        u8_logf(LOG_WARN,u8_strerror(errno),"file_pool_setcache:mmap %s",
                fp->poolid);
        fp->pool_offdata = NULL;
        fp->pool_offdata_size = 0;
        errno = 0;}
      fp->pool_offdata = offsets = newmmap+6;
      fp->pool_offdata_size = fp->pool_load;
#else
      kno_inbuf ins = kno_readbuf(s);
      kno_setpos(s,12);
      fp->pool_load = load = kno_read_4bytes(ins);
      offsets = u8_big_alloc_n(load,unsigned int);
      kno_setpos(s,24);
      kno_read_ints(ins,load,offsets);
      fp->pool_offdata = offsets; fp->pool_offdata_size = load;
#endif
      kno_unlock_pool_struct(p);}
  else if (level < 2) {
    if (fp->pool_offdata == NULL) return;
    else {
      int retval;
      kno_lock_pool_struct(p,1);
#if KNO_USE_MMAP
      /* Since we were just reading, the buffer was only as big
         as the load, not the capacity. */
      retval = munmap((fp->pool_offdata)-6,4*fp->pool_load+24);
      if (retval<0) {
        u8_logf(LOG_WARN,
                u8_strerror(errno),"file_pool_setcache:munmap %s",
                fp->poolid);
        fp->pool_offdata = NULL; errno = 0;}
#else
      u8_big_free(fp->pool_offdata);
#endif
      fp->pool_offdata = NULL; fp->pool_offdata_size = 0;}}
}

static void reload_file_pool_cache(struct KNO_FILE_POOL *fp,int lock)
{
#if KNO_USE_MMAP
  /* This should grow the offsets if the load has changed. */
#else
  kno_stream s = &(fp->pool_stream);
  kno_inbuf ins = kno_readbuf(s);
  /* Read new offsets table, compare it with the current, and
     only void those OIDs */
  unsigned int new_load, *offsets, *nscan, *oscan, *olim;
  if (lock) kno_lock_pool_struct(p,1);
  oscan = fp->pool_offdata; olim = oscan+fp->pool_offdata_size;
  kno_setpos(s,16); new_load = kno_read_4bytes(ins);
  nscan = offsets = u8_big_alloc_n(new_load,unsigned int);
  kno_setpos(s,24);
  kno_read_ints(ins,new_load,offsets);
  while (oscan < olim)
    if (*oscan == *nscan) {oscan++; nscan++;}
    else {
      KNO_OID addr = KNO_OID_PLUS(fp->pool_base,(nscan-offsets));
      lispval changed_oid = kno_make_oid(addr);
      kno_hashtable_op(&(fp->pool_cache),kno_table_replace,changed_oid,VOID);
      oscan++; nscan++;}
  u8_big_free(fp->pool_offdata);
  fp->pool_offdata = offsets;
  fp->pool_load = fp->pool_offdata_size = new_load;
  update_modtime(fp);
  if (lock) kno_unlock_pool_struct(p);
#endif
}

static void file_pool_close(kno_pool p)
{
  struct KNO_FILE_POOL *fp = (struct KNO_FILE_POOL *)p;
  kno_lock_pool_struct(p,1);
  /* Finish delete */
  /*
    if (write_file_pool_load(fp)<0)
    u8_logf(LOG_CRIT,"FileError","Can't update load for %s",fp->poolid);
  */
  kno_close_stream(&(fp->pool_stream),0);
  if (fp->pool_offdata) {
#if KNO_USE_MMAP
    /* Since we were just reading, the buffer was only as big
       as the load, not the capacity. */
    int retval = munmap((fp->pool_offdata)-6,4*fp->pool_offdata_size+24);
    if (retval<0) {
      u8_logf(LOG_WARN,u8_strerror(errno),"file_pool_close:munmap %s",fp->poolid);
      errno = 0;}
#else
    u8_big_free(fp->pool_offdata);
#endif
    fp->pool_offdata = NULL; fp->pool_offdata_size = 0;
    fp->pool_cache_level = -1;}
  kno_unlock_pool_struct(p);
}

/* Making file pools */

KNO_EXPORT
/* kno_make_file_pool:
   Arguments: a filename string, a magic number (usigned int), an KNO_OID,
   a capacity, and a dtype pointer to a metadata description (a slotmap).
   Returns: -1 on error, 1 on success. */
int kno_make_file_pool
(u8_string filename,unsigned int magicno,
 KNO_OID base,unsigned int capacity,unsigned int load)
{
  int i, hi, lo;
  if (load>capacity) {
    u8_seterr("LoadOverFlow","make_bigpool",
              u8_sprintf(NULL,256,
                         "Specified load (%u) > capacity (%u) for '%s'",
                         load,capacity,filename));
    return -1;}

  struct KNO_STREAM _stream;
  struct KNO_STREAM *stream=
    kno_init_file_stream(&_stream,filename,KNO_FILE_CREATE,-1,kno_driver_bufsize);
  struct KNO_OUTBUF *outstream = (stream) ? (kno_writebuf(stream)) : (NULL);

  if (outstream == NULL) return -1;
  else if ((stream->stream_flags)&KNO_STREAM_READ_ONLY) {
    kno_seterr3(kno_CantWrite,"kno_make_file_pool",filename);
    kno_free_stream(stream);
    return -1;}

  u8_logf(LOG_INFO,"CreateFilePool",
          "Creating a file pool '%s' for %u OIDs based at %x/%x",
          filename,capacity,KNO_OID_HI(base),KNO_OID_LO(base));

  stream->stream_flags &= ~KNO_STREAM_IS_CONSED;
  kno_setpos(stream,0);
  hi = KNO_OID_HI(base); lo = KNO_OID_LO(base);
  kno_write_4bytes(outstream,magicno);
  kno_write_4bytes(outstream,hi);
  kno_write_4bytes(outstream,lo);
  kno_write_4bytes(outstream,capacity);
  kno_write_4bytes(outstream,load); /* load */
  kno_write_4bytes(outstream,0); /* label pos */
  i = 0; while (i<capacity) {kno_write_4bytes(outstream,0); i++;}
  /* Write an initially empty metadata block */
  kno_write_4bytes(outstream,0xFFFFFFFE);
  kno_write_4bytes(outstream,40);
  i = 0; while (i<8) {kno_write_4bytes(outstream,0); i++;}
  kno_close_stream(stream,KNO_STREAM_FREEDATA);
  return 1;
}

static kno_pool filepool_create(u8_string spec,void *type_data,
                               kno_storage_flags flags,lispval opts)
{
  lispval base_oid = kno_getopt(opts,kno_intern("base"),VOID);
  lispval capacity_arg = kno_getopt(opts,kno_intern("capacity"),VOID);
  lispval load_arg = kno_getopt(opts,kno_intern("load"),KNO_FIXZERO);
  unsigned int capacity, load;
  unsigned int magic_number = (unsigned int)((unsigned long)type_data);
  int rv = 0;
  if (u8_file_existsp(spec)) {
    kno_seterr(_("FileAlreadyExists"),"filepool_create",spec,VOID);
    return NULL;}
  else if (!(OIDP(base_oid))) {
    kno_seterr("Not a base oid","filepool_create",spec,base_oid);
    rv = -1;}
  else if (KNO_ISINT(capacity_arg)) {
    int capval = kno_getint(capacity_arg);
    if (capval<=0) {
      kno_seterr("Not a valid capacity","filepool_create",
                spec,capacity_arg);
      rv = -1;}
    else capacity = capval;}
  else {
    kno_seterr("Not a valid capacity","filepool_create",
              spec,capacity_arg);
    rv = -1;}
  if (rv<0) {}
  else if (KNO_ISINT(load_arg)) {
    int loadval = kno_getint(load_arg);
    if (loadval<0) {
      kno_seterr("Not a valid load","filepool_create",
                spec,load_arg);
      rv = -1;}
    else if (loadval > capacity) {
      kno_seterr("Not a valid load","filepool_create",
                spec,load_arg);
      rv = -1;}
    else load = loadval;}
  else if ( (FALSEP(load_arg)) || (EMPTYP(load_arg)) ||
            (VOIDP(load_arg)) || (load_arg == KNO_DEFAULT_VALUE))
    load=0;
  else {
    kno_seterr("Not a valid load","filepool_create",
              spec,load_arg);
    rv = -1;}
  if (rv<0) return NULL;
  else rv = kno_make_file_pool(spec,magic_number,
                              KNO_OID_ADDR(base_oid),capacity,load);
  if (rv>=0) {
    kno_set_file_opts(spec,opts);
    return kno_open_pool(spec,flags,opts);}
  else return NULL;
}

/* FILEPOOL get oids */

static lispval filepool_getoids(kno_file_pool fp)
{
  if (fp->pool_cache_level<0) {
    kno_pool_setcache((kno_pool)fp,kno_default_cache_level);}
  lispval results = EMPTY;
  KNO_OID base = fp->pool_base;
  unsigned int i=0, load=fp->pool_load;
  kno_stream stream = &(fp->pool_stream);
  if (fp->pool_offdata) {
    unsigned int *offdata = fp->pool_offdata;
    while (i<load) {
      unsigned int data_off = offdata[i];
      if (data_off>0) {
        KNO_OID addr = KNO_OID_PLUS(base,i);
        KNO_ADD_TO_CHOICE(results,kno_make_oid(addr));}
      i++;}}
  else while (i<load) {
      unsigned int off =
        kno_read_4bytes_at(stream,24+(sizeof(unsigned int)*i),KNO_ISLOCKED);
      if (off>0) {
        KNO_OID addr = KNO_OID_PLUS(base,i);
        KNO_ADD_TO_CHOICE(results,kno_make_oid(addr));}
      i++;}
  return results;
}


/* File pool ops function */

static lispval label_file_pool(struct KNO_FILE_POOL *fp,lispval label);

static lispval file_pool_ctl(kno_pool p,lispval op,int n,lispval *args)
{
  struct KNO_FILE_POOL *fp = (struct KNO_FILE_POOL *)p;
  if ((n>0)&&(args == NULL))
    return kno_err("BadPoolOpCall","filepool_op",fp->poolid,VOID);
  else if (n<0)
    return kno_err("BadPoolOpCall","filepool_op",fp->poolid,VOID);
  else if (op == kno_cachelevel_op) {
    if (n==0)
      return KNO_INT(fp->pool_cache_level);
    else {
      lispval arg = (args)?(args[0]):(VOID);
      if ((FIXNUMP(arg))&&(FIX2INT(arg)>=0)&&
          (FIX2INT(arg)<0x100)) {
        file_pool_setcache(p,FIX2INT(arg));
        return KNO_INT(fp->pool_cache_level);}
      else return kno_type_error
             (_("cachelevel"),"filepool_op/cachelevel",arg);}}
  else if (op == kno_label_op) {
    if (n==0) {
      if (!(fp->pool_label))
        return KNO_FALSE;
      else return lispval_string(fp->pool_label);}
    else {
      lispval label = args[0];
      if (STRINGP(label))
        return label_file_pool(fp,label);
      else return kno_type_error("pool label","filepool_op/label",label);}}
  else if (op == kno_bufsize_op) {
    if (n==0)
      return KNO_INT(fp->pool_stream.buf.raw.buflen);
    else if (FIXNUMP(args[0])) {
      kno_lock_pool_struct(p,1);
      kno_setbufsize(&(fp->pool_stream),FIX2INT(args[0]));
      kno_unlock_pool_struct(p);
      return KNO_INT(fp->pool_stream.buf.raw.buflen);}
    else return kno_type_error("buffer size","filepool_op/bufsize",args[0]);}
  else if (op == kno_capacity_op)
    return KNO_INT(fp->pool_capacity);
  else if (op == kno_load_op)
    return KNO_INT(fp->pool_load);
  else if (op == kno_keys_op) {
    lispval keys = filepool_getoids(fp);
    return kno_simplify_choice(keys);}
  else if ( (op == kno_metadata_op) && (n == 0) )
    return kno_pool_base_metadata(p);
  else return kno_default_poolctl(p,op,n,args);
}

static lispval label_file_pool(struct KNO_FILE_POOL *fp,lispval label)
{
  int retval = -1;
  if ((KNO_POOLSTREAM_LOCKEDP(fp)) &&
      (kno_lock_stream(&(fp->pool_stream))>0)) {
    kno_stream stream = &(fp->pool_stream);
    kno_off_t endpos = kno_endpos(stream);
    if (endpos>0) {
      kno_outbuf out = kno_writebuf(stream);
      if (kno_write_dtype(out,label)>=0) {
        kno_write_4bytes_at(stream,(unsigned int)endpos,20);
        retval = 1;}}
    kno_unlock_stream(stream);}
  if (retval<0) return KNO_ERROR;
  else return KNO_TRUE;
}


/* The handler struct */

static struct KNO_POOL_HANDLER file_pool_handler={
  "file_pool", 1, sizeof(struct KNO_FILE_POOL), 12,
  file_pool_close, /* close */
  file_pool_alloc, /* alloc */
  file_pool_fetch, /* fetch */
  file_pool_fetchn, /* fetchn */
  file_pool_load, /* getload */
  file_pool_lock, /* lock */
  file_pool_unlock, /* release */
  file_pool_commit, /* commit */
  NULL, /* swapout */
  filepool_create, /* create */
  NULL,  /* walk */
  NULL, /* recycle */
  file_pool_ctl /* poolctl */
};

/* Matching pool names */

/* Module (file) Initialization */

KNO_EXPORT void kno_init_file_pool_c()
{
  u8_register_source_file(_FILEINFO);

  kno_register_pool_type
    ("filepool",
     &file_pool_handler,
     open_file_pool,
     kno_match_pool_file,
     (void *)U8_INT2PTR(KNO_FILE_POOL_MAGIC_NUMBER));
  kno_register_pool_type
    ("damaged_filepool",
     &file_pool_handler,
     open_file_pool,
     kno_match_pool_file,
     (void *)(U8_INT2PTR(KNO_FILE_POOL_TO_RECOVER)));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
