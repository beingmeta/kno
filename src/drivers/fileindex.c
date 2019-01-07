/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/components/storage_layer.h"
#define FD_INLINE_BUFIO 1
#define FD_INLINE_CHOICES 1
#define FD_FAST_CHOICE_CONTAINSP 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

#include "headers/fileindex.h"

#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/libu8io.h>
#include <libu8/u8printf.h>

#include <errno.h>
#include <sys/stat.h>

#if (FD_USE_MMAP)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#define MMAP_FLAGS MAP_SHARED
#endif

#if ((FD_USE_MMAP) && (!(WORDS_BIGENDIAN)))
#define offget(offvec,offset) (fd_flip_word((offvec)[offset]))
#define set_offset(offvec,offset,v) (offvec)[offset]=(fd_flip_word(v))
#else
#define offget(offvec,offset) ((offvec)[offset])
#define set_offset(offvec,offset,v) (offvec)[offset]=(v)
#endif

#define BAD_VALUEP(value)                                               \
  (PRED_FALSE((FD_EODP(value))||(FD_EOFP(value))||(FD_ABORTP(value))))

#define SLOTSIZE (sizeof(unsigned int))

static ssize_t fileindex_default_size=32000;

static lispval set_symbol, drop_symbol, slotids_symbol;
static lispval buckets_slot, slotids_slot;
static struct FD_INDEX_HANDLER fileindex_handler;

static lispval fileindex_fetch(fd_index ix,lispval key);

static fd_index open_fileindex(u8_string fname,fd_storage_flags flags,lispval opts)
{
  struct FD_FILEINDEX *index = u8_alloc(struct FD_FILEINDEX);
  int read_only = U8_BITP(flags,FD_STORAGE_READ_ONLY);

  if ( (read_only == 0) && (u8_file_writablep(fname)) ) {
    if (fd_check_rollback("open_fileindex",fname)<0) {
      /* If we can't apply the rollback, open the file read-only */
      u8_log(LOG_WARN,"RollbackFailed",
             "Opening fileindex %s as read-only due to failed rollback",
             fname);
      fd_clear_errors(1);
      read_only=1;}}
  else read_only=1;

  u8_string abspath = u8_abspath(fname,NULL);
  u8_string realpath = u8_realpath(fname,NULL);

  fd_init_index((fd_index)index,&fileindex_handler,
                fname,abspath,realpath,
                flags,VOID,opts);

  int consed = U8_BITP(flags,FD_STORAGE_UNREGISTERED);
  unsigned int magicno;
  fd_stream_mode mode=
    ((read_only) ? (FD_FILE_READ) : (FD_FILE_MODIFY));
  struct FD_STREAM *s = fd_init_file_stream
    (&(index->index_stream),abspath,mode,
     ((read_only)?(FD_DEFAULT_FILESTREAM_FLAGS|FD_STREAM_READ_ONLY):
      (FD_DEFAULT_FILESTREAM_FLAGS)),
     fd_driver_bufsize);
  u8_free(abspath); u8_free(realpath);

  if (s == NULL) {
    u8_free(index);
    fd_seterr3(u8_CantOpenFile,"open_fileindex",fname);
    return NULL;}

  /* See if it ended up read only */
  if (index->index_stream.stream_flags&FD_STREAM_READ_ONLY) read_only = 1;
  s->stream_flags &= ~FD_STREAM_IS_CONSED;
  magicno = fd_read_4bytes_at(s,0,FD_ISLOCKED);
  index->index_n_slots = fd_read_4bytes_at(s,4,FD_ISLOCKED);
  if (magicno == FD_FILEINDEX_MAGIC_NUMBER) index->index_hashv = 1;
  else if (magicno == FD_MULT_FILEINDEX_MAGIC_NUMBER) index->index_hashv = 2;
  else if (magicno == FD_MULT_FILE3_INDEX_MAGIC_NUMBER) index->index_hashv = 3;
  else {
    fd_seterr3(fd_NotAFileIndex,"open_fileindex",fname);
    u8_free(index);
    return NULL;}
  index->index_offsets = NULL;
  if (read_only)
    U8_SETBITS(index->index_flags,FD_STORAGE_READ_ONLY);
  u8_init_mutex(&(index->index_lock));
  index->slotids = VOID;
  {
    lispval slotids = fileindex_fetch((fd_index)index,slotids_symbol);
    if (!(EMPTYP(slotids)))
      index->slotids = fd_simplify_choice(slotids);}
  if (!(consed)) fd_register_index((fd_index)index);
  return (fd_index)index;
}

static unsigned int get_offset(fd_fileindex ix,int slotno)
{
  fd_stream stream = &(ix->index_stream);
  return fd_read_4bytes(fd_start_read(stream,slotno*4+8));
}

static void fileindex_setcache(fd_index ix,int level)
{
  struct FD_FILEINDEX *fx = (struct FD_FILEINDEX *)ix;
  if (level == 2)
    if (fx->index_offsets) return;
    else {
      fd_stream s = &(fx->index_stream);
      unsigned int *offsets, *newmmap;
      fd_lock_index(fx);
      if (fx->index_offsets) {
        fd_unlock_index(fx);
        return;}
#if FD_USE_MMAP
      newmmap=
        mmap(NULL,(fx->index_n_slots*SLOTSIZE)+8,
             PROT_READ,MMAP_FLAGS,s->stream_fileno,0);
      if ((newmmap == NULL) || (newmmap == MAP_FAILED)) {
        u8_logf(LOG_CRIT,u8_strerror(errno),
                "fileindex_setcache:mmap %s",fx->index_source);
        fx->index_offsets = NULL;
        errno = 0;}
      else fx->index_offsets = offsets = newmmap+2;
#else
      offsets = u8_big_alloc(SLOTSIZE*(fx->index_n_slots));
      fd_start_read(s,8);
      fd_read_ints(s,fx->ht_n_buckets,offsets);
      fx->index_offsets = offsets;
#endif
      fd_unlock_index(fx);}
  else if (level < 2) {
    if (fx->index_offsets == NULL)
      return;
    else {
      int retval;
      fd_lock_index(fx);
#if FD_USE_MMAP
      retval = munmap(fx->index_offsets-2,(fx->index_n_slots*SLOTSIZE)+8);
      if (retval<0) {
        u8_logf(LOG_CRIT,u8_strerror(errno),
                "fileindex_setcache:munmap %s",fx->index_source);
        fx->index_offsets = NULL;
        errno = 0;}
#else
      u8_big_free(fx->index_offsets);
#endif
      fx->index_offsets = NULL;
      fd_unlock_index(fx);}}
}

FD_FASTOP unsigned int fileindex_hash(struct FD_FILEINDEX *fx,lispval x)
{
  switch (fx->index_hashv) {
  case 0: case 1:
    return fd_hash_lisp1(x);
  case 2:
    return fd_hash_lisp2(x);
  case 3:
    return fd_hash_lisp3(x);
  default:
    u8_raise(_("Bad hash version"),"fileindex_hash",fx->indexid);}
  /* Never reached */
  return -1;
}

FD_FASTOP int redundantp(struct FD_FILEINDEX *fx,lispval key)
{
  if ((PAIRP(key)) && (!(VOIDP(fx->slotids)))) {
    lispval slotid = FD_CAR(key), slotids = fx->slotids;
    if ((SYMBOLP(slotid)) || (OIDP(slotid)))
      if ((CHOICEP(slotids)) ?
          (fast_choice_containsp(slotid,(fd_choice)slotids)) :
          (FD_EQ(slotid,slotids)))
        return 0;
      else return 1;
    else return  0;}
  else return 0;
}

static lispval fileindex_fetch(fd_index ix,lispval key)
{
  struct FD_FILEINDEX *fx = (struct FD_FILEINDEX *)ix;
  if (redundantp(fx,key)) return EMPTY;
  else  fd_lock_index(fx);
  {
    fd_stream stream = &(fx->index_stream);
    fd_inbuf instream = fd_readbuf(stream);
    unsigned int hashval = fileindex_hash(fx,key);
    unsigned int n_probes = 0;
    unsigned int probe = hashval%(fx->index_n_slots);
    unsigned int chain_width = (hashval%(fx->index_n_slots-2))+1;
    unsigned int *offsets = fx->index_offsets;
    unsigned int pos_offset = fx->index_n_slots*4;
    unsigned int keypos=
      ((offsets) ? (offget(offsets,probe)) : (get_offset(fx,probe)));
    while (keypos) {
      lispval thiskey; unsigned int n_vals; fd_off_t val_start;
      n_vals = fd_read_4bytes(fd_start_read(stream,keypos+pos_offset));
      val_start = fd_read_4bytes(instream);
      if (PRED_FALSE((n_vals==0) && (val_start)))
        u8_logf(LOG_CRIT,fd_IndexDriverError,
                "fileindex_fetch %s",u8_strdup(ix->indexid));
      thiskey = fd_read_dtype(instream);
      if (LISP_EQUAL(key,thiskey)) {
        if (n_vals==0) {
          u8_unlock_mutex(&fx->index_lock);
          fd_decref(thiskey);
          return EMPTY;}
        else {
          int i = 0, atomicp = 1;
          struct FD_CHOICE *result = fd_alloc_choice(n_vals);
          lispval *values = (lispval *)FD_XCHOICE_DATA(result);
          fd_off_t next_pos = val_start;
          while (next_pos) {
            lispval v;
            if (PRED_FALSE(i>=n_vals))
              u8_raise(_("inconsistent file index"),
                       "fileindex_fetch",u8_strdup(ix->indexid));
            if (next_pos>1) fd_setpos(stream,next_pos+pos_offset);
            v = fd_read_dtype(instream);
            if ((atomicp) && (CONSP(v))) atomicp = 0;
            values[i++]=v;
            next_pos = fd_read_4bytes(instream);}
          u8_unlock_mutex(&fx->index_lock); fd_decref(thiskey);
          return fd_init_choice(result,i,NULL,
                                FD_CHOICE_DOSORT|
                                ((atomicp)?(FD_CHOICE_ISATOMIC):
                                 (FD_CHOICE_ISCONSES))|
                                FD_CHOICE_REALLOC);}}
      else if (n_probes>256) {
        u8_unlock_mutex(&fx->index_lock);
        fd_decref(thiskey);
        return fd_err(fd_FileIndexSizeOverflow,"fileindex_fetch",
                      u8_strdup(fx->index_source),thiskey);}
      else {
        n_probes++;
        fd_decref(thiskey);
        probe = (probe+chain_width)%(fx->index_n_slots);
        keypos = ((offsets) ? (offget(offsets,probe)) : (get_offset(fx,probe)));}}
    u8_unlock_mutex(&fx->index_lock);
    return EMPTY;}
}

/* Fetching sizes */

static int fileindex_fetchsize(fd_index ix,lispval key)
{
  struct FD_FILEINDEX *fx = (struct FD_FILEINDEX *)ix;
  fd_lock_index(fx);
  {
    fd_stream stream = &(fx->index_stream);
    fd_inbuf instream = fd_readbuf(stream);
    unsigned int hashval = fileindex_hash(fx,key);
    unsigned int n_probes = 0;
    unsigned int probe = hashval%(fx->index_n_slots),
      chain_width = (hashval%(fx->index_n_slots-2))+1;
    unsigned int *offsets = fx->index_offsets;
    unsigned int keypos=
      ((offsets) ? (offget(offsets,probe)) : (get_offset(fx,probe)));
    while (keypos) {
      lispval thiskey; unsigned int n_vals; /* fd_off_t val_start; */
      instream = fd_start_read(stream,keypos+(fx->index_n_slots)*4);
      n_vals = fd_read_4bytes(instream);
      /* val_start = */ fd_read_4bytes(instream);
      thiskey = fd_read_dtype(instream);
      if (LISP_EQUAL(key,thiskey)) {
        fd_unlock_index(fx);
        return n_vals;}
      else if (n_probes>256) {
        u8_unlock_mutex(&fx->index_lock);
        return fd_err(fd_FileIndexSizeOverflow,
                      "fileindex_fetchsize",
                      u8_strdup(fx->index_source),VOID);}
      else {n_probes++; probe = (probe+chain_width)%(fx->index_n_slots);}}
    u8_unlock_mutex(&fx->index_lock);
    return EMPTY;}
}

/* Fetching keys */

static int sort_offsets(const void *vox,const void *voy)
{
  const unsigned int *ox = vox, *oy = voy;
  if (*ox<*oy) return -1;
  else if (*ox>*oy) return 1;
  else return 0;
}

static int compress_offsets(unsigned int *offsets,int n)
{
  unsigned int *read = offsets, *write = offsets, *limit = read+n;
  while (read<limit)
    if (*read)
      if (read == write) {read++; write++;}
      else *write++= *read++;
    else read++;
  return write-offsets;
}

static lispval *fileindex_fetchkeys(fd_index ix,int *n)
{
  struct FD_FILEINDEX *fx = (struct FD_FILEINDEX *)ix;
  struct FD_STREAM *stream = &(fx->index_stream);
  unsigned int n_slots, i = 0, pos_offset, *offsets, n_keys;
  fd_lock_index(fx);
  n_slots = fx->index_n_slots;
  offsets = u8_big_alloc(SLOTSIZE*n_slots);
  pos_offset = SLOTSIZE*n_slots;
  fd_start_read(stream,8);
  fd_read_ints(stream,fx->index_n_slots,offsets);
  n_keys = compress_offsets(offsets,fx->index_n_slots);
  if (n_keys==0) {
    *n = n_keys;
    u8_big_free(offsets);
    return NULL;}
  else {
    lispval *keys = u8_big_alloc_n(n_keys,lispval);
    qsort(offsets,n_keys,SLOTSIZE,sort_offsets);
    while (i < n_keys) {
      keys[i]=fd_read_dtype(fd_start_read(stream,pos_offset+offsets[i]+8));
      i++;}
    *n = n_keys;
    u8_big_free(offsets);
    return keys;}
}

static struct FD_KEY_SIZE *fileindex_fetchinfo(fd_index ix,fd_choice filter,int *n)
{
  struct FD_FILEINDEX *fx = (struct FD_FILEINDEX *)ix;
  struct FD_STREAM *stream = &(fx->index_stream);
  struct FD_INBUF *instream = fd_readbuf(stream);
  unsigned int n_slots, i = 0, pos_offset, *offsets, n_keys;
  fd_lock_index(fx);
  n_slots = fx->index_n_slots; offsets = u8_big_alloc(SLOTSIZE*n_slots);
  pos_offset = SLOTSIZE*n_slots;
  fd_start_read(stream,8);
  fd_read_ints(stream,fx->index_n_slots,offsets);
  n_keys = compress_offsets(offsets,fx->index_n_slots);
  if (n_keys==0) {
    *n = 0;
    u8_big_free(offsets);
    return NULL;}
  else {
    struct FD_KEY_SIZE *sizes = u8_big_alloc_n(n_keys,FD_KEY_SIZE);
    unsigned int key_count=0;
    qsort(offsets,n_keys,SLOTSIZE,sort_offsets);
    while (i < n_keys) {
      lispval key; int size;
      instream = fd_start_read(stream,pos_offset+offsets[i]);
      size = fd_read_4bytes(instream);
      /* vpos = */ fd_read_4bytes(instream);
      key = fd_read_dtype(instream);
      if ( (filter == NULL) || (fast_choice_containsp(key,filter)) ) {
        sizes[key_count].keysize_key = key;
        sizes[key_count].keysize_count = size;
        key_count++;}
      else fd_decref(key);
      i++;}
    *n = key_count;
    u8_big_free(offsets);
    return sizes;}
}

/* Fetch N */

/* In the following structures,
   key: is the key being sought;
   index: is the position in the values vector where the value will be
   written; this is negative until the key has been found, and zero
   after the value is complete.
   filepos: is the next file position to look at; this is negative when
   the lookup is done.
   probe: is the slot in the hashtable being considered; when negative,
   the offset of the key entry is being sought (this only makes sense
   when the index offsets are NULL).
   chain_width: how big a step to take when trying new buckets.
*/

struct FETCH_SCHEDULE {
  lispval key; int index; fd_off_t filepos;
  int probe, chain_width;};
struct KEY_FETCH_SCHEDULE {
  lispval key; int index; fd_off_t filepos;
  int probe, chain_width;};
struct VALUE_FETCH_SCHEDULE {
  lispval key; int index;  fd_off_t filepos;
  int probe, n_values;};
union SCHEDULE {
  struct FETCH_SCHEDULE fs;
  struct KEY_FETCH_SCHEDULE ks;
  struct VALUE_FETCH_SCHEDULE vs;};

static int sort_by_filepos(const void *xp,const void *yp)
{
  struct FETCH_SCHEDULE *xs = (struct FETCH_SCHEDULE *)xp;
  struct FETCH_SCHEDULE *ys = (struct FETCH_SCHEDULE *)yp;
  if (xs->filepos<=0)
    if (ys->filepos<=0) return 0; else return 1;
  else if (ys->filepos<=0) return -1;
  else if (xs->filepos<ys->filepos) return -1;
  else if (xs->filepos>ys->filepos) return 1;
  else return 0;
}

static int run_schedule(struct FD_FILEINDEX *fx,int n,
                        union SCHEDULE *schedule,
                        unsigned int *offsets,
                        lispval *values)
{
  struct FD_STREAM *stream = &(fx->index_stream);
  struct FD_INBUF *instream = fd_readbuf(stream);
  unsigned i = 0, pos_offset = fx->index_n_slots*4;
  while (i < n) {
    if (schedule[i].fs.filepos<=0) return i;
    if ((offsets == NULL) && (schedule[i].fs.probe<0)) {
      /* When the probe offset is negative, it means we are reading
         the offset itself. */
      fd_setpos(&(fx->index_stream),schedule[i].fs.filepos);
      schedule[i].fs.filepos = (fd_off_t)(fd_read_4bytes(instream));
      if (schedule[i].fs.filepos)
        schedule[i].fs.probe = (-schedule[i].fs.probe)-1;
      else {
        /* The key has no key entry, thus no values.  Morph it into
           a completed value entry. */
        struct VALUE_FETCH_SCHEDULE *vs = &(schedule[i].vs);
        int real_index = -(vs->index)-1;
        /* This is where it would have been. */
        vs->probe = -(vs->probe)-1;
        if (VOIDP(values[real_index]))
          values[real_index]=EMPTY;
        vs->filepos = -1; vs->index = real_index;
        vs->n_values = 0;}}
    else if (schedule[i].fs.index<0) { /* Still looking for the key */
      unsigned int n_values; fd_off_t vpos; lispval key;
      struct KEY_FETCH_SCHEDULE *ks=
        (struct KEY_FETCH_SCHEDULE *)(&(schedule[i]));
      /* Go to the key location and read the keydata */
      fd_setpos(&(fx->index_stream),schedule[i].fs.filepos+pos_offset);
      n_values = fd_read_4bytes(instream);
      vpos = (fd_off_t)fd_read_4bytes(instream);
      key = fd_read_dtype(instream);
      if (FD_ABORTP(key)) return fd_interr(key);
      else if (LISP_EQUAL(key,schedule[i].fs.key)) {
        /* If you found the key, morph the entry into a value
           fetching entry. */
        struct VALUE_FETCH_SCHEDULE *vs = (struct VALUE_FETCH_SCHEDULE *)ks;
        /* Make the index positive, indicating that you are collecting
           the value now. */
        unsigned int index = vs->index = -(vs->index)-1;
        vs->filepos = vpos+pos_offset; vs->n_values = n_values;
        assert(((vs->filepos)<((fd_off_t)(0x100000000LL))));
        fd_decref(key); /* No longer needed */
        if (n_values>1)
          if (VOIDP(values[index]))
            values[index]=fd_init_prechoice(NULL,n_values,0);
          else {
            lispval val = values[index];
            values[index]=fd_init_prechoice(NULL,n_values,0);
            CHOICE_ADD(values[index],val);}
        else if (n_values==0) {
          /* If there are no values, store the empty choice and
             declare the entry done by setting its filepos to -1. */
          vs->filepos = -1;
          values[index]=EMPTY;}
        else if (VOIDP(values[index]))
          values[index]=EMPTY;}
      else {
        /* Keep looking for the key */
        struct KEY_FETCH_SCHEDULE *ks=
          (struct KEY_FETCH_SCHEDULE *)(&(schedule[i]));
        ks->probe = (ks->probe+ks->chain_width)%(fx->index_n_slots);
        fd_decref(key); /* No longer needed */
        if (offsets == NULL) {
          /* Do an offset probe now. */
          ks->filepos = (ks->probe*4)+8; ks->probe = -(ks->probe+1);}
        else if (offsets[ks->probe])
          /* If you have the next key location, store it. */
          ks->filepos = offget(offsets,ks->probe);
        else {
          /* In this case, you know the key isn't in the table, so
             set the value to the empty choice and the filepos to 0. */
          int index = -(ks->index)-1;
          if (VOIDP(values[index]))
            values[index]=EMPTY;
          ks->index = 0;
          ks->filepos = -1;}}}
    else {
      struct VALUE_FETCH_SCHEDULE *vs=
        (struct VALUE_FETCH_SCHEDULE *)(&(schedule[i]));
      fd_off_t vpos = vs->filepos; lispval val; int index = vs->index;
      fd_setpos(stream,vpos);
      val = fd_read_dtype(instream);
      if (FD_ABORTP(val)) return fd_interr(val);
      CHOICE_ADD(values[index],val);
      vpos = (fd_off_t)fd_read_4bytes(instream);
      while (vpos==1) {
        val = fd_read_dtype(instream);
        if (FD_ABORTP(val)) return fd_interr(val);
        CHOICE_ADD(values[index],val);
        vpos = (fd_off_t)fd_read_4bytes(instream);}
      if (vpos==0) {vs->filepos = -1;}
      else vs->filepos = vpos+pos_offset;
      assert(((vs->filepos)<((fd_off_t)(0x100000000LL))));}
    i++;}
  return n;
}

static lispval *fetchn(struct FD_FILEINDEX *fx,int n,
                       const lispval *keys,
                       int lock_adds)
{
  unsigned int *offsets = fx->index_offsets;
  union SCHEDULE *schedule = u8_big_alloc_n(n,union SCHEDULE);
  lispval *values = u8_big_alloc_n(n,lispval);
  int i = 0, schedule_size = 0, init_schedule_size; while (i < n) {
    lispval key = keys[i], cached = fd_hashtable_get(&(fx->index_cache),key,VOID);
    if (redundantp(fx,key))
      values[i++]=EMPTY;
    else if (VOIDP(cached)) {
      struct KEY_FETCH_SCHEDULE *ksched=
        (struct KEY_FETCH_SCHEDULE *)&(schedule[schedule_size]);
      int hashcode = fileindex_hash(fx,key);
      int probe = hashcode%(fx->index_n_slots);
      ksched->key = key; ksched->index = -(i+1);
      if (offsets) {
        ksched->filepos = offget(offsets,probe);
        ksched->probe = probe;}
      else {
        ksched->filepos = 8+probe*4;
        ksched->probe = (-(probe+1));}
      ksched->chain_width = hashcode%(fx->index_n_slots-2)+1;
      if (ksched->filepos)
        if (lock_adds)
          values[i]=fd_hashtable_get(&(fx->index_adds),key,VOID);
        else values[i]=fd_hashtable_get_nolock(&(fx->index_adds),key,VOID);
      else if (lock_adds)
        values[i]=fd_hashtable_get(&(fx->index_adds),key,EMPTY);
      else values[i]=fd_hashtable_get_nolock(&(fx->index_adds),key,EMPTY);
      i++; schedule_size++;}
    else values[i++]=cached;}
  init_schedule_size = schedule_size;
  qsort(schedule,schedule_size,sizeof(struct FETCH_SCHEDULE),sort_by_filepos);
  while (schedule_size>0) {
    schedule_size = run_schedule(fx,schedule_size,schedule,offsets,values);
    if (schedule_size<=0) break;
    qsort(schedule,schedule_size,sizeof(struct FETCH_SCHEDULE),sort_by_filepos);}
  /* Note that we should now look at fx->index_edits and integrate any changes,
     but we're not doing that now. */
  if (schedule_size<0) {
    int k = 0; while (k<init_schedule_size) {
      if (schedule[k].fs.filepos<0)
        fd_decref(values[schedule[k].fs.index]);
      k++;}
    u8_big_free(values);
    u8_big_free(schedule);
    return NULL;}
  else {
    int k = 0; while (k<n) {
      lispval v = values[k++];
      if (PRECHOICEP(v)) {
        struct FD_PRECHOICE *ac = (struct FD_PRECHOICE *)v;
        ac->prechoice_uselock = 1;}}
    u8_big_free(schedule);
    return values;}
}

static lispval *fileindex_fetchn(fd_index ix,int n,const lispval *keys)
{
  struct FD_FILEINDEX *fx = (struct FD_FILEINDEX *)ix;
  lispval *results;
  fd_lock_index(fx);
  results = fetchn(fx,n,keys,1);
  fd_unlock_index(fx);
  return results;
}

/* Committing indexes */

/* This is more complicated than you might think because the structure
   is optimized for adding new values to a key.  So when we commit, we have
   to go to the file to find what current values are currently there
   (or at least where they are stored in the file).

   This is made even hairier because we're trying to organize file access
   optimally by ordering seeks and doing multiple passes.  The most common use
   case is that we are storing lots of keys, so we don't want to just iterate
   over all of the keys and do a bunch of seeks for each one.

   One good side effect of all this hair is that we write all the data
   to the file before we write the offsets, meaning that if it fails before
   that point, the file remains consistent.
*/

/* Utility functions and structures */

/* This is used to organize fetching and writing a particular key.
   A negative value for chain_width indicates that the entry is completed.
   A negative value for slotno indicates that we are fetching the
   offset for that slot (in the case where we don't have cached offsets).
*/
struct KEYDATA {
  lispval key; lispval add;
  int serial, slotno, chain_width, n_values;
  fd_off_t pos;};

/* This is used to track which slotnos are newly filled when we are writing
   a file index without a vector of cached offsets. */
struct RESERVATIONS {
  unsigned int *slotnos;
  int n_reservations, max_reservations;};

static int sort_keydata(const void *kvx,const void *kvy)
{
  const struct KEYDATA *x = kvx, *y = kvy;
  if (x->chain_width<0)
    if (y->chain_width>0) return 1; else return 0;
  else if (y->chain_width<0) return -1;
  else if (x->pos < y->pos) return -1;
  else if (x->pos > y->pos) return 1;
  else return 0;
}

static int sort_keydata_pos(const void *kvx,const void *kvy)
{
  const struct KEYDATA *x = kvx, *y = kvy;
  if (x->pos < y->pos) return -1;
  else if (x->pos > y->pos) return 1;
  else return 0;
}

static int sort_keydata_serial(const void *kvx,const void *kvy)
{
  const struct KEYDATA *x = kvx, *y = kvy;
  if (x->serial < y->serial) return -1;
  else if (x->serial > y->serial) return 1;
  else return 0;
}

static int reserve_slotno(struct RESERVATIONS *r,unsigned int slotno)
{
  unsigned int *slotnos = r->slotnos, *lim = r->slotnos+r->n_reservations;
  unsigned int *bottom = slotnos, *top = lim;
  unsigned int *middle = bottom+(top-bottom)/2, insertoff, *insertpos;
  if (r->n_reservations==0) {
    bottom[0]=slotno; r->n_reservations++;
    return 1;}
  else while (bottom<=top) {
      middle = bottom+(top-bottom)/2;
      if ((middle<slotnos) || (middle>=lim)) break;
      else if (*middle == slotno) return 0;
      else if (slotno>*middle) bottom = middle+1;
      else top = middle-1;}
  insertoff = (middle-slotnos);
  if ((middle<lim) && (slotno>*middle)) insertoff++;
  if (r->n_reservations == r->max_reservations) {
    int new_max = r->max_reservations*2;
    slotnos = r->slotnos = u8_realloc(r->slotnos,SLOTSIZE*new_max);
    r->max_reservations = new_max;}
  insertpos = slotnos+insertoff;
  if (!(((insertpos<=slotnos) || (slotno>insertpos[-1])) &&
        ((insertpos>=(slotnos+r->n_reservations)) || (slotno<insertpos[0]))))
    u8_logf(LOG_CRIT,fd_IndexDriverError,
            "Corrupt reservations table when saving index");
  if (insertoff<r->n_reservations)
    memmove(insertpos+1,insertpos,(SLOTSIZE*(r->n_reservations-insertoff)));
  *insertpos = slotno;
  r->n_reservations++;
  return 1;
}

/* Fetching keydata:
   This finds where all the keys are, reserving slots for them if neccessary.
   It also fetches the current number of values and the valuepos.
   Returns the number of new keys or -1 on error. */
static int fetch_keydata(struct FD_FILEINDEX *fx,
                         struct FD_INBUF *instream,
                         struct KEYDATA *kdata,
                         int n,unsigned int *offsets)
{
  struct RESERVATIONS reserved;
  struct FD_STREAM *stream = &(fx->index_stream);
  unsigned int pos_offset = fx->index_n_slots*4, chain_length = 0;
  int i = 0, max = n, new_keys = 0;
  if (offsets == NULL) {
    reserved.slotnos = u8_malloc(SLOTSIZE*64);
    reserved.n_reservations = 0; reserved.max_reservations = 64;}
  else {
    reserved.slotnos = NULL;
    reserved.n_reservations = 0; reserved.max_reservations = 0;}
  /* Setup the key data */
  while (i < n) {
    lispval key = kdata[i].key;
    int hash = fileindex_hash(fx,key);
    int probe = hash%(fx->index_n_slots), chain_width = hash%(fx->index_n_slots-2)+1;
    if (offsets) {
      int koff = offsets[probe];
      /* Skip over all the reserved slots */
      while (koff==1) {
        probe = (probe+chain_width)%(fx->index_n_slots);
        koff = offsets[probe];}
      if (koff) {
        /* We found a full slot, queue it for examination. */
        kdata[i].slotno = probe; kdata[i].chain_width = chain_width;
        kdata[i].pos = koff+pos_offset;}
      else {
        /* We have an empty slot we can fill */
        offsets[probe]=1; new_keys++; /* Fill it */
        kdata[i].slotno = probe;
        kdata[i].chain_width = -1; /* Declare it found */
        /* We initialize .n_values to zero unless it has already been
           initialized.  This would be the case if the key's value was edited
           (e.g. had the value set or values dropped), which means that the
           values were already written. */
        if (kdata[i].n_values<0) kdata[i].n_values = 0;
        kdata[i].pos = 0;}}
    else {
      /* A negative probe value means that we are getting an offset
         from the offset table. */
      kdata[i].slotno = -(probe+1); kdata[i].chain_width = chain_width;
      kdata[i].pos = 8+SLOTSIZE*probe;}
    i++;}
  /* Now, collect keydata */
  while (max>0) {
    /* Sort by filepos for more coherent and hopefully faster disk access.
       We use a negative chain width to indicate that we're done with the
       entry. */
    qsort(kdata,max,sizeof(struct KEYDATA),sort_keydata);
    i = 0; while (i<max) {
      if (kdata[i].chain_width<0) break;
      fd_setpos(stream,kdata[i].pos);
      if (kdata[i].slotno<0) { /* fetching offset */
        unsigned int off = fd_read_4bytes(instream);
        unsigned int slotno = (-(kdata[i].slotno)-1);
        if (off) {
          kdata[i].slotno = slotno;
          kdata[i].pos = off+SLOTSIZE*(fx->index_n_slots);}
        else if (reserve_slotno(&reserved,slotno)) {
          kdata[i].slotno = slotno; new_keys++;
          kdata[i].chain_width = -1; kdata[i].pos = 0;
          if (kdata[i].n_values<0) kdata[i].n_values = 0;}
        else {
          int next_probe = (slotno+kdata[i].chain_width)%(fx->index_n_slots);
          kdata[i].slotno = -(next_probe+1);
          kdata[i].pos = 8+SLOTSIZE*next_probe;}
        i++;}
      else {
        unsigned int n_vals, vpos; lispval key;
        n_vals = fd_read_4bytes(instream);
        vpos = fd_read_4bytes(instream);
        key = fd_read_dtype(instream);
        if (LISP_EQUAL(key,kdata[i].key)) {
          kdata[i].pos = vpos; kdata[i].chain_width = -1;
          if (kdata[i].n_values<0) kdata[i].n_values = n_vals;}
        else if (offsets) {
          int next_probe = (kdata[i].slotno+kdata[i].chain_width)%(fx->index_n_slots);
          /* Compute the next probe location, skipping slots
             already taken by keys being dumped for the first time,
             which is indicated by an offset value of 1. */
          while ((offsets[next_probe]) && ((offsets[next_probe])==1))
            next_probe = (next_probe+kdata[i].chain_width)%(fx->index_n_slots);
          if (offsets[next_probe]) {
            /* If we have an offset, it is a key on disk that we need
               to look at. */
            kdata[i].slotno = next_probe;
            kdata[i].pos = offsets[next_probe]+pos_offset;}
          else {
            /* Otherwise, we have an empty slot we can put this value in. */
            new_keys++;
            kdata[i].slotno = next_probe;
            kdata[i].pos = 0;
            if (kdata[i].n_values<0) kdata[i].n_values = 0;
            offsets[next_probe]=1;
            kdata[i].chain_width = -1;}}
        else {
          int next_probe = (kdata[i].slotno+kdata[i].chain_width)%(fx->index_n_slots);
          kdata[i].slotno = -(next_probe+1);
          kdata[i].pos = 8+(SLOTSIZE*next_probe);}
        fd_decref(key);
        i++;}}
    if (max == i)
      if (chain_length>256) {
        if (offsets == NULL) u8_free(reserved.slotnos);
        return fd_reterr(fd_FileIndexSizeOverflow,"fetch_keydata",
                         u8_strdup(fx->indexid),VOID);}
      else chain_length++;
    else chain_length = 0;
    max = i;}
  if (offsets == NULL) u8_free(reserved.slotnos);
  return new_keys;
}

static int write_values(fd_stream stream,
                        struct FD_OUTBUF *outstream,
                        lispval values,
                        unsigned int nextpos,
                        int *n_valuesp)
{
  lispval realval = ((PRECHOICEP(values)) ? (fd_make_simple_choice(values)) :
                     (values));
  if (EMPTYP(realval)) {
    *n_valuesp = 0; return 0;}
  else if (CHOICEP(realval)) {
    struct FD_CHOICE *ch=
      FD_CONSPTR(fd_choice,realval);
    int size = 0;
    const lispval *scan = FD_XCHOICE_DATA(ch), *limit = scan+FD_XCHOICE_SIZE(ch);
    while (scan < limit) {
      size = size+fd_write_dtype(outstream,*scan)+4; scan++;
      if (scan == limit) fd_write_4bytes(outstream,nextpos);
      else fd_write_4bytes(outstream,1);}
    *n_valuesp = FD_XCHOICE_SIZE(ch);
    if (PRECHOICEP(values)) fd_decref(realval);
    return size;}
  else {
    int size = fd_write_dtype(outstream,realval);
    *n_valuesp = 1;
    fd_write_4bytes(outstream,nextpos);
    return size+4;}
}

/* Committing edits */

/* This extends the KEYDATA vector with entries for the values
   which are being set or modified by dropping. */
static int commit_stores(struct FD_CONST_KEYVAL *stores,int n_stores,
                         struct FD_FILEINDEX *f,
                         struct FD_OUTBUF *outstream,
                         struct KEYDATA *kdata,
                         unsigned int *valpos,
                         int kdata_i)
{
  struct FD_STREAM *stream = &(f->index_stream);
  fd_set_direction(stream,fd_byteflow_write);
  fd_off_t filepos = fd_endpos(stream);
  int store_i = 0; while (store_i < n_stores) {
    lispval key = stores[store_i].kv_key;
    lispval val = stores[store_i].kv_val;
    int decref_val = 0;
    if (FD_PRECHOICEP(val)) {
      val = fd_make_simple_choice(val);
      decref_val=1;}
    kdata[kdata_i].key    = key;
    kdata[kdata_i].serial = kdata_i;
    kdata[kdata_i].add    = VOID;
    if (EMPTYP(val)) {
      kdata[kdata_i].n_values = 0;
      valpos[kdata_i] = 0;}
    else {
      kdata[kdata_i].n_values = FD_CHOICE_SIZE(val);
      valpos[kdata_i] = filepos;
      filepos = filepos+
        write_values(stream,outstream,val,0,
                     &(kdata[kdata_i].n_values));}
    if (decref_val) fd_decref(val);
    store_i++;
    kdata_i++;}
  return kdata_i;
}

/* This extends the KEYDATA vector with entries for the values
   which are being set or modified by dropping. */
static int commit_drops(struct FD_CONST_KEYVAL *drops,int n_drops,
                        struct FD_FILEINDEX *f,
                        struct FD_OUTBUF *outstream,
                        struct KEYDATA *kdata,
                        unsigned int *valpos,
                        int kdata_i)
{
  struct FD_STREAM *stream = &(f->index_stream);
  lispval *dropkeys = u8_big_alloc_n(n_drops,lispval);
  /* Change some drops to sets when cached, record the ones that can't
     be changed so they can be fetched and resolved. */
  int drop_i = 0; while (drop_i < n_drops) {
    dropkeys[drop_i] = drops[drop_i].kv_key;
    drop_i++;}
  lispval *dropvals = fetchn(f,n_drops,dropkeys,0);
  u8_big_free(dropkeys);

  fd_set_direction(stream,fd_byteflow_write);
  fd_off_t filepos = fd_endpos(stream);

  drop_i=0; while (drop_i<n_drops) {
    lispval key = drops[drop_i].kv_key;
    lispval dropped = drops[drop_i].kv_val;
    lispval ondisk = dropvals[drop_i];
    lispval val = fd_difference(ondisk,dropped);

    if (FD_PRECHOICEP(val))
      val = fd_simplify_choice(val);

    kdata[kdata_i].key    = key;
    kdata[kdata_i].add    = VOID;
    kdata[kdata_i].serial = kdata_i;

    if (EMPTYP(val)) {
      kdata[kdata_i].n_values = 0;
      valpos[kdata_i] = 0;}
    else {
      kdata[kdata_i].n_values = FD_CHOICE_SIZE(val);
      valpos[kdata_i] = filepos;
      filepos = filepos +
        write_values(stream,outstream,val,0,
                     &(kdata[kdata_i].n_values));}

    fd_decref(val);
    drop_i++;
    kdata_i++;}

  fd_decref_vec(dropvals,n_drops);
  u8_big_free(dropvals);

  return kdata_i;
}

static void write_keys(struct FD_FILEINDEX *fx,
                       struct FD_OUTBUF *outstream,
                       int n,struct KEYDATA *kdata,
                       unsigned int *offsets)
{
  unsigned int pos_offset = fx->index_n_slots*4;
  struct FD_STREAM *stream = &(fx->index_stream);
  fd_off_t pos = fd_endpos(stream);
  int i = 0; while (i<n) {
    fd_off_t kpos = pos;
    fd_write_4bytes(outstream,kdata[i].n_values);
    fd_write_4bytes(outstream,(unsigned int)kdata[i].pos);
    pos = pos+fd_write_dtype(outstream,kdata[i].key)+8;
    if (offsets)
      offsets[kdata[i].slotno]=(kpos-pos_offset);
    else kdata[i].pos = (kpos-pos_offset);
    i++;}
}

static void write_offsets(struct FD_FILEINDEX *fx,
                          struct FD_OUTBUF *outstream,
                          int n,
                          struct KEYDATA *kdata,
                          unsigned int *offsets)
{
  struct FD_STREAM *stream = &(fx->index_stream);
  if (offsets) {
    fd_start_write(stream,8);
    fd_write_ints(stream,fx->index_n_slots,offsets);
    fd_flush_stream(stream);}
  else {
    int i = 0;
    qsort(kdata,n,sizeof(struct KEYDATA),sort_keydata_pos);
    while (i < n) {
      fd_setpos(stream,8+kdata[i].slotno*4);
      fd_write_4bytes(outstream,(unsigned int)kdata[i].pos);
      i++;}
    fd_flush_stream(stream);}
}

/* Putting it all together */

static int fileindex_save(struct FD_INDEX *ix,
                          struct FD_CONST_KEYVAL *adds,int n_adds,
                          struct FD_CONST_KEYVAL *drops,int n_drops,
                          struct FD_CONST_KEYVAL *stores,int n_stores,
                          lispval changed_metadata)
{
  struct FD_FILEINDEX *fx = (struct FD_FILEINDEX *)ix;
  struct FD_STREAM *stream = &(fx->index_stream);
  unsigned int *new_offsets = NULL, gc_new_offsets = 0;
  int pos_offset = fx->index_n_slots*4, newcount;
  double started = u8_elapsed_time();
  /* Get the current offsets from the index */
  if (n_adds+n_drops+n_stores) {
    int kdata_i = 0, kdata_edits=0, n_changes = n_adds+n_drops+n_stores;
    fd_lock_index(fx);
#if FD_USE_MMAP
    if (fx->index_offsets) {
      int i = 0, n = fx->index_n_slots;
      /* We have to copy these if they're MMAPd, because
         we can't modify them (while updating) otherwise.  */
      new_offsets = u8_big_alloc_n((fx->index_n_slots),unsigned int);
      gc_new_offsets = 1;
      while (i<n) {
        new_offsets[i]=offget(fx->index_offsets,i);
        i++;}}
#else
    if (fx->index_offsets) {
      fd_inbuf *instream = fd_readbuf(stream);
      new_offsets = fx->index_offsets;
      fd_start_read(stream,8);
      fd_read_ints(instream,fx->ht_n_buckets,new_offsets);}
#endif

    struct FD_OUTBUF *outstream = fd_writebuf(stream);
    struct KEYDATA *kdata = u8_big_alloc_n(n_changes,struct KEYDATA);
    unsigned int *valpos = u8_big_alloc_n(n_changes,unsigned int);
    int i=0; while ( i < n_adds ) {
      lispval key = adds[i].kv_key, val = adds[i].kv_val;
      kdata[kdata_i].key = key;
      kdata[kdata_i].add = val;
      /* We'll use this to sort back into the order of the adds table */
      kdata[kdata_i].serial = kdata_i;
      /* Initialize the other fields */
      kdata[kdata_i].n_values = -1;
      kdata[kdata_i].slotno = -1;
      kdata[kdata_i].pos = -1;
      i++; kdata_i++;}
    kdata_edits = kdata_i;

    if (n_stores)
      kdata_i = commit_stores(stores,n_stores,fx,outstream,kdata,valpos,kdata_i);
    if (n_drops)
      kdata_i = commit_drops(drops,n_drops,fx,outstream,kdata,valpos,kdata_i);

    /* This figures out which slot all of the keys go into, trying to be
       clever about reading things in order, if that matters anymore. */
    newcount = fetch_keydata(fx,fd_readbuf(stream),kdata,kdata_i,new_offsets);

    if (newcount<0) {
      u8_big_free(kdata);
      u8_big_free(valpos);
      if (gc_new_offsets) u8_big_free(new_offsets);
      fd_unlock_index(fx);
      return newcount;}

    /* Sort back into the original order. */
    qsort(kdata,kdata_i,sizeof(struct KEYDATA),sort_keydata_serial);

    /* Set the .pos fields for edits, since we're not using them */
    i = kdata_edits; while (i<kdata_i) {
      if (valpos[i])
        kdata[i].pos = ((fd_off_t)(valpos[i]-pos_offset));
      else kdata[i].pos = ((fd_off_t)0);
      i++;}

    /* Now, scan the adds again and write the added values to disk. */
    fd_off_t filepos = fd_endpos(stream);
    i = 0; while (i < kdata_edits) {
      struct FD_OUTBUF *outstream = fd_writebuf(stream);
      fd_off_t writepos = filepos;
      lispval add = kdata[i].add;
      int new_values;
      filepos = filepos +
        write_values(stream,outstream,add,kdata[i].pos,&new_values);
      kdata[i].pos = writepos-pos_offset;
      kdata[i].n_values = kdata[i].n_values+new_values;
      i++;}
    write_keys(fx,fd_writebuf(stream),kdata_i,kdata,new_offsets);
    /* Now, start writing the offsets themselves */
    write_offsets(fx,fd_writebuf(stream),kdata_i,kdata,new_offsets);
    fd_unlock_index(fx);
    if (valpos) u8_big_free(valpos);
    u8_big_free(kdata);

    u8_logf(LOG_NOTICE,"FileIndexCommit",
            _("Saved mappings for %d keys to %s in %f secs"),
            n_changes,ix->indexid,u8_elapsed_time()-started);

    if (gc_new_offsets) u8_big_free(new_offsets);
    return kdata_i;}
  else return 0;
}

static int fileindex_commit(fd_index ix,fd_commit_phase phase,
                            struct FD_INDEX_COMMITS *commit)
{
  struct FD_FILEINDEX *fx = (fd_fileindex) ix;
  switch (phase) {
  case fd_commit_start: {
    u8_string source = fx->index_source;
    int lock_rv = fd_streamctl(&(fx->index_stream),fd_stream_lockfile,NULL);
    if (lock_rv <= 0) {
      u8_graberrno("fileindex_commit",u8_strdup(source));
      return -1;}
    return fd_write_rollback("fileindex_commit",ix->indexid,source,
                             8+(4*(fx->index_n_slots)));}
  case fd_commit_write: {
    return fileindex_save(ix,
                          (struct FD_CONST_KEYVAL *)commit->commit_adds,
                          commit->commit_n_adds,
                          (struct FD_CONST_KEYVAL *)commit->commit_drops,
                          commit->commit_n_drops,
                          (struct FD_CONST_KEYVAL *)commit->commit_stores,
                          commit->commit_n_stores,
                          commit->commit_metadata);}
  case fd_commit_sync:
    return 0;
  case fd_commit_rollback: {
    u8_string source = ix->index_source;
    u8_string rollback_file = u8_string_append(source,".rollback",NULL);
    if (u8_file_existsp(rollback_file)) {
      ssize_t rv = fd_apply_head(rollback_file,source);
      u8_free(rollback_file);
      if (rv<0) return -1; else return 1;}
    else {
      u8_logf(LOG_CRIT,"NoRollbackFile",
              "The rollback file %s for %s doesn't exist",
              rollback_file,ix->indexid);
      u8_free(rollback_file);
      return -1;}}
  case fd_commit_cleanup: {
    u8_string source = ix->index_source;
    int unlock_rv = fd_streamctl(&(fx->index_stream),fd_stream_unlockfile,NULL);
    if (unlock_rv <= 0) {
      int saved_errno = errno; errno=0;
      u8_logf(LOG_CRIT,"UnlockFailed",
              "Couldn't unlock %s for hashindex %s errno=%d:%s",
              source,fx->indexid,saved_errno,u8_strerror(saved_errno));}
    u8_string rollback_file = u8_string_append(source,".rollback",NULL);
    if (u8_file_existsp(rollback_file)) {
      int rv = u8_removefile(rollback_file);
      u8_free(rollback_file);
      return rv;}
    else {
      u8_logf(LOG_WARN,"Rollback file %s was deleted",rollback_file);
      u8_free(rollback_file);
      return -1;}}
  default: {
    u8_logf(LOG_WARN,"NoPhasedCommit",
            "The index %s doesn't support phased commits",
            ix->indexid);
    return -1;}
  }
}

static void fileindex_close(fd_index ix)
{
  struct FD_FILEINDEX *fx = (struct FD_FILEINDEX *)ix;
  fd_lock_index(fx);
  fd_close_stream(&(fx->index_stream),0);
  if (fx->index_offsets) {
#if FD_USE_MMAP
    int retval = munmap(fx->index_offsets-2,(SLOTSIZE*fx->index_n_slots)+8);
    if (retval<0) {
      u8_logf(LOG_CRIT,u8_strerror(errno),
              "[%d:%d] fileindex_close:munmap %s",
              retval,errno,fx->index_source);
      errno = 0;}
#else
    u8_big_free(fx->index_offsets);
#endif
    fx->index_offsets = NULL;
    fx->index_cache_level = -1;}
  fd_unlock_index(fx);
}

static void fileindex_setbuf(fd_index ix,int bufsiz)
{
  struct FD_FILEINDEX *fx = (struct FD_FILEINDEX *)ix;
  fd_lock_index(fx);
  fd_setbufsize(&(fx->index_stream),bufsiz);
  fd_unlock_index(fx);
}

/* File index ops */

static lispval fileindex_ctl(fd_index ix,lispval op,int n,lispval *args)
{
  struct FD_FILEINDEX *flx = (struct FD_FILEINDEX *)ix;
  if ( ((n>0)&&(args == NULL)) || (n<0) )
    return fd_err("BadIndexOpCall","fileindex_ctl",
                  flx->indexid,VOID);
  else if (op == fd_cachelevel_op) {
    if (n==0)
      return FD_INT(flx->index_cache_level);
    else {
      lispval arg = (args)?(args[0]):(VOID);
      if ((FIXNUMP(arg))&&(FIX2INT(arg)>=0)&&
          (FIX2INT(arg)<0x100)) {
        fileindex_setcache(ix,FIX2INT(arg));
        return FD_INT(flx->index_cache_level);}
      else return fd_type_error
             (_("cachelevel"),"fileindex_ctl/cachelevel",arg);}}
  else if (op == fd_bufsize_op) {
    if (n==0)
      return FD_INT(flx->index_stream.buf.raw.buflen);
    else if (FIXNUMP(args[0])) {
      fileindex_setbuf(ix,FIX2INT(args[0]));
      return FD_INT(flx->index_stream.buf.raw.buflen);}
    else return fd_type_error("buffer size","fileindex_ctl/bufsize",args[0]);}
  else if ( (op == fd_metadata_op) && (n == 0) ) {
    lispval base = fd_index_base_metadata(ix);
    fd_store(base,buckets_slot,FD_INT(flx->index_n_slots));
    if (!(FD_VOIDP(flx->slotids)))
      fd_store(base,slotids_slot,flx->slotids);
    return base;}
  else if (op == fd_index_hashop) {
    if (n==0)
      return FD_INT(flx->index_n_slots);
    else {
      lispval mod_arg = (n>1) ? (args[1]) : (VOID);
      unsigned int hash = fileindex_hash(flx,args[0]);
      if (FIXNUMP(mod_arg))
        return FD_INT((hash%FIX2INT(mod_arg)));
      else if ((FALSEP(mod_arg))||(VOIDP(mod_arg)))
        return FD_INT(hash);
      else return FD_INT(hash%(flx->index_n_slots));}}
  else if (op == fd_capacity_op)
    return FD_INT(flx->index_n_slots);
  else if (op == fd_keycount_op)
    return FD_INT(flx->index_n_slots);
  else if (op == fd_load_op)
    return EMPTY;
  else return fd_default_indexctl(ix,op,n,args);
}

/* Making file indexes */

FD_EXPORT
/* fd_make_fileindex:
   Arguments: a filename string, a magic number (usigned int), an FD_OID,
   a capacity, and a dtype pointer to a metadata description (a slotmap).
   Returns: -1 on error, 1 on success. */
int fd_make_fileindex(u8_string filename,unsigned int magicno,int n_slots_arg)
{
  int i, n_slots;
  struct FD_STREAM _stream;
  struct FD_STREAM *stream=
    fd_init_file_stream(&_stream,filename,FD_FILE_CREATE,-1,fd_driver_bufsize);
  struct FD_OUTBUF *outstream = (stream) ? (fd_writebuf(stream)) : (NULL);
  if (outstream == NULL)
    return -1;
  else if ((stream->stream_flags)&FD_STREAM_READ_ONLY) {
    fd_seterr3(fd_CantWrite,"fd_make_fileindex",filename);
    fd_free_stream(stream);
    return -1;}

  stream->stream_flags &= ~FD_STREAM_IS_CONSED;
  if (n_slots_arg<0) n_slots = -n_slots_arg;
  else n_slots = fd_get_hashtable_size(n_slots_arg);

  u8_logf(LOG_INFO,"CreateFileIndex",
          "Creating a file index '%s' with %ld slots",
          filename,n_slots);

  fd_setpos(stream,0);
  fd_write_4bytes(outstream,magicno);
  fd_write_4bytes(outstream,n_slots);
  i = 0; while (i<n_slots) {fd_write_4bytes(outstream,0); i++;}
  fd_write_4bytes(outstream,0xFFFFFFFE);
  fd_write_4bytes(outstream,40);
  i = 0; while (i<8) {fd_write_4bytes(outstream,0); i++;}
  fd_close_stream(stream,FD_STREAM_FREEDATA);
  return 1;
}

static fd_index fileindex_create(u8_string spec,void *type_data,
                                 fd_storage_flags flags,
                                 lispval opts)
{
  lispval n_slots = fd_getopt(opts,fd_intern("SLOTS"),
                              fd_getopt(opts,fd_intern("SIZE"),
                                        FD_INT(fileindex_default_size)));
  if (!(FD_UINTP(n_slots))) {
    fd_seterr("NumberOfIndexSlots","fileindex_create",spec,n_slots);
    return NULL;}
  else if (fd_make_fileindex(spec,
                             (unsigned int)(FD_PTRVAL(type_data)),
                             FIX2INT(n_slots))>=0) {
    fd_set_file_opts(spec,opts);
    return fd_open_index(spec,flags,VOID);}
  else return NULL;
}


/* Initializing the driver module */

static struct FD_INDEX_HANDLER fileindex_handler={
  "fileindex", 1, sizeof(struct FD_FILEINDEX), 12,
  fileindex_close, /* close */
  fileindex_commit, /* commit */
  fileindex_fetch, /* fetch */
  fileindex_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  fileindex_fetchn, /* fetchn */
  fileindex_fetchkeys, /* fetchkeys */
  fileindex_fetchinfo, /* fetchinfo */
  NULL, /* batchadd */
  fileindex_create, /* create */
  NULL, /* walk */
  NULL, /* recycle */
  fileindex_ctl  /* indexctl */
};

FD_EXPORT void fd_init_fileindex_c()
{
  u8_register_source_file(_FILEINFO);

  set_symbol = fd_intern("SET");
  drop_symbol = fd_intern("DROP");
  slotids_symbol = fd_intern("%%SLOTIDS");
  slotids_slot = fd_intern("SLOTIDS");
  buckets_slot = fd_intern("BUCKETS");
  fd_register_index_type("fileindex",
                         &fileindex_handler,
                         open_fileindex,
                         fd_match_index_file,
                         (void *) U8_INT2PTR(FD_FILEINDEX_MAGIC_NUMBER));
  fd_register_index_type("fileindex.v2",
                         &fileindex_handler,
                         open_fileindex,
                         fd_match_index_file,
                         (void *) U8_INT2PTR(FD_MULT_FILEINDEX_MAGIC_NUMBER));
  fd_register_index_type("damaged_fileindex",
                         &fileindex_handler,
                         open_fileindex,
                         fd_match_index_file,
                         (void *) U8_INT2PTR(FD_FILEINDEX_TO_RECOVER));
  fd_register_index_type("damaged_fileindex.v2",
                         &fileindex_handler,
                         open_fileindex,
                         fd_match_index_file,
                         (void *) U8_INT2PTR(FD_MULT_FILEINDEX_TO_RECOVER));
  fd_register_config("FILEINDEX:SIZE","The default size for file indexes",
                     fd_sizeconfig_get,fd_sizeconfig_set,
                     &fileindex_default_size);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
