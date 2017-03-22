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
#include "framerd/fdkbase.h"
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

#if (HAVE_MMAP)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#define MMAP_FLAGS MAP_SHARED
#endif

#if ((HAVE_MMAP) && (!(WORDS_BIGENDIAN)))
#define offget(offvec,offset) (fd_flip_word((offvec)[offset]))
#define set_offset(offvec,offset,v) (offvec)[offset]=(fd_flip_word(v))
#else
#define offget(offvec,offset) ((offvec)[offset])
#define set_offset(offvec,offset,v) (offvec)[offset]=(v)
#endif

#define BAD_VALUEP(value) \
  (FD_EXPECT_FALSE((FD_EODP(value))||(FD_EOFP(value))||(FD_ABORTP(value))))

static void write_file_index_recovery_data(struct FD_FILE_INDEX *fx,unsigned int *);
static int recover_file_index(struct FD_FILE_INDEX *fx);

#define SLOTSIZE (sizeof(unsigned int))

fd_exception fd_FileIndexOverflow=_("file index hash overflow");
fd_exception fd_FileIndexError=_("Internal error with index file");

static fdtype set_symbol, drop_symbol, slotids_symbol;
static struct FD_INDEX_HANDLER file_index_handler;

static fdtype file_index_fetch(fd_index ix,fdtype key);

static fd_index open_file_index(u8_string fname,fdkb_flags flags,fdtype opts)
{
  struct FD_FILE_INDEX *index=u8_alloc(struct FD_FILE_INDEX);
  struct FD_STREAM *s=&(index->index_stream);
  int read_only=U8_BITP(flags,FDKB_READ_ONLY);
  int consed=U8_BITP(flags,FDKB_ISCONSED);
  unsigned int magicno;
  fd_stream_mode mode=
    ((read_only) ? (FD_FILE_READ) : (FD_FILE_MODIFY));
  fd_init_index((fd_index)index,&file_index_handler,
                fname,u8_realpath(fname,NULL),
                consed);
  if (fd_init_file_stream(s,fname,mode,
                          ((read_only)?
                           (FD_DEFAULT_FILESTREAM_FLAGS|FD_STREAM_READ_ONLY):
                           (FD_DEFAULT_FILESTREAM_FLAGS)),
                          fd_driver_bufsize) == NULL) {
    u8_free(index);
    u8_free(realpath);
    fd_seterr3(u8_CantOpenFile,"open_file_index",u8_strdup(fname));
    return NULL;}
  /* See if it ended up read only */
  if (index->index_stream.stream_flags&FD_STREAM_READ_ONLY) read_only=1;
  s->stream_flags&=~FD_STREAM_IS_CONSED;
  magicno=fd_read_4bytes_at(s,0);
  index->index_n_slots=fd_read_4bytes_at(s,4);
  if ((magicno==FD_FILE_INDEX_TO_RECOVER) ||
      (magicno==FD_MULT_FILE_INDEX_TO_RECOVER) ||
      (magicno==FD_MULT_FILE3_INDEX_TO_RECOVER)) {
    u8_log(LOG_WARN,fd_RecoveryRequired,"Recovering the file index %s",fname);
    recover_file_index(index);
    magicno=magicno&(~0x20);}
  if (magicno == FD_FILE_INDEX_MAGIC_NUMBER) index->index_hashv=1;
  else if (magicno == FD_MULT_FILE_INDEX_MAGIC_NUMBER) index->index_hashv=2;
  else if (magicno == FD_MULT_FILE3_INDEX_MAGIC_NUMBER) index->index_hashv=3;
  else {
    fd_seterr3(fd_NotAFileIndex,"open_file_index",u8_strdup(fname));
    u8_free(index);
    return NULL;}
  index->index_offsets=NULL;
  if (read_only)
    U8_SETBITS(index->index_flags,FDKB_READ_ONLY);
  u8_init_mutex(&(index->index_lock));
  index->slotids=FD_VOID;
  {
    fdtype slotids=file_index_fetch((fd_index)index,slotids_symbol);
    if (!(FD_EMPTY_CHOICEP(slotids)))
      index->slotids=fd_simplify_choice(slotids);}
  if (!(consed)) fd_register_index((fd_index)index);
  return (fd_index)index;
}

static unsigned int get_offset(fd_file_index ix,int slotno)
{
  fd_stream stream=&(ix->index_stream);
  return fd_read_4bytes(fd_start_read(stream,slotno*4+8));
}

static void file_index_setcache(fd_index ix,int level)
{
  struct FD_FILE_INDEX *fx=(struct FD_FILE_INDEX *)ix;
  if (level == 2)
    if (fx->index_offsets) return;
    else {
      fd_stream s=&(fx->index_stream);
      unsigned int *offsets, *newmmap;
      fd_lock_index(fx);
      if (fx->index_offsets) {
        fd_unlock_index(fx);
        return;}
#if HAVE_MMAP
      newmmap=
        mmap(NULL,(fx->index_n_slots*SLOTSIZE)+8,
             PROT_READ,MMAP_FLAGS,s->stream_fileno,0);
      if ((newmmap==NULL) || (newmmap==((void *)-1))) {
        u8_log(LOG_CRIT,u8_strerror(errno),
               "file_index_setcache:mmap %s",fx->index_source);
        fx->index_offsets=NULL; errno=0;}
      else fx->index_offsets=offsets=newmmap+2;
#else
      offsets=u8_malloc(SLOTSIZE*(fx->ht_n_buckets));
      fd_start_read(s,8);
      fd_read_ints(s,fx->ht_n_buckets,offsets);
      fx->index_offsets=offsets;
#endif
      fd_unlock_index(fx);}
  else if (level < 2) {
    if (fx->index_offsets == NULL) return;
    else {
      int retval;
      fd_lock_index(fx);
#if HAVE_MMAP
      retval=munmap(fx->index_offsets-2,(fx->index_n_slots*SLOTSIZE)+8);
      if (retval<0) {
        u8_log(LOG_CRIT,u8_strerror(errno),
               "file_index_setcache:munmap %s",fx->index_source);
        fx->index_offsets=NULL; errno=0;}
#else
      u8_free(fx->index_offsets);
#endif
      fx->index_offsets=NULL;
      fd_unlock_index(fx);}}
}

FD_FASTOP unsigned int file_index_hash(struct FD_FILE_INDEX *fx,fdtype x)
{
  switch (fx->index_hashv) {
  case 0: case 1:
    return fd_hash_dtype1(x);
  case 2:
    return fd_hash_dtype2(x);
  case 3:
    return fd_hash_dtype3(x);
  default:
    u8_raise(_("Bad hash version"),"file_index_hash",fx->indexid);}
  /* Never reached */
  return -1;
}

/* This does a simple binary search of a sorted choice vector,
   looking for a particular element. */
FD_FASTOP int choice_containsp(fdtype x,struct FD_CHOICE *choice)
{
  int size=FD_XCHOICE_SIZE(choice);
  const fdtype *bottom=FD_XCHOICE_DATA(choice), *top=bottom+(size-1);
  while (top>=bottom) {
    const fdtype *middle=bottom+(top-bottom)/2;
    if (x == *middle) return 1;
    else if (x < *middle) top=middle-1;
    else bottom=middle+1;}
  return 0;
}

FD_FASTOP int redundantp(struct FD_FILE_INDEX *fx,fdtype key)
{
  if ((FD_PAIRP(key)) && (!(FD_VOIDP(fx->slotids)))) {
    fdtype slotid=FD_CAR(key), slotids=fx->slotids;
    if ((FD_SYMBOLP(slotid)) || (FD_OIDP(slotid)))
      if ((FD_CHOICEP(slotids)) ?
          (choice_containsp(slotid,(fd_choice)slotids)) :
          (FD_EQ(slotid,slotids)))
        return 0;
      else return 1;
    else return  0;}
  else return 0;
}

static fdtype file_index_fetch(fd_index ix,fdtype key)
{
  struct FD_FILE_INDEX *fx=(struct FD_FILE_INDEX *)ix;
  if (redundantp(fx,key)) return FD_EMPTY_CHOICE;
  else  fd_lock_index(fx);
  {
    fd_stream stream=&(fx->index_stream);
    fd_inbuf instream=fd_readbuf(stream);
    unsigned int hashval=file_index_hash(fx,key);
    unsigned int n_probes=0;
    unsigned int probe=hashval%(fx->index_n_slots);
    unsigned int chain_width=(hashval%(fx->index_n_slots-2))+1;
    unsigned int *offsets=fx->index_offsets;
    unsigned int pos_offset=fx->index_n_slots*4;
    unsigned int keypos=
      ((offsets) ? (offget(offsets,probe)) : (get_offset(fx,probe)));
    while (keypos) {
      fdtype thiskey; unsigned int n_vals; fd_off_t val_start;
      n_vals=fd_read_4bytes(fd_start_read(stream,keypos+pos_offset));
      val_start=fd_read_4bytes(instream);
      if (FD_EXPECT_FALSE((n_vals==0) && (val_start)))
        u8_log(LOG_CRIT,fd_FileIndexError,
               "file_index_fetch %s",u8_strdup(ix->indexid));
      thiskey=fd_read_dtype(instream);
      if (FDTYPE_EQUAL(key,thiskey)) {
        if (n_vals==0) {
          u8_unlock_mutex(&fx->index_lock);
          fd_decref(thiskey);
          return FD_EMPTY_CHOICE;}
        else {
          int i=0, atomicp=1;
          struct FD_CHOICE *result=fd_alloc_choice(n_vals);
          fdtype *values=(fdtype *)FD_XCHOICE_DATA(result);
          fd_off_t next_pos=val_start;
          while (next_pos) {
            fdtype v;
            if (FD_EXPECT_FALSE(i>=n_vals))
              u8_raise(_("inconsistent file index"),
                       "file_index_fetch",u8_strdup(ix->indexid));
            if (next_pos>1) fd_setpos(stream,next_pos+pos_offset);
            v=fd_read_dtype(instream);
            if ((atomicp) && (FD_CONSP(v))) atomicp=0;
            values[i++]=v;
            next_pos=fd_read_4bytes(instream);}
          u8_unlock_mutex(&fx->index_lock); fd_decref(thiskey);
          return fd_init_choice(result,i,NULL,
                                FD_CHOICE_DOSORT|
                                ((atomicp)?(FD_CHOICE_ISATOMIC):
                                 (FD_CHOICE_ISCONSES))|
                                FD_CHOICE_REALLOC);}}
      else if (n_probes>256) {
        u8_unlock_mutex(&fx->index_lock);
        fd_decref(thiskey);
        return fd_err(fd_FileIndexOverflow,"file_index_fetch",
                      u8_strdup(fx->index_source),thiskey);}
      else {
        n_probes++;
        fd_decref(thiskey);
        probe=(probe+chain_width)%(fx->index_n_slots);
        keypos=((offsets) ? (offget(offsets,probe)) : (get_offset(fx,probe)));}}
    u8_unlock_mutex(&fx->index_lock);
    return FD_EMPTY_CHOICE;}
}

/* Fetching sizes */

static int file_index_fetchsize(fd_index ix,fdtype key)
{
  struct FD_FILE_INDEX *fx=(struct FD_FILE_INDEX *)ix;
  fd_lock_index(fx);
  {
    fd_stream stream=&(fx->index_stream);
    fd_inbuf instream=fd_readbuf(stream);
    unsigned int hashval=file_index_hash(fx,key);
    unsigned int n_probes=0;
    unsigned int probe=hashval%(fx->index_n_slots),
      chain_width=(hashval%(fx->index_n_slots-2))+1;
    unsigned int *offsets=fx->index_offsets;
    unsigned int keypos=
      ((offsets) ? (offget(offsets,probe)) : (get_offset(fx,probe)));
    while (keypos) {
      fdtype thiskey; unsigned int n_vals; /* fd_off_t val_start; */
      instream=fd_start_read(stream,keypos+(fx->index_n_slots)*4);
      n_vals=fd_read_4bytes(instream);
      /* val_start=*/ fd_read_4bytes(instream);
      thiskey=fd_read_dtype(instream);
      if (FDTYPE_EQUAL(key,thiskey)) {
        fd_unlock_index(fx);
        return n_vals;}
      else if (n_probes>256) {
        u8_unlock_mutex(&fx->index_lock);
        return fd_err(fd_FileIndexOverflow,
                      "file_index_fetchsize",
                      u8_strdup(fx->index_source),FD_VOID);}
      else {n_probes++; probe=(probe+chain_width)%(fx->index_n_slots);}}
    u8_unlock_mutex(&fx->index_lock);
    return FD_EMPTY_CHOICE;}
}

/* Fetching keys */

static int sort_offsets(const void *vox,const void *voy)
{
  const unsigned int *ox=vox, *oy=voy;
  if (*ox<*oy) return -1;
  else if (*ox>*oy) return 1;
  else return 0;
}

static int compress_offsets(unsigned int *offsets,int n)
{
  unsigned int *read=offsets, *write=offsets, *limit=read+n;
  while (read<limit)
    if (*read)
      if (read==write) {read++; write++;}
      else *write++=*read++;
    else read++;
  return write-offsets;
}

static fdtype *file_index_fetchkeys(fd_index ix,int *n)
{
  struct FD_FILE_INDEX *fx=(struct FD_FILE_INDEX *)ix;
  struct FD_STREAM *stream=&(fx->index_stream);
  unsigned int n_slots, i=0, pos_offset, *offsets, n_keys;
  fd_lock_index(fx);
  n_slots=fx->index_n_slots; offsets=u8_malloc(SLOTSIZE*n_slots);
  pos_offset=SLOTSIZE*n_slots;
  fd_start_read(stream,8);
  fd_read_ints(stream,fx->index_n_slots,offsets);
  n_keys=compress_offsets(offsets,fx->index_n_slots);
  if (n_keys==0) {
    *n=n_keys;
    u8_free(offsets);
    return NULL;}
  else {
    fdtype *keys=u8_alloc_n(n_keys,fdtype);
    qsort(offsets,n_keys,SLOTSIZE,sort_offsets);
    while (i < n_keys) {
      keys[i]=fd_read_dtype(fd_start_read(stream,pos_offset+offsets[i]+8));
      i++;}
    *n=n_keys;
    u8_free(offsets);
    return keys;}
}

static struct FD_KEY_SIZE *file_index_fetchsizes(fd_index ix,int *n)
{
  struct FD_FILE_INDEX *fx=(struct FD_FILE_INDEX *)ix;
  struct FD_STREAM *stream=&(fx->index_stream);
  struct FD_INBUF *instream=fd_readbuf(stream);
  unsigned int n_slots, i=0, pos_offset, *offsets, n_keys;
  fd_lock_index(fx);
  n_slots=fx->index_n_slots; offsets=u8_malloc(SLOTSIZE*n_slots);
  pos_offset=SLOTSIZE*n_slots;
  fd_start_read(stream,8);
  fd_read_ints(stream,fx->index_n_slots,offsets);
  n_keys=compress_offsets(offsets,fx->index_n_slots);
  if (n_keys==0) {
    *n=0;
    u8_free(offsets);
    return NULL;}
  else {
    struct FD_KEY_SIZE *sizes=u8_alloc_n(n_keys,FD_KEY_SIZE);
    qsort(offsets,n_keys,SLOTSIZE,sort_offsets);
    while (i < n_keys) {
      fdtype key; int size;
      instream=fd_start_read(stream,pos_offset+offsets[i]);
      size=fd_read_4bytes(instream);
      /* vpos=*/ fd_read_4bytes(instream);
      key=fd_read_dtype(instream);
      sizes[i].keysizekey=key; sizes[i].keysizenvals=size;
      i++;}
    *n=n_keys;
    u8_free(offsets);
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
  fdtype key; int index; fd_off_t filepos;
  int probe, chain_width;};
struct KEY_FETCH_SCHEDULE {
  fdtype key; int index; fd_off_t filepos;
  int probe, chain_width;};
struct VALUE_FETCH_SCHEDULE {
  fdtype key; int index;  fd_off_t filepos;
  int probe, n_values;};
union SCHEDULE {
  struct FETCH_SCHEDULE fs;
  struct KEY_FETCH_SCHEDULE ks;
  struct VALUE_FETCH_SCHEDULE vs;};

static int sort_by_filepos(const void *xp,const void *yp)
{
  struct FETCH_SCHEDULE *xs=(struct FETCH_SCHEDULE *)xp;
  struct FETCH_SCHEDULE *ys=(struct FETCH_SCHEDULE *)yp;
  if (xs->filepos<=0)
    if (ys->filepos<=0) return 0; else return 1;
  else if (ys->filepos<=0) return -1;
  else if (xs->filepos<ys->filepos) return -1;
  else if (xs->filepos>ys->filepos) return 1;
  else return 0;
}

static int run_schedule(struct FD_FILE_INDEX *fx,int n,
                        union SCHEDULE *schedule,
                        unsigned int *offsets,
                        fdtype *values)
{
  struct FD_STREAM *stream=&(fx->index_stream);
  struct FD_INBUF *instream=fd_readbuf(stream);
  unsigned i=0, pos_offset=fx->index_n_slots*4;
  while (i < n) {
    if (schedule[i].fs.filepos<=0) return i;
    if ((offsets == NULL) && (schedule[i].fs.probe<0)) {
      /* When the probe offset is negative, it means we are reading
         the offset itself. */
      fd_setpos(&(fx->index_stream),schedule[i].fs.filepos);
      schedule[i].fs.filepos=(fd_off_t)(fd_read_4bytes(instream));
      if (schedule[i].fs.filepos)
        schedule[i].fs.probe=(-schedule[i].fs.probe)-1;
      else {
        /* The key has no key entry, thus no values.  Morph it into
           a completed value entry. */
        struct VALUE_FETCH_SCHEDULE *vs=&(schedule[i].vs);
        int real_index=-(vs->index)-1;
        /* This is where it would have been. */
        vs->probe=-(vs->probe)-1;
        if (FD_VOIDP(values[real_index]))
          values[real_index]=FD_EMPTY_CHOICE;
        vs->filepos=-1; vs->index=real_index;
        vs->n_values=0;}}
    else if (schedule[i].fs.index<0) { /* Still looking for the key */
      unsigned int n_values; fd_off_t vpos; fdtype key;
      struct KEY_FETCH_SCHEDULE *ks=
        (struct KEY_FETCH_SCHEDULE *)(&(schedule[i]));
      /* Go to the key location and read the keydata */
      fd_setpos(&(fx->index_stream),schedule[i].fs.filepos+pos_offset);
      n_values=fd_read_4bytes(instream);
      vpos=(fd_off_t)fd_read_4bytes(instream);
      key=fd_read_dtype(instream);
      if (FD_ABORTP(key)) return fd_interr(key);
      else if (FDTYPE_EQUAL(key,schedule[i].fs.key)) {
        /* If you found the key, morph the entry into a value
           fetching entry. */
        struct VALUE_FETCH_SCHEDULE *vs=(struct VALUE_FETCH_SCHEDULE *)ks;
        /* Make the index positive, indicating that you are collecting
           the value now. */
        unsigned int index=vs->index=-(vs->index)-1;
        vs->filepos=vpos+pos_offset; vs->n_values=n_values;
        assert(((vs->filepos)<((fd_off_t)(0x100000000LL))));
        fd_decref(key); /* No longer needed */
        if (n_values>1)
          if (FD_VOIDP(values[index]))
            values[index]=fd_init_achoice(NULL,n_values,0);
          else {
            fdtype val=values[index];
            values[index]=fd_init_achoice(NULL,n_values,0);
            FD_ADD_TO_CHOICE(values[index],val);}
        else if (n_values==0) {
          /* If there are no values, store the empty choice and
             declare the entry done by setting its filepos to -1. */
          vs->filepos=-1;
          values[index]=FD_EMPTY_CHOICE;}
        else if (FD_VOIDP(values[index]))
          values[index]=FD_EMPTY_CHOICE;}
      else {
        /* Keep looking for the key */
        struct KEY_FETCH_SCHEDULE *ks=
          (struct KEY_FETCH_SCHEDULE *)(&(schedule[i]));
        ks->probe=(ks->probe+ks->chain_width)%(fx->index_n_slots);
        fd_decref(key); /* No longer needed */
        if (offsets==NULL) {
          /* Do an offset probe now. */
          ks->filepos=(ks->probe*4)+8; ks->probe=-(ks->probe+1);}
        else if (offsets[ks->probe])
          /* If you have the next key location, store it. */
          ks->filepos=offget(offsets,ks->probe);
        else {
          /* In this case, you know the key isn't in the table, so
             set the value to the empty choice and the filepos to 0. */
          int index=-(ks->index)-1;
          if (FD_VOIDP(values[index]))
            values[index]=FD_EMPTY_CHOICE;
          ks->index=0;
          ks->filepos=-1;}}}
    else {
      struct VALUE_FETCH_SCHEDULE *vs=
        (struct VALUE_FETCH_SCHEDULE *)(&(schedule[i]));
      fd_off_t vpos=vs->filepos; fdtype val; int index=vs->index;
      fd_setpos(stream,vpos);
      val=fd_read_dtype(instream);
      if (FD_ABORTP(val)) return fd_interr(val);
      FD_ADD_TO_CHOICE(values[index],val);
      vpos=(fd_off_t)fd_read_4bytes(instream);
      while (vpos==1) {
        val=fd_read_dtype(instream);
        if (FD_ABORTP(val)) return fd_interr(val);
        FD_ADD_TO_CHOICE(values[index],val);
        vpos=(fd_off_t)fd_read_4bytes(instream);}
      if (vpos==0) {vs->filepos=-1;}
      else vs->filepos=vpos+pos_offset;
      assert(((vs->filepos)<((fd_off_t)(0x100000000LL))));}
    i++;}
  return n;
}

static fdtype *fetchn(struct FD_FILE_INDEX *fx,int n,fdtype *keys,int lock_adds)
{
  unsigned int *offsets=fx->index_offsets;
  union SCHEDULE *schedule=u8_alloc_n(n,union SCHEDULE);
  fdtype *values=u8_alloc_n(n,fdtype);
  int i=0, schedule_size=0, init_schedule_size; while (i < n) {
    fdtype key=keys[i], cached=fd_hashtable_get(&(fx->index_cache),key,FD_VOID);
    if (redundantp(fx,key))
      values[i++]=FD_EMPTY_CHOICE;
    else if (FD_VOIDP(cached)) {
      struct KEY_FETCH_SCHEDULE *ksched=
        (struct KEY_FETCH_SCHEDULE *)&(schedule[schedule_size]);
      int hashcode=file_index_hash(fx,key);
      int probe=hashcode%(fx->index_n_slots);
      ksched->key=key; ksched->index=-(i+1);
      if (offsets) {
        ksched->filepos=offget(offsets,probe);
        ksched->probe=probe;}
      else {
        ksched->filepos=8+probe*4;
        ksched->probe=(-(probe+1));}
      ksched->chain_width=hashcode%(fx->index_n_slots-2)+1;
      if (ksched->filepos)
        if (lock_adds)
          values[i]=fd_hashtable_get(&(fx->index_adds),key,FD_VOID);
        else values[i]=fd_hashtable_get_nolock(&(fx->index_adds),key,FD_VOID);
      else if (lock_adds)
        values[i]=fd_hashtable_get(&(fx->index_adds),key,FD_EMPTY_CHOICE);
      else values[i]=fd_hashtable_get_nolock(&(fx->index_adds),key,FD_EMPTY_CHOICE);
      i++; schedule_size++;}
    else values[i++]=cached;}
  init_schedule_size=schedule_size;
  qsort(schedule,schedule_size,sizeof(struct FETCH_SCHEDULE),sort_by_filepos);
  while (schedule_size>0) {
    schedule_size=run_schedule(fx,schedule_size,schedule,offsets,values);
    if (schedule_size<=0) break;
    qsort(schedule,schedule_size,sizeof(struct FETCH_SCHEDULE),sort_by_filepos);}
  /* Note that we should now look at fx->index_edits and integrate any changes,
     but we're not doing that now. */
  if (schedule_size<0) {
    int k=0; while (k<init_schedule_size) {
      if (schedule[k].fs.filepos<0)
        fd_decref(values[schedule[k].fs.index]);
      k++;}
    u8_free(values);
    u8_free(schedule);
    return NULL;}
  else {
    int k=0; while (k<n) {
      fdtype v=values[k++];
      if (FD_ACHOICEP(v)) {
        struct FD_ACHOICE *ac=(struct FD_ACHOICE *)v;
        ac->achoice_uselock=1;}}
    u8_free(schedule);
    return values;}
}

static fdtype *file_index_fetchn(fd_index ix,int n,fdtype *keys)
{
  struct FD_FILE_INDEX *fx=(struct FD_FILE_INDEX *)ix;
  fdtype *results;
  fd_lock_index(fx);
  results=fetchn(fx,n,keys,1);
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
  fdtype key; int serial, slotno, chain_width, n_values;
  fd_off_t pos;};

/* This is used to track which slotnos are newly filled when we are writing
    a file index without a vector of cached offsets. */
struct RESERVATIONS {
  unsigned int *slotnos;
  int n_reservations, max_reservations;};

static int sort_keydata(const void *kvx,const void *kvy)
{
  const struct KEYDATA *x=kvx, *y=kvy;
  if (x->chain_width<0)
    if (y->chain_width>0) return 1; else return 0;
  else if (y->chain_width<0) return -1;
  else if (x->pos < y->pos) return -1;
  else if (x->pos > y->pos) return 1;
  else return 0;
}

static int sort_keydata_pos(const void *kvx,const void *kvy)
{
  const struct KEYDATA *x=kvx, *y=kvy;
  if (x->pos < y->pos) return -1;
  else if (x->pos > y->pos) return 1;
  else return 0;
}

static int sort_keydata_serial(const void *kvx,const void *kvy)
{
  const struct KEYDATA *x=kvx, *y=kvy;
  if (x->serial < y->serial) return -1;
  else if (x->serial > y->serial) return 1;
  else return 0;
}

static int reserve_slotno(struct RESERVATIONS *r,unsigned int slotno)
{
  unsigned int *slotnos=r->slotnos, *lim=r->slotnos+r->n_reservations;
  unsigned int *bottom=slotnos, *top=lim;
  unsigned int *middle=bottom+(top-bottom)/2, insertoff, *insertpos;
  if (r->n_reservations==0) {
    bottom[0]=slotno; r->n_reservations++;
    return 1;}
  else while (bottom<=top) {
    middle=bottom+(top-bottom)/2;
    if ((middle<slotnos) || (middle>=lim)) break;
    else if (*middle==slotno) return 0;
    else if (slotno>*middle) bottom=middle+1;
    else top=middle-1;}
  insertoff=(middle-slotnos);
  if ((middle<lim) && (slotno>*middle)) insertoff++;
  if (r->n_reservations == r->max_reservations) {
    int new_max=r->max_reservations*2;
    slotnos=r->slotnos=u8_realloc(r->slotnos,SLOTSIZE*new_max);
    r->max_reservations=new_max;}
  insertpos=slotnos+insertoff;
  if (!(((insertpos<=slotnos) || (slotno>insertpos[-1])) &&
        ((insertpos>=(slotnos+r->n_reservations)) || (slotno<insertpos[0]))))
    u8_log(LOG_CRIT,fd_FileIndexError,
           "Corrupt reservations table when saving index");
  if (insertoff<r->n_reservations)
    memmove(insertpos+1,insertpos,(SLOTSIZE*(r->n_reservations-insertoff)));
  *insertpos=slotno;
  r->n_reservations++;
  return 1;
}

/* Fetching keydata:
    This finds where all the keys are, reserving slots for them if neccessary.
    It also fetches the current number of values and the valuepos.
   Returns the number of new keys or -1 on error. */
static int fetch_keydata(struct FD_FILE_INDEX *fx,
                         struct FD_INBUF *instream,
                         struct KEYDATA *kdata,
                         int n,unsigned int *offsets)
{
  struct RESERVATIONS reserved;
  struct FD_STREAM *stream=&(fx->index_stream);
  unsigned int pos_offset=fx->index_n_slots*4, chain_length=0;
  int i=0, max=n, new_keys=0;
  if (offsets == NULL) {
    reserved.slotnos=u8_malloc(SLOTSIZE*64);
    reserved.n_reservations=0; reserved.max_reservations=64;}
  else {
    reserved.slotnos=NULL;
    reserved.n_reservations=0; reserved.max_reservations=0;}
  /* Setup the key data */
  while (i < n) {
    fdtype key=kdata[i].key;
    int hash=file_index_hash(fx,key);
    int probe=hash%(fx->index_n_slots), chain_width=hash%(fx->index_n_slots-2)+1;
    if (offsets) {
      int koff=offsets[probe];
      /* Skip over all the reserved slots */
      while (koff==1) {
        probe=(probe+chain_width)%(fx->index_n_slots);
        koff=offsets[probe];}
      if (koff) {
        /* We found a full slot, queue it for examination. */
        kdata[i].slotno=probe; kdata[i].chain_width=chain_width;
        kdata[i].pos=koff+pos_offset;}
      else {
        /* We have an empty slot we can fill */
        offsets[probe]=1; new_keys++; /* Fill it */
        kdata[i].slotno=probe;
        kdata[i].chain_width=-1; /* Declare it found */
        /* We initialize .n_values to zero unless it has already been
           initialized.  This would be the case if the key's value was edited
           (e.g. had the value set or values dropped), which means that the
           values were already written. */
        if (kdata[i].n_values<0) kdata[i].n_values=0;
        kdata[i].pos=0;}}
    else {
      /* A negative probe value means that we are getting an offset
         from the offset table. */
      kdata[i].slotno=-(probe+1); kdata[i].chain_width=chain_width;
      kdata[i].pos=8+SLOTSIZE*probe;}
    i++;}
  /* Now, collect keydata */
  while (max>0) {
    /* Sort by filepos for more coherent and hopefully faster disk access.
       We use a negative chain width to indicate that we're done with the
       entry. */
    qsort(kdata,max,sizeof(struct KEYDATA),sort_keydata);
    i=0; while (i<max) {
      if (kdata[i].chain_width<0) break;
      fd_setpos(stream,kdata[i].pos);
      if (kdata[i].slotno<0) { /* fetching offset */
        unsigned int off=fd_read_4bytes(instream);
        unsigned int slotno=(-(kdata[i].slotno)-1);
        if (off) {
          kdata[i].slotno=slotno;
          kdata[i].pos=off+SLOTSIZE*(fx->index_n_slots);}
        else if (reserve_slotno(&reserved,slotno)) {
          kdata[i].slotno=slotno; new_keys++;
          kdata[i].chain_width=-1; kdata[i].pos=0;
          if (kdata[i].n_values<0) kdata[i].n_values=0;}
        else {
          int next_probe=(slotno+kdata[i].chain_width)%(fx->index_n_slots);
          kdata[i].slotno=-(next_probe+1);
          kdata[i].pos=8+SLOTSIZE*next_probe;}
        i++;}
      else {
        unsigned int n_vals, vpos; fdtype key;
        n_vals=fd_read_4bytes(instream);
        vpos=fd_read_4bytes(instream);
        key=fd_read_dtype(instream);
        if (FDTYPE_EQUAL(key,kdata[i].key)) {
          kdata[i].pos=vpos; kdata[i].chain_width=-1;
          if (kdata[i].n_values<0) kdata[i].n_values=n_vals;}
        else if (offsets) {
          int next_probe=(kdata[i].slotno+kdata[i].chain_width)%(fx->index_n_slots);
          /* Compute the next probe location, skipping slots
             already taken by keys being dumped for the first time,
             which is indicated by an offset value of 1. */
          while ((offsets[next_probe]) && ((offsets[next_probe])==1))
            next_probe=(next_probe+kdata[i].chain_width)%(fx->index_n_slots);
          if (offsets[next_probe]) {
            /* If we have an offset, it is a key on disk that we need
               to look at. */
            kdata[i].slotno=next_probe;
            kdata[i].pos=offsets[next_probe]+pos_offset;}
          else {
            /* Otherwise, we have an empty slot we can put this value in. */
            new_keys++;
            kdata[i].slotno=next_probe;
            kdata[i].pos=0;
            if (kdata[i].n_values<0) kdata[i].n_values=0;
            offsets[next_probe]=1;
            kdata[i].chain_width=-1;}}
        else {
          int next_probe=(kdata[i].slotno+kdata[i].chain_width)%(fx->index_n_slots);
          kdata[i].slotno=-(next_probe+1);
          kdata[i].pos=8+(SLOTSIZE*next_probe);}
        fd_decref(key);
        i++;}}
    if (max==i)
      if (chain_length>256) {
        if (offsets == NULL) u8_free(reserved.slotnos);
        return fd_reterr(fd_FileIndexOverflow,"fetch_keydata",
                         u8_strdup(fx->indexid),FD_VOID);}
      else chain_length++;
    else chain_length=0;
    max=i;}
  if (offsets == NULL) u8_free(reserved.slotnos);
  return new_keys;
}

#if 0 /* For debugging */
static void check_reservations(unsigned int *iv,int len)
{
  int i=0, last=-1; while (i<len) {
    if ((i==0) || (last<iv[i]))
      fprintf(stdout,"Reservation: %d\n",iv[i]);
    else fprintf(stdout,"!!! %d\n",iv[i]);
    last=iv[i]; i++;}
}
#endif

static int write_values(fd_stream stream,
                        struct FD_OUTBUF *outstream,
                        fdtype values,
                        unsigned int nextpos,
                        int *n_valuesp)
{
  fdtype realval=((FD_ACHOICEP(values)) ? (fd_make_simple_choice(values)) :
                   (values));
  if (FD_EMPTY_CHOICEP(realval)) {
    *n_valuesp=0; return 0;}
  else if (FD_CHOICEP(realval)) {
    struct FD_CHOICE *ch=
      FD_CONSPTR(fd_choice,realval);
    int size=0;
    const fdtype *scan=FD_XCHOICE_DATA(ch), *limit=scan+FD_XCHOICE_SIZE(ch);
    while (scan < limit) {
      size=size+fd_write_dtype(outstream,*scan)+4; scan++;
      if (scan == limit) fd_write_4bytes(outstream,nextpos);
      else fd_write_4bytes(outstream,1);}
    *n_valuesp=FD_XCHOICE_SIZE(ch);
    if (FD_ACHOICEP(values)) fd_decref(realval);
    return size;}
  else {
    int size=fd_write_dtype(outstream,realval);
    *n_valuesp=1;
    fd_write_4bytes(outstream,nextpos);
    return size+4;}
}

/* Committing edits */

/* This extends the KEYDATA vector with entries for the values
   which are being set or modified by dropping. */
static int commit_edits(struct FD_FILE_INDEX *f,
                        struct FD_OUTBUF *outstream,
                        struct KEYDATA *kdata)
{
  struct FD_STREAM *stream=&(f->index_stream);
  int i=0, n_edits=0, n_drops=0; fd_off_t filepos;
  fdtype *dropkeys, *dropvals;
  struct FD_HASH_BUCKET **scan, **limit;
  if (f->index_edits.table_n_keys==0) return 0;
  dropkeys=u8_alloc_n(f->index_edits.table_n_keys,fdtype);
  scan=f->index_edits.ht_buckets; limit=scan+f->index_edits.ht_n_buckets;
  while (scan < limit)
    if (*scan) {
      /* Now we go through the edits table, finding all the drops.
         We need to retrieve their values on disk in order to write
         out a new value. */
      struct FD_HASH_BUCKET *e=*scan; int n_keyvals=e->fd_n_entries;
      struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+n_keyvals;
      while (kvscan<kvlimit) {
        fdtype key=kvscan->kv_key;
        if ((FD_PAIRP(key)) &&
            (FD_EQ(FD_CAR(key),drop_symbol)) &&
            (!(FD_VOIDP(kvscan->kv_val)))) {
          fdtype cached=fd_hashtable_get(&(f->index_cache),FD_CDR(key),FD_VOID);
          if (!(FD_VOIDP(cached))) {
            /* If the value of the key is cached, it will be up to date with
               these drops, so we just convert the key to a "set" key
               and store the cached value there.  Note that this breaks the
               hashtable, but it doesn't matter because we're going to reset
               it anyway. */
            struct FD_PAIR *pair=
              fd_consptr(struct FD_PAIR *,key,fd_pair_type);
            fd_decref(kvscan->kv_val); kvscan->kv_val=cached;
            pair->car=set_symbol;}
          else dropkeys[n_drops++]=FD_CDR(key);}
        kvscan++;}
      scan++;}
    else scan++;
  fd_set_direction(stream,fd_byteflow_write);
  if (n_drops)
    dropvals=fetchn(f,n_drops,dropkeys,0);
  else dropvals=NULL;
  filepos=fd_endpos(stream);
  scan=f->index_edits.ht_buckets; limit=scan+f->index_edits.ht_n_buckets;
  while (scan < limit) {
    fd_outbuf outstream=fd_writebuf(stream);
    if (*scan) {
      struct FD_HASH_BUCKET *e=*scan; int n_keyvals=e->fd_n_entries;
      struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+n_keyvals;
      while (kvscan<kvlimit) {
        fdtype key=kvscan->kv_key;
        if (FD_VOIDP(kvscan->kv_val)) kvscan++;
        else if (FD_PAIRP(key)) {
          kdata[n_edits].key=FD_CDR(key); kdata[n_edits].pos=filepos;
          if (FD_EQ(FD_CAR(key),set_symbol)) {
            /* If it's a set edit, just write out the whole thing */
            if (FD_EMPTY_CHOICEP(kvscan->kv_val)) {
              kdata[n_edits].n_values=0; kdata[n_edits].pos=0;}
            else filepos=filepos+
                   write_values(stream,outstream,
                                kvscan->kv_val,0,
                                &(kdata[n_edits].n_values));}
          else if (FD_EQ(FD_CAR(key),drop_symbol)) {
            /* If it's a drop edit, you got the value, so compute
               the difference and write that out.*/
            fdtype new_value=fd_difference(dropvals[i],kvscan->kv_val);
            if (FD_EMPTY_CHOICEP(new_value)) {
              kdata[n_edits].n_values=0; kdata[n_edits].pos=0;}
            else filepos=filepos+write_values
                   (stream,outstream,new_value,0,&(kdata[n_edits].n_values));
            fd_decref(new_value);}
          n_edits++; kvscan++;}
        else kvscan++;}
      scan++;}
    else scan++;}
  if (n_drops) {
    i=0; while (i<n_drops) {fd_decref(dropvals[i]); i++;}
    u8_free(dropvals);}
  u8_free(dropkeys);
  return n_edits;
}

static void write_keys(struct FD_FILE_INDEX *fx,
                       struct FD_OUTBUF *outstream,
                       int n,struct KEYDATA *kdata,
                       unsigned int *offsets)
{
  unsigned int pos_offset=fx->index_n_slots*4;
  struct FD_STREAM *stream=&(fx->index_stream);
  fd_off_t pos=fd_endpos(stream);
  int i=0; while (i<n) {
    fd_off_t kpos=pos;
    fd_write_4bytes(outstream,kdata[i].n_values);
    fd_write_4bytes(outstream,(unsigned int)kdata[i].pos);
    pos=pos+fd_write_dtype(outstream,kdata[i].key)+8;
    if (offsets)
      offsets[kdata[i].slotno]=(kpos-pos_offset);
    else kdata[i].pos=(kpos-pos_offset);
    i++;}
}

static void write_offsets(struct FD_FILE_INDEX *fx,
                          struct FD_OUTBUF *outstream,
                          int n,
                          struct KEYDATA *kdata,
                          unsigned int *offsets)
{
  struct FD_STREAM *stream=&(fx->index_stream);
  if (offsets) {
    fd_start_write(stream,8);
    fd_write_ints(stream,fx->index_n_slots,offsets);
    fd_flush_stream(stream);}
  else {
    int i=0;
    qsort(kdata,n,sizeof(struct KEYDATA),sort_keydata_pos);
    while (i < n) {
      fd_setpos(stream,8+kdata[i].slotno*4);
      fd_write_4bytes(outstream,(unsigned int)kdata[i].pos);
      i++;}
    fd_flush_stream(stream);}
}

/* Putting it all together */

static int file_index_commit(struct FD_INDEX *ix)
{
  struct FD_FILE_INDEX *fx=(struct FD_FILE_INDEX *)ix;
  struct FD_STREAM *stream=&(fx->index_stream);
  unsigned int *new_offsets=NULL, gc_new_offsets=0;
  int pos_offset=fx->index_n_slots*4, newcount;
  double started=u8_elapsed_time();
  fd_write_lock_table(&(ix->index_adds));
  fd_write_lock_table(&(ix->index_edits));
  fd_lock_index(fx);
  /* Get the current offsets from the index */
#if HAVE_MMAP
  if (fx->index_offsets) {
    int i=0, n=fx->index_n_slots;
    /* We have to copy these if they're MMAPd, because
       we can't modify them (while updating) otherwise.  */
    new_offsets=u8_alloc_n((fx->index_n_slots),unsigned int);
    gc_new_offsets=1;
    while (i<n) {
      new_offsets[i]=offget(fx->index_offsets,i); i++;}}
#else
  if (fx->index_offsets) {
    fd_inbuf *instream=fd_readbuf(stream);
    new_offsets=fx->index_offsets;
    fd_start_read(stream,8);
    fd_read_ints(instream,fx->ht_n_buckets,new_offsets);}
#endif
  {
    fd_off_t filepos;
    struct FD_OUTBUF *outstream=fd_writebuf(stream);
    int n_adds=ix->index_adds.table_n_keys, n_edits=ix->index_edits.table_n_keys;
    int i=0, n=0, n_changes=n_adds+n_edits, add_index;
    struct KEYDATA *kdata=u8_alloc_n(n_changes,struct KEYDATA);
    unsigned int *value_locs=
      ((n_edits) ? (u8_alloc_n(n_edits,unsigned int)) : (NULL));
    struct FD_HASH_BUCKET **scan=ix->index_adds.ht_buckets;
    struct FD_HASH_BUCKET **limit=scan+ix->index_adds.ht_n_buckets;
    while (scan < limit)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int n_keyvals=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+n_keyvals;
        while (kvscan<kvlimit) {
          fdtype key=kvscan->kv_key;
          /* It would be nice to update slotids here, but we'll
             decline for now and require that those be managed
             manually. */
          kdata[n].key=key;
          /* We'll use this to sort back into the order of the adds table */
          kdata[n].serial=n;
          /* Initialize the other fields */
          kdata[n].n_values=-1;
          kdata[n].slotno=-1;
          kdata[n].pos=-1;
          n++; kvscan++;}
        scan++;}
      else scan++;
    /* add_index is the point were key entries for simple additions end and
       key entries for edits begin. */
    add_index=n;
    n=n+commit_edits(fx,outstream,kdata+n);
    /* Copy the value locations recorded by commit_edits into
       value_locs.  (The .pos field will be used by fetch_keydata). */
    i=add_index; while (i<n) {
      kdata[i].serial=i; value_locs[i-add_index]=((unsigned int)kdata[i].pos);
      i++;}
    newcount=fetch_keydata(fx,fd_readbuf(stream),kdata,n,new_offsets);
    if (newcount<0) {
      u8_free(kdata);
      if (value_locs) u8_free(value_locs);
      if (gc_new_offsets) u8_free(new_offsets);
      fd_unlock_table((&(ix->index_adds)));
      fd_unlock_table((&(ix->index_edits)));
      fd_unlock_index(fx);
      return newcount;}
    /* Sort back into the original order. */
    qsort(kdata,n,sizeof(struct KEYDATA),sort_keydata_serial);
    /* Copy back the value locations written by commit_edits. */
    i=add_index; while (i<n) {
      if (value_locs[i-add_index])
        kdata[i].pos=((fd_off_t)(value_locs[i-add_index]-pos_offset));
      else kdata[i].pos=((fd_off_t)0);
      i++;}
    /* Now, scan the adds again and write the added values. */
    scan=ix->index_adds.ht_buckets; limit=scan+ix->index_adds.ht_n_buckets;
    i=0; while (scan < limit) {
      struct FD_OUTBUF *outstream=fd_writebuf(stream);
      filepos=fd_endpos(stream);
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int n_keyvals=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+n_keyvals;
        while (kvscan<kvlimit) {
          fd_off_t writepos=filepos; int new_values;
          filepos=filepos+
            write_values(stream,outstream,
                         kvscan->kv_val,kdata[i].pos,
                         &new_values);
          kdata[i].pos=writepos-pos_offset;
          kdata[i].n_values=kdata[i].n_values+new_values;
          i++; kvscan++;}
        scan++;}
      else scan++;}
    write_keys(fx,fd_writebuf(stream),n,kdata,new_offsets);
    /* Write recovery information which can be used to restore the
       offsets table. */
    if ((fd_acid_files) && (new_offsets))
      write_file_index_recovery_data(fx,new_offsets);
    /* Now, start writing the offsets themselves */
    write_offsets(fx,fd_writebuf(stream),n,kdata,new_offsets);
    if (fd_acid_files) {
      int retval=0;
      if (new_offsets) fd_setpos(stream,0);
      if (new_offsets==NULL) {}
      else if (fx->index_hashv==1)
        fd_write_4bytes(outstream,FD_FILE_INDEX_MAGIC_NUMBER);
      else if (fx->index_hashv==2)
        fd_write_4bytes(outstream,FD_MULT_FILE_INDEX_MAGIC_NUMBER);
      fd_flush_stream(stream);
      /* Now erase the recovery information, since we don't need it anymore. */
      if (new_offsets) {
        fd_off_t end=fd_endpos(stream);
        fd_movepos(stream,-(4*(fx->index_n_slots)));
        retval=ftruncate(stream->stream_fileno,end-(4*(fx->index_n_slots)));
        if (retval<0)
          u8_log(LOG_ERR,"file_index_commit",
                 "Trouble truncating recovery information from %s",
                 fx->indexid);}}
    fd_unlock_index(fx);
    if (value_locs) u8_free(value_locs);
    u8_free(kdata);
    if (gc_new_offsets) u8_free(new_offsets);

    u8_log(fdkb_loglevel,"FileIndexCommit",
           "Saved mappings for %d keys to %s in %f secs",
           n_changes,ix->indexid,u8_elapsed_time()-started);

    fd_reset_hashtable(&(ix->index_adds),67,0);
    fd_unlock_table(&(ix->index_adds));
    fd_reset_hashtable(&(ix->index_edits),67,0);
    fd_unlock_table(&(ix->index_edits));
    return n;}
}

static void write_file_index_recovery_data(struct FD_FILE_INDEX *fx,
                                           unsigned int *offsets)
{
  struct FD_STREAM *stream=&(fx->index_stream);
  struct FD_OUTBUF *outstream;
  int i=0, n_slots=fx->index_n_slots; unsigned int magic_no;
  fd_endpos(stream);
  outstream=fd_writebuf(stream);
  while (i<n_slots) {
    unsigned int off=offget(offsets,i);
    fd_write_4bytes(outstream,off);
    i++;}
  fd_setpos(stream,0);
  magic_no=fd_read_4bytes(fd_readbuf(stream));
  /* Compute a variant magic number indicating that the
     index needs to be restored. */
  magic_no=magic_no|0x20;
  fd_setpos(stream,0);
  fd_write_4bytes(fd_writebuf(stream),magic_no);
  fd_flush_stream(stream);
}

static int recover_file_index(struct FD_FILE_INDEX *fx)
{
  /* This reads the offsets vector written at the end of the file
     during commitment. */
  int i=0, len=fx->index_n_slots, retval=0; fd_off_t new_end;
  unsigned int *offsets=u8_malloc(4*len), magic_no;
  struct FD_STREAM *s=&(fx->index_stream);
  fd_outbuf outstream; fd_inbuf instream;
  fd_endpos(s); new_end=fd_movepos(s,-(4*len));
  instream=fd_readbuf(s);
  while (i<len) {
    offsets[i]=fd_read_4bytes(instream); i++;}
  outstream=fd_writebuf(s);
  fd_setpos(s,24);
  i=0; while (i<len) {
    fd_write_4bytes(outstream,offsets[i]); i++;}
  instream=fd_readbuf(s);
  fd_setpos(s,0);
  magic_no=fd_read_4bytes(instream);
  outstream=fd_writebuf(s);
  fd_setpos(s,0);
  fd_write_4bytes(outstream,(magic_no&(~0x20)));
  fd_flush_stream(s);
  retval=ftruncate(s->stream_fileno,new_end);
  if (retval<0) return retval;
  else retval=fsync(s->stream_fileno);
  return retval;
}

static void file_index_close(fd_index ix)
{
  struct FD_FILE_INDEX *fx=(struct FD_FILE_INDEX *)ix;
  fd_lock_index(fx);
  fd_close_stream(&(fx->index_stream),0);
  if (fx->index_offsets) {
#if HAVE_MMAP
    int retval=munmap(fx->index_offsets-2,(SLOTSIZE*fx->index_n_slots)+8);
    if (retval<0) {
      u8_log(LOG_CRIT,u8_strerror(errno),
             "[%d:%d] file_index_close:munmap %s",
             retval,errno,fx->index_source);
      errno=0;}
#else
    u8_free(fx->index_offsets);
#endif
    fx->index_offsets=NULL;
    fx->index_cache_level=-1;}
  fd_unlock_index(fx);
}

static void file_index_setbuf(fd_index ix,int bufsiz)
{
  struct FD_FILE_INDEX *fx=(struct FD_FILE_INDEX *)ix;
  fd_lock_index(fx);
  fd_stream_setbufsize(&(fx->index_stream),bufsiz);
  fd_unlock_index(fx);
}

/* File index ops */

static fdtype file_index_op(fd_index ix,int op,int n,fdtype *args)
{
  struct FD_FILE_INDEX *hx=(struct FD_FILE_INDEX *)ix;
  if ( ((n>0)&&(args==NULL)) || (n<0) )
    return fd_err("BadIndexOpCall","file_index_op",
                  hx->indexid,FD_VOID);
  else switch (op) {
    case FD_INDEXOP_CACHELEVEL:
      if (n==0)
        return FD_INT(hx->index_cache_level);
      else {
        fdtype arg=(args)?(args[0]):(FD_VOID);
        if ((FD_FIXNUMP(arg))&&(FD_FIX2INT(arg)>=0)&&
            (FD_FIX2INT(arg)<0x100)) {
          file_index_setcache(ix,FD_FIX2INT(arg));
          return FD_INT(hx->index_cache_level);}
        else return fd_type_error
               (_("cachelevel"),"file_index_op/cachelevel",arg);}
    case FD_INDEXOP_BUFSIZE: {
      if (n==0)
        return FD_INT(hx->index_stream.buf.raw.buflen);
      else if (FD_FIXNUMP(args[0])) {
        file_index_setbuf(ix,FD_FIX2INT(args[0]));
        return FD_INT(hx->index_stream.buf.raw.buflen);}
      else return fd_type_error("buffer size","file_index_op/bufsize",args[0]);}
    case FD_INDEXOP_HASH: {
      if (n==0)
        return FD_INT(hx->index_n_slots);
      else {
        fdtype mod_arg=(n>1) ? (args[1]) : (FD_VOID);
        unsigned int hash=file_index_hash(hx,args[0]);
        if (FD_FIXNUMP(mod_arg))
          return FD_INT((hash%FD_FIX2INT(mod_arg)));
        else if ((FD_FALSEP(mod_arg))||(FD_VOIDP(mod_arg)))
          return FD_INT(hash);
        else return FD_INT(hash%(hx->index_n_slots));}}
    default:
      return FD_FALSE;}
}

/* Making file indexes */

FD_EXPORT
/* fd_make_file_index:
    Arguments: a filename string, a magic number (usigned int), an FD_OID,
    a capacity, and a dtype pointer to a metadata description (a slotmap).
    Returns: -1 on error, 1 on success. */
int fd_make_file_index(u8_string filename,unsigned int magicno,int n_slots_arg)
{
  int i, n_slots;
  struct FD_STREAM _stream;
  struct FD_STREAM *stream=
    fd_init_file_stream(&_stream,filename,FD_FILE_CREATE,-1,fd_driver_bufsize);
  struct FD_OUTBUF *outstream=fd_writebuf(stream);
  if (stream==NULL) return -1;
  else if ((stream->stream_flags)&FD_STREAM_READ_ONLY) {
    fd_seterr3(fd_CantWrite,"fd_make_file_index",u8_strdup(filename));
    fd_free_stream(stream);
    return -1;}

  stream->stream_flags&=~FD_STREAM_IS_CONSED;
  if (n_slots_arg<0) n_slots=-n_slots_arg;
  else n_slots=fd_get_hashtable_size(n_slots_arg);

  u8_log(LOG_INFO,"CreateFileIndex",
         "Creating a file index '%s' with %ld slots",
         filename,n_slots);

  fd_setpos(stream,0);
  fd_write_4bytes(outstream,magicno);
  fd_write_4bytes(outstream,n_slots);
  i=0; while (i<n_slots) {fd_write_4bytes(outstream,0); i++;}
  fd_write_4bytes(outstream,0xFFFFFFFE);
  fd_write_4bytes(outstream,40);
  i=0; while (i<8) {fd_write_4bytes(outstream,0); i++;}
  fd_close_stream(stream,FD_STREAM_FREEDATA);
  return 1;
}

static fd_index file_index_create(u8_string spec,void *type_data,
                                  fdkb_flags flags,fdtype opts)
{
  fdtype n_slots=fd_getopt(opts,fd_intern("SLOTS"),FD_INT(32000));
  if (!(FD_UINTP(n_slots))) {
    fd_seterr("NumberOfIndexSlots","file_index_create",spec,n_slots);
    return NULL;}
  else if (fd_make_file_index(spec,
                              (unsigned int)((unsigned long long)type_data),
                              FD_FIX2INT(n_slots))>=0)
    return fd_open_index(spec,flags,FD_VOID);
  else return NULL;
}


/* The handler struct */

static struct FD_INDEX_HANDLER file_index_handler={
  "file_index", 1, sizeof(struct FD_FILE_INDEX), 12,
  file_index_close, /* close */
  file_index_commit, /* commit */
  file_index_fetch, /* fetch */
  file_index_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  file_index_fetchn, /* fetchn */
  file_index_fetchkeys, /* fetchkeys */
  file_index_fetchsizes, /* fetchsizes */
  NULL, /* batchadd */
  NULL, /* metadata */
  file_index_create, /* create */
  NULL, /* walk */
  NULL, /* recycle */
  file_index_op  /* indexctl */
};

static u8_string match_index_name(u8_string spec,void *data)
{
  if ((u8_file_existsp(spec)) &&
      (fd_match4bytes(spec,data)))
    return spec;
  else if (u8_has_suffix(spec,".index",1))
    return NULL;
  else {
    u8_string variation=u8_mkstring("%s.index",spec);
    if ((u8_file_existsp(variation))&&
        (fd_match4bytes(variation,data)))
      return variation;
    else {
      u8_free(variation);
      return NULL;}}
}

FD_EXPORT void fd_init_fileindex_c()
{
  u8_register_source_file(_FILEINFO);

  set_symbol=fd_intern("SET");
  drop_symbol=fd_intern("DROP");
  slotids_symbol=fd_intern("%%SLOTIDS");
  fd_register_index_type("fileindex",
                         &file_index_handler,
                         open_file_index,
                         match_index_name,
                         (void *) U8_INT2PTR(FD_FILE_INDEX_MAGIC_NUMBER));
  fd_register_index_type("fileindex.v2",
                         &file_index_handler,
                         open_file_index,
                         match_index_name,
                         (void *) U8_INT2PTR(FD_MULT_FILE_INDEX_MAGIC_NUMBER));
  fd_register_index_type("damaged_fileindex",
                         &file_index_handler,
                         open_file_index,
                         match_index_name,
                         (void *) U8_INT2PTR(FD_FILE_INDEX_TO_RECOVER));
  fd_register_index_type("damaged_fileindex.v2",
                         &file_index_handler,
                         open_file_index,
                         match_index_name,
                         (void *) U8_INT2PTR(FD_MULT_FILE_INDEX_TO_RECOVER));
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
