/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "chunkrefs.h"

/* Hash indexes */

#define MYSTERIOUS_MODULUS 2000239099 /* 256000001 */

#define FD_HASH_INDEX_MAGIC_NUMBER 0x8011308
#define FD_HASH_INDEX_TO_RECOVER 0x8011328

#define FD_HASH_INDEX_FN_MASK       0x0F
#define FD_HASH_INDEX_OFFTYPE_MASK  0x30
#define FD_HASH_INDEX_DTYPEV2       0x40
#define FD_HASH_INDEX_ODDKEYS       (FD_HASH_INDEX_DTYPEV2<<1)

#ifndef HASHINDEX_PREFETCH_WINDOW
#ifdef FD_MMAP_PREFETCH_WINDOW
#define HASHINDEX_PREFETCH_WINDOW FD_MMAP_PREFETCH_WINDOW
#else
#define HASHINDEX_PREFETCH_WINDOW 0
#endif
#endif

/* Used to generate hash codes */
#define MAGIC_MODULUS 16777213 /* 256000001 */
#define MIDDLIN_MODULUS 573786077 /* 256000001 */
#define MYSTERIOUS_MODULUS 2000239099 /* 256000001 */

#define FD_HASH_INDEX_KEYCOUNT_POS 16
#define FD_HASH_INDEX_SLOTIDS_POS 20
#define FD_HASH_INDEX_BASEOIDS_POS 32
#define FD_HASH_INDEX_METADATA_POS 44

/* The hash index structure */

typedef struct FD_HASH_INDEX {
  FD_INDEX_FIELDS;
  U8_MUTEX_DECL(index_lock);

  /* flags controls hash functions, compression, etc.
     hxcustom is a placeholder for a value to customize
     the hash function. */
  unsigned int fdkb_xformat, index_custom, table_n_keys;
  fd_offset_type index_offtype;

  /* This is used to store compressed keys and values. */
  int index_n_slotids, index_new_slotids; fdtype *index_slotids;
  struct FD_SLOTID_LOOKUP *slotid_lookup;
  int index_n_baseoids, index_new_baseoids;
  unsigned int *index_baseoid_ids;
  short *index_ids2baseoids;

  /* Pointers into keyblocks for the hashtable */
  unsigned int *index_offdata; int index_n_buckets;

  /* The stream accessing the file.  This is only used
     for modification if the file is memmaped. */
  struct FD_STREAM index_stream;
  /* When non-null, a memmapped pointer to the file contents. */
  size_t index_mmap_size; unsigned char *index_mmap;} *fd_hash_index;

/* Structure definitions */

struct KEY_SCHEDULE {
  int ksched_i; fdtype ksched_key;
  unsigned int ksched_keyoff, ksched_dtsize;
  int ksched_bucket;
  FD_CHUNK_REF ksched_chunk;};
struct VALUE_SCHEDULE {
  int vsched_i; 
  fdtype *vsched_write; 
  int vsched_atomicp; 
  FD_CHUNK_REF vsched_chunk;};

struct POPULATE_SCHEDULE {
  fdtype key; unsigned int fd_bucketno;
  unsigned int size;};
struct BUCKET_REF {
  /* max_new is only used when committing. */
  unsigned int bucketno, max_new;
  FD_CHUNK_REF bck_ref;};

struct COMMIT_SCHEDULE {
  fdtype commit_key, commit_values; 
  short commit_replace;
  int commit_bucket;};

struct KEYENTRY {
  int ke_nvals, ke_dtrep_size; 
  /* This points to the point in keybucket's kb_keybuf
     where the dtype representation of the key begins. */
  const unsigned char *ke_dtstart;
  fdtype ke_values; 
  FD_CHUNK_REF ke_vref;};

struct KEYBUCKET {
  int kb_bucketno, kb_n_keys;
  unsigned char *kb_keybuf;
  struct KEYENTRY kb_elt0;};

typedef struct FD_SLOTID_LOOKUP {
  int zindex; fdtype slotid;} FD_SLOTID_LOOKUP;
typedef struct FD_SLOTID_LOOKUP *fd_slotid_lookup;

typedef struct FD_BASEOID_LOOKUP {
  unsigned int zindex; FD_OID baseoid;} FD_BASEOID_LOOKUP;
typedef struct FD_BASEOID_LOOKUP *fd_baseoid_lookup;

/* Utilities for DTYPE I/O */

#define nobytes(in,nbytes) (FD_EXPECT_FALSE(!(fd_needs_bytes(in,nbytes))))
#define havebytes(in,nbytes) (FD_EXPECT_TRUE(fd_needs_bytes(in,nbytes)))

#define output_byte(out,b) \
  if (fd_write_byte(out,b)<0) return -1; else {}
#define output_4bytes(out,w) \
  if (fd_write_4bytes(out,w)<0) return -1; else {}
#define output_bytes(out,bytes,n) \
  if (fd_write_bytes(out,bytes,n)<0) return -1; else {}


FD_EXPORT int fd_populate_hash_index
  (struct FD_HASH_INDEX *hx,fdtype from,
   const fdtype *keys,int n_keys, int blocksize);
FD_EXPORT int fd_make_hash_index
  (u8_string,int,unsigned int,unsigned int,
   fdtype,fdtype,time_t,time_t);
FD_EXPORT int fd_hash_index_bucket
   (struct FD_HASH_INDEX *hx,fdtype key,int modulate);
FD_EXPORT int fd_hash_indexp(struct FD_INDEX *ix);
FD_EXPORT fdtype fd_hash_index_stats(struct FD_HASH_INDEX *ix);
