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


typedef struct FD_SLOTID_LOOKUP {
  int zindex; fdtype slotid;} FD_SLOTID_LOOKUP;
typedef struct FD_SLOTID_LOOKUP *fd_slotid_lookup;

typedef struct FD_BASEOID_LOOKUP {
  unsigned int zindex; FD_OID baseoid;} FD_BASEOID_LOOKUP;
typedef struct FD_BASEOID_LOOKUP *fd_baseoid_lookup;

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
  size_t index_mmap_size; unsigned char *index_mmap;} FD_HASH_INDEX;
typedef struct FD_HASH_INDEX *fd_hash_index;

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
