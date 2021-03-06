/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu) (ken.haase@alum.mit.edu)
*/

#include "zcompress.h"

/* Hash indexes */

#define MYSTERIOUS_MODULUS 2000239099 /* 256000001 */

#define KNO_KINDEX_MAGIC_NUMBER 0x9011308
#define KNO_KINDEX_TO_RECOVER 0x9011328

#define KNO_KINDEX_FN_MASK	   0x0F
#define KNO_KINDEX_OFFTYPE_MASK  0x30
#define KNO_KINDEX_ODDKEYS	   0x40
#define KNO_KINDEX_READ_ONLY	   0x80
#define KNO_KINDEX_ONESLOT	   0x100

/* Used to generate hash codes */
#define MAGIC_MODULUS 16777213 /* 256000001 */
#define MIDDLIN_MODULUS 573786077 /* 256000001 */
#define MYSTERIOUS_MODULUS 2000239099 /* 256000001 */

#define KNO_KINDEX_NBUCKETS_POS	4
#define KNO_KINDEX_FORMAT_POS		8
#define KNO_KINDEX_NKEYS_POS	       16
#define KNO_KINDEX_KEYCOUNT_POS      16
#define KNO_KINDEX_NREFS_POS	       20
#define KNO_KINDEX_XREFS_POS	       24
#define KNO_KINDEX_XREFS_SIZE_POS    32
#define KNO_KINDEX_METADATA_POS      36

/* The hash index structure */

typedef struct KNO_KINDEX {
  KNO_INDEX_FIELDS;
  U8_MUTEX_DECL(index_lock);

  /* flags controls hash functions, compression, etc.
     hxcustom is a placeholder for a value to customize
     the hash function. */
  unsigned int kindex_format, index_custom, table_n_keys;
  kno_offset_type index_offtype;

  kno_off_t kx_metadata_pos;
  kno_off_t kx_xrefs_pos;
  struct XTYPE_REFS index_xrefs;

  /* Pointers to keyblocks for the hashtable */
  unsigned int *index_offdata;
  int index_n_buckets;

  /* The stream accessing the file.  This is only used
     for modification if the file is memmaped. */
  struct KNO_STREAM index_stream;} *kno_kindex;

/* Structure definitions */

struct KEY_SCHEDULE {
  int ksched_i; lispval ksched_key;
  unsigned int ksched_keyoff, ksched_xtsize;
  int ksched_bucket;
  KNO_CHUNK_REF ksched_chunk;};
struct VALUE_SCHEDULE {
  int vsched_i;
  lispval *vsched_write;
  int vsched_atomicp;
  KNO_CHUNK_REF vsched_chunk;};

struct POPULATE_SCHEDULE {
  lispval key; unsigned int kno_bucketno;
  unsigned int size;};
struct BUCKET_REF {
  /* max_new is only used when committing. */
  unsigned int bucketno, max_new;
  KNO_CHUNK_REF bck_ref;};

struct COMMIT_SCHEDULE {
  lispval commit_key, commit_values;
  int commit_replace:1, free_values:1;
  int commit_bucket;};

struct KEYENTRY {
  int ke_nvals, ke_xtrep_size;
  /* This points to the point in keybucket's kb_keybuf
     where the xtype representation of the key begins. */
  const unsigned char *ke_xtstart;
  lispval ke_values;
  KNO_CHUNK_REF ke_vref;};

struct KEYBUCKET {
  int kb_bucketno, kb_n_keys;
  unsigned char *kb_keybuf;
  struct KEYENTRY kb_elt0;};

typedef struct KNO_SLOTID_LOOKUP {
  int zindex; lispval slotid;} KNO_SLOTID_LOOKUP;
typedef struct KNO_SLOTID_LOOKUP *kno_slotid_lookup;

typedef struct KNO_BASEOID_LOOKUP {
  unsigned int zindex; KNO_OID baseoid;} KNO_BASEOID_LOOKUP;
typedef struct KNO_BASEOID_LOOKUP *kno_baseoid_lookup;

/* Utilities for XTYPE I/O */

#define nobytes(in,nbytes) (RARELY(!(kno_request_bytes(in,nbytes))))
#define havebytes(in,nbytes) (USUALLY(kno_request_bytes(in,nbytes)))

#define output_byte(out,b) \
  if (kno_write_byte(out,b)<0) return -1; else {}
#define output_4bytes(out,w) \
  if (kno_write_4bytes(out,w)<0) return -1; else {}
#define output_bytes(out,bytes,n) \
  if (kno_write_bytes(out,bytes,n)<0) return -1; else {}

KNO_EXPORT int kno_kindexp(struct KNO_INDEX *ix);

KNO_EXPORT ssize_t kno_kindex_bucket
(lispval index,lispval key,lispval modulate);

KNO_EXPORT lispval kno_kindex_keyinfo
(lispval lix,lispval min_count,lispval max_count);

