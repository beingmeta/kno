/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
   This file is part of beingmeta's Kno platform.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES KNO_DO_INLINE
#define KNO_INLINE_TABLES KNO_DO_INLINE
#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/hash.h"
#include "kno/tables.h"
#include "kno/numbers.h"
#include "kno/bloom.h"

#include <libu8/u8printf.h>

#include <math.h>

/* Original license/file headers (attributions) at the bottom of the file. */

// Note - This code makes a few assumptions about how your machine behaves -

// 1. We can read a 4-byte value from any address without crashing
// 2. sizeof(int) == 4

// And it has a few limitations -

// 1. It will not work incrementally.
// 2. It will not produce the same results on little-endian and big-endian
//    machines.

static unsigned int murmurhash2(const void * key, int len, const unsigned int seed)
{
  // 'm' and 'r' are mixing constants generated offline.
  // They're not really 'magic', they just happen to work well.

  const unsigned int m = 0x5bd1e995;
  const int r = 24;

  // Initialize the hash to a 'random' value

  unsigned int h = seed ^ len;

  // Mix 4 bytes at a time into the hash

  const unsigned char * data = (const unsigned char *)key;

  while(len >= 4)
    {
      unsigned int k = *(unsigned int *)data;

      k *= m;
      k ^= k >> r;
      k *= m;

      h *= m;
      h ^= k;

      data += 4;
      len -= 4;
    }

  // Handle the last few bytes of the input array

  switch(len)
    {
    case 3: h ^= data[2] << 16;
    case 2: h ^= data[1] << 8;
    case 1: h ^= data[0];
      h *= m;
    };

  // Do a few final mixes of the hash to ensure the last few
  // bytes are well-incorporated.

  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;

  return h;
}

#define MAKESTRING(n) STRING(n)
#define STRING(n) #n

inline static int test_bit_set_bit(unsigned char * buf,
                                   unsigned int x, int set_bit)
{
  unsigned int byte = x >> 3;
  unsigned char c = buf[byte];        // expensive memory access
  unsigned int mask = 1 << (x % 8);

  if (c & mask) {
    return 1;
  } else {
    if (set_bit) {
      buf[byte] = c | mask;
    }
    return 0;
  }
}


static int bloom_check_add(struct KNO_BLOOM * bloom,
                           const void * buffer, int len, int add)
{
  int hits = 0;
  if (bloom->bf == NULL)
    return -1;
  register unsigned int a = murmurhash2(buffer, len, 0x9747b28c);
  register unsigned int b = murmurhash2(buffer, len, a);
  register unsigned int x;
  register unsigned int i;

  for (i = 0; i < bloom->hashes; i++) {
    x = (a + i*b) % bloom->bits;
    if (test_bit_set_bit(bloom->bf, x, add)) {
      hits++;
    }
  }

  if (hits == bloom->hashes) {
    return 1;                // 1 == element already in (or collision)
  }

  if (add) bloom->bloom_adds++;

  return 0;
}

KNO_EXPORT
struct KNO_BLOOM *
kno_init_bloom_filter(struct KNO_BLOOM *use_bloom,int entries,double error)
{
  struct KNO_BLOOM *bloom = NULL;
  if (entries < 1)
    return KNO_ERR(NULL,kno_TypeError,"kno_bloom_init","bad n_entries arg",KNO_INT(entries));
  else if (error <= 0)
    return KNO_ERR(NULL,kno_TypeError,"kno_bloom_init","bad allowed error value",
                   kno_make_double(error));
  else if (use_bloom == NULL)
    bloom = u8_alloc(struct KNO_BLOOM);
  else bloom = use_bloom;

  if (use_bloom) {
    KNO_SET_CONS_TYPE(bloom,kno_bloom_filter_type);}
  else KNO_INIT_CONS(bloom,kno_bloom_filter_type);

  bloom->entries = entries;
  bloom->error = error;

  double num = log(bloom->error);
  double denom = 0.480453013918201; // ln(2)^2
  bloom->bpe = -(num / denom);

  double dentries = (double)entries;
  bloom->bits = (int)(dentries * bloom->bpe);

  if (bloom->bits % 8) {
    bloom->bytes = (bloom->bits / 8) + 1;
  } else {
    bloom->bytes = bloom->bits / 8;
  }

  bloom->hashes = (int)ceil(0.693147180559945 * bloom->bpe);  // ln(2)

  bloom->bf = u8_zmalloc(bloom->bytes);
  if (bloom->bf == NULL) {
    u8_graberrno("kno_bloom_init:mallocbytes",NULL);
    if (use_bloom == NULL) u8_free(bloom);
    return NULL;}

  return bloom;
}

KNO_EXPORT struct KNO_BLOOM *
kno_import_bloom_filter(struct KNO_BLOOM *use_bloom,
                       int entries,double error,
                       const unsigned char *bytes,
                       size_t n_bytes)
{
  struct KNO_BLOOM *bloom = NULL;
  if (entries < 1)
    return KNO_ERR(NULL,kno_TypeError,"kno_bloom_init","bad n_entries arg",KNO_INT(entries));
  else if (error <= 0)
    return KNO_ERR(NULL,kno_TypeError,"kno_bloom_init","bad allowed error value",
                   kno_make_double(error));
  else if (use_bloom == NULL)
    bloom = u8_alloc(struct KNO_BLOOM);
  else bloom = use_bloom;

  if (use_bloom) {
    KNO_SET_CONS_TYPE(bloom,kno_bloom_filter_type);}
  else KNO_INIT_CONS(bloom,kno_bloom_filter_type);

  bloom->entries = entries;
  bloom->error = error;

  double num = log(bloom->error);
  double denom = 0.480453013918201; // ln(2)^2
  bloom->bpe = -(num / denom);

  double dentries = (double)entries;
  bloom->bits = (int)(dentries * bloom->bpe);

  if (bloom->bits % 8) {
    bloom->bytes = (bloom->bits / 8) + 1;
  } else {
    bloom->bytes = bloom->bits / 8;
  }

  if (bloom->bytes != n_bytes) {
    u8_seterr("Bloom/BadLength","kno_import_bloom_filter",
              u8_mkstring("Inconsistent length %lld ≠ %lld (e=%f,n=%lld)",
                          (size_t)n_bytes,(size_t)bloom->bytes,
                          error,(long long)entries));
    if (use_bloom == NULL) u8_free(bloom);
    return NULL;}


  bloom->hashes = (int)ceil(0.693147180559945 * bloom->bpe);  // ln(2)

  if (bytes) 
    bloom->bf = u8_memdup(bloom->bytes,bytes);
  else bloom->bf = u8_zmalloc(bloom->bytes);
  if (bloom->bf == NULL) {
    u8_graberrno("kno_bloom_init:mallocbytes",NULL);
    if (use_bloom == NULL) u8_free(bloom);
    return NULL;}

  return bloom;
}

/* Adding and checking primitives */

int bloom_check_add_dtype(struct KNO_BLOOM *bloom,lispval key,
                          int add,int raw,int err)
{
  int rv=0;
  if (raw) {
    if (STRINGP(key))
      rv=bloom_check_add(bloom,CSTRING(key),STRLEN(key),add);
    else if (PACKETP(key))
      rv=bloom_check_add(bloom,KNO_PACKET_DATA(key),KNO_PACKET_LENGTH(key),add);
    else if (err)
      return KNO_ERR(-1,"Raw bloom arg wasn't a string or packet",
                     "bloom_check_add_dtype",NULL,key);
    if (rv<0)
      u8_seterr("BadBloomFilter","bloom_check_add_dtype",NULL);
    return rv;}
  else {
    KNO_DECL_OUTBUF(out,1024);
    size_t dtype_len = kno_write_dtype(&out,key);
    if ( RARELY (dtype_len<0) ) {
      if (err) {
        if (u8_current_exception) u8_pop_exception();
        return 0;}
      else return -1;}
    else if ( (out.buf_flags) & (KNO_BUFFER_ALLOC) ) {
      int rv=bloom_check_add(bloom,out.buffer,out.bufwrite-out.buffer,add);
      kno_close_outbuf(&out);
      if (rv<0) u8_seterr("BadBloomFilter","bloom_check_add_dtype",NULL);
      return rv;}
    else return bloom_check_add(bloom,out.buffer,out.bufwrite-out.buffer,add);}
}

KNO_EXPORT int kno_bloom_op(struct KNO_BLOOM * bloom, lispval key,int flags)
{
  int raw=flags&KNO_BLOOM_RAW, err=flags&KNO_BLOOM_ERR;
  int check=flags&KNO_BLOOM_CHECK, add=flags&KNO_BLOOM_ADD;
  if (PRECHOICEP(key)) {
    lispval simple=kno_make_simple_choice(key);
    int rv=kno_bloom_op(bloom,simple,flags);
    kno_decref(simple);
    return rv;}
  else if (CHOICEP(key)) {
    unsigned int count=0;
    DO_CHOICES(elt,key) {
      int rv=bloom_check_add_dtype(bloom, elt, add, raw, err);
      if (rv<0) {
        KNO_STOP_DO_CHOICES;
        return rv;}
      else if ( (rv) && (check) )
        return 1;
      else if ( (add) && (rv==0) ) count++;
      else if ( (!(add)) && (rv==1) ) count++;
      else {}}
    return count;}
  else return bloom_check_add_dtype(bloom, key, add, raw, err);
}

int kno_bloom_checkbuf(struct KNO_BLOOM * bloom, const void * buffer, int len)
{
  return bloom_check_add(bloom, buffer, len, 0);
}


int kno_bloom_addbuf(struct KNO_BLOOM * bloom, const void * buffer, int len)
{
  return bloom_check_add(bloom, buffer, len, 1);
}


int unparse_bloom(u8_output out,lispval x)
{
  struct KNO_BLOOM *filter = (struct KNO_BLOOM *)x;
  u8_printf(out,"#<BLOOM #!%llx %lld/%lld(%f)>",
            (U8_PTR2INT(filter)),
            filter->bloom_adds,
            filter->entries,
            filter->error);
  return 1;
}


void recycle_bloom(struct KNO_RAW_CONS *c)
{
  struct KNO_BLOOM * bloom = (struct KNO_BLOOM *)c;
  if (bloom->bf) free(bloom->bf);
  if (!(KNO_STATIC_CONSP(bloom))) {
    memset(bloom,0,sizeof(struct KNO_BLOOM));
    u8_free(bloom);}
}

static ssize_t dtype_bloom(struct KNO_OUTBUF *out,lispval x)
{
  struct KNO_BLOOM * bloom = (struct KNO_BLOOM *)x;
  kno_write_byte(out,dt_compound);
  kno_write_byte(out,dt_symbol);
  kno_write_4bytes(out,11);
  kno_write_bytes(out,"bloomfilter",11);
  lispval len_val = KNO_INT(bloom->bytes);
  lispval entries_val = KNO_INT(bloom->entries);
  struct KNO_FLONUM err; memset(&err,0,sizeof(KNO_FLONUM));
  KNO_INIT_STATIC_CONS(&err,kno_flonum_type);
  err.floval = bloom->error;
  unsigned char buf[100];
  struct KNO_OUTBUF header = { 0 }; KNO_INIT_BYTE_OUTBUF(&header,buf,100);
  kno_write_dtype(&header,len_val);
  kno_write_dtype(&header,entries_val);
  kno_write_dtype(&header,(lispval)&err);
  kno_decref(len_val); kno_decref(entries_val);
  size_t header_len = header.bufwrite-header.buffer;
  size_t packet_len = header_len + bloom->bytes;
  kno_write_byte(out,dt_packet);
  kno_write_4bytes(out,packet_len);
  kno_write_bytes(out,header.buffer,header_len);
  kno_write_bytes(out,bloom->bf,bloom->bytes);
  return 1 + 1 + 4 + 11 + 1 + 4 + header_len + bloom->bytes;
}

static lispval restore_bloom(lispval tag,lispval x,kno_typeinfo e)
{
  if (KNO_PACKETP(x)) {
    struct KNO_INBUF in = { 0 };
    KNO_INIT_INBUF(&in,KNO_PACKET_DATA(x),KNO_PACKET_LENGTH(x),0);
    lispval len_val = kno_read_dtype(&in);
    lispval entries_val = kno_read_dtype(&in);
    lispval err_val = kno_read_dtype(&in);
    if ( (FIXNUMP(len_val)) && (FIXNUMP(entries_val)) && (KNO_FLONUMP(err_val)) ) {
      long long len = kno_getint(len_val);
      long long entries = kno_getint(entries_val);
      double err = KNO_FLONUM(err_val);
      if ( (len > 0) && (entries > 0) && 
           ( len == (in.buflim-in.bufread) ) ) {
        struct KNO_BLOOM *filter =
          kno_import_bloom_filter(NULL,entries,err,in.bufread,len);
        if (filter == NULL)
          return KNO_ERROR_VALUE;
        else return (lispval) filter;}
      else return kno_err(kno_DTypeError,"bad bloom filter data",NULL,x);}
    else return kno_err(kno_DTypeError,"bad bloom filter data",NULL,x);}
  else return kno_err(kno_DTypeError,"bad bloom filter data",NULL,x);
}

void kno_init_bloom_c()
{
  kno_unparsers[kno_bloom_filter_type]=unparse_bloom;
  kno_recyclers[kno_bloom_filter_type]=recycle_bloom;
  kno_dtype_writers[kno_bloom_filter_type]=dtype_bloom;

  lispval bloom_tag = kno_intern("bloomfilter");

  struct KNO_TYPEINFO *e = kno_use_typeinfo(bloom_tag);
  e->type_restorefn = restore_bloom;

  u8_register_source_file(_FILEINFO);
}

/* Original headers and license information */

/* MurmurHash2.c is taken from
   http://sites.google.com/site/murmurhash/

   According to the above document:
   All code is released to the public domain. For business purposes,
   Murmurhash is under the MIT license.

   ---------------------------------------------------------------
   MurmurHash2, by Austin Appleby
*/

/*
 *  Copyright (c) 2012-2016, Jyri J. Virkki
 *  All rights reserved.
 *
 *  This file is under BSD license.
 */

/*
 * Refer to bloom.h for documentation on the public interfaces.
 */

