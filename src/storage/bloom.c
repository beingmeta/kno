/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1
#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/hash.h"
#include "framerd/tables.h"
#include "framerd/numbers.h"
#include "framerd/bloom.h"

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


static int bloom_check_add(struct FD_BLOOM * bloom,
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

FD_EXPORT
struct FD_BLOOM *
fd_init_bloom_filter(struct FD_BLOOM *use_bloom,int entries,double error)
{
  struct FD_BLOOM *bloom = NULL;
  if (entries < 1) {
    fd_seterr(fd_TypeError,"fd_bloom_init","bad n_entries arg",FD_INT(entries));
    return NULL;}
  else if (error <= 0) {
    fd_seterr(fd_TypeError,"fd_bloom_init","bad allowed error value",
              fd_make_double(error));
    return NULL;}
  else if (use_bloom == NULL)
    bloom = u8_alloc(struct FD_BLOOM);
  else bloom = use_bloom;

  if (use_bloom) {
    FD_SET_CONS_TYPE(bloom,fd_bloom_filter_type);}
  else FD_INIT_CONS(bloom,fd_bloom_filter_type);

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
    u8_graberrno("fd_bloom_init:mallocbytes",NULL);
    if (use_bloom == NULL) u8_free(bloom);
    return NULL;}

  return bloom;
}

FD_EXPORT struct FD_BLOOM *
fd_import_bloom_filter(struct FD_BLOOM *use_bloom,
                       int entries,double error,
                       const unsigned char *bytes,
                       size_t n_bytes)
{
  struct FD_BLOOM *bloom = NULL;
  if (entries < 1) {
    fd_seterr(fd_TypeError,"fd_bloom_init","bad n_entries arg",FD_INT(entries));
    return NULL;}
  else if (error <= 0) {
    fd_seterr(fd_TypeError,"fd_bloom_init","bad allowed error value",
              fd_make_double(error));
    return NULL;}
  else if (use_bloom == NULL)
    bloom = u8_alloc(struct FD_BLOOM);
  else bloom = use_bloom;

  if (use_bloom) {
    FD_SET_CONS_TYPE(bloom,fd_bloom_filter_type);}
  else FD_INIT_CONS(bloom,fd_bloom_filter_type);

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
    u8_seterr("Bloom/BadLength","fd_import_bloom_filter",
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
    u8_graberrno("fd_bloom_init:mallocbytes",NULL);
    if (use_bloom == NULL) u8_free(bloom);
    return NULL;}

  return bloom;
}

/* Adding and checking primitives */

int bloom_check_add_dtype(struct FD_BLOOM *bloom,lispval key,
                          int add,int raw,int err)
{
  int rv=0;
  if (raw) {
    if (STRINGP(key))
      rv=bloom_check_add(bloom,CSTRING(key),STRLEN(key),add);
    else if (PACKETP(key))
      rv=bloom_check_add(bloom,FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),add);
    else if (err) {
      fd_seterr("Raw bloom arg wasn't a string or packet",
                "bloom_check_add_dtype",NULL,key);
      return -1;}
    if (rv<0)
      u8_seterr("BadBloomFilter","bloom_check_add_dtype",NULL);
    return rv;}
  else {
    FD_DECL_OUTBUF(out,1024);
    size_t dtype_len = fd_write_dtype(&out,key);
    if ( PRED_FALSE (dtype_len<0) ) {
      if (err) {
        if (u8_current_exception) u8_pop_exception();
        return 0;}
      else return -1;}
    else if ( (out.buf_flags) & (FD_BUFFER_ALLOC) ) {
      int rv=bloom_check_add(bloom,out.buffer,out.bufwrite-out.buffer,add);
      fd_close_outbuf(&out);
      if (rv<0) u8_seterr("BadBloomFilter","bloom_check_add_dtype",NULL);
      return rv;}
    else return bloom_check_add(bloom,out.buffer,out.bufwrite-out.buffer,add);}
}

FD_EXPORT int fd_bloom_op(struct FD_BLOOM * bloom, lispval key,int flags)
{
  int raw=flags&FD_BLOOM_RAW, err=flags&FD_BLOOM_ERR;
  int check=flags&FD_BLOOM_CHECK, add=flags&FD_BLOOM_ADD;
  if (PRECHOICEP(key)) {
    lispval simple=fd_make_simple_choice(key);
    int rv=fd_bloom_op(bloom,simple,flags);
    fd_decref(simple);
    return rv;}
  else if (CHOICEP(key)) {
    unsigned int count=0;
    DO_CHOICES(elt,key) {
      int rv=bloom_check_add_dtype(bloom, elt, add, raw, err);
      if (rv<0) {
        FD_STOP_DO_CHOICES;
        return rv;}
      else if ( (rv) && (check) )
        return 1;
      else if ( (add) && (rv==0) ) count++;
      else if ( (!(add)) && (rv==1) ) count++;
      else {}}
    return count;}
  else return bloom_check_add_dtype(bloom, key, add, raw, err);
}

int fd_bloom_checkbuf(struct FD_BLOOM * bloom, const void * buffer, int len)
{
  return bloom_check_add(bloom, buffer, len, 0);
}


int fd_bloom_addbuf(struct FD_BLOOM * bloom, const void * buffer, int len)
{
  return bloom_check_add(bloom, buffer, len, 1);
}


int unparse_bloom(u8_output out,lispval x)
{
  struct FD_BLOOM *filter = (struct FD_BLOOM *)x;
  u8_printf(out,"#<BLOOM #!%llx %lld/%lld(%f)>",
            (U8_PTR2INT(filter)),
            filter->bloom_adds,
            filter->entries,
            filter->error);
  return 1;
}


void recycle_bloom(struct FD_RAW_CONS *c)
{
  struct FD_BLOOM * bloom = (struct FD_BLOOM *)c;
  if (bloom->bf) free(bloom->bf);
  if (!(FD_STATIC_CONSP(bloom))) {
    memset(bloom,0,sizeof(struct FD_BLOOM));
    u8_free(bloom);}
}

static ssize_t dtype_bloom(struct FD_OUTBUF *out,lispval x)
{
  struct FD_BLOOM * bloom = (struct FD_BLOOM *)x;
  fd_write_byte(out,dt_compound);
  fd_write_byte(out,dt_symbol);
  fd_write_4bytes(out,11);
  fd_write_bytes(out,"bloomfilter",11);
  lispval len_val = FD_INT(bloom->bytes);
  lispval entries_val = FD_INT(bloom->entries);
  struct FD_FLONUM err; memset(&err,0,sizeof(FD_FLONUM));
  FD_INIT_STATIC_CONS(&err,fd_flonum_type);
  err.floval = bloom->error;
  unsigned char buf[100];
  struct FD_OUTBUF header = { 0 }; FD_INIT_BYTE_OUTBUF(&header,buf,100);
  fd_write_dtype(&header,len_val);
  fd_write_dtype(&header,entries_val);
  fd_write_dtype(&header,(lispval)&err);
  fd_decref(len_val); fd_decref(entries_val);
  size_t header_len = header.bufwrite-header.buffer;
  size_t packet_len = header_len + bloom->bytes;
  fd_write_byte(out,dt_packet);
  fd_write_4bytes(out,packet_len);
  fd_write_bytes(out,header.buffer,header_len);
  fd_write_bytes(out,bloom->bf,bloom->bytes);
  return 1 + 1 + 4 + 11 + 1 + 4 + header_len + bloom->bytes;
}

static lispval restore_bloom(lispval tag,lispval x,fd_compound_typeinfo e)
{
  if (FD_PACKETP(x)) {
    struct FD_INBUF in = { 0 };
    FD_INIT_INBUF(&in,FD_PACKET_DATA(x),FD_PACKET_LENGTH(x),0);
    lispval len_val = fd_read_dtype(&in);
    lispval entries_val = fd_read_dtype(&in);
    lispval err_val = fd_read_dtype(&in);
    if ( (FIXNUMP(len_val)) && (FIXNUMP(entries_val)) && (FD_FLONUMP(err_val)) ) {
      long long len = fd_getint(len_val);
      long long entries = fd_getint(entries_val);
      double err = FD_FLONUM(err_val);
      if ( (len > 0) && (entries > 0) && 
           ( len == (in.buflim-in.bufread) ) ) {
        struct FD_BLOOM *filter =
          fd_import_bloom_filter(NULL,entries,err,in.bufread,len);
        if (filter == NULL)
          return FD_ERROR_VALUE;
        else return (lispval) filter;}
      else return fd_err(fd_DTypeError,"bad bloom filter data",NULL,x);}
    else return fd_err(fd_DTypeError,"bad bloom filter data",NULL,x);}
  else return fd_err(fd_DTypeError,"bad bloom filter data",NULL,x);
}

void fd_init_bloom_c()
{
  fd_unparsers[fd_bloom_filter_type]=unparse_bloom;
  fd_recyclers[fd_bloom_filter_type]=recycle_bloom;
  fd_dtype_writers[fd_bloom_filter_type]=dtype_bloom;

  lispval bloom_tag = fd_intern("bloomfilter");

  struct FD_COMPOUND_TYPEINFO *e=fd_register_compound(bloom_tag,NULL,NULL);
  e->compound_restorefn = restore_bloom;

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

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
