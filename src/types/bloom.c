/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1

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
    fd_seterr(fd_TypeError,"fd_bloom_init",
	      u8_strdup("bad n_entries arg"),
	      FD_INT(entries));
    return NULL;}
  else if (error <= 0) {
    fd_seterr(fd_TypeError,"fd_bloom_init",
	      u8_strdup("bad allowed error value"),
	      fd_make_double(error));
    return NULL;}
  else if (use_bloom == NULL)
    bloom = u8_zalloc(struct FD_BLOOM);
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

  bloom->bf = u8_mallocz(bloom->bytes);
  if (bloom->bf == NULL) {
    u8_graberrno("fd_bloom_init:mallocbytes",NULL);
    if (use_bloom == NULL) u8_free(bloom);
    return NULL;}

  return bloom;
}


int fd_bloom_check(struct FD_BLOOM * bloom, const void * buffer, int len)
{
  return bloom_check_add(bloom, buffer, len, 0);
}


int fd_bloom_add(struct FD_BLOOM * bloom, const void * buffer, int len)
{
  return bloom_check_add(bloom, buffer, len, 1);
}


int unparse_bloom(u8_output out,fdtype x)
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
  if (!(FD_STATIC_CONSP(bloom))) u8_free(bloom);
}


void fd_init_bloom_c()
{

  fd_unparsers[fd_bloom_filter_type]=unparse_bloom;
  fd_recyclers[fd_bloom_filter_type]=recycle_bloom;

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

