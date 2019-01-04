/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_BLOOM_H
#define FRAMERD_BLOOM_H 1
#ifndef FRAMERD_BLOOM_H_INFO
#define FRAMERD_BLOOM_H_INFO "include/framerd/bloom.h"
#endif

/*
 *  Copyright (c) 2012-2016, Jyri J. Virkki
 *  All rights reserved.
 *
 *  This file is under BSD license. See LICENSE file.
 */

/** ***************************************************************************
 * Structure to keep track of one bloom filter.  Caller needs to
 * allocate this and pass it to the functions below. First call for
 * every struct must be to bloom_init().
 *
 */
struct FD_BLOOM
{
  FD_CONS_HEADER;
  unsigned long long bloom_adds;
  // These fields are part of the public interface of this structure.
  // Client code may read these values if desired. Client code MUST NOT
  // modify any of these.
  int entries;
  double error;
  int bits;
  int bytes;
  int hashes;

  // Fields below are private to the implementation. These may go away or
  // change incompatibly at any moment. Client code MUST NOT access or rely
  // on these.
  double bpe;
  unsigned char * bf;
};

/** ***************************************************************************
 * Initialize the bloom filter for use.
 *
 * The filter is initialized with a bit field and number of hash functions
 * according to the computations from the wikipedia entry:
 *     http://en.wikipedia.org/wiki/Bloom_filter
 *
 * Optimal number of bits is:
 *     bits = (entries * ln(error)) / ln(2)^2
 *
 * Optimal number of hash functions is:
 *     hashes = bpe * ln(2)
 *
 * Parameters:
 * -----------
 *     bloom   - Pointer to an allocated struct bloom (see above).
 *     entries - The expected number of entries which will be inserted.
 *     error   - Probability of collision (as long as entries are not
 *               exceeded).
 *
 * Return:
 * -------
 *     bloom filter objects - on success
 *     NULL - on failure
 *
 */
struct FD_BLOOM *fd_init_bloom_filter(struct FD_BLOOM * bloom, int entries, double error);

/** ***************************************************************************
 * Initialize the bloom filter for use.
 *
 * The filter is initialized with a bit field and number of hash functions
 * according to the computations from the wikipedia entry:
 *     http://en.wikipedia.org/wiki/Bloom_filter
 *
 * Optimal number of bits is:
 *     bits = (entries * ln(error)) / ln(2)^2
 *
 * Optimal number of hash functions is:
 *     hashes = bpe * ln(2)
 *
 * Parameters:
 * -----------
 *     bloom   - Pointer to an allocated struct bloom (see above).
 *     entries - The expected number of entries which will be inserted.
 *     error   - Probability of collision (as long as entries are not
 *               exceeded).
 *     bytes   - bytes representing a bloom filter
 *     n_bytes - number of bytes in *data*
 *
 * Return:
 * -------
 *     bloom filter objects - on success
 *     NULL - on failure
 *
 * Errors:
 *  If n_bytes don't correspond to the filter specified by the parameters.
 */
FD_EXPORT
struct FD_BLOOM *
fd_import_bloom_filter(struct FD_BLOOM *use_bloom,
                       int entries,double error,
                       const unsigned char *bytes,
                       size_t n_bytes);


/** ***************************************************************************
 * Check if the given element is in the bloom filter. Remember this may
 * return false positive if a collision occured.
 *
 * Parameters:
 * -----------
 *     bloom  - Pointer to an allocated struct bloom (see above).
 *     buffer - Pointer to buffer containing element to check.
 *     len    - Size of 'buffer'.
 *
 * Return:
 * -------
 *     0 - element is not present
 *     1 - element is present (or false positive due to collision)
 *    -1 - bloom not initialized
 *
 */
int fd_bloom_checkbuf(struct FD_BLOOM * bloom, const void * buffer, int len);


/** ***************************************************************************
 * Add the given element to the bloom filter.
 * The return code indicates if the element (or a collision) was already in,
 * so for the common check+add use case, no need to call check separately.
 *
 * Parameters:
 * -----------
 *     bloom  - Pointer to an allocated struct bloom (see above).
 *     buffer - Pointer to buffer containing element to add.
 *     len    - Size of 'buffer'.
 *
 * Return:
 * -------
 *     0 - element was not present and was added
 *     1 - element (or a collision) had already been added previously
 *    -1 - bloom not initialized
 *
 */
int fd_bloom_addbuf(struct FD_BLOOM * bloom, const void * buffer, int len);

/* Operates on a bloom filter and a lisp value
   @param bloom a pointer to a bloom filter
   @param key a lisp value
   @param flags options

   @returns 1 (an element or a collision was already present), 0 (the
     element was not present and was added if requested, or -1 (an error
     was encountered)
*/
int fd_bloom_op(struct FD_BLOOM * bloom, lispval val, int flags);

/* Flags for fd_bloom_op */
#define FD_BLOOM_ADD 0x01
#define FD_BLOOM_RAW 0x02
#define FD_BLOOM_ERR 0x04
#define FD_BLOOM_CHECK 0x08

#endif /* FRAMERD_BLOOM_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
