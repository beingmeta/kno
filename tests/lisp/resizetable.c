/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/streams.h"

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

static struct timeval start;
static int started = 0;

double get_elapsed()
{
  struct timeval now;
  if (started == 0) {
    gettimeofday(&start,NULL);
    started = 1;
    return 0;}
  else {
    gettimeofday(&now,NULL);
    return (now.tv_sec-start.tv_sec)+
      (now.tv_usec-start.tv_usec)*0.000001;}
}

#define SLOTMAP(x) (fd_constpr(struct FD_SLOTMAP *,x,fd_slotmap_type))
#define HASHTABLE(x) (fd_consptr(struct FD_HASHTABLE *,x,fd_hashtable_type))

static void report_on_hashtable(lispval ht)
{
  int n_slots, n_keys, n_buckets, n_collisions, max_bucket, n_vals, max_vals;
  fd_hashtable_stats(fd_consptr(struct FD_HASHTABLE *,ht,fd_hashtable_type),
		     &n_slots,&n_keys,&n_buckets,&n_collisions,&max_bucket,
		     &n_vals,&max_vals);
  fprintf(stderr,"Table distributes %d keys over %d slots in %d buckets\n",
	  n_keys,n_slots,n_buckets);
  fprintf(stderr,"%d collisions, averaging %f keys per bucket (max=%d)\n",
	  n_collisions,((1.0*n_keys)/n_buckets),max_bucket);
  fprintf(stderr,
	  "The keys refer to %d values all together (mean=%f,max=%d)\n",
	  n_vals,((1.0*n_vals)/n_keys),max_vals);
}

static void check_consistency
  (unsigned int *buf,struct FD_HASH_BUCKET **slots,int n_slots)
{
  int i = 0; while (i < n_slots)
    if (((buf[i]==0) && (slots[i] == NULL)) ||
	((buf[i]) && (slots[i]) &&
	 (slots[i]->bucket_len == buf[i])) )
      i++;
    else {
      int real_values = ((slots[i]==NULL) ? 0 : (slots[i]->bucket_len));
      fprintf(stderr,"Trouble in slot %d, %ud differs from %ud\n",
	      i,buf[i],real_values);
      i++;}
}

static unsigned int read_size_from_stdin()
{
  unsigned int size; int retval;
  retval = fscanf(stdin,"%ud\n",&size);
  if (retval<0) return retval;
  else return size;
}

int main(int argc,char **argv)
{
  int i = 0, n_tries;
  unsigned int n_slots, n_keys, *hashv;
  unsigned int *tmpbuf, tmpbuf_size;
  int best_size, best_buckets;
  struct FD_STREAM *in, *out;
  struct FD_INBUF *inbuf;
  struct FD_OUTBUF *outbuf;
  lispval ht, keys, watch_for = FD_VOID;
  FD_DO_LIBINIT(fd_init_lisp_types);
  n_tries = atol(argv[2]);
  in = fd_open_file(argv[1],FD_FILE_READ);
  inbuf = fd_readbuf(in);
  ht = fd_read_dtype(inbuf);
  fd_close_stream(in,FD_STREAM_CLOSE_FULL);
  report_on_hashtable(ht);
  n_keys = FD_HASHTABLE_NKEYS(ht);
  n_slots = FD_HASHTABLE_NBUCKETS(ht);
  tmpbuf = u8_alloc_n(n_keys*6,unsigned int);
  tmpbuf_size = n_keys*6;
  hashv = u8_alloc_n(n_keys,unsigned int);
  keys = fd_hashtable_keys(FD_XHASHTABLE(ht));
  {
    FD_DO_CHOICES(key,keys) {
      int hash = fd_hash_lisp(key);
      if (LISP_EQUAL(key,watch_for))
	fprintf(stderr,"Hashing key\n");
      if (hash==0)
	fprintf(stderr,"Warning: zero hash value\n");
      hashv[i++]=hash;}
    fprintf(stderr,"Initialized hash values for %d keys\n",
	    FD_CHOICE_SIZE(keys));}
  {
    unsigned int n_buckets, max_bucket, n_collisions;
    fd_hash_quality(hashv,n_keys,n_slots,
		    tmpbuf,tmpbuf_size,
		    &n_buckets,&max_bucket,&n_collisions);
    fprintf(stderr,
	    "With %d slots, %f keys per bucket (max=%d), %d buckets, %d collisions\n",
	    n_slots,((double)(1.0*n_keys))/n_buckets,
	    max_bucket,n_buckets,n_collisions);
    check_consistency(tmpbuf,FD_XHASHTABLE(ht)->ht_buckets,n_slots);
    best_size = n_slots; best_buckets = n_buckets;}
  {
    unsigned int n_buckets, max_bucket, n_collisions;
    fd_hash_quality(hashv,n_keys,n_keys,
		    tmpbuf,tmpbuf_size,
		    &n_buckets,&max_bucket,&n_collisions);
    fprintf(stderr,
	    "With %d slots, %f keys per bucket (max=%d), %d buckets, %d collisions\n",
	    n_keys,((double)(1.0*n_keys))/n_buckets,
	    max_bucket,n_buckets,n_collisions);
    if (n_buckets<best_buckets) {
      best_size = n_keys; best_buckets = n_buckets;}}
  i = 0; while (i < n_tries) {
    unsigned int trial_slots = read_size_from_stdin();
    unsigned int n_buckets, max_bucket, n_collisions;
    fd_hash_quality(hashv,n_keys,trial_slots,
		    tmpbuf,tmpbuf_size,
		    &n_buckets,&max_bucket,&n_collisions);
    fprintf(stderr,
	    "With %d slots, %f keys per bucket (max=%d), %d buckets, %d collisions\n",
	    trial_slots,((double)(1.0*n_keys))/n_buckets,
	    max_bucket,n_buckets,n_collisions);
    if (n_buckets<best_buckets) {
      best_size = trial_slots; best_buckets = n_buckets;}
    i++;}
  fd_resize_hashtable(FD_XHASHTABLE(ht),best_size);
  report_on_hashtable(ht);
  if (argc>3) out = fd_open_file(argv[3],FD_FILE_CREATE);
  else out = fd_open_file(argv[1],FD_FILE_CREATE);
  outbuf = fd_writebuf(out);
  fd_write_dtype(outbuf,ht);
  fd_close_stream(out,FD_STREAM_CLOSE_FULL);
  report_on_hashtable(ht);
  fd_decref(ht); ht = FD_VOID;
  exit(0);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
