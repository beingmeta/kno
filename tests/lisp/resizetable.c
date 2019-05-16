/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "kno/dtype.h"
#include "kno/streams.h"

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

#define SLOTMAP(x) (kno_constpr(struct KNO_SLOTMAP *,x,kno_slotmap_type))
#define HASHTABLE(x) (kno_consptr(struct KNO_HASHTABLE *,x,kno_hashtable_type))

static void report_on_hashtable(lispval ht)
{
  int n_slots, n_keys, n_buckets, n_collisions, max_bucket, n_vals, max_vals;
  kno_hashtable_stats(kno_consptr(struct KNO_HASHTABLE *,ht,kno_hashtable_type),
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
  (unsigned int *buf,struct KNO_HASH_BUCKET **slots,int n_slots)
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
  struct KNO_STREAM *in, *out;
  struct KNO_INBUF *inbuf;
  struct KNO_OUTBUF *outbuf;
  lispval ht, keys, watch_for = KNO_VOID;
  KNO_DO_LIBINIT(kno_init_lisp_types);
  n_tries = atol(argv[2]);
  in = kno_open_file(argv[1],KNO_FILE_READ);
  inbuf = kno_readbuf(in);
  ht = kno_read_dtype(inbuf);
  kno_close_stream(in,KNO_STREAM_CLOSE_FULL);
  report_on_hashtable(ht);
  n_keys = KNO_HASHTABLE_NKEYS(ht);
  n_slots = KNO_HASHTABLE_NBUCKETS(ht);
  tmpbuf = u8_alloc_n(n_keys*6,unsigned int);
  tmpbuf_size = n_keys*6;
  hashv = u8_alloc_n(n_keys,unsigned int);
  keys = kno_hashtable_keys(KNO_XHASHTABLE(ht));
  {
    KNO_DO_CHOICES(key,keys) {
      int hash = kno_hash_lisp(key);
      if (LISP_EQUAL(key,watch_for))
	fprintf(stderr,"Hashing key\n");
      if (hash==0)
	fprintf(stderr,"Warning: zero hash value\n");
      hashv[i++]=hash;}
    fprintf(stderr,"Initialized hash values for %d keys\n",
	    KNO_CHOICE_SIZE(keys));}
  {
    unsigned int n_buckets, max_bucket, n_collisions;
    kno_hash_quality(hashv,n_keys,n_slots,
		    tmpbuf,tmpbuf_size,
		    &n_buckets,&max_bucket,&n_collisions);
    fprintf(stderr,
	    "With %d slots, %f keys per bucket (max=%d), %d buckets, %d collisions\n",
	    n_slots,((double)(1.0*n_keys))/n_buckets,
	    max_bucket,n_buckets,n_collisions);
    check_consistency(tmpbuf,KNO_XHASHTABLE(ht)->ht_buckets,n_slots);
    best_size = n_slots; best_buckets = n_buckets;}
  {
    unsigned int n_buckets, max_bucket, n_collisions;
    kno_hash_quality(hashv,n_keys,n_keys,
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
    kno_hash_quality(hashv,n_keys,trial_slots,
		    tmpbuf,tmpbuf_size,
		    &n_buckets,&max_bucket,&n_collisions);
    fprintf(stderr,
	    "With %d slots, %f keys per bucket (max=%d), %d buckets, %d collisions\n",
	    trial_slots,((double)(1.0*n_keys))/n_buckets,
	    max_bucket,n_buckets,n_collisions);
    if (n_buckets<best_buckets) {
      best_size = trial_slots; best_buckets = n_buckets;}
    i++;}
  kno_resize_hashtable(KNO_XHASHTABLE(ht),best_size);
  report_on_hashtable(ht);
  if (argc>3) out = kno_open_file(argv[3],KNO_FILE_CREATE);
  else out = kno_open_file(argv[1],KNO_FILE_CREATE);
  outbuf = kno_writebuf(out);
  kno_write_dtype(outbuf,ht);
  kno_close_stream(out,KNO_STREAM_CLOSE_FULL);
  report_on_hashtable(ht);
  kno_decref(ht); ht = KNO_VOID;
  exit(0);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
