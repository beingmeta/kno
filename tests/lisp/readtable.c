/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "kno/lisp.h"
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

#define SLOTMAP(x)   (kno_constpr(struct KNO_SLOTMAP *,x,kno_slotmap_type))
#define HASHTABLE(x) (kno_consptr(,struct KNO_HASHTABLE *,x,kno_hashtable_type))

static void report_on_hashtable(lispval ht)
{
  int n_slots, n_keys, n_buckets, n_collisions, max_bucket, n_vals, max_vals;
  kno_hashtable_stats(KNO_GET_CONS(ht,kno_hashtable_type,struct KNO_HASHTABLE *),
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

int main(int argc,char **argv)
{
  struct KNO_STREAM *in; lispval ht;
  struct KNO_INBUF *inbuf;
  KNO_DO_LIBINIT(kno_init_lisp_types);
  in = kno_open_file(argv[1],KNO_FILE_READ);
  inbuf = kno_readbuf(in);
  ht = kno_read_dtype(inbuf);
  kno_close_stream(in,KNO_STREAM_CLOSE_FULL);
  report_on_hashtable(ht);
  kno_decref(ht); ht = KNO_VOID;
  exit(0);
}

