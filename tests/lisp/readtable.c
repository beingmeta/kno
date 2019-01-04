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

#define SLOTMAP(x)   (fd_constpr(struct FD_SLOTMAP *,x,fd_slotmap_type))
#define HASHTABLE(x) (fd_consptr(,struct FD_HASHTABLE *,x,fd_hashtable_type))

static void report_on_hashtable(lispval ht)
{
  int n_slots, n_keys, n_buckets, n_collisions, max_bucket, n_vals, max_vals;
  fd_hashtable_stats(FD_GET_CONS(ht,fd_hashtable_type,struct FD_HASHTABLE *),
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
  struct FD_STREAM *in; lispval ht;
  struct FD_INBUF *inbuf;
  FD_DO_LIBINIT(fd_init_lisp_types);
  in = fd_open_file(argv[1],FD_FILE_READ);
  inbuf = fd_readbuf(in);
  ht = fd_read_dtype(inbuf);
  fd_close_stream(in,FD_STREAM_CLOSE_FULL);
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
