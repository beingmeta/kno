/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/streams.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

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

#define SLOTMAP(x) (FD_GET_CONS(struct FD_SLOTMAP *,x,fd_slotmap_type))
#define HASHTABLE(x) (FD_GET_CONS(struct FD_HASHTABLE *,x,fd_slotmap_type))

static void report_on_hashtable(lispval ht)
{
  int n_slots, n_keys, n_buckets, n_collisions, max_bucket, n_vals, max_vals;
  fd_hashtable_stats(fd_consptr(struct FD_HASHTABLE *,ht,fd_hashtable_type),
		     &n_slots,&n_keys,&n_buckets,&n_collisions,&max_bucket,
		     &n_vals,&max_vals);
  u8_fprintf
    (stderr,"Table distributes %d keys over %d slots in %d buckets\n",
     n_keys,n_slots,n_buckets);
  u8_fprintf
    (stderr,"%d collisions, averaging %f keys per bucket (max=%d)\n",
     n_collisions,((1.0*n_keys)/n_buckets),max_bucket);
  u8_fprintf
    (stderr,
     "The keys refer to %d values all together (mean=%f,max=%d)\n",
     n_vals,((1.0*n_vals)/n_keys),max_vals);

}

int main(int argc,char **argv)
{
  lispval ht, item, key = FD_VOID; int i = 0;
  struct FD_STREAM *in, *out;
  struct FD_INBUF *inbuf;
  double span;
  FD_DO_LIBINIT(fd_init_lisp_types);
  span = get_elapsed(); /* Start the timer */
  ht = fd_make_hashtable(NULL,64);
  in = fd_open_file(argv[1],FD_FILE_READ);
  if (in == NULL) {
    u8_log(LOG_ERR,"No such file","Couldn't open file %s",argv[1]);
    exit(1);}
  else inbuf = fd_readbuf(in);
  fd_setbufsize(in,65536*2);
  item = fd_read_dtype(inbuf); i = 1;
  while (!(FD_EODP(item))) {
    if (i%100000 == 0) {
      double tmp = get_elapsed();
      u8_fprintf(stderr,"%d: %f %f %ld\n",i,tmp,(tmp-span),fd_getpos(in));
      span = tmp;}
    if (FD_PAIRP(item)) {
      fd_decref(key); key = fd_incref(item);}
    else fd_hashtable_add
	   (fd_consptr(struct FD_HASHTABLE *,ht,fd_hashtable_type),
	    key,item);
    fd_decref(item); item = fd_read_dtype(inbuf);
    i = i+1;}
  report_on_hashtable(ht);
  fd_close_stream(in,FD_STREAM_CLOSE_FULL);
  out = fd_open_file(argv[2],FD_FILE_CREATE);
  if (out) {
    struct FD_OUTBUF *outbuf = fd_writebuf(out);
    fd_write_dtype(outbuf,ht);
    fd_close_stream(out,FD_STREAM_CLOSE_FULL);}
  fd_decref(ht); ht = FD_VOID;
  exit(0);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
