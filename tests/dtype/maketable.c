/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2012 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "framerd/dtype.h"
#include "framerd/dtypestream.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

static struct timeval start;
static int started=0;

double get_elapsed()
{
  struct timeval now;
  if (started == 0) {
    gettimeofday(&start,NULL);
    started=1;
    return 0;}
  else {
    gettimeofday(&now,NULL);
    return (now.tv_sec-start.tv_sec)+
      (now.tv_usec-start.tv_usec)*0.000001;}
}

#define SLOTMAP(x) (FD_GET_CONS(x,fd_slotmap_type,struct FD_SLOTMAP *))
#define HASHTABLE(x) (FD_GET_CONS(x,fd_hashtable_type,struct FD_HASHTABLE *))

static void report_on_hashtable(fdtype ht)
{
  int n_slots, n_keys, n_buckets, n_collisions, max_bucket, n_vals, max_vals;
  fd_hashtable_stats(FD_GET_CONS(ht,fd_hashtable_type,struct FD_HASHTABLE *),
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
  fdtype ht, item, key=FD_VOID; int i=0;
  struct FD_DTYPE_STREAM *in, *out;
  double span;
  FD_DO_LIBINIT(fd_init_dtypelib);
  span=get_elapsed(); /* Start the timer */
  ht=fd_make_hashtable(NULL,64);
  in=fd_dtsopen(argv[1],FD_DTSTREAM_READ);
  if (in==NULL) {
    u8_log(LOG_ERR,"No such file","Couldn't open file %s",argv[1]);
    exit(1);}
  fd_dtsbufsize(in,65536*2);
  item=fd_dtsread_dtype(in); i=1;
  while (!(FD_EODP(item))) {
    if (i%100000 == 0) {
      double tmp=get_elapsed();
      u8_fprintf(stderr,"%d: %f %f %ld\n",i,tmp,(tmp-span),fd_getpos(in));
      span=tmp;}
    if (FD_PAIRP(item)) {
      fd_decref(key); key=fd_incref(item);}
    else fd_hashtable_add
	   (FD_GET_CONS(ht,fd_hashtable_type,struct FD_HASHTABLE *),
	    key,item);
    fd_decref(item); item=fd_dtsread_dtype(in);
    i=i+1;}
  report_on_hashtable(ht);
  fd_dtsclose(in,FD_DTSCLOSE_FULL);
  out=fd_dtsopen(argv[2],FD_DTSTREAM_CREATE);
  fd_dtswrite_dtype(out,ht);
  fd_dtsclose(out,FD_DTSCLOSE_FULL);
  fd_decref(ht); ht=FD_VOID;
  exit(0);
}
