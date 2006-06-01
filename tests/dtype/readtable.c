/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id: readtable.c,v 1.13 2006/01/26 14:44:33 haase Exp $";

#include "fdb/dtype.h"
#include "fdb/dtypestream.h"

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

static struct timeval start;
static started=0;

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

static fdtype read_dtype_from_file(FILE *f)
{
  fdtype object;
  struct FD_BYTE_OUTPUT out; struct FD_BYTE_INPUT in;
  char buf[1024]; int bytes_read=0, delta=0;
  FD_INIT_BYTE_OUTPUT(&out,1024,NULL);
  while (delta=fread(buf,1,1024,f)) {
    fd_write_bytes(&out,buf,delta);}
  FD_INIT_BYTE_INPUT(&in,out.start,out.ptr-out.start);
  object=fd_read_dtype(&in,NULL);
  u8_free(out.start);
  return object;
}

static void write_dtype_to_file(fdtype object,FILE *f)
{
  struct FD_BYTE_OUTPUT out;
  FD_INIT_BYTE_OUTPUT(&out,65536,NULL);
  fd_write_dtype(&out,object);
  fwrite(out.start,1,out.ptr-out.start,f);
  u8_free(out.start);
}


#define SLOTMAP(x) (FD_GET_CONS(x,fd_slotmap_type,struct FD_SLOTMAP *))
#define HASHTABLE(x) (FD_GET_CONS(x,fd_hashtable_type,struct FD_HASHTABLE *))

static report_on_hashtable(fdtype ht)
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
  struct FD_DTYPE_STREAM *in;
  fdtype ht, keys, watch_for=FD_VOID;
  FD_DO_LIBINIT(fd_init_dtypelib);
  in=fd_dtsopen(argv[1],FD_DTSTREAM_READ);
  ht=fd_dtsread_dtype(in);
  fd_dtsclose(in,FD_DTSCLOSE_FULL);
  report_on_hashtable(ht);
  fd_decref(ht); ht=FD_VOID;
  exit(0);
}


/* The CVS log for this file
   $Log: readtable.c,v $
   Revision 1.13  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.12  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.11  2005/05/30 17:48:09  haase
   Fixed some header ordering problems

   Revision 1.10  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.9  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
