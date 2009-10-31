/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2009 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/dtypestream.h"

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

static fdtype read_dtype_from_file(FILE *f)
{
  fdtype object;
  struct FD_BYTE_OUTPUT out; struct FD_BYTE_INPUT in;
  char buf[1024]; int bytes_read=0, delta=0;
  FD_INIT_BYTE_OUTPUT(&out,1024);
  while (delta=fread(buf,1,1024,f)) {
    if (delta<0)
      if (errno==EAGAIN) {}
      else u8_raise("Read error","u8recode",NULL);
    else fd_write_bytes(&out,buf,delta);}
  FD_INIT_BYTE_INPUT(&in,out.start,out.ptr-out.start);
  object=fd_read_dtype(&in);
  u8_free(out.start);
  return object;
}

static void write_dtype_to_file(fdtype object,FILE *f)
{
  struct FD_BYTE_OUTPUT out; int retval;
  FD_INIT_BYTE_OUTPUT(&out,65536);
  fd_write_dtype(&out,object);
  retval=fwrite(out.start,1,out.ptr-out.start,f);
  u8_free(out.start);
}


#define SLOTMAP(x) (FD_GET_CONS(x,fd_slotmap_type,struct FD_SLOTMAP *))
#define HASHTABLE(x) (FD_GET_CONS(x,fd_hashtable_type,struct FD_HASHTABLE *))

static void report_on_hashtable(fdtype ht)
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

static void check_consistency
  (unsigned int *buf,struct FD_HASHENTRY **slots,int n_slots)
{
  int i=0; while (i < n_slots) 
    if (((buf[i]==0) && (slots[i] == NULL)) ||
	((buf[i]) && (slots[i]) &&
	 (slots[i]->n_keyvals==buf[i])) )
      i++;
    else {
      int real_values=((slots[i]==NULL) ? 0 : (slots[i]->n_keyvals));
      fprintf(stderr,"Trouble in slot %d, %ud differs from %ud\n",
	      i,buf[i],real_values);
      i++;}
}

static unsigned int read_size_from_stdin()
{
  unsigned int size; int retval;
  retval=fscanf(stdin,"%ud\n",&size);
  return size;
}

int main(int argc,char **argv)
{
  int i=0, n_tries;
  unsigned int n_slots, n_keys, *hashv;
  unsigned int *tmpbuf, tmpbuf_size;
  int best_size, best_buckets;
  struct FD_DTYPE_STREAM *in, *out;
  fdtype ht, keys, watch_for=FD_VOID;
  FD_DO_LIBINIT(fd_init_dtypelib);
  n_tries=atol(argv[2]);
  in=fd_dtsopen(argv[1],FD_DTSTREAM_READ);
  ht=fd_dtsread_dtype(in);
  fd_dtsclose(in,FD_DTSCLOSE_FULL);
  report_on_hashtable(ht);
  n_keys=FD_HASHTABLE_SIZE(ht);
  n_slots=FD_HASHTABLE_SLOTS(ht);
  tmpbuf=u8_alloc_n(n_keys*6,unsigned int);
  tmpbuf_size=n_keys*6;
  hashv=u8_alloc_n(n_keys,unsigned int);
  keys=fd_hashtable_keys(FD_XHASHTABLE(ht));
  {
    FD_DO_CHOICES(key,keys) {
      int hash=fd_hash_lisp(key);
      if (FDTYPE_EQUAL(key,watch_for))
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
    check_consistency(tmpbuf,FD_XHASHTABLE(ht)->slots,n_slots);
    best_size=n_slots; best_buckets=n_buckets;}
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
      best_size=n_keys; best_buckets=n_buckets;}}
  i=0; while (i < n_tries) {
    unsigned int trial_slots=read_size_from_stdin();
    unsigned int n_buckets, max_bucket, n_collisions;
    fd_hash_quality(hashv,n_keys,trial_slots,
		    tmpbuf,tmpbuf_size,
		    &n_buckets,&max_bucket,&n_collisions);
    fprintf(stderr,
	    "With %d slots, %f keys per bucket (max=%d), %d buckets, %d collisions\n",
	    trial_slots,((double)(1.0*n_keys))/n_buckets,
	    max_bucket,n_buckets,n_collisions);
    if (n_buckets<best_buckets) {
      best_size=trial_slots; best_buckets=n_buckets;}
    i++;}
  fd_resize_hashtable(FD_XHASHTABLE(ht),best_size);
  report_on_hashtable(ht);
  if (argc>3) out=fd_dtsopen(argv[3],FD_DTSTREAM_CREATE);
  else out=fd_dtsopen(argv[1],FD_DTSTREAM_CREATE);
  fd_dtswrite_dtype(out,ht);
  fd_dtsclose(out,FD_DTSCLOSE_FULL);
  report_on_hashtable(ht);
  fd_decref(ht); ht=FD_VOID;
  exit(0);
}
