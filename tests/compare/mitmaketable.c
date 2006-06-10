#include <framerd/dtypes.h>
#include "strings.h"
#include "stdlib.h"
#include "stdio.h"
#include "sys/time.h"
#include "time.h"

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

int main(int argc,char **argv)
{
  FILE *f=fopen(argv[1],"rb");
  struct FD_HASHTABLE *ht;
  fd_lisp item, key=FD_VOID; int i=0;
  double span;
  fd_initialize_framerd();
  ht=fd_make_hashtable(64);
  span=get_elapsed(); /* Start the timer */
  item=fd_fread_dtype(f); i=1;
  while (!(FD_EOF_OBJECTP(item))) {
    if (i%100000 == 0) {
      double tmp=get_elapsed();
      fprintf(stderr,"%d: %f %f %ld\n",i,tmp,tmp-span,ftello(f));
      span=tmp;}
    if (FD_PAIRP(item)) {
      fd_decref(key); key=fd_incref(item);}
    else fd_hashtable_add(ht,key,item);
    fd_decref(item); item=fd_fread_dtype(f);
    i=i+1;}
  exit(0);
}



