/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/dtypestream.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

static int write_dtype_to_file(fdtype object,FILE *f)
{
  struct FD_BYTE_OUTPUT out; int retval;
  FD_INIT_BYTE_OUTPUT(&out,1024);
  retval=fd_write_dtype(&out,object);
  retval=fwrite(out.start,1,out.ptr-out.start,f);
  u8_free(out.start);
  return retval;
}

int main(int argc,char **argv)
{
  int lispv=fd_init_dtypelib();
  fdtype fix1=FD_INT(33994);
  fdtype dbl1=fd_init_flonum(NULL,3.445);
  fdtype dbl2=fd_init_flonum(u8_alloc(struct FD_FLONUM),-3.9994);
  fdtype string1=fd_make_string(NULL,3,"foo");
  fdtype string2=fd_init_string(u8_alloc(struct FD_STRING),3,u8_strdup("bar"));
  fdtype compound=
    fd_init_compound(NULL,
                     fd_probe_symbol("QUOTE",5),0,1,
                     fd_make_pair(FD_INT(5),FD_TRUE));
  fdtype vec=fd_make_nvector(3,fix1,dbl1,string1);
  fdtype lst=fd_make_list(4,vec,string2,dbl2,compound);
  u8_string as_string=fd_dtype2string(lst);
  fdtype tmp=fd_parse(as_string);
  FILE *f=fopen("testcapi.dtype","wb");
  if (lispv<0) {
    u8_log(LOG_WARN,"STARTUP","Couldn't initialize DTypes");
    exit(1);}

  if (write_dtype_to_file(lst,f)<0)
    fprintf(stderr,"write_dtype failed\n");
  if (FDTYPE_EQUAL(tmp,lst)) fd_decref(tmp);
  else fprintf(stderr,"Reparse didn't work\n");
  fclose(f);
  fprintf(stdout,"%s\n",as_string);
  fd_decref(lst);
  u8_free(as_string);
  return 0;
}
