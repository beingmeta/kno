/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>

static int write_dtype_to_file(fdtype object,FILE *f)
{
  struct FD_BYTE_OUTBUF out; int n;
  FD_INIT_BYTE_OUTBUF(&out,1024);
  fd_write_dtype(&out,object);
  n=fwrite(out.bufbase,1,out.bufpoint-out.bufbase,f);
  u8_free(out.bufbase);
  return n;
}

char *read_text_file(char *filename)
{
  FILE *f=fopen(filename,"r");
  char *buf=u8_malloc(1024), *ptr=buf, *buflim=buf+1024;
  int c=getc(f);
  while (c>=0) {
    if (ptr>=buflim) {
      unsigned int off=ptr-buf, old_size=(buflim-buf);
      buf=u8_realloc(buf,old_size*2);
      ptr=buf+off; buflim=buf+old_size*2;}
    *ptr++=c; c=getc(f);}
  if (ptr>=buflim) {
    unsigned int off=ptr-buf, old_size=(buflim-buf);
    buf=u8_realloc(buf,old_size*2);
    ptr=buf+off; buflim=buf+old_size*2;}
  *ptr='\0';
  fclose(f);
  return buf;
}

int main(int argc,char **argv)
{
  fdtype object;
  FILE *f=fopen(argv[1],"wb");
  FD_DO_LIBINIT(fd_init_dtypelib);
  if ((argv[2][0]=='-') && (argv[2][1]=='f')) {
    unsigned char *buf=read_text_file(argv[2]+2);
    object=fd_parse(buf);
    u8_free(buf);}
  else object=fd_parse(argv[2]);
  write_dtype_to_file(object,f);
  u8_fprintf(stdout,"Dumped the %s %q\n",
             fd_type_names[FD_PTR_TYPE(object)],object);
  fd_decref(object); object=FD_VOID;
  fclose(f);
  exit(0);
}
