/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

static fdtype read_dtype_from_file(FILE *f)
{
  fdtype object;
  struct FD_BYTE_OUTPUT out; struct FD_BYTE_INPUT in;
  char buf[1024]; int delta=0;
  FD_INIT_BYTE_OUTPUT(&out,1024);
  while ((delta=fread(buf,1,1024,f))) {
    if (delta<0)
      if (errno==EAGAIN) {}
      else u8_raise("Read error","u8recode",NULL);
    else fd_write_bytes(&out,buf,delta);}
  FD_INIT_BYTE_INPUT(&in,out.start,(out.ptr-out.start));
  object=fd_read_dtype(&in);
  u8_free(out.start);
  return object;
}

static int write_dtype_to_file(fdtype object,FILE *f)
{
  struct FD_BYTE_OUTPUT out; int retval;
  FD_INIT_BYTE_OUTPUT(&out,1024);
  fd_write_dtype(&out,object);
  retval=fwrite(out.start,1,out.ptr-out.start,f);
  u8_free(out.start);
  return retval;
}

#define free_val(x) fd_decref(x); x=FD_VOID

#define SLOTMAP(x) (FD_GET_CONS(x,fd_slotmap_type,struct FD_SLOTMAP *))

int main(int argc,char **argv)
{
  FILE *f=fopen(argv[1],"rb");
  fdtype smap;
  FD_DO_LIBINIT(fd_init_dtypelib);
  if (f) {
    smap=read_dtype_from_file(f); fclose(f);}
  else smap=fd_empty_slotmap();
  if (argc == 2) {
    fdtype keys=fd_slotmap_keys(SLOTMAP(smap));
    FD_DO_CHOICES(key,keys) {
      fdtype v=fd_slotmap_get(SLOTMAP(smap),key,FD_EMPTY_CHOICE);
      u8_fprintf(stdout,"%s=%s\n",key,v);
      fd_decref(v);}
    free_val(keys); free_val(smap);
    exit(0);}
  else if (argc == 3) {
    fdtype slotid=fd_parse(argv[2]);
    fdtype value=fd_slotmap_get(SLOTMAP(smap),slotid,FD_VOID);
    u8_fprintf(stdout,"%q=%q\n",slotid,value);
    free_val(value);}
  else if (argv[3][0] == '+') {
    fdtype slotid=fd_parse(argv[2]);
    fdtype value=fd_parse(argv[3]+1);
    fd_slotmap_add(SLOTMAP(smap),slotid,value);
    f=fopen(argv[1],"wb");
    write_dtype_to_file(smap,f);
    free_val(slotid); free_val(value);
    fclose(f);}
  else if (argv[3][0] == '-') {
    fdtype slotid=fd_parse(argv[2]);
    fdtype value=fd_parse(argv[3]+1);
    fd_slotmap_drop(SLOTMAP(smap),slotid,value);
    f=fopen(argv[1],"wb");
    write_dtype_to_file(smap,f);
    free_val(slotid); free_val(value);
    fclose(f);}
  else {
    fdtype slotid=fd_parse(argv[2]);
    fdtype value=fd_parse(argv[3]);
    fd_slotmap_store(SLOTMAP(smap),slotid,value);
    f=fopen(argv[1],"wb");
    write_dtype_to_file(smap,f);
    free_val(slotid); free_val(value);
    fclose(f);}
  u8_fprintf(stdout,"%q\n",smap);
  free_val(smap);
  exit(0);
}
