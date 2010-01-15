/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2010 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"

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
  FD_INIT_BYTE_OUTPUT(&out,1024);
  fd_write_dtype(&out,object);
  retval=fwrite(out.start,1,out.ptr-out.start,f);
  u8_free(out.start);
}


#define SLOTMAP(x) (FD_GET_CONS(x,fd_slotmap_type,struct FD_SLOTMAP *))
#define HASHTABLE(x) (FD_GET_CONS(x,fd_hashtable_type,struct FD_HASHTABLE *))

#define free_var(var) fd_decref(var); var=FD_VOID

int main(int argc,char **argv)
{
  struct FD_BYTE_OUTPUT out; FILE *f=fopen(argv[1],"rb");
  fdtype ht, slotid, value;
  FD_DO_LIBINIT(fd_init_dtypelib);
  if (f) {
    ht=read_dtype_from_file(f); fclose(f);}
  else ht=fd_make_hashtable(NULL,64);
  if (argc == 2) {
    fdtype keys=fd_hashtable_keys(HASHTABLE(ht));
    FD_DO_CHOICES(key,keys) {
      fdtype v=fd_hashtable_get(HASHTABLE(ht),key,FD_EMPTY_CHOICE);
      u8_fprintf(stderr,"%q=%q\n",key,v);}
    exit(0);}
  slotid=fd_probe_symbol(argv[2],strlen(argv[2]));
  slotid=fd_parse(argv[2]);
  if (argc == 3) {
    value=fd_hashtable_get(HASHTABLE(ht),slotid,FD_VOID);
    u8_fprintf(stderr,"%q=%q\n",slotid,value);
    free_var(value);}
  else if (argv[3][0] == '+') {
    value=fd_parse(argv[3]+1);
    fd_hashtable_add(HASHTABLE(ht),slotid,value);
    f=fopen(argv[1],"wb");
    write_dtype_to_file(ht,f); fclose(f);}
  else if (argv[3][0] == '-') {
    value=fd_parse(argv[3]+1);
    fd_hashtable_drop(HASHTABLE(ht),slotid,value);
    f=fopen(argv[1],"wb");
    write_dtype_to_file(ht,f); fclose(f);}
  else {
    value=fd_parse(argv[3]);
    fd_hashtable_store(HASHTABLE(ht),slotid,value);
    f=fopen(argv[1],"wb");
    write_dtype_to_file(ht,f); fclose(f);}
  fd_decref(value);
  free_var(ht);
  exit(0);
}
