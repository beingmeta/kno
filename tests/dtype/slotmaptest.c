/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"

#include <libu8/u8.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

static fdtype read_dtype_from_file(FILE *f)
{
  fdtype object;
  struct FD_BYTE_OUTPUT out; struct FD_BYTE_INPUT in;
  char buf[1024]; int bytes_read=0, delta=0;
  FD_INIT_BYTE_OUTPUT(&out,1024,NULL);
  while (delta=fread(buf,1,1024,f)) {
    if (delta<0)
      if (errno==EAGAIN) {}
      else u8_raise("Read error","u8recode",NULL);
    else fd_write_bytes(&out,buf,delta);}
  FD_INIT_BYTE_INPUT(&in,out.start,(out.ptr-out.start));
  object=fd_read_dtype(&in,NULL);
  u8_free(out.start);
  return object;
}

static void write_dtype_to_file(fdtype object,FILE *f)
{
  struct FD_BYTE_OUTPUT out;
  FD_INIT_BYTE_OUTPUT(&out,1024,NULL);
  fd_write_dtype(&out,object);
  fwrite(out.start,1,out.ptr-out.start,f);
  u8_free(out.start);
}

#define free_val(x) fd_decref(x); x=FD_VOID

#define SLOTMAP(x) (FD_GET_CONS(x,fd_slotmap_type,struct FD_SLOTMAP *))

int main(int argc,char **argv)
{
  struct FD_BYTE_OUTPUT out; FILE *f=fopen(argv[1],"rb");
  fdtype smap;
  FD_DO_LIBINIT(fd_init_dtypelib);
  if (f) {
    smap=read_dtype_from_file(f); fclose(f);}
  else smap=fd_init_slotmap(NULL,0,NULL,NULL);
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
    fd_slotmap_set(SLOTMAP(smap),slotid,value);
    f=fopen(argv[1],"wb");
    write_dtype_to_file(smap,f);
    free_val(slotid); free_val(value);
    fclose(f);}
  u8_fprintf(stdout,"%q\n",smap);
  free_val(smap);
  exit(0);
}


/* The CVS log for this file
   $Log: slotmaptest.c,v $
   Revision 1.15  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.14  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.13  2005/05/30 17:48:09  haase
   Fixed some header ordering problems

   Revision 1.12  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.11  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
