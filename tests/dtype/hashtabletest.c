/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

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
  FD_INIT_BYTE_OUTPUT(&out,1024,NULL);
  fd_write_dtype(&out,object);
  fwrite(out.start,1,out.ptr-out.start,f);
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
  else ht=fd_make_hashtable(NULL,64,NULL);
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
    fd_hashtable_set(HASHTABLE(ht),slotid,value);
    f=fopen(argv[1],"wb");
    write_dtype_to_file(ht,f); fclose(f);}
  fd_decref(value);
  free_var(ht);
  exit(0);
}


/* The CVS log for this file
   $Log: hashtabletest.c,v $
   Revision 1.15  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.14  2005/12/28 23:03:29  haase
   Made choices be direct blocks of elements, including various fixes, simplifications, and more detailed documentation.

   Revision 1.13  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.12  2005/05/30 17:48:09  haase
   Fixed some header ordering problems

   Revision 1.11  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.10  2005/02/14 01:29:35  haase
   Increased code coverage of tests

   Revision 1.9  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
