/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
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

static lispval read_dtype_from_file(FILE *f)
{
  lispval object;
  struct FD_OUTBUF out = { 0 };
  struct FD_INBUF in = { 0 };
  char buf[1024]; int delta = 0;
  FD_INIT_BYTE_OUTPUT(&out,1024);
  while ((delta = fread(buf,1,1024,f))) {
    if (delta<0)
      if (errno == EAGAIN) {}
      else u8_raise("Read error","u8recode",NULL);
    else fd_write_bytes(&out,buf,delta);}
  FD_INIT_BYTE_INPUT(&in,out.buffer,out.bufwrite-out.buffer);
  object = fd_read_dtype(&in);
  fd_close_outbuf(&out);
  return object;
}

static int write_dtype_to_file(lispval object,FILE *f)
{
  struct FD_OUTBUF out = { 0 };
  int retval;
  FD_INIT_BYTE_OUTPUT(&out,1024);
  fd_write_dtype(&out,object);
  retval = fwrite(out.buffer,1,out.bufwrite-out.buffer,f);
  fd_close_outbuf(&out);
  return retval;
}

#define SLOTMAP(x) (fd_consptr(struct FD_SLOTMAP *,x,fd_slotmap_type))
#define HASHTABLE(x) (fd_consptr(struct FD_HASHTABLE *,x,fd_hashtable_type))

#define free_var(var) fd_decref(var); var = FD_VOID

int main(int argc,char **argv)
{
  FILE *f = fopen(argv[1],"rb");
  lispval ht, slotid, value;
  FD_DO_LIBINIT(fd_init_lisp_types);
  if (f) {
    ht = read_dtype_from_file(f); fclose(f);}
  else ht = fd_make_hashtable(NULL,64);
  if (argc == 2) {
    lispval keys = fd_hashtable_keys(HASHTABLE(ht));
    FD_DO_CHOICES(key,keys) {
      lispval v = fd_hashtable_get(HASHTABLE(ht),key,FD_EMPTY_CHOICE);
      u8_fprintf(stderr,"%q=%q\n",key,v);}
    exit(0);}
  slotid = fd_probe_symbol(argv[2],strlen(argv[2]));
  slotid = fd_parse(argv[2]);
  if (argc == 3) {
    value = fd_hashtable_get(HASHTABLE(ht),slotid,FD_VOID);
    u8_fprintf(stderr,"%q=%q\n",slotid,value);
    free_var(value);}
  else if (argv[3][0] == '+') {
    value = fd_parse(argv[3]+1);
    fd_hashtable_add(HASHTABLE(ht),slotid,value);
    f = fopen(argv[1],"wb");
    write_dtype_to_file(ht,f); fclose(f);}
  else if (argv[3][0] == '-') {
    value = fd_parse(argv[3]+1);
    fd_hashtable_drop(HASHTABLE(ht),slotid,value);
    f = fopen(argv[1],"wb");
    write_dtype_to_file(ht,f); fclose(f);}
  else {
    value = fd_parse(argv[3]);
    fd_hashtable_store(HASHTABLE(ht),slotid,value);
    f = fopen(argv[1],"wb");
    write_dtype_to_file(ht,f); fclose(f);}
  fd_decref(value);
  free_var(ht);
  exit(0);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
