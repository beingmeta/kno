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
  FD_INIT_BYTE_INPUT(&in,out.buffer,(out.bufwrite-out.buffer));
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

#define free_val(x) fd_decref(x); x = FD_VOID

#define SLOTMAP(x) (fd_consptr(struct FD_SLOTMAP *,x,fd_slotmap_type))

int main(int argc,char **argv)
{
  FILE *f = fopen(argv[1],"rb");
  lispval smap;
  FD_DO_LIBINIT(fd_init_lisp_types);
  if (f) {
    smap = read_dtype_from_file(f); fclose(f);}
  else smap = fd_empty_slotmap();
  if (argc == 2) {
    lispval keys = fd_slotmap_keys(SLOTMAP(smap));
    FD_DO_CHOICES(key,keys) {
      lispval v = fd_slotmap_get(SLOTMAP(smap),key,FD_EMPTY_CHOICE);
      u8_fprintf(stdout,"%s=%s\n",key,v);
      fd_decref(v);}
    free_val(keys); free_val(smap);
    exit(0);}
  else if (argc == 3) {
    lispval slotid = fd_parse(argv[2]);
    lispval value = fd_slotmap_get(SLOTMAP(smap),slotid,FD_VOID);
    u8_fprintf(stdout,"%q=%q\n",slotid,value);
    free_val(value);}
  else if (argv[3][0] == '+') {
    lispval slotid = fd_parse(argv[2]);
    lispval value = fd_parse(argv[3]+1);
    fd_slotmap_add(SLOTMAP(smap),slotid,value);
    f = fopen(argv[1],"wb");
    write_dtype_to_file(smap,f);
    free_val(slotid); free_val(value);
    fclose(f);}
  else if (argv[3][0] == '-') {
    lispval slotid = fd_parse(argv[2]);
    lispval value = fd_parse(argv[3]+1);
    fd_slotmap_drop(SLOTMAP(smap),slotid,value);
    f = fopen(argv[1],"wb");
    write_dtype_to_file(smap,f);
    free_val(slotid); free_val(value);
    fclose(f);}
  else {
    lispval slotid = fd_parse(argv[2]);
    lispval value = fd_parse(argv[3]);
    fd_slotmap_store(SLOTMAP(smap),slotid,value);
    f = fopen(argv[1],"wb");
    write_dtype_to_file(smap,f);
    free_val(slotid); free_val(value);
    fclose(f);}
  u8_fprintf(stdout,"%q\n",smap);
  free_val(smap);
  exit(0);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
