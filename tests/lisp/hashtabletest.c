/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "kno/lisp.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

static lispval read_dtype_from_file(FILE *f)
{
  lispval object;
  struct KNO_OUTBUF out = { 0 };
  struct KNO_INBUF in = { 0 };
  char buf[1024]; int delta = 0;
  KNO_INIT_BYTE_OUTPUT(&out,1024);
  while ((delta = fread(buf,1,1024,f))) {
    if (delta<0)
      if (errno == EAGAIN) {}
      else u8_raise("Read error","u8recode",NULL);
    else kno_write_bytes(&out,buf,delta);}
  KNO_INIT_BYTE_INPUT(&in,out.buffer,out.bufwrite-out.buffer);
  object = kno_read_dtype(&in);
  kno_close_outbuf(&out);
  return object;
}

static int write_dtype_to_file(lispval object,FILE *f)
{
  struct KNO_OUTBUF out = { 0 };
  int retval;
  KNO_INIT_BYTE_OUTPUT(&out,1024);
  kno_write_dtype(&out,object);
  retval = fwrite(out.buffer,1,out.bufwrite-out.buffer,f);
  kno_close_outbuf(&out);
  return retval;
}

#define SLOTMAP(x) (kno_consptr(struct KNO_SLOTMAP *,x,kno_slotmap_type))
#define HASHTABLE(x) (kno_consptr(struct KNO_HASHTABLE *,x,kno_hashtable_type))

#define free_var(var) kno_decref(var); var = KNO_VOID

int main(int argc,char **argv)
{
  FILE *f = fopen(argv[1],"rb");
  lispval ht, slotid, value;
  KNO_DO_LIBINIT(kno_init_lisp_types);
  if (f) {
    ht = read_dtype_from_file(f); fclose(f);}
  else ht = kno_make_hashtable(NULL,64);
  if (argc == 2) {
    lispval keys = kno_hashtable_keys(HASHTABLE(ht));
    KNO_DO_CHOICES(key,keys) {
      lispval v = kno_hashtable_get(HASHTABLE(ht),key,KNO_EMPTY_CHOICE);
      u8_fprintf(stderr,"%q=%q\n",key,v);}
    exit(0);}
  slotid = kno_probe_symbol(argv[2],strlen(argv[2]));
  slotid = kno_parse(argv[2]);
  if (argc == 3) {
    value = kno_hashtable_get(HASHTABLE(ht),slotid,KNO_VOID);
    u8_fprintf(stderr,"%q=%q\n",slotid,value);
    free_var(value);}
  else if (argv[3][0] == '+') {
    value = kno_parse(argv[3]+1);
    kno_hashtable_add(HASHTABLE(ht),slotid,value);
    f = fopen(argv[1],"wb");
    write_dtype_to_file(ht,f); fclose(f);}
  else if (argv[3][0] == '-') {
    value = kno_parse(argv[3]+1);
    kno_hashtable_drop(HASHTABLE(ht),slotid,value);
    f = fopen(argv[1],"wb");
    write_dtype_to_file(ht,f); fclose(f);}
  else {
    value = kno_parse(argv[3]);
    kno_hashtable_store(HASHTABLE(ht),slotid,value);
    f = fopen(argv[1],"wb");
    write_dtype_to_file(ht,f); fclose(f);}
  kno_decref(value);
  free_var(ht);
  exit(0);
}

