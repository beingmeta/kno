/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/drivers.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include "strings.h"
#include "stdlib.h"
#include "stdio.h"
#include "sys/time.h"
#include "time.h"

lispval kno_qparse(u8_string arg)
{
  if (strchr(arg,'=')) {
    u8_byte *copy = u8_strdup(arg), *ptr = strchr(copy,'=');
    lispval slotid, value, results;
    *ptr='\0'; slotid = kno_parse(copy); value = kno_parse_arg(ptr+1);
    results = kno_bgfind(slotid,value,KNO_VOID);
    u8_free(copy); kno_decref(slotid); kno_decref(value);
    return results;}
  else return kno_parse_arg(arg);
}

static void print_table(lispval frames,lispval slotids)
{
  KNO_DO_CHOICES(frame,frames) {
    u8_fprintf(stdout,"%q\n",frame);
    {KNO_DO_CHOICES(slotid,slotids) {
      lispval value = kno_frame_get(frame,slotid);
      u8_string valstring = kno_lisp2string(value);
      if (strlen(valstring)>40) {
	u8_fprintf(stderr,"  %q\n",slotid);
	{KNO_DO_CHOICES(v,value) u8_fprintf(stdout,"\t%q\n",v);}}
      else u8_fprintf(stderr,"  %q\t %s\n",slotid,valstring);
      kno_decref(value);
      u8_free(valstring);}}}
}

int main(int argc,char **argv)
{
  int kno_version = kno_init_drivers();
  if (kno_version<0) exit(1);
  kno_set_config("OIDDISPLAY",KNO_INT(3));
  if (argc==2) {
    lispval frames = kno_qparse(argv[1]);
    KNO_DO_CHOICES(frame,frames)
      u8_fprintf(stdout," %q\n",frame);
    kno_decref(frames);}
  else if (argc==3) {
    lispval frames = kno_qparse(argv[1]);
    lispval slotids = kno_qparse(argv[2]);
    print_table(frames,slotids);
    kno_decref(frames); kno_decref(slotids);}
  else if (argc==4) {
    lispval frames = kno_qparse(argv[1]);
    lispval slotids = kno_qparse(argv[2]);
    enum FRAMEOP { drop, add, set}
    op = ((argv[3][0]=='+') ? (add) :
	(argv[3][0]=='-') ? (drop) :
	(argv[3][0]=='=') ? (set) : (add));
    lispval values = ((strchr("+-=",argv[3][0])) ? (kno_qparse(argv[3]+1)) :
		   (kno_qparse(argv[3])));
    KNO_DO_CHOICES(frame,frames) {
      KNO_DO_CHOICES(slotid,slotids) {
	if ((op == add) || (op == drop)) {
	  KNO_DO_CHOICES(value,values)
	    if (op == add)
	      kno_frame_add(frame,slotid,value);
	    else if (op == drop)
	      kno_frame_drop(frame,slotid,value);}
	else {
	  lispval current = kno_frame_get(frame,slotid);
	  lispval toadd = kno_difference(values,current);
	  lispval todrop = kno_difference(current,values);
	  {KNO_DO_CHOICES(a,toadd) kno_frame_add(frame,slotid,a);}
	  {KNO_DO_CHOICES(a,todrop) kno_frame_drop(frame,slotid,a);}
	  kno_decref(toadd); kno_decref(todrop); kno_decref(current);}}}
    kno_decref(frames); kno_decref(slotids); kno_decref(values);}
  kno_commit_pools();
  kno_commit_indexes();
#if 1
  kno_swapout_pools();
  kno_swapout_indexes();
  kno_clear_slotcaches();
  kno_clear_callcache(KNO_VOID);
#endif
  return 0;
}

