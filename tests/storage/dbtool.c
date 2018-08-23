/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/drivers.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include "strings.h"
#include "stdlib.h"
#include "stdio.h"
#include "sys/time.h"
#include "time.h"

lispval fd_qparse(u8_string arg)
{
  if (strchr(arg,'=')) {
    u8_byte *copy = u8_strdup(arg), *ptr = strchr(copy,'=');
    lispval slotid, value, results;
    *ptr='\0'; slotid = fd_parse(copy); value = fd_parse_arg(ptr+1);
    results = fd_bgfind(slotid,value,FD_VOID);
    u8_free(copy); fd_decref(slotid); fd_decref(value);
    return results;}
  else return fd_parse_arg(arg);
}

static void print_table(lispval frames,lispval slotids)
{
  FD_DO_CHOICES(frame,frames) {
    u8_fprintf(stdout,"%q\n",frame);
    {FD_DO_CHOICES(slotid,slotids) {
      lispval value = fd_frame_get(frame,slotid);
      u8_string valstring = fd_lisp2string(value);
      if (strlen(valstring)>40) {
	u8_fprintf(stderr,"  %q\n",slotid);
	{FD_DO_CHOICES(v,value) u8_fprintf(stdout,"\t%q\n",v);}}
      else u8_fprintf(stderr,"  %q\t %s\n",slotid,valstring);
      fd_decref(value);
      u8_free(valstring);}}}
}

int main(int argc,char **argv)
{
  int fd_version = fd_init_drivers();
  if (fd_version<0) exit(1);
  fd_set_config("OIDDISPLAY",FD_INT(3));
  if (argc==2) {
    lispval frames = fd_qparse(argv[1]);
    FD_DO_CHOICES(frame,frames)
      u8_fprintf(stdout," %q\n",frame);
    fd_decref(frames);}
  else if (argc==3) {
    lispval frames = fd_qparse(argv[1]);
    lispval slotids = fd_qparse(argv[2]);
    print_table(frames,slotids);
    fd_decref(frames); fd_decref(slotids);}
  else if (argc==4) {
    lispval frames = fd_qparse(argv[1]);
    lispval slotids = fd_qparse(argv[2]);
    enum FRAMEOP { drop, add, set}
    op = ((argv[3][0]=='+') ? (add) :
	(argv[3][0]=='-') ? (drop) :
	(argv[3][0]=='=') ? (set) : (add));
    lispval values = ((strchr("+-=",argv[3][0])) ? (fd_qparse(argv[3]+1)) :
		   (fd_qparse(argv[3])));
    FD_DO_CHOICES(frame,frames) {
      FD_DO_CHOICES(slotid,slotids) {
	if ((op == add) || (op == drop)) {
	  FD_DO_CHOICES(value,values)
	    if (op == add)
	      fd_frame_add(frame,slotid,value);
	    else if (op == drop)
	      fd_frame_drop(frame,slotid,value);}
	else {
	  lispval current = fd_frame_get(frame,slotid);
	  lispval toadd = fd_difference(values,current);
	  lispval todrop = fd_difference(current,values);
	  {FD_DO_CHOICES(a,toadd) fd_frame_add(frame,slotid,a);}
	  {FD_DO_CHOICES(a,todrop) fd_frame_drop(frame,slotid,a);}
	  fd_decref(toadd); fd_decref(todrop); fd_decref(current);}}}
    fd_decref(frames); fd_decref(slotids); fd_decref(values);}
  fd_commit_pools();
  fd_commit_indexes();
#if 1
  fd_swapout_pools();
  fd_swapout_indexes();
  fd_clear_slotcaches();
  fd_clear_callcache(FD_VOID);
#endif
  return 0;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
