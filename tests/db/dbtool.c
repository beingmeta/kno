/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id: dbtool.c,v 1.9 2006/01/26 14:44:33 haase Exp $";

#include "strings.h"
#include "stdlib.h"
#include "stdio.h"
#include "sys/time.h"
#include "time.h"
#include "libu8/u8.h"
#include "fdb/dtype.h"
#include "fdb/pools.h"
#include "fdb/indices.h"

fdtype fd_qparse(u8_string arg)
{
  if (strchr(arg,'=')) {
    u8_byte *copy=u8_strdup(arg), *ptr=strchr(copy,'=');
    fdtype slotid, value, results;
    *ptr='\0'; slotid=fd_parse(copy); value=fd_parse_arg(ptr+1);
    results=fd_bgfind(slotid,value,FD_VOID);
    u8_free(copy); fd_decref(slotid); fd_decref(value);
    return results;}
  else return fd_parse_arg(arg);
}

static void print_table(fdtype frames,fdtype slotids)
{
  FD_DO_CHOICES(frame,frames) {
    u8_fprintf(stdout,"%q\n",frame);
    {FD_DO_CHOICES(slotid,slotids) {
      fdtype value=fd_frame_get(frame,slotid);
      u8_string valstring=fd_dtype2string(value);
      if (strlen(valstring)>40) {
	u8_fprintf(stderr,"  %q\n",slotid);
	{FD_DO_CHOICES(v,value) u8_fprintf(stdout,"\t%q\n",v);}}
      else u8_fprintf(stderr,"  %q\t %s\n",slotid,valstring);
      fd_decref(value);
      u8_free(valstring);}}}
}

int main(int argc,char **argv)
{
  int fd_version=fd_init_dbfile();
  if (argc==2) {
    fdtype frames=fd_qparse(argv[1]);
    FD_DO_CHOICES(frame,frames)
      u8_fprintf(stdout," %q\n",frame);
    fd_decref(frames);}
  else if (argc==3) {
    fdtype frames=fd_qparse(argv[1]);
    fdtype slotids=fd_qparse(argv[2]);
    print_table(frames,slotids);
    fd_decref(frames); fd_decref(slotids);}
  else if (argc==4) {
    fdtype frames=fd_qparse(argv[1]);
    fdtype slotids=fd_qparse(argv[2]);
    enum FRAMEOP { drop, add, set}
    op=((argv[3][0]=='+') ? (add) :
	(argv[3][0]=='-') ? (drop) :
	(argv[3][0]=='=') ? (set) : (add));
    fdtype values=((strchr("+-=",argv[3][0])) ? (fd_qparse(argv[3]+1)) :
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
	  fdtype current=fd_frame_get(frame,slotid);
	  fdtype toadd=fd_difference(values,current);
	  fdtype todrop=fd_difference(current,values);
	  {FD_DO_CHOICES(a,toadd) fd_frame_add(frame,slotid,a);}
	  {FD_DO_CHOICES(a,todrop) fd_frame_drop(frame,slotid,a);}
	  fd_decref(toadd); fd_decref(todrop); fd_decref(current);}}}
    fd_decref(frames); fd_decref(slotids); fd_decref(values);}
  fd_commit_pools(); fd_swapout_pools();
  fd_commit_indices(); fd_swapout_indices();
}


/* The CVS log for this file
   $Log: dbtool.c,v $
   Revision 1.9  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.8  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.7  2005/05/29 22:38:47  haase
   Simplified db layer fd_use_pool and fd_use_index

   Revision 1.6  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.5  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
