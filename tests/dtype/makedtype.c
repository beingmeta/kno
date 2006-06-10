/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>

static void write_dtype_to_file(fdtype object,FILE *f)
{
  struct FD_BYTE_OUTPUT out;
  FD_INIT_BYTE_OUTPUT(&out,1024,NULL);
  fd_write_dtype(&out,object);
  fwrite(out.start,1,out.ptr-out.start,f);
  u8_free(out.start);
}

char *read_text_file(char *filename)
{
  FILE *f=fopen(filename,"r");
  char *buf=u8_malloc(1024), *ptr=buf, *buflim=buf+1024;
  int c=getc(f);
  while (c>=0) {
    if (ptr>=buflim) {
      unsigned int off=ptr-buf, old_size=(buflim-buf);
      buf=u8_realloc(buf,old_size*2);
      ptr=buf+off; buflim=buf+old_size*2;}
    *ptr++=c; c=getc(f);}
  if (ptr>=buflim) {
    unsigned int off=ptr-buf, old_size=(buflim-buf);
    buf=u8_realloc(buf,old_size*2);
    ptr=buf+off; buflim=buf+old_size*2;}
  *ptr='\0';
  fclose(f);
  return buf;
}

int main(int argc,char **argv)
{
  fdtype object;
  FILE *f=fopen(argv[1],"wb");
  FD_DO_LIBINIT(fd_init_dtypelib);
  if ((argv[2][0]=='-') && (argv[2][1]=='f')) {
    unsigned char *buf=read_text_file(argv[2]+2);
    object=fd_parse(buf);
    u8_free(buf);}
  else object=fd_parse(argv[2]);
  write_dtype_to_file(object,f);
  u8_fprintf(stdout,"Dumped the %s %q\n",
	     fd_type_names[FD_PTR_TYPE(object)],object);
  fd_decref(object); object=FD_VOID;
  fclose(f);
  exit(0);
}


/* The CVS log for this file
   $Log: makedtype.c,v $
   Revision 1.16  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.15  2005/12/21 19:03:46  haase
   Null terminate the filestring when reading text representations

   Revision 1.14  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.13  2005/05/30 17:48:09  haase
   Fixed some header ordering problems

   Revision 1.12  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.11  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
