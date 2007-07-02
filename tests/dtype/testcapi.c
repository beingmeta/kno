/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/numbers.h"
#include "fdb/dtypestream.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

static int write_dtype_to_file(fdtype object,FILE *f)
{
  struct FD_BYTE_OUTPUT out; int retval;
  FD_INIT_BYTE_OUTPUT(&out,1024,NULL);
  retval=fd_write_dtype(&out,object);
  fwrite(out.start,1,out.ptr-out.start,f);
  u8_free(out.start);
  return retval;
}

int main(int argc,char **argv)
{
  int lispv=fd_init_dtypelib();
  fdtype fix1=FD_INT2DTYPE(33994);
  fdtype dbl1=fd_init_double(NULL,3.445);
  fdtype dbl2=fd_init_double(u8_malloc_type(struct FD_DOUBLE),-3.9994);
  fdtype string1=fd_init_string(NULL,3,u8_strdup("foo"));
  fdtype string2=fd_init_string
    (u8_malloc_type(struct FD_STRING),3,u8_strdup("bar"));
  fdtype compound=
    fd_init_compound(NULL,
		     fd_probe_symbol("QUOTE",5),
		     fd_make_pair(FD_INT2DTYPE(5),FD_TRUE));
  fdtype vec=fd_make_vector(3,fix1,dbl1,string1);
  fdtype lst=fd_make_list(4,vec,string2,dbl2,compound);
  u8_string as_string=fd_dtype2string(lst);
  fdtype tmp=fd_parse(as_string);
  FILE *f=fopen("testcapi.dtype","wb");
  if (write_dtype_to_file(lst,f)<0)
    fprintf(stderr,"write_dtype failed\n");
  if (FDTYPE_EQUAL(tmp,lst)) fd_decref(tmp);
  else fprintf(stderr,"Reparse didn't work\n");
  fclose(f);
  fprintf(stdout,"%s\n",as_string);
  fd_decref(lst);
  u8_free(as_string); 
  return 0;
}


/* The CVS log for this file
   $Log: testcapi.c,v $
   Revision 1.7  2006/01/31 13:47:24  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.6  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.5  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.4  2005/05/30 17:48:09  haase
   Fixed some header ordering problems

   Revision 1.3  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.2  2005/02/15 13:34:32  haase
   Updated fd_parser to use input streams rather than just strings

   Revision 1.1  2005/02/14 02:11:36  haase
   Added C API test program

*/
