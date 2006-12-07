/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"

static fd_exception OIDBaseOverflow;

static FD_OID _base_oids[1024];
FD_OID *fd_base_oids=_base_oids;
int fd_n_base_oids=0;
#if FD_THREADS_ENABLED
static u8_mutex base_oid_lock;
#endif

static int get_base_oid_index(FD_OID base)
{
  int i=0, len=fd_n_base_oids; 
  while (i < len)
    if (FD_OID_COMPARE(base,fd_base_oids[i]) == 0) 
      return i;
    else i++;
  return -1;
}

static int add_base_oid_index(FD_OID base)
{
  int boi=get_base_oid_index(base);
  if (boi>=0) return boi;
  fd_lock_mutex(&(base_oid_lock));
  if (fd_n_base_oids >= 1024) {
    fd_unlock_mutex(&(base_oid_lock));
    return -1;}
  else {
    boi=fd_n_base_oids;
    fd_base_oids[fd_n_base_oids++]=base;
    fd_unlock_mutex(&(base_oid_lock));
    return boi;}
}

FD_EXPORT int fd_get_oid_base_index(FD_OID addr,int add)
{
  FD_OID base=addr; 
  int boi=0, len=fd_n_base_oids;
  FD_SET_OID_LO(base,((FD_OID_LO(base))&0xFFF00000U));
  if (add) {
    int retval=add_base_oid_index(base);
    if (retval<0) fd_seterr1(OIDBaseOverflow);
    return retval;}
  else return get_base_oid_index(base);
}

FD_EXPORT fdtype fd_make_oid(FD_OID addr)
{
  FD_OID base=addr;
  int boi=0, len=fd_n_base_oids; 
  unsigned int offset=FD_OID_LO(addr)&0xFFFFFU;
  FD_SET_OID_LO(base,(FD_OID_LO(base)&0xFFF00000U));
  boi=add_base_oid_index(base);
  return FD_CONSTRUCT_OID(boi,offset);
}

fd_oid_info_fn _fd_oid_info;

/* This is just for use from the debugger, so we can allocate it
   statically. */
static char oid_info_buf[128];

static u8_string _simple_oid_info(fdtype oid)
{
  if (FD_OIDP(oid)) {
    FD_OID addr=FD_OID_ADDR(oid);
    unsigned int hi=FD_OID_HI(addr), lo=FD_OID_LO(addr);
    sprintf(oid_info_buf,"@%x/%x",hi,lo);
    return oid_info_buf;}
  else return "not an oid!";
}

void fd_init_oids_c()
{
  fd_register_source_file(versionid);

  fd_type_names[fd_oid_type]="OID";

  _fd_oid_info=_simple_oid_info;

#if FD_THREADS_ENABLED
  fd_init_mutex(&(base_oid_lock));
#endif
}


/* The CVS log for this file
   $Log: oids.c,v $
   Revision 1.11  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.10  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.9  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.8  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
