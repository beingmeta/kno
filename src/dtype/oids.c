/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/preoids.h"

/* For sprintf */
#include <stdio.h>

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
  fd_lock_mutex(&base_oid_lock);
  if (fd_n_base_oids >= 1024) {
    fd_unlock_mutex(&base_oid_lock);
    return -1;}
  else {
    boi=fd_n_base_oids;
    fd_base_oids[fd_n_base_oids++]=base;
    fd_unlock_mutex(&base_oid_lock);
    return boi;}
}

FD_EXPORT int fd_get_oid_base_index(FD_OID addr,int add)
{
  FD_OID base=addr; 
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
  int boi=0; 
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

fdtype fd_preoids=FD_EMPTY_CHOICE;

static void init_oids()
{
  int i=0; while (i<N_OID_INITS) {
    FD_OID base=FD_MAKE_OID(_fd_oid_inits[i].hi,_fd_oid_inits[i].lo);
    unsigned int cap=_fd_oid_inits[i].cap;
    int j=0, lim=1+(cap/(FD_OID_BUCKET_SIZE));
    while (j<lim) {
      fdtype oid=fd_make_oid(base);
      FD_ADD_TO_CHOICE(fd_preoids,oid);
      base=FD_OID_PLUS(base,FD_OID_BUCKET_SIZE);
      j++;}
    i++;}
}

void fd_init_oids_c()
{
  fd_register_source_file(versionid);

  fd_type_names[fd_oid_type]="OID";

  _fd_oid_info=_simple_oid_info;

#if FD_THREADS_ENABLED
  fd_init_mutex(&(base_oid_lock));
#endif

  init_oids();
}
