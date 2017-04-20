/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/preoids.h"

/* For sprintf */
#include <ctype.h>

fd_exception fd_NotAnOID=_("Not an OID");

fdtype fd_zero_pool_values[4096]={FD_VOID};
fdtype *fd_zero_pool_buckets[FD_ZERO_POOL_MAX/4096]={fd_zero_pool_values,NULL};
unsigned int fd_zero_pool_load = 0;
static u8_mutex zero_pool_lock;

FD_OID fd_base_oids[FD_N_OID_BUCKETS];
int fd_n_base_oids = 0;
static u8_mutex base_oid_lock;
static fd_exception OIDBucketOverflow="Out of OID buckets";

static int get_base_oid_index(FD_OID base)
{
  int i = 0, len = fd_n_base_oids;
  while (i < len)
    if (FD_OID_COMPARE(base,fd_base_oids[i]) == 0)
      return i;
    else i++;
  return -1;
}

static int add_base_oid_index(FD_OID base)
{
  int boi = get_base_oid_index(base);
  if (boi>=0) return boi;
  u8_lock_mutex(&base_oid_lock);
  if (fd_n_base_oids >= FD_N_OID_BUCKETS) {
    u8_unlock_mutex(&base_oid_lock);
    return -1;}
  else {
    boi = fd_n_base_oids++;
    fd_base_oids[boi]=base;
    u8_unlock_mutex(&base_oid_lock);
    return boi;}
}

FD_EXPORT int fd_get_oid_base_index(FD_OID addr,int add)
{
  FD_OID base = addr;
  FD_SET_OID_LO(base,((FD_OID_LO(base))&0xFFF00000U));
  if (add) {
    int retval = add_base_oid_index(base);
    if (retval<0) fd_seterr1(OIDBucketOverflow);
    return retval;}
  else return get_base_oid_index(base);
}

FD_EXPORT fdtype fd_make_oid(FD_OID addr)
{
  FD_OID base = addr;
  int boi = 0;
  unsigned int offset = FD_OID_LO(addr)&(FD_OID_OFFSET_MASK);
  unsigned int bucket_base = FD_OID_LO(base)&(~(FD_OID_OFFSET_MASK));
  FD_SET_OID_LO(base,bucket_base);
  boi = add_base_oid_index(base);
  return FD_CONSTRUCT_OID(boi,offset);
}

fd_oid_info_fn _fd_oid_info;

/* This is just for use from the debugger, so we can allocate it
   statically. */
static char oid_info_buf[128];

static u8_string _simple_oid_info(fdtype oid)
{
  if (FD_OIDP(oid)) {
    FD_OID addr = FD_OID_ADDR(oid);
    unsigned int hi = FD_OID_HI(addr), lo = FD_OID_LO(addr);
    unsigned char tmpbuf[32];
    strcpy(oid_info_buf,"@");
    strcat(oid_info_buf,u8_uitoa16(hi,tmpbuf));
    strcat(oid_info_buf,"/");
    strcat(oid_info_buf,u8_uitoa16(lo,tmpbuf));
    return oid_info_buf;}
  else return "not an oid!";
}

/* B32 representation */

static char b32_chars[32]="0123456789abcdefghjkmnpqrtuvwxyz";
static char b32_weights[]=
  {-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
   -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
   -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
   0,1,2,3,4,5,6,7,8,9,-1,-1,-1,-1,-1,-1,
   -1,10,11,12,13,14,15,16,17,-1,18,19,20,21,22,-1,
   23,24,25,-1,26,-1,27,28,29,30,31,-1,-1,-1,-1,-1,
   -1,10,11,12,13,14,15,16,17,-1,18,19,20,21,22,-1,
   23,24,25,-1,26,-1,27,28,29,30,31,-1,-1,-1,-1,-1};

FD_EXPORT char *fd_ulonglong_to_b32(unsigned long long offset,
                                    char *buf,int *len)
{
  char tmpbuf[32]; int rem = offset, outlen = 0;
  int buflen = ((len)?(*len):((sizeof(unsigned long long)/5)+1));
  char *write = tmpbuf, *read, *limit = tmpbuf+buflen; 
  while (rem>0) {
    char digit = rem&0x1F, ch = b32_chars[(int)digit];
    if (write<limit) *write = ch; 
    else outlen = (write-tmpbuf);
    write++; rem = rem>>5;}
  if (outlen) {*len = -outlen; return NULL;}
  else outlen = write-tmpbuf;
  if (!(buf)) buf = u8_malloc((write-tmpbuf)+1);
  read = write-1; write = buf;
  while (read>=tmpbuf) *write++= *read--;
  *write++='\0';
  if (len) *len = outlen;
  return buf;
}

FD_EXPORT int fd_b32_to_ulonglong
  (const char *digits,unsigned long long *out)
{
  unsigned long long sum = 0; long long xsum;
  const char *scan = digits; int err = 0, weight;
  while (*scan) {
    int ch = *scan++;
    if ((ch>=128)||(ispunct(ch))||(isspace(ch)))
      continue;
    weight = b32_weights[ch];
    if (weight<0) err = 1;
    else {sum = sum<<5; sum = sum+weight;}}
  *out = sum; xsum = sum;
  if (err) return -1;
  else if (xsum<0) return 0;
  else return xsum;
}

FD_EXPORT long long fd_b32_to_longlong(const char *digits)
{
  unsigned long long sum = 0; long long xsum;
  const char *scan = digits; int err = 0;
  while (*scan) {
    int ch = *scan++;
    if ((ch>=128)||(ispunct(ch))||(isspace(ch)))
      continue;
    int weight = b32_weights[ch];
    if (weight<0) err = 1;
    else {sum = sum<<5; sum = sum+weight;}}
  xsum = sum;
  if ((err)||(xsum<0)) return -1;
  else return xsum;
}

/* Zero pool OIDs */

FD_EXPORT fdtype fd_zero_pool_value(fdtype oid)
{
  if (FD_EXPECT_TRUE(FD_OIDP(oid))) {
    FD_OID addr = FD_OID_ADDR(oid);
    if (FD_EXPECT_TRUE(FD_OID_HI(addr)==0)) {
      unsigned int off = FD_OID_LO(addr);
      unsigned int bucket_no = off/4096;
      unsigned int bucket_off = off%4096;
      if (off<fd_zero_pool_load) {
        if (fd_zero_pool_buckets[bucket_no])
          return fd_incref(fd_zero_pool_buckets[bucket_no][bucket_off]);
        else return FD_VOID;}
      else return FD_VOID;}
    else return fd_type_error("zero_pool oid","fd_zero_pool_value",oid);}
  else return fd_type_error("oid","fd_zero_pool_value",oid);
}
FD_EXPORT fdtype fd_zero_pool_store(fdtype oid,fdtype value)
{
  if (FD_EXPECT_TRUE(FD_OIDP(oid))) {
    FD_OID addr = FD_OID_ADDR(oid);
    if (FD_EXPECT_TRUE(FD_OID_HI(addr)==0)) {
      unsigned int off = FD_OID_LO(addr);
      unsigned int bucket_no = off/4096;
      unsigned int bucket_off = off%4096;
      if (off>=FD_ZERO_POOL_MAX)
        return FD_VOID;
      else {
        u8_lock_mutex(&zero_pool_lock);
        fdtype *bucket = fd_zero_pool_buckets[bucket_no];
        if (FD_EXPECT_FALSE(bucket == NULL)) {
          bucket = u8_alloc_n(4096,fdtype);
          fd_zero_pool_buckets[bucket_no]=bucket;}
        fdtype current = bucket[bucket_off];
        if ((current)&&(FD_CONSP(current)))
          fd_decref(current);
        bucket[bucket_off]=fd_incref(value);
        u8_unlock_mutex(&zero_pool_lock);
        return FD_VOID;}}
    else return fd_type_error("zero_pool oid","fd_zero_pool_store",oid);}
  else return fd_type_error("oid","fd_zero_pool_store",oid);
}

fdtype fd_preoids = FD_EMPTY_CHOICE;

static void init_oids()
{
  int i = 0; while (i<N_OID_INITS) {
    FD_OID base = FD_MAKE_OID(_fd_oid_inits[i].hi,_fd_oid_inits[i].lo);
    unsigned int cap=_fd_oid_inits[i].cap;
    int j = 0, lim = 1+(cap/(FD_OID_BUCKET_SIZE));
    while (j<lim) {
      fdtype oid = fd_make_oid(base);
      FD_ADD_TO_CHOICE(fd_preoids,oid);
      base = FD_OID_PLUS(base,FD_OID_BUCKET_SIZE);
      j++;}
    i++;}
  u8_init_mutex(&zero_pool_lock);
  memset(fd_zero_pool_buckets,0,sizeof(fd_zero_pool_buckets));
}

void fd_init_oids_c()
{
  u8_register_source_file(_FILEINFO);
  fd_type_names[fd_oid_type]="OID";
  _fd_oid_info=_simple_oid_info;
  u8_init_mutex(&(base_oid_lock));
  init_oids();

  u8_init_mutex(&zero_pool_lock);
  memset(fd_zero_pool_values,0,sizeof(fd_zero_pool_values));
  memset(fd_zero_pool_buckets,0,sizeof(fd_zero_pool_buckets));
  fd_zero_pool_buckets[0]=fd_zero_pool_values;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/