/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/preoids.h"

#include <libu8/u8printf.h>
#include <ctype.h> /* For sprintf */

#define HASH_MULTIPLIER 2654435769U

u8_condition kno_NotAnOID=_("Not an OID");

lispval kno_zero_pool_values[4096]={VOID};
lispval *kno_zero_pool_buckets[KNO_ZERO_POOL_MAX/4096]={kno_zero_pool_values,NULL};
unsigned int kno_zero_pool_load = 0;
static u8_mutex zero_pool_lock;

const int _KNO_OID_BUCKET_WIDTH=KNO_OID_BUCKET_WIDTH;
const int _KNO_OID_BUCKET_MASK=KNO_OID_BUCKET_MASK;

KNO_OID kno_base_oids[KNO_N_OID_BUCKETS];
struct KNO_OID_BUCKET *kno_oid_buckets=NULL;
int kno_n_base_oids = 0;
int kno_oid_buckets_len = KNO_N_OID_BUCKETS*4-1;
static u8_rwlock base_oid_lock;
static u8_condition OIDBucketOverflow="Out of OID buckets";

static int n_chains=0, sum_chains=0;

KNO_FASTOP int find_base_oid(KNO_OID base)
{
  /* TODO: Improve this hashing algorithm? */
  unsigned int hi = KNO_OID_HI(base), lo=KNO_OID_LO(base);
  unsigned long hi_times = hi*HASH_MULTIPLIER, lo_times = lo*HASH_MULTIPLIER;
  unsigned long hashval = hi_times ^ lo_times;
  int init_probe = hashval%kno_oid_buckets_len, probe=init_probe;
  while (kno_oid_buckets[probe].bucket_no>=0) {
    if (KNO_OID_COMPARE(base,kno_oid_buckets[probe].bucket_base)==0)
      return kno_oid_buckets[probe].bucket_no;
    probe++;
    if (probe>=kno_oid_buckets_len) probe=0;
    if (probe==init_probe)
      return -1;}
  return -1;
}

static int add_base_oid_index(KNO_OID base)
{
  int boi = find_base_oid(base);
  if (boi>=0)
    return boi;
  else if (kno_n_base_oids >= KNO_N_OID_BUCKETS)
    return -1;
  else {
    unsigned int boi=kno_n_base_oids++;
    unsigned int hi = KNO_OID_HI(base), lo=KNO_OID_LO(base);
    unsigned long hi_times = hi*HASH_MULTIPLIER, lo_times = lo*HASH_MULTIPLIER;
    unsigned long hashval = hi_times ^ lo_times;
    int init_probe = hashval%kno_oid_buckets_len, probe=init_probe, chain_len=0;
    while (kno_oid_buckets[probe].bucket_no>=0) {
      if (KNO_OID_COMPARE(base,kno_oid_buckets[probe].bucket_base)==0)
        return kno_oid_buckets[probe].bucket_no;
      probe++; chain_len++;
      if (probe>=kno_oid_buckets_len) probe=0;
      if (probe==init_probe) {
        u8_log(LOG_CRIT,"Out of OID buckets",
               "The OID bucket table has reached saturaration at %d buckets",
               kno_n_base_oids);
        return -1;}}
    n_chains++; sum_chains+=chain_len;
    kno_oid_buckets[probe].bucket_base=base;
    kno_oid_buckets[probe].bucket_no=boi;
    kno_base_oids[boi]=base;
    return boi;}
}

KNO_EXPORT int kno_get_oid_base_index(KNO_OID addr,int add)
{
  KNO_OID base = addr;
  KNO_SET_OID_LO(base,((KNO_OID_LO(base))&0xFFF00000U));
  if (add) {
    int retval = add_base_oid_index(base);
    if (retval<0) kno_seterr1(OIDBucketOverflow);
    return retval;}
  else return find_base_oid(base);
}

KNO_EXPORT lispval kno_make_oid(KNO_OID addr)
{
  KNO_OID base = addr;
  int boi = 0;
  unsigned int offset = KNO_OID_LO(addr)&(KNO_OID_OFFSET_MASK);
  unsigned int bucket_base = KNO_OID_LO(base)&(~(KNO_OID_OFFSET_MASK));
  KNO_SET_OID_LO(base,bucket_base);
  boi = add_base_oid_index(base);
  return KNO_CONSTRUCT_OID(boi,offset);
}

kno_oid_info_fn _kno_oid_info;

/* This is just for use from the debugger, so we can allocate it
   statically. */
static char oid_info_buf[128];

static u8_string _simple_oid_info(lispval oid)
{
  if (OIDP(oid)) {
    KNO_OID addr = KNO_OID_ADDR(oid);
    unsigned int hi = KNO_OID_HI(addr), lo = KNO_OID_LO(addr);
    unsigned char tmpbuf[32];
    strcpy(oid_info_buf,"@");
    strcat(oid_info_buf,u8_uitoa16(hi,tmpbuf));
    strcat(oid_info_buf,"/");
    strcat(oid_info_buf,u8_uitoa16(lo,tmpbuf));
    return oid_info_buf;}
  else return "not an oid!";
}

KNO_EXPORT u8_string kno_oid2string(lispval oidval,u8_byte *buf,ssize_t len)
{
  KNO_OID addr = KNO_OID_ADDR(oidval);
  if ( (buf == NULL) || (len < 0) )
    return u8_mkstring("@%x/%x",KNO_OID_HI(addr),KNO_OID_LO(addr));
  else return u8_sprintf(buf,len,"@%x/%x",KNO_OID_HI(addr),KNO_OID_LO(addr));
}

/* B32 representation */

#if 0
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

KNO_EXPORT char *kno_ulonglong_to_b32(unsigned long long offset,
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

KNO_EXPORT int kno_b32_to_ulonglong
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

KNO_EXPORT long long kno_b32_to_longlong(const char *digits)
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

#endif

/* Zero pool OIDs */

KNO_EXPORT lispval kno_zero_pool_value(lispval oid)
{
  if (PRED_TRUE(OIDP(oid))) {
    KNO_OID addr = KNO_OID_ADDR(oid);
    if (PRED_TRUE(KNO_OID_HI(addr)==0)) {
      unsigned int off = KNO_OID_LO(addr);
      unsigned int bucket_no = off/4096;
      unsigned int bucket_off = off%4096;
      if (off<kno_zero_pool_load) {
        if (kno_zero_pool_buckets[bucket_no])
          return kno_incref(kno_zero_pool_buckets[bucket_no][bucket_off]);
        else return VOID;}
      else return VOID;}
    else return kno_type_error("zero_pool oid","kno_zero_pool_value",oid);}
  else return kno_type_error("oid","kno_zero_pool_value",oid);
}
KNO_EXPORT lispval kno_zero_pool_store(lispval oid,lispval value)
{
  if (PRED_TRUE(OIDP(oid))) {
    KNO_OID addr = KNO_OID_ADDR(oid);
    if (PRED_TRUE(KNO_OID_HI(addr)==0)) {
      unsigned int off = KNO_OID_LO(addr);
      unsigned int bucket_no = off/4096;
      unsigned int bucket_off = off%4096;
      if (off>=KNO_ZERO_POOL_MAX)
        return VOID;
      else {
        u8_lock_mutex(&zero_pool_lock);
        lispval *bucket = kno_zero_pool_buckets[bucket_no];
        if (PRED_FALSE(bucket == NULL)) {
          bucket = u8_alloc_n(4096,lispval);
          kno_zero_pool_buckets[bucket_no]=bucket;}
        lispval current = bucket[bucket_off];
        if ((current)&&(CONSP(current)))
          kno_decref(current);
        bucket[bucket_off]=kno_incref(value);
        u8_unlock_mutex(&zero_pool_lock);
        return VOID;}}
    else return kno_type_error("zero_pool oid","kno_zero_pool_store",oid);}
  else return kno_type_error("oid","kno_zero_pool_store",oid);
}

lispval kno_preoids = EMPTY;

static void init_oids()
{
  int i=0;
  kno_oid_buckets=u8_alloc_n(kno_oid_buckets_len,struct KNO_OID_BUCKET);
  while (i<kno_oid_buckets_len) kno_oid_buckets[i++].bucket_no=-1;
  i=0; while (i<N_OID_INITS) {
    KNO_OID base = KNO_MAKE_OID(_kno_oid_inits[i].hi,_kno_oid_inits[i].lo);
    unsigned int cap=_kno_oid_inits[i].cap;
    int j = 0, lim = 1+(cap/(KNO_OID_BUCKET_SIZE));
    while (j<lim) {
      lispval oid = kno_make_oid(base);
      CHOICE_ADD(kno_preoids,oid);
      base = KNO_OID_PLUS(base,KNO_OID_BUCKET_SIZE);
      j++;}
    i++;}
  u8_init_mutex(&zero_pool_lock);
  memset(kno_zero_pool_buckets,0,sizeof(kno_zero_pool_buckets));
}

void kno_init_oids_c()
{
  u8_register_source_file(_FILEINFO);
  kno_type_names[kno_oid_type]="OID";
  _kno_oid_info=_simple_oid_info;
  u8_init_rwlock(&(base_oid_lock));
  init_oids();

  u8_init_mutex(&zero_pool_lock);
  memset(kno_zero_pool_values,0,sizeof(kno_zero_pool_values));
  memset(kno_zero_pool_buckets,0,sizeof(kno_zero_pool_buckets));
  kno_zero_pool_buckets[0]=kno_zero_pool_values;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
