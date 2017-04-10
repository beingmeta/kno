/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_HASH_H
#define FRAMERD_HASH_H 1
#ifndef FRAMERD_HASH_H_INFO
#define FRAMERD_HASH_H_INFO "include/framerd/hash.h"
#endif

#define MYSTERIOUS_MODULUS 256001281
#define MYSTERIOUS_MULTIPLIER 2654435769U

#if (SIZEOF_LONG_LONG == 8)
static unsigned int hash_mult(unsigned int x,unsigned int y)
{
  return ((x*y)%(MYSTERIOUS_MODULUS));
}
#else
static unsigned int hash_mult(unsigned int x,unsigned int y)
{
  if (x == 1) return y;
  else if (y == 1) return x;
  else if ((x == 0) || (y == 0)) return 0;
  else {
    unsigned int a=(x>>16), b=(x&0xFFFF);
    unsigned int c=(y>>16), d=(y&0xFFFF);
    unsigned int bd=b*d, ad=a*d, bc=b*c, ac=a*c;
    unsigned int hi=ac, lo=(bd&0xFFFF), tmp, carry, i;
    tmp=(bd>>16)+(ad&0xFFFF)+(bc&0xFFFF);
    lo=lo+((tmp&0xFFFF)<<16); carry=(tmp>>16);
    hi=hi+carry+(ad>>16)+(bc>>16);
    i=0; while (i++ < 4) {
      hi=((hi<<8)|(lo>>24))%(MYSTERIOUS_MODULUS); lo=lo<<8;}
    return hi;}
}
#endif


#if 0
#if (SIZEOF_LONG_LONG == 8)
static unsigned int hash_multr(unsigned int x,unsigned int y,
                               unsigned int r)
{
  return ((x*y)%(r));
}
#else
static unsigned int hash_multr(unsigned int x,unsigned int y,
                               unsigned int r)
{
  if (x == 1) return y;
  else if (y == 1) return x;
  if ((x == 0) || (y == 0)) return 0;
  else {
    unsigned int a=(x>>16), b=(x&0xFFFF);
    unsigned int c=(y>>16), d=(y&0xFFFF);
    unsigned int bd=b*d, ad=a*d, bc=b*c, ac=a*c;
    unsigned int hi=ac, lo=(bd&0xFFFF), tmp, carry, i;
    tmp=(bd>>16)+(ad&0xFFFF)+(bc&0xFFFF);
    lo=lo+((tmp&0xFFFF)<<16); carry=(tmp>>16);
    hi=hi+carry+(ad>>16)+(bc>>16);
    i=0; while (i++ < 4) {
      hi=((hi<<8)|(lo>>24))%(r); lo=lo<<8;}
    return hi;}
}
#endif
#endif

static U8_MAYBE_UNUSED
unsigned int hash_combine(unsigned int x,unsigned int y)
{
  if ((x == 0) && (y == 0))
    return MYSTERIOUS_MODULUS+2;
  else if ((x == 0) || (y == 0))
    return x+y;
  else return hash_mult(x,y);
}

#endif /* FRAMERD_HASH_H */
