/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define U8_INLINE_IO 1

#include "fdb/dtype.h"
#include "fdb/dbfile.h"

/* Used to generate hash codes */
#define MAGIC_MODULUS 16777213 /* 256000001 */
#define MIDDLIN_MODULUS 573786077 /* 256000001 */
#define MYSTERIOUS_MODULUS 2000239099 /* 256000001 */

/** Hashing lisp objects **/

FD_FASTOP unsigned int hash_string_dtype1 (fdtype x)
{
  char *ptr=FD_STRDATA(x), *limit=ptr+FD_STRLEN(x);
  unsigned int sum=0;
  while (ptr < limit) {
    sum=(sum<<8)+(*ptr++); 
    sum=sum%(MAGIC_MODULUS);}
  return sum;
}

static unsigned int hash_unicode_string_dtype1 (fdtype x)
{
  unsigned char *ptr=FD_STRDATA(x), *limit=ptr+FD_STRLEN(x);
  unsigned int sum=0;
  while (ptr < limit) {
    int c=u8_sgetc(&ptr);
    sum=(sum<<8)+(c);
    sum=sum%(MAGIC_MODULUS);}
  return sum;
}

static unsigned int hash_packet(fdtype x)
{
  int size=FD_PACKET_LENGTH(x);
  unsigned char *data=FD_PACKET_DATA(x);
  unsigned char *ptr=data, *limit=ptr+size;
  unsigned int sum=0;
  while (ptr < limit) sum=((sum<<4)+(*ptr++))%MAGIC_MODULUS;
  return sum;
}

/* hash_symbol_dtype1: (static)
     Arguments: a dtype pointer (to a symbol)
     Returns: a hash value (an unsigned int)

  Computes an iterative hash over the characters in the symbol's name.
*/
FD_FASTOP unsigned int hash_symbol_dtype1(fdtype x)
{
  unsigned char *s = FD_SYMBOL_NAME (x);
  unsigned int sum=0;
  while (*s != '\0') {
    int c;
    if (*s < 0x80) c=*s++;
    else if (*s < 0xe0) {
      c=(int)((*s++)&(0x3f)<<6);
      c=c|((int)((*s++)&(0x3f)));}
    else {
      c=(int)((*s++)&(0x1f)<<12);
      c=c|(int)(((*s++)&(0x3f))<<6);
      c=c|(int)((*s++)&(0x3f));}
    sum=(sum<<8)+(c); sum=sum%(MAGIC_MODULUS);}
  return sum;
}

static unsigned int hash_pair_dtype1(fdtype string);

/*
   hash_dtype: (static inline)
     Arguments: a "lisp" pointer
     Returns: an unsigned int computed from the pointer

     Basic strategy:
      integers become themselves, immediates get special codes,
       strings and symbols get characters scanned, objects use their
       low order bytes, vectors and pairs get their elements combined
       with an ordering (to keep it asymmetric).

     Notes: we assume 24 bit hash values so that any LISP implementation
      can compute hashes using just fixnum arithmetic.
*/
FD_FASTOP unsigned int hash_dtype1(fdtype x)
{
  if (FD_IMMEDIATEP(x))
    if ((FD_EMPTY_LISTP(x)) || (FD_FALSEP(x))) return 37;
    else if (FD_TRUEP(x)) return 17;
    else if (FD_EMPTY_CHOICEP(x)) return 13;
    else if (FD_SYMBOLP(x))
      return hash_symbol_dtype1(x);
    else return 19;
  else if (FD_FIXNUMP(x))
    return (FD_FIX2INT(x))%(MAGIC_MODULUS);
  else if (FD_STRINGP(x))
    return hash_string_dtype1(x);
  else if (FD_PACKETP(x))
    return hash_packet(x);
  else if (FD_PAIRP(x))
    return hash_pair_dtype1(x);
  else if (FD_SYMBOLP(x))
    return hash_symbol_dtype1(x);
  else if (FD_OIDP(x)) {
    FD_OID id=FD_OID_ADDR(x);
#if FD_STRUCT_OIDS
    unsigned int hi=FD_OID_HI(id), lo=FD_OID_LO(id);
    int i=0; while (i++ < 4)
      {hi=((hi<<8)|(lo>>24))%(MAGIC_MODULUS); lo=lo<<8;}
    return hi;
#else
    return id%(MAGIC_MODULUS);
#endif
      }
  else if (FD_CHOICEP(x)) {
    unsigned int sum=0;
    FD_DO_CHOICES(elt,x)
      sum=((sum+hash_dtype1(elt))%MAGIC_MODULUS);
    return sum;}
  else if (FD_QCHOICEP(x)) {
    struct FD_QCHOICE *qc=FD_XQCHOICE(x);
    return hash_dtype1(qc->choice);}
  else if (FD_CHARACTERP(x)) return (FD_CHARCODE(x))%(MAGIC_MODULUS);
  else if (FD_PTR_TYPEP(x,fd_double_type)) {
    unsigned int as_int;
    float *f=(float *)(&as_int);
    *f=FD_FLONUM(x);
    return (as_int)%(MAGIC_MODULUS);}
  else if (FD_VECTORP(x)) {
    int size=FD_VECTOR_LENGTH(x); unsigned int i=0, sum=0;
    while (i < size) {
      sum=(((sum<<4)+(hash_dtype1(FD_VECTOR_REF(x,i))))%MAGIC_MODULUS);
      i++;}
    return sum;}
  else if (FD_SLOTMAPP(x)) {
    unsigned int sum=0;
    struct FD_SLOTMAP *sm=FD_XSLOTMAP(x);
    struct FD_KEYVAL *scan=sm->keyvals, *limit=scan+FD_XSLOTMAP_SIZE(sm);
    while (scan<limit) {
      sum=(sum+hash_dtype1(scan->key))%MAGIC_MODULUS;
      sum=(sum+(fd_flip_word(hash_dtype1(scan->value))%MAGIC_MODULUS))%MAGIC_MODULUS;
      scan++;}
    return sum;}
  else if (FD_PTR_TYPEP(x,fd_rational_type)) {
    struct FD_PAIR *p=FD_STRIP_CONS(x,fd_rational_type,struct FD_PAIR *);
    unsigned int sum=hash_dtype1(p->car)%MAGIC_MODULUS;
    sum=(sum<<4)+hash_dtype1(p->cdr)%MAGIC_MODULUS;
    return sum%MAGIC_MODULUS;}
  else if (FD_PTR_TYPEP(x,fd_complex_type)) {
    struct FD_PAIR *p=FD_STRIP_CONS(x,fd_complex_type,struct FD_PAIR *);
    unsigned int sum=hash_dtype1(p->car)%MAGIC_MODULUS;
    sum=(sum<<4)+hash_dtype1(p->cdr)%MAGIC_MODULUS;
    return sum%MAGIC_MODULUS;}
  else return 17;
}

/* hash_pair_dtype1: (static)
     Arguments: a dtype pointer (to a pair)
     Returns: a hash value (an unsigned int)

     Adds the hash of the list's elements together, shifting each
     value by 4 bits and ending with the final CDR.  The shift is
     done to have the hash function be sensitive to element permuations.
*/
static unsigned int hash_pair_dtype1(fdtype x)
{
  fdtype ptr=x; unsigned int sum=0;
  /* The shift here is introduced to make the hash function asymmetric */
  while (FD_PAIRP(ptr)) {
    sum=((sum*2)+hash_dtype1(FD_CAR(ptr)))%(MAGIC_MODULUS);
    ptr=FD_CDR(ptr);}
  if (!(FD_EMPTY_LISTP(ptr))) {
    unsigned int cdr_hash=hash_dtype1(ptr);
    sum=(sum+(fd_flip_word(cdr_hash)>>8))%(MAGIC_MODULUS);}
  return sum;
}

FD_EXPORT
/* fd_hash_dtype:
     Arguments: a list pointer
     Returns: an unsigned int
  This returns an integer corresponding to its argument which will
   be the same on any platform.
*/
unsigned int fd_hash_dtype1(fdtype x)
{
  return hash_dtype1(x);
}

/* Multiplier heavy hashing */

FD_FASTOP unsigned int hash_symbol_dtype2(fdtype x);
FD_FASTOP unsigned int hash_pair_dtype2(fdtype x);
FD_FASTOP unsigned int hash_record_dtype2(fdtype x);

typedef unsigned long long ull;

static unsigned int hash_mult(unsigned int x,unsigned int y)
{
  unsigned long long prod=((ull)x)*((ull)y);
  return (prod*2100000523)%(MYSTERIOUS_MODULUS);
}

static unsigned int hash_combine(unsigned int x,unsigned int y)
{
  if ((x == 0) && (y == 0)) return MAGIC_MODULUS+2;
  else if ((x == 0) || (y == 0))
    return x+y;
  else return hash_mult(x,y);
}

FD_FASTOP unsigned int mult_hash_string(unsigned char *start,int len)
{
  unsigned int prod=1, asint;
  unsigned char *ptr=start, *limit=ptr+len;
  /* Compute a starting place */
  while (ptr < limit) prod=prod+*ptr++;
  /* Now do a multiplication */
  ptr=start; limit=ptr+((len%4) ? (4*(len/4)) : (len));
  while (ptr < limit) {
    asint=(ptr[0]<<24)|(ptr[1]<<16)|(ptr[2]<<8)|(ptr[3]);
    prod=hash_combine(prod,asint); ptr=ptr+4;}
  switch (len%4) {
  case 0: asint=1; break;
  case 1: asint=ptr[0]; break;
  case 2: asint=ptr[0]|(ptr[1]<<8); break;
  case 3: asint=ptr[0]|(ptr[1]<<8)|(ptr[2]<<16); break;}
  return hash_combine(prod,asint);
}

/* hash_string_dtype: (static)
     Arguments: a dtype pointer (to a string)
     Returns: a hash value (an unsigned int)

  Computes an iterative hash over the characters in the string.
*/
FD_FASTOP unsigned int hash_string_dtype2(fdtype x)
{
  int len=FD_STRLEN(x);
  if (len == 0) return MAGIC_MODULUS+2;
  else if (len == 1)
    if (*(FD_STRDATA(x))) return (*(FD_STRDATA(x)));
    else return MAGIC_MODULUS-2;
  else return mult_hash_string(FD_STRDATA(x),len);
}

FD_FASTOP unsigned int hash_dtype2(fdtype x)
{
  if (FD_IMMEDIATEP(x))
    if ((FD_EMPTY_LISTP(x)) || (FD_FALSEP(x))) return 37;
    else if (FD_TRUEP(x)) return 17;
    else if (FD_EMPTY_CHOICEP(x)) return 13;
    else if (FD_SYMBOLP(x))
      return hash_symbol_dtype2(x);
    else if (FD_CHARACTERP(x))
      return (FD_CHARCODE(x))%(MAGIC_MODULUS);
    else return 19;
  else if (FD_FIXNUMP(x))
    return (FD_FIX2INT(x))%(MAGIC_MODULUS);
  else if (FD_OIDP(x)) {
    FD_OID id=FD_OID_ADDR(x);
#if FD_STRUCT_OIDS
    unsigned int hi=FD_OID_HI(id), lo=FD_OID_LO(id);
    int i=0; while (i++ < 4) {
      hi=((hi<<8)|(lo>>24))%(MAGIC_MODULUS); lo=lo<<8;}
    return hi;
#else
    return id%(MAGIC_MODULUS);
#endif
  }
  else if (FD_CONSP(x)) {
    int ctype=FD_PTR_TYPE(x);
    switch (ctype) {
    case fd_string_type:
      return hash_string_dtype2(x);
    case fd_packet_type:
      return hash_packet(x);
    case fd_pair_type:
      return hash_pair_dtype2(x);
    case fd_qchoice_type: {
      struct FD_QCHOICE *qc=FD_XQCHOICE(x);
      return hash_dtype2(qc->choice);}
    case fd_choice_type: {
      unsigned int sum=0;
      FD_DO_CHOICES(elt,x)
	sum=(sum+(hash_dtype2(elt))%MAGIC_MODULUS);
      return sum;}
    case fd_vector_type: {
      int i=0, size=FD_VECTOR_LENGTH(x); unsigned int prod=1;
      while (i < size) {
	prod=hash_combine(prod,hash_dtype2(FD_VECTOR_REF(x,i))); i++;}
      return prod;}
    case fd_slotmap_type: {
      unsigned int sum=0;
      struct FD_SLOTMAP *sm=FD_XSLOTMAP(x);
      struct FD_KEYVAL *scan=sm->keyvals, *limit=scan+FD_XSLOTMAP_SIZE(sm);
      while (scan<limit) {
	unsigned int prod=
	  hash_combine(hash_dtype2(scan->key),hash_dtype2(scan->value));
	sum=(sum+prod)%(MYSTERIOUS_MODULUS);
	scan++;}
      return sum;}
    case fd_rational_type: {
      struct FD_PAIR *p=FD_STRIP_CONS(x,fd_rational_type,struct FD_PAIR *);
      return hash_combine(hash_dtype2(p->car),hash_dtype2(p->cdr));}
    case fd_complex_type: {
      struct FD_PAIR *p=FD_STRIP_CONS(x,fd_complex_type,struct FD_PAIR *);
      return hash_combine(hash_dtype2(p->car),hash_dtype2(p->cdr));}
    default:
      if ((ctype<FD_TYPE_MAX) && (fd_hashfns[ctype]))
	return fd_hashfns[ctype](x,fd_hash_dtype2);
      else return 17;}}
}

/* hash_symbol_dtype2: (static)
     Arguments: a dtype pointer (to a symbol)
     Returns: a hash value (an unsigned int)

  Computes an iterative hash over the characters in the symbol's name.
*/
FD_FASTOP unsigned int hash_symbol_dtype2(fdtype x)
{
  unsigned char *s = FD_SYMBOL_NAME (x);
  unsigned int sum=0;
  while (*s != '\0') {
    int c;
    if (*s < 0x80) c=*s++;
    else if (*s < 0xe0) {
      c=(int)((*s++)&(0x3f)<<6);
      c=c|((int)((*s++)&(0x3f)));}
    else {
      c=(int)((*s++)&(0x1f)<<12);
      c=c|(int)(((*s++)&(0x3f))<<6);
      c=c|(int)((*s++)&(0x3f));}
    sum=(sum<<8)+(c); sum=sum%(MAGIC_MODULUS);}
  return sum;
}

FD_EXPORT
/* fd_hash_dtype2:
     Arguments: a list pointer
     Returns: an unsigned int
  This is a better hashing algorithm than the legacy used for years.
*/
unsigned int fd_hash_dtype2(fdtype x)
{
  return hash_dtype2(x);
}

/* hash_pair_dtype2: (static)
     Arguments: a dtype pointer (to a pair)
     Returns: a hash value (an unsigned int)
*/
static unsigned int hash_pair_dtype2(fdtype x)
{
  fdtype ptr=x; unsigned int prod=1;
  while (FD_PAIRP(ptr)) {
    prod=hash_combine(prod,hash_dtype2(FD_CAR(ptr)));
    ptr=FD_CDR(ptr);}
  if (!(FD_EMPTY_LISTP(ptr))) {
    unsigned int cdr_hash=hash_dtype2(ptr);
    prod=hash_combine(prod,cdr_hash);}
  return prod;
}

/* hash_dtype3 */

FD_FASTOP unsigned int hash_dtype3(fdtype x);

/* hash_pair_dtype3: (static)
     Arguments: a dtype pointer (to a pair)
     Returns: a hash value (an unsigned int)
*/
static unsigned int hash_pair_dtype3(fdtype x)
{
  fdtype ptr=x; unsigned int prod=1;
  while (FD_PAIRP(ptr)) {
    prod=hash_combine(prod,hash_dtype3(FD_CAR(ptr)));
    ptr=FD_CDR(ptr);}
  if (!(FD_EMPTY_LISTP(ptr))) {
    unsigned int cdr_hash=hash_dtype3(ptr);
    prod=hash_combine(prod,cdr_hash);}
  return prod;
}

FD_FASTOP unsigned int hash_dtype3(fdtype x)
{
  if (FD_IMMEDIATEP(x))
    if ((FD_EMPTY_LISTP(x)) || (FD_FALSEP(x))) return 37;
    else if (FD_TRUEP(x)) return 17;
    else if (FD_EMPTY_CHOICEP(x)) return 13;
    else if (FD_SYMBOLP(x))
      return hash_symbol_dtype2(x);
    else if (FD_CHARACTERP(x))
      return (FD_CHARCODE(x))%(MAGIC_MODULUS);
    else return 19;
  else if (FD_FIXNUMP(x))
    return (FD_FIX2INT(x))%(MAGIC_MODULUS);
  else if (FD_OIDP(x)) {
    FD_OID id=FD_OID_ADDR(x);
#if FD_STRUCT_OIDS
    unsigned int hi=FD_OID_HI(id), lo=FD_OID_LO(id);
    int i=0; while (i++ < 4) {
      hi=((hi<<8)|(lo>>24))%(MYSTERIOUS_MODULUS); lo=lo<<8;}
    return hi;
#else
    return id%(MYSTERIOUS_MODULUS);
#endif
  }
  else if (FD_CONSP(x)) {
    int ctype=FD_PTR_TYPE(x);
    switch (ctype) {
    case fd_string_type:
      return hash_string_dtype2(x);
    case fd_packet_type:
      return hash_packet(x);
    case fd_pair_type:
      return hash_pair_dtype3(x);
    case fd_qchoice_type: {
      struct FD_QCHOICE *qc=FD_XQCHOICE(x);
      return hash_dtype3(qc->choice);}
    case fd_choice_type: {
      unsigned int sum=0;
      FD_DO_CHOICES(elt,x)
	sum=(sum+(hash_dtype3(elt))%MIDDLIN_MODULUS);
      return sum;}
    case fd_vector_type: {
      int i=0, size=FD_VECTOR_LENGTH(x); unsigned int prod=1;
      while (i < size) {
	prod=hash_combine(prod,hash_dtype3(FD_VECTOR_REF(x,i))); i++;}
      return prod;}
    case fd_slotmap_type: {
      unsigned int sum=0;
      struct FD_SLOTMAP *sm=FD_XSLOTMAP(x);
      struct FD_KEYVAL *scan=sm->keyvals, *limit=scan+FD_XSLOTMAP_SIZE(sm);
      while (scan<limit) {
	unsigned int prod=
	  hash_combine(hash_dtype3(scan->key),hash_dtype3(scan->value));
	sum=(sum+prod)%(MYSTERIOUS_MODULUS);
	scan++;}
      return sum;}
    case fd_rational_type: {
      struct FD_PAIR *p=FD_STRIP_CONS(x,fd_rational_type,struct FD_PAIR *);
      return hash_combine(hash_dtype3(p->car),hash_dtype3(p->cdr));}
    case fd_complex_type: {
      struct FD_PAIR *p=FD_STRIP_CONS(x,fd_complex_type,struct FD_PAIR *);
      return hash_combine(hash_dtype3(p->car),hash_dtype3(p->cdr));}
    default:
      if ((ctype<FD_TYPE_MAX) && (fd_hashfns[ctype]))
	return fd_hashfns[ctype](x,fd_hash_dtype3);
      else return 17;}}
}

FD_EXPORT
/* fd_hash_dtype3:
     Arguments: a list pointer
     Returns: an unsigned int
  This is a better hashing algorithm than the legacy used for years.
*/
unsigned int fd_hash_dtype3(fdtype x)
{
  return hash_dtype3(x);
}

FD_EXPORT
/* fd_hash_dtype_rep:
     Arguments: a list pointer
     Returns: an unsigned int
  This is a better hashing algorithm than the legacy used for years.
*/
unsigned int fd_hash_dtype_rep(fdtype x)
{
  struct FD_BYTE_OUTPUT out; unsigned int hashval;
  FD_INIT_BYTE_OUTPUT(&out,1024,NULL);
  fd_write_dtype(&out,x);
  hashval=mult_hash_string(out.start,out.ptr-out.start);
  u8_free(out.start);
  return hashval;
}


/* Another hash function */

#if WORDS_BIGENDIAN
# define HASH_LITTLE_ENDIAN 0
# define HASH_BIG_ENDIAN 1
#else
# define HASH_LITTLE_ENDIAN 1
# define HASH_BIG_ENDIAN 0
#endif

#define hashsize(n) ((uint32_t)1<<(n))
#define hashmask(n) (hashsize(n)-1)
#define rot(x,k) (((x)<<(k)) | ((x)>>(32-(k))))

/*
-------------------------------------------------------------------------------
mix -- mix 3 32-bit values reversibly.

This is reversible, so any information in (a,b,c) before mix() is
still in (a,b,c) after mix().

If four pairs of (a,b,c) inputs are run through mix(), or through
mix() in reverse, there are at least 32 bits of the output that
are sometimes the same for one pair and different for another pair.
This was tested for:
* pairs that differed by one bit, by two bits, in any combination
  of top bits of (a,b,c), or in any combination of bottom bits of
  (a,b,c).
* "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
  the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
  is commonly produced by subtraction) look like a single 1-bit
  difference.
* the base values were pseudorandom, all zero but one bit set, or 
  all zero plus a counter that starts at zero.

Some k values for my "a-=c; a^=rot(c,k); c+=b;" arrangement that
satisfy this are
    4  6  8 16 19  4
    9 15  3 18 27 15
   14  9  3  7 17  3
Well, "9 15 3 18 27 15" didn't quite get 32 bits diffing
for "differ" defined as + with a one-bit base and a two-bit delta.  I
used http://burtleburtle.net/bob/hash/avalanche.html to choose 
the operations, constants, and arrangements of the variables.

This does not achieve avalanche.  There are input bits of (a,b,c)
that fail to affect some output bits of (a,b,c), especially of a.  The
most thoroughly mixed value is c, but it doesn't really even achieve
avalanche in c.

This allows some parallelism.  Read-after-writes are good at doubling
the number of bits affected, so the goal of mixing pulls in the opposite
direction as the goal of parallelism.  I did what I could.  Rotates
seem to cost as much as shifts on every machine I could lay my hands
on, and rotates are much kinder to the top and bottom bits, so I used
rotates.
-------------------------------------------------------------------------------
*/
#define mix(a,b,c) \
{ \
  a -= c;  a ^= rot(c, 4);  c += b; \
  b -= a;  b ^= rot(a, 6);  a += c; \
  c -= b;  c ^= rot(b, 8);  b += a; \
  a -= c;  a ^= rot(c,16);  c += b; \
  b -= a;  b ^= rot(a,19);  a += c; \
  c -= b;  c ^= rot(b, 4);  b += a; \
}

/*
-------------------------------------------------------------------------------
final -- final mixing of 3 32-bit values (a,b,c) into c

Pairs of (a,b,c) values differing in only a few bits will usually
produce values of c that look totally different.  This was tested for
* pairs that differed by one bit, by two bits, in any combination
  of top bits of (a,b,c), or in any combination of bottom bits of
  (a,b,c).
* "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
  the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
  is commonly produced by subtraction) look like a single 1-bit
  difference.
* the base values were pseudorandom, all zero but one bit set, or 
  all zero plus a counter that starts at zero.

These constants passed:
 14 11 25 16 4 14 24
 12 14 25 16 4 14 24
and these came close:
  4  8 15 26 3 22 24
 10  8 15 26 3 22 24
 11  8 15 26 3 22 24
-------------------------------------------------------------------------------
*/
#define final(a,b,c) \
{ \
  c ^= b; c -= rot(b,14); \
  a ^= c; a -= rot(c,11); \
  b ^= a; b -= rot(a,25); \
  c ^= b; c -= rot(b,16); \
  a ^= c; a -= rot(c,4);  \
  b ^= a; b -= rot(a,14); \
  c ^= b; c -= rot(b,24); \
}

/*
--------------------------------------------------------------------
 This works on all machines.  To be useful, it requires
 -- that the key be an array of uint32_t's, and
 -- that the length be the number of uint32_t's in the key

 The function hashword() is identical to hashlittle() on little-endian
 machines, and identical to hashbig() on big-endian machines,
 except that the length has to be measured in uint32_ts rather than in
 bytes.  hashlittle() is more complicated than hashword() only because
 hashlittle() has to dance around fitting the key bytes into registers.
--------------------------------------------------------------------
*/
uint32_t hashword(
const uint32_t *k,                   /* the key, an array of uint32_t values */
size_t          length,               /* the length of the key, in uint32_ts */
uint32_t        initval)         /* the previous hash, or an arbitrary value */
{
  uint32_t a,b,c;

  /* Set up the internal state */
  a = b = c = 0xdeadbeef + (((uint32_t)length)<<2) + initval;

  /*------------------------------------------------- handle most of the key */
  while (length > 3)
  {
    a += k[0];
    b += k[1];
    c += k[2];
    mix(a,b,c);
    length -= 3;
    k += 3;
  }

  /*------------------------------------------- handle the last 3 uint32_t's */
  switch(length)                     /* all the case statements fall through */
  { 
  case 3 : c+=k[2];
  case 2 : b+=k[1];
  case 1 : a+=k[0];
    final(a,b,c);
  case 0:     /* case 0: nothing left to add */
    break;
  }
  /*------------------------------------------------------ report the result */
  return c;
}


/*
--------------------------------------------------------------------
hashword2() -- same as hashword(), but take two seeds and return two
32-bit values.  pc and pb must both be nonnull, and *pc and *pb must
both be initialized with seeds.  If you pass in (*pb)==0, the output 
(*pc) will be the same as the return value from hashword().
--------------------------------------------------------------------
*/
void hashword2 (
const uint32_t *k,                   /* the key, an array of uint32_t values */
size_t          length,               /* the length of the key, in uint32_ts */
uint32_t       *pc,                      /* IN: seed OUT: primary hash value */
uint32_t       *pb)               /* IN: more seed OUT: secondary hash value */
{
  uint32_t a,b,c;

  /* Set up the internal state */
  a = b = c = 0xdeadbeef + ((uint32_t)(length<<2)) + *pc;
  c += *pb;

  /*------------------------------------------------- handle most of the key */
  while (length > 3)
  {
    a += k[0];
    b += k[1];
    c += k[2];
    mix(a,b,c);
    length -= 3;
    k += 3;
  }

  /*------------------------------------------- handle the last 3 uint32_t's */
  switch(length)                     /* all the case statements fall through */
  { 
  case 3 : c+=k[2];
  case 2 : b+=k[1];
  case 1 : a+=k[0];
    final(a,b,c);
  case 0:     /* case 0: nothing left to add */
    break;
  }
  /*------------------------------------------------------ report the result */
  *pc=c; *pb=b;
}


/*
-------------------------------------------------------------------------------
hashlittle() -- hash a variable-length key into a 32-bit value
  k       : the key (the unaligned variable-length array of bytes)
  length  : the length of the key, counting by bytes
  initval : can be any 4-byte value
Returns a 32-bit value.  Every bit of the key affects every bit of
the return value.  Two keys differing by one or two bits will have
totally different hash values.

The best hash table sizes are powers of 2.  There is no need to do
mod a prime (mod is sooo slow!).  If you need less than 32 bits,
use a bitmask.  For example, if you need only 10 bits, do
  h = (h & hashmask(10));
In which case, the hash table should have hashsize(10) elements.

If you are hashing n strings (uint8_t **)k, do it like this:
  for (i=0, h=0; i<n; ++i) h = hashlittle( k[i], len[i], h);

By Bob Jenkins, 2006.  bob_jenkins@burtleburtle.net.  You may use this
code any way you wish, private, educational, or commercial.  It's free.

Use for hash table lookup, or anything where one collision in 2^^32 is
acceptable.  Do NOT use for cryptographic purposes.
-------------------------------------------------------------------------------
*/

uint32_t hashlittle( const void *key, size_t length, uint32_t initval)
{
  uint32_t a,b,c;                                          /* internal state */
  union { const void *ptr; size_t i; } u;     /* needed for Mac Powerbook G4 */

  /* Set up the internal state */
  a = b = c = 0xdeadbeef + ((uint32_t)length) + initval;

  u.ptr = key;
  if (HASH_LITTLE_ENDIAN && ((u.i & 0x3) == 0)) {
    const uint32_t *k = (const uint32_t *)key;         /* read 32-bit chunks */
    const uint8_t  *k8;

    /*------ all but last block: aligned reads and affect 32 bits of (a,b,c) */
    while (length > 12)
    {
      a += k[0];
      b += k[1];
      c += k[2];
      mix(a,b,c);
      length -= 12;
      k += 3;
    }

    /*----------------------------- handle the last (probably partial) block */
    /* 
     * "k[2]&0xffffff" actually reads beyond the end of the string, but
     * then masks off the part it's not allowed to read.  Because the
     * string is aligned, the masked-off tail is in the same word as the
     * rest of the string.  Every machine with memory protection I've seen
     * does it on word boundaries, so is OK with this.  But VALGRIND will
     * still catch it and complain.  The masking trick does make the hash
     * noticably faster for short strings (like English words).
     */
#ifndef VALGRIND

    switch(length)
    {
    case 12: c+=k[2]; b+=k[1]; a+=k[0]; break;
    case 11: c+=k[2]&0xffffff; b+=k[1]; a+=k[0]; break;
    case 10: c+=k[2]&0xffff; b+=k[1]; a+=k[0]; break;
    case 9 : c+=k[2]&0xff; b+=k[1]; a+=k[0]; break;
    case 8 : b+=k[1]; a+=k[0]; break;
    case 7 : b+=k[1]&0xffffff; a+=k[0]; break;
    case 6 : b+=k[1]&0xffff; a+=k[0]; break;
    case 5 : b+=k[1]&0xff; a+=k[0]; break;
    case 4 : a+=k[0]; break;
    case 3 : a+=k[0]&0xffffff; break;
    case 2 : a+=k[0]&0xffff; break;
    case 1 : a+=k[0]&0xff; break;
    case 0 : return c;              /* zero length strings require no mixing */
    }

#else /* make valgrind happy */

    k8 = (const uint8_t *)k;
    switch(length)
    {
    case 12: c+=k[2]; b+=k[1]; a+=k[0]; break;
    case 11: c+=((uint32_t)k8[10])<<16;  /* fall through */
    case 10: c+=((uint32_t)k8[9])<<8;    /* fall through */
    case 9 : c+=k8[8];                   /* fall through */
    case 8 : b+=k[1]; a+=k[0]; break;
    case 7 : b+=((uint32_t)k8[6])<<16;   /* fall through */
    case 6 : b+=((uint32_t)k8[5])<<8;    /* fall through */
    case 5 : b+=k8[4];                   /* fall through */
    case 4 : a+=k[0]; break;
    case 3 : a+=((uint32_t)k8[2])<<16;   /* fall through */
    case 2 : a+=((uint32_t)k8[1])<<8;    /* fall through */
    case 1 : a+=k8[0]; break;
    case 0 : return c;
    }

#endif /* !valgrind */

  } else if (HASH_LITTLE_ENDIAN && ((u.i & 0x1) == 0)) {
    const uint16_t *k = (const uint16_t *)key;         /* read 16-bit chunks */
    const uint8_t  *k8;

    /*--------------- all but last block: aligned reads and different mixing */
    while (length > 12)
    {
      a += k[0] + (((uint32_t)k[1])<<16);
      b += k[2] + (((uint32_t)k[3])<<16);
      c += k[4] + (((uint32_t)k[5])<<16);
      mix(a,b,c);
      length -= 12;
      k += 6;
    }

    /*----------------------------- handle the last (probably partial) block */
    k8 = (const uint8_t *)k;
    switch(length)
    {
    case 12: c+=k[4]+(((uint32_t)k[5])<<16);
             b+=k[2]+(((uint32_t)k[3])<<16);
             a+=k[0]+(((uint32_t)k[1])<<16);
             break;
    case 11: c+=((uint32_t)k8[10])<<16;     /* fall through */
    case 10: c+=k[4];
             b+=k[2]+(((uint32_t)k[3])<<16);
             a+=k[0]+(((uint32_t)k[1])<<16);
             break;
    case 9 : c+=k8[8];                      /* fall through */
    case 8 : b+=k[2]+(((uint32_t)k[3])<<16);
             a+=k[0]+(((uint32_t)k[1])<<16);
             break;
    case 7 : b+=((uint32_t)k8[6])<<16;      /* fall through */
    case 6 : b+=k[2];
             a+=k[0]+(((uint32_t)k[1])<<16);
             break;
    case 5 : b+=k8[4];                      /* fall through */
    case 4 : a+=k[0]+(((uint32_t)k[1])<<16);
             break;
    case 3 : a+=((uint32_t)k8[2])<<16;      /* fall through */
    case 2 : a+=k[0];
             break;
    case 1 : a+=k8[0];
             break;
    case 0 : return c;                     /* zero length requires no mixing */
    }

  } else {                        /* need to read the key one byte at a time */
    const uint8_t *k = (const uint8_t *)key;

    /*--------------- all but the last block: affect some 32 bits of (a,b,c) */
    while (length > 12)
    {
      a += k[0];
      a += ((uint32_t)k[1])<<8;
      a += ((uint32_t)k[2])<<16;
      a += ((uint32_t)k[3])<<24;
      b += k[4];
      b += ((uint32_t)k[5])<<8;
      b += ((uint32_t)k[6])<<16;
      b += ((uint32_t)k[7])<<24;
      c += k[8];
      c += ((uint32_t)k[9])<<8;
      c += ((uint32_t)k[10])<<16;
      c += ((uint32_t)k[11])<<24;
      mix(a,b,c);
      length -= 12;
      k += 12;
    }

    /*-------------------------------- last block: affect all 32 bits of (c) */
    switch(length)                   /* all the case statements fall through */
    {
    case 12: c+=((uint32_t)k[11])<<24;
    case 11: c+=((uint32_t)k[10])<<16;
    case 10: c+=((uint32_t)k[9])<<8;
    case 9 : c+=k[8];
    case 8 : b+=((uint32_t)k[7])<<24;
    case 7 : b+=((uint32_t)k[6])<<16;
    case 6 : b+=((uint32_t)k[5])<<8;
    case 5 : b+=k[4];
    case 4 : a+=((uint32_t)k[3])<<24;
    case 3 : a+=((uint32_t)k[2])<<16;
    case 2 : a+=((uint32_t)k[1])<<8;
    case 1 : a+=k[0];
             break;
    case 0 : return c;
    }
  }

  final(a,b,c);
  return c;
}


/*
 * hashlittle2: return 2 32-bit hash values
 *
 * This is identical to hashlittle(), except it returns two 32-bit hash
 * values instead of just one.  This is good enough for hash table
 * lookup with 2^^64 buckets, or if you want a second hash if you're not
 * happy with the first, or if you want a probably-unique 64-bit ID for
 * the key.  *pc is better mixed than *pb, so use *pc first.  If you want
 * a 64-bit value do something like "*pc + (((uint64_t)*pb)<<32)".
 */
void hashlittle2( 
  const void *key,       /* the key to hash */
  size_t      length,    /* length of the key */
  uint32_t   *pc,        /* IN: primary initval, OUT: primary hash */
  uint32_t   *pb)        /* IN: secondary initval, OUT: secondary hash */
{
  uint32_t a,b,c;                                          /* internal state */
  union { const void *ptr; size_t i; } u;     /* needed for Mac Powerbook G4 */

  /* Set up the internal state */
  a = b = c = 0xdeadbeef + ((uint32_t)length) + *pc;
  c += *pb;

  u.ptr = key;
  if (HASH_LITTLE_ENDIAN && ((u.i & 0x3) == 0)) {
    const uint32_t *k = (const uint32_t *)key;         /* read 32-bit chunks */
    const uint8_t  *k8;

    /*------ all but last block: aligned reads and affect 32 bits of (a,b,c) */
    while (length > 12)
    {
      a += k[0];
      b += k[1];
      c += k[2];
      mix(a,b,c);
      length -= 12;
      k += 3;
    }

    /*----------------------------- handle the last (probably partial) block */
    /* 
     * "k[2]&0xffffff" actually reads beyond the end of the string, but
     * then masks off the part it's not allowed to read.  Because the
     * string is aligned, the masked-off tail is in the same word as the
     * rest of the string.  Every machine with memory protection I've seen
     * does it on word boundaries, so is OK with this.  But VALGRIND will
     * still catch it and complain.  The masking trick does make the hash
     * noticably faster for short strings (like English words).
     */
#ifndef VALGRIND

    switch(length)
    {
    case 12: c+=k[2]; b+=k[1]; a+=k[0]; break;
    case 11: c+=k[2]&0xffffff; b+=k[1]; a+=k[0]; break;
    case 10: c+=k[2]&0xffff; b+=k[1]; a+=k[0]; break;
    case 9 : c+=k[2]&0xff; b+=k[1]; a+=k[0]; break;
    case 8 : b+=k[1]; a+=k[0]; break;
    case 7 : b+=k[1]&0xffffff; a+=k[0]; break;
    case 6 : b+=k[1]&0xffff; a+=k[0]; break;
    case 5 : b+=k[1]&0xff; a+=k[0]; break;
    case 4 : a+=k[0]; break;
    case 3 : a+=k[0]&0xffffff; break;
    case 2 : a+=k[0]&0xffff; break;
    case 1 : a+=k[0]&0xff; break;
    case 0 : *pc=c; *pb=b; return;  /* zero length strings require no mixing */
    }

#else /* make valgrind happy */

    k8 = (const uint8_t *)k;
    switch(length)
    {
    case 12: c+=k[2]; b+=k[1]; a+=k[0]; break;
    case 11: c+=((uint32_t)k8[10])<<16;  /* fall through */
    case 10: c+=((uint32_t)k8[9])<<8;    /* fall through */
    case 9 : c+=k8[8];                   /* fall through */
    case 8 : b+=k[1]; a+=k[0]; break;
    case 7 : b+=((uint32_t)k8[6])<<16;   /* fall through */
    case 6 : b+=((uint32_t)k8[5])<<8;    /* fall through */
    case 5 : b+=k8[4];                   /* fall through */
    case 4 : a+=k[0]; break;
    case 3 : a+=((uint32_t)k8[2])<<16;   /* fall through */
    case 2 : a+=((uint32_t)k8[1])<<8;    /* fall through */
    case 1 : a+=k8[0]; break;
    case 0 : *pc=c; *pb=b; return;  /* zero length strings require no mixing */
    }

#endif /* !valgrind */

  } else if (HASH_LITTLE_ENDIAN && ((u.i & 0x1) == 0)) {
    const uint16_t *k = (const uint16_t *)key;         /* read 16-bit chunks */
    const uint8_t  *k8;

    /*--------------- all but last block: aligned reads and different mixing */
    while (length > 12)
    {
      a += k[0] + (((uint32_t)k[1])<<16);
      b += k[2] + (((uint32_t)k[3])<<16);
      c += k[4] + (((uint32_t)k[5])<<16);
      mix(a,b,c);
      length -= 12;
      k += 6;
    }

    /*----------------------------- handle the last (probably partial) block */
    k8 = (const uint8_t *)k;
    switch(length)
    {
    case 12: c+=k[4]+(((uint32_t)k[5])<<16);
             b+=k[2]+(((uint32_t)k[3])<<16);
             a+=k[0]+(((uint32_t)k[1])<<16);
             break;
    case 11: c+=((uint32_t)k8[10])<<16;     /* fall through */
    case 10: c+=k[4];
             b+=k[2]+(((uint32_t)k[3])<<16);
             a+=k[0]+(((uint32_t)k[1])<<16);
             break;
    case 9 : c+=k8[8];                      /* fall through */
    case 8 : b+=k[2]+(((uint32_t)k[3])<<16);
             a+=k[0]+(((uint32_t)k[1])<<16);
             break;
    case 7 : b+=((uint32_t)k8[6])<<16;      /* fall through */
    case 6 : b+=k[2];
             a+=k[0]+(((uint32_t)k[1])<<16);
             break;
    case 5 : b+=k8[4];                      /* fall through */
    case 4 : a+=k[0]+(((uint32_t)k[1])<<16);
             break;
    case 3 : a+=((uint32_t)k8[2])<<16;      /* fall through */
    case 2 : a+=k[0];
             break;
    case 1 : a+=k8[0];
             break;
    case 0 : *pc=c; *pb=b; return;  /* zero length strings require no mixing */
    }

  } else {                        /* need to read the key one byte at a time */
    const uint8_t *k = (const uint8_t *)key;

    /*--------------- all but the last block: affect some 32 bits of (a,b,c) */
    while (length > 12)
    {
      a += k[0];
      a += ((uint32_t)k[1])<<8;
      a += ((uint32_t)k[2])<<16;
      a += ((uint32_t)k[3])<<24;
      b += k[4];
      b += ((uint32_t)k[5])<<8;
      b += ((uint32_t)k[6])<<16;
      b += ((uint32_t)k[7])<<24;
      c += k[8];
      c += ((uint32_t)k[9])<<8;
      c += ((uint32_t)k[10])<<16;
      c += ((uint32_t)k[11])<<24;
      mix(a,b,c);
      length -= 12;
      k += 12;
    }

    /*-------------------------------- last block: affect all 32 bits of (c) */
    switch(length)                   /* all the case statements fall through */
    {
    case 12: c+=((uint32_t)k[11])<<24;
    case 11: c+=((uint32_t)k[10])<<16;
    case 10: c+=((uint32_t)k[9])<<8;
    case 9 : c+=k[8];
    case 8 : b+=((uint32_t)k[7])<<24;
    case 7 : b+=((uint32_t)k[6])<<16;
    case 6 : b+=((uint32_t)k[5])<<8;
    case 5 : b+=k[4];
    case 4 : a+=((uint32_t)k[3])<<24;
    case 3 : a+=((uint32_t)k[2])<<16;
    case 2 : a+=((uint32_t)k[1])<<8;
    case 1 : a+=k[0];
             break;
    case 0 : *pc=c; *pb=b; return;  /* zero length strings require no mixing */
    }
  }

  final(a,b,c);
  *pc=c; *pb=b;
}



/*
 * hashbig():
 * This is the same as hashword() on big-endian machines.  It is different
 * from hashlittle() on all machines.  hashbig() takes advantage of
 * big-endian byte ordering. 
 */
uint32_t hashbig( const void *key, size_t length, uint32_t initval)
{
  uint32_t a,b,c;
  union { const void *ptr; size_t i; } u; /* to cast key to (size_t) happily */

  /* Set up the internal state */
  a = b = c = 0xdeadbeef + ((uint32_t)length) + initval;

  u.ptr = key;
  if (HASH_BIG_ENDIAN && ((u.i & 0x3) == 0)) {
    const uint32_t *k = (const uint32_t *)key;         /* read 32-bit chunks */
    const uint8_t  *k8;

    /*------ all but last block: aligned reads and affect 32 bits of (a,b,c) */
    while (length > 12)
    {
      a += k[0];
      b += k[1];
      c += k[2];
      mix(a,b,c);
      length -= 12;
      k += 3;
    }

    /*----------------------------- handle the last (probably partial) block */
    /* 
     * "k[2]<<8" actually reads beyond the end of the string, but
     * then shifts out the part it's not allowed to read.  Because the
     * string is aligned, the illegal read is in the same word as the
     * rest of the string.  Every machine with memory protection I've seen
     * does it on word boundaries, so is OK with this.  But VALGRIND will
     * still catch it and complain.  The masking trick does make the hash
     * noticably faster for short strings (like English words).
     */
#ifndef VALGRIND

    switch(length)
    {
    case 12: c+=k[2]; b+=k[1]; a+=k[0]; break;
    case 11: c+=k[2]&0xffffff00; b+=k[1]; a+=k[0]; break;
    case 10: c+=k[2]&0xffff0000; b+=k[1]; a+=k[0]; break;
    case 9 : c+=k[2]&0xff000000; b+=k[1]; a+=k[0]; break;
    case 8 : b+=k[1]; a+=k[0]; break;
    case 7 : b+=k[1]&0xffffff00; a+=k[0]; break;
    case 6 : b+=k[1]&0xffff0000; a+=k[0]; break;
    case 5 : b+=k[1]&0xff000000; a+=k[0]; break;
    case 4 : a+=k[0]; break;
    case 3 : a+=k[0]&0xffffff00; break;
    case 2 : a+=k[0]&0xffff0000; break;
    case 1 : a+=k[0]&0xff000000; break;
    case 0 : return c;              /* zero length strings require no mixing */
    }

#else  /* make valgrind happy */

    k8 = (const uint8_t *)k;
    switch(length)                   /* all the case statements fall through */
    {
    case 12: c+=k[2]; b+=k[1]; a+=k[0]; break;
    case 11: c+=((uint32_t)k8[10])<<8;  /* fall through */
    case 10: c+=((uint32_t)k8[9])<<16;  /* fall through */
    case 9 : c+=((uint32_t)k8[8])<<24;  /* fall through */
    case 8 : b+=k[1]; a+=k[0]; break;
    case 7 : b+=((uint32_t)k8[6])<<8;   /* fall through */
    case 6 : b+=((uint32_t)k8[5])<<16;  /* fall through */
    case 5 : b+=((uint32_t)k8[4])<<24;  /* fall through */
    case 4 : a+=k[0]; break;
    case 3 : a+=((uint32_t)k8[2])<<8;   /* fall through */
    case 2 : a+=((uint32_t)k8[1])<<16;  /* fall through */
    case 1 : a+=((uint32_t)k8[0])<<24; break;
    case 0 : return c;
    }

#endif /* !VALGRIND */

  } else {                        /* need to read the key one byte at a time */
    const uint8_t *k = (const uint8_t *)key;

    /*--------------- all but the last block: affect some 32 bits of (a,b,c) */
    while (length > 12)
    {
      a += ((uint32_t)k[0])<<24;
      a += ((uint32_t)k[1])<<16;
      a += ((uint32_t)k[2])<<8;
      a += ((uint32_t)k[3]);
      b += ((uint32_t)k[4])<<24;
      b += ((uint32_t)k[5])<<16;
      b += ((uint32_t)k[6])<<8;
      b += ((uint32_t)k[7]);
      c += ((uint32_t)k[8])<<24;
      c += ((uint32_t)k[9])<<16;
      c += ((uint32_t)k[10])<<8;
      c += ((uint32_t)k[11]);
      mix(a,b,c);
      length -= 12;
      k += 12;
    }

    /*-------------------------------- last block: affect all 32 bits of (c) */
    switch(length)                   /* all the case statements fall through */
    {
    case 12: c+=k[11];
    case 11: c+=((uint32_t)k[10])<<8;
    case 10: c+=((uint32_t)k[9])<<16;
    case 9 : c+=((uint32_t)k[8])<<24;
    case 8 : b+=k[7];
    case 7 : b+=((uint32_t)k[6])<<8;
    case 6 : b+=((uint32_t)k[5])<<16;
    case 5 : b+=((uint32_t)k[4])<<24;
    case 4 : a+=k[3];
    case 3 : a+=((uint32_t)k[2])<<8;
    case 2 : a+=((uint32_t)k[1])<<16;
    case 1 : a+=((uint32_t)k[0])<<24;
             break;
    case 0 : return c;
    }
  }

  final(a,b,c);
  return c;
}

FD_EXPORT
/* fd_hash_dtype_rep2:
     Arguments: a list pointer
     Returns: an unsigned int
  This is a better hashing algorithm than the legacy used for years.
*/
unsigned int fd_hash_dtype_rep2(fdtype x)
{
  struct FD_BYTE_OUTPUT out; unsigned int len, word_len, gap, hashval=0;
  FD_INIT_BYTE_OUTPUT(&out,1024,NULL);
  fd_write_dtype(&out,x);
#if (WORDS_BIGENDIAN)
  hashval=hashbig(out.start,out.ptr-out.start,0);
#else
  hashval=hashlittle(out.start,out.ptr-out.start,0);
#endif
  u8_free(out.start);
  return hashval;
}

/* Yet another hash function */

 unsigned int joaat_hash(unsigned char *key, size_t key_len)
 {
     unsigned int hash = 0;
     size_t i;
     
     for (i = 0; i < key_len; i++) {
         hash += key[i];
         hash += (hash << 10);
         hash ^= (hash >> 6);
     }
     hash += (hash << 3);
     hash ^= (hash >> 11);
     hash += (hash << 15);
     return hash;
 }

FD_EXPORT
/* fd_hash_dtype_rep3:
     Arguments: a list pointer
     Returns: an unsigned int
  This is a better hashing algorithm than the legacy used for years.
*/
unsigned int fd_hash_dtype_rep3(fdtype x)
{
  struct FD_BYTE_OUTPUT out; unsigned int len, word_len, gap, hashval=0;
  FD_INIT_BYTE_OUTPUT(&out,1024,NULL);
  fd_write_dtype(&out,x);
  hashval=joaat_hash(out.start,out.ptr-out.start);
  u8_free(out.start);
  return hashval;
}

/* Initialization function */

FD_EXPORT fd_init_hashdtype_c()
{
  fd_register_source_file(versionid);
}


/* File specific stuff */

/* Emacs local variables
;;;  Local variables: ***
;;;  compile-command: "cd ../..; make" ***
;;;  End: ***
*/
