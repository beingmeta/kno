/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id: hashdtype.c,v 1.16 2006/01/26 14:44:32 haase Exp $";

#define U8_INLINE_IO 1

#include "fdb/dtype.h"
#include "fdb/dbfile.h"

/* Used to generate hash codes */
#define MAGIC_MODULUS 16777213 /* 256000001 */
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
    return id%(MYSTERIOUS_MODULUS);
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
  else if (FD_PRIM_TYPEP(x,fd_double_type)) {
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
  else if (FD_PRIM_TYPEP(x,fd_rational_type)) {
    struct FD_PAIR *p=FD_STRIP_CONS(x,fd_rational_type,struct FD_PAIR *);
    unsigned int sum=hash_dtype1(p->car)%MAGIC_MODULUS;
    sum=(sum<<4)+hash_dtype1(p->cdr)%MAGIC_MODULUS;
    return sum%MAGIC_MODULUS;}
  else if (FD_PRIM_TYPEP(x,fd_complex_type)) {
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

static unsigned int hash_symbol_dtype2(fdtype x);
static unsigned int hash_pair_dtype2(fdtype x);
static unsigned int hash_record_dtype2(fdtype x);

typedef unsigned long long ull;

static unsigned int hash_mult(unsigned int x,unsigned int y)
{
  unsigned long long prod=((ull)x)*((ull)y);
  return (prod*2100000523)%(MYSTERIOUS_MODULUS);
}

static unsigned int non_hash_mult(unsigned int x,unsigned int y)
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
      hi=((hi<<8)|(lo>>24))%(MYSTERIOUS_MODULUS); lo=lo<<8;}
    return hi;}
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


FD_EXPORT fd_init_hashdtype_c()
{
  fd_register_source_file(versionid);
}


/* File specific stuff */

/* The CVS log for this file
   $Log: hashdtype.c,v $
   Revision 1.16  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.15  2005/12/20 00:55:39  haase
   Fixes to packet and double hashing

   Revision 1.14  2005/12/19 19:23:56  haase
   Added more hashing functions

   Revision 1.13  2005/12/19 18:47:32  haase
   Added choice hashing

   Revision 1.12  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.11  2005/07/13 21:39:31  haase
   XSLOTMAP/XSCHEMAP renaming

   Revision 1.10  2005/05/17 20:30:27  haase
   Fixed bug in the legacy dtype hash implementation

   Revision 1.9  2005/03/03 17:58:15  haase
   Moved stdio dependencies out of fddb and reorganized make structure

   Revision 1.8  2005/02/15 14:51:38  haase
   Added declarations to inline some U8 stream ops

   Revision 1.7  2005/02/11 02:51:14  haase
   Added in-file CVS logs

   Revision 1.6  2005/02/11 02:34:33  haase
   Added source file tracking

   Revision 1.5  2005/02/09 15:50:32  haase
   Added copyright statements and file version ids

   Revision 1.4  2004/09/19 00:04:42  haase
   Updated to use fragmented libu8 libraries

   Revision 1.3  2004/09/14 18:37:55  haase
   More fixes to index prefetching

   Revision 1.2  2004/09/09 03:44:45  haase
   Added indices.h header for hashdtypes

   Revision 1.1  2004/09/09 03:41:53  haase
   Added initial file index code

   Revision 1.18  2004/07/20 17:57:48  haase
   New hash algorith constants

   Revision 1.17  2004/07/20 09:16:13  haase
   Updated copyright year, removed dead code, and cleaned up some whitespace

   Revision 1.16  2004/07/16 17:51:58  haase
   Fix doc header for hash functions

   Revision 1.15  2004/07/16 16:43:41  haase
   Made OIDs be long longs if they're big enough

   Revision 1.14  2004/05/03 22:49:22  haase
   New portahash function

   Revision 1.3  2004/04/26 11:15:50  haase
   Patches to index hash patches

   Revision 1.2  2004/04/25 23:10:29  haase
   Fixed fixed point in the new hashing algorithm

   Revision 1.1.1.1  2004/04/06 11:15:24  haase
   Initial import of proprietary FramerD into beingmeta CVS

   Revision 1.13  2004/03/31 17:44:46  haase
   Removed left over irreleveant comment

   Revision 1.12  2004/03/30 11:32:15  haase
   Renamed mult_hash functions

   Revision 1.11  2004/03/14 00:29:58  haase
   Fixed degenerate case of new hash algorithm

   Revision 1.10  2004/03/13 00:08:56  haase
   New improved hashing algorithm for a new kind of index better in general and especially better suited to very large indices

   Revision 1.9  2004/03/12 20:30:26  haase
   Added new kind of index with multiplicative hash function more appropriate to very large indices

   Revision 1.8  2003/08/27 10:53:30  haase
   Merged 2.4 patches into trunk, started 2.5

   Revision 1.7.2.1  2003/08/06 18:41:07  haase
   Updated copyright notices to 2003

   Revision 1.7  2002/04/19 13:19:52  haase
   Fixed bugs involving NULs in UTF-8 strings

   Revision 1.6  2002/04/02 21:39:33  haase
   Added log and emacs init entries to C source files

*/

/* Emacs local variables
;;;  Local variables: ***
;;;  compile-command: "cd ../..; make" ***
;;;  End: ***
*/
