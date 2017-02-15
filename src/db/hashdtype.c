/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/dbfile.h"

/* Used to generate hash codes */
#define MAGIC_MODULUS 16777213 /* 256000001 */
#define MIDDLIN_MODULUS 573786077 /* 256000001 */
#define MYSTERIOUS_MODULUS 2000239099 /* 256000001 */

/** Hashing lisp objects **/

FD_FASTOP unsigned int hash_string_dtype1 (fdtype x)
{
  const char *ptr=FD_STRDATA(x), *limit=ptr+FD_STRLEN(x);
  unsigned int sum=0;
  while (ptr < limit) {
    sum=(sum<<8)+(*ptr++);
    sum=sum%(MAGIC_MODULUS);}
  return sum;
}

static unsigned int hash_unicode_string_dtype1 (fdtype x)
{
  const unsigned char *ptr=FD_STRDATA(x), *limit=ptr+FD_STRLEN(x);
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
  const unsigned char *data=FD_PACKET_DATA(x);
  const unsigned char *ptr=data, *limit=ptr+size;
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
  const unsigned char *s = FD_SYMBOL_NAME (x);
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
  else if (FD_STRINGP(x)) {
    const u8_byte *scan=FD_STRDATA(x), *lim=scan+FD_STRLEN(x);
    while (scan<lim)
      if (*scan>=0x80) return hash_unicode_string_dtype1(x);
      else scan++;
    return hash_string_dtype1(x);}
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
    return hash_dtype1(qc->fd_choiceval);}
  else if (FD_CHARACTERP(x)) return (FD_CHARCODE(x))%(MAGIC_MODULUS);
  else if (FD_FLONUMP(x)) {
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
    const struct FD_KEYVAL *scan=sm->sm_keyvals;
    const struct FD_KEYVAL *limit=scan+FD_XSLOTMAP_SIZE(sm);
    while (scan<limit) {
      sum=(sum+hash_dtype1(scan->fd_kvkey))%MAGIC_MODULUS;
      sum=(sum+(fd_flip_word(hash_dtype1(scan->fd_keyval))%MAGIC_MODULUS))%MAGIC_MODULUS;
      scan++;}
    return sum;}
  else if (FD_TYPEP(x,fd_rational_type)) {
    struct FD_PAIR *p=FD_CONSPTR(fd_pair,x);
    unsigned int sum=hash_dtype1(p->fd_car)%MAGIC_MODULUS;
    sum=(sum<<4)+hash_dtype1(p->fd_cdr)%MAGIC_MODULUS;
    return sum%MAGIC_MODULUS;}
  else if (FD_TYPEP(x,fd_complex_type)) {
    struct FD_PAIR *p=FD_CONSPTR(fd_pair,x);
    unsigned int sum=hash_dtype1(p->fd_car)%MAGIC_MODULUS;
    sum=(sum<<4)+hash_dtype1(p->fd_cdr)%MAGIC_MODULUS;
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

typedef unsigned long long ull;

FD_FASTOP unsigned int hash_mult(unsigned int x,unsigned int y)
{
  unsigned long long prod=((ull)x)*((ull)y);
  return (prod*2100000523)%(MYSTERIOUS_MODULUS);
}

FD_FASTOP unsigned int hash_combine(unsigned int x,unsigned int y)
{
  if ((x == 0) && (y == 0)) return MAGIC_MODULUS+2;
  else if ((x == 0) || (y == 0))
    return x+y;
  else return hash_mult(x,y);
}

FD_FASTOP unsigned int mult_hash_string(const unsigned char *start,int len)
{
  unsigned int prod=1, asint=0;
  const unsigned char *ptr=start, *limit=ptr+len;
  /* Compute a starting place */
  while (ptr < limit) prod=prod+*ptr++;
  /* Now do a multiplication */
  ptr=start; limit=ptr+((len%4) ? (4*(len/4)) : (len));
  while (ptr < limit) {
    /* This could be optimized for endian-ness */
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
  if (FD_IMMEDIATEP(x)) {
    if ((FD_EMPTY_LISTP(x)) || (FD_FALSEP(x))) return 37;
    else if (FD_TRUEP(x)) return 17;
    else if (FD_EMPTY_CHOICEP(x)) return 13;
    else if (FD_SYMBOLP(x))
      return hash_symbol_dtype2(x);
    else if (FD_CHARACTERP(x))
      return (FD_CHARCODE(x))%(MAGIC_MODULUS);
    else return 19;}
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
  else { /*  if (FD_CONSP(x)) */
    int ctype=FD_PTR_TYPE(x);
    switch (ctype) {
    case fd_string_type:
      return hash_string_dtype2(x);
    case fd_packet_type: case fd_secret_type:
      return hash_packet(x);
    case fd_pair_type:
      return hash_pair_dtype2(x);
    case fd_qchoice_type: {
      struct FD_QCHOICE *qc=FD_XQCHOICE(x);
      return hash_dtype2(qc->fd_choiceval);}
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
      const struct FD_KEYVAL *scan=sm->sm_keyvals;
      const struct FD_KEYVAL *limit=scan+FD_XSLOTMAP_SIZE(sm);
      while (scan<limit) {
        unsigned int prod=
          hash_combine(hash_dtype2(scan->fd_kvkey),hash_dtype2(scan->fd_keyval));
        sum=(sum+prod)%(MYSTERIOUS_MODULUS);
        scan++;}
      return sum;}
    case fd_rational_type: {
      struct FD_PAIR *p=FD_CONSPTR(fd_pair,x);
      return hash_combine(hash_dtype2(p->fd_car),hash_dtype2(p->fd_cdr));}
    case fd_complex_type: {
      struct FD_PAIR *p=FD_CONSPTR(fd_pair,x);
      return hash_combine(hash_dtype2(p->fd_car),hash_dtype2(p->fd_cdr));}
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
  const unsigned char *s = FD_SYMBOL_NAME (x);
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
  if (FD_IMMEDIATEP(x)) {
    if ((FD_EMPTY_LISTP(x)) || (FD_FALSEP(x))) return 37;
    else if (FD_TRUEP(x)) return 17;
    else if (FD_EMPTY_CHOICEP(x)) return 13;
    else if (FD_SYMBOLP(x))
      return hash_symbol_dtype2(x);
    else if (FD_CHARACTERP(x))
      return (FD_CHARCODE(x))%(MAGIC_MODULUS);
    else return 19;}
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
  else { /*  if (FD_CONSP(x)) */
    int ctype=FD_PTR_TYPE(x);
    switch (ctype) {
    case fd_string_type:
      return hash_string_dtype2(x);
    case fd_packet_type: case fd_secret_type:
      return hash_packet(x);
    case fd_pair_type:
      return hash_pair_dtype3(x);
    case fd_qchoice_type: {
      struct FD_QCHOICE *qc=FD_XQCHOICE(x);
      return hash_dtype3(qc->fd_choiceval);}
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
      const struct FD_KEYVAL *scan=sm->sm_keyvals;
      const struct FD_KEYVAL *limit=scan+FD_XSLOTMAP_SIZE(sm);
      while (scan<limit) {
        unsigned int prod=
          hash_combine(hash_dtype3(scan->fd_kvkey),hash_dtype3(scan->fd_keyval));
        sum=(sum+prod)%(MYSTERIOUS_MODULUS);
        scan++;}
      return sum;}
    case fd_rational_type: {
      struct FD_PAIR *p=FD_CONSPTR(fd_pair,x);
      return hash_combine(hash_dtype3(p->fd_car),hash_dtype3(p->fd_cdr));}
    case fd_complex_type: {
      struct FD_PAIR *p=FD_CONSPTR(fd_pair,x);
      return hash_combine(hash_dtype3(p->fd_car),hash_dtype3(p->fd_cdr));}
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
  FD_INIT_BYTE_OUTPUT(&out,1024);
  fd_write_dtype(&out,x);
  hashval=mult_hash_string(out.fd_bufstart,out.fd_bufptr-out.fd_bufstart);
  u8_free(out.fd_bufstart);
  return hashval;
}

/* Initialization function */

FD_EXPORT void fd_init_hashdtype_c()
{
  u8_register_source_file(_FILEINFO);
}


/* File specific stuff */

/* Emacs local variables
;;;  Local variables: ***
;;;  compile-command: "cd ../..; make" ***
;;;  End: ***
*/

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
