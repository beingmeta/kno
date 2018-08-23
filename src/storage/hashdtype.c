/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1
#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

/* Used to generate hash codes */
#define MAGIC_MODULUS 16777213 /* 256000001 */
#define MIDDLIN_MODULUS 573786077 /* 256000001 */
#define MYSTERIOUS_MODULUS 2000239099 /* 256000001 */

/** Hashing lisp objects **/

FD_FASTOP unsigned int hash_string_dtype1 (lispval x)
{
  const char *ptr = CSTRING(x), *limit = ptr+STRLEN(x);
  unsigned int sum = 0;
  while (ptr < limit) {
    sum = (sum<<8)+(*ptr++);
    sum = sum%(MAGIC_MODULUS);}
  return sum;
}

static unsigned int hash_unicode_string_dtype1 (lispval x)
{
  const unsigned char *ptr = CSTRING(x), *limit = ptr+STRLEN(x);
  unsigned int sum = 0;
  while (ptr < limit) {
    int c = u8_sgetc(&ptr);
    sum = (sum<<8)+(c);
    sum = sum%(MAGIC_MODULUS);}
  return sum;
}

static unsigned int hash_packet(lispval x)
{
  int size = FD_PACKET_LENGTH(x);
  const unsigned char *data = FD_PACKET_DATA(x);
  const unsigned char *ptr = data, *limit = ptr+size;
  unsigned int sum = 0;
  while (ptr < limit) sum = ((sum<<4)+(*ptr++))%MAGIC_MODULUS;
  return sum;
}

/* hash_symbol_dtype1: (static)
     Arguments: a dtype pointer (to a symbol)
     Returns: a hash value (an unsigned int)

  Computes an iterative hash over the characters in the symbol's name.
*/
FD_FASTOP unsigned int hash_symbol_dtype1(lispval x)
{
  const unsigned char *s = SYM_NAME (x);
  unsigned int sum = 0;
  while (*s != '\0') {
    int c;
    if (*s < 0x80) c = *s++;
    else if (*s < 0xe0) {
      c = (int)((*s++)&(0x3f)<<6);
      c = c|((int)((*s++)&(0x3f)));}
    else {
      c = (int)((*s++)&(0x1f)<<12);
      c = c|(int)(((*s++)&(0x3f))<<6);
      c = c|(int)((*s++)&(0x3f));}
    sum = (sum<<8)+(c); sum = sum%(MAGIC_MODULUS);}
  return sum;
}

static unsigned int hash_pair_dtype1(lispval string);

/*
   hash_lisp: (static inline)
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
FD_FASTOP unsigned int hash_lisp1(lispval x)
{
  if (FD_IMMEDIATEP(x))
    if ((NILP(x)) || (FALSEP(x))) return 37;
    else if (FD_TRUEP(x)) return 17;
    else if (EMPTYP(x)) return 13;
    else if (SYMBOLP(x))
      return hash_symbol_dtype1(x);
    else return 19;
  else if (FIXNUMP(x))
    return (FIX2INT(x))%(MAGIC_MODULUS);
  else if (STRINGP(x)) {
    const u8_byte *scan = CSTRING(x), *lim = scan+STRLEN(x);
    while (scan<lim)
      if (*scan>=0x80) return hash_unicode_string_dtype1(x);
      else scan++;
    return hash_string_dtype1(x);}
  else if (PACKETP(x))
    return hash_packet(x);
  else if (PAIRP(x))
    return hash_pair_dtype1(x);
  else if (SYMBOLP(x))
    return hash_symbol_dtype1(x);
  else if (OIDP(x)) {
    FD_OID id = FD_OID_ADDR(x);
#if FD_STRUCT_OIDS
    unsigned int hi = FD_OID_HI(id), lo = FD_OID_LO(id);
    int i = 0; while (i++ < 4)
      {hi = ((hi<<8)|(lo>>24))%(MAGIC_MODULUS); lo = lo<<8;}
    return hi;
#else
    return id%(MAGIC_MODULUS);
#endif
      }
  else if (CHOICEP(x)) {
    unsigned int sum = 0;
    DO_CHOICES(elt,x)
      sum = ((sum+hash_lisp1(elt))%MAGIC_MODULUS);
    return sum;}
  else if (QCHOICEP(x)) {
    struct FD_QCHOICE *qc = FD_XQCHOICE(x);
    return hash_lisp1(qc->qchoiceval);}
  else if (FD_CHARACTERP(x)) return (FD_CHARCODE(x))%(MAGIC_MODULUS);
  else if (FD_FLONUMP(x)) {
    unsigned int as_int;
    float *f = (float *)(&as_int);
    *f = FD_FLONUM(x);
    return (as_int)%(MAGIC_MODULUS);}
  else if (VECTORP(x)) {
    int size = VEC_LEN(x); unsigned int i = 0, sum = 0;
    while (i < size) {
      sum = (((sum<<4)+(hash_lisp1(VEC_REF(x,i))))%MAGIC_MODULUS);
      i++;}
    return sum;}
  else if (SLOTMAPP(x)) {
    unsigned int sum = 0;
    struct FD_SLOTMAP *sm = FD_XSLOTMAP(x);
    const struct FD_KEYVAL *scan = sm->sm_keyvals;
    const struct FD_KEYVAL *limit = scan+FD_XSLOTMAP_NUSED(sm);
    while (scan<limit) {
      sum = (sum+hash_lisp1(scan->kv_key))%MAGIC_MODULUS;
      sum = (sum+(fd_flip_word(hash_lisp1(scan->kv_val))%MAGIC_MODULUS))%MAGIC_MODULUS;
      scan++;}
    return sum;}
  else if (TYPEP(x,fd_rational_type)) {
    struct FD_PAIR *p = FD_CONSPTR(fd_pair,x);
    unsigned int sum = hash_lisp1(p->car)%MAGIC_MODULUS;
    sum = (sum<<4)+hash_lisp1(p->cdr)%MAGIC_MODULUS;
    return sum%MAGIC_MODULUS;}
  else if (TYPEP(x,fd_complex_type)) {
    struct FD_PAIR *p = FD_CONSPTR(fd_pair,x);
    unsigned int sum = hash_lisp1(p->car)%MAGIC_MODULUS;
    sum = (sum<<4)+hash_lisp1(p->cdr)%MAGIC_MODULUS;
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
static unsigned int hash_pair_dtype1(lispval x)
{
  lispval ptr = x; unsigned int sum = 0;
  /* The shift here is introduced to make the hash function asymmetric */
  while (PAIRP(ptr)) {
    sum = ((sum*2)+hash_lisp1(FD_CAR(ptr)))%(MAGIC_MODULUS);
    ptr = FD_CDR(ptr);}
  if (!(NILP(ptr))) {
    unsigned int cdr_hash = hash_lisp1(ptr);
    sum = (sum+(fd_flip_word(cdr_hash)>>8))%(MAGIC_MODULUS);}
  return sum;
}

FD_EXPORT
/* fd_hash_lisp:
     Arguments: a list pointer
     Returns: an unsigned int
  This returns an integer corresponding to its argument which will
   be the same on any platform.
*/
unsigned int fd_hash_lisp1(lispval x)
{
  return hash_lisp1(x);
}

/* Multiplier heavy hashing */

FD_FASTOP unsigned int hash_symbol_lisp2(lispval x);
FD_FASTOP unsigned int hash_pair_lisp2(lispval x);

typedef unsigned long long ull;

FD_FASTOP unsigned int hash_mult(unsigned int x,unsigned int y)
{
  unsigned long long prod = ((ull)x)*((ull)y);
  return (prod*2100000523)%(MYSTERIOUS_MODULUS);
}

FD_FASTOP unsigned int hash_combine(unsigned int x,unsigned int y)
{
  if ((x == 0) && (y == 0)) return MAGIC_MODULUS+2;
  else if ((x == 0) || (y == 0))
    return x+y;
  else return hash_mult(x,y);
}

FD_FASTOP unsigned int mult_hash_bytes(const unsigned char *start,int len)
{
  unsigned int prod = 1, asint = 0;
  const unsigned char *ptr = start, *limit = ptr+len;
  /* Compute a starting place */
  while (ptr < limit) prod = prod+*ptr++;
  /* Now do a multiplication */
  ptr = start; limit = ptr+((len%4) ? (4*(len/4)) : (len));
  while (ptr < limit) {
    /* This could be optimized for endian-ness */
    asint = (ptr[0]<<24)|(ptr[1]<<16)|(ptr[2]<<8)|(ptr[3]);
    prod = hash_combine(prod,asint); ptr = ptr+4;}
  switch (len%4) {
  case 0: asint = 1; break;
  case 1: asint = ptr[0]; break;
  case 2: asint = ptr[0]|(ptr[1]<<8); break;
  case 3: asint = ptr[0]|(ptr[1]<<8)|(ptr[2]<<16); break;}
  return hash_combine(prod,asint);
}

/* hash_string_dtype: (static)
     Arguments: a dtype pointer (to a string)
     Returns: a hash value (an unsigned int)

  Computes an iterative hash over the characters in the string.
*/
FD_FASTOP unsigned int hash_string_lisp2(lispval x)
{
  int len = STRLEN(x);
  if (len == 0) return MAGIC_MODULUS+2;
  else if (len == 1)
    if (*(CSTRING(x))) return (*(CSTRING(x)));
    else return MAGIC_MODULUS-2;
  else return mult_hash_bytes(CSTRING(x),len);
}

FD_FASTOP unsigned int hash_lisp2(lispval x)
{
  if (FD_IMMEDIATEP(x)) {
    if ((NILP(x)) || (FALSEP(x))) return 37;
    else if (FD_TRUEP(x)) return 17;
    else if (EMPTYP(x)) return 13;
    else if (SYMBOLP(x))
      return hash_symbol_lisp2(x);
    else if (FD_CHARACTERP(x))
      return (FD_CHARCODE(x))%(MAGIC_MODULUS);
    else return 19;}
  else if (FIXNUMP(x))
    return (FIX2INT(x))%(MAGIC_MODULUS);
  else if (OIDP(x)) {
    FD_OID id = FD_OID_ADDR(x);
#if FD_STRUCT_OIDS
    unsigned int hi = FD_OID_HI(id), lo = FD_OID_LO(id);
    int i = 0; while (i++ < 4) {
      hi = ((hi<<8)|(lo>>24))%(MAGIC_MODULUS); lo = lo<<8;}
    return hi;
#else
    return id%(MAGIC_MODULUS);
#endif
  }
  else { /*  if (CONSP(x)) */
    int ctype = FD_PTR_TYPE(x);
    switch (ctype) {
    case fd_string_type:
      return hash_string_lisp2(x);
    case fd_packet_type: case fd_secret_type:
      return hash_packet(x);
    case fd_pair_type:
      return hash_pair_lisp2(x);
    case fd_qchoice_type: {
      struct FD_QCHOICE *qc = FD_XQCHOICE(x);
      return hash_lisp2(qc->qchoiceval);}
    case fd_choice_type: {
      unsigned int sum = 0;
      DO_CHOICES(elt,x)
        sum = (sum+(hash_lisp2(elt))%MAGIC_MODULUS);
      return sum;}
    case fd_vector_type: {
      int i = 0, size = VEC_LEN(x); unsigned int prod = 1;
      while (i < size) {
        prod = hash_combine(prod,hash_lisp2(VEC_REF(x,i))); i++;}
      return prod;}
    case fd_slotmap_type: {
      unsigned int sum = 0;
      struct FD_SLOTMAP *sm = FD_XSLOTMAP(x);
      const struct FD_KEYVAL *scan = sm->sm_keyvals;
      const struct FD_KEYVAL *limit = scan+FD_XSLOTMAP_NUSED(sm);
      while (scan<limit) {
        unsigned int prod=
          hash_combine(hash_lisp2(scan->kv_key),hash_lisp2(scan->kv_val));
        sum = (sum+prod)%(MYSTERIOUS_MODULUS);
        scan++;}
      return sum;}
    case fd_rational_type: {
      struct FD_PAIR *p = FD_CONSPTR(fd_pair,x);
      return hash_combine(hash_lisp2(p->car),hash_lisp2(p->cdr));}
    case fd_complex_type: {
      struct FD_PAIR *p = FD_CONSPTR(fd_pair,x);
      return hash_combine(hash_lisp2(p->car),hash_lisp2(p->cdr));}
    default:
      if ((ctype<FD_TYPE_MAX) && (fd_hashfns[ctype]))
        return fd_hashfns[ctype](x,fd_hash_lisp2);
      else return 17;}}
}

/* hash_symbol_lisp2: (static)
     Arguments: a dtype pointer (to a symbol)
     Returns: a hash value (an unsigned int)

  Computes an iterative hash over the characters in the symbol's name.
*/
FD_FASTOP unsigned int hash_symbol_lisp2(lispval x)
{
  const unsigned char *s = SYM_NAME (x);
  unsigned int sum = 0;
  while (*s != '\0') {
    int c;
    if (*s < 0x80) c = *s++;
    else if (*s < 0xe0) {
      c = (int)((*s++)&(0x3f)<<6);
      c = c|((int)((*s++)&(0x3f)));}
    else {
      c = (int)((*s++)&(0x1f)<<12);
      c = c|(int)(((*s++)&(0x3f))<<6);
      c = c|(int)((*s++)&(0x3f));}
    sum = (sum<<8)+(c); sum = sum%(MAGIC_MODULUS);}
  return sum;
}

FD_EXPORT
/* fd_hash_lisp2:
     Arguments: a list pointer
     Returns: an unsigned int
  This is a better hashing algorithm than the legacy used for years.
*/
unsigned int fd_hash_lisp2(lispval x)
{
  return hash_lisp2(x);
}

/* hash_pair_lisp2: (static)
     Arguments: a dtype pointer (to a pair)
     Returns: a hash value (an unsigned int)
*/
static unsigned int hash_pair_lisp2(lispval x)
{
  lispval ptr = x; unsigned int prod = 1;
  while (PAIRP(ptr)) {
    prod = hash_combine(prod,hash_lisp2(FD_CAR(ptr)));
    ptr = FD_CDR(ptr);}
  if (!(NILP(ptr))) {
    unsigned int cdr_hash = hash_lisp2(ptr);
    prod = hash_combine(prod,cdr_hash);}
  return prod;
}

/* hash_lisp3 */

FD_FASTOP unsigned int hash_lisp3(lispval x);

/* hash_pair_dtype3: (static)
     Arguments: a dtype pointer (to a pair)
     Returns: a hash value (an unsigned int)
*/
static unsigned int hash_pair_dtype3(lispval x)
{
  lispval ptr = x; unsigned int prod = 1;
  while (PAIRP(ptr)) {
    prod = hash_combine(prod,hash_lisp3(FD_CAR(ptr)));
    ptr = FD_CDR(ptr);}
  if (!(NILP(ptr))) {
    unsigned int cdr_hash = hash_lisp3(ptr);
    prod = hash_combine(prod,cdr_hash);}
  return prod;
}

FD_FASTOP unsigned int hash_lisp3(lispval x)
{
  if (FD_IMMEDIATEP(x)) {
    if ((NILP(x)) || (FALSEP(x))) return 37;
    else if (FD_TRUEP(x)) return 17;
    else if (EMPTYP(x)) return 13;
    else if (SYMBOLP(x))
      return hash_symbol_lisp2(x);
    else if (FD_CHARACTERP(x))
      return (FD_CHARCODE(x))%(MAGIC_MODULUS);
    else return 19;}
  else if (FIXNUMP(x))
    return (FIX2INT(x))%(MAGIC_MODULUS);
  else if (OIDP(x)) {
    FD_OID id = FD_OID_ADDR(x);
#if FD_STRUCT_OIDS
    unsigned int hi = FD_OID_HI(id), lo = FD_OID_LO(id);
    int i = 0; while (i++ < 4) {
      hi = ((hi<<8)|(lo>>24))%(MYSTERIOUS_MODULUS); lo = lo<<8;}
    return hi;
#else
    return id%(MYSTERIOUS_MODULUS);
#endif
  }
  else { /*  if (CONSP(x)) */
    int ctype = FD_PTR_TYPE(x);
    switch (ctype) {
    case fd_string_type:
      return hash_string_lisp2(x);
    case fd_packet_type: case fd_secret_type:
      return hash_packet(x);
    case fd_pair_type:
      return hash_pair_dtype3(x);
    case fd_qchoice_type: {
      struct FD_QCHOICE *qc = FD_XQCHOICE(x);
      return hash_lisp3(qc->qchoiceval);}
    case fd_choice_type: {
      unsigned int sum = 0;
      DO_CHOICES(elt,x)
        sum = (sum+(hash_lisp3(elt))%MIDDLIN_MODULUS);
      return sum;}
    case fd_vector_type: {
      int i = 0, size = VEC_LEN(x); unsigned int prod = 1;
      while (i < size) {
        prod = hash_combine(prod,hash_lisp3(VEC_REF(x,i))); i++;}
      return prod;}
    case fd_slotmap_type: {
      unsigned int sum = 0;
      struct FD_SLOTMAP *sm = FD_XSLOTMAP(x);
      const struct FD_KEYVAL *scan = sm->sm_keyvals;
      const struct FD_KEYVAL *limit = scan+FD_XSLOTMAP_NUSED(sm);
      while (scan<limit) {
        unsigned int prod=
          hash_combine(hash_lisp3(scan->kv_key),hash_lisp3(scan->kv_val));
        sum = (sum+prod)%(MYSTERIOUS_MODULUS);
        scan++;}
      return sum;}
    case fd_rational_type: {
      struct FD_PAIR *p = FD_CONSPTR(fd_pair,x);
      return hash_combine(hash_lisp3(p->car),hash_lisp3(p->cdr));}
    case fd_complex_type: {
      struct FD_PAIR *p = FD_CONSPTR(fd_pair,x);
      return hash_combine(hash_lisp3(p->car),hash_lisp3(p->cdr));}
    default:
      if ((ctype<FD_TYPE_MAX) && (fd_hashfns[ctype]))
        return fd_hashfns[ctype](x,fd_hash_lisp3);
      else return 17;}}
}

FD_EXPORT
/* fd_hash_lisp3:
     Arguments: a list pointer
     Returns: an unsigned int
  This is a better hashing algorithm than the legacy used for years.
*/
unsigned int fd_hash_lisp3(lispval x)
{
  return hash_lisp3(x);
}

FD_EXPORT
/* fd_hash_dtype_rep:
     Arguments: a list pointer
     Returns: an unsigned int
  This is a better hashing algorithm than the legacy used for years.
*/
unsigned int fd_hash_dtype_rep(lispval x)
{
  FD_DECL_OUTBUF(out,1024);
  unsigned int hashval;
  fd_write_dtype(&out,x);
  hashval = mult_hash_bytes(out.buffer,out.bufwrite-out.buffer);
  fd_close_outbuf(&out);
  return hashval;
}

/* Initialization function */

FD_EXPORT void fd_init_hashdtype_c()
{
  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
