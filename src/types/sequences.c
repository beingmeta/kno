/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_CHOICES 1
#define FD_INLINE_FCNIDS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/sequences.h"
#include "framerd/numbers.h"
#include "framerd/apply.h"


#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>

#include <limits.h>

struct FD_SEQFNS *fd_seqfns[FD_TYPE_MAX];

#define FD_EQV(x,y) \
  ((FD_EQ(x,y)) || \
   ((FD_NUMBERP(x)) && (FD_NUMBERP(y)) && \
    (fd_numcompare(x,y)==0)))

#define string_start(bytes,i) ((i==0) ? (bytes) : (u8_substring(bytes,i)))

static fdtype make_float_vector(int n,fdtype *from_elts);
static fdtype make_double_vector(int n,fdtype *from_elts);
static fdtype make_short_vector(int n,fdtype *from_elts);
static fdtype make_int_vector(int n,fdtype *from_elts);
static fdtype make_long_vector(int n,fdtype *from_elts);

struct FD_SEQFNS *fd_seqfns[FD_TYPE_MAX];
FD_EXPORT int fd_seq_length(fdtype x)
{
  int ctype = FD_PTR_TYPE(x);
  switch (ctype) {
  case fd_vector_type:
    return FD_VECTOR_LENGTH(x);
  case fd_rail_type:
    return FD_RAIL_LENGTH(x);
  case fd_packet_type: case fd_secret_type:
    return FD_PACKET_LENGTH(x);
  case fd_numeric_vector_type:
    return FD_NUMVEC_LENGTH(x);
  case fd_pair_type: {
    int i = 0; fdtype scan = x;
    while (FD_PAIRP(scan)) {i++; scan = FD_CDR(scan);}
    return i;}
  case fd_string_type:
    return u8_strlen_x(FD_STRDATA(x),FD_STRING_LENGTH(x));
  default:
    if (FD_EMPTY_LISTP(x)) return 0;
    else if ((fd_seqfns[ctype]) && (fd_seqfns[ctype]->len) &&
             ((fd_seqfns[ctype]->len) != fd_seq_length))
      return (fd_seqfns[ctype]->len)(x);
    else return -1;}
}

FD_EXPORT fdtype fd_seq_elt(fdtype x,int i)
{
  int ctype = FD_PTR_TYPE(x);
  if (i<0) 
    return FD_RANGE_ERROR;
  else switch (ctype) {
    case fd_vector_type:
      if (i>=FD_VECTOR_LENGTH(x)) return FD_RANGE_ERROR;
      else return fd_incref(FD_VECTOR_REF(x,i));
    case fd_rail_type:
      if (i>=FD_RAIL_LENGTH(x)) return FD_RANGE_ERROR;
      else return fd_incref(FD_RAIL_REF(x,i));
    case fd_packet_type: case fd_secret_type:
      if (i>=FD_PACKET_LENGTH(x)) return FD_RANGE_ERROR;
      else {
        int val = FD_PACKET_DATA(x)[i];
        return FD_INT(val);}
    case fd_numeric_vector_type:
      if (i>=FD_NUMVEC_LENGTH(x))
        return FD_RANGE_ERROR;
      else switch (FD_NUMVEC_TYPE(x)) {
        case fd_short_elt:
          return FD_SHORT2FIX(FD_NUMVEC_SHORT(x,i));
        case fd_int_elt:
          return FD_INT(FD_NUMVEC_INT(x,i));
        case fd_long_elt:
          return FD_INT(FD_NUMVEC_LONG(x,i));
        case fd_float_elt:
          return fd_make_flonum(FD_NUMVEC_FLOAT(x,i));
        case fd_double_elt:
          return fd_make_flonum(FD_NUMVEC_DOUBLE(x,i));
        default:
          return fd_err("Corrupted numvec","fd_seq_elt",NULL,fd_incref(x));}
    case fd_pair_type: {
      int j = 0; fdtype scan = x;
      while (FD_PAIRP(scan))
        if (j == i) return fd_incref(FD_CAR(scan));
        else {j++; scan = FD_CDR(scan);}
      return FD_RANGE_ERROR;}
    case fd_string_type: {
      const u8_byte *sdata = FD_STRDATA(x);
      const u8_byte *starts = string_start(sdata,i);
      if ((starts) && (starts<sdata+FD_STRING_LENGTH(x))) {
        int c = u8_sgetc(&starts);
        return FD_CODE2CHAR(c);}
      else return FD_RANGE_ERROR;}
    default:
      if (FD_EMPTY_LISTP(x)) return FD_RANGE_ERROR;
      else if (FD_EMPTY_CHOICEP(x)) return x;
      else if ((fd_seqfns[ctype]) && (fd_seqfns[ctype]->elt) &&
               ((fd_seqfns[ctype]->elt)!=fd_seq_elt))
        return (fd_seqfns[ctype]->elt)(x,i);
      else if (fd_seqfns[ctype]==NULL)
        return fd_type_error(_("sequence"),"fd_seq_elt",x);
      else return fd_err(fd_NoMethod,"fd_seq_elt",NULL,x);}
}

FD_EXPORT fdtype fd_slice(fdtype x,int start,int end)
{
  int ctype = FD_PTR_TYPE(x);
  if (start<0) return FD_RANGE_ERROR;
  else switch (ctype) {
    case fd_vector_type: case fd_rail_type: {
      fdtype *elts, *write, *read, *limit, result;
      if (end<0) end = FD_VECTOR_LENGTH(x);
      else if (start>FD_VECTOR_LENGTH(x)) return FD_RANGE_ERROR;
      else if (end>FD_VECTOR_LENGTH(x)) return FD_RANGE_ERROR;
      write = elts = u8_alloc_n((end-start),fdtype);
      read = FD_VECTOR_DATA(x)+start; limit = FD_VECTOR_DATA(x)+end;
      while (read<limit) {
        fdtype v = *read++; fd_incref(v); *write++=v;}
      if (ctype == fd_vector_type)
        result = fd_make_vector(end-start,elts);
      else result = fd_make_rail(end-start,elts);
      u8_free(elts);
      return result;}
    case fd_packet_type: case fd_secret_type: {
      const unsigned char *data = FD_PACKET_DATA(x);
      if (end<0) end = FD_PACKET_LENGTH(x);
      else if (end>FD_PACKET_LENGTH(x)) return FD_VOID;
      else if (ctype == fd_secret_type) {
        fdtype packet = fd_make_packet(NULL,end-start,data+start);
        FD_SET_CONS_TYPE(packet,ctype);
        return packet;}
      else {}
      return fd_make_packet(NULL,end-start,data+start);}
    case fd_numeric_vector_type: {
      int len = FD_NUMVEC_LENGTH(x), newlen;
      if (start>len) return FD_RANGE_ERROR;
      else if (end>len) return FD_RANGE_ERROR;
      else if (end<0) {
        newlen = (FD_NUMVEC_LENGTH(x)+end)-start;
        if (newlen<0) return FD_RANGE_ERROR;}
      else newlen = end-start;
      switch (FD_NUMVEC_TYPE(x)) {
      case fd_short_elt:
        return fd_make_short_vector(newlen,FD_NUMVEC_SHORT_SLICE(x,start));
      case fd_int_elt:
        return fd_make_int_vector(newlen,FD_NUMVEC_INT_SLICE(x,start));
      case fd_long_elt:
        return fd_make_long_vector(newlen,FD_NUMVEC_LONG_SLICE(x,start));
      case fd_float_elt:
        return fd_make_float_vector(newlen,FD_NUMVEC_FLOAT_SLICE(x,start));
      case fd_double_elt:
        return fd_make_double_vector(newlen,FD_NUMVEC_DOUBLE_SLICE(x,start));
      default:
        return fd_err("Corrupted numvec","fd_seq_elt",NULL,fd_incref(x));}}
    case fd_pair_type: {
      int j = 0; fdtype scan = x, head = FD_EMPTY_LIST, *tail = &head;
      while (FD_PAIRP(scan))
        if (j == end) return head;
        else if (j>=start) {
          fdtype cons = fd_conspair(fd_incref(FD_CAR(scan)),FD_EMPTY_LIST);
          *tail = cons; tail = &(((struct FD_PAIR *)cons)->cdr);
          j++; scan = FD_CDR(scan);}
        else {j++; scan = FD_CDR(scan);}
      if (j<start) return FD_RANGE_ERROR;
      else if (j<=end) return head;
      else {
        fd_decref(head); return FD_RANGE_ERROR;}}
    case fd_string_type: {
      const u8_byte *starts = string_start(FD_STRDATA(x),start);
      if (starts == NULL) return FD_RANGE_ERROR;
      else if (end<0)
        return fd_substring(starts,NULL);
      else {
        const u8_byte *ends = u8_substring(starts,(end-start));
        if (ends)
          return fd_substring(starts,ends);
        else return FD_RANGE_ERROR;}}
    default:
      if (FD_EMPTY_LISTP(x))
        if ((start == end) && (start == 0)) return FD_EMPTY_LIST;
        else return FD_RANGE_ERROR;
      /* else if (FD_EMPTY_CHOICEP(x)) return x; */
      else if ((fd_seqfns[ctype]) && (fd_seqfns[ctype]->slice) &&
               (fd_seqfns[ctype]->slice!=fd_slice))
        return (fd_seqfns[ctype]->slice)(x,start,end);
      else if (fd_seqfns[ctype]==NULL)
        return fd_type_error(_("sequence"),"fd_slice",x);
      else return fd_err(fd_NoMethod,"fd_slice",NULL,x);}
}

/* Element search functions */

FD_EXPORT int fd_position(fdtype key,fdtype seq,int start,int limit)
{
  int ctype = FD_PTR_TYPE(seq), len = fd_seq_length(seq);
  int end = (limit<0)?(len+limit+1):(limit>len)?(len):(limit);
  int delta = (start<end)?(1):(-1);
  int min = ((start<end)?(start):(end)), max = ((start<end)?(end):(start));
  if ((start<0)||(end<0)) return -2;
  else if (start>end) return -1;
  else switch (ctype) {
    case fd_vector_type: case fd_rail_type: {
      fdtype *data = FD_VECTOR_ELTS(seq);
      int i = start; while (i!=end) {
        if (FDTYPE_EQUAL(key,data[i])) return i;
        else if (FD_CHOICEP(data[i]))
          if (fd_overlapp(key,data[i])) return i;
          else i = i+delta;
        else i = i+delta;}
      return -1;}
    case fd_packet_type: case fd_secret_type: {
      int intval = (FD_INTP(key))?(FD_FIX2INT(key)):(-1);
      if ((FD_UINTP(key))&&(intval>=0)&&(intval<0x100)) {
        const unsigned char *data = FD_PACKET_DATA(seq);
        int i = start; while (i!=end) {
          if (intval == data[i]) return i;
          else i = i+delta;}
        return -1;}
      else return -1;}
    case fd_numeric_vector_type:
      if (!(FD_NUMBERP(key)))
        return -1;
      else return fd_generic_position(key,seq,start,end);
    case fd_string_type:
      if (FD_CHARACTERP(key)) {
        int code = FD_CHAR2CODE(key);
        u8_string data = FD_STRDATA(seq), found;
        u8_string lower = u8_substring(data,min);
        u8_string upper = u8_substring(lower,max-min);
        if (code<0x80) {
          if (delta<0) 
            found = strrchr(upper,code);
          else found = strchr(lower,code);
          if (found == NULL) return -1;
          else return u8_charoffset(data,found-data);}
        else {
          int c, pos = start, last_match = -1;
          u8_string scan = lower; while (scan<upper) {
            c = u8_sgetc(&scan);
            if (c == code) {
              if (delta<0) last_match = pos;
              else return pos;}
            else pos++;}
          return last_match;}}
      else return -1;
    case fd_pair_type: {
      int pos = 0; fdtype scan = seq;
      if (start == end) return -1;
      while (FD_PAIRP(scan))
        if (pos<start) {pos++; scan = FD_CDR(scan);}
        else if (pos>=end) return -1;
        else if (FDTYPE_EQUAL(FD_CAR(scan),key)) return pos;
        else {pos++; scan = FD_CDR(scan);}
      if ((pos<start) || ((end>0) && (pos<end))) return -2;
      else return -1;}
    default:
      if (FD_EMPTY_LISTP(seq)) return -1;
      else if ((fd_seqfns[ctype]) && (fd_seqfns[ctype]->position) &&
               ((fd_seqfns[ctype]->position)!=fd_position))
        return (fd_seqfns[ctype]->position)(key,seq,start,end);
      else if (fd_seqfns[ctype])
        return fd_generic_position(key,seq,start,end);
      else {
        fd_seterr(_("Not a sequence"),"fd_position",NULL,seq); fd_incref(seq);
        return -3;}}
}

FD_EXPORT int fd_rposition(fdtype key,fdtype x,int start,int end)
{
  if (FD_EMPTY_LISTP(x)) return -1;
  else if ((FD_STRINGP(x)) && (FD_CHARACTERP(key)) &&
           (FD_CHAR2CODE(key)<0x80)) {
    u8_string data = FD_STRDATA(x);
    int code = FD_CHAR2CODE(key);
    u8_string found = strrchr(data+start,code);
    if (found)
      if (found<data+end) return u8_charoffset(data,found-data);
      else return -1;
    else return -1;}
  else switch (FD_PTR_TYPE(x)) {
  case fd_vector_type: case fd_rail_type: {
    fdtype *data = FD_VECTOR_DATA(x);
    int len = FD_VECTOR_LENGTH(x);
    if (end<0) end = len;
    if ((start<0) || (end<start) || (start>len) || (end>len))
      return -2;
    else while (start<end--)
           if (FDTYPE_EQUAL(key,data[end])) return end;
    return -1;}
  case fd_packet_type: case fd_secret_type: {
    const unsigned char *data = FD_PACKET_DATA(x);
    int len = FD_PACKET_LENGTH(x), keyval;
    if (end<0) end = len;
    if (FD_FIXNUMP(key)) keyval = FD_FIX2INT(key); else return -1;
    if ((keyval<0) || (keyval>255)) return -1;
    else if ((start<0) || (end<start) || (start>len) || (end>len))
      return -2;
    else while (start<end--) {
        if (keyval == data[end]) return start;}
    return -1;}
  default: {
    int last = -1, pos;
    while ((start<end) &&
           (pos = fd_position(key,x,start,end))>=0) {
      last = pos; start = pos+1;}
    return last;}}
}

/* Generic position */
FD_EXPORT int fd_generic_position(fdtype key,fdtype x,int start,int end)
{
  int len = fd_seq_length(x);
  if (end<0) end = len+end;
  else if (end<start)  {
    int tmp = start; start = end; end = tmp;}
  else {}
  if ((start<0)||(end<0))
    return FD_RANGE_ERROR;
  else if (start>end) 
    return -1;
  else {
    int delta = (start<end)?(1):(-1);
    int i = start; while (i<len) {
      fdtype elt = fd_seq_elt(x,i);
      if (FD_EQUALP(elt,x)) {
        fd_decref(elt);
        return i;}
      else {
        fd_decref(elt); 
        i = i+delta;}}
    return -1;}
}

/* Sub-sequence search */ 

static int packet_search(fdtype subseq,fdtype seq,int start,int end);
static int vector_search(fdtype subseq,fdtype seq,int start,int end);

FD_EXPORT int fd_search(fdtype subseq,fdtype seq,int start,int end)
{
  if (start>=end) return -1;
  else if ((FD_STRINGP(subseq)) && (FD_STRINGP(seq))) {
    const u8_byte *starts = string_start(FD_STRDATA(seq),start), *found;
    if (starts == NULL) return -2;
    found = strstr(starts,FD_STRDATA(subseq));
    if (end<0) {
      if (found) return start+u8_strlen_x(starts,found-starts);
      else return -1;}
    else {
      const u8_byte *ends = string_start(starts,end-start);
      if (ends == NULL) return -2;
      else if ((found) && (found<ends))
        return start+u8_strlen_x(starts,found-starts);
      else return -1;}}
  else if (FD_EMPTY_LISTP(seq)) return -1;
  else {
    int ctype = FD_PTR_TYPE(seq);
    if ((fd_seqfns[ctype]) && (fd_seqfns[ctype]->search) &&
        ((fd_seqfns[ctype]->search)!=fd_search))
      return (fd_seqfns[ctype]->search)(subseq,seq,start,end);
    else if ((FD_PACKETP(seq)) && (FD_PACKETP(subseq)))
      return packet_search(subseq,seq,start,end);
    else if ((FD_VECTORP(seq)) && (FD_VECTORP(subseq)))
      return vector_search(subseq,seq,start,end);
    else return fd_generic_search(subseq,seq,start,end);}
}

FD_EXPORT int fd_generic_search(fdtype subseq,fdtype seq,int start,int end)
{
  /* Generic implementation */
  int subseqlen = fd_seq_length(subseq), pos = start;
  fdtype subseqstart = fd_seq_elt(subseq,0);
  if (end<0) end = fd_seq_length(seq);
  while ((pos = fd_position(subseqstart,seq,pos,pos-subseqlen))>=0) {
    int i = 1, j = pos+1;
    while (i < subseqlen) {
      fdtype kelt = fd_seq_elt(subseq,i), velt = fd_seq_elt(seq,j);
      if ((FDTYPE_EQUAL(kelt,velt)) ||
          ((FD_CHOICEP(velt)) && (fd_overlapp(kelt,velt)))) {
        fd_decref(kelt); fd_decref(velt); i++; j++;}
      else {fd_decref(kelt); fd_decref(velt); break;}}
    if (i == subseqlen) {
      fd_decref(subseqstart);
      return pos;}
    else pos++;}
  fd_decref(subseqstart);
  return -1;
}

static int packet_search(fdtype key,fdtype x,int start,int end)
{
  int klen = FD_PACKET_LENGTH(key);
  const unsigned char *kdata = FD_PACKET_DATA(key), first_byte = kdata[0];
  const unsigned char *data = FD_PACKET_DATA(x), *scan, *lim = data+end;
  if (klen>(end-start)) return -1;
  scan = data+start;
  while ((scan = memchr(scan,first_byte,lim-scan))) {
    if (memcmp(scan,kdata,klen)==0) return scan-data;
    else scan++;}
  return -1;
}

static int vector_search(fdtype key,fdtype x,int start,int end)
{
  int klen = FD_VECTOR_LENGTH(key);
  fdtype *kdata = FD_VECTOR_DATA(key), first_elt = kdata[0];
  fdtype *data = FD_VECTOR_DATA(x), *scan, *lim = data+(end-klen)+1;
  if (klen>(end-start)) return -1;
  scan = data+start;
  while (scan<lim)
    if ((scan[0]==first_elt) ||
        (FDTYPE_EQUAL(scan[0],first_elt)) ||
        ((FD_CHOICEP(scan[0])) &&
         (fd_overlapp(scan[0],first_elt)))) {
      fdtype *kscan = kdata+1, *klim = kdata+klen, *vscan = scan+1;
      while ((kscan<klim) &&
             ((*kscan== *vscan) ||
              (FDTYPE_EQUAL(*kscan,*vscan)) ||
              ((FD_CHOICEP(*vscan)) &&
               (fd_overlapp(*kscan,*vscan))))) {
        kscan++; vscan++;}
      if (kscan == klim) return scan-data;
      else scan++;}
    else scan++;
  return -1;
}

/* Creating and extracting data */

/* fd_elts:
     Arguments: a lisp sequence and a pointer to an int
     Returns: a C vector of dtype pointers
   This returns a vector of dtype pointers representing the elements
     of the sequence.  For strings these are characters, for packets, they
     are ints. */
fdtype *fd_elts(fdtype seq,int *n)
{
  int len = fd_seq_length(seq);
  if (len==0) {*n = 0; return NULL;}
  else {
    fd_ptr_type ctype = FD_PTR_TYPE(seq);
    fdtype *vec = u8_alloc_n(len,fdtype);
    *n = len;
    switch (ctype) {
    case fd_packet_type: case fd_secret_type: {
      const unsigned char *packet = FD_PACKET_DATA(seq);
      int i = 0; while (i < len) {
        int byte = packet[i];
        vec[i]=FD_INT(byte); i++;}
      break;}
    case fd_string_type: {
      int i = 0;
      const u8_byte *scan = FD_STRING_DATA(seq),
        *limit = scan+FD_STRING_LENGTH(seq);
      while (scan<limit)
        if (*scan=='\0') {
          vec[i]=FD_CODE2CHAR(0); scan++; i++;}
        else {
          int ch = u8_sgetc(&scan);
          if (ch<0) break;
          vec[i]=FD_CODE2CHAR(ch); i++;}
      *n = i;
      break;}
    case fd_vector_type: case fd_rail_type: {
      int i = 0;
      fdtype *scan = FD_VECTOR_DATA(seq),
        *limit = scan+FD_VECTOR_LENGTH(seq);
      while (scan<limit) {
        vec[i]=fd_incref(*scan); i++; scan++;}
      break;}
    case fd_numeric_vector_type: {
      int i = 0;
      switch (FD_NUMVEC_TYPE(seq)) {
      case fd_short_elt: {
        fd_short *shorts = FD_NUMVEC_SHORTS(seq);
        while (i<len) {vec[i]=FD_SHORT2FIX(shorts[i]); i++;}
        break;}
      case fd_int_elt: {
        fd_int *ints = FD_NUMVEC_INTS(seq);
        while (i<len) {vec[i]=FD_INT(ints[i]); i++;}
        break;}
      case fd_long_elt: {
        fd_long *longs = FD_NUMVEC_LONGS(seq);
        while (i<len) {vec[i]=FD_INT(longs[i]); i++;}
        break;}
      case fd_float_elt: {
        fd_float *floats = FD_NUMVEC_FLOATS(seq);
        while (i<len) {vec[i]=fd_make_double(floats[i]); i++;}
        break;}
      case fd_double_elt: {
        fd_double *doubles = FD_NUMVEC_DOUBLES(seq);
        while (i<len) {vec[i]=fd_make_double(doubles[i]); i++;}
        break;}}
      break;}
    case fd_pair_type: {
      int i = 0; FD_DOLIST(elt,seq) {
        fdtype e = fd_incref(elt); vec[i++]=e;}
      break;}
    default:
      if ((fd_seqfns[ctype]) && (fd_seqfns[ctype]->elts))
        return (fd_seqfns[ctype]->elts)(seq,n);
      else return NULL;}
    return vec;}
}

FD_EXPORT
/* fd_makeseq:
    Arguments: a sequence type, a length, and a C vector of dtype pointers.
    Returns: a sequence
  Creates a sequence of the designated type out of the given elements. */
fdtype fd_makeseq(fd_ptr_type ctype,int n,fdtype *v)
{
  switch (ctype) {
  case fd_string_type: {
    struct U8_OUTPUT out; int i = 0;
    if (n==0) return fd_make_string(NULL,0,"");
    U8_INIT_OUTPUT(&out,n*2);
    while (i < n) {
      if (FD_CHARACTERP(v[i])) u8_putc(&out,FD_CHAR2CODE(v[i]));
      else if (FD_UINTP(v[i])) u8_putc(&out,FD_FIX2INT(v[i]));
      else {
        u8_free(out.u8_outbuf);
        return fd_type_error(_("character"),"fd_makeseq",v[i]);}
      i++;}
    return fd_stream2string(&out);}
  case fd_packet_type: case fd_secret_type: {
    fdtype result = FD_VOID;
    unsigned char *bytes = u8_malloc(n); int i = 0;
    while (i < n) {
      if (FD_UINTP(v[i])) bytes[i]=FD_FIX2INT(v[i]);
      else if (FD_CHARACTERP(v[i])) {
        unsigned int code = FD_CHAR2CODE(v[i]);
        bytes[i]=code&0xFF;}
      else {
        u8_free(bytes);
        return fd_type_error(_("byte"),"fd_makeseq",v[i]);}
      i++;}
    result = fd_make_packet(NULL,n,bytes);
    if (ctype == fd_secret_type) {
      FD_SET_CONS_TYPE(result,fd_secret_type);}
    u8_free(bytes);
    return result;}
  case fd_vector_type: {
    int i = 0; while (i < n) {fd_incref(v[i]); i++;}
    return fd_make_vector(n,v);}
  case fd_rail_type: {
    int i = 0; while (i < n) {fd_incref(v[i]); i++;}
    return fd_make_rail(n,v);}
  case fd_pair_type:
    if (n == 0) return FD_EMPTY_LIST;
    else {
      fdtype head = FD_EMPTY_LIST, *tail = &head;
      int i = 0; while (i < n) {
        fdtype cons = fd_make_pair(v[i],FD_EMPTY_LIST);
        *tail = cons; tail = &(((struct FD_PAIR *)cons)->cdr);
        i++;}
      return head;}
  default:
    if ((fd_seqfns[ctype]) && (fd_seqfns[ctype]->make))
      return (fd_seqfns[ctype]->make)(n,v);
    else return fd_type_error(_("sequence type"),"fd_make_seq",FD_VOID);}
}

/* Complex primitives */

FD_EXPORT fdtype fd_reverse(fdtype sequence)
{
  if (FD_EMPTY_LISTP(sequence)) return sequence;
  else {
    int i, j, len; fdtype *elts = fd_elts(sequence,&len), result;
    fdtype *tmp = ((len) ? (u8_alloc_n(len,fdtype)) : (NULL));
    if (len) {
      i = 0; j = len-1; while (i < len) {tmp[j]=elts[i]; i++; j--;}}
    if (FD_TYPEP(sequence,fd_numeric_vector_type)) {
      switch (FD_NUMVEC_TYPE(sequence)) {
      case fd_float_elt:
        result = make_float_vector(len,elts); break;
      case fd_double_elt:
        result = make_double_vector(len,elts); break;
      case fd_short_elt:
        result = make_short_vector(len,elts); break;
      case fd_int_elt:
        result = make_int_vector(len,elts); break;
      case fd_long_elt:
        result = make_long_vector(len,elts); break;}}
    else result = fd_makeseq(FD_PTR_TYPE(sequence),len,tmp);
    i = 0; while (i<len) {fd_decref(elts[i]); i++;}
    if (elts) u8_free(elts); if (tmp) u8_free(tmp);
    return result;}
}

typedef fdtype *fdtypep;

FD_EXPORT fdtype fd_append(int n,fdtype *sequences)
{
  if (n == 0) return FD_EMPTY_LIST;
  else {
    fd_ptr_type result_type = FD_PTR_TYPE(sequences[0]);
    fdtype result, **elts, *_elts[16], *combined;
    int i = 0, k = 0, *lengths, _lengths[16], total_length = 0;
    if (FD_EMPTY_LISTP(sequences[0])) result_type = fd_pair_type;
    if (n>16) {
      lengths = u8_alloc_n(n,int);
      elts = u8_alloc_n(n,fdtypep);}
    else {lengths=_lengths; elts=_elts;}
    while (i < n) {
      fdtype seq = sequences[i];
      if ((FD_EMPTY_LISTP(seq)) && (result_type == fd_pair_type)) {}
      else if (FD_PTR_TYPE(seq) == result_type) {}
      else if ((result_type == fd_secret_type)&&
               ((FD_PTR_TYPE(seq) == fd_packet_type)||
                (FD_PTR_TYPE(seq) == fd_string_type))) 
        result_type = fd_secret_type;
      else if ((result_type == fd_packet_type)&&
               (FD_PTR_TYPE(seq) == fd_secret_type))
        result_type = fd_secret_type;
      else if ((result_type == fd_string_type)&&
               (FD_PTR_TYPE(seq) == fd_secret_type))
        result_type = fd_secret_type;
      else if ((result_type == fd_string_type)&&
               (FD_PTR_TYPE(seq) == fd_packet_type))
        result_type = fd_packet_type;
      else if (FD_PTR_TYPE(seq) != result_type)
        result_type = fd_vector_type;
      else {}
      elts[i]=fd_elts(seq,&(lengths[i]));
      total_length = total_length+lengths[i];
      if (lengths[i]==0) i++;
      else if (elts[i]==NULL)  {
        if (n>16) {u8_free(lengths); u8_free(elts);}
        return fd_type_error(_("sequence"),"fd_append",seq);}
      else i++;}
    combined = u8_alloc_n(total_length,fdtype);
    i = 0; while (i < n) {
      int j = 0, lim = lengths[i]; fdtype *seqelts = elts[i];
      while (j<lim) combined[k++]=seqelts[j++];
      u8_free(elts[i]);
      i++;}
    result = fd_makeseq(result_type,total_length,combined);
    i = 0; while (i<total_length) {fd_decref(combined[i]); i++;}
    if (n>16) {u8_free(lengths); u8_free(elts);}
    u8_free(combined);
    return result;}
}

/* removal */

FD_EXPORT fdtype fd_remove(fdtype item,fdtype sequence)
{
  if (!(FD_SEQUENCEP(sequence)))
    return fd_type_error("sequence","fd_remove",sequence);
  else if (FD_EMPTY_LISTP(sequence)) return sequence;
  else {
    int i = 0, j = 0, removals = 0, len = fd_seq_length(sequence);
    fd_ptr_type result_type = FD_PTR_TYPE(sequence);
    fdtype *results = u8_alloc_n(len,fdtype), result;
    while (i < len) {
      fdtype elt = fd_seq_elt(sequence,i); i++;
      if (FDTYPE_EQUAL(elt,item)) {removals++; fd_decref(elt);}
      else results[j++]=elt;}
    if (removals) {
      result = fd_makeseq(result_type,j,results);
      i = 0; while (i<j) {fd_decref(results[i]); i++;}
      u8_free(results);
      return result;}
    else {
      i = 0; while (i<j) {fd_decref(results[i]); i++;}
      u8_free(results);
      return fd_incref(sequence);}}
}


/* Making particular types of sequences */

static fdtype make_short_vector(int n,fdtype *from_elts)
{
  int i = 0; fdtype vec = fd_make_numeric_vector(n,fd_short_elt);
  fd_short *elts = FD_NUMVEC_SHORTS(vec);
  while (i<n) {
    fdtype elt = from_elts[i];
    if (FD_SHORTP(elt))
      elts[i++]=(fd_short)(FD_FIX2INT(elt));
    else {
      u8_free((struct FD_CONS *)vec); fd_incref(elt);
      return fd_type_error(_("short element"),"make_short_vector",elt);}}
  return vec;
}

static fdtype make_int_vector(int n,fdtype *from_elts)
{
  int i = 0; fdtype vec = fd_make_numeric_vector(n,fd_int_elt);
  fd_int *elts = FD_NUMVEC_INTS(vec);
  while (i<n) {
    fdtype elt = from_elts[i];
    if (FD_INTP(elt))
      elts[i++]=((fd_int)(FD_FIX2INT(elt)));
    else if ((FD_BIGINTP(elt))&&
             (fd_bigint_fits_in_word_p((fd_bigint)elt,sizeof(fd_int),1)))
      elts[i++]=fd_bigint_to_long((fd_bigint)elt);
    else {
      u8_free((struct FD_CONS *)vec); fd_incref(elt);
      return fd_type_error(_("int element"),"make_int_vector",elt);}}
  return vec;
}

static fdtype make_long_vector(int n,fdtype *from_elts)
{
  int i = 0; fdtype vec = fd_make_numeric_vector(n,fd_long_elt);
  fd_long *elts = FD_NUMVEC_LONGS(vec);
  while (i<n) {
    fdtype elt = from_elts[i];
    if (FD_FIXNUMP(elt))
      elts[i++]=((fd_long)(FD_FIX2INT(elt)));
    else if (FD_BIGINTP(elt))
      elts[i++]=fd_bigint_to_long_long((fd_bigint)elt);
    else {
      u8_free((struct FD_CONS *)vec); fd_incref(elt);
      return fd_type_error(_("flonum element"),"make_long_vector",elt);}}
  return vec;
}

static fdtype make_float_vector(int n,fdtype *from_elts)
{
  int i = 0; fdtype vec = fd_make_numeric_vector(n,fd_float_elt);
  float *elts = FD_NUMVEC_FLOATS(vec);
  while (i<n) {
    fdtype elt = from_elts[i];
    if (FD_FLONUMP(elt))
      elts[i++]=FD_FLONUM(elt);
    else if (FD_NUMBERP(elt))
      elts[i++]=fd_todouble(elt);
    else {
      u8_free((struct FD_CONS *)vec); fd_incref(elt);
      return fd_type_error(_("float element"),"make_float_vector",elt);}}
  return vec;
}

static fdtype make_double_vector(int n,fdtype *from_elts)
{
  int i = 0; fdtype vec = fd_make_numeric_vector(n,fd_double_elt);
  double *elts = FD_NUMVEC_DOUBLES(vec);
  while (i<n) {
    fdtype elt = from_elts[i];
    if (FD_FLONUMP(elt))
      elts[i++]=FD_FLONUM(elt);
    else if (FD_NUMBERP(elt))
      elts[i++]=fd_todouble(elt);
    else {
      u8_free((struct FD_CONS *)vec); fd_incref(elt);
      return fd_type_error(_("double(float) element"),"make_double_vector",elt);}}
  return vec;
}

/* Miscellaneous sequence creation functions */

static fdtype makepair(int n,fdtype *elts) {
  return fd_makeseq(fd_pair_type,n,elts);}
static fdtype makestring(int n,fdtype *elts) {
  return fd_makeseq(fd_string_type,n,elts);}
static fdtype makepacket(int n,fdtype *elts) {
  return fd_makeseq(fd_packet_type,n,elts);}
static fdtype makesecret(int n,fdtype *elts) {
  return fd_makeseq(fd_secret_type,n,elts);}
static fdtype makevector(int n,fdtype *elts) {
  return fd_makeseq(fd_vector_type,n,elts);}
static fdtype makerail(int n,fdtype *elts) {
  return fd_makeseq(fd_rail_type,n,elts);}

static struct FD_SEQFNS pair_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  makepair};
static struct FD_SEQFNS string_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  makestring};
static struct FD_SEQFNS packet_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  makepacket};
static struct FD_SEQFNS vector_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  makevector};
static struct FD_SEQFNS numeric_vector_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  makevector};
static struct FD_SEQFNS rail_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  makerail};
static struct FD_SEQFNS secret_seqfns={
  fd_seq_length,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  makesecret};


FD_EXPORT void fd_init_sequences_c()
{
  int i = 0; while (i<FD_TYPE_MAX) fd_seqfns[i++]=NULL;
  fd_seqfns[fd_pair_type]= &pair_seqfns;
  fd_seqfns[fd_string_type]= &string_seqfns;
  fd_seqfns[fd_packet_type]= &packet_seqfns;
  fd_seqfns[fd_secret_type]= &secret_seqfns;
  fd_seqfns[fd_vector_type]= &vector_seqfns;
  fd_seqfns[fd_rail_type]= &rail_seqfns;
  fd_seqfns[fd_numeric_vector_type]= &numeric_vector_seqfns;

  u8_register_source_file(_FILEINFO);
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
