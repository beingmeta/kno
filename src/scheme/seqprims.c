/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_PPTRS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/fddb.h"
#include "framerd/eval.h"
#include "framerd/sequences.h"
#include "framerd/numbers.h"
#include "framerd/sorting.h"

#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>

#define FD_EQV(x,y) ((FD_EQ(x,y)) || ((FD_NUMBERP(x)) && (FD_NUMBERP(y)) && (fd_numcompare(x,y)==0)))

#define string_start(bytes,i) ((i==0) ? (bytes) : (u8_substring(bytes,i)))

static fd_exception SequenceMismatch=_("Mismatch of sequence lengths");
static fd_exception EmptyReduce=_("No sequence elements to reduce");

struct FD_SEQFNS *fd_seqfns[FD_TYPE_MAX];

FD_EXPORT int fd_seq_length(fdtype x)
{
  int ctype=FD_PTR_TYPE(x);
  switch (ctype) {
  case fd_vector_type:
    return FD_VECTOR_LENGTH(x);
  case fd_rail_type:
    return FD_RAIL_LENGTH(x);
  case fd_packet_type: case fd_secret_type:
    return FD_PACKET_LENGTH(x);
  case fd_pair_type: {
    int i=0; fdtype scan=x;
    while (FD_PAIRP(scan)) {i++; scan=FD_CDR(scan);}
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
  int ctype=FD_PTR_TYPE(x);
  switch (ctype) {
  case fd_vector_type:
    if (i>=FD_VECTOR_LENGTH(x)) return FD_RANGE_ERROR;
    else return fd_incref(FD_VECTOR_REF(x,i));
  case fd_rail_type:
    if (i>=FD_RAIL_LENGTH(x)) return FD_RANGE_ERROR;
    else return fd_incref(FD_RAIL_REF(x,i));
  case fd_packet_type: case fd_secret_type:
    if (i>=FD_PACKET_LENGTH(x)) return FD_RANGE_ERROR;
    else {
      int val=FD_PACKET_DATA(x)[i];
      return FD_INT2DTYPE(val);}
  case fd_pair_type: {
    int j=0; fdtype scan=x;
    while (FD_PAIRP(scan))
      if (j==i) return fd_incref(FD_CAR(scan));
      else {j++; scan=FD_CDR(scan);}
    return FD_RANGE_ERROR;}
  case fd_string_type: {
    const u8_byte *sdata=FD_STRDATA(x);
    const u8_byte *starts=string_start(sdata,i);
    if ((starts) && (starts<sdata+FD_STRING_LENGTH(x))) {
      int c=u8_sgetc(&starts);
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
  int ctype=FD_PTR_TYPE(x);
  switch (ctype) {
  case fd_vector_type: case fd_rail_type: {
    fdtype *elts, *write, *read, *limit, result;
    if (end<0) end=FD_VECTOR_LENGTH(x);
    else if (start>FD_VECTOR_LENGTH(x)) return FD_RANGE_ERROR;
    else if (end>FD_VECTOR_LENGTH(x)) return FD_RANGE_ERROR;
    write=elts=u8_alloc_n((end-start),fdtype);
    read=FD_VECTOR_DATA(x)+start; limit=FD_VECTOR_DATA(x)+end;
    while (read<limit) {
      fdtype v=*read++; fd_incref(v); *write++=v;}
    if (ctype==fd_vector_type)
      result=fd_make_vector(end-start,elts);
    else result=fd_make_rail(end-start,elts);
    u8_free(elts);
    return result;}
  case fd_packet_type: case fd_secret_type: {
    const unsigned char *data=FD_PACKET_DATA(x);
    if (end<0) end=FD_PACKET_LENGTH(x);
    else if (end>FD_PACKET_LENGTH(x)) return FD_VOID;
    else if (ctype==fd_secret_type) {
      fdtype packet=fd_make_packet(NULL,end-start,data+start);
      FD_SET_CONS_TYPE(packet,ctype);
      return packet;}
    return fd_make_packet(NULL,end-start,data+start);}
  case fd_pair_type: {
    int j=0; fdtype scan=x, head=FD_EMPTY_LIST, *tail=&head;
    while (FD_PAIRP(scan))
      if (j==end) return head;
      else if (j>=start) {
        fdtype cons=fd_init_pair(NULL,fd_incref(FD_CAR(scan)),FD_EMPTY_LIST);
        *tail=cons; tail=&(((struct FD_PAIR *)cons)->cdr);
        j++; scan=FD_CDR(scan);}
      else {j++; scan=FD_CDR(scan);}
    if (j<start) return FD_RANGE_ERROR;
    else if (j<=end) return head;
    else {
      fd_decref(head); return FD_RANGE_ERROR;}}
  case fd_string_type: {
    const u8_byte *starts=string_start(FD_STRDATA(x),start);
    if (starts==NULL) return FD_RANGE_ERROR;
    else if (end<0)
      return fd_substring(starts,NULL);
    else {
      const u8_byte *ends=u8_substring(starts,(end-start));
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

FD_EXPORT int fd_position(fdtype key,fdtype x,int start,int end)
{
  int ctype=FD_PTR_TYPE(x);
  switch (ctype) {
  case fd_vector_type: case fd_rail_type: {
    fdtype *data=FD_VECTOR_ELTS(x);
    int len=FD_VECTOR_LENGTH(x);
    if (end<0) end=len;
    if ((start<0) || (end<start) || (start>len) || (end>len))
      return -2;
    else if (start==end) return -1;
    else while (start<end)
      if (FDTYPE_EQUAL(key,data[start])) return start;
      else if (FD_CHOICEP(data[start]))
        if (fd_overlapp(key,data[start])) return start;
        else start++;
      else start++;
    return -1;}
  case fd_packet_type: case fd_secret_type: {
    const unsigned char *data=FD_PACKET_DATA(x);
    int len=FD_PACKET_LENGTH(x), keyval;
    if (end<0) end=len;
    if (FD_FIXNUMP(key)) keyval=FD_FIX2INT(key); else return -1;
    if ((keyval<0) || (keyval>255)) return -1;
    else if ((start<0) || (end<start) || (start>len) || (end>len))
      return -2;
    else if (start==end) return -1;
    else while (start<end)
      if (keyval==data[start]) return start;
      else start++;
    return -1;}
  case fd_pair_type: {
    int pos=0; fdtype scan=x;
    if (start==end) return -1;
    while (FD_PAIRP(scan))
      if (pos<start) {pos++; scan=FD_CDR(scan);}
      else if ((end>=0) && (pos>=end)) return -1;
      else if (FDTYPE_EQUAL(FD_CAR(scan),key)) return pos;
      else if (FD_CHOICEP(FD_CAR(scan)))
        if (fd_overlapp(key,FD_CAR(scan))) return pos;
        else {pos++; scan=FD_CDR(scan);}
      else {pos++; scan=FD_CDR(scan);}
    if ((pos<start) || ((end>0) && (pos<end))) return -2;
    else return -1;}
  case fd_string_type:
    if (FD_CHARACTERP(key)) {
      int keychar=FD_CHAR2CODE(key), c;
      u8_string starts=string_start(FD_STRDATA(x),start);
      u8_string scan=starts, last=scan;
      if (end<0) while ((c=u8_sgetc(&scan))>=0)
        if (c==keychar)
          return start+u8_strlen_x(starts,last-starts);
        else last=scan;
      else {
        u8_string lim=string_start(FD_STRDATA(x),end);
        while ((scan<lim) && ((c=u8_sgetc(&scan))>=0))
          if (c==keychar)
            return start+u8_strlen_x(starts,last-starts);
          else last=scan;
        return -1;}}
    else return -1;
  default:
    if (FD_EMPTY_LISTP(x)) return -1;
    else if ((fd_seqfns[ctype]) && (fd_seqfns[ctype]->position) &&
             ((fd_seqfns[ctype]->search)!=fd_position))
      return (fd_seqfns[ctype]->position)(key,x,start,end);
    else return -3;}
}

FD_EXPORT int fd_rposition(fdtype key,fdtype x,int start,int end)
{
  if (FD_EMPTY_LISTP(x)) return FD_FALSE;
  else if ((FD_STRINGP(x)) && (FD_CHARACTERP(key)) &&
           (FD_CHAR2CODE(key)<0x80)) {
    u8_string data=FD_STRDATA(x);
    int code=FD_CHAR2CODE(key);
    u8_string found=strrchr(data+start,code);
    if (found)
      if (found<data+end) return u8_charoffset(data,found-data);
      else {}
    else return -1;}
  switch (FD_PTR_TYPE(x)) {
  case fd_vector_type: case fd_rail_type: {
    fdtype *data=FD_VECTOR_DATA(x);
    int len=FD_VECTOR_LENGTH(x);
    if (end<0) end=len;
    if ((start<0) || (end<start) || (start>len) || (end>len))
      return -2;
    else while (start<end--)
      if (FDTYPE_EQUAL(key,data[end])) return end;
    return -1;}
  case fd_packet_type: case fd_secret_type: {
    const unsigned char *data=FD_PACKET_DATA(x);
    int len=FD_PACKET_LENGTH(x), keyval;
    if (end<0) end=len;
    if (FD_FIXNUMP(key)) keyval=FD_FIX2INT(key); else return -1;
    if ((keyval<0) || (keyval>255)) return -1;
    else if ((start<0) || (end<start) || (start>len) || (end>len))
      return -2;
    else while (start<end--)
      if (keyval==data[end]) return start;
    return -1;}
  default: {
    int last=-1, pos;
    while ((start<end) &&
           (pos=fd_position(key,x,start,end))>=0) {
      last=pos; start=pos+1;}
    return last;}
  }
}

static int packet_search(fdtype key,fdtype x,int start,int end)
{
  int klen=FD_PACKET_LENGTH(key);
  const unsigned char *kdata=FD_PACKET_DATA(key), first_byte=kdata[0];
  const unsigned char *data=FD_PACKET_DATA(x), *scan, *lim=data+end;
  if (klen>(end-start)) return -1;
  scan=data+start;
  while ((scan=memchr(scan,first_byte,lim-scan))) {
    if (memcmp(scan,kdata,klen)==0) return scan-data;
    else scan++;}
  return -1;
}

static int vector_search(fdtype key,fdtype x,int start,int end)
{
  int klen=FD_VECTOR_LENGTH(key);
  fdtype *kdata=FD_VECTOR_DATA(key), first_elt=kdata[0];
  fdtype *data=FD_VECTOR_DATA(x), *scan, *lim=data+(end-klen)+1;
  if (klen>(end-start)) return -1;
  scan=data+start;
  while (scan<lim)
    if ((scan[0]==first_elt) ||
        (FDTYPE_EQUAL(scan[0],first_elt)) ||
        ((FD_CHOICEP(scan[0])) &&
         (fd_overlapp(scan[0],first_elt)))) {
      fdtype *kscan=kdata+1, *klim=kdata+klen, *vscan=scan+1;
      while ((kscan<klim) &&
             ((*kscan==*vscan) ||
              (FDTYPE_EQUAL(*kscan,*vscan)) ||
              ((FD_CHOICEP(*vscan)) &&
               (fd_overlapp(*kscan,*vscan))))) {
        kscan++; vscan++;}
      if (kscan==klim) return scan-data;
      else scan++;}
    else scan++;
  return -1;
}

FD_EXPORT int fd_search(fdtype key,fdtype x,int start,int end)
{
  if ((FD_STRINGP(key)) && (FD_STRINGP(x))) {
    const u8_byte *starts=string_start(FD_STRDATA(x),start), *found;
    if (starts == NULL) return -2;
    found=strstr(starts,FD_STRDATA(key));
    if (end<0) {
      if (found) return start+u8_strlen_x(starts,found-starts);
      else return -1;}
    else {
      const u8_byte *ends=string_start(starts,end-start);
      if (ends==NULL) return -2;
      else if ((found) && (found<ends))
        return start+u8_strlen_x(starts,found-starts);
      else return -1;}}
  else if (FD_EMPTY_LISTP(x)) return FD_FALSE;
  else {
    int ctype=FD_PTR_TYPE(x);
    if ((fd_seqfns[ctype]) && (fd_seqfns[ctype]->search) &&
        ((fd_seqfns[ctype]->search)!=fd_search))
      return (fd_seqfns[ctype]->search)(key,x,start,end);
    else if ((FD_PACKETP(x)) && (FD_PACKETP(key)))
      return packet_search(key,x,start,end);
    else if ((FD_VECTORP(x)) && (FD_VECTORP(key)))
      return vector_search(key,x,start,end);
    else {
      int keylen=fd_seq_length(key), pos=start;
      fdtype keystart=fd_seq_elt(key,0);
      if (end<0) end=fd_seq_length(x);
      while ((pos=fd_position(keystart,x,pos,pos-keylen))>=0) {
        int i=1, j=pos+1;
        while (i < keylen) {
          fdtype kelt=fd_seq_elt(key,i), velt=fd_seq_elt(x,j);
          if ((FDTYPE_EQUAL(kelt,velt)) ||
              ((FD_CHOICEP(velt)) && (fd_overlapp(kelt,velt)))) {
            fd_decref(kelt); fd_decref(velt); i++; j++;}
          else {fd_decref(kelt); fd_decref(velt); break;}}
        if (i == keylen) {
          fd_decref(keystart);
          return pos;}
        else pos++;}
      fd_decref(keystart);
      return -1;}}
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
  int len=fd_seq_length(seq);
  if (len==0) {*n=0; return NULL;}
  else {
    fd_ptr_type ctype=FD_PTR_TYPE(seq);
    fdtype *vec=u8_alloc_n(len,fdtype);
    *n=len;
    switch (ctype) {
    case fd_packet_type: case fd_secret_type: {
      const unsigned char *packet=FD_PACKET_DATA(seq);
      int i=0; while (i < len) {
        int byte=packet[i];
        vec[i]=FD_INT2DTYPE(byte); i++;}
      break;}
    case fd_string_type: {
      int i=0;
      const u8_byte *scan=FD_STRING_DATA(seq),
        *limit=scan+FD_STRING_LENGTH(seq);
      while (scan<limit)
        if (*scan=='\0') {
          vec[i]=FD_CODE2CHAR(0); scan++; i++;}
        else {
          int ch=u8_sgetc(&scan);
          if (ch<0) break;
          vec[i]=FD_CODE2CHAR(ch); i++;}
      *n=i;
      break;}
    case fd_vector_type: case fd_rail_type: {
      int i=0;
      fdtype *scan=FD_VECTOR_DATA(seq),
        *limit=scan+FD_VECTOR_LENGTH(seq);
      while (scan<limit) {
        vec[i]=fd_incref(*scan); i++; scan++;}
      break;}
    case fd_pair_type: {
      int i=0; FD_DOLIST(elt,seq) {
        fdtype e=fd_incref(elt); vec[i++]=e;}
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
    struct U8_OUTPUT out; int i=0;
    if (n==0) return fd_make_string(NULL,0,"");
    U8_INIT_OUTPUT(&out,n*2);
    while (i < n) {
      if (FD_CHARACTERP(v[i])) u8_putc(&out,FD_CHAR2CODE(v[i]));
      else if (FD_FIXNUMP(v[i])) u8_putc(&out,FD_FIX2INT(v[i]));
      else {
        u8_free(out.u8_outbuf);
        return fd_type_error(_("character"),"fd_makeseq",v[i]);}
      i++;}
    return fd_stream2string(&out);}
  case fd_packet_type: case fd_secret_type: {
    fdtype result=FD_VOID;
    unsigned char *bytes=u8_malloc(n); int i=0;
    while (i < n) {
      if (FD_FIXNUMP(v[i])) bytes[i]=FD_FIX2INT(v[i]);
      else if (FD_CHARACTERP(v[i])) {
        unsigned int code=FD_CHAR2CODE(v[i]);
        bytes[i]=code&0xFF;}
      else {
        u8_free(bytes);
        return fd_type_error(_("byte"),"fd_makeseq",v[i]);}
      i++;}
    result=fd_make_packet(NULL,n,bytes);
    if (ctype==fd_secret_type) {
      FD_SET_CONS_TYPE(result,fd_secret_type);}
    u8_free(bytes);
    return result;}
  case fd_vector_type: {
    int i=0; while (i < n) {fd_incref(v[i]); i++;}
    return fd_make_vector(n,v);}
  case fd_rail_type: {
    int i=0; while (i < n) {fd_incref(v[i]); i++;}
    return fd_make_rail(n,v);}
  case fd_pair_type:
    if (n == 0) return FD_EMPTY_LIST;
    else {
      fdtype head=FD_EMPTY_LIST, *tail=&head;
      int i=0; while (i < n) {
        fdtype cons=fd_make_pair(v[i],FD_EMPTY_LIST);
        *tail=cons; tail=&(((struct FD_PAIR *)cons)->cdr);
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
    int i, j, len; fdtype *elts=fd_elts(sequence,&len), result;
    fdtype *tmp=((len) ? (u8_alloc_n(len,fdtype)) : (NULL));
    if (len) {
      i=0; j=len-1; while (i < len) {tmp[j]=elts[i]; i++; j--;}}
    result=fd_makeseq(FD_PTR_TYPE(sequence),len,tmp);
    i=0; while (i<len) {fd_decref(elts[i]); i++;}
    if (elts) u8_free(elts); if (tmp) u8_free(tmp);
    return result;}
}

typedef fdtype *fdtypep;

FD_EXPORT fdtype fd_append(int n,fdtype *sequences)
{
  if (n == 0) return FD_EMPTY_LIST;
  else {
    fd_ptr_type result_type=FD_PTR_TYPE(sequences[0]);
    fdtype result, **elts, *_elts[16], *combined;
    int i=0, k=0, *lengths, _lengths[16], total_length=0;
    if (FD_EMPTY_LISTP(sequences[0])) result_type=fd_pair_type;
    if (n>16) {
      lengths=u8_alloc_n(n,int);
      elts=u8_alloc_n(n,fdtypep);}
    else {lengths=_lengths; elts=_elts;}
    while (i < n) {
      fdtype seq=sequences[i];
      if ((FD_EMPTY_LISTP(seq)) && (result_type==fd_pair_type)) {}
      else if (FD_PTR_TYPE(seq) == result_type) {}
      else if ((result_type==fd_secret_type)&&
               ((FD_PTR_TYPE(seq)==fd_packet_type)||
                (FD_PTR_TYPE(seq)==fd_string_type))) {}
      else if ((result_type==fd_packet_type)&&(FD_PTR_TYPE(seq)==fd_secret_type))
        result_type=fd_secret_type;
      else if ((result_type==fd_string_type)&&(FD_PTR_TYPE(seq)==fd_secret_type))
        result_type=fd_secret_type;
      else if ((result_type==fd_string_type)&&(FD_PTR_TYPE(seq)==fd_packet_type))
        result_type=fd_packet_type;
      else if (FD_PTR_TYPE(seq) != result_type) result_type=fd_vector_type;
      elts[i]=fd_elts(seq,&(lengths[i]));
      total_length=total_length+lengths[i];
      if (lengths[i]==0) i++;
      else if (elts[i]==NULL)  {
        if (n>16) {u8_free(lengths); u8_free(elts);}
        return fd_type_error(_("sequence"),"fd_append",seq);}
      else i++;}
    combined=u8_alloc_n(total_length,fdtype);
    i=0; while (i < n) {
      int j=0, lim=lengths[i]; fdtype *seqelts=elts[i];
      while (j<lim) combined[k++]=seqelts[j++];
      u8_free(elts[i]);
      i++;}
    result=fd_makeseq(result_type,total_length,combined);
    i=0; while (i<total_length) {fd_decref(combined[i]); i++;}
    if (n>16) {u8_free(lengths); u8_free(elts);}
    u8_free(combined);
    return result;}
}

/* Mapping */

FD_EXPORT fdtype fd_mapseq(int n,fdtype *args)
{
  int i=1, n_seqs=n-1, seqlen=-1;
  fdtype fn=args[0], *sequences=args+1, firstseq=sequences[0];
  fdtype result, *results, _argvec[8], *argvec=NULL;
  fd_ptr_type result_type=FD_PTR_TYPE(firstseq);
  if (FD_PPTRP(fn)) fn=fd_pptr_ref(fn);
  if ((FD_TABLEP(fn)) || (FD_ATOMICP(fn))) {
    if (n_seqs>1)
      return fd_err(fd_TooManyArgs,"fd_foreach",NULL,fn);
    else if (FD_EMPTY_LISTP(firstseq)) return firstseq;}
  if (!(FD_SEQUENCEP(firstseq)))
    return fd_type_error("sequence","fd_mapseq",firstseq);
  else seqlen=fd_seq_length(firstseq);
  while (i<n_seqs)
    if (!(FD_SEQUENCEP(sequences[i])))
      return fd_type_error("sequence","fd_mapseq",sequences[i]);
    else if (seqlen!=fd_seq_length(sequences[i]))
      return fd_err(SequenceMismatch,"fd_foreach",NULL,sequences[i]);
    else i++;
  if (seqlen==0) return fd_incref(firstseq);
  results=u8_alloc_n(seqlen,fdtype);
  if (FD_APPLICABLEP(fn)) {
    if (n_seqs<8) argvec=_argvec;
    else argvec=u8_alloc_n(n_seqs,fdtype);}
  i=0; while (i < seqlen) {
    fdtype elt=fd_seq_elt(firstseq,i), new_elt;
    if (FD_APPLICABLEP(fn)) {
      int j=1; while (j<n_seqs) {
        argvec[j]=fd_seq_elt(sequences[j],i); j++;}
      argvec[0]=elt;
      new_elt=fd_apply(fn,n_seqs,argvec);
      j=1; while (j<n_seqs) {fd_decref(argvec[j]); j++;}}
    else if (FD_TABLEP(fn))
      new_elt=fd_get(fn,elt,elt);
    else if (FD_OIDP(elt))
      new_elt=fd_frame_get(elt,fn);
    else new_elt=fd_get(elt,fn,elt);
    fd_decref(elt);
    if (result_type == fd_string_type) {
      if (!(FD_PTR_TYPEP(new_elt,fd_character_type)))
        result_type=fd_vector_type;}
    else if ((result_type == fd_packet_type)||
             (result_type == fd_secret_type)) {
      if (FD_FIXNUMP(new_elt)) {
        int intval=FD_FIX2INT(new_elt);
        if ((intval<0) || (intval>=0x100))
          result_type=fd_vector_type;}
      else result_type=fd_vector_type;}
    if (FD_ABORTP(new_elt)) {
      int j=0; while (j<i) {fd_decref(results[j]); j++;}
      u8_free(results);
      return new_elt;}
    else results[i++]=new_elt;}
  result=fd_makeseq(result_type,seqlen,results);
  i=0; while (i<seqlen) {fd_decref(results[i]); i++;}
  u8_free(results);
  return result;
}

FD_EXPORT fdtype fd_foreach(int n,fdtype *args)
{
  int i=1, n_seqs=n-1, seqlen=-1;
  fdtype fn=args[0], *sequences=args+1, firstseq=sequences[0];
  fdtype _argvec[8], *argvec=NULL;
  fd_ptr_type result_type=FD_PTR_TYPE(firstseq);
  if ((FD_TABLEP(fn)) || (FD_ATOMICP(fn))) {
    if (n_seqs>1)
      return fd_err(fd_TooManyArgs,"fd_foreach",NULL,fn);
    else if (FD_EMPTY_LISTP(firstseq)) return firstseq;}
  if (!(FD_SEQUENCEP(firstseq)))
    return fd_type_error("sequence","fd_mapseq",firstseq);
  else seqlen=fd_seq_length(firstseq);
  while (i<n_seqs)
    if (!(FD_SEQUENCEP(sequences[i])))
      return fd_type_error("sequence","fd_mapseq",sequences[i]);
    else if (seqlen!=fd_seq_length(sequences[i]))
      return fd_err(SequenceMismatch,"fd_foreach",NULL,sequences[i]);
    else i++;
  if (seqlen==0) return FD_VOID;
  if (FD_APPLICABLEP(fn)) {
    if (n_seqs<8) argvec=_argvec;
    else argvec=u8_alloc_n(n_seqs,fdtype);}
  i=0; while (i < seqlen) {
    fdtype elt=fd_seq_elt(firstseq,i), new_elt;
    if (FD_TABLEP(fn))
      new_elt=fd_get(fn,elt,elt);
    else if (FD_OIDP(elt))
      new_elt=fd_frame_get(elt,fn);
    else if (FD_APPLICABLEP(fn)) {
      int j=1; while (j<n_seqs) {
        argvec[j]=fd_seq_elt(sequences[j],i); j++;}
      argvec[0]=elt;
      new_elt=fd_apply(fn,n_seqs,argvec);
      j=1; while (j<n_seqs) {fd_decref(argvec[j]); j++;}}
    else new_elt=fd_get(elt,fn,elt);
    fd_decref(elt);
    if (result_type == fd_string_type) {
      if (!(FD_PTR_TYPEP(new_elt,fd_character_type)))
        result_type=fd_vector_type;}
    else if ((result_type == fd_packet_type)||
             (result_type == fd_secret_type)) {
      if (FD_FIXNUMP(new_elt)) {
        int intval=FD_FIX2INT(new_elt);
        if ((intval<0) || (intval>=0x100))
          result_type=fd_vector_type;}
      else result_type=fd_vector_type;}
    if (FD_ABORTP(new_elt)) return new_elt;
    else {fd_decref(new_elt); i++;}}
  return FD_VOID;
}

FD_EXPORT fdtype fd_map2choice(fdtype fn,fdtype sequence)
{
  if (!(FD_SEQUENCEP(sequence)))
    return fd_type_error("sequence","fd_mapseq",sequence);
  else if (FD_EMPTY_LISTP(sequence)) return sequence;
  else if ((FD_APPLICABLEP(fn)) || (FD_TABLEP(fn)) || (FD_ATOMICP(fn))) {
    int i=0, len=fd_seq_length(sequence);
    fd_ptr_type result_type=FD_PTR_TYPE(sequence);
    fdtype *results=u8_alloc_n(len,fdtype);
    while (i < len) {
      fdtype elt=fd_seq_elt(sequence,i), new_elt;
      if (FD_TABLEP(fn))
        new_elt=fd_get(fn,elt,elt);
      else if (FD_APPLICABLEP(fn))
        new_elt=fd_apply(fn,1,&elt);
      else if (FD_OIDP(elt))
        new_elt=fd_frame_get(elt,fn);
      else new_elt=fd_get(elt,fn,elt);
      fd_decref(elt);
      if (result_type == fd_string_type) {
        if (!(FD_PTR_TYPEP(new_elt,fd_character_type)))
          result_type=fd_vector_type;}
      else if ((result_type == fd_packet_type)||
               (result_type == fd_secret_type)) {
        if (FD_FIXNUMP(new_elt)) {
          int intval=FD_FIX2INT(new_elt);
          if ((intval<0) || (intval>=0x100))
            result_type=fd_vector_type;}
        else result_type=fd_vector_type;}
      if (FD_ABORTP(new_elt)) {
        int j=0; while (j<i) {fd_decref(results[j]); j++;}
        u8_free(results);
        return new_elt;}
      else results[i++]=new_elt;}
    return fd_make_choice(len,results,0);}
  else return fd_err(fd_NotAFunction,"MAP",NULL,fn);
}

/* removal */

FD_EXPORT fdtype fd_remove(fdtype item,fdtype sequence)
{
  if (!(FD_SEQUENCEP(sequence)))
    return fd_type_error("sequence","fd_remove",sequence);
  else if (FD_EMPTY_LISTP(sequence)) return sequence;
  else {
    int i=0, j=0, removals=0, len=fd_seq_length(sequence);
    fd_ptr_type result_type=FD_PTR_TYPE(sequence);
    fdtype *results=u8_alloc_n(len,fdtype), result;
    while (i < len) {
      fdtype elt=fd_seq_elt(sequence,i); i++;
      if (FDTYPE_EQUAL(elt,item)) {removals++; fd_decref(elt);}
      else results[j++]=elt;}
    if (removals) {
      result=fd_makeseq(result_type,j,results);
      i=0; while (i<j) {fd_decref(results[i]); i++;}
      u8_free(results);
      return result;}
    else {
      i=0; while (i<j) {fd_decref(results[i]); i++;}
      u8_free(results);
      return fd_incref(sequence);}}
}

FD_EXPORT int applytest(fdtype test,fdtype elt)
{
  if (FD_HASHSETP(test))
    return fd_hashset_get(FD_XHASHSET(test),elt);
  else if (FD_HASHTABLEP(test))
    return fd_hashtable_probe(FD_XHASHTABLE(test),elt);
  else if ((FD_SYMBOLP(test)) || (FD_OIDP(test)))
    return fd_test(elt,test,FD_VOID);
  else if (FD_TABLEP(test))
    return fd_test(test,elt,FD_VOID);
  else if (FD_APPLICABLEP(test)) {
    fdtype result=fd_apply(test,1,&elt);
    if (FD_FALSEP(result)) return 0;
    else {fd_decref(result); return 1;}}
  else if (FD_EMPTY_CHOICEP(test)) return 0;
  else if (FD_CHOICEP(test)) {
    FD_DO_CHOICES(each_test,test) {
      int result=applytest(each_test,elt);
      if (result<0) return result;
      else if (result) return result;}
    return 0;}
  else {
    fd_seterr(fd_TypeError,"removeif",u8_strdup(_("test")),test);
    return -1;}
}

FD_EXPORT fdtype fd_removeif(fdtype test,fdtype sequence,int invert)
{
  if (!(FD_SEQUENCEP(sequence)))
    return fd_type_error("sequence","fd_remove",sequence);
  else if (FD_EMPTY_LISTP(sequence)) return sequence;
  else {
    int i=0, j=0, removals=0, len=fd_seq_length(sequence);
    fd_ptr_type result_type=FD_PTR_TYPE(sequence);
    fdtype *results=u8_alloc_n(len,fdtype), result;
    while (i < len) {
      fdtype elt=fd_seq_elt(sequence,i);
      int compare=applytest(test,elt); i++;
      if (compare<0) {u8_free(results); return -1;}
      else if ((invert) ? (!(compare)) : (compare)) {
        removals++; fd_decref(elt);}
      else results[j++]=elt;}
    if (removals) {
      result=fd_makeseq(result_type,j,results);
      i=0; while (i<j) {fd_decref(results[i]); i++;}
      u8_free(results);
      return result;}
    else {
      i=0; while (i<j) {fd_decref(results[i]); i++;}
      u8_free(results);
      return fd_incref(sequence);}}
}


/* Reduction */

FD_EXPORT fdtype fd_reduce(fdtype fn,fdtype sequence,fdtype result)
{
  int i=0, len=fd_seq_length(sequence);
  if (len==0)
    if (FD_VOIDP(result))
      return fd_err(EmptyReduce,"fd_reduce",NULL,sequence);
    else return fd_incref(result);
  else if (len<0)
    return FD_ERROR_VALUE;
  else if (!(FD_APPLICABLEP(fn)))
    return fd_err(fd_NotAFunction,"MAP",NULL,fn);
  else if (FD_VOIDP(result)) {
    result=fd_seq_elt(sequence,0); i=1;}
  while (i < len) {
    fdtype elt=fd_seq_elt(sequence,i), rail[2], new_result;
    rail[0]=elt; rail[1]=result;
    new_result=fd_apply(fn,2,rail);
    fd_decref(result); fd_decref(elt);
    result=new_result;
    i++;}
  return result;
}

/* Scheme primitives */

static fdtype sequencep_prim(fdtype x)
{
  if (FD_SEQUENCEP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype seqlen_prim(fdtype x)
{
  int len=fd_seq_length(x);
  if (len<0) return fd_type_error(_("sequence"),"seqlen",x);
  else return FD_INT2DTYPE(len);
}

static fdtype seqelt_prim(fdtype x,fdtype offset)
{
  char buf[16]; int off; fdtype result;
  if (!(FD_SEQUENCEP(x)))
    return fd_type_error(_("sequence"),"seqelt_prim",x);
  else if (FD_FIXNUMP(offset)) off=FD_FIX2INT(offset);
  else return fd_type_error(_("fixnum"),"seqelt_prim",offset);
  if (off<0) off=fd_seq_length(x)+off;
  result=fd_seq_elt(x,off);
  if (result == FD_TYPE_ERROR)
    return fd_type_error(_("sequence"),"seqelt_prim",x);
  else if (result == FD_RANGE_ERROR) {
    sprintf(buf,"%d",fd_getint(offset));
    return fd_err(fd_RangeError,"seqelt_prim",u8_strdup(buf),x);}
  else return result;
}

/* This is for use in filters, especially PICK and REJECT */

enum COMPARISON {
  cmp_lt, cmp_lte, cmp_eq, cmp_gte, cmp_gt };

static int has_length_helper(fdtype x,fdtype length_arg,enum COMPARISON cmp)
{
  int seqlen=fd_seq_length(x), testlen;
  if (seqlen<0) return fd_type_error(_("sequence"),"seqlen",x);
  else if (FD_FIXNUMP(length_arg))
    testlen=(FD_FIX2INT(length_arg));
  else return fd_type_error(_("fixnum"),"has-length?",x);
  switch (cmp) {
  case cmp_lt: return (seqlen<testlen);
  case cmp_lte: return (seqlen<=testlen);
  case cmp_eq: return (seqlen==testlen);
  case cmp_gte: return (seqlen>=testlen);
  case cmp_gt: return (seqlen>testlen);
  default:
    fd_seterr("Unknown length comparison","has_length_helper",NULL,x);
    return -1;}
}

static fdtype has_length_prim(fdtype x,fdtype length_arg)
{
  int retval=has_length_helper(x,length_arg,cmp_eq);
  if (retval<0) return FD_ERROR_VALUE;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype has_length_lt_prim(fdtype x,fdtype length_arg)
{
  int retval=has_length_helper(x,length_arg,cmp_lt);
  if (retval<0) return FD_ERROR_VALUE;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype has_length_lte_prim(fdtype x,fdtype length_arg)
{
  int retval=has_length_helper(x,length_arg,cmp_lte);
  if (retval<0) return FD_ERROR_VALUE;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype has_length_gt_prim(fdtype x,fdtype length_arg)
{
  int retval=has_length_helper(x,length_arg,cmp_gt);
  if (retval<0) return FD_ERROR_VALUE;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype has_length_gte_prim(fdtype x,fdtype length_arg)
{
  int retval=has_length_helper(x,length_arg,cmp_gte);
  if (retval<0) return FD_ERROR_VALUE;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype has_length_gt_zero_prim(fdtype x)
{
  int seqlen=fd_seq_length(x);
  if (seqlen>0) return FD_TRUE; else return FD_FALSE;
}

static fdtype has_length_gt_one_prim(fdtype x)
{
  int seqlen=fd_seq_length(x);
  if (seqlen>1) return FD_TRUE; else return FD_FALSE;
}

/* Element access */

/* We separate this out to give the compiler the option of not inline coding this guy. */
static fdtype check_empty_list_range
  (u8_string prim,fdtype seq,
   fdtype start_arg,fdtype end_arg,
   int *startp,int *endp)
{
  if ((FD_FALSEP(start_arg)) ||
      (FD_VOIDP(start_arg)) ||
      (start_arg==FD_INT2DTYPE(0))) {}
  else if (FD_FIXNUMP(start_arg))
    return fd_err(fd_RangeError,prim,"start",start_arg);
  else return fd_type_error("fixnum",prim,start_arg);
  if ((FD_FALSEP(end_arg)) ||
      (FD_VOIDP(end_arg)) ||
      (end_arg==FD_INT2DTYPE(0))) {}
  else if (FD_FIXNUMP(end_arg))
    return fd_err(fd_RangeError,prim,"start",end_arg);
  else return fd_type_error("fixnum",prim,end_arg);
  *startp=0; *endp=0;
  return FD_VOID;
}

static fdtype check_range(u8_string prim,fdtype seq,
                           fdtype start_arg,fdtype end_arg,
                           int *startp,int *endp)
{
  if (FD_EMPTY_LISTP(seq))
    return check_empty_list_range(prim,seq,start_arg,end_arg,startp,endp);
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error(_("sequence"),prim,seq);
  else if (FD_FIXNUMP(start_arg)) {
    int arg=FD_FIX2INT(start_arg);
    if (arg<0)
      return fd_err(fd_RangeError,prim,"start",start_arg);
    else *startp=arg;}
  else if ((FD_VOIDP(start_arg)) || (FD_FALSEP(start_arg)))
    *startp=0;
  else return fd_type_error("fixnum",prim,start_arg);
  if ((FD_VOIDP(end_arg)) || (FD_FALSEP(end_arg)))
    *endp=fd_seq_length(seq);
  else if (FD_FIXNUMP(end_arg)) {
    int arg=FD_FIX2INT(end_arg);
    if (arg<0) {
      int len=fd_seq_length(seq), off=len+arg;
      if (off>=0) *endp=off;
      else return fd_err(fd_RangeError,prim,"start",end_arg);}
    else if (arg>fd_seq_length(seq))
      return fd_err(fd_RangeError,prim,"start",end_arg);
    else *endp=arg;}
  else return fd_type_error("fixnum",prim,end_arg);
  return FD_VOID;
}

static fdtype slice_prim(fdtype x,fdtype start_arg,fdtype end_arg)
{
  int start, end; char buf[32];
  fdtype result=check_range("slice_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTP(result)) return result;
  else result=fd_slice(x,start,end);
  if (result == FD_TYPE_ERROR)
    return fd_type_error(_("sequence"),"slice_prim",x);
  else if (result == FD_RANGE_ERROR) {
    sprintf(buf,"%d[%d:%d]",fd_seq_length(x),start,end);
    return fd_err(fd_RangeError,"slice_prim",u8_strdup(buf),x);}
  else return result;
}

static fdtype position_prim(fdtype key,fdtype x,fdtype start_arg,fdtype end_arg)
{
  int result, start, end; char buf[32];
  fdtype check=check_range("position_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTP(check)) return check;
  else result=fd_position(key,x,start,end);
  if (result>=0) return FD_INT2DTYPE(result);
  else if (result == -1) return FD_FALSE;
  else if (result == -2)
    return fd_type_error(_("sequence"),"position_prim",x);
  else if (result == -3) {
    sprintf(buf,"%d[%d:%d]",fd_seq_length(x),start,end);
    return fd_err(fd_RangeError,"position_prim",u8_strdup(buf),x);}
  else return FD_INT2DTYPE(result);
}

static fdtype rposition_prim(fdtype key,fdtype x,fdtype start_arg,fdtype end_arg)
{
  int result, start, end; char buf[32];
  fdtype check=check_range("rposition_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTP(check)) return check;
  else result=fd_rposition(key,x,start,end);
  if (result>=0) return FD_INT2DTYPE(result);
  else if (result == -1) return FD_FALSE;
  else if (result == -2)
    return fd_type_error(_("sequence"),"rposition_prim",x);
  else if (result == -3) {
    sprintf(buf,"%d[%d:%d]",fd_seq_length(x),start,end);
    return fd_err(fd_RangeError,"rposition_prim",u8_strdup(buf),x);}
  else return FD_INT2DTYPE(result);
}

static fdtype find_prim(fdtype key,fdtype x,fdtype start_arg,fdtype end_arg)
{
  int result, start, end; char buf[32];
  fdtype check=check_range("find_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTP(check)) return check;
  else result=fd_position(key,x,start,end);
  if (result>=0) return FD_TRUE;
  else if (result == -1) return FD_FALSE;
  else if (result == -2)
    return fd_type_error(_("sequence"),"find_prim",x);
  else if (result == -3) {
    sprintf(buf,"%d[%d:%d]",fd_seq_length(x),start,end);
    return fd_err(fd_RangeError,"find_prim",u8_strdup(buf),x);}
  else return FD_FALSE;
}

static fdtype search_prim(fdtype key,fdtype x,fdtype start_arg,fdtype end_arg)
{
  int result, start, end; char buf[32];
  fdtype check=check_range("search_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTP(check)) return check;
  else result=fd_search(key,x,start,end);
  if (result>=0) return FD_INT2DTYPE(result);
  else if (result == -1) return FD_FALSE;
  else if (result == -2) {
    sprintf(buf,"%d:%d",start,end);
    return fd_err(fd_RangeError,"search_prim",u8_strdup(buf),x);}
  else if (result == -3)
    return fd_type_error(_("sequence"),"search_prim",x);
  else return result;
}

static fdtype every_prim(fdtype proc,fdtype x,fdtype start_arg,fdtype end_arg)
{
  int start, end;
  fdtype check=check_range("every_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTP(check)) return check;
  else if (!(FD_APPLICABLEP(proc)))
    return fd_type_error(_("function"),"every_prim",x);
  else if (FD_STRINGP(x)) {
    const u8_byte *scan=FD_STRDATA(x);
    int i=0; while (i<end) {
      int c=u8_sgetc(&scan);
      if (i<start) i++;
      else if (i<end) {
        fdtype lc=FD_CODE2CHAR(c);
        fdtype testval=fd_apply(proc,1,&lc);
        if (FD_FALSEP(testval)) return FD_FALSE;
        else if (FD_ABORTP(testval)) return testval;
        else {
          fd_decref(testval); i++;}}
      else return FD_TRUE;}
    return FD_TRUE;}
  else {
    int i=start; while (i<end) {
      fdtype elt=fd_seq_elt(x,i);
      fdtype testval=fd_apply(proc,1,&elt);
      if (FD_ABORTP(testval)) {
        fd_decref(elt); return testval;}
      else if (FD_FALSEP(testval)) {
        fd_decref(elt); return FD_FALSE;}
      else {
        fd_decref(elt); fd_decref(testval); i++;}}
    return FD_TRUE;}
}

static fdtype some_prim(fdtype proc,fdtype x,fdtype start_arg,fdtype end_arg)
{
  int start, end;
  fdtype check=check_range("some_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTP(check)) return check;
  else if (!(FD_APPLICABLEP(proc)))
    return fd_type_error(_("function"),"some_prim",x);
  else if (FD_STRINGP(x)) {
    const u8_byte *scan=FD_STRDATA(x);
    int i=0; while (i<end) {
      int c=u8_sgetc(&scan);
      if (i<start) i++;
      else if (i< end) {
        fdtype lc=FD_CODE2CHAR(c);
        fdtype testval=fd_apply(proc,1,&lc);
        if (FD_ABORTP(testval)) return testval;
        else if (FD_FALSEP(testval)) i++;
        else {
          fd_decref(testval); return FD_TRUE;}}
      else return FD_FALSE;}
    return FD_FALSE;}
  else {
    int i=start; while (i<end) {
      fdtype elt=fd_seq_elt(x,i);
      fdtype testval=fd_apply(proc,1,&elt);
      if (FD_ABORTP(testval)) {
        fd_decref(elt); return testval;}
      else if ((FD_FALSEP(testval)) || (FD_EMPTY_CHOICEP(testval))) {
        fd_decref(elt); i++;}
      else {
        fd_decref(elt); fd_decref(testval);
        return FD_TRUE;}}
    return FD_FALSE;}
}

static fdtype removeif_prim(fdtype test,fdtype sequence)
{
  if (FD_EMPTY_CHOICEP(sequence)) return sequence;
  else if (FD_CHOICEP(sequence)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(seq,sequence) {
      fdtype r=fd_removeif(test,seq,0);
      if (FD_ABORTP(r)) {
        fd_decref(results); return r;}
      FD_ADD_TO_CHOICE(results,r);}
    return results;}
  else return fd_removeif(test,sequence,0);
}

static fdtype removeifnot_prim(fdtype test,fdtype sequence)
{
  if (FD_EMPTY_CHOICEP(sequence)) return sequence;
  else if (FD_CHOICEP(sequence)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(seq,sequence) {
      fdtype r=fd_removeif(test,seq,1);
      if (FD_ABORTP(r)) {
        fd_decref(results); return r;}
      FD_ADD_TO_CHOICE(results,r);}
    return results;}
  else return fd_removeif(test,sequence,1);
}

/* Small element functions */

static fdtype seq_elt(fdtype x,char *cxt,int i)
{
  fdtype v=fd_seq_elt(x,i);
  if (FD_TROUBLEP(v))
    return fd_err(fd_retcode_to_exception(v),cxt,NULL,x);
  else return v;
}

static fdtype first(fdtype x)
{
  return seq_elt(x,"first",0);
}
static fdtype rest(fdtype x)
{
  if (FD_PAIRP(x)) return fd_incref(FD_CDR(x));
  else {
    fdtype v=fd_slice(x,1,-1);
    if (FD_TROUBLEP(v))
      return fd_err(fd_retcode_to_exception(v),"rest",NULL,x);
    else return v;}
}

static fdtype second(fdtype x)
{
  return seq_elt(x,"second",1);
}
static fdtype third(fdtype x)
{
  return seq_elt(x,"third",2);
}
static fdtype fourth(fdtype x)
{
  return seq_elt(x,"fourth",3);
}
static fdtype fifth(fdtype x)
{
  return seq_elt(x,"fifth",4);
}
static fdtype sixth(fdtype x)
{
  return seq_elt(x,"sixth",5);
}
static fdtype seventh(fdtype x)
{
  return seq_elt(x,"seventh",6);
}

/* Pair functions */

static fdtype nullp(fdtype x)
{
  if (FD_EMPTY_LISTP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype cons(fdtype x,fdtype y)
{
  return fd_make_pair(x,y);
}
static fdtype car(fdtype x)
{
  return fd_incref(FD_CAR(x));
}
static fdtype cdr(fdtype x)
{
  return fd_incref(FD_CDR(x));
}
static fdtype cddr(fdtype x)
{
  if (FD_PAIRP(FD_CDR(x)))
    return fd_incref(FD_CDR(FD_CDR(x)));
  else return fd_err(fd_RangeError,"CDDR",NULL,x);
}
static fdtype cadr(fdtype x)
{
  if (FD_PAIRP(FD_CDR(x)))
    return fd_incref(FD_CAR(FD_CDR(x)));
  else return fd_err(fd_RangeError,"CADR",NULL,x);
}
static fdtype cdar(fdtype x)
{
  if (FD_PAIRP(FD_CAR(x)))
    return fd_incref(FD_CDR(FD_CAR(x)));
  else return fd_err(fd_RangeError,"CDAR",NULL,x);
}
static fdtype caar(fdtype x)
{
  if (FD_PAIRP(FD_CAR(x)))
    return fd_incref(FD_CAR(FD_CAR(x)));
  else return fd_err(fd_RangeError,"CAAR",NULL,x);
}
static fdtype caddr(fdtype x)
{
  if ((FD_PAIRP(FD_CDR(x)))&&(FD_PAIRP(FD_CDR(FD_CDR(x)))))
    return fd_incref(FD_CAR(FD_CDR(FD_CDR(x))));
  else return fd_err(fd_RangeError,"CADR",NULL,x);
}
static fdtype cdddr(fdtype x)
{
  if ((FD_PAIRP(FD_CDR(x)))&&(FD_PAIRP(FD_CDR(FD_CDR(x)))))
    return fd_incref(FD_CDR(FD_CDR(FD_CDR(x))));
  else return fd_err(fd_RangeError,"CADR",NULL,x);
}

static fdtype cons_star(int n,fdtype *args)
{
  int i=n-2; fdtype list=fd_incref(args[n-1]);
  while (i>=0) {
    list=fd_init_pair(NULL,fd_incref(args[i]),list);
    i--;}
  return list;
}

/* Association list functions */

static fdtype assq_prim(fdtype key,fdtype list)
{
  if (FD_EMPTY_LISTP(list)) return FD_FALSE;
  else if (!(FD_PAIRP(list)))
    return fd_type_error("alist","assq_prim",list);
  else {
    fdtype scan=list;
    while (FD_PAIRP(scan))
      if (!(FD_PAIRP(FD_CAR(scan))))
        return fd_type_error("alist","assq_prim",list);
      else {
        fdtype item=FD_CAR(FD_CAR(scan));
        if (FD_EQ(key,item))
          return fd_incref(FD_CAR(scan));
        else scan=FD_CDR(scan);}
    return FD_FALSE;}
}

static fdtype assv_prim(fdtype key,fdtype list)
{
  if (FD_EMPTY_LISTP(list)) return FD_FALSE;
  else if (!(FD_PAIRP(list)))
    return fd_type_error("alist","assv_prim",list);
  else {
    fdtype scan=list;
    while (FD_PAIRP(scan))
      if (!(FD_PAIRP(FD_CAR(scan))))
        return fd_type_error("alist","assv_prim",list);
      else {
        fdtype item=FD_CAR(FD_CAR(scan));
        if (FD_EQV(key,item))
          return fd_incref(FD_CAR(scan));
        else scan=FD_CDR(scan);}
    return FD_FALSE;}
}

static fdtype assoc_prim(fdtype key,fdtype list)
{
  if (FD_EMPTY_LISTP(list)) return FD_FALSE;
  else if (!(FD_PAIRP(list)))
    return fd_type_error("alist","assoc_prim",list);
  else {
    fdtype scan=list;
    while (FD_PAIRP(scan))
      if (!(FD_PAIRP(FD_CAR(scan))))
        return fd_type_error("alist","assoc_prim",list);
      else {
        fdtype item=FD_CAR(FD_CAR(scan));
        if (FDTYPE_EQUAL(key,item))
          return fd_incref(FD_CAR(scan));
        else scan=FD_CDR(scan);}
    return FD_FALSE;}
}

/* MEMBER functions */

static fdtype memq_prim(fdtype key,fdtype list)
{
  if (FD_EMPTY_LISTP(list)) return FD_FALSE;
  else if (FD_PAIRP(list)) {
    fdtype scan=list;
    while (FD_PAIRP(scan))
      if (FD_EQ(FD_CAR(scan),key))
        return fd_incref(scan);
      else scan=FD_CDR(scan);
    return FD_FALSE;}
  else return fd_type_error("list","memq_prim",list);
}

static fdtype memv_prim(fdtype key,fdtype list)
{
  if (FD_EMPTY_LISTP(list)) return FD_FALSE;
  else if (FD_PAIRP(list)) {
    fdtype scan=list;
    while (FD_PAIRP(scan))
      if (FD_EQV(FD_CAR(scan),key))
        return fd_incref(scan);
      else scan=FD_CDR(scan);
    return FD_FALSE;}
  else return fd_type_error("list","memv_prim",list);
}

static fdtype member_prim(fdtype key,fdtype list)
{
  if (FD_EMPTY_LISTP(list)) return FD_FALSE;
  else if (FD_PAIRP(list)) {
    fdtype scan=list;
    while (FD_PAIRP(scan))
      if (FD_EQUAL(FD_CAR(scan),key))
        return fd_incref(scan);
      else scan=FD_CDR(scan);
    return FD_FALSE;}
  else return fd_type_error("list","member_prim",list);
}

/* LIST AND VECTOR */

static fdtype list(int n,fdtype *elts)
{
  fdtype head=FD_EMPTY_LIST, *tail=&head; int i=0;
  while (i < n) {
    fdtype pair=fd_make_pair(elts[i],FD_EMPTY_LIST);
    *tail=pair; tail=&((struct FD_PAIR *)pair)->cdr; i++;}
  return head;
}

static fdtype vector(int n,fdtype *elts)
{
  int i=0; while (i < n) {fd_incref(elts[i]); i++;}
  return fd_make_vector(n,elts);
}

static fdtype make_vector(fdtype size,fdtype dflt)
{
  int n=fd_getint(size);
  if (n==0)
    return fd_init_vector(NULL,0,NULL);
  else if (n>0) {
    fdtype result=fd_init_vector(NULL,n,NULL);
    fdtype *elts=FD_VECTOR_ELTS(result);
    int i=0; while (i < n) {
      elts[i]=fd_incref(dflt); i++;}
    return result;}
  else return fd_type_error(_("positive"),"make_vector",size);
}

static fdtype seq2vector(fdtype seq)
{
  if (FD_EMPTY_LISTP(seq))
    return fd_init_vector(NULL,0,NULL);
  else if (FD_SEQUENCEP(seq)) {
    int n; fdtype *data=fd_elts(seq,&n);
    fdtype result=fd_make_vector(n,data);
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2vector",seq);
}

static fdtype onevector_prim(int n,fdtype *args)
{
  fdtype elts[32], result=FD_VOID;
  struct U8_PILE pile; int i=0;
  if (n==0) return fd_init_vector(NULL,0,NULL);
  else if (n==1) {
    if (FD_VECTORP(args[0])) return fd_incref(args[0]);
    else if (FD_PAIRP(args[0])) {}
    else if ((FD_EMPTY_CHOICEP(args[0]))||(FD_EMPTY_QCHOICEP(args[0])))
      return fd_init_vector(NULL,0,NULL);
    else if (!(FD_CONSP(args[0])))
      return fd_make_vector(1,args);
    else {
      fd_incref(args[0]); return fd_make_vector(1,args);}}
  U8_INIT_STATIC_PILE((&pile),elts,32);
  while (i<n) {
    fdtype arg=args[i++];
    FD_DO_CHOICES(each,arg) {
      if (!(FD_CONSP(each))) {u8_pile_add((&pile),each);}
      else if (FD_VECTORP(each)) {
        int len=FD_VECTOR_LENGTH(each); int j=0;
        while (j<len) {
          fdtype elt=FD_VECTOR_REF(each,j); fd_incref(elt);
          u8_pile_add(&pile,elt); j++;}}
      else if (FD_PAIRP(each)) {
        FD_DOLIST(elt,each) {
          fd_incref(elt); u8_pile_add(&pile,elt);}}
      else {
        fd_incref(each); u8_pile_add(&pile,each);}}}
  result=fd_make_vector(pile.u8_len,(fdtype *)pile.u8_elts);
  if (pile.u8_mallocd) u8_free(pile.u8_elts);
  return result;
}

static fdtype seq2rail(fdtype seq)
{
  if (FD_EMPTY_LISTP(seq))
    return fd_make_rail(0,NULL);
  else if (FD_SEQUENCEP(seq)) {
    int n; fdtype *data=fd_elts(seq,&n);
    fdtype result=fd_make_rail(n,data);
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2rail",seq);
}

static fdtype seq2list(fdtype seq)
{
  if (FD_EMPTY_LISTP(seq)) return FD_EMPTY_LIST;
  else if (FD_SEQUENCEP(seq)) {
    int n; fdtype *data=fd_elts(seq,&n), result=FD_EMPTY_LIST;
    n--; while (n>=0) {
      result=fd_init_pair(NULL,data[n],result); n--;}
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2list",seq);
}

static fdtype seq2packet(fdtype seq)
{
  if (FD_EMPTY_LISTP(seq))
    return fd_init_packet(NULL,0,NULL);
  else if (FD_SEQUENCEP(seq)) {
    int i=0, n;
    fdtype result=FD_VOID;
    fdtype *data=fd_elts(seq,&n);
    unsigned char *bytes=u8_malloc(n);
    while (i<n) {
      if (FD_FIXNUMP(data[i])) {
        bytes[i]=FD_FIX2INT(data[i]); i++;}
      else {
        fdtype bad=fd_incref(data[i]);
        i=0; while (i < n) {fd_decref(data[i]); i++;}
        u8_free(data);
        return fd_type_error(_("byte"),"seq2packet",bad);}}
    u8_free(data);
    result=fd_make_packet(NULL,n,bytes);
    u8_free(bytes);
    return result;}
  else return fd_type_error(_("sequence"),"seq2packet",seq);
}

static fdtype x2string(fdtype seq)
{
  if (FD_SYMBOLP(seq))
    return fdtype_string(FD_SYMBOL_NAME(seq));
  else if (FD_CHARACTERP(seq)) {
    int c=FD_CHAR2CODE(seq);
    U8_OUTPUT out; u8_byte buf[16];
    U8_INIT_STATIC_OUTPUT_BUF(out,16,buf);
    u8_putc(&out,c);
    return fdtype_string(out.u8_outbuf);}
  else if (FD_EMPTY_LISTP(seq)) return fdtype_string("");
  else if (FD_STRINGP(seq)) return fd_incref(seq);
  else if (FD_SEQUENCEP(seq)) {
    U8_OUTPUT out;
    int i=0, n;
    fdtype *data=fd_elts(seq,&n);
    U8_INIT_OUTPUT(&out,n*2);
    while (i<n) {
      if (FD_FIXNUMP(data[i])) {
        int charcode=FD_FIX2INT(data[i]);
        if (charcode>=0x10000) {
          fdtype bad=fd_incref(data[i]);
          i=0; while (i<n) {fd_decref(data[i]); i++;}
          u8_free(data);
          return fd_type_error(_("character"),"seq2string",bad);}
        u8_putc(&out,charcode); i++;}
      else if (FD_CHARACTERP(data[i])) {
        int charcode=FD_CHAR2CODE(data[i]);
        u8_putc(&out,charcode); i++;}
      else {
        fdtype bad=fd_incref(data[i]);
        i=0; while (i<n) {fd_decref(data[i]); i++;}
        u8_free(data);
        return fd_type_error(_("character"),"seq2string",bad);}}
    u8_free(data);
    return fd_stream2string(&out);}
  else return fd_type_error(_("sequence"),"x2string",seq);
}

FD_EXPORT fdtype fd_seq_elts(fdtype x)
{
  if (FD_CHOICEP(x)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(e,x) {
      fdtype subelts=fd_seq_elts(e);
      FD_ADD_TO_CHOICE(results,subelts);}
    return results;}
  else {
    fdtype result=FD_EMPTY_CHOICE;
    int ctype=FD_PTR_TYPE(x), i=0, len;
    switch (ctype) {
    case fd_vector_type: case fd_rail_type:
      len=FD_VECTOR_LENGTH(x); while (i < len) {
        fdtype elt=fd_incref(FD_VECTOR_REF(x,i));
        FD_ADD_TO_CHOICE(result,elt); i++;}
      return result;
    case fd_packet_type: case fd_secret_type:
      len=FD_PACKET_LENGTH(x); while (i < len) {
        fdtype elt=FD_BYTE2LISP(FD_PACKET_REF(x,i));
        FD_ADD_TO_CHOICE(result,elt); i++;}
      return result;
    case fd_pair_type: {
      fdtype scan=x;
      while (FD_PAIRP(scan)) {
        fdtype elt=fd_incref(FD_CAR(scan));
        FD_ADD_TO_CHOICE(result,elt);
        scan=FD_CDR(scan);}
      if (!(FD_EMPTY_LISTP(scan))) {
        fdtype tail=fd_incref(scan);
        FD_ADD_TO_CHOICE(result,tail);}
      return result;}
    case fd_string_type: {
      const u8_byte *scan=FD_STRDATA(x);
      int c=u8_sgetc(&scan);
      while (c>=0) {
        FD_ADD_TO_CHOICE(result,FD_CODE2CHAR(c));
        c=u8_sgetc(&scan);}
      return result;}
    default:
      len=fd_seq_length(x); while (i<len) {
        fdtype elt=fd_seq_elt(x,i);
        FD_ADD_TO_CHOICE(result,elt); i++;}
      return result;
    }}
}

static fdtype elts_prim(fdtype x,fdtype start_arg,fdtype end_arg)
{
  int start, end;
  fdtype check=check_range("elts_prim",x,start_arg,end_arg,&start,&end);
  fdtype results=FD_EMPTY_CHOICE;
  int ctype=FD_PTR_TYPE(x);
  if (FD_ABORTP(check)) return check;
  else switch (ctype) {
    case fd_vector_type: case fd_rail_type: {
      fdtype *read, *limit;
      read=FD_VECTOR_DATA(x)+start; limit=FD_VECTOR_DATA(x)+end;
      while (read<limit) {
        fdtype v=*read++; fd_incref(v);
        FD_ADD_TO_CHOICE(results,v);}
      return results;}
    case fd_packet_type: case fd_secret_type: {
      const unsigned char *read=FD_PACKET_DATA(x), *lim=read+end;
      while (read<lim) {
        int v=*read++;
        FD_ADD_TO_CHOICE(results,FD_INT2DTYPE(v));}
      return results;}
    case fd_pair_type: {
      int j=0; fdtype scan=x;
      while (FD_PAIRP(scan))
        if (j==end) return results;
        else if (j>=start) {
          fdtype car=FD_CAR(scan); fd_incref(car);
          FD_ADD_TO_CHOICE(results,car);
          j++; scan=FD_CDR(scan);}
        else {j++; scan=FD_CDR(scan);}
      return results;}
    case fd_string_type: {
      int count=0, c;
      const u8_byte *scan=FD_STRDATA(x);
      while ((c=u8_sgetc(&scan))>=0)
        if (count<start) count++;
        else if (count>=end) break;
        else {FD_ADD_TO_CHOICE(results,FD_CODE2CHAR(c));}
      return results;}
    default:
      if (FD_EMPTY_LISTP(x))
        if ((start == end) && (start == 0))
          return FD_EMPTY_CHOICE;
        else return FD_RANGE_ERROR;
      else if ((fd_seqfns[ctype]) && (fd_seqfns[ctype]->elt)) {
        int scan=start; while (start<end) {
          fdtype elt=(fd_seqfns[ctype]->elt)(x,scan);
          FD_ADD_TO_CHOICE(results,elt); scan++;}
        return results;}
      else if (fd_seqfns[ctype]==NULL)
        return fd_type_error(_("sequence"),"elts_prim",x);
      else return fd_err(fd_NoMethod,"elts_prim",NULL,x);}
  return results;
}

static fdtype vec2elts_prim(fdtype x)
{
  if (FD_VECTORP(x)) {
    fdtype result=FD_EMPTY_CHOICE;
    int i=0; int len=FD_VECTOR_LENGTH(x); fdtype *elts=FD_VECTOR_ELTS(x);
    while (i<len) {
      fdtype e=elts[i++]; fd_incref(e); FD_ADD_TO_CHOICE(result,e);}
    return result;}
  else return fd_incref(x);
}

/* Matching vectors */

static fdtype seqmatch_prim(fdtype prefix,fdtype seq,fdtype startarg)
{
  int start=FD_FIX2INT(startarg);
  if ((FD_VECTORP(prefix))&&(FD_VECTORP(seq))) {
    int plen=FD_VECTOR_LENGTH(prefix);
    int seqlen=FD_VECTOR_LENGTH(seq);
    if ((start+plen)<=seqlen) {
      int j=0; int i=start;
      while (j<plen)
        if (FD_EQUAL(FD_VECTOR_REF(prefix,j),FD_VECTOR_REF(seq,i))) {
          j++; i++;}
        else return FD_FALSE;
      return FD_TRUE;}
    else return FD_FALSE;}
  else if ((FD_STRINGP(prefix))&&(FD_STRINGP(seq))) {
    int plen=FD_STRLEN(prefix);
    int seqlen=FD_STRLEN(seq);
    int off=u8_byteoffset(FD_STRDATA(seq),start,seqlen);
    if ((off+plen)<=seqlen)
      if (strncmp(FD_STRDATA(prefix),FD_STRDATA(seq)+off,plen)==0)
        return FD_TRUE;
      else return FD_FALSE;
    else return FD_FALSE;}
  else if (!(FD_SEQUENCEP(prefix)))
    return fd_type_error("sequence","seqmatch_prim",prefix);
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error("sequence","seqmatch_prim",seq);
  else {
    int plen=fd_seq_length(prefix);
    int seqlen=fd_seq_length(seq);
    if ((start+plen)<=seqlen) {
      int j=0; int i=start;
      while (j<plen) {
        fdtype pelt=fd_seq_elt(prefix,j);
        fdtype velt=fd_seq_elt(prefix,i);
        int cmp=FD_EQUAL(pelt,velt);
        fd_decref(pelt); fd_decref(velt);
        if (cmp) {j++; i++;}
        else return FD_FALSE;}
      return FD_TRUE;}
    else return FD_FALSE;}
}

/* Sorting vectors */

static fdtype sortvec_primfn(fdtype vec,fdtype keyfn,int reverse,int lexsort)
{
  if (FD_VECTOR_LENGTH(vec)==0)
    return fd_init_vector(NULL,0,NULL);
  else if (FD_VECTOR_LENGTH(vec)==1)
    return fd_incref(vec);
  else {
    int i=0, n=FD_VECTOR_LENGTH(vec), j=0;
    fdtype result=fd_init_vector(NULL,n,NULL);
    fdtype *vecdata=FD_VECTOR_ELTS(result);
    struct FD_SORT_ENTRY *sentries=u8_alloc_n(n,struct FD_SORT_ENTRY);
    while (i<n) {
      fdtype elt=FD_VECTOR_REF(vec,i);
      fdtype value=_fd_apply_keyfn(elt,keyfn);
      if (FD_ABORTP(value)) {
        int j=0; while (j<i) {fd_decref(sentries[j].value); j++;}
        u8_free(sentries); u8_free(vecdata);
        return value;}
      sentries[i].value=elt;
      sentries[i].key=value;
      i++;}
    if (lexsort)
      qsort(sentries,n,sizeof(struct FD_SORT_ENTRY),_fd_lexsort_helper);
    else qsort(sentries,n,sizeof(struct FD_SORT_ENTRY),_fd_sort_helper);
    i=0; j=n-1; if (reverse) while (i < n) {
      fd_decref(sentries[i].key);
      vecdata[j]=fd_incref(sentries[i].value);
      i++; j--;}
    else while (i < n) {
      fd_decref(sentries[i].key);
      vecdata[i]=fd_incref(sentries[i].value);
      i++;}
    u8_free(sentries);
    return result;}
}

static fdtype sortvec_prim(fdtype vec,fdtype keyfn)
{
  return sortvec_primfn(vec,keyfn,0,0);
}

static fdtype lexsortvec_prim(fdtype vec,fdtype keyfn)
{
  return sortvec_primfn(vec,keyfn,0,1);
}

static fdtype rsortvec_prim(fdtype vec,fdtype keyfn)
{
  return sortvec_primfn(vec,keyfn,1,0);
}

/* RECONS reconstitutes CONSes, returning the original if
   nothing has changed. This is handy for some recursive list
   functions. */

static fdtype recons_prim(fdtype car,fdtype cdr,fdtype orig)
{
  if ((FD_EQ(car,FD_CAR(orig)))&&(FD_EQ(cdr,FD_CDR(orig))))
    return fd_incref(orig);
  else {
    fdtype cons=fd_init_pair(NULL,car,cdr);
    fd_incref(car); fd_incref(cdr);
    return cons;}
}

/* Rails */

static fdtype make_rail(int n,fdtype *elts)
{
  int i=0; while (i<n) {
    fdtype v=elts[i++]; fd_incref(v);}
  return fd_make_rail(n,elts);
}

/* side effecting operations (not threadsafe) */

static fdtype set_car(fdtype pair,fdtype val)
{
  struct FD_PAIR *p=FD_GET_CONS(pair,fd_pair_type,struct FD_PAIR *);
  fdtype oldv=p->car; p->car=fd_incref(val);
  fd_decref(oldv);
  return FD_VOID;
}

static fdtype set_cdr(fdtype pair,fdtype val)
{
  struct FD_PAIR *p=FD_GET_CONS(pair,fd_pair_type,struct FD_PAIR *);
  fdtype oldv=p->cdr; p->cdr=fd_incref(val);
  fd_decref(oldv);
  return FD_VOID;
}

static fdtype vector_set(fdtype vec,fdtype index,fdtype val)
{
  struct FD_VECTOR *v=FD_GET_CONS(vec,fd_vector_type,struct FD_VECTOR *);
  int offset=FD_FIX2INT(index); fdtype *elts=v->data;
  if (offset>v->length) {
    char buf[256]; sprintf(buf,"%d",offset);
    return fd_err(fd_RangeError,"vector_set",buf,vec);}
  else {
    fdtype oldv=elts[offset];
    elts[offset]=fd_incref(val);
    fd_decref(oldv);
    return FD_VOID;}
}

/* Miscellaneous sequence creation functions */

static fdtype seqpair(int n,fdtype *elts) {
  return fd_makeseq(fd_pair_type,n,elts);}
static fdtype seqstring(int n,fdtype *elts) {
  return fd_makeseq(fd_string_type,n,elts);}
static fdtype seqpacket(int n,fdtype *elts) {
  return fd_makeseq(fd_packet_type,n,elts);}
static fdtype seqsecret(int n,fdtype *elts) {
  return fd_makeseq(fd_secret_type,n,elts);}
static fdtype seqvector(int n,fdtype *elts) {
  return fd_makeseq(fd_vector_type,n,elts);}
static fdtype seqrail(int n,fdtype *elts) {
  return fd_makeseq(fd_rail_type,n,elts);}

static struct FD_SEQFNS pair_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  seqpair};
static struct FD_SEQFNS string_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  seqstring};
static struct FD_SEQFNS packet_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  seqpacket};
static struct FD_SEQFNS vector_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  seqvector};
static struct FD_SEQFNS rail_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  seqrail};
static struct FD_SEQFNS secret_seqfns={
  fd_seq_length,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  seqsecret};


FD_EXPORT void fd_init_sequences_c()
{
  int i=0; while (i<FD_TYPE_MAX) fd_seqfns[i++]=NULL;
  fd_seqfns[fd_pair_type]=&pair_seqfns;
  fd_seqfns[fd_string_type]=&string_seqfns;
  fd_seqfns[fd_packet_type]=&packet_seqfns;
  fd_seqfns[fd_secret_type]=&secret_seqfns;
  fd_seqfns[fd_vector_type]=&vector_seqfns;
  fd_seqfns[fd_rail_type]=&rail_seqfns;

  u8_register_source_file(_FILEINFO);

  /* Generic sequence functions */
  fd_idefn(fd_scheme_module,fd_make_cprim1("SEQUENCE?",sequencep_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("LENGTH",seqlen_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("LENGTH=",has_length_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("LENGTH>",has_length_gt_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("LENGTH>=",has_length_gte_prim,2));
  fd_defalias(fd_scheme_module,"LENGTH=>","LENGTH>=");
  fd_idefn(fd_scheme_module,fd_make_cprim2("LENGTH<",has_length_lt_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("LENGTH=<",has_length_lte_prim,2));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("LENGTH>0",has_length_gt_zero_prim,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("LENGTH>1",has_length_gt_one_prim,1));
  fd_defalias(fd_scheme_module,"LENGTH<=","LENGTH=<");
  fd_idefn(fd_scheme_module,fd_make_cprim2("ELT",seqelt_prim,2));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("SLICE",slice_prim,2,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_FALSE));
  fd_defalias(fd_scheme_module,"SUBSEQ","SLICE");
  /*
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("SUBSEQ",slice_prim,2,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_FALSE));
  */
  fd_idefn(fd_scheme_module,
           fd_make_cprim4x("POSITION",position_prim,2,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_INT2DTYPE(0),
                           -1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim4x("RPOSITION",rposition_prim,2,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_INT2DTYPE(0),
                           -1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim4x("FIND",find_prim,2,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_INT2DTYPE(0),
                           -1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim4x("SEARCH",search_prim,2,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_INT2DTYPE(0),
                           -1,FD_FALSE));
  fd_idefn(fd_scheme_module,fd_make_cprim1("REVERSE",fd_reverse,1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("APPEND",fd_append,0));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("MATCH?",seqmatch_prim,2,
                           -1,FD_VOID,-1,FD_VOID,
                           fd_fixnum_type,FD_INT2DTYPE(0)));

  /* Initial sequence functions */
  fd_idefn(fd_scheme_module,fd_make_cprim1("FIRST",first,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SECOND",second,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("THIRD",third,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("FOURTH",fourth,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("FIFTH",fifth,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SIXTH",sixth,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SEVENTH",seventh,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("REST",rest,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("NULL?",nullp,1));

  /* Standard pair functions */
  fd_idefn(fd_scheme_module,fd_make_cprim2("CONS",cons,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("CONS*",cons_star,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CAR",car,1,fd_pair_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CDR",cdr,1,fd_pair_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CADR",cadr,1,fd_pair_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CDDR",cddr,1,fd_pair_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CAAR",caar,1,fd_pair_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CDAR",cdar,1,fd_pair_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CADDR",caddr,1,fd_pair_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CDDDR",cdddr,1,fd_pair_type,FD_VOID));


  /* Assoc functions */
  fd_idefn(fd_scheme_module,fd_make_cprim2("ASSQ",assq_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("ASSV",assv_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("ASSOC",assoc_prim,2));

  /* Member functions */
  fd_idefn(fd_scheme_module,fd_make_cprim2("MEMQ",memq_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("MEMV",memv_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("MEMBER",member_prim,2));


  /* Standard lexprs */
  fd_idefn(fd_scheme_module,fd_make_cprimn("LIST",list,0));
  fd_idefn(fd_scheme_module,fd_make_cprimn("VECTOR",vector,0));
  fd_idefn(fd_scheme_module,fd_make_cprimn("MAKE-RAIL",make_rail,0));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("MAKE-VECTOR",make_vector,1,
                           fd_fixnum_type,FD_VOID,
                           -1,FD_FALSE));

  fd_idefn(fd_scheme_module,fd_make_cprim1("->RAIL",seq2rail,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("->VECTOR",seq2vector,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("->LIST",seq2list,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("->STRING",x2string,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("->PACKET",seq2packet,1));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("1VECTOR",onevector_prim,0)));
  fd_defalias(fd_scheme_module,"ONEVECTOR","1VECTOR");

  fd_idefn(fd_scheme_module,fd_make_cprim3("ELTS",elts_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprimn("MAP",fd_mapseq,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("FOR-EACH",fd_foreach,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("MAP->CHOICE",fd_map2choice,2));
  fd_idefn(fd_scheme_module,fd_make_cprim3("REDUCE",fd_reduce,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("REMOVE",fd_remove,2));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("REMOVE-IF",removeif_prim,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2
                          ("REMOVE-IF-NOT",removeifnot_prim,2)));

  fd_idefn(fd_scheme_module,fd_make_cprim4("SOME?",some_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim4("EVERY?",every_prim,2));

  fd_idefn(fd_scheme_module,fd_make_cprim1("VECTOR->ELTS",vec2elts_prim,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("SORTVEC",sortvec_prim,1,
                           fd_vector_type,FD_VOID,
                           -1,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("LEXSORTVEC",lexsortvec_prim,1,
                           fd_vector_type,FD_VOID,
                           -1,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("RSORTVEC",rsortvec_prim,1,
                           fd_vector_type,FD_VOID,
                           -1,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("RECONS",recons_prim,3,
                           -1,FD_VOID,-1,FD_VOID,
                           fd_pair_type,FD_VOID));

  /* Note that these are not threadsafe */
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("SET-CAR!",set_car,2,
                           fd_pair_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("SET-CDR!",set_cdr,2,
                           fd_pair_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("VECTOR-SET!",vector_set,3,
                           fd_vector_type,FD_VOID,fd_fixnum_type,FD_VOID,
                           -1,FD_VOID));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
