/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/sequences.h"

#include <libu8/u8.h>
#include <libu8/stringfns.h>

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
  case fd_packet_type:
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
  case fd_packet_type:
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
    u8_byte *sdata=FD_STRDATA(x);
    u8_byte *starts=string_start(sdata,i);
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
  case fd_vector_type: {
    fdtype *elts, *write, *read, *limit;
    if (end<0) end=FD_VECTOR_LENGTH(x);
    else if (start>FD_VECTOR_LENGTH(x)) return FD_RANGE_ERROR;
    else if (end>FD_VECTOR_LENGTH(x)) return FD_RANGE_ERROR;
    write=elts=u8_malloc(sizeof(fdtype)*(end-start));
    read=FD_VECTOR_DATA(x)+start; limit=FD_VECTOR_DATA(x)+end;
    while (read<limit) {
      fdtype v=*read++; *write++=fd_incref(v);}
    return fd_init_vector(NULL,end-start,elts);}
  case fd_packet_type: {
    unsigned char *data=u8_malloc(end-start);
    if (end<0) end=FD_PACKET_LENGTH(x);
    else if (end>FD_PACKET_LENGTH(x)) return FD_VOID;
    memcpy(data,FD_PACKET_DATA(x)+start,end-start);
    return fd_init_packet(NULL,end-start,data);}
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
    u8_byte *starts=string_start(FD_STRDATA(x),start);
    if (starts==NULL) return FD_RANGE_ERROR;
    else if (end<0)
      return fd_extract_string(NULL,starts,NULL);
    else {
      u8_byte *ends=u8_substring(starts,(end-start));
      if (ends)
	return fd_extract_string(NULL,starts,ends);
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
  case fd_vector_type: {
    fdtype *data=FD_VECTOR_DATA(x);
    int len=FD_VECTOR_LENGTH(x);
    if (end<0) end=len;
    if ((start<0) || (end<start) || (start>len) || (end>len))
      return -2;
    else if (start==end) return -1;
    else while (start<end)
      if (FDTYPE_EQUAL(key,data[start])) return start;
      else start++;
    return -1;}
  case fd_packet_type: {
    unsigned char *data=FD_PACKET_DATA(x);
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
    int pos=0; fdtype scan=x, head;
    if (start==end) return -1;
    while (FD_PAIRP(scan))
      if (pos<start) {pos++; scan=FD_CDR(scan);}
      else if ((end>=0) && (pos>=end)) return -1;
      else if (FDTYPE_EQUAL(FD_CAR(scan),key)) return pos;
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
    u8_string found=strrchr(data,code);
    if (found) return u8_charoffset(data,found-data);
    else return -1;}
  else switch (FD_PTR_TYPE(x)) {
  case fd_vector_type: {
    fdtype *data=FD_VECTOR_DATA(x);
    int len=FD_VECTOR_LENGTH(x);
    if (end<0) end=len;
    if ((start<0) || (end<start) || (start>len) || (end>len))
      return -2;
    else while (start<end--)
      if (FDTYPE_EQUAL(key,data[end])) return end;
    return -1;}
  case fd_packet_type: {
    unsigned char *data=FD_PACKET_DATA(x);
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

FD_EXPORT int fd_search(fdtype key,fdtype x,int start,int end)
{
  if ((FD_STRINGP(key)) && (FD_STRINGP(x))) {
    u8_byte *starts=string_start(FD_STRDATA(x),start), *found, *ends;
    if (starts == NULL) return -2;
    found=strstr(starts,FD_STRDATA(key));
    if (end<0) {
      if (found) return start+u8_strlen_x(starts,found-starts);
      else return -1;}
    else {
      u8_byte *ends=string_start(starts,end-start);
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
    else {
      int keylen=fd_seq_length(key), pos;
      fdtype keystart=fd_seq_elt(key,0);
      if (end<0) end=fd_seq_length(x);
      while ((pos=fd_position(keystart,x,start,end-keylen))>=0) {
	int i=1, j=pos+1;
	while (i < keylen) {
	  fdtype kelt=fd_seq_elt(key,i), velt=fd_seq_elt(x,j);
	  if (FDTYPE_EQUAL(kelt,velt)) {
	    fd_decref(kelt); fd_decref(velt); i++; j++;}
	  else break;}
	if (i == keylen) {
	  fd_decref(keystart);
	  return pos;}}
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
    fdtype *vec=u8_malloc(len*sizeof(fdtype));
    *n=len;
    switch (ctype) {
    case fd_packet_type: {
      unsigned char *packet=FD_PACKET_DATA(seq);
      int i=0; while (i < len) {
	int byte=packet[i];
	vec[i]=FD_INT2DTYPE(byte); i++;}
      break;}
    case fd_string_type: {
      int i=0;
      u8_byte *scan=FD_STRING_DATA(seq),
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
    case fd_vector_type: {
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
    U8_INIT_OUTPUT(&out,n*2);
    while (i < n) {
      if (FD_CHARACTERP(v[i])) u8_sputc(&out,FD_CHAR2CODE(v[i]));
      else if (FD_FIXNUMP(v[i])) u8_sputc(&out,FD_FIX2INT(v[i]));
      else {
	u8_free(out.bytes);
	return fd_type_error(_("character"),"fd_makeseq",v[i]);}
      i++;}
    return fd_init_string(NULL,out.point-out.bytes,out.bytes);}
  case fd_packet_type: {
    unsigned char *bytes=u8_malloc(n); int i=0;
    while (i < n) {
      if (FD_FIXNUMP(v[i])) bytes[i]=FD_FIX2INT(v[i]);
      else {
	u8_free(bytes);
	return fd_type_error(_("byte"),"fd_makeseq",v[i]);}
      i++;}
    return fd_init_packet(NULL,n,bytes);}
  case fd_vector_type: {
    fdtype *elts=u8_malloc(sizeof(fdtype)*n); int i=0;
    while (i < n) {
      elts[i]=fd_incref(v[i]); i++;}
    return fd_init_vector(NULL,n,elts);}
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
    fdtype *tmp=((len) ? (u8_malloc(sizeof(fdtype)*len)) : (NULL));
    if (len) {
      i=0; j=len-1; while (i < len) {tmp[j]=elts[i]; i++; j--;}}
    result=fd_makeseq(FD_PTR_TYPE(sequence),len,tmp);
    i=0; while (i<len) {fd_decref(elts[i]); i++;}
    if (elts) u8_free(elts); if (tmp) u8_free(tmp);
    return result;}
}

FD_EXPORT fdtype fd_append(int n,fdtype *sequences)
{
  if (n == 0) return FD_EMPTY_LIST;
  else {
    fd_ptr_type result_type=FD_PTR_TYPE(sequences[0]);
    fdtype result, **elts, *_elts[16], *combined;
    int i=0, k=0, *lengths, _lengths[16], total_length=0;
    if (FD_EMPTY_LISTP(sequences[0])) result_type=fd_pair_type;
    if (n>16) {
      lengths=u8_malloc(n*sizeof(int));
      elts=u8_malloc(n*(sizeof(fdtype *)));}
    else {lengths=_lengths; elts=_elts;}
    while (i < n) {
      fdtype seq=sequences[i];
      if ((FD_EMPTY_LISTP(seq)) && (result_type==fd_pair_type)) {}
      else if (FD_PTR_TYPE(seq) != result_type) result_type=fd_vector_type;
      elts[i]=fd_elts(seq,&(lengths[i]));
      total_length=total_length+lengths[i];
      if (lengths[i]==0) i++;
      else if (elts[i]==NULL)  {
	if (n>16) {u8_free(lengths); u8_free(elts);}
	return fd_type_error(_("sequence"),"fd_append",seq);}
      else i++;}
    combined=u8_malloc(sizeof(fdtype)*total_length);
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
  if ((FD_TABLEP(fn)) || (FD_ATOMICP(fn)))
    if (n_seqs>1)
      return fd_err(fd_TooManyArgs,"fd_foreach",NULL,fn);
    else if (FD_EMPTY_LISTP(firstseq)) return firstseq;
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
  results=u8_malloc(sizeof(fdtype)*seqlen);
  if (FD_APPLICABLEP(fn)) {
    if (n_seqs<8) argvec=_argvec;
    else argvec=u8_malloc(n_seqs*sizeof(fdtype));}
  i=0; while (i < seqlen) {
    fdtype elt=fd_seq_elt(firstseq,i), new_elt;
    if (FD_APPLICABLEP(fn)) {
      int j=1; while (j<n_seqs) {
	argvec[j]=fd_seq_elt(sequences[j],i); j++;}
      argvec[0]=elt;
      new_elt=fd_apply((fd_function)fn,n_seqs,argvec);
      j=1; while (j<n_seqs) {fd_decref(argvec[j]); j++;}}
    else if (FD_TABLEP(fn))
      new_elt=fd_get(fn,elt,elt);
    else if (FD_OIDP(elt))
      new_elt=fd_frame_get(elt,fn);
    else new_elt=fd_get(elt,fn,elt);
    fd_decref(elt);
    if (result_type == fd_string_type) {
      if (!(FD_PRIM_TYPEP(new_elt,fd_character_type)))
	result_type=fd_vector_type;}
    else if (result_type == fd_packet_type) 
      if (FD_FIXNUMP(new_elt)) {
	int intval=FD_FIX2INT(new_elt);
	if ((intval<0) || (intval>=0x100))
	  result_type=fd_vector_type;}
      else result_type=fd_vector_type;
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
  if ((FD_TABLEP(fn)) || (FD_ATOMICP(fn)))
    if (n_seqs>1)
      return fd_err(fd_TooManyArgs,"fd_foreach",NULL,fn);
    else if (FD_EMPTY_LISTP(firstseq)) return firstseq;
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
    else argvec=u8_malloc(n_seqs*sizeof(fdtype));}
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
      new_elt=fd_apply((fd_function)fn,n_seqs,argvec);
      j=1; while (j<n_seqs) {fd_decref(argvec[j]); j++;}}
    else new_elt=fd_get(elt,fn,elt);
    fd_decref(elt);
    if (result_type == fd_string_type) {
      if (!(FD_PRIM_TYPEP(new_elt,fd_character_type)))
	result_type=fd_vector_type;}
    else if (result_type == fd_packet_type) 
      if (FD_FIXNUMP(new_elt)) {
	int intval=FD_FIX2INT(new_elt);
	if ((intval<0) || (intval>=0x100))
	  result_type=fd_vector_type;}
      else result_type=fd_vector_type;
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
    fdtype *results=u8_malloc(sizeof(fdtype)*len), result;
    while (i < len) {
      fdtype elt=fd_seq_elt(sequence,i), new_elt;
      if (FD_TABLEP(fn))
	new_elt=fd_get(fn,elt,elt);
      else if (FD_APPLICABLEP(fn))
	new_elt=fd_apply((struct FD_FUNCTION *)(fn),1,&elt);
      else if (FD_OIDP(elt))
	new_elt=fd_frame_get(elt,fn);
      else new_elt=fd_get(elt,fn,elt);
      fd_decref(elt);
      if (result_type == fd_string_type) {
	if (!(FD_PRIM_TYPEP(new_elt,fd_character_type)))
	  result_type=fd_vector_type;}
      else if (result_type == fd_packet_type) 
	if (FD_FIXNUMP(new_elt)) {
	  int intval=FD_FIX2INT(new_elt);
	  if ((intval<0) || (intval>=0x100))
	    result_type=fd_vector_type;}
	else result_type=fd_vector_type;
      if (FD_EXCEPTIONP(new_elt)) {
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
    fdtype *results=u8_malloc(sizeof(fdtype)*len), result;
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
    fdtype result=fd_apply((fd_function)test,1,&elt);
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

FD_EXPORT fdtype fd_removeif(fdtype test,fdtype sequence)
{
  if (!(FD_SEQUENCEP(sequence)))
    return fd_type_error("sequence","fd_remove",sequence);
  else if (FD_EMPTY_LISTP(sequence)) return sequence;
  else {
    int i=0, j=0, removals=0, len=fd_seq_length(sequence);
    fd_ptr_type result_type=FD_PTR_TYPE(sequence);
    fdtype *results=u8_malloc(sizeof(fdtype)*len), result;
    while (i < len) {
      fdtype elt=fd_seq_elt(sequence,i);
      int compare=applytest(test,elt); i++;
      if (compare<0) {u8_free(results); return -1;}
      else if (compare) {removals++; fd_decref(elt);}
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
    return fd_erreify();
  else if (!(FD_APPLICABLEP(fn)))
    return fd_err(fd_NotAFunction,"MAP",NULL,fn);
  else if (FD_VOIDP(result)) {
    result=fd_seq_elt(sequence,0); i=1;}
  while (i < len) {
    fdtype elt=fd_seq_elt(sequence,i), rail[2], new_result;
    rail[0]=elt; rail[1]=result;
    new_result=fd_apply((struct FD_FUNCTION *)(fn),2,rail);
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
  int len;
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

static fdtype slice_prim(fdtype x,fdtype start,fdtype end)
{
  int startval, endval; char buf[32];
  fdtype result=check_range("slice_prim",x,start,end,&startval,&endval);
  if (FD_ABORTP(result)) return result;
  else result=fd_slice(x,startval,endval);
  if (result == FD_TYPE_ERROR)
    return fd_type_error(_("sequence"),"slice_prim",x);
  else if (result == FD_RANGE_ERROR) {
    sprintf(buf,"%d[%d:%d]",fd_seq_length(x),fd_getint(start),endval);
    return fd_err(fd_RangeError,"slice_prim",u8_strdup(buf),x);}
  else return result;
}

static fdtype position_prim(fdtype key,fdtype x,fdtype start,fdtype end)
{
  int result, startval, endval; char buf[32];
  fdtype check=check_range("position_prim",x,start,end,&startval,&endval);
  if (FD_ABORTP(check)) return check;
  else result=fd_position(key,x,startval,endval);
  if (result>=0) return FD_INT2DTYPE(result);
  else if (result == -1) return FD_FALSE;
  else if (result == -2) 
    return fd_type_error(_("sequence"),"position_prim",x);
  else if (result == -3) {
    sprintf(buf,"%d[%d:%d]",fd_seq_length(x),fd_getint(start),endval);
    return fd_err(fd_RangeError,"position_prim",u8_strdup(buf),x);}
  else return FD_INT2DTYPE(result);
}

static fdtype rposition_prim(fdtype key,fdtype x,fdtype start,fdtype end)
{
  int result, startval, endval; char buf[32];
  fdtype check=check_range("rposition_prim",x,start,end,&startval,&endval);
  if (FD_ABORTP(check)) return check;
  else result=fd_rposition(key,x,startval,endval);
  if (result>=0) return FD_INT2DTYPE(result);
  else if (result == -1) return FD_FALSE;
  else if (result == -2) 
    return fd_type_error(_("sequence"),"rposition_prim",x);
  else if (result == -3) {
    sprintf(buf,"%d[%d:%d]",fd_seq_length(x),fd_getint(start),endval);
    return fd_err(fd_RangeError,"rposition_prim",u8_strdup(buf),x);}
  else return FD_INT2DTYPE(result);
}

static fdtype find_prim(fdtype key,fdtype x,fdtype start,fdtype end)
{
  int result, startval, endval; char buf[32];
  fdtype check=check_range("find_prim",x,start,end,&startval,&endval);
  if (FD_ABORTP(check)) return check;
  else result=fd_position(key,x,startval,endval);
  if (result>=0) return FD_TRUE;
  else if (result == -1) return FD_FALSE;
  else if (result == -2) 
    return fd_type_error(_("sequence"),"find_prim",x);
  else if (result == -3) {
    sprintf(buf,"%d[%d:%d]",fd_seq_length(x),fd_getint(start),endval);
    return fd_err(fd_RangeError,"find_prim",u8_strdup(buf),x);}
  else return FD_FALSE;
}

static fdtype search_prim(fdtype key,fdtype x,fdtype start,fdtype end)
{
  int result, startval, endval; char buf[32];
  fdtype check=check_range("search_prim",x,start,end,&startval,&endval);
  if (FD_ABORTP(check)) return check;
  else result=fd_search(key,x,startval,endval);
  if (result>=0) return FD_INT2DTYPE(result);
  else if (result == -1) return FD_FALSE;
  else if (result == -2) {
    sprintf(buf,"%d:%d",start,endval);
    return fd_err(fd_RangeError,"search_prim",u8_strdup(buf),x);}
  else if (result == -3) 
    return fd_type_error(_("sequence"),"search_prim",x);
  else return result;
}

static fdtype every_prim(fdtype proc,fdtype x,fdtype startval,fdtype endval)
{
  int start, end, len;
  fdtype check=check_range("every_prim",x,start,end,&start,&end);
  if (FD_ABORTP(check)) return check;
  else if (!(FD_APPLICABLEP(proc)))
    return fd_type_error(_("function"),"every_prim",x);
  else if (FD_STRINGP(x)) {
    u8_byte *scan=FD_STRDATA(x); 
    int i=0; while (i<end) {
      int c=u8_sgetc(&scan);
      if (i<start) i++;
      else if (i<end) {
	fdtype lc=FD_CODE2CHAR(c);
	fdtype testval=fd_apply((fd_function)proc,1,&lc);
	if (FD_FALSEP(testval)) return FD_FALSE;
	else if (FD_ABORTP(testval)) return testval;
	else {
	  fd_decref(testval); i++;}}
      else return FD_TRUE;}
    return FD_TRUE;}
  else {
    int i=start; while (i<end) {
      fdtype elt=fd_seq_elt(x,i);
      fdtype testval=fd_apply((fd_function)proc,1,&elt);
      if (FD_ABORTP(testval)) {
	fd_decref(elt); return testval;}
      else if (FD_FALSEP(testval)) {
	fd_decref(elt); return FD_FALSE;}
      else {
	fd_decref(elt); fd_decref(testval); i++;}}
    return FD_TRUE;}
}

static fdtype some_prim(fdtype proc,fdtype x,fdtype startval,fdtype endval)
{
  int start, end, len;
  fdtype check=check_range("some_prim",x,start,end,&start,&end);
  if (FD_ABORTP(check)) return check;
  else if (!(FD_APPLICABLEP(proc)))
    return fd_type_error(_("function"),"some_prim",x);
  else if (FD_STRINGP(x)) {
    u8_byte *scan=FD_STRDATA(x); 
    int i=0; while (i<end) {
      int c=u8_sgetc(&scan);
      if (i<start) i++;
      else if (i< end) {
	fdtype lc=FD_CODE2CHAR(c);
	fdtype testval=fd_apply((fd_function)proc,1,&lc);
	if (FD_ABORTP(testval)) return testval;
	else if (FD_FALSEP(testval)) i++;
	else {
	  fd_decref(testval); return FD_TRUE;}}
      else return FD_FALSE;}
    return FD_FALSE;}
  else {
    int i=start; while (i<end) {
      fdtype elt=fd_seq_elt(x,i);
      fdtype testval=fd_apply((fd_function)proc,1,&elt);
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
  if (FD_CHOICEP(sequence)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(seq,sequence) {
      fdtype r=fd_removeif(test,seq);
      if (FD_ABORTP(r)) {
	fd_decref(results); return r;}
      FD_ADD_TO_CHOICE(results,r);}
    return results;}
  else return fd_removeif(test,sequence);
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
      fd_err(fd_retcode_to_exception(v),"rest",NULL,x);
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
  fdtype *copied=u8_malloc(n*sizeof(fdtype));
  int i=0; while (i < n) {
    copied[i]=fd_incref(elts[i]); i++;}
  return fd_init_vector(NULL,n,copied);
}

static fdtype make_vector(fdtype size,fdtype dflt)
{
  int n=fd_getint(size);
  if (n==0)
    return fd_init_vector(NULL,0,NULL);
  else if (n>0) {
    fdtype *data=u8_malloc(n*sizeof(fdtype));
    int i=0; while (i < n) {
      data[i]=fd_incref(dflt); i++;}
    return fd_init_vector(NULL,n,data);}
  else return fd_type_error(_("positive"),"make_vector",size);
}

static fdtype seq2vector(fdtype seq)
{
  if (FD_EMPTY_LISTP(seq))
    return fd_init_vector(NULL,0,NULL);
  else if (FD_SEQUENCEP(seq)) {
    int i=0, n; fdtype *data=fd_elts(seq,&n);
    return fd_init_vector(NULL,n,data);}
  else return fd_type_error(_("sequence"),"seq2list",seq);
}

static fdtype seq2list(fdtype seq)
{
  if (FD_EMPTY_LISTP(seq)) return FD_EMPTY_LIST;
  else if (FD_SEQUENCEP(seq)) {
    int n; fdtype *data=fd_elts(seq,&n), result=FD_EMPTY_LIST;
    n--; while (n>=0) {
      result=fd_make_pair(data[n],result); n--;}
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
    return fd_init_packet(NULL,n,bytes);}
  else return fd_type_error(_("sequence"),"seq2packet",seq);
}
  
static fdtype x2string(fdtype seq)
{
  if (FD_SYMBOLP(seq))
    return fdtype_string(FD_SYMBOL_NAME(seq));
  else if (FD_CHARACTERP(seq)) {
    int c=FD_CHAR2CODE(seq);
    U8_OUTPUT out; u8_byte buf[16];
    U8_INIT_OUTPUT_X(&out,16,buf,NULL);
    u8_putc(&out,c);
    return fdtype_string(out.bytes);}
  else if (FD_EMPTY_LISTP(seq)) return fdtype_string("");
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
	u8_sputc(&out,charcode); i++;}
      else if (FD_CHARACTERP(data[i])) {
	int charcode=FD_CHAR2CODE(data[i]);
	u8_sputc(&out,charcode); i++;}
      else {
	fdtype bad=fd_incref(data[i]);
	i=0; while (i<n) {fd_decref(data[i]); i++;}
	u8_free(data);
	return fd_type_error(_("character"),"seq2string",bad);}}
    u8_free(data);
    return fd_init_string(NULL,out.point-out.bytes,out.bytes);}
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
    case fd_vector_type:
      len=FD_VECTOR_LENGTH(x); while (i < len) {
	fdtype elt=fd_incref(FD_VECTOR_REF(x,i));
	FD_ADD_TO_CHOICE(result,elt); i++;}
      return result;
    case fd_packet_type:
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
      u8_byte *scan=FD_STRDATA(x); int c=u8_sgetc(&scan);
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
  case fd_vector_type: {
    fdtype *elts, *write, *read, *limit;
    read=FD_VECTOR_DATA(x)+start; limit=FD_VECTOR_DATA(x)+end;
    while (read<limit) {
      fdtype v=*read++; FD_ADD_TO_CHOICE(results,fd_incref(v));}
    return results;}
  case fd_packet_type: {
    unsigned char *read=FD_PACKET_DATA(x), *lim=read+end;
    while (read<lim) {
      int v=*read++; FD_ADD_TO_CHOICE(results,FD_INT2DTYPE(v));}
    return results;}
  case fd_pair_type: {
    int j=0; fdtype scan=x, head=FD_EMPTY_LIST, *tail=&head;
    while (FD_PAIRP(scan))
      if (j==end) return head;
      else if (j>=start) {
	FD_ADD_TO_CHOICE(results,fd_incref(FD_CAR(scan)));
	j++; scan=FD_CDR(scan);}
      else {j++; scan=FD_CDR(scan);}
    return results;}
  case fd_string_type: {
    int count=0; u8_byte *scan=FD_STRDATA(x); int c;
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


/* Miscellaneous sequence creation functions */

static fdtype seqpair(int n,fdtype *elts) {
  return fd_makeseq(fd_pair_type,n,elts);}
static fdtype seqstring(int n,fdtype *elts) {
  return fd_makeseq(fd_string_type,n,elts);}
static fdtype seqpacket(int n,fdtype *elts) {
  return fd_makeseq(fd_packet_type,n,elts);}
static fdtype seqvector(int n,fdtype *elts) {
  return fd_makeseq(fd_vector_type,n,elts);}

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

FD_EXPORT void fd_init_sequences_c()
{
  int i=0; while (i<FD_TYPE_MAX) fd_seqfns[i++]=NULL;
  fd_seqfns[fd_pair_type]=&pair_seqfns;
  fd_seqfns[fd_string_type]=&string_seqfns;
  fd_seqfns[fd_packet_type]=&packet_seqfns;
  fd_seqfns[fd_vector_type]=&vector_seqfns;
  
  fd_register_source_file(versionid);

  /* Generic sequence functions */
  fd_idefn(fd_scheme_module,fd_make_cprim1("SEQUENCE?",sequencep_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("LENGTH",seqlen_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("ELT",seqelt_prim,2));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim3x("SLICE",slice_prim,2,
			   -1,FD_VOID,-1,FD_VOID,
			   -1,FD_FALSE));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim3x("SUBSEQ",slice_prim,2,
			   -1,FD_VOID,-1,FD_VOID,
			   -1,FD_FALSE));
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

  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("MAKE-VECTOR",make_vector,1,
			   fd_fixnum_type,FD_VOID,
			   -1,FD_FALSE));
	   
  fd_idefn(fd_scheme_module,fd_make_cprim1("->VECTOR",seq2vector,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("->LIST",seq2list,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("->STRING",x2string,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("->PACKET",seq2packet,1));

  fd_idefn(fd_scheme_module,fd_make_cprim3("ELTS",elts_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprimn("MAP",fd_mapseq,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("FOR-EACH",fd_foreach,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("MAP->CHOICE",fd_map2choice,2));
  fd_idefn(fd_scheme_module,fd_make_cprim3("REDUCE",fd_reduce,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("REMOVE",fd_remove,2));
  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim2("REMOVEIF",removeif_prim,2)));

  fd_idefn(fd_scheme_module,fd_make_cprim4("SOME?",some_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim4("EVERY?",every_prim,2));

}


/* The CVS log for this file
   $Log: sequences.c,v $
   Revision 1.64  2006/02/13 18:35:43  haase
   Removed type declarations from sequence functions to allow #f to denote defaults

   Revision 1.63  2006/02/10 13:04:59  haase
   Made SEARCH accept #f as a start arg (default to zero).

   Revision 1.62  2006/02/05 03:42:53  haase
   Fixes to mapseq function handling

   Revision 1.61  2006/02/03 14:03:39  haase
   Added RPOSITION

   Revision 1.60  2006/01/31 13:47:24  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.59  2006/01/31 03:15:24  haase
   Made REDUCE handle the case of no start argument by using the first element of the list

   Revision 1.58  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.57  2006/01/23 00:34:49  haase
   Fixed typo in consolidated sequence range checking

   Revision 1.56  2006/01/21 19:00:30  haase
   Consolidated sequence range checking functions

   Revision 1.55  2006/01/16 22:02:23  haase
   Various sequence fixes and exports

   Revision 1.54  2006/01/09 20:18:56  haase
   Fixed handling of NULs in string sequences

   Revision 1.53  2006/01/07 13:59:02  haase
   Fixed leak in ->vector primitive

   Revision 1.52  2005/12/30 23:00:30  haase
   Fixed bug in fd_makeseq for strings

   Revision 1.51  2005/12/28 23:03:29  haase
   Made choices be direct blocks of elements, including various fixes, simplifications, and more detailed documentation.

   Revision 1.50  2005/12/22 14:36:25  haase
   Fixed some leaks in sequence appends

   Revision 1.49  2005/12/19 00:42:19  haase
   Fixed leak in generic search procedure

   Revision 1.48  2005/12/17 15:10:09  haase
   Made ELTS iterate over choice args

   Revision 1.47  2005/12/17 14:19:59  haase
   Fixes to sequence functions and added sequence tests

   Revision 1.46  2005/12/17 05:54:46  haase
   Added multi-argument MAP and FOR-EACH, added ASS* and MEM* functions, and fixed various packet manipulation bugs

   Revision 1.45  2005/10/27 15:36:59  haase
   Added REMOVEIF and made REMOVE/REMOVEIF return their argument if they do nothing

   Revision 1.44  2005/10/06 16:40:07  haase
   Avoid infinite recurrence in some generic sequence functions

   Revision 1.43  2005/09/05 12:38:25  haase
   Beginning of subseq slice

   Revision 1.42  2005/08/25 20:35:13  haase
   Added SIXTH and SEVENTH

   Revision 1.41  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.40  2005/07/26 20:49:07  haase
   Fixed bug with reversing empty lists and other sequences

   Revision 1.39  2005/07/21 00:19:44  haase
   Fixed some error passing bugs

   Revision 1.38  2005/07/12 01:50:48  haase
   Fixed initialization bug revealed when extracing an empty list slice

   Revision 1.37  2005/06/20 15:50:11  haase
   Fixed bug in slicing lists to the end

   Revision 1.36  2005/06/04 21:03:36  haase
   Added MAKE-VECTOR

   Revision 1.35  2005/06/04 01:24:25  haase
   Fix return value for position

   Revision 1.34  2005/06/02 17:55:24  haase
   Added CONS*

   Revision 1.33  2005/06/01 01:22:05  haase
   Added error checking and empty list handling to sequence conversion functions

   Revision 1.32  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.31  2005/05/12 22:10:52  haase
   Added some? and every?

   Revision 1.30  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.29  2005/05/09 20:03:19  haase
   Catch empty choice cases for sequence functions

   Revision 1.28  2005/04/30 16:12:33  haase
   Fixed GC error in ->vector

   Revision 1.27  2005/04/29 18:48:54  haase
   Undid some changes to position/search and cleaned up handling of negative arguments

   Revision 1.26  2005/04/29 13:20:17  haase
   Initial introduction of backwards searching

   Revision 1.25  2005/04/26 13:11:10  haase
   Made remove from the empty list not signal an error

   Revision 1.24  2005/04/16 16:54:36  haase
   Fix empty list case for MAP

   Revision 1.23  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.22  2005/04/12 13:27:06  haase
   Made empty lists count as sequences and fixed some static details fd_err bugs

   Revision 1.21  2005/04/11 22:43:47  haase
   Fixed bug in negative sequence offsets

   Revision 1.20  2005/04/11 22:32:41  haase
   Added semantics for negative sequence indices

   Revision 1.19  2005/04/11 22:07:12  haase
   Added fd_remove and REMOVE

   Revision 1.18  2005/03/30 14:48:44  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.17  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.16  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.15  2005/03/04 04:08:33  haase
   Fixes for minor libu8 changes

   Revision 1.14  2005/02/19 19:16:05  haase
   Fixed fencepost error in some sequence element operations

   Revision 1.13  2005/02/19 16:22:03  haase
   Added generic sequence to particular type functions

   Revision 1.12  2005/02/15 03:03:40  haase
   Updated to use the new libu8

   Revision 1.11  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
