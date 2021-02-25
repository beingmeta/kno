/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES KNO_DO_INLINE
#define KNO_INLINE_FCNIDS KNO_DO_INLINE

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/sequences.h"
#include "kno/numbers.h"
#include "kno/apply.h"

#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8printf.h>

#include <limits.h>

u8_condition kno_SecretData=_("SecretData");
u8_condition kno_NotASequence=_("NotASequence");

struct KNO_SEQFNS *kno_seqfns[KNO_TYPE_MAX];

#define KNO_EQV(x,y)                            \
  ((KNO_EQ(x,y)) ||                             \
   ((NUMBERP(x)) && (NUMBERP(y)) &&             \
    (kno_numcompare(x,y)==0)))

#define string_start(bytes,i) ((i==0) ? (bytes) : (u8_substring(bytes,i)))

static lispval make_float_vector(int n,lispval *from_elts);
static lispval make_double_vector(int n,lispval *from_elts);
static lispval make_short_vector(int n,lispval *from_elts);
static lispval make_int_vector(int n,lispval *from_elts);
static lispval make_long_vector(int n,lispval *from_elts);

static lispval *compound2vector(lispval x,int *lenp)
{
  struct KNO_COMPOUND *compound = (kno_compound) x;
  if ((compound->compound_seqoff)<0) {
    *lenp = -1;
    return NULL;}
  else {
    int off = compound->compound_seqoff;
    int len = (compound->compound_length)-off;
    *lenp = len;
    return (&(compound->compound_0))+off;}
}

KNO_FASTOP int seq_length(lispval x)
{
  int ctype = KNO_TYPEOF(x);
  switch (ctype) {
  case kno_vector_type:
    return VEC_LEN(x);
  case kno_compound_type: {
    struct KNO_COMPOUND *compound = (kno_compound) x;
    if ( (compound->compound_seqoff) < 0 )
      return -1;
    else return (compound->compound_length)-(compound->compound_seqoff);}
    return VEC_LEN(x);
  case kno_packet_type: case kno_secret_type:
    return KNO_PACKET_LENGTH(x);
  case kno_numeric_vector_type:
    return KNO_NUMVEC_LENGTH(x);
  case kno_string_type:
    return u8_strlen_x(CSTRING(x),KNO_STRING_LENGTH(x));
  case kno_pair_type: {
    int i = 0; lispval scan = x;
    while (PAIRP(scan)) {i++; scan = KNO_CDR(scan);}
    return i;}
  default:
    if (NILP(x)) return 0;
    else if ((kno_seqfns[ctype]) && (kno_seqfns[ctype]->len) &&
             ((kno_seqfns[ctype]->len) != kno_seq_length))
      return (kno_seqfns[ctype]->len)(x);
    else return -1;}
}

struct KNO_SEQFNS *kno_seqfns[KNO_TYPE_MAX];
KNO_EXPORT int kno_seq_length(lispval x)
{
  ssize_t len = seq_length(x);
  if (RARELY(len<0)) kno_seterr(kno_NotASequence,"kno_seq_length",NULL,x);
  return len;
}

static lispval range_error(u8_context cxt,lispval x,int i)
{
  u8_byte intbuf[64];
  kno_seterr(kno_RangeError,cxt,u8_bprintf(intbuf,"%d",i),x);
  return KNO_RANGE_ERROR;
}

KNO_EXPORT lispval kno_seq_elt(lispval x,int i)
{
  int ctype = KNO_TYPEOF(x);
  if (i<0)
    return range_error("kno_seq_elt",x,i);
  else switch (ctype) {
    case kno_vector_type:
      if (i>=VEC_LEN(x))
	return range_error("kno_seq_elt",x,i);
      else return kno_incref(VEC_REF(x,i));
    case kno_secret_type:
      return KNO_ERR(KNO_ERROR,kno_SecretData,"kno_seq_elt",NULL,x);
    case kno_packet_type:
      if (i>=KNO_PACKET_LENGTH(x))
	return range_error("kno_seq_elt",x,i);
      else {
        int val = KNO_PACKET_DATA(x)[i];
        return KNO_INT(val);}
    case kno_compound_type: {
      int len; lispval *elts = compound2vector(x,&len);
      if (elts == NULL)
        return kno_err("NotACompoundVector","kno_seq_elt",NULL,x);
      else if (i>=len)
	return range_error("kno_seq_elt",x,i);
      else return kno_incref(elts[i]);}
    case kno_numeric_vector_type:
      if (i>=KNO_NUMVEC_LENGTH(x))
	return range_error("kno_seq_elt",x,i);
      else switch (KNO_NUMVEC_TYPE(x)) {
        case kno_short_elt:
          return KNO_SHORT2FIX(KNO_NUMVEC_SHORT(x,i));
        case kno_int_elt:
          return KNO_INT(KNO_NUMVEC_INT(x,i));
        case kno_long_elt:
          return KNO_INT(KNO_NUMVEC_LONG(x,i));
        case kno_float_elt:
          return kno_make_flonum(KNO_NUMVEC_FLOAT(x,i));
        case kno_double_elt:
          return kno_make_flonum(KNO_NUMVEC_DOUBLE(x,i));
        default:
          return kno_err("Corrupted numvec","kno_seq_elt",NULL,x);}
    case kno_pair_type: {
      int j = 0; lispval scan = x;
      while (PAIRP(scan))
        if (j == i) return kno_incref(KNO_CAR(scan));
        else {j++; scan = KNO_CDR(scan);}
      return range_error("kno_seq_elt",x,i);}
    case kno_string_type: {
      const u8_byte *sdata = CSTRING(x);
      const u8_byte *starts = string_start(sdata,i);
      if ((starts) && (starts<sdata+KNO_STRING_LENGTH(x))) {
        int c = u8_sgetc(&starts);
	return KNO_CODE2CHAR(c);}
      return range_error("kno_seq_elt",x,i);}
    default:
      if (NILP(x))
	return range_error("kno_seq_elt",x,i);
      else if (EMPTYP(x)) return x;
      else if ((kno_seqfns[ctype]) && (kno_seqfns[ctype]->elt) &&
               ((kno_seqfns[ctype]->elt)!=kno_seq_elt))
        return (kno_seqfns[ctype]->elt)(x,i);
      else if (kno_seqfns[ctype]==NULL)
        return kno_type_error(_("sequence"),"kno_seq_elt",x);
      else return kno_err(kno_NoMethod,"kno_seq_elt",NULL,x);}
}

KNO_EXPORT lispval kno_slice(lispval x,int start,int end)
{
  int ctype = KNO_TYPEOF(x);
  if (start<0) return KNO_RANGE_ERROR;
  else switch (ctype) {
    case kno_vector_type: {
      lispval *elts, *write, *read, *limit, result;
      if (end<0) end = VEC_LEN(x);
      else if (start>VEC_LEN(x)) return KNO_RANGE_ERROR;
      else if (end>VEC_LEN(x)) return KNO_RANGE_ERROR;
      write = elts = u8_alloc_n((end-start),lispval);
      read = VEC_DATA(x)+start; limit = VEC_DATA(x)+end;
      while (read<limit) {
        lispval v = *read++;
        kno_incref(v);
        *write++=v;}
      result = kno_make_vector(end-start,elts);
      u8_free(elts);
      return result;}
    case kno_packet_type: case kno_secret_type: {
      const unsigned char *data = KNO_PACKET_DATA(x);
      if (end<0) end = KNO_PACKET_LENGTH(x);
      else if (end>KNO_PACKET_LENGTH(x))
        return VOID;
      else if (ctype == kno_secret_type) {
        lispval packet = kno_make_packet(NULL,end-start,data+start);
        KNO_SET_CONS_TYPE(packet,ctype);
        return packet;}
      else {}
      return kno_make_packet(NULL,end-start,data+start);}
    case kno_numeric_vector_type: {
      int len = KNO_NUMVEC_LENGTH(x), newlen;
      if (start>len)
        return KNO_RANGE_ERROR;
      else if (end>len)
        return KNO_RANGE_ERROR;
      else if (end<0)
        newlen = KNO_NUMVEC_LENGTH(x)-start;
      else newlen = end-start;
      switch (KNO_NUMVEC_TYPE(x)) {
      case kno_short_elt:
        return kno_make_short_vector(newlen,KNO_NUMVEC_SHORT_SLICE(x,start));
      case kno_int_elt:
        return kno_make_int_vector(newlen,KNO_NUMVEC_INT_SLICE(x,start));
      case kno_long_elt:
        return kno_make_long_vector(newlen,KNO_NUMVEC_LONG_SLICE(x,start));
      case kno_float_elt:
        return kno_make_float_vector(newlen,KNO_NUMVEC_FLOAT_SLICE(x,start));
      case kno_double_elt:
        return kno_make_double_vector(newlen,KNO_NUMVEC_DOUBLE_SLICE(x,start));
      default:
        return kno_err("Corrupted numvec","kno_seq_elt",NULL,x);}}
    case kno_pair_type: {
      int j = 0; lispval scan = x, head = NIL, *tail = &head;
      while (PAIRP(scan))
        if (j == end) return head;
        else if (j>=start) {
          lispval cons = kno_conspair(kno_incref(KNO_CAR(scan)),NIL);
          *tail = cons;
          tail = &(((struct KNO_PAIR *)cons)->cdr);
          scan = KNO_CDR(scan);
          j++;}
        else {
          scan = KNO_CDR(scan);
          j++;}
      if (j<start)
        return KNO_RANGE_ERROR;
      else if (j<=end)
        return head;
      else {
        kno_decref(head);
        return KNO_RANGE_ERROR;}}
    case kno_string_type: {
      const u8_byte *starts = string_start(CSTRING(x),start);
      if (starts == NULL) return KNO_RANGE_ERROR;
      else if (end<0)
        return kno_substring(starts,NULL);
      else {
        const u8_byte *ends = u8_substring(starts,(end-start));
        if (ends)
          return kno_substring(starts,ends);
        else return KNO_RANGE_ERROR;}}
    default:
      if (NILP(x))
        if ((start == end) && (start == 0)) return NIL;
        else return KNO_RANGE_ERROR;
      /* else if (EMPTYP(x)) return x; */
      else if ((kno_seqfns[ctype]) && (kno_seqfns[ctype]->slice) &&
               (kno_seqfns[ctype]->slice!=kno_slice))
        return (kno_seqfns[ctype]->slice)(x,start,end);
      else if (kno_seqfns[ctype]==NULL)
        return kno_type_error(_("sequence"),"kno_slice",x);
      else return kno_err(kno_NoMethod,"kno_slice",NULL,x);}
}

/* Element search functions */

KNO_EXPORT int kno_position(lispval key,lispval seq,int start,int limit)
{
  int ctype = KNO_TYPEOF(seq), len = seq_length(seq);
  if (RARELY(len<0))
    return KNO_ERR(-2,kno_NotASequence,"kno_position",NULL,seq);
  else if (RARELY(ctype == kno_secret_type))
    return KNO_ERR(-2,kno_SecretData,"kno_position",NULL,seq);
  else NO_ELSE;
  int end = (limit<0)?(len+limit+1):(limit>len)?(len):(limit);
  int dir = (start<end)?(1):(-1);
  int min = ((start<end)?(start):(end)), max = ((start<end)?(end):(start));
  if ( (start<0) || (end<0) )
    return -2;
  else if (start>end)
    return -3;
  else switch (ctype) {
    case kno_vector_type: {
      lispval *data = KNO_VECTOR_ELTS(seq);
      int i = start; while (i!=end) {
        if (LISP_EQUAL(key,data[i])) return i;
        else if (CHOICEP(data[i]))
          if (kno_overlapp(key,data[i])) return i;
          else i = i+dir;
        else i = i+dir;}
      return -1;}
    case kno_compound_type: {
      lispval *data = KNO_COMPOUND_VECELTS(seq);
      int i = start; while (i!=end) {
        if (LISP_EQUAL(key,data[i])) return i;
        else if (CHOICEP(data[i]))
          if (kno_overlapp(key,data[i])) return i;
          else i = i+dir;
        else i = i+dir;}
      return -1;}
    case kno_secret_type: {
      kno_err(kno_SecretData,"kno_position",NULL,seq);
      return -1;}
    case kno_packet_type: {
      int intval = (KNO_INTP(key))?(FIX2INT(key)):(-1);
      if ((KNO_UINTP(key))&&(intval>=0)&&(intval<0x100)) {
        const unsigned char *data = KNO_PACKET_DATA(seq);
        int i = start; while (i!=end) {
          if (intval == data[i]) return i;
          else i = i+dir;}
        return -1;}
      else return -1;}
    case kno_numeric_vector_type:
      if (!(NUMBERP(key)))
        return -1;
      else return kno_generic_position(key,seq,start,end);
    case kno_string_type:
      if (KNO_CHARACTERP(key)) {
	int code = KNO_CHAR2CODE(key);
	u8_string data = CSTRING(seq), found;
	u8_string lower = u8_substring(data,min);
	u8_string upper = u8_substring(lower,max-min);
	if (code<0x80) {
	  if (dir<0)
	    found = strrchr(upper,code);
	  else found = strchr(lower,code);
	  if (found == NULL) return -1;
	  else return u8_charoffset(data,found-data);}
	else {
	  int c, pos = start, last_match = -1;
	  u8_string scan = lower; while (scan<upper) {
	    c = u8_sgetc(&scan);
	    if (c == code) {
	      if (dir<0) last_match = pos;
	      else return pos;}
	    else pos++;}
	  return last_match;}}
      else return -1;
    case kno_pair_type: {
      int pos = 0; lispval scan = seq;
      if (start == end) return -1;
      while (PAIRP(scan))
        if (pos<start) {pos++; scan = KNO_CDR(scan);}
        else if (pos>=end) return -1;
        else if (LISP_EQUAL(KNO_CAR(scan),key)) return pos;
        else {pos++; scan = KNO_CDR(scan);}
      if ((pos<start) || ((end>0) && (pos<end)))
        return -3;
      else return -1;}
    default:
      if (NILP(seq)) return -1;
      else if ((kno_seqfns[ctype]) && (kno_seqfns[ctype]->position) &&
               ((kno_seqfns[ctype]->position)!=kno_position))
        return (kno_seqfns[ctype]->position)(key,seq,start,end);
      else if (kno_seqfns[ctype])
        return kno_generic_position(key,seq,start,end);
      else return KNO_ERR(-2,_("Not a sequence"),"kno_position",NULL,seq);}
}

KNO_EXPORT int kno_rposition(lispval key,lispval x,int start,int end)
{
  if (NILP(x)) return -1;
  else if ((STRINGP(x)) &&
           (KNO_CHARACTERP(key)) &&
           (KNO_CHAR2CODE(key)<0x80)) {
    u8_string data = CSTRING(x);
    int code = KNO_CHAR2CODE(key);
    u8_string found = strrchr(data+start,code);
    if (found) {
      int found_pos = u8_charoffset(data,found-data);
      if (found_pos<end)
	return found_pos;
      else return -1;}
    else return -1;}
  else switch (KNO_TYPEOF(x)) {
    case kno_vector_type: {
      lispval *data = VEC_DATA(x);
      int len = VEC_LEN(x);
      if (end<0) end = len;
      if ((start<0) || (end<start) || (start>len) || (end>len))
        return -3;
      else while (start<end--)
             if (LISP_EQUAL(key,data[end]))
               return end;
      return -1;}
    case kno_compound_type: {
      lispval *data = KNO_COMPOUND_VECELTS(x);
      int len = KNO_COMPOUND_VECLEN(x);
      if (end<0) end = len;
      if ((start<0) || (end<start) || (start>len) || (end>len))
        return -3;
      else while (start<end--)
             if (LISP_EQUAL(key,data[end])) return end;
      return -1;}
    case kno_secret_type: {
      kno_err(kno_SecretData,"kno_rposition",NULL,x);
      return -2;}
    case kno_packet_type: {
      const unsigned char *data = KNO_PACKET_DATA(x);
      int len = KNO_PACKET_LENGTH(x), keyval;
      if (end<0) end = len;
      if (FIXNUMP(key))
        keyval = FIX2INT(key);
      else return -1;
      if ((keyval<0) || (keyval>255))
        return -1;
      else if ((start<0) || (end<start) || (start>len) || (end>len))
        return -3;
      else while (start<end--) {
          if (keyval == data[end])
            return end;}
      return -1;}
    default: {
      int last = -1, pos;
      while ((start<end) &&
             (pos = kno_position(key,x,start,end))>=0) {
        last = pos;
        start = pos+1;}
      if (pos < -1)
        return pos;
      else return last;}}
}

/* Generic position */
KNO_EXPORT int kno_generic_position(lispval key,lispval x,int start,int end)
{
  int len = seq_length(x);
  if (RARELY(len<0)) {
    kno_seterr(kno_NotASequence,"kno_generic_position",NULL,x);
    return len;}
  else if (end<0)
    end = len+end;
  else if (end<start)  {
    int tmp = start;
    start = end;
    end = tmp;}
  else NO_ELSE;
  if ((start<0)||(end<0))
    return KNO_RANGE_ERROR;
  else if (start>end)
    return -1;
  else {
    int delta = (start<end)?(1):(-1);
    int i = start; while (i<len) {
      lispval elt = kno_seq_elt(x,i);
      if (KNO_EQUALP(elt,key)) {
        kno_decref(elt);
        return i;}
      else {
        kno_decref(elt);
        i = i+delta;}}
    return -1;}
}

/* Sub-sequence search */

static int packet_search(lispval subseq,lispval seq,int start,int end);
static int vector_search(lispval subseq,lispval seq,int start,int end);

KNO_EXPORT int kno_search(lispval subseq,lispval seq,int start,int end)
{
  if (start>=end)
    return -1;
  else if ((STRINGP(subseq)) && (STRINGP(seq))) {
    if (STRLEN(subseq) == 0)
      return 0;
    else if (STRLEN(seq) == 0)
      return -1;
    else NO_ELSE;
    const u8_byte *starts = string_start(CSTRING(seq),start), *found;
    if (starts == NULL) {
      char buf[128];
      return KNO_ERR(-2,kno_RangeError,"kno_search",
                     u8_bprintf(buf,"%d:%d",start,end),
                     seq);}
    found = strstr(starts,CSTRING(subseq));
    if (end<0) {
      if (found) return start+u8_strlen_x(starts,found-starts);
      else return -1;}
    else {
      const u8_byte *ends = string_start(starts,end-start);
      if (ends == NULL) return -2;
      else if ((found) && (found<ends))
        return start+u8_strlen_x(starts,found-starts);
      else return -1;}}
  else if (NILP(seq))
    return -1;
  else {
    int ctype = KNO_TYPEOF(seq), keytype = KNO_TYPEOF(subseq);
    if (ctype == kno_secret_type)
      return KNO_ERR(-2,kno_SecretData,"kno_seq_search",NULL,seq);
    else if (keytype == kno_secret_type)
      return KNO_ERR(-2,kno_SecretData,"kno_seq_search",NULL,seq);
    else if ((kno_seqfns[ctype]) && (kno_seqfns[ctype]->search) &&
             ((kno_seqfns[ctype]->search)!=kno_search))
      return (kno_seqfns[ctype]->search)(subseq,seq,start,end);
    else if ((PACKETP(seq)) && (PACKETP(subseq)))
      return packet_search(subseq,seq,start,end);
    else if ((VECTORP(seq)) && (VECTORP(subseq)))
      return vector_search(subseq,seq,start,end);
    else return kno_generic_search(subseq,seq,start,end);}
}

KNO_EXPORT int kno_generic_search(lispval subseq,lispval seq,int start,int end)
{
  /* Generic implementation */
  int seqlen = seq_length(seq), subseqlen = seq_length(subseq), pos = start;
  if (subseqlen < 0)
    return KNO_ERR(-2,kno_NotASequence,"kno_generic_search",NULL,seq);
  else if (subseqlen == 0)
    return 0;
  else NO_ELSE;
  lispval subseqstart = kno_seq_elt(subseq,0);
  if (ABORTED(subseqstart)) return subseqstart;
  if (end<0) end = seqlen;
  else if (end > seqlen) {
    u8_byte buf[64];
    return KNO_ERR(-2,kno_RangeError,"kno_generic_search",
		   u8_bprintf(buf,"%d:%d",start,end),
		   seq);}
  else NO_ELSE;
  while ((pos = kno_position(subseqstart,seq,pos,pos-subseqlen))>=0) {
    int i = 1, j = pos+1;
    while (i < subseqlen) {
      lispval kelt = kno_seq_elt(subseq,i), velt = kno_seq_elt(seq,j);
      if ((LISP_EQUAL(kelt,velt)) ||
          ((CHOICEP(velt)) && (kno_overlapp(kelt,velt)))) {
        kno_decref(kelt); kno_decref(velt); i++; j++;}
      else {kno_decref(kelt); kno_decref(velt); break;}}
    if (i == subseqlen) {
      kno_decref(subseqstart);
      return pos;}
    else pos++;}
  kno_decref(subseqstart);
  return -1;
}

static int packet_search(lispval key,lispval x,int start,int end)
{
  int klen = KNO_PACKET_LENGTH(key);
  if (klen == 0) return 0;
  const unsigned char *kdata = KNO_PACKET_DATA(key), first_byte = kdata[0];
  const unsigned char *data = KNO_PACKET_DATA(x), *scan, *lim = data+end;
  if (klen>(end-start)) return -1;
  scan = data+start;
  while ((scan = memchr(scan,first_byte,lim-scan))) {
    if (memcmp(scan,kdata,klen)==0) return scan-data;
    else scan++;}
  return -1;
}

static int vector_search(lispval key,lispval x,int start,int end)
{
  int klen = VEC_LEN(key);
  if (klen == 0) return 0;
  lispval *kdata = VEC_DATA(key), first_elt = kdata[0];
  lispval *data = VEC_DATA(x), *scan, *lim = data+(end-klen)+1;
  if (klen>(end-start)) return -1;
  scan = data+start;
  while (scan<lim)
    if ((scan[0]==first_elt) ||
        (LISP_EQUAL(scan[0],first_elt)) ||
        ((CHOICEP(scan[0])) &&
         (kno_overlapp(scan[0],first_elt)))) {
      lispval *kscan = kdata+1, *klim = kdata+klen, *vscan = scan+1;
      while ((kscan<klim) &&
             ((*kscan== *vscan) ||
              (LISP_EQUAL(*kscan,*vscan)) ||
              ((CHOICEP(*vscan)) &&
               (kno_overlapp(*kscan,*vscan))))) {
        kscan++; vscan++;}
      if (kscan == klim) return scan-data;
      else scan++;}
    else scan++;
  return -1;
}

/* Creating and extracting data */

/* kno_seq_elts:
   Arguments: a lisp sequence and a pointer to an int
   Returns: a C vector of lisp pointers
   This returns a vector of lisp pointers representing the elements
   of the sequence.  For strings these are characters, for packets, they
   are ints. */
lispval *kno_seq_elts(lispval seq,int *n)
{
  int len = seq_length(seq);
  if (len < 0) {
    *n = -1;
    kno_seterr(kno_NotASequence,"kno_seq_elts",NULL,seq);
    return NULL;}
  else if (len==0) {
    *n = 0;
    return NULL;}
  else {
    kno_lisp_type ctype = KNO_TYPEOF(seq);
    if ((kno_seqfns[ctype]) && (kno_seqfns[ctype]->elts) &&
	(kno_seqfns[ctype]->elts != kno_seq_elts))
      return (kno_seqfns[ctype]->elts)(seq,n);
    lispval *vec = u8_alloc_n(len,lispval);
    *n = len;
    switch (ctype) {
    case kno_secret_type: {
      *n = -1;
      kno_err(kno_SecretData,"kno_elts",NULL,seq);
      return NULL;}
    case kno_packet_type: {
      const unsigned char *packet = KNO_PACKET_DATA(seq);
      int i = 0; while (i < len) {
        int byte = packet[i];
        vec[i]=KNO_INT(byte); i++;}
      break;}
    case kno_string_type: {
      int i = 0;
      const u8_byte *scan = KNO_STRING_DATA(seq),
        *limit = scan+KNO_STRING_LENGTH(seq);
      while (scan<limit)
        if (*scan=='\0') {
          vec[i]=KNO_CODE2CHAR(0); scan++; i++;}
        else {
          int ch = u8_sgetc(&scan);
          if (ch<0) break;
          vec[i]=KNO_CODE2CHAR(ch); i++;}
      *n = i;
      break;}
    case kno_vector_type: {
      int i = 0;
      lispval *scan = VEC_DATA(seq),
        *limit = scan+VEC_LEN(seq);
      while (scan<limit) {
        vec[i]=kno_incref(*scan);
        scan++;
        i++;}
      break;}
    case kno_compound_type: {
      int i = 0, len; lispval *elts = compound2vector(seq,&len);
      if (elts == NULL) {
        *n = -1;
        return KNO_ERR(NULL,"NotACompoundVector","kno_seq_elts",NULL,seq);}
      while (i<len) {
        lispval v = elts[i];
        vec[i] = kno_incref(v);
        i++;}
      *n = len;
      break;}
    case kno_numeric_vector_type: {
      int i = 0;
      switch (KNO_NUMVEC_TYPE(seq)) {
      case kno_short_elt: {
        kno_short *shorts = KNO_NUMVEC_SHORTS(seq);
        while (i<len) {vec[i]=KNO_SHORT2FIX(shorts[i]); i++;}
        break;}
      case kno_int_elt: {
        kno_int *ints = KNO_NUMVEC_INTS(seq);
        while (i<len) {vec[i]=KNO_INT(ints[i]); i++;}
        break;}
      case kno_long_elt: {
        kno_long *longs = KNO_NUMVEC_LONGS(seq);
        while (i<len) {vec[i]=KNO_INT(longs[i]); i++;}
        break;}
      case kno_float_elt: {
        kno_float *floats = KNO_NUMVEC_FLOATS(seq);
        while (i<len) {vec[i]=kno_make_double(floats[i]); i++;}
        break;}
      case kno_double_elt: {
        kno_double *doubles = KNO_NUMVEC_DOUBLES(seq);
        while (i<len) {vec[i]=kno_make_double(doubles[i]); i++;}
        break;}}
      break;}
    case kno_pair_type: {
      int i=0; lispval scan = seq; while (KNO_PAIRP(scan)) {
        lispval elt = kno_incref(KNO_CAR(scan));
        scan=KNO_CDR(scan);
        vec[i++]=elt;}
      break;}
    default:
      if ((kno_seqfns[ctype]) && (kno_seqfns[ctype]->elt)) {
	lispval (*getelt)(lispval x,int i) = kno_seqfns[ctype]->elt;
	int i = 0; while (i<len) {
	  lispval elt = getelt(seq,i);
	  if (KNO_ABORTED(elt)) {
	    kno_decref_vec(vec,i);
	    u8_free(vec);
	    *n=-1;
	    return NULL;}
	  else vec[i++]=elt;}}
      else return NULL;}
    return vec;}
}

KNO_EXPORT
/* kno_makeseq:
   Arguments: a sequence type, a length, and a C vector of lisp pointers.
   Returns: a sequence
   Creates a sequence of the designated type out of the given elements. */
lispval kno_makeseq(kno_lisp_type ctype,int n,kno_argvec v)
{
  if (ctype == kno_compound_type) ctype = kno_vector_type;
  switch (ctype) {
  case kno_string_type: {
    struct U8_OUTPUT out; int i = 0;
    if (n==0) return kno_make_string(NULL,0,"");
    U8_INIT_OUTPUT(&out,n*2);
    while (i < n) {
      if (KNO_CHARACTERP(v[i])) u8_putc(&out,KNO_CHAR2CODE(v[i]));
      else if (KNO_UINTP(v[i])) u8_putc(&out,FIX2INT(v[i]));
      else {
        u8_free(out.u8_outbuf);
        return kno_type_error(_("character"),"kno_makeseq",v[i]);}
      i++;}
    return kno_stream2string(&out);}
  case kno_packet_type: case kno_secret_type: {
    lispval result = VOID;
    unsigned char *bytes = u8_malloc(n); int i = 0;
    while (i < n) {
      if (KNO_UINTP(v[i])) bytes[i]=FIX2INT(v[i]);
      else if (KNO_CHARACTERP(v[i])) {
        unsigned int code = KNO_CHAR2CODE(v[i]);
        bytes[i]=code&0xFF;}
      else {
        u8_free(bytes);
        return kno_type_error(_("byte"),"kno_makeseq",v[i]);}
      i++;}
    result = kno_make_packet(NULL,n,bytes);
    if (ctype == kno_secret_type) {
      KNO_SET_CONS_TYPE(result,kno_secret_type);}
    u8_free(bytes);
    return result;}
  case kno_vector_type: {
    int i = 0; while (i < n) {kno_incref(v[i]); i++;}
    return kno_make_vector(n,(lispval *)v);}
  case kno_pair_type:
    if (n == 0) return NIL;
    else {
      lispval result = NIL;
      int i = n-1; while (i >= 0) {
	lispval elt = v[i]; kno_incref(elt);
	result = kno_init_pair(NULL,elt,result);
	i--;}
      return result;}
  default:
    if ((kno_seqfns[ctype]) && (kno_seqfns[ctype]->make))
      return (kno_seqfns[ctype]->make)(n,v);
    else return kno_type_error(_("sequence type"),"kno_make_seq",VOID);}
}

/* Complex primitives */

KNO_EXPORT lispval kno_reverse(lispval sequence)
{
  if (NILP(sequence))
    return sequence;
  else if (KNO_PAIRP(sequence))
    return kno_reverse_list(sequence);
  else {
    int i, j, len; lispval *elts = kno_seq_elts(sequence,&len), result;
    lispval *tmp = ((len) ? (u8_alloc_n(len,lispval)) : (NULL));
    if (len) {
      i = 0; j = len-1; while (i < len) {tmp[j]=elts[i]; i++; j--;}}
    if (TYPEP(sequence,kno_numeric_vector_type)) {
      switch (KNO_NUMVEC_TYPE(sequence)) {
      case kno_float_elt:
        result = make_float_vector(len,elts); break;
      case kno_double_elt:
        result = make_double_vector(len,elts); break;
      case kno_short_elt:
        result = make_short_vector(len,elts); break;
      case kno_int_elt:
        result = make_int_vector(len,elts); break;
      case kno_long_elt:
        result = make_long_vector(len,elts); break;}}
    else result = kno_makeseq(KNO_TYPEOF(sequence),len,tmp);
    i = 0; while (i<len) {kno_decref(elts[i]); i++;}
    if (elts) u8_free(elts); if (tmp) u8_free(tmp);
    return result;}
}

typedef lispval *kno_types;

KNO_EXPORT lispval kno_append(int n,kno_argvec sequences)
{
  if (n == 0)
    return NIL;
  else {
    kno_lisp_type result_type = KNO_TYPEOF(sequences[0]);
    lispval result, *elts[n], *combined;
    int i = 0, k = 0, lengths[n], total_length = 0;
    if (NILP(sequences[0])) result_type = kno_pair_type;
    while (i < n) {
      lispval seq = sequences[i];
      if ((NILP(seq)) && (result_type == kno_pair_type)) {}
      else if (KNO_TYPEOF(seq) == result_type) {}
      else if ((result_type == kno_secret_type)&&
               ((KNO_TYPEOF(seq) == kno_packet_type)||
                (KNO_TYPEOF(seq) == kno_string_type)))
        result_type = kno_secret_type;
      else if ((result_type == kno_packet_type)&&
               (KNO_TYPEOF(seq) == kno_secret_type))
        result_type = kno_secret_type;
      else if ((result_type == kno_string_type)&&
               (KNO_TYPEOF(seq) == kno_secret_type))
        result_type = kno_secret_type;
      else if ((result_type == kno_string_type)&&
               (KNO_TYPEOF(seq) == kno_packet_type))
        result_type = kno_packet_type;
      else if (KNO_TYPEOF(seq) != result_type)
        result_type = kno_vector_type;
      else {}
      elts[i]=kno_seq_elts(seq,&(lengths[i]));
      total_length = total_length+lengths[i];
      if (lengths[i]==0) i++;
      else if (elts[i]==NULL)  {
	int j = 0; while (j<i) { u8_free(elts[j]); j++;}
	return kno_type_error(_("sequence"),"kno_append",seq);}
      else i++;}
    combined = u8_alloc_n(total_length,lispval);
    i = 0; while (i < n) {
      int j = 0, lim = lengths[i]; lispval *seqelts = elts[i];
      while (j<lim) combined[k++]=seqelts[j++];
      u8_free(elts[i]);
      i++;}
    result = kno_makeseq(result_type,total_length,combined);
    i = 0; while (i<total_length) {kno_decref(combined[i]); i++;}
    u8_free(combined);
    return result;}
}

/* removal */

KNO_EXPORT lispval kno_remove(lispval item,lispval sequence)
{
  if (!(KNO_SEQUENCEP(sequence)))
    return kno_type_error("sequence","kno_remove",sequence);
  else if (NILP(sequence)) return sequence;
  else {
    int i = 0, j = 0, removals = 0, len = seq_length(sequence);
    if (RARELY(len<0))
      return kno_err(kno_NotASequence,"kno_remove",NULL,sequence);
    kno_lisp_type result_type = KNO_TYPEOF(sequence);
    lispval *results = u8_alloc_n(len,lispval), result;
    while (i < len) {
      lispval elt = kno_seq_elt(sequence,i); i++;
      if (LISP_EQUAL(elt,item)) {removals++; kno_decref(elt);}
      else results[j++]=elt;}
    if (removals) {
      result = kno_makeseq(result_type,j,results);
      i = 0; while (i<j) {kno_decref(results[i]); i++;}
      u8_free(results);
      return result;}
    else {
      i = 0; while (i<j) {kno_decref(results[i]); i++;}
      u8_free(results);
      return kno_incref(sequence);}}
}


/* Making particular types of sequences */

static lispval make_short_vector(int n,lispval *from_elts)
{
  int i = 0; lispval vec = kno_make_numeric_vector(n,kno_short_elt);
  kno_short *elts = KNO_NUMVEC_SHORTS(vec);
  while (i<n) {
    lispval elt = from_elts[i];
    if (KNO_SHORTP(elt))
      elts[i++]=(kno_short)(FIX2INT(elt));
    else {
      u8_free((struct KNO_CONS *)vec); kno_incref(elt);
      return kno_type_error(_("short element"),"make_short_vector",elt);}}
  return vec;
}

static lispval make_int_vector(int n,lispval *from_elts)
{
  int i = 0; lispval vec = kno_make_numeric_vector(n,kno_int_elt);
  kno_int *elts = KNO_NUMVEC_INTS(vec);
  while (i<n) {
    lispval elt = from_elts[i];
    if (KNO_INTP(elt))
      elts[i++]=((kno_int)(FIX2INT(elt)));
    else if ((KNO_BIGINTP(elt))&&
             (kno_bigint_fits_in_word_p((kno_bigint)elt,sizeof(kno_int),1)))
      elts[i++]=kno_bigint_to_long((kno_bigint)elt);
    else {
      u8_free((struct KNO_CONS *)vec); kno_incref(elt);
      return kno_type_error(_("int element"),"make_int_vector",elt);}}
  return vec;
}

static lispval make_long_vector(int n,lispval *from_elts)
{
  int i = 0; lispval vec = kno_make_numeric_vector(n,kno_long_elt);
  kno_long *elts = KNO_NUMVEC_LONGS(vec);
  while (i<n) {
    lispval elt = from_elts[i];
    if (FIXNUMP(elt))
      elts[i++]=((kno_long)(FIX2INT(elt)));
    else if (KNO_BIGINTP(elt))
      elts[i++]=kno_bigint_to_long_long((kno_bigint)elt);
    else {
      u8_free((struct KNO_CONS *)vec); kno_incref(elt);
      return kno_type_error(_("flonum element"),"make_long_vector",elt);}}
  return vec;
}

static lispval make_float_vector(int n,lispval *from_elts)
{
  int i = 0; lispval vec = kno_make_numeric_vector(n,kno_float_elt);
  float *elts = KNO_NUMVEC_FLOATS(vec);
  while (i<n) {
    lispval elt = from_elts[i];
    if (KNO_FLONUMP(elt))
      elts[i++]=KNO_FLONUM(elt);
    else if (NUMBERP(elt))
      elts[i++]=kno_todouble(elt);
    else {
      u8_free((struct KNO_CONS *)vec); kno_incref(elt);
      return kno_type_error(_("float element"),"make_float_vector",elt);}}
  return vec;
}

static lispval make_double_vector(int n,lispval *from_elts)
{
  int i = 0; lispval vec = kno_make_numeric_vector(n,kno_double_elt);
  double *elts = KNO_NUMVEC_DOUBLES(vec);
  while (i<n) {
    lispval elt = from_elts[i];
    if (KNO_FLONUMP(elt))
      elts[i++]=KNO_FLONUM(elt);
    else if (NUMBERP(elt))
      elts[i++]=kno_todouble(elt);
    else {
      u8_free((struct KNO_CONS *)vec); kno_incref(elt);
      return kno_type_error(_("double(float) element"),"make_double_vector",elt);}}
  return vec;
}

/* Miscellaneous sequence creation functions */

static lispval makepair(int n,kno_argvec elts)
{
  return kno_makeseq(kno_pair_type,n,elts);
}

static lispval makestring(int n,kno_argvec elts)
{
  return kno_makeseq(kno_string_type,n,elts);
}

static lispval makepacket(int n,kno_argvec elts)
{
  return kno_makeseq(kno_packet_type,n,elts);
}

static lispval makesecret(int n,kno_argvec elts)
{
  return kno_makeseq(kno_secret_type,n,elts);
}

static lispval makevector(int n,kno_argvec elts)
{
  return kno_makeseq(kno_vector_type,n,elts);
}

static struct KNO_SEQFNS pair_seqfns={
  kno_seq_length,
  kno_seq_elt,
  kno_slice,
  kno_position,
  kno_search,
  kno_seq_elts,
  makepair};
static struct KNO_SEQFNS string_seqfns={
  kno_seq_length,
  kno_seq_elt,
  kno_slice,
  kno_position,
  kno_search,
  kno_seq_elts,
  makestring};
static struct KNO_SEQFNS packet_seqfns={
  kno_seq_length,
  kno_seq_elt,
  kno_slice,
  kno_position,
  kno_search,
  kno_seq_elts,
  makepacket};
static struct KNO_SEQFNS vector_seqfns={
  kno_seq_length,
  kno_seq_elt,
  kno_slice,
  kno_position,
  kno_search,
  kno_seq_elts,
  makevector};
static struct KNO_SEQFNS numeric_vector_seqfns={
  kno_seq_length,
  kno_seq_elt,
  kno_slice,
  kno_position,
  kno_search,
  kno_seq_elts,
  makevector};
static struct KNO_SEQFNS secret_seqfns={
  kno_seq_length,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  makesecret};

KNO_EXPORT void kno_init_sequences_c()
{
  int i = 0; while (i<KNO_TYPE_MAX) kno_seqfns[i++]=NULL;

  kno_seqfns[kno_pair_type]= &pair_seqfns;
  kno_seqfns[kno_string_type]= &string_seqfns;
  kno_seqfns[kno_packet_type]= &packet_seqfns;
  kno_seqfns[kno_secret_type]= &secret_seqfns;
  kno_seqfns[kno_vector_type]= &vector_seqfns;
  kno_seqfns[kno_numeric_vector_type]= &numeric_vector_seqfns;

  u8_register_source_file(_FILEINFO);
}
