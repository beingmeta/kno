/* C Mode */

/* texttools.c
   This is the core texttools file for the Kno library
   Copyright (C) 2005-2019 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/sequences.h"
#include "kno/ports.h"
#include "kno/texttools.h"
#include "kno/cprims.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>

#include <ctype.h>

static lispval texttools_module;

u8_condition kno_BadExtractData=_("Bad extract data");
u8_condition kno_BadMorphRule=_("Bad morphrule");

/* Utility functions */

static int egetc(u8_string *s)
{
  if (**s=='\0') return -1;
  else if (**s<0x80)
    if (**s=='&')
      if (strncmp(*s,"&nbsp;",6)==0) {
	*s = *s+6;
	return 0xa0;}
      else {
	const u8_byte *end = NULL;
	int code = u8_parse_entity((*s)+1,&end);
	if (code>0) {
	  *s = end;
	  return code;}
	else {(*s)++; return '&';}}
    else {
      int c = **s; (*s)++; return c;}
  else return u8_sgetc(s);
}

static u8_byteoff _forward_char(const u8_byte *s,u8_byteoff i)
{
  const u8_byte *next = u8_substring(s+i,1);
  if (next) return next-s; else return i+1;
}

#define forward_char(s,i)                                               \
  ((s[i] == 0) ? (i) : (s[i] >= 0x80) ? (_forward_char(s,i)) : (i+1))

static u8_input get_input_port(lispval portarg)
{
  if (VOIDP(portarg))
    return NULL; /* get_default_output(); */
  else if (KNO_PORTP(portarg)) {
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,portarg,kno_ioport_type);
    return p->port_input;}
  else return NULL;
}

/* This is for greedy matching */
KNO_FASTOP size_t getlongmatch(lispval matches)
{
  if (EMPTYP(matches)) return -1;
  else if (FIXNUMP(matches))
    return FIX2INT(matches);
  else if ( (CHOICEP(matches))||
	    (PRECHOICEP(matches)) ) {
    u8_byteoff max = -1;
    DO_CHOICES(match,matches) {
      u8_byteoff ival = kno_getint(match);
      if (ival>max)
	max = ival;}
    if (max<0)
      return EMPTY;
    else return max;}
  else return kno_getint(matches);
}

/* Segmenting */

static u8_string skip_whitespace(u8_string s)
{
  if (s == NULL) return NULL;
  else if (*s) {
    const u8_byte *scan = s, *last = s; int c = egetc(&scan);
    while ((c>0) && (u8_isspace(c))) {last = scan; c = egetc(&scan);}
    return last;}
  else return NULL;
}

static u8_string skip_nonwhitespace(u8_string s)
{
  if (s == NULL) return NULL;
  else if (*s) {
    const u8_byte *scan = s, *last = s; int c = egetc(&scan);
    while ((c>0) && (!(u8_isspace(c)))) {last = scan; c = egetc(&scan);}
    return last;}
  else return NULL;
}

static lispval whitespace_segment(u8_string s)
{
  lispval result = NIL, *lastp = &result;
  const u8_byte *start = skip_whitespace(s);
  const u8_byte *end = skip_nonwhitespace(start);
  while (start) {
    lispval newcons=
      kno_conspair(kno_substring(start,end),NIL);
    *lastp = newcons; lastp = &(KNO_CDR(newcons));
    start = skip_whitespace(end); end = skip_nonwhitespace(start);}
  return result;
}

static lispval dosegment(u8_string string,lispval separators)
{
  const u8_byte *scan = string;
  lispval result = NIL, *resultp = &result;
  while (scan) {
    lispval sepstring = EMPTY, pair;
    u8_byte *brk = NULL;
    DO_CHOICES(sep,separators)
      if (STRINGP(sep)) {
	u8_byte *try = strstr(scan,CSTRING(sep));
	if (try == NULL) {}
	else if ((brk == NULL) || (try<brk)) {
	  sepstring = sep; brk = try;}}
      else return kno_type_error(_("string"),"dosegment",sep);
    if (brk == NULL) {
      pair = kno_conspair(kno_mkstring(scan),NIL);
      *resultp = pair;
      return result;}
    pair = kno_conspair(kno_substring(scan,brk),NIL);
    *resultp = pair;
    resultp = &(((struct KNO_PAIR *)pair)->cdr);
    scan = brk+STRLEN(sepstring);}
  return result;
}

DEFPRIM2("segment",segment_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	 "`(SEGMENT *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval segment_prim(lispval inputs,lispval separators)
{
  if (EMPTYP(inputs)) return EMPTY;
  else if (CHOICEP(inputs)) {
    lispval results = EMPTY;
    DO_CHOICES(input,inputs) {
      lispval result = segment_prim(input,separators);
      if (KNO_ABORTP(result)) {
	kno_decref(results); return result;}
      CHOICE_ADD(results,result);}
    return results;}
  else if (STRINGP(inputs))
    if (VOIDP(separators))
      return whitespace_segment(CSTRING(inputs));
    else return dosegment(CSTRING(inputs),separators);
  else return kno_type_error(_("string"),"dosegment",inputs);
}

DEFPRIM1("decode-entities",decode_entities_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(DECODE-ENTITIES *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval decode_entities_prim(lispval input)
{
  if (STRLEN(input)==0) return kno_incref(input);
  else if (strchr(CSTRING(input),'&')) {
    struct U8_OUTPUT out;
    u8_string scan = CSTRING(input);
    int c = egetc(&scan);
    U8_INIT_OUTPUT(&out,STRLEN(input));
    while (c>=0) {
      u8_putc(&out,c); c = egetc(&scan);}
    return kno_stream2string(&out);}
  else return kno_incref(input);
}

static lispval encode_entities(lispval input,int nonascii,
			       u8_string ascii_chars,lispval other_chars)
{
  struct U8_OUTPUT out;
  u8_string scan = CSTRING(input);
  int c = u8_sgetc(&scan), enc = 0;
  if (STRLEN(input)==0) return kno_incref(input);
  U8_INIT_OUTPUT(&out,2*STRLEN(input));
  if (ascii_chars == NULL) ascii_chars="<&>";
  while (c>=0) {
    if (((c>128)&&(nonascii))||
	((c<128)&&(ascii_chars)&&(strchr(ascii_chars,c)))) {
      u8_printf(&out,"&#%d",c); enc = 1;}
    else if (EMPTYP(other_chars))
      u8_putc(&out,c);
    else {
      lispval code = KNO_CODE2CHAR(c);
      if (kno_choice_containsp(code,other_chars)) {
	u8_printf(&out,"&#%d",c); enc = 1;}
      else u8_putc(&out,c);}
    c = u8_sgetc(&scan);}
  if (enc) return kno_stream2string(&out);
  else {
    u8_free(out.u8_outbuf);
    return kno_incref(input);}
}

DEFPRIM3("encode-entities",encode_entities_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "`(ENCODE-ENTITIES *arg0* [*arg1*] [*arg2*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval encode_entities_prim(lispval input,lispval chars,
				    lispval nonascii)
{
  int na = (!((VOIDP(nonascii))||(FALSEP(nonascii))));
  if (STRLEN(input)==0) return kno_incref(input);
  else if (VOIDP(chars))
    return encode_entities(input,na,"<&>",EMPTY);
  else {
    lispval other_chars = EMPTY;
    struct U8_OUTPUT ascii_chars; u8_byte buf[128];
    U8_INIT_FIXED_OUTPUT(&ascii_chars,128,buf); buf[0]='\0';
    {DO_CHOICES(xch,chars) {
	if (STRINGP(xch)) {
	  u8_string string = CSTRING(xch);
	  const u8_byte *scan = string;
	  int c = u8_sgetc(&scan);
	  while (c>=0) {
	    if (c<128) {
	      if (strchr(buf,c))
		u8_putc(&ascii_chars,c);}
	    else {
	      lispval xch = KNO_CODE2CHAR(c);
	      CHOICE_ADD(other_chars,xch);}
	    c = u8_sgetc(&scan);}}
	else if (KNO_CHARACTERP(xch)) {
	  int ch = KNO_CHAR2CODE(xch);
	  if (ch<128) u8_putc(&ascii_chars,ch);
	  else {CHOICE_ADD(other_chars,xch);}}
	else {
	  KNO_STOP_DO_CHOICES;
	  return kno_type_error("character or string",
				"encode_entities_prim",
				xch);}}}
    if (EMPTYP(other_chars))
      return encode_entities(input,na,buf,other_chars);
    else {
      lispval oc = kno_simplify_choice(other_chars);
      lispval result = encode_entities(input,na,buf,oc);
      kno_decref(oc);
      return result;}}
}

/* Breaking up strings into words */

/* We have three categories: space, punctuation, and everything else (words).
   All the segmentation functions delete spaces and either delete or retain
   punctuation. */

#define spacecharp(c) ((c>0) && ((c<0x80) ? (isspace(c)) : (u8_isspace(c))))
#define punctcharp(c) ((c>0) && ((c<0x80) ? (ispunct(c)) : (u8_ispunct(c))))
#define wordcharp(c) ((c>0) && ((c<0x80) ? (!((ispunct(c))||(isspace(c)))) : (!((u8_ispunct(c))||(u8_isspace(c))))))

static u8_string skip_spaces(u8_string start)
{
  if (start == NULL) return start;
  else if (*start) {
    const u8_byte *last = start, *scan = start; int c = egetc(&scan);
    while (spacecharp(c)) {
      last = scan; c = egetc(&scan);}
    return last;}
  else return NULL;
}

static u8_string skip_punct(u8_string start)
{
  if (start == NULL) return start;
  else if (*start) {
    const u8_byte *last = start, *scan = start; int c = egetc(&scan);
    while (punctcharp(c)) {
      last = scan; c = egetc(&scan);}
    return last;}
  else return NULL;
}

/* This is a little messier, because we want words to be allowd to include single
   embedded punctuation characters. */
static u8_string skip_word(u8_string start)
{
  if (start == NULL) return start;
  else if (*start) {
    const u8_byte *last = start, *scan = start; int c = egetc(&scan);
    while (c>0) {
      if (spacecharp(c)) break;
      else if (punctcharp(c)) {
	int nc = egetc(&scan);
	if (!(wordcharp(nc))) return last;}
      else {}
      last = scan; c = egetc(&scan);}
    return last;}
  else return NULL;
}

typedef enum KNO_TEXTSPAN_TYPE { spacespan, punctspan, wordspan, nullspan }
  textspantype;

static u8_string skip_span(u8_string start,enum KNO_TEXTSPAN_TYPE *type)
{
  u8_string scan = start; int c = egetc(&scan);
  if ((c<0)||(c==0)) {
    *type = nullspan; return NULL;}
  else if (spacecharp(c)) {
    *type = spacespan; return skip_spaces(start);}
  else if (punctcharp(c)) {
    *type = punctspan; return skip_punct(start);}
  else {
    *type = wordspan; return skip_word(start);}
}

KNO_EXPORT lispval kno_words2list(u8_string string,int keep_punct)
{
  lispval result = NIL, *lastp = &result;
  textspantype spantype;
  u8_string start = string, last = start, scan = skip_span(last,&spantype);
  while (1)
    if (spantype == spacespan) {
      if (scan == NULL) break;
      last = scan; scan = skip_span(last,&spantype);}
    else if (((spantype == punctspan) && (keep_punct)) ||
	     (spantype == wordspan)) {
      lispval newcons;
      lispval extraction =
	((scan) ? (kno_substring(last,scan)) : (kno_mkstring(last)));
      newcons = kno_conspair(extraction,NIL);
      *lastp = newcons; lastp = &(KNO_CDR(newcons));
      if (scan == NULL) break;
      last = scan; scan = skip_span(last,&spantype);}
    else {
      if (scan == NULL) break;
      last = scan; scan = skip_span(last,&spantype);}
  return result;
}

KNO_EXPORT lispval kno_words2vector(u8_string string,int keep_punct)
{
  int n = 0, max = 16; lispval _buf[16];
  lispval *wordsv=_buf, result = VOID;
  textspantype spantype;
  u8_string start = string, last = start, scan = skip_span(last,&spantype);
  while (1)
    if (spantype == spacespan) {
      if (scan == NULL) break;
      last = scan; scan = skip_span(last,&spantype);}
    else if (((spantype == punctspan) && (keep_punct)) ||
	     (spantype == wordspan)) {
      if (n>=max) {
	if (wordsv==_buf) {
	  lispval *newv = u8_alloc_n(max*2,lispval);
	  memcpy(newv,wordsv,LISPVEC_BYTELEN(n));
	  wordsv = newv; max = max*2;}
	else {
	  int newmax = ((n>=1024) ? (n+1024) : (n*2));
	  wordsv = u8_realloc_n(wordsv,newmax,lispval);
	  max = newmax;}}
      wordsv[n++]=((scan) ? (kno_substring(last,scan)) :
		   (kno_mkstring(last)));
      if (scan == NULL) break;
      last = scan; scan = skip_span(last,&spantype);}
    else {
      if (scan == NULL) break;
      last = scan; scan = skip_span(last,&spantype);}
  result = kno_make_vector(n,wordsv);
  if (wordsv!=_buf) u8_free(wordsv);
  return result;
}

DEFPRIM2("getwords",getwords_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(GETWORDS *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval getwords_prim(lispval arg,lispval punctflag)
{
  int keep_punct = ((!(VOIDP(punctflag))) && (KNO_TRUEP(punctflag)));
  return kno_words2list(CSTRING(arg),keep_punct);
}

DEFPRIM2("words->vector",getwordsv_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(WORDS->VECTOR *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval getwordsv_prim(lispval arg,lispval punctflag)
{
  int keep_punct = ((!(VOIDP(punctflag))) && (KNO_TRUEP(punctflag)));
  return kno_words2vector(CSTRING(arg),keep_punct);
}

/* Making fragments from word vectors */

/* This function takes a vector of words and returns a choice
   of lists enumerating subsequences of the vector.  It also
   creates subsequences starting and ending with false (#f)
   indicating prefix or suffix subsequences.
   The window argument specifies the maximnum span to generate and
   all spans of length smaller than or equal to the window are generated.
   The output of this function is useful for indexing strings
   for purposes of partial indexing.
*/
DEFPRIM3("vector->frags",vector2frags_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "`(VECTOR->FRAGS *arg0* [*arg1*] [*arg2*])` **undocumented**",
	 kno_vector_type,KNO_VOID,kno_any_type,KNO_CPP_INT(2),
	 kno_any_type,KNO_TRUE);
static lispval vector2frags_prim(lispval vec,lispval window,lispval with_affix)
{
  int i = 0, n = VEC_LEN(vec), minspan = 1, maxspan;
  lispval *data = VEC_DATA(vec), results = EMPTY;
  int with_affixes = (!(FALSEP(with_affix)));
  if (KNO_INTP(window)) maxspan = FIX2INT(window);
  else if ((PAIRP(window))&&
	   (KNO_INTP(KNO_CAR(window)))&&
	   (KNO_INTP(KNO_CDR(window)))) {
    minspan = FIX2INT(KNO_CAR(window));
    maxspan = FIX2INT(KNO_CDR(window));}
  else if ((VOIDP(window))||(FALSEP(window)))
    maxspan = n-1;
  else return kno_type_error(_("fragment spec"),"vector2frags",window);
  if ((maxspan<0)||(minspan<0))
    return kno_type_error(_("natural number"),"vector2frags",window);
  if (n==0) return results;
  else if (n==1) {
    lispval elt = VEC_REF(vec,0); kno_incref(elt);
    return kno_conspair(elt,NIL);}
  else if (maxspan<=0)
    return kno_type_error(_("natural number"),"vector2frags",window);
  if (with_affixes) { int span = maxspan; while (span>=minspan) {
      /* Compute prefix fragments of length = span */
      lispval frag = NIL;
      int i = span-1; while ((i>=0) && (i<n)) {
	lispval elt = data[i]; kno_incref(elt);
	frag = kno_conspair(elt,frag);
	i--;}
      frag = kno_conspair(KNO_FALSE,frag);
      CHOICE_ADD(results,frag);
      span--;}}
  /* Compute suffix fragments
     We're a little clever here, because we can use the same sublist
     repeatedly.  */
  if (with_affixes) {
    lispval frag = kno_conspair(KNO_FALSE,NIL);
    int stopat = n-maxspan; if (stopat<0) stopat = 0;
    i = n-minspan; while (i>=stopat) {
      lispval elt = data[i]; kno_incref(elt);
      frag = kno_conspair(elt,frag);
      /* We incref it because we're going to point to it from both the
	 result and from the next longer frag */
      kno_incref(frag);
      CHOICE_ADD(results,frag);
      i--;}
    /* We need to decref frag here, because we incref'd it above to do
       our list-reuse trick. */
    kno_decref(frag);}
  { /* Now compute internal spans */
    int end = n-1; while (end>=0) {
      lispval frag = NIL;
      int i = end; int lim = end-maxspan;
      if (lim<0) lim = -1;
      while (i>lim) {
	lispval elt = data[i]; kno_incref(elt);
	frag = kno_conspair(elt,frag);
	if ((1+(end-i))>=minspan) {
	  kno_incref(frag);
	  CHOICE_ADD(results,frag);}
	i--;}
      kno_decref(frag);
      end--;}}

  return results;
}

DEFPRIM1("list->phrase",list2phrase_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(LIST->PHRASE *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval list2phrase_prim(lispval arg)
{
  int dospace = 0; struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  {KNO_DOLIST(word,arg) {
      if ((FALSEP(word))||(EMPTYP(word))||
	  (NILP(word)))
	continue;
      if (dospace) {u8_putc(&out,' ');} else dospace = 1;
      if (STRINGP(word)) u8_puts(&out,KNO_STRING_DATA(word));
      else u8_printf(&out,"%q",word);}}
  return kno_stream2string(&out);
}

static lispval seq2phrase_ndhelper
(u8_string base,lispval seq,int start,int end,int dospace);

DEFPRIM3("seq->phrase",seq2phrase_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "`(SEQ->PHRASE *arg0* [*arg1*] [*arg2*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_CPP_INT(0),
	 kno_fixnum_type,KNO_VOID);
static lispval seq2phrase_prim(lispval arg,lispval start_arg,lispval end_arg)
{
  if (PRED_FALSE(!(KNO_SEQUENCEP(arg))))
    return kno_type_error("sequence","seq2phrase_prim",arg);
  else if (STRINGP(arg)) return kno_incref(arg);
  else if (!(KNO_UINTP(start_arg)))
    return kno_type_error("uint","seq2phrase_prim",start_arg);
  else {
    int dospace = 0, start = FIX2INT(start_arg), end;
    int len = kno_seq_length(arg); char tmpbuf[32];

    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
    if (start<0) start = len+start;
    if ((start<0) || (start>len)) {
      return kno_err(kno_RangeError,"seq2phrase_prim",
		     u8_itoa10(FIX2INT(start_arg),tmpbuf),
		     arg);}
    if (!(KNO_INTP(end_arg))) end = len;
    else {
      end = FIX2INT(end_arg);
      if (end<0) end = len+end;
      if ((end<0) || (end>len)) {
	return kno_err(kno_RangeError,"seq2phrase_prim",
		       u8_itoa10(FIX2INT(end_arg),tmpbuf),
		       arg);}}
    while (start<end) {
      lispval word = kno_seq_elt(arg,start);
      if (CHOICEP(word)) {
	lispval result=
	  seq2phrase_ndhelper(out.u8_outbuf,arg,start,end,dospace);
	kno_decref(word);
	u8_free(out.u8_outbuf);
	return kno_simplify_choice(result);}
      else if ( (FALSEP(word)) || (EMPTYP(word)) || (NILP(word)) ) {
	start++;
	continue;}
      else if (dospace) {u8_putc(&out,' ');} else dospace = 1;
      if (STRINGP(word)) u8_puts(&out,KNO_STRING_DATA(word));
      else u8_printf(&out,"%q",word);
      kno_decref(word);
      start++;}
    return kno_stream2string(&out);}
}

static lispval seq2phrase_ndhelper
(u8_string base,lispval seq,int start,int end,int dospace)
{
  if (start == end)
    return kno_wrapstring(u8_strdup(base));
  else {
    lispval elt = kno_seq_elt(seq,start), results = EMPTY;
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
    DO_CHOICES(s,elt) {
      lispval result;
      if (!(STRINGP(s))) {
	kno_decref(elt); kno_decref(results);
	u8_free(out.u8_outbuf);
	return kno_type_error(_("string"),"seq2phrase_ndhelper",s);}
      out.u8_write = out.u8_outbuf;
      u8_puts(&out,base);
      if (dospace) u8_putc(&out,' ');
      u8_puts(&out,CSTRING(s));
      result = seq2phrase_ndhelper
	(out.u8_outbuf,seq,forward_char(base,start),end,1);
      if (KNO_ABORTP(result)) {
	kno_decref(elt); kno_decref(results);
	u8_free(out.u8_outbuf);
	return result;}
      CHOICE_ADD(results,result);}
    kno_decref(elt);
    u8_free(out.u8_outbuf);
    return results;}
}

/* String predicates */

DEFPRIM1("isspace%",isspace_percentage,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ISSPACE% *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval isspace_percentage(lispval string)
{
  u8_string scan = CSTRING(string);
  if (*scan=='\0') return KNO_INT(0);
  else {
    int non_space = 0, space = 0, c;
    while ((c = egetc(&scan))>=0)
      if (u8_isspace(c)) space++;
      else non_space++;
    return KNO_INT((space*100)/(space+non_space));}
}

DEFPRIM1("isalpha%",isalpha_percentage,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ISALPHA% *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval isalpha_percentage(lispval string)
{
  u8_string scan = CSTRING(string);
  if (*scan=='\0') return KNO_INT(0);
  else {
    int non_alpha = 0, alpha = 0, c;
    while ((c = egetc(&scan))>0)
      if (u8_isalpha(c)) alpha++;
      else non_alpha++;
    return KNO_INT((alpha*100)/(alpha+non_alpha));}
}

DEFPRIM1("isalphalen",isalphalen,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ISALPHALEN *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval isalphalen(lispval string)
{
  u8_string scan = CSTRING(string);
  if (*scan=='\0') return KNO_INT(0);
  else {
    int non_alpha = 0, alpha = 0, c;
    while ((c = egetc(&scan))>0)
      if (u8_isalpha(c)) alpha++;
      else non_alpha++;
    return KNO_INT(alpha);}
}

DEFPRIM1("count-words",count_words,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(COUNT-WORDS *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval count_words(lispval string)
{
  u8_string scan = CSTRING(string);
  int c = egetc(&scan), word_count = 0;
  while (u8_isspace(c)) c = egetc(&scan);
  if (c<0) return KNO_INT(0);
  else while (c>0) {
      while ((c>0) && (!(u8_isspace(c)))) c = egetc(&scan);
      word_count++;
      while ((c>0) && (u8_isspace(c))) c = egetc(&scan);}
  return KNO_INT(word_count);
}

DEFPRIM1("markup%",ismarkup_percentage,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(MARKUP% *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval ismarkup_percentage(lispval string)
{
  u8_string scan = CSTRING(string);
  if (*scan=='\0') return KNO_INT(0);
  else {
    int content = 0, markup = 0, c = egetc(&scan);
    while (c>0)
      if (c=='<') {
	if (strncmp(scan,"!--",3)==0) {
	  u8_string end = strstr(scan,"-->");
	  if (end) scan = end+3; else break;}
	else while ((c>0) && (c!='>')) {
	    markup++; c = egetc(&scan);}
	if (c>0) {markup++; c = egetc(&scan);}
	else break;}
      else if (u8_isspace(c)) {
	/* Count only one character of whitespace */
	content++; c = egetc(&scan);
	while ((c>0) && (u8_isspace(c))) c = egetc(&scan);}
      else {content++; c = egetc(&scan);}
    if ((content+markup==0)) return 0;
    else return KNO_INT((markup*100)/(content+markup));}
}

/* Stemming */

KNO_EXPORT u8_byte *kno_stem_english_word(const u8_byte *original);

DEFPRIM1("porter-stem",stem_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(PORTER-STEM *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval stem_prim(lispval arg)
{
  u8_byte *stemmed = kno_stem_english_word(CSTRING(arg));
  return kno_wrapstring(stemmed);
}

/* Disemvoweling */

static u8_string default_vowels="aeiouy";

static int all_asciip(u8_string s)
{
  int c = u8_sgetc(&s);
  while (c>0)
    if (c>=0x80) return 0;
    else c = u8_sgetc(&s);
  return 1;
}

DEFPRIM2("disemvowel",disemvowel,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(DISEMVOWEL *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_string_type,KNO_VOID);
static lispval disemvowel(lispval string,lispval vowels)
{
  struct U8_OUTPUT out; struct U8_INPUT in;
  U8_INIT_STRING_INPUT(&in,STRLEN(string),CSTRING(string));
  U8_INIT_OUTPUT(&out,KNO_STRING_LENGTH(string));
  int c = u8_getc(&in), all_ascii;
  u8_string vowelset;
  if (STRINGP(vowels)) vowelset = CSTRING(vowels);
  else vowelset = default_vowels;
  all_ascii = all_asciip(vowelset);
  while (c>=0) {
    int bc = u8_base_char(c);
    if (bc<0x80) {
      if (strchr(vowelset,bc) == NULL)
	u8_putc(&out,c);}
    else if (all_ascii) {}
    else {
      char buf[16]; U8_OUTPUT tmp;
      U8_INIT_FIXED_OUTPUT(&tmp,16,buf);
      u8_putc(&tmp,bc);
      if (strstr(vowelset,buf) == NULL)
	u8_putc(&out,c);}
    c = u8_getc(&in);}
  return kno_stream2string(&out);
}

/* Depuncting strings (removing punctuation and whitespace) */

DEFPRIM1("depunct",depunct,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(DEPUNCT *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval depunct(lispval string)
{
  struct U8_OUTPUT out; struct U8_INPUT in;
  U8_INIT_STRING_INPUT(&in,STRLEN(string),CSTRING(string));
  U8_INIT_OUTPUT(&out,KNO_STRING_LENGTH(string));
  int c = u8_getc(&in);
  while (c>=0) {
    if (!((u8_isspace(c)) || (u8_ispunct(c))))
      u8_putc(&out,c);
    c = u8_getc(&in);}
  return kno_stream2string(&out);
}

/* Skipping markup */

DEFPRIM2("strip-markup",strip_markup,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(STRIP-MARKUP *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval strip_markup(lispval string,lispval insert_space_arg)
{
  int c, insert_space = KNO_TRUEP(insert_space_arg);
  u8_string start = CSTRING(string), scan = start, last = start;
  if (*start) {
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,STRLEN(string));
    while ((c = egetc(&scan))>0)
      if (c=='<')
	if (strncmp(scan,"!--",3)==0) {
	  u8_string end = strstr(scan,"-->");
	  if (end) scan = end+3; else break;}
	else if (strncmp(scan,"<[CDATA[",8)==0) {
	  u8_string end = strstr(scan,"-->");
	  if (end) scan = end+3; else break;}
	else {
	  while ((c>0) && (c!='>')) c = egetc(&scan);
	  if (c<=0) break;
	  else start = last = scan;
	  if (insert_space) u8_putc(&out,' ');}
      else u8_putc(&out,c);
    u8_putn(&out,start,last-start);
    return kno_stream2string(&out);}
  else return kno_incref(string);
}

/* Columnizing */

DEFPRIM3("columnize",columnize_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(COLUMNIZE *arg0* *arg1* [*arg2*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_FALSE);
static lispval columnize_prim(lispval string,lispval cols,lispval parse)
{
  u8_string scan = CSTRING(string), limit = scan+STRLEN(string);
  u8_byte *buf;
  int i = 0, field = 0, n_fields = kno_seq_length(cols), parselen = 0;
  lispval *fields;
  while (i<n_fields) {
    lispval elt = kno_seq_elt(cols,i);
    if (FIXNUMP(elt)) i++;
    else return kno_type_error(_("column width"),"columnize_prim",elt);}
  if (KNO_SEQUENCEP(parse)) parselen = kno_seq_length(parselen);
  fields = u8_alloc_n(n_fields,lispval);
  buf = u8_malloc(STRLEN(string)+1);
  while (field<n_fields) {
    lispval parsefn;
    int j = 0, width = kno_getint(kno_seq_elt(cols,field));
    u8_string start = scan;
    /* Get the parse function */
    if (field<parselen)
      parsefn = kno_seq_elt(parse,field);
    else parsefn = parse;
    /* Skip over the field */
    while (j<width)
      if (scan>=limit) break;
      else {u8_sgetc(&scan); j++;}
    /* If you're at the end, set the field to a default. */
    if (scan == start)
      if (FALSEP(parsefn))
	fields[field++]=kno_init_string(NULL,0,NULL);
      else fields[field++]=KNO_FALSE;
    /* If the parse function is false, make a string. */
    else if (FALSEP(parsefn))
      fields[field++]=kno_substring(start,scan);
    /* If the parse function is #t, use the lisp parser */
    else if (KNO_TRUEP(parsefn)) {
      lispval value;
      strncpy(buf,start,scan-start); buf[scan-start]='\0';
      value = kno_parse_arg(buf);
      if (KNO_ABORTP(value)) {
	int k = 0; while (k<field) {kno_decref(fields[k]); k++;}
	u8_free(fields);
	u8_free(buf);
	return value;}
      else fields[field++]=value;}
    /* If the parse function is applicable, make a string
       and apply the parse function. */
    else if (KNO_APPLICABLEP(parsefn)) {
      lispval stringval = kno_substring(start,scan);
      lispval value = kno_apply(parse,1,&stringval);
      if (field<parselen) kno_decref(parsefn);
      if (KNO_ABORTP(value)) {
	int k = 0; while (k<field) {kno_decref(fields[k]); k++;}
	u8_free(fields);
	u8_free(buf);
	kno_decref(stringval);
	return value;}
      else {
	kno_decref(stringval);
	fields[field++]=value;}}
    else {
      int k = 0; while (k<field) {kno_decref(fields[k]); k++;}
      u8_free(fields);
      u8_free(buf);
      return kno_type_error(_("column parse function"),
			    "columnize_prim",parsefn);}}
  u8_free(buf);
  if (FALSEP(parse))
    while (field<n_fields)
      fields[field++]=kno_init_string(NULL,0,NULL);
  else while (field<n_fields) fields[field++]=KNO_FALSE;
  lispval result=kno_makeseq(KNO_TYPEOF(cols),n_fields,fields);
  u8_free(fields);
  return result;
}

/* The Matcher */

static lispval return_offsets(u8_string s,lispval results)
{
  lispval final_results = EMPTY;
  DO_CHOICES(off,results)
    if (KNO_UINTP(off)) {
      u8_charoff charoff = u8_charoffset(s,FIX2INT(off));
      CHOICE_ADD(final_results,KNO_INT(charoff));}
    else {}
  kno_decref(results);
  return final_results;
}

#define convert_arg(off,string,max) u8_byteoffset(string,off,max)

static void convert_offsets
(lispval string,lispval offset,lispval limit,u8_byteoff *off,u8_byteoff *lim)
{
  u8_charoff offval = kno_getint(offset);
  if (KNO_INTP(limit)) {
    int intlim = FIX2INT(limit), len = STRLEN(string);
    u8_string data = CSTRING(string);
    if (intlim<0) {
      int char_len = u8_strlen(data);
      *lim = u8_byteoffset(data,char_len+intlim,len);}
    else *lim = u8_byteoffset(CSTRING(string),intlim,len);}
  else *lim = STRLEN(string);
  *off = u8_byteoffset(CSTRING(string),offval,*lim);
}

DEFPRIM4("textmatcher",textmatcher,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "`(TEXTMATCHER *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval textmatcher(lispval pattern,lispval string,
			   lispval offset,lispval limit)
{
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return kno_err(kno_RangeError,"textmatcher",NULL,VOID);
  else {
    lispval match_result = kno_text_matcher
      (pattern,NULL,CSTRING(string),off,lim,0);
    if (KNO_ABORTP(match_result))
      return match_result;
    else return return_offsets(CSTRING(string),match_result);}
}

DEFPRIM4("textmatch",textmatch,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "`(TEXTMATCH *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval textmatch(lispval pattern,lispval string,
			 lispval offset,lispval limit)
{
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return kno_err(kno_RangeError,"textmatch",NULL,VOID);
  else {
    int retval=
      kno_text_match(pattern,NULL,CSTRING(string),off,lim,0);
    if (retval<0) return KNO_ERROR;
    else if (retval) return KNO_TRUE;
    else return KNO_FALSE;}
}

DEFPRIM4("textsearch",textsearch,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "`(TEXTSEARCH *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval textsearch(lispval pattern,lispval string,
			  lispval offset,lispval limit)
{
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return kno_err(kno_RangeError,"textsearch",NULL,VOID);
  else {
    int pos = kno_text_search(pattern,NULL,CSTRING(string),off,lim,0);
    if (pos<0)
      if (pos== -2) return KNO_ERROR;
      else return KNO_FALSE;
    else return KNO_INT(u8_charoffset(CSTRING(string),pos));}
}

DEFPRIM4("textract",textract,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "`(TEXTRACT *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval textract(lispval pattern,lispval string,
			lispval offset,lispval limit)
{
  lispval results = EMPTY;
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return kno_err(kno_RangeError,"textract",NULL,VOID);
  else {
    lispval extract_results = kno_text_extract
      (pattern,NULL,CSTRING(string),off,lim,0);
    if (KNO_ABORTP(extract_results))
      return extract_results;
    else {
      DO_CHOICES(extraction,extract_results) {
	if (KNO_ABORTP(extraction)) {
	  kno_decref(results);
	  kno_incref(extraction);
	  kno_decref(extract_results);
	  return extraction;}
	else if (PAIRP(extraction))
	  if (kno_getint(KNO_CAR(extraction)) == lim) {
	    lispval extract = kno_incref(KNO_CDR(extraction));
	    CHOICE_ADD(results,extract);}
	  else {}
	else {}}
      kno_decref(extract_results);
      return results;}}
}

static lispval textgather_base(lispval pattern,lispval string,
			       lispval offset,lispval limit,
			       int star)
{
  lispval results = EMPTY;
  u8_string data = CSTRING(string);
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return kno_err(kno_RangeError,"textgather",NULL,VOID);
  else {
    int start = kno_text_search(pattern,NULL,data,off,lim,0);
    while ((start>=0)&&(start<lim)) {
      lispval substring, match_result=
	kno_text_matcher(pattern,NULL,CSTRING(string),start,lim,0);
      int maxpoint = -1;
      if (EMPTYP(match_result)) {
	start = forward_char(data,start);
	continue;}
      else if (FIXNUMP(match_result)) {
	int pt = FIX2INT(match_result);
	if ((pt>maxpoint)&&(pt<=lim)) {
	  maxpoint = pt;}}
      else {
	DO_CHOICES(match,match_result) {
	  int pt = kno_getint(match);
	  if ((pt>maxpoint)&&(pt<=lim)) {
	    maxpoint = pt;}}
	kno_decref(match_result);}
      if (maxpoint<0)
	return results;
      else if (maxpoint>start) {
	substring = kno_substring(data+start,data+maxpoint);
	CHOICE_ADD(results,substring);}
      if (star)
	start = kno_text_search(pattern,NULL,data,forward_char(data,start),lim,0);
      else if (maxpoint == lim) return results;
      else if (maxpoint>start)
	start = kno_text_search(pattern,NULL,data,maxpoint,lim,0);
      else start = kno_text_search(pattern,NULL,data,forward_char(data,start),lim,0);}
    if (start== -2) {
      kno_decref(results);
      return KNO_ERROR;}
    else return results;}
}

DEFPRIM4("gather",textgather,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "`(GATHER *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval textgather(lispval pattern,lispval string,
			  lispval offset,lispval limit)
{
  return textgather_base(pattern,string,offset,limit,0);
}

DEFPRIM4("gather*",textgather_star,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "`(GATHER* *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval textgather_star(lispval pattern,lispval string,
			       lispval offset,lispval limit)
{
  return textgather_base(pattern,string,offset,limit,1);
}

DEFPRIM4("gather->list",textgather2list,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "`(GATHER->LIST *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval textgather2list(lispval pattern,lispval string,
			       lispval offset,lispval limit)
{
  lispval head = NIL, *tail = &head;
  u8_string data = CSTRING(string);
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return kno_err(kno_RangeError,"textgather",NULL,VOID);
  else {
    int start = kno_text_search(pattern,NULL,data,off,lim,0);
    while (start>=0) {
      lispval match_result=
	kno_text_matcher(pattern,NULL,CSTRING(string),start,lim,0);
      int end = -1;
      if (EMPTYP(match_result)) {}
      else if (FIXNUMP(match_result)) {
	int point = FIX2INT(match_result);
	if (point>end) end = point;}
      else if (KNO_ABORTP(match_result)) {
	kno_decref(head);
	return match_result;}
      else {
	DO_CHOICES(match,match_result) {
	  int point = kno_getint(match); if (point>end) end = point;}
	kno_decref(match_result);}
      if (end<0) return head;
      else if (end>start) {
	lispval newpair=
	  kno_conspair(kno_substring(data+start,data+end),NIL);
	*tail = newpair; tail = &(KNO_CDR(newpair));
	start = kno_text_search(pattern,NULL,data,end,lim,0);}
      else if (end == lim)
	return head;
      else start = kno_text_search
	     (pattern,NULL,data,forward_char(data,end),lim,0);}
    if (start== -2) {
      kno_decref(head);
      return KNO_ERROR;}
    else return head;}
}

/* Text rewriting and substitution
   Rewriting rewrites a string which matches a pattern, substitution
   rewrites all the substrings matching a pattern.
*/

static lispval subst_symbol;
static lispval rewrite_apply(lispval fcn,lispval content,lispval args);
static int dorewrite(u8_output out,lispval xtract)
{
  if (STRINGP(xtract))
    u8_putn(out,CSTRING(xtract),STRLEN(xtract));
  else if (VECTORP(xtract)) {
    int i = 0, len = VEC_LEN(xtract);
    lispval *data = VEC_DATA(xtract);
    while (i<len) {
      int retval = dorewrite(out,data[i]);
      if (retval<0) return retval; else i++;}}
  else if (PAIRP(xtract)) {
    lispval sym = KNO_CAR(xtract);
    if ((sym == KNOSYM_STAR) || (sym == KNOSYM_PLUS) || (sym == KNOSYM_OPT)) {
      lispval elts = KNO_CDR(xtract);
      if (NILP(elts)) {}
      else {
	KNO_DOLIST(elt,elts) {
	  int retval = dorewrite(out,elt);
	  if (retval<0) return retval;}}}
    else if (sym == KNOSYM_LABEL) {
      lispval content = kno_get_arg(xtract,2);
      if (VOIDP(content)) {
	kno_seterr(kno_BadExtractData,"dorewrite",NULL,xtract);
	return -1;}
      else if (dorewrite(out,content)<0) return -1;}
    else if (sym == subst_symbol) {
      lispval args = KNO_CDR(KNO_CDR(xtract)), content, head, params;
      int free_head = 0;
      if (NILP(args)) return 1;
      content = KNO_CAR(KNO_CDR(xtract)); head = KNO_CAR(args); params = KNO_CDR(args);
      if (SYMBOLP(head)) {
	lispval probe = kno_get(texttools_module,head,VOID);
	if (VOIDP(probe)) probe = kno_get(kno_scheme_module,head,VOID);
	if (VOIDP(probe)) {
	  kno_seterr(_("Unknown subst function"),"dorewrite",NULL,head);
	  return -1;}
	head = probe; free_head = 1;}
      if ((STRINGP(head))&&(NILP(params))) {
	u8_putn(out,CSTRING(head),STRLEN(head));
	if (free_head) kno_decref(head);}
      else if (KNO_APPLICABLEP(head)) {
	lispval xformed = rewrite_apply(head,content,params);
	if (KNO_ABORTP(xformed)) {
	  if (free_head) kno_decref(head);
	  return -1;}
	u8_putn(out,CSTRING(xformed),STRLEN(xformed));
	kno_decref(xformed);
	return 1;}
      else {
	KNO_DOLIST(elt,args)
	  if (STRINGP(elt))
	    u8_putn(out,CSTRING(elt),STRLEN(elt));
	  else if (KNO_TRUEP(elt))
	    u8_putn(out,CSTRING(content),STRLEN(content));
	  else if (KNO_APPLICABLEP(elt)) {
	    lispval xformed = rewrite_apply(elt,content,NIL);
	    if (KNO_ABORTP(xformed)) {
	      if (free_head) kno_decref(head);
	      return -1;}
	    u8_putn(out,CSTRING(xformed),STRLEN(xformed));
	    kno_decref(xformed);}
	  else {}
	if (free_head) kno_decref(head);
	return 1;}}
    else {
      kno_seterr(kno_BadExtractData,"dorewrite",NULL,xtract);
      return -1;}}
  else {
    kno_seterr(kno_BadExtractData,"dorewrite",NULL,xtract);
    return -1;}
  return 1;
}

static lispval rewrite_apply(lispval fcn,lispval content,lispval args)
{
  if (NILP(args))
    return kno_apply(fcn,1,&content);
  else {
    lispval argvec[16]; int i = 1;
    KNO_DOLIST(arg,args) {
      if (i>=16) return kno_err(kno_TooManyArgs,"rewrite_apply",NULL,fcn);
      else argvec[i++]=arg;}
    argvec[0]=content;
    return kno_apply(fcn,i,argvec);}
}

DEFPRIM4("textrewrite",textrewrite,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "`(TEXTREWRITE *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval textrewrite(lispval pattern,lispval string,
			   lispval offset,lispval limit)
{
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return kno_err(kno_RangeError,"textrewrite",NULL,VOID);
  else if ((lim-off)==0)
    return kno_mkstring("");
  else {
    lispval extract_results = kno_text_extract
      (pattern,NULL,CSTRING(string),off,lim,0);
    if (KNO_ABORTP(extract_results))
      return extract_results;
    else {
      lispval subst_results = EMPTY;
      DO_CHOICES(extraction,extract_results)
	if ((kno_getint(KNO_CAR(extraction))) == lim) {
	  struct U8_OUTPUT out; lispval stringval;
	  U8_INIT_OUTPUT(&out,(lim-off)*2);
	  if (dorewrite(&out,KNO_CDR(extraction))<0) {
	    kno_decref(subst_results); kno_decref(extract_results);
	    u8_free(out.u8_outbuf); return KNO_ERROR;}
	  stringval = kno_stream2string(&out);
	  CHOICE_ADD(subst_results,stringval);}
      kno_decref(extract_results);
      return subst_results;}}
}

DEFPRIM5("textsubst",textsubst,KNO_MAX_ARGS(5)|KNO_MIN_ARGS(2),
	 "`(TEXTSUBST *arg0* *arg1* [*arg2*] [*arg3*] [*arg4*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_CPP_INT(0),
	 kno_fixnum_type,KNO_VOID);
static lispval textsubst(lispval string,
			 lispval pattern,lispval replace,
			 lispval offset,lispval limit)
{
  u8_byteoff off, lim;
  u8_string data = CSTRING(string);
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return kno_err(kno_RangeError,"textsubst",NULL,VOID);
  else if ((lim-off)==0)
    return kno_mkstring("");
  else {
    int start = kno_text_search(pattern,NULL,data,off,lim,0), last = off;
    if (start>=0) {
      U8_OUTPUT out; U8_INIT_OUTPUT(&out,2*(lim-off));
      while (start>=0) {
	lispval match_result=
	  kno_text_matcher(pattern,NULL,CSTRING(string),start,lim,0);
	int end = -1;
	if (KNO_ABORTP(match_result))
	  return match_result;
	else if (FIXNUMP(match_result)) {
	  int point = FIX2INT(match_result);
	  if (point>end) end = point;}
	else {
	  DO_CHOICES(match,match_result) {
	    int point = kno_getint(match);
	    if (point>end) end = point;}
	  kno_decref(match_result);}
	if (end<0) {
	  u8_puts(&out,data+last);
	  return kno_stream2string(&out);}
	else if (end>start) {
	  u8_putn(&out,data+last,start-last);
	  if (STRINGP(replace))
	    u8_puts(&out,CSTRING(replace));
	  else {
	    u8_string stringdata = CSTRING(string);
	    lispval lisp_lim = KNO_INT(u8_charoffset(stringdata,lim));
	    lispval replace_pat, xtract;
	    if (VOIDP(replace)) replace_pat = pattern;
	    else replace_pat = replace;
	    xtract = kno_text_extract(replace_pat,NULL,stringdata,start,lim,0);
	    if (KNO_ABORTP(xtract)) {
	      u8_free(out.u8_outbuf);
	      return xtract;}
	    else if (EMPTYP(xtract)) {
	      /* This is the incorrect case where the matcher works
		 but extraction does not.  We simply report an error. */
	      u8_byte buf[256];
	      int showlen = (((end-start)<256)?(end-start):(255));
	      strncpy(buf,data+start,showlen); buf[showlen]='\0';
	      u8_log(LOG_WARN,kno_BadExtractData,
		     "Pattern %q matched '%s' but couldn't extract",
		     pattern,buf);
	      u8_putn(&out,data+start,end-start);}
	    else if ((CHOICEP(xtract)) || (PRECHOICEP(xtract))) {
	      lispval results = EMPTY;
	      DO_CHOICES(xt,xtract) {
		u8_byteoff newstart = kno_getint(KNO_CAR(xt));
		if (newstart == lim) {
		  lispval stringval;
		  struct U8_OUTPUT tmpout;
		  U8_INIT_OUTPUT(&tmpout,512);
		  u8_puts(&tmpout,out.u8_outbuf);
		  if (dorewrite(&tmpout,KNO_CDR(xt))<0) {
		    u8_free(tmpout.u8_outbuf); u8_free(out.u8_outbuf);
		    kno_decref(results); results = KNO_ERROR;
		    KNO_STOP_DO_CHOICES; break;}
		  stringval = kno_stream2string(&tmpout);
		  CHOICE_ADD(results,stringval);}
		else {
		  u8_charoff new_char_off = u8_charoffset(stringdata,newstart);
		  lispval remainder = textsubst
		    (string,pattern,replace,
		     KNO_INT(new_char_off),lisp_lim);
		  if (KNO_ABORTP(remainder)) return remainder;
		  else {
		    DO_CHOICES(rem,remainder) {
		      lispval stringval;
		      struct U8_OUTPUT tmpout;
		      U8_INIT_OUTPUT(&tmpout,512);
		      u8_puts(&tmpout,out.u8_outbuf);
		      if (dorewrite(&tmpout,KNO_CDR(xt))<0) {
			u8_free(tmpout.u8_outbuf); u8_free(out.u8_outbuf);
			kno_decref(results); results = KNO_ERROR;
			KNO_STOP_DO_CHOICES; break;}
		      u8_puts(&tmpout,CSTRING(rem));
		      stringval = kno_stream2string(&tmpout);
		      CHOICE_ADD(results,stringval);}}
		  kno_decref(remainder);}}
	      u8_free(out.u8_outbuf);
	      kno_decref(xtract);
	      return results;}
	    else {
	      if (dorewrite(&out,KNO_CDR(xtract))<0) {
		u8_free(out.u8_outbuf); kno_decref(xtract);
		return KNO_ERROR;}
	      kno_decref(xtract);}}
	  last = end; start = kno_text_search(pattern,NULL,data,last,lim,0);}
	else if (end == lim) break;
	else start = kno_text_search
	       (pattern,NULL,data,forward_char(data,end),lim,0);}
      u8_puts(&out,data+last);
      return kno_stream2string(&out);}
    else if (start== -2)
      return KNO_ERROR;
    else return kno_substring(data+off,data+lim);}
}

/* Gathering and rewriting together */

static lispval gathersubst_base(lispval pattern,lispval string,
				lispval offset,lispval limit,
				int star)
{
  lispval results = EMPTY;
  u8_string data = CSTRING(string);
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return kno_err(kno_RangeError,"textgather",NULL,VOID);
  else {
    int start = kno_text_search(pattern,NULL,data,off,lim,0);
    while ((start>=0)&&(start<lim)) {
      lispval result, extract_result=
	kno_text_extract(pattern,NULL,CSTRING(string),start,lim,0);
      int end = -1; lispval longest = VOID;
      if (KNO_ABORTP(extract_result)) {
	kno_decref(results);
	return extract_result;}
      else {
	DO_CHOICES(extraction,extract_result) {
	  int point = kno_getint(KNO_CAR(extraction));
	  if ((point>end)&&(point<=lim)) {
	    end = point; longest = KNO_CDR(extraction);}}}
      kno_incref(longest);
      kno_decref(extract_result);
      if (end<0) return results;
      else if (end>start) {
	struct U8_OUTPUT tmpout; U8_INIT_OUTPUT(&tmpout,128);
	dorewrite(&tmpout,longest);
	result = kno_stream2string(&tmpout);
	CHOICE_ADD(results,result);
	kno_decref(longest);}
      if (star)
	start = kno_text_search(pattern,NULL,data,forward_char(data,start),lim,0);
      else if (end == lim)
	return results;
      else if (end>start)
	start = kno_text_search(pattern,NULL,data,end,lim,0);
      else start = kno_text_search(pattern,NULL,data,forward_char(data,end),lim,0);}
    if (start== -2) {
      kno_decref(results);
      return KNO_ERROR;}
    else return results;}
}

DEFPRIM4("gathersubst",gathersubst,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "`(GATHERSUBST *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval gathersubst(lispval pattern,lispval string,
			   lispval offset,lispval limit)
{
  return gathersubst_base(pattern,string,offset,limit,0);
}

DEFPRIM4("gathersubst*",gathersubst_star,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "`(GATHERSUBST* *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval gathersubst_star(lispval pattern,lispval string,
				lispval offset,lispval limit)
{
  return gathersubst_base(pattern,string,offset,limit,1);
}

/* Handy filtering functions */

DEFPRIM2("textfilter",textfilter,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(TEXTFILTER *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval textfilter(lispval strings,lispval pattern)
{
  lispval results = EMPTY;
  DO_CHOICES(string,strings)
    if (STRINGP(string)) {
      int rv = kno_text_match(pattern,NULL,CSTRING(string),0,STRLEN(string),0);
      if (rv<0) return KNO_ERROR;
      else if (rv) {
	string = kno_incref(string);
	CHOICE_ADD(results,string);}
      else {}}
    else {
      kno_decref(results);
      return kno_type_error("string","textfiler",string);}
  return results;
}

/* PICK/REJECT predicates */

/* These are matching functions with the arguments reversed
   to be especially useful as arguments to PICK and REJECT. */

static int getnonstring(lispval choice)
{
  DO_CHOICES(x,choice) {
    if (!(STRINGP(x))) {
      KNO_STOP_DO_CHOICES;
      return x;}
    else {}}
  return VOID;
}

DEFPRIM4("string-matches?",string_matches,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(STRING-MATCHES? *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval string_matches(lispval string,lispval pattern,
			      lispval start_arg,lispval end_arg)
{
  int retval;
  u8_byteoff off, lim;
  lispval notstring;
  if (QCHOICEP(pattern)) pattern = (KNO_XQCHOICE(pattern))->qchoiceval;
  if ((EMPTYP(pattern))||(EMPTYP(string)))
    return KNO_FALSE;
  notstring = ((STRINGP(string))?(VOID):
	       (KNO_AMBIGP(string))?(getnonstring(string)):
	       (string));
  if (!(VOIDP(notstring)))
    return kno_type_error("string","string_matches",notstring);
  else if (KNO_AMBIGP(string)) {
    DO_CHOICES(s,string) {
      convert_offsets(s,start_arg,end_arg,&off,&lim);
      if ((off<0) || (lim<0)) {
	KNO_STOP_DO_CHOICES;
	return kno_err(kno_RangeError,"textmatcher",NULL,VOID);}
      else retval = kno_text_match(pattern,NULL,CSTRING(s),off,lim,0);
      if (retval!=0) {
	KNO_STOP_DO_CHOICES;
	if (retval<0) return KNO_ERROR;
	else return KNO_TRUE;}}
    return KNO_FALSE;}
  else {
    convert_offsets(string,start_arg,end_arg,&off,&lim);
    if ((off<0) || (lim<0))
      return kno_err(kno_RangeError,"textmatcher",NULL,VOID);
    else retval = kno_text_match(pattern,NULL,CSTRING(string),off,lim,0);
    if (retval<0) return KNO_ERROR;
    else if (retval) return KNO_TRUE;
    else return KNO_FALSE;}
}

DEFPRIM4("string-contains?",string_contains,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(STRING-CONTAINS? *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval string_contains(lispval string,lispval pattern,
			       lispval start_arg,lispval end_arg)
{
  int retval;
  u8_byteoff off, lim;
  lispval notstring;
  if (QCHOICEP(pattern)) pattern = (KNO_XQCHOICE(pattern))->qchoiceval;
  if ((EMPTYP(pattern))||(EMPTYP(string)))
    return KNO_FALSE;
  notstring = ((STRINGP(string))?(VOID):
	       (KNO_AMBIGP(string))?(getnonstring(string)):
	       (string));
  if (!(VOIDP(notstring)))
    return kno_type_error("string","string_matches",notstring);
  else if (KNO_AMBIGP(string)) {
    DO_CHOICES(s,string) {
      convert_offsets(s,start_arg,end_arg,&off,&lim);
      if ((off<0) || (lim<0)) {
	KNO_STOP_DO_CHOICES;
	return kno_err(kno_RangeError,"textmatcher",NULL,VOID);}
      else retval = kno_text_search(pattern,NULL,CSTRING(s),off,lim,0);
      if (retval== -1) {}
      else if (retval<0) {
	KNO_STOP_DO_CHOICES;
	return KNO_ERROR;}
      else {
	KNO_STOP_DO_CHOICES;
	return KNO_TRUE;}}
    return KNO_FALSE;}
  else {
    convert_offsets(string,start_arg,end_arg,&off,&lim);
    if ((off<0) || (lim<0))
      return kno_err(kno_RangeError,"textmatcher",NULL,VOID);
    else retval = kno_text_search(pattern,NULL,CSTRING(string),off,lim,0);
    if (retval<-1) return KNO_ERROR;
    else if (retval<0) return KNO_FALSE;
    else return KNO_TRUE;}
}

DEFPRIM4("string-starts-with?",string_starts_with,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(STRING-STARTS-WITH? *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval string_starts_with(lispval string,lispval pattern,
				  lispval start_arg,lispval end_arg)
{
  u8_byteoff off, lim;
  lispval match_result, notstring;
  if (QCHOICEP(pattern))
    pattern = (KNO_XQCHOICE(pattern))->qchoiceval;
  if ((EMPTYP(pattern))||(EMPTYP(string)))
    return KNO_FALSE;
  notstring = ((STRINGP(string))?(VOID):
	       (KNO_AMBIGP(string))?(getnonstring(string)):
	       (string));
  if (!(VOIDP(notstring)))
    return kno_type_error("string","string_matches",notstring);
  else if (KNO_AMBIGP(string)) {
    DO_CHOICES(s,string) {
      convert_offsets(s,start_arg,end_arg,&off,&lim);
      if ((off<0) || (lim<0)) {
	KNO_STOP_DO_CHOICES;
	return kno_err(kno_RangeError,"textmatcher",NULL,VOID);}
      match_result = kno_text_matcher(pattern,NULL,CSTRING(s),off,lim,0);
      if (KNO_ABORTP(match_result)) {
	KNO_STOP_DO_CHOICES;
	return KNO_ERROR;}
      else if (EMPTYP(match_result)) {}
      else {
	KNO_STOP_DO_CHOICES;
	kno_decref(match_result);
	return KNO_TRUE;}}
    return KNO_FALSE;}
  else {
    convert_offsets(string,start_arg,end_arg,&off,&lim);
    if ((off<0) || (lim<0))
      return kno_err(kno_RangeError,"textmatcher",NULL,VOID);
    match_result = kno_text_matcher(pattern,NULL,CSTRING(string),off,lim,0);
    if (KNO_ABORTP(match_result))
      return match_result;
    else if (EMPTYP(match_result))
      return KNO_FALSE;
    else {
      kno_decref(match_result);
      return KNO_TRUE;}}
}

static lispval string_ends_with_test(lispval string,lispval pattern,
				     int off,int lim)
{
  u8_string data = CSTRING(string); int start;
  lispval end = KNO_INT(lim);
  if (QCHOICEP(pattern)) pattern = (KNO_XQCHOICE(pattern))->qchoiceval;
  if (EMPTYP(pattern)) return KNO_FALSE;
  start = kno_text_search(pattern,NULL,data,off,lim,0);
  /* -2 is an error, -1 is not found */
  if (start<-1) return -1;
  while (start>=0) {
    lispval matches = kno_text_matcher(pattern,NULL,data,start,lim,0);
    if (KNO_ABORTP(matches))
      return -1;
    else if (matches == end)
      return 1;
    else if (FIXNUMP(matches)) {}
    else {
      DO_CHOICES(match,matches)
	if (match == end) {
	  kno_decref(matches);
	  KNO_STOP_DO_CHOICES;
	  return 1;}
      kno_decref(matches);}
    start = kno_text_search(pattern,NULL,data,forward_char(data,start),lim,0);
    if (start<-1) return -1;}
  return 0;
}

DEFPRIM4("string-ends-with?",string_ends_with,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(STRING-ENDS-WITH? *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval string_ends_with(lispval string,lispval pattern,
				lispval start_arg,lispval end_arg)
{
  int retval;
  u8_byteoff off, lim;
  lispval notstring;
  if (EMPTYP(string)) return KNO_FALSE;
  notstring = ((STRINGP(string))?(VOID):
	       (KNO_AMBIGP(string))?(getnonstring(string)):
	       (string));
  if (!(VOIDP(notstring)))
    return kno_type_error("string","string_matches",notstring);
  convert_offsets(string,start_arg,end_arg,&off,&lim);
  if ((off<0) || (lim<0))
    return kno_err(kno_RangeError,"textmatcher",NULL,VOID);
  else if (KNO_AMBIGP(string)) {
    DO_CHOICES(s,string) {
      convert_offsets(s,start_arg,end_arg,&off,&lim);
      if ((off<0) || (lim<0)) {
	KNO_STOP_DO_CHOICES;
	return kno_err(kno_RangeError,"textmatcher",NULL,VOID);}
      retval = string_ends_with_test(s,pattern,off,lim);
      if (retval<0) return KNO_ERROR;
      else if (retval) {
	KNO_STOP_DO_CHOICES; return KNO_TRUE;}
      else {}}
    return KNO_FALSE;}
  else {
    convert_offsets(string,start_arg,end_arg,&off,&lim);
    if ((off<0) || (lim<0))
      return kno_err(kno_RangeError,"textmatcher",NULL,VOID);
    if (string_ends_with_test(string,pattern,off,lim))
      return KNO_TRUE;
    else return KNO_FALSE;}
}

/* text2frame */

static int framify(lispval f,u8_output out,lispval xtract)
{
  if (STRINGP(xtract)) {
    if (out) u8_putn(out,CSTRING(xtract),STRLEN(xtract));}
  else if (VECTORP(xtract)) {
    int i = 0, len = VEC_LEN(xtract);
    lispval *data = VEC_DATA(xtract);
    while (i<len) {
      int retval = framify(f,out,data[i]);
      if (retval<0) return retval; else i++;}}
  else if (PAIRP(xtract)) {
    lispval sym = KNO_CAR(xtract);
    if ((sym == KNOSYM_STAR) || (sym == KNOSYM_PLUS) || (sym == KNOSYM_OPT)) {
      lispval elts = KNO_CDR(xtract);
      if (NILP(elts)) {}
      else {
	KNO_DOLIST(elt,elts) {
	  int retval = framify(f,out,elt);
	  if (retval<0) return retval;}}}
    else if (sym == KNOSYM_LABEL) {
      lispval slotid = kno_get_arg(xtract,1);
      lispval content = kno_get_arg(xtract,2);
      if (VOIDP(content)) {
	kno_seterr(kno_BadExtractData,"framify",NULL,xtract);
	return -1;}
      else if (!((SYMBOLP(slotid)) || (OIDP(slotid)))) {
	kno_seterr(kno_BadExtractData,"framify",NULL,xtract);
	return -1;}
      else {
	lispval parser = kno_get_arg(xtract,3);
	struct U8_OUTPUT _out; int retval;
	U8_INIT_OUTPUT(&_out,128);
	retval = framify(f,&_out,content);
	if (retval<0) return -1;
	else if (out)
	  u8_putn(out,_out.u8_outbuf,_out.u8_write-_out.u8_outbuf);
	if (VOIDP(parser)) {
	  lispval stringval = kno_stream2string(&_out);
	  kno_add(f,slotid,stringval);
	  kno_decref(stringval);}
	else if (KNO_APPLICABLEP(parser)) {
	  lispval stringval = kno_stream2string(&_out);
	  lispval parsed_val = kno_dapply(parser,1,&stringval);
	  if (!(KNO_ABORTP(parsed_val)))
	    kno_add(f,slotid,parsed_val);
	  kno_decref(parsed_val);
	  kno_decref(stringval);
	  if (KNO_ABORTP(parsed_val)) return -1;}
	else if (KNO_TRUEP(parser)) {
	  lispval parsed_val = kno_parse(_out.u8_outbuf);
	  kno_add(f,slotid,parsed_val);
	  kno_decref(parsed_val);
	  u8_free(_out.u8_outbuf);}
	else {
	  lispval stringval = kno_stream2string(&_out);
	  kno_add(f,slotid,stringval);
	  kno_decref(stringval);}
	return 1;}}
    else if (sym == subst_symbol) {
      lispval content = kno_get_arg(xtract,2);
      if (VOIDP(content)) {
	kno_seterr(kno_BadExtractData,"framify",NULL,xtract);
	return -1;}
      else if (framify(f,out,content)<0) return -1;}
    else {
      kno_seterr(kno_BadExtractData,"framify",NULL,xtract);
      return -1;}}
  else {
    kno_seterr(kno_BadExtractData,"framify",NULL,xtract);
    return -1;}
  return 1;
}

DEFPRIM4("text->frame",text2frame,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "`(TEXT->FRAME *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval text2frame(lispval pattern,lispval string,
			  lispval offset,lispval limit)
{
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return kno_err(kno_RangeError,"text2frame",NULL,VOID);
  else {
    lispval extract_results=
      kno_text_extract(pattern,NULL,CSTRING(string),off,lim,0);
    if (KNO_ABORTP(extract_results)) return extract_results;
    else {
      lispval frame_results = EMPTY;
      DO_CHOICES(extraction,extract_results) {
	if (kno_getint(KNO_CAR(extraction)) == lim) {
	  lispval frame = kno_empty_slotmap();
	  if (framify(frame,NULL,KNO_CDR(extraction))<0) {
	    kno_decref(frame);
	    kno_decref(frame_results);
	    kno_decref(extract_results);
	    return KNO_ERROR;}
	  CHOICE_ADD(frame_results,frame);}}
      kno_decref(extract_results);
      return frame_results;}}
}

DEFPRIM4("text->frames",text2frames,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "`(TEXT->FRAMES *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_fixnum_type,KNO_CPP_INT(0),kno_fixnum_type,KNO_VOID);
static lispval text2frames(lispval pattern,lispval string,
			   lispval offset,lispval limit)
{
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return kno_err(kno_RangeError,"text2frames",NULL,VOID);
  else {
    lispval results = EMPTY;
    u8_string data = CSTRING(string);
    int start = kno_text_search(pattern,NULL,data,off,lim,0);
    while (start>=0) {
      lispval extractions = kno_text_extract
	(pattern,NULL,CSTRING(string),start,lim,0);
      lispval longest = EMPTY;
      int max = -1;
      if (KNO_ABORTP(extractions)) {
	kno_decref(results);
	return extractions;}
      else if ((CHOICEP(extractions)) || (PRECHOICEP(extractions))) {
	DO_CHOICES(extraction,extractions) {
	  int xlen = kno_getint(KNO_CAR(extraction));
	  if (xlen == max) {
	    lispval cdr = KNO_CDR(extraction);
	    kno_incref(cdr); CHOICE_ADD(longest,cdr);}
	  else if (xlen>max) {
	    kno_decref(longest); longest = kno_incref(KNO_CDR(extraction));
	    max = xlen;}
	  else {}}}
      else if (EMPTYP(extractions)) {}
      else {
	max = kno_getint(KNO_CAR(extractions));
	longest = kno_incref(KNO_CDR(extractions));}
      /* Should we signal an internal error here if longest is empty,
	 since search stopped at start, but we don't have a match? */
      {
	DO_CHOICES(extraction,longest) {
	  lispval f = kno_empty_slotmap();
	  framify(f,NULL,extraction);
	  CHOICE_ADD(results,f);}}
      kno_decref(longest);
      kno_decref(extractions);
      if (max>start)
	start = kno_text_search(pattern,NULL,data,max,lim,0);
      else if (max == lim)
	return results;
      else start = kno_text_search
	     (pattern,NULL,data,forward_char(data,max),lim,0);}
    if (start== -2) {
      kno_decref(results);
      return KNO_ERROR;}
    else return results;}
}

/* Slicing */

static int interpret_keep_arg(lispval keep_arg)
{
  if (FALSEP(keep_arg)) return 0;
  else if (KNO_TRUEP(keep_arg)) return 1;
  else if (KNO_INTP(keep_arg))
    return FIX2INT(keep_arg);
  else if (KNO_EQ(keep_arg,KNOSYM_SUFFIX))
    return 1;
  else if (KNO_EQ(keep_arg,KNOSYM_PREFIX))
    return -1;
  else if (KNO_EQ(keep_arg,KNOSYM_SEP))
    return 2;
  else return 0;
}

DEFPRIM5("textslice",textslice,KNO_MAX_ARGS(5)|KNO_MIN_ARGS(2),
	 "`(TEXTSLICE *arg0* *arg1* [*arg2*] [*arg3*] [*arg4*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_TRUE,kno_fixnum_type,KNO_CPP_INT(0),
	 kno_fixnum_type,KNO_VOID);
static lispval textslice(lispval string,lispval sep,lispval keep_arg,
			 lispval offset,lispval limit)
{
  u8_byteoff start, len;
  convert_offsets(string,offset,limit,&start,&len);
  if ((start<0) || (len<0))
    return kno_err(kno_RangeError,"textslice",NULL,VOID);
  else {
    /* We accumulate a list CDRwards */
    lispval slices = NIL, *tail = &slices;
    u8_string data = CSTRING(string);
    /* keep indicates whether matched separators go with the preceding
       string (keep<0), the succeeding string (keep>0) or is discarded
       (keep = 0). */
    int keep = interpret_keep_arg(keep_arg);
    /* scan is pointing at a substring matching the sep,
       start is where we last added a string, and end is the greedy limit
       of the matched sep. */
    u8_byteoff scan = kno_text_search(sep,NULL,data,start,len,0);
    while ((scan>=0) && (scan<len)) {
      lispval match_result=
	kno_text_matcher(sep,NULL,data,scan,len,0);
      lispval sepstring = VOID, substring = VOID, newpair;
      int end = -1;
      if (KNO_ABORTP(match_result))
	return match_result;
      else if (FIXNUMP(match_result)) {
	int point = FIX2INT(match_result);
	if (point>end) end = point;}
      else {
	/* Figure out how long the sep is, taking the longest result. */
	DO_CHOICES(match,match_result) {
	  int point = kno_getint(match); if (point>end) end = point;}
	kno_decref(match_result);}
      /* Here's what it should look like:
	 [start] ... [scan] ... [end]
	 where [start] was the beginning of the current scan,
	 [scan] is where the separator starts and [end] is where the
	 separator ends */
      /* If you're attaching separator as prefixes (keep<0),
	 extract the string from start to end, otherwise,
	 just attach start to scan. */
      if ( (end > 0) && (start >= 0) && (end > start) )
	if (keep == 0)
	  substring = kno_substring(data+start,data+scan);
	else if (keep == 2) {
	  sepstring = kno_substring(data+scan,data+end);
	  substring = kno_substring(data+start,data+scan);}
	else if (keep>0)
	  substring = kno_substring(data+start,data+end);
	else substring = kno_substring(data+start,data+scan);
      else {}
      /* Advance to the next separator.  Use a start from the current
	 separator if you're attaching separators as suffixes (keep<0), and
	 a start from just past the separator if you're dropping
	 separators or attaching them forward (keep>=0). */
      if (end>=0) {
	if (keep<0)
	  start = scan;
	else start = end;}
      if ((end<0) || (scan == end))
	/* If the 'separator' is the empty string, start your
	   search from one character past the current end.  This
	   keeps match/search weirdness from leading to infinite
	   loops. */
	scan = kno_text_search(sep,NULL,data,forward_char(data,scan),len,0);
      else scan = kno_text_search(sep,NULL,data,end,len,0);
      /* Push it onto the list. */
      if (!(VOIDP(substring))) {
	newpair = kno_conspair(substring,NIL);
	*tail = newpair; tail = &(KNO_CDR(newpair));}
      /* Push the separator if you're keeping it */
      if (!(VOIDP(sepstring))) {
	newpair = kno_conspair(sepstring,NIL);
	*tail = newpair; tail = &(KNO_CDR(newpair));}}
    /* scan== -2 indicates a real error, not just a failed search. */
    if (scan== -2) {
      kno_decref(slices);
      return KNO_ERROR;}
    else if (start<len) {
      /* If you ran out of separators, just add the tail end to the list. */
      lispval substring = kno_substring(data+start,data+len);
      lispval newpair = kno_conspair(substring,NIL);
      *tail = newpair; tail = &(KNO_CDR(newpair));}
    return slices;
  }
}

/* Word has-suffix/prefix */

DEFPRIM3("has-word-suffix?",has_word_suffix,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(HAS-WORD-SUFFIX? *arg0* *arg1* [*arg2*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval has_word_suffix(lispval string,lispval suffix,lispval strictarg)
{
  int strict = (KNO_TRUEP(strictarg));
  u8_string string_data = KNO_STRING_DATA(string);
  u8_string suffix_data = KNO_STRING_DATA(suffix);
  int string_len = KNO_STRING_LENGTH(string);
  int suffix_len = KNO_STRING_LENGTH(suffix);
  if (suffix_len>string_len) return KNO_FALSE;
  else if (suffix_len == string_len)
    if (strict) return KNO_FALSE;
    else if (strncmp(string_data,suffix_data,suffix_len) == 0)
      return KNO_TRUE;
    else return KNO_FALSE;
  else {
    u8_string string_data = KNO_STRING_DATA(string);
    u8_string suffix_data = KNO_STRING_DATA(suffix);
    if ((strncmp(string_data+(string_len-suffix_len),
		 suffix_data,
		 suffix_len) == 0) &&
	(string_data[(string_len-suffix_len)-1]==' '))
      return KNO_TRUE;
    else return KNO_FALSE;}
}

DEFPRIM3("has-word-prefix?",has_word_prefix,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(HAS-WORD-PREFIX? *arg0* *arg1* [*arg2*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval has_word_prefix(lispval string,lispval prefix,lispval strictarg)
{
  int strict = (KNO_TRUEP(strictarg));
  u8_string string_data = KNO_STRING_DATA(string);
  u8_string prefix_data = KNO_STRING_DATA(prefix);
  int string_len = KNO_STRING_LENGTH(string);
  int prefix_len = KNO_STRING_LENGTH(prefix);
  if (prefix_len>string_len) return KNO_FALSE;
  else if (prefix_len == string_len)
    if (strict) return KNO_FALSE;
    else if (strncmp(string_data,prefix_data,prefix_len) == 0)
      return KNO_TRUE;
    else return KNO_FALSE;
  else {
    if ((strncmp(string_data,prefix_data,prefix_len) == 0)  &&
	(string_data[prefix_len]==' '))
      return KNO_TRUE;
    else return KNO_FALSE;}
}

DEFPRIM2("firstword",firstword_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(FIRSTWORD *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_TRUE);
static lispval firstword_prim(lispval string,lispval sep)
{
  u8_string string_data = CSTRING(string);
  if (STRINGP(sep)) {
    u8_string end = strstr(string_data,CSTRING(sep));
    if (end) return kno_substring(string_data,end);
    else return kno_incref(string);}
  else if ((VOIDP(sep))||(FALSEP(sep))||(KNO_TRUEP(sep)))  {
    const u8_byte *scan = (u8_byte *)string_data, *last = scan;
    int c = u8_sgetc(&scan); while ((c>0)&&(!(u8_isspace(c)))) {
      last = scan; c = u8_sgetc(&scan);}
    return kno_substring(string_data,last);}
  else {
    int search = kno_text_search(sep,NULL,string_data,0,STRLEN(string),0);
    if (search<0) return kno_incref(string);
    else return kno_substring(string_data,string_data+search);}
}

static int match_end(lispval sep,u8_string data,int off,int lim);
DEFPRIM2("lastword",lastword_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(LASTWORD *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_TRUE);
static lispval lastword_prim(lispval string,lispval sep)
{
  u8_string string_data = CSTRING(string);
  if (STRINGP(sep)) {
    u8_string end = string_data, scan = strstr(string_data,CSTRING(sep));
    if (!(scan)) return kno_incref(string);
    else while (scan) {
	end = scan+STRLEN(sep);
	scan = strstr(scan,CSTRING(sep));}
    return kno_substring(end+STRLEN(string),NULL);}
  else if ((VOIDP(sep))||(FALSEP(sep))||(KNO_TRUEP(sep)))  {
    const u8_byte *scan = (u8_byte *)string_data, *last = scan;
    int c = u8_sgetc(&scan); while (c>0) {
      if (u8_isspace(c)) {
	u8_string word = scan; c = u8_sgetc(&scan);
	while ((c>0)&&(u8_isspace(c))) {word = scan; c = u8_sgetc(&scan);}
	if (c>0) last = word;}
      else c = u8_sgetc(&scan);}
    return kno_substring(last,NULL);}
  else {
    int lim = STRLEN(string);
    int end = 0, search = kno_text_search(sep,NULL,string_data,0,lim,0);
    if (search<0) return kno_incref(string);
    else {
      while (search>=0) {
	end = match_end(sep,string_data,search,lim);
	search = kno_text_search(sep,NULL,string_data,end,lim,0);}
      return kno_substring(string_data+end,NULL);}}
}

static int match_end(lispval sep,u8_string data,int off,int lim)
{
  lispval matches = kno_text_matcher(sep,NULL,data,off,lim,KNO_MATCH_BE_GREEDY);
  if (KNO_ABORTP(matches))
    return -1;
  else if (EMPTYP(matches))
    return forward_char(data,off);
  else if (KNO_UINTP(matches))
    return FIX2INT(matches);
  else if (FIXNUMP(matches))
    return forward_char(data,off);
  else {
    int max = forward_char(data,off);
    DO_CHOICES(match,matches) {
      int matchlen = ((KNO_UINTP(match))?(FIX2INT(match)):(-1));
      if (matchlen>max) max = matchlen;}
    return max;}
}

/* Morphrule */

static int has_suffix(lispval string,lispval suffix)
{
  int slen = STRLEN(string), sufflen = STRLEN(suffix);
  if (slen<sufflen) return 0;
  else if (strncmp(CSTRING(string)+(slen-sufflen),
		   CSTRING(suffix),
		   sufflen)==0)
    return 1;
  else return 0;
}

static lispval check_string(lispval string,lispval lexicon)
{
  if (KNO_TRUEP(lexicon)) return string;
  else if (TYPEP(lexicon,kno_hashset_type))
    if (kno_hashset_get((kno_hashset)lexicon,string))
      return string;
    else return EMPTY;
  else if (TYPEP(lexicon,kno_hashtable_type))
    if (kno_hashtable_probe((kno_hashtable)lexicon,string))
      return string;
    else return EMPTY;
  else if (PAIRP(lexicon)) {
    lispval table = KNO_CAR(lexicon);
    lispval key = KNO_CDR(lexicon);
    lispval value = kno_get(table,string,EMPTY);
    if (EMPTYP(value)) return EMPTY;
    else {
      lispval subvalue = kno_get(value,key,EMPTY);
      if ((EMPTYP(subvalue)) ||
	  (VOIDP(subvalue)) ||
	  (FALSEP(subvalue))) {
	kno_decref(value);
	return EMPTY;}
      else {
	kno_decref(value); kno_decref(subvalue);
	return string;}}}
  else if (KNO_APPLICABLEP(lexicon)) {
    lispval result = kno_dapply(lexicon,1,&string);
    if (KNO_ABORTP(result)) return KNO_ERROR;
    else if (EMPTYP(result)) return EMPTY;
    else if (FALSEP(result)) return EMPTY;
    else {
      kno_decref(result); return string;}}
  else return 0;
}

static lispval apply_suffixrule
(lispval string,lispval suffix,lispval replacement,
 lispval lexicon)
{
  if (STRLEN(string)>128) return EMPTY;
  else if (has_suffix(string,suffix))
    if (STRINGP(replacement)) {
      struct KNO_STRING stack_string; lispval result;
      U8_OUTPUT out; u8_byte buf[256];
      int slen = STRLEN(string), sufflen = STRLEN(suffix);
      int replen = STRLEN(replacement);
      U8_INIT_STATIC_OUTPUT_BUF(out,256,buf);
      u8_putn(&out,CSTRING(string),(slen-sufflen));
      u8_putn(&out,CSTRING(replacement),replen);
      KNO_INIT_STATIC_CONS(&stack_string,kno_string_type);
      stack_string.str_bytes = out.u8_outbuf;
      stack_string.str_bytelen = out.u8_write-out.u8_outbuf;
      result = check_string((lispval)&stack_string,lexicon);
      if (KNO_ABORTP(result)) return result;
      else if (EMPTYP(result)) return result;
      else return kno_deep_copy((lispval)&stack_string);}
    else if (KNO_APPLICABLEP(replacement)) {
      lispval xform = kno_apply(replacement,1,&string);
      if (KNO_ABORTP(xform)) return xform;
      else if (STRINGP(xform)) {
	lispval checked = check_string(xform,lexicon);
	if (STRINGP(checked)) return checked;
	else {
	  kno_decref(xform); return checked;}}
      else {kno_decref(xform); return EMPTY;}}
    else if (VECTORP(replacement)) {
      lispval rewrites = textrewrite(replacement,string,KNO_INT(0),VOID);
      if (KNO_ABORTP(rewrites)) return rewrites;
      else if (KNO_TRUEP(lexicon)) return rewrites;
      else if (CHOICEP(rewrites)) {
	lispval accepted = EMPTY;
	DO_CHOICES(rewrite,rewrites) {
	  if (STRINGP(rewrite)) {
	    lispval checked = check_string(rewrite,lexicon);
	    if (KNO_ABORTP(checked)) {
	      kno_decref(rewrites); return checked;}
	    kno_incref(checked);
	    CHOICE_ADD(accepted,checked);}}
	kno_decref(rewrites);
	return accepted;}
      else if (STRINGP(rewrites))
	return check_string(rewrites,lexicon);
      else { kno_decref(rewrites); return EMPTY;}}
    else return EMPTY;
  else return EMPTY;
}

static lispval apply_morphrule(lispval string,lispval rule,lispval lexicon)
{
  if (VECTORP(rule)) {
    lispval results = EMPTY;
    lispval candidates = textrewrite(rule,string,KNO_INT(0),VOID);
    if (KNO_ABORTP(candidates)) return candidates;
    else if (EMPTYP(candidates)) {}
    else if (KNO_TRUEP(lexicon))
      return candidates;
    else {
      DO_CHOICES(candidate,candidates)
	if (check_string(candidate,lexicon)) {
	  kno_incref(candidate);
	  CHOICE_ADD(results,candidate);}
      kno_decref(candidates);
      if (!(EMPTYP(results))) return results;}}
  else if (NILP(rule))
    if (check_string(string,lexicon))
      return kno_incref(string);
    else return EMPTY;
  else if (PAIRP(rule)) {
    lispval suffixes = KNO_CAR(rule);
    lispval replacement = KNO_CDR(rule);
    lispval results = EMPTY;
    DO_CHOICES(suff,suffixes)
      if (STRINGP(suff)) {
	DO_CHOICES(repl,replacement)
	  if ((STRINGP(repl)) || (VECTORP(repl)) ||
	      (KNO_APPLICABLEP(repl))) {
	    lispval result = apply_suffixrule(string,suff,repl,lexicon);
	    if (KNO_ABORTP(result)) {
	      kno_decref(results); return result;}
	    else {CHOICE_ADD(results,result);}}
	  else {
	    kno_decref(results);
	    return kno_err(kno_BadMorphRule,"morphrule",NULL,rule);}}
      else return kno_err(kno_BadMorphRule,"morphrule",NULL,rule);
    return results;}
  else if (CHOICEP(rule)) {
    lispval results = EMPTY;
    DO_CHOICES(alternate,rule) {
      lispval result = apply_morphrule(string,alternate,lexicon);
      if (KNO_ABORTP(result)) {
	kno_decref(results);
	return result;}
      CHOICE_ADD(results,result);}
    return results;}
  else return kno_type_error(_("morphrule"),"morphrule",rule);
  return EMPTY;
}

static int proper_listp(lispval list)
{
  while (KNO_PAIRP(list)) { list = KNO_CDR(list); }
  if (list == KNO_NIL) return 1; else return 0;
}

DEFPRIM3("morphrule",morphrule,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(MORPHRULE *arg0* *arg1* [*arg2*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_TRUE);
static lispval morphrule(lispval string,lispval rules,lispval lexicon)
{
  if (KNO_CHOICEP(string)) {
    lispval results = KNO_EMPTY;
    KNO_DO_CHOICES(s,string) {
      lispval r = morphrule(s,rules,lexicon);
      if (KNO_ABORTP(r)) {
	KNO_STOP_DO_CHOICES;
	kno_decref(results);
	return r;}
      else {KNO_ADD_TO_CHOICE(results,r);}}
    return results;}
  else  if (NILP(rules))
    if (check_string(string,lexicon)) return kno_incref(string);
    else return EMPTY;
  else if ( (KNO_PAIRP(rules)) && (proper_listp(rules)) ) {
    KNO_DOLIST(rule,rules) {
      lispval result = apply_morphrule(string,rule,lexicon);
      if (KNO_ABORTP(result)) return result;
      if (!(EMPTYP(result))) return result;}
    return KNO_EMPTY;}
  else if (KNO_CHOICEP(rules)) {
    lispval results = KNO_EMPTY;
    KNO_DO_CHOICES(rule,rules) {
      lispval found = apply_morphrule(string,rule,lexicon);
      if ((KNO_ABORTP(found))) {
	KNO_STOP_DO_CHOICES;
	kno_decref(results);
	return found;}
      else {KNO_ADD_TO_CHOICE(results,found);}}
    return results;}
  else return apply_morphrule(string,rules,lexicon);
}

/* textclosure prim */

static lispval textclosure_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval pattern_arg = kno_get_arg(expr,1);
  lispval pattern = kno_eval(pattern_arg,env);
  if (VOIDP(pattern_arg))
    return kno_err(kno_SyntaxError,"textclosure_evalfn",NULL,expr);
  else if (KNO_ABORTP(pattern)) return pattern;
  else {
    lispval closure = kno_textclosure(pattern,env);
    kno_decref(pattern);
    return closure;}
}

DEFPRIM1("textclosure?",textclosurep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(TEXTCLOSURE? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval textclosurep(lispval arg)
{
  if (TYPEP(arg,kno_txclosure_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* ISSUFFIX/ISPREFIX */

DEFPRIM2("is-prefix?",is_prefix_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(IS-PREFIX? *arg0* *arg1*)` **undocumented**",
	 kno_string_type,KNO_VOID,kno_string_type,KNO_VOID);
static lispval is_prefix_prim(lispval prefix,lispval string)
{
  int string_len = KNO_STRING_LENGTH(string);
  int prefix_len = KNO_STRING_LENGTH(prefix);
  if (prefix_len>string_len) return KNO_FALSE;
  else {
    u8_string string_data = KNO_STRING_DATA(string);
    u8_string prefix_data = KNO_STRING_DATA(prefix);
    if (strncmp(string_data,prefix_data,prefix_len) == 0)
      return KNO_TRUE;
    else return KNO_FALSE;}
}

DEFPRIM2("is-suffix?",is_suffix_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(IS-SUFFIX? *arg0* *arg1*)` **undocumented**",
	 kno_string_type,KNO_VOID,kno_string_type,KNO_VOID);
static lispval is_suffix_prim(lispval suffix,lispval string)
{
  int string_len = KNO_STRING_LENGTH(string);
  int suffix_len = KNO_STRING_LENGTH(suffix);
  if (suffix_len>string_len) return KNO_FALSE;
  else {
    u8_string string_data = KNO_STRING_DATA(string);
    u8_string suffix_data = KNO_STRING_DATA(suffix);
    if (strncmp(string_data+(string_len-suffix_len),
		suffix_data,
		suffix_len) == 0)
      return KNO_TRUE;
    else return KNO_FALSE;}
}

/* Reading matches (streaming GATHER) */

static ssize_t get_more_data(u8_input in,size_t lim);

DEFPRIM3("read-match",read_match,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(READ-MATCH *port* *pattern* [*limit*])` **undocumented**",
	 kno_ioport_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval read_match(lispval port,lispval pat,lispval limit_arg)
{
  ssize_t lim;
  U8_INPUT *in = get_input_port(port);
  if (in == NULL)
    return kno_type_error(_("input port"),"record_reader",port);
  if (VOIDP(limit_arg)) lim = 0;
  else if (KNO_UINTP(limit_arg)) lim = FIX2INT(limit_arg);
  else return kno_type_error(_("fixnum"),"record_reader",limit_arg);
  ssize_t buflen = in->u8_inlim-in->u8_read; int eof = 0;
  off_t start = kno_text_search(pat,NULL,in->u8_read,0,buflen,KNO_MATCH_BE_GREEDY);
  lispval ends = ((start>=0)?
		  (kno_text_matcher
		   (pat,NULL,in->u8_read,start,buflen,KNO_MATCH_BE_GREEDY)):
		  (EMPTY));
  size_t end = getlongmatch(ends);
  kno_decref(ends);
  if ((start>=0)&&(end>start)&&
      ((lim==0)|(end<lim))&&
      ((end<buflen)||(eof))) {
    lispval result = kno_substring(in->u8_read+start,in->u8_read+end);
    in->u8_read = in->u8_read+end;
    return result;}
  else if ((lim)&&(end>lim))
    return KNO_EOF;
  else if (in->u8_fillfn)
    while (!((start>=0)&&(end>start)&&((end<buflen)||(eof)))) {
      int delta = get_more_data(in,lim); size_t new_end;
      if (delta==0) {eof = 1; break;}
      buflen = in->u8_inlim-in->u8_read;
      if (start<0)
	start = kno_text_search
	  (pat,NULL,in->u8_read,0,buflen,KNO_MATCH_BE_GREEDY);
      if (start<0) continue;
      ends = ((start>=0)?
	      (kno_text_matcher
	       (pat,NULL,in->u8_read,start,buflen,KNO_MATCH_BE_GREEDY)):
	      (EMPTY));
      new_end = getlongmatch(ends);
      if ((lim>0)&&(new_end>lim)) eof = 1;
      else end = new_end;
      kno_decref(ends);}
  if ((start>=0)&&(end>start)&&((end<buflen)||(eof))) {
    lispval result = kno_substring(in->u8_read+start,in->u8_read+end);
    in->u8_read = in->u8_read+end;
    return result;}
  else return KNO_EOF;
}

static ssize_t get_more_data(u8_input in,size_t lim)
{
  if ((in->u8_inbuf == in->u8_read)&&
      ((in->u8_inlim - in->u8_inbuf) == in->u8_bufsz)) {
    /* This is the case where the buffer is full of unread data */
    size_t bufsz = in->u8_bufsz;
    if (bufsz>=lim)
      return -1;
    else {
      size_t new_size = ((bufsz*2)>=U8_BUF_THROTTLE_POINT)?
	(bufsz+(U8_BUF_THROTTLE_POINT/2)):
	(bufsz*2);
      if (new_size>lim) new_size = lim;
      new_size = u8_grow_input_stream(in,new_size);
      if (new_size > bufsz)
	return in->u8_fillfn(in);
      else return 0;}}
  else return in->u8_fillfn(in);
}

/* Character-based escaped segmentation */

DEFPRIM5("findsep",findsep_prim,KNO_MAX_ARGS(5)|KNO_MIN_ARGS(2),
	 "`(FINDSEP *arg0* *arg1* [*arg2*] [*arg3*] [*arg4*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_character_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_character_type,KNO_CODE2CHAR('\\'));
static lispval findsep_prim(lispval string,lispval sep,
			    lispval offset,lispval limit,
			    lispval esc)
{
  int c = KNO_CHARCODE(sep), e = KNO_CHARCODE(esc);
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return kno_err(kno_RangeError,"findsep_prim",NULL,VOID);
  else if (c>=0x80)
    return kno_type_error("ascii char","findsep_prim",sep);
  else if (e>=0x80)
    return kno_type_error("ascii char","findsep_prim",esc);
  else {
    const u8_byte *str = CSTRING(string), *start = str+off, *limit = str+lim;
    const u8_byte *scan = start, *pos = strchr(scan,c);
    while ((pos) && (scan<limit)) {
      if (pos == start)
	return KNO_INT(u8_charoffset(str,(pos-str)));
      else if (*(pos-1) == e) {
	pos++;
	u8_sgetc(&pos);
	scan = pos;
	pos = strchr(scan,c);}
      else return KNO_INT(u8_charoffset(str,(pos-str)));}
    return KNO_FALSE;}
}

/* Various custom parsing/extraction functions */

DEFPRIM5("splitsep",splitsep_prim,KNO_MAX_ARGS(5)|KNO_MIN_ARGS(2),
	 "`(SPLITSEP *arg0* *arg1* [*arg2*] [*arg3*] [*arg4*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_character_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_character_type,KNO_CODE2CHAR('\\'));
static lispval splitsep_prim(lispval string,lispval sep,
			     lispval offset,lispval limit,
			     lispval esc)
{
  int c = KNO_CHARCODE(sep), e = KNO_CHARCODE(esc);
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return kno_err(kno_RangeError,"splitsep_prim",NULL,VOID);
  else if (c>=0x80)
    return kno_type_error("ascii char","splitsep_prim",sep);
  else if (e>=0x80)
    return kno_type_error("ascii char","splitsep_prim",esc);
  else {
    lispval head = VOID, pair = VOID;
    const u8_byte *str = CSTRING(string), *start = str+off, *limit = str+lim;
    const u8_byte *scan = start, *pos = strchr(scan,c);
    if (pos)
      while ((scan) && (scan<limit)) {
	if ((pos) && (pos>start) && (*(pos-1) == e)) {
	  pos = strchr(pos+1,c);}
	else if (pos == scan) {
	  scan = pos+1; pos = strchr(scan,c);}
	else  {
	  lispval seg = kno_substring(scan,pos);
	  lispval elt = kno_conspair(seg,NIL);
	  if (VOIDP(head)) head = pair = elt;
	  else {
	    KNO_RPLACD(pair,elt); pair = elt;}
	  if (pos) {scan = pos+1; pos = strchr(scan,c);}
	  else scan = NULL;}}
    else head = kno_conspair(kno_incref(string),NIL);
    return head;}
}

static char *stdlib_escapes="ntrfab\\";
static char *stdlib_unescaped="\n\t\r\f\a\b\\";

DEFPRIM4("unslashify",unslashify_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(1),
	 "`(UNSLASHIFY *arg0* [*arg1*] [*arg2*] [*arg3*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_any_type,KNO_FALSE);
static lispval unslashify_prim(lispval string,lispval offset,lispval limit_arg,
			       lispval dostd)
{
  u8_string sdata = CSTRING(string), start, limit, split1;
  int handle_stdlib = (!(FALSEP(dostd)));
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit_arg,&off,&lim);
  if ((off<0) || (lim<0))
    return kno_err(kno_RangeError,"unslashify_prim",NULL,VOID);
  start = sdata+off; limit = sdata+lim; split1 = strchr(start,'\\');
  if ((split1) && (split1<limit)) {
    const u8_byte *scan = start;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,STRLEN(string));
    while (scan) {
      u8_byte *split = strchr(scan,'\\');
      if ((!split) || (split>=limit)) {
	u8_putn(&out,scan,limit-scan); break;}
      else {
	int nc;
	u8_putn(&out,scan,split-scan); scan = split+1;
	nc = u8_sgetc(&scan);
	if ((handle_stdlib) && (nc<0x80) && (isalpha(nc))) {
	  char *cpos = strchr(stdlib_escapes,nc);
	  if (cpos == NULL) {}
	  else nc = stdlib_unescaped[cpos-stdlib_escapes];}
	u8_putc(&out,nc);}}
    return kno_stream2string(&out);}
  else if ((off==0) && (lim == STRLEN(string)))
    return kno_incref(string);
  else return kno_substring(start,limit);
}


/* Phonetic prims */

DEFPRIM2("soundex",soundex_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(SOUNDEX *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_FALSE);
static lispval soundex_prim(lispval string,lispval packetp)
{
  if (FALSEP(packetp))
    return kno_wrapstring(kno_soundex(CSTRING(string)));
  else return kno_init_packet(NULL,4,kno_soundex(CSTRING(string)));
}

DEFPRIM2("metaphone",metaphone_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(METAPHONE *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_FALSE);
static lispval metaphone_prim(lispval string,lispval packetp)
{
  if (FALSEP(packetp))
    return kno_wrapstring(kno_metaphone(CSTRING(string),0));
  else {
    u8_string dblm = kno_metaphone(CSTRING(string),0);
    return kno_init_packet(NULL,strlen(dblm),dblm);}
}

DEFPRIM2("metaphone+",metaphone_plus_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(METAPHONE+ *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_FALSE);
static lispval metaphone_plus_prim(lispval string,lispval packetp)
{
  if (FALSEP(packetp))
    return kno_wrapstring(kno_metaphone(CSTRING(string),1));
  else {
    u8_string dblm = kno_metaphone(CSTRING(string),1);
    return kno_init_packet(NULL,strlen(dblm),dblm);}
}

/* Digest functions */

DEFPRIM1("md5",md5_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(MD5 *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval md5_prim(lispval input)
{
  unsigned char *digest = NULL;
  if (STRINGP(input))
    digest = u8_md5(CSTRING(input),STRLEN(input),NULL);
  else if (PACKETP(input))
    digest = u8_md5(KNO_PACKET_DATA(input),KNO_PACKET_LENGTH(input),NULL);
  else {
    struct KNO_OUTBUF out = { 0 };
    KNO_INIT_BYTE_OUTPUT(&out,1024);
    kno_write_dtype(&out,input);
    digest = u8_md5(out.buffer,out.bufwrite-out.buffer,NULL);
    kno_close_outbuf(&out);}
  if (digest == NULL)
    return KNO_ERROR;
  else return kno_init_packet(NULL,16,digest);

}

DEFPRIM1("sha1",sha1_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SHA1 *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval sha1_prim(lispval input)
{
  unsigned char *digest = NULL;
  if (STRINGP(input))
    digest = u8_sha1(CSTRING(input),STRLEN(input),NULL);
  else if (PACKETP(input))
    digest = u8_sha1(KNO_PACKET_DATA(input),KNO_PACKET_LENGTH(input),NULL);
  else {
    struct KNO_OUTBUF out = { 0 };
    KNO_INIT_BYTE_OUTPUT(&out,1024);
    kno_write_dtype(&out,input);
    digest = u8_sha1(out.buffer,out.bufwrite-out.buffer,NULL);
    kno_close_outbuf(&out);}
  if (digest == NULL)
    return KNO_ERROR;
  else return kno_init_packet(NULL,20,digest);

}

DEFPRIM1("sha256",sha256_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SHA256 *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval sha256_prim(lispval input)
{
  unsigned char *digest = NULL;
  if (STRINGP(input))
    digest = u8_sha256(CSTRING(input),STRLEN(input),NULL);
  else if (PACKETP(input))
    digest = u8_sha256(KNO_PACKET_DATA(input),KNO_PACKET_LENGTH(input),NULL);
  else {
    struct KNO_OUTBUF out = { 0 };
    KNO_INIT_BYTE_OUTPUT(&out,1024);
    kno_write_dtype(&out,input);
    digest = u8_sha256(out.buffer,out.bufwrite-out.buffer,NULL);
    kno_close_outbuf(&out);}
  if (digest == NULL)
    return KNO_ERROR;
  else return kno_init_packet(NULL,32,digest);

}

DEFPRIM1("sha384",sha384_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SHA384 *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval sha384_prim(lispval input)
{
  unsigned char *digest = NULL;
  if (STRINGP(input))
    digest = u8_sha384(CSTRING(input),STRLEN(input),NULL);
  else if (PACKETP(input))
    digest = u8_sha384(KNO_PACKET_DATA(input),KNO_PACKET_LENGTH(input),NULL);
  else {
    struct KNO_OUTBUF out = { 0 };
    KNO_INIT_BYTE_OUTPUT(&out,1024);
    kno_write_dtype(&out,input);
    digest = u8_sha384(out.buffer,out.bufwrite-out.buffer,NULL);
    kno_close_outbuf(&out);}
  if (digest == NULL)
    return KNO_ERROR;
  else return kno_init_packet(NULL,48,digest);

}

DEFPRIM1("sha512",sha512_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SHA512 *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval sha512_prim(lispval input)
{
  unsigned char *digest = NULL;
  if (STRINGP(input))
    digest = u8_sha512(CSTRING(input),STRLEN(input),NULL);
  else if (PACKETP(input))
    digest = u8_sha512(KNO_PACKET_DATA(input),KNO_PACKET_LENGTH(input),NULL);
  else {
    struct KNO_OUTBUF out = { 0 };
    KNO_INIT_BYTE_OUTPUT(&out,1024);
    kno_write_dtype(&out,input);
    digest = u8_sha512(out.buffer,out.bufwrite-out.buffer,NULL);
    kno_close_outbuf(&out);}
  if (digest == NULL)
    return KNO_ERROR;
  else return kno_init_packet(NULL,64,digest);

}

DEFPRIM2("hmac-sha1",hmac_sha1_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(HMAC-SHA1 *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval hmac_sha1_prim(lispval key,lispval input)
{
  const unsigned char *data, *keydata, *digest = NULL;
  int data_len, key_len, digest_len, free_key = 0, free_data = 0;
  if (STRINGP(input)) {
    data = CSTRING(input); data_len = STRLEN(input);}
  else if (PACKETP(input)) {
    data = KNO_PACKET_DATA(input); data_len = KNO_PACKET_LENGTH(input);}
  else {
    struct KNO_OUTBUF out = { 0 };
    KNO_INIT_BYTE_OUTPUT(&out,1024);
    kno_write_dtype(&out,input);
    data = out.buffer; data_len = out.bufwrite-out.buffer; free_data = 1;}
  if (STRINGP(key)) {
    keydata = CSTRING(key); key_len = STRLEN(key);}
  else if (PACKETP(key)) {
    keydata = KNO_PACKET_DATA(key); key_len = KNO_PACKET_LENGTH(key);}
  else {
    struct KNO_OUTBUF out = { 0 };
    KNO_INIT_BYTE_OUTPUT(&out,1024);
    kno_write_dtype(&out,key);
    keydata = out.buffer; key_len = out.bufwrite-out.buffer; free_key = 1;}
  digest = u8_hmac_sha1(keydata,key_len,data,data_len,NULL,&digest_len);
  if (free_data) u8_free(data);
  if (free_key) u8_free(keydata);
  if (digest == NULL)
    return KNO_ERROR;
  else return kno_init_packet(NULL,digest_len,digest);
}

DEFPRIM2("hmac-sha256",hmac_sha256_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(HMAC-SHA256 *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval hmac_sha256_prim(lispval key,lispval input)
{
  const unsigned char *data, *keydata, *digest = NULL;
  int data_len, key_len, digest_len, free_key = 0, free_data = 0;
  if (STRINGP(input)) {
    data = CSTRING(input); data_len = STRLEN(input);}
  else if (PACKETP(input)) {
    data = KNO_PACKET_DATA(input); data_len = KNO_PACKET_LENGTH(input);}
  else {
    struct KNO_OUTBUF out = { 0 };
    KNO_INIT_BYTE_OUTPUT(&out,1024);
    kno_write_dtype(&out,input);
    data = out.buffer; data_len = out.bufwrite-out.buffer; free_data = 1;}
  if (STRINGP(key)) {
    keydata = CSTRING(key); key_len = STRLEN(key);}
  else if (PACKETP(key)) {
    keydata = KNO_PACKET_DATA(key); key_len = KNO_PACKET_LENGTH(key);}
  else {
    struct KNO_OUTBUF out = { 0 };
    KNO_INIT_BYTE_OUTPUT(&out,1024);
    kno_write_dtype(&out,key);
    keydata = out.buffer; key_len = out.bufwrite-out.buffer; free_key = 1;}
  digest = u8_hmac_sha256(keydata,key_len,data,data_len,NULL,&digest_len);
  if (free_data) u8_free(data);
  if (free_key) u8_free(keydata);
  if (digest == NULL)
    return KNO_ERROR;
  else return kno_init_packet(NULL,digest_len,digest);
}

DEFPRIM2("hmac-sha384",hmac_sha384_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(HMAC-SHA384 *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval hmac_sha384_prim(lispval key,lispval input)
{
  const unsigned char *data, *keydata, *digest = NULL;
  int data_len, key_len, digest_len, free_key = 0, free_data = 0;
  if (STRINGP(input)) {
    data = CSTRING(input); data_len = STRLEN(input);}
  else if (PACKETP(input)) {
    data = KNO_PACKET_DATA(input); data_len = KNO_PACKET_LENGTH(input);}
  else {
    struct KNO_OUTBUF out = { 0 };
    KNO_INIT_BYTE_OUTPUT(&out,1024);
    kno_write_dtype(&out,input);
    data = out.buffer; data_len = out.bufwrite-out.buffer; free_data = 1;}
  if (STRINGP(key)) {
    keydata = CSTRING(key); key_len = STRLEN(key);}
  else if (PACKETP(key)) {
    keydata = KNO_PACKET_DATA(key); key_len = KNO_PACKET_LENGTH(key);}
  else {
    struct KNO_OUTBUF out = { 0 };
    KNO_INIT_BYTE_OUTPUT(&out,1024);
    kno_write_dtype(&out,key);
    keydata = out.buffer; key_len = out.bufwrite-out.buffer; free_key = 1;}
  digest = u8_hmac_sha384(keydata,key_len,data,data_len,NULL,&digest_len);
  if (free_data) u8_free(data);
  if (free_key) u8_free(keydata);
  if (digest == NULL)
    return KNO_ERROR;
  else return kno_init_packet(NULL,digest_len,digest);
}

DEFPRIM2("hmac-sha512",hmac_sha512_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(HMAC-SHA512 *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval hmac_sha512_prim(lispval key,lispval input)
{
  const unsigned char *data, *keydata, *digest = NULL;
  int data_len, key_len, digest_len, free_key = 0, free_data = 0;
  if (STRINGP(input)) {
    data = CSTRING(input); data_len = STRLEN(input);}
  else if (PACKETP(input)) {
    data = KNO_PACKET_DATA(input); data_len = KNO_PACKET_LENGTH(input);}
  else {
    struct KNO_OUTBUF out = { 0 };
    KNO_INIT_BYTE_OUTPUT(&out,1024);
    kno_write_dtype(&out,input);
    data = out.buffer; data_len = out.bufwrite-out.buffer; free_data = 1;}
  if (STRINGP(key)) {
    keydata = CSTRING(key); key_len = STRLEN(key);}
  else if (PACKETP(key)) {
    keydata = KNO_PACKET_DATA(key); key_len = KNO_PACKET_LENGTH(key);}
  else {
    struct KNO_OUTBUF out = { 0 };
    KNO_INIT_BYTE_OUTPUT(&out,1024);
    kno_write_dtype(&out,key);
    keydata = out.buffer; key_len = out.bufwrite-out.buffer; free_key = 1;}
  digest = u8_hmac_sha512(keydata,key_len,data,data_len,NULL,&digest_len);
  if (free_data) u8_free(data);
  if (free_key) u8_free(keydata);
  if (digest == NULL)
    return KNO_ERROR;
  else return kno_init_packet(NULL,digest_len,digest);
}

/* Match def */

DEFPRIM2("matchdef!",matchdef_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(MATCHDEF! *arg0* *arg1*)` **undocumented**",
	 kno_symbol_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval matchdef_prim(lispval symbol,lispval value)
{
  int retval = kno_matchdef(symbol,value);
  if (retval<0) return KNO_ERROR;
  else if (retval) return KNO_TRUE;
  else return KNO_FALSE;
}

/* Initialization */

static int texttools_init = 0;

void kno_init_texttools()
{
  int knoscheme_version = kno_init_scheme();
  if (texttools_init) return;
  u8_register_source_file(_FILEINFO);
  texttools_init = knoscheme_version;
  texttools_module = kno_new_cmodule("texttools",0,kno_init_texttools);
  kno_init_match_c();
  kno_init_phonetic_c();

  kno_def_evalfn(texttools_module,"TEXTCLOSURE",textclosure_evalfn,
		 "*undocumented*");
  link_local_cprims();

  subst_symbol = kno_intern("subst");

  u8_threadcheck();

  kno_finish_module(texttools_module);
}

static void link_local_cprims()
{
  KNO_LINK_PRIM("matchdef!",matchdef_prim,2,texttools_module);
  KNO_LINK_PRIM("hmac-sha512",hmac_sha512_prim,2,texttools_module);
  KNO_LINK_PRIM("hmac-sha384",hmac_sha384_prim,2,texttools_module);
  KNO_LINK_PRIM("hmac-sha256",hmac_sha256_prim,2,texttools_module);
  KNO_LINK_PRIM("hmac-sha1",hmac_sha1_prim,2,texttools_module);
  KNO_LINK_PRIM("sha512",sha512_prim,1,texttools_module);
  KNO_LINK_PRIM("sha384",sha384_prim,1,texttools_module);
  KNO_LINK_PRIM("sha256",sha256_prim,1,texttools_module);
  KNO_LINK_PRIM("sha1",sha1_prim,1,texttools_module);
  KNO_LINK_PRIM("md5",md5_prim,1,texttools_module);
  KNO_LINK_PRIM("metaphone+",metaphone_plus_prim,2,texttools_module);
  KNO_LINK_PRIM("metaphone",metaphone_prim,2,texttools_module);
  KNO_LINK_PRIM("soundex",soundex_prim,2,texttools_module);
  KNO_LINK_PRIM("unslashify",unslashify_prim,4,texttools_module);
  KNO_LINK_PRIM("splitsep",splitsep_prim,5,texttools_module);
  KNO_LINK_PRIM("findsep",findsep_prim,5,texttools_module);
  KNO_LINK_PRIM("read-match",read_match,3,texttools_module);
  KNO_LINK_PRIM("is-suffix?",is_suffix_prim,2,texttools_module);
  KNO_LINK_PRIM("is-prefix?",is_prefix_prim,2,texttools_module);
  KNO_LINK_PRIM("textclosure?",textclosurep,1,texttools_module);
  KNO_LINK_PRIM("morphrule",morphrule,3,texttools_module);
  KNO_LINK_PRIM("lastword",lastword_prim,2,texttools_module);
  KNO_LINK_PRIM("firstword",firstword_prim,2,texttools_module);
  KNO_LINK_PRIM("has-word-prefix?",has_word_prefix,3,texttools_module);
  KNO_LINK_PRIM("has-word-suffix?",has_word_suffix,3,texttools_module);
  KNO_LINK_PRIM("textslice",textslice,5,texttools_module);
  KNO_LINK_PRIM("text->frames",text2frames,4,texttools_module);
  KNO_LINK_PRIM("text->frame",text2frame,4,texttools_module);
  KNO_LINK_PRIM("string-ends-with?",string_ends_with,4,texttools_module);
  KNO_LINK_PRIM("string-starts-with?",string_starts_with,4,texttools_module);
  KNO_LINK_PRIM("string-contains?",string_contains,4,texttools_module);
  KNO_LINK_PRIM("string-matches?",string_matches,4,texttools_module);
  KNO_LINK_PRIM("textfilter",textfilter,2,texttools_module);
  KNO_LINK_PRIM("gathersubst*",gathersubst_star,4,texttools_module);
  KNO_LINK_PRIM("gathersubst",gathersubst,4,texttools_module);
  KNO_LINK_PRIM("textsubst",textsubst,5,texttools_module);
  KNO_LINK_PRIM("textrewrite",textrewrite,4,texttools_module);
  KNO_LINK_PRIM("gather->list",textgather2list,4,texttools_module);
  KNO_LINK_PRIM("gather*",textgather_star,4,texttools_module);
  KNO_LINK_PRIM("gather",textgather,4,texttools_module);
  KNO_LINK_PRIM("textract",textract,4,texttools_module);
  KNO_LINK_PRIM("textsearch",textsearch,4,texttools_module);
  KNO_LINK_PRIM("textmatch",textmatch,4,texttools_module);
  KNO_LINK_PRIM("textmatcher",textmatcher,4,texttools_module);
  KNO_LINK_PRIM("columnize",columnize_prim,3,texttools_module);
  KNO_LINK_PRIM("strip-markup",strip_markup,2,texttools_module);
  KNO_LINK_PRIM("depunct",depunct,1,texttools_module);
  KNO_LINK_PRIM("disemvowel",disemvowel,2,texttools_module);
  KNO_LINK_PRIM("porter-stem",stem_prim,1,texttools_module);
  KNO_LINK_PRIM("markup%",ismarkup_percentage,1,texttools_module);
  KNO_LINK_PRIM("count-words",count_words,1,texttools_module);
  KNO_LINK_PRIM("isalphalen",isalphalen,1,texttools_module);
  KNO_LINK_PRIM("isalpha%",isalpha_percentage,1,texttools_module);
  KNO_LINK_PRIM("isspace%",isspace_percentage,1,texttools_module);
  KNO_LINK_PRIM("seq->phrase",seq2phrase_prim,3,texttools_module);
  KNO_LINK_PRIM("list->phrase",list2phrase_prim,1,texttools_module);
  KNO_LINK_PRIM("vector->frags",vector2frags_prim,3,texttools_module);
  KNO_LINK_PRIM("words->vector",getwordsv_prim,2,texttools_module);
  KNO_LINK_PRIM("getwords",getwords_prim,2,texttools_module);
  KNO_LINK_PRIM("encode-entities",encode_entities_prim,3,texttools_module);
  KNO_LINK_PRIM("decode-entities",decode_entities_prim,1,texttools_module);
  KNO_LINK_PRIM("segment",segment_prim,2,texttools_module);

  KNO_LINK_ALIAS("gather->seq",textgather2list,texttools_module);
}

