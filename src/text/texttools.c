/* C Mode */

/* texttools.c
   This is the core texttools file for the FramerD library
   Copyright (C) 2005-2018 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/sequences.h"
#include "framerd/ports.h"
#include "framerd/texttools.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>

#include <ctype.h>

static lispval texttools_module;

u8_condition fd_BadExtractData=_("Bad extract data");
u8_condition fd_BadMorphRule=_("Bad morphrule");

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
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    return p->port_input;}
  else return NULL;
}

/* This is for greedy matching */
FD_FASTOP size_t getlongmatch(lispval matches)
{
  if (EMPTYP(matches)) return -1;
  else if (FIXNUMP(matches))
    return FIX2INT(matches);
  else if ((CHOICEP(matches)) ||
           (PRECHOICEP(matches))) {
    u8_byteoff max = -1;
    DO_CHOICES(match,matches) {
      u8_byteoff ival = fd_getint(match);
      if (ival>max)
        max = ival;}
    if (max<0)
      return EMPTY;
    else return max;}
  else return fd_getint(matches);
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
      fd_conspair(fd_substring(start,end),NIL);
    *lastp = newcons; lastp = &(FD_CDR(newcons));
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
      else return fd_type_error(_("string"),"dosegment",sep);
    if (brk == NULL) {
      pair = fd_conspair(lispval_string(scan),NIL);
      *resultp = pair;
      return result;}
    pair = fd_conspair(fd_substring(scan,brk),NIL);
    *resultp = pair;
    resultp = &(((struct FD_PAIR *)pair)->cdr);
    scan = brk+STRLEN(sepstring);}
  return result;
}

static lispval segment_prim(lispval inputs,lispval separators)
{
  if (EMPTYP(inputs)) return EMPTY;
  else if (CHOICEP(inputs)) {
    lispval results = EMPTY;
    DO_CHOICES(input,inputs) {
      lispval result = segment_prim(input,separators);
      if (FD_ABORTP(result)) {
        fd_decref(results); return result;}
      CHOICE_ADD(results,result);}
    return results;}
  else if (STRINGP(inputs)) 
    if (VOIDP(separators))
      return whitespace_segment(CSTRING(inputs));
    else return dosegment(CSTRING(inputs),separators);
  else return fd_type_error(_("string"),"dosegment",inputs);
}

static lispval decode_entities_prim(lispval input)
{
  if (STRLEN(input)==0) return fd_incref(input);
  else if (strchr(CSTRING(input),'&')) {
    struct U8_OUTPUT out;
    u8_string scan = CSTRING(input);
    int c = egetc(&scan);
    U8_INIT_OUTPUT(&out,STRLEN(input));
    while (c>=0) {
      u8_putc(&out,c); c = egetc(&scan);}
    return fd_stream2string(&out);}
  else return fd_incref(input);
}

static lispval encode_entities(lispval input,int nonascii,
                              u8_string ascii_chars,lispval other_chars)
{
  struct U8_OUTPUT out;
  u8_string scan = CSTRING(input);
  int c = u8_sgetc(&scan), enc = 0;
  if (STRLEN(input)==0) return fd_incref(input);
  U8_INIT_OUTPUT(&out,2*STRLEN(input));
  if (ascii_chars == NULL) ascii_chars="<&>";
  while (c>=0) {
    if (((c>128)&&(nonascii))||
        ((c<128)&&(ascii_chars)&&(strchr(ascii_chars,c)))) {
      u8_printf(&out,"&#%d",c); enc = 1;}
    else if (EMPTYP(other_chars))
      u8_putc(&out,c);
    else {
      lispval code = FD_CODE2CHAR(c);
      if (fd_choice_containsp(code,other_chars)) {
        u8_printf(&out,"&#%d",c); enc = 1;}
      else u8_putc(&out,c);}
    c = u8_sgetc(&scan);}
  if (enc) return fd_stream2string(&out);
  else {
    u8_free(out.u8_outbuf);
    return fd_incref(input);}
}

static lispval encode_entities_prim(lispval input,lispval chars,
                                    lispval nonascii)
{
  int na = (!((VOIDP(nonascii))||(FALSEP(nonascii))));
  if (STRLEN(input)==0) return fd_incref(input);
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
              lispval xch = FD_CODE2CHAR(c);
              CHOICE_ADD(other_chars,xch);}
            c = u8_sgetc(&scan);}}
        else if (FD_CHARACTERP(xch)) {
          int ch = FD_CHAR2CODE(xch);
          if (ch<128) u8_putc(&ascii_chars,ch);
          else {CHOICE_ADD(other_chars,xch);}}
        else {
          FD_STOP_DO_CHOICES;
          return fd_type_error("character or string",
                               "encode_entities_prim",
                               xch);}}}
    if (EMPTYP(other_chars))
      return encode_entities(input,na,buf,other_chars);
    else {
      lispval oc = fd_simplify_choice(other_chars);
      lispval result = encode_entities(input,na,buf,oc);
      fd_decref(oc);
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

typedef enum FD_TEXTSPAN_TYPE { spacespan, punctspan, wordspan, nullspan }
  textspantype;

static u8_string skip_span(u8_string start,enum FD_TEXTSPAN_TYPE *type)
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

FD_EXPORT lispval fd_words2list(u8_string string,int keep_punct)
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
        ((scan) ? (fd_substring(last,scan)) : (lispval_string(last)));
      newcons = fd_conspair(extraction,NIL);
      *lastp = newcons; lastp = &(FD_CDR(newcons));
      if (scan == NULL) break;
      last = scan; scan = skip_span(last,&spantype);}
    else {
      if (scan == NULL) break;
      last = scan; scan = skip_span(last,&spantype);}
  return result;
}

FD_EXPORT lispval fd_words2vector(u8_string string,int keep_punct)
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
      wordsv[n++]=((scan) ? (fd_substring(last,scan)) :
                   (lispval_string(last)));
      if (scan == NULL) break;
      last = scan; scan = skip_span(last,&spantype);}
    else {
      if (scan == NULL) break;
      last = scan; scan = skip_span(last,&spantype);}
  result = fd_make_vector(n,wordsv);
  if (wordsv!=_buf) u8_free(wordsv);
  return result;
}

static lispval getwords_prim(lispval arg,lispval punctflag)
{
  int keep_punct = ((!(VOIDP(punctflag))) && (FD_TRUEP(punctflag)));
  return fd_words2list(CSTRING(arg),keep_punct);
}

static lispval getwordsv_prim(lispval arg,lispval punctflag)
{
  int keep_punct = ((!(VOIDP(punctflag))) && (FD_TRUEP(punctflag)));
  return fd_words2vector(CSTRING(arg),keep_punct);
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
static lispval vector2frags_prim(lispval vec,lispval window,lispval with_affix)
{
  int i = 0, n = VEC_LEN(vec), minspan = 1, maxspan;
  lispval *data = VEC_DATA(vec), results = EMPTY;
  int with_affixes = (!(FALSEP(with_affix)));
  if (FD_INTP(window)) maxspan = FIX2INT(window);
  else if ((PAIRP(window))&&
           (FD_INTP(FD_CAR(window)))&&
           (FD_INTP(FD_CDR(window)))) {
    minspan = FIX2INT(FD_CAR(window));
    maxspan = FIX2INT(FD_CDR(window));}
  else if ((VOIDP(window))||(FALSEP(window)))
    maxspan = n-1;
  else return fd_type_error(_("fragment spec"),"vector2frags",window);
  if ((maxspan<0)||(minspan<0))
    return fd_type_error(_("natural number"),"vector2frags",window);
  if (n==0) return results;
  else if (n==1) {
    lispval elt = VEC_REF(vec,0); fd_incref(elt);
    return fd_conspair(elt,NIL);}
  else if (maxspan<=0)
    return fd_type_error(_("natural number"),"vector2frags",window);
  if (with_affixes) { int span = maxspan; while (span>=minspan) {
      /* Compute prefix fragments of length = span */
      lispval frag = NIL;
      int i = span-1; while ((i>=0) && (i<n)) {
        lispval elt = data[i]; fd_incref(elt);
        frag = fd_conspair(elt,frag);
        i--;}
      frag = fd_conspair(FD_FALSE,frag);
      CHOICE_ADD(results,frag);
      span--;}}
  /* Compute suffix fragments
     We're a little clever here, because we can use the same sublist
     repeatedly.  */
  if (with_affixes) {
    lispval frag = fd_conspair(FD_FALSE,NIL);
    int stopat = n-maxspan; if (stopat<0) stopat = 0;
    i = n-minspan; while (i>=stopat) {
      lispval elt = data[i]; fd_incref(elt);
      frag = fd_conspair(elt,frag);
      /* We incref it because we're going to point to it from both the
         result and from the next longer frag */
      fd_incref(frag);
      CHOICE_ADD(results,frag);
      i--;}
    /* We need to decref frag here, because we incref'd it above to do
       our list-reuse trick. */
    fd_decref(frag);}
  { /* Now compute internal spans */
    int end = n-1; while (end>=0) {
      lispval frag = NIL;
      int i = end; int lim = end-maxspan;
      if (lim<0) lim = -1;
      while (i>lim) {
        lispval elt = data[i]; fd_incref(elt);
        frag = fd_conspair(elt,frag);
        if ((1+(end-i))>=minspan) {
          fd_incref(frag);
          CHOICE_ADD(results,frag);}
        i--;}
      fd_decref(frag);
      end--;}}

  return results;
}

static lispval list2phrase_prim(lispval arg)
{
  int dospace = 0; struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  {FD_DOLIST(word,arg) {
      if ((FALSEP(word))||(EMPTYP(word))||
          (NILP(word)))
        continue;
      if (dospace) {u8_putc(&out,' ');} else dospace = 1;
      if (STRINGP(word)) u8_puts(&out,FD_STRING_DATA(word));
      else u8_printf(&out,"%q",word);}}
  return fd_stream2string(&out);
}

static lispval seq2phrase_ndhelper
(u8_string base,lispval seq,int start,int end,int dospace);

static lispval seq2phrase_prim(lispval arg,lispval start_arg,lispval end_arg)
{
  if (PRED_FALSE(!(FD_SEQUENCEP(arg))))
    return fd_type_error("sequence","seq2phrase_prim",arg);
  else if (STRINGP(arg)) return fd_incref(arg);
  else if (!(FD_UINTP(start_arg)))
    return fd_type_error("uint","seq2phrase_prim",start_arg);
  else {
    int dospace = 0, start = FIX2INT(start_arg), end;
    int len = fd_seq_length(arg); char tmpbuf[32];

    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
    if (start<0) start = len+start;
    if ((start<0) || (start>len)) {
      return fd_err(fd_RangeError,"seq2phrase_prim",
                    u8_itoa10(FIX2INT(start_arg),tmpbuf),
                    arg);}
    if (!(FD_INTP(end_arg))) end = len;
    else {
      end = FIX2INT(end_arg);
      if (end<0) end = len+end;
      if ((end<0) || (end>len)) {
        return fd_err(fd_RangeError,"seq2phrase_prim",
                      u8_itoa10(FIX2INT(end_arg),tmpbuf),
                      arg);}}
    while (start<end) {
      lispval word = fd_seq_elt(arg,start);
      if (CHOICEP(word)) {
        lispval result=
          seq2phrase_ndhelper(out.u8_outbuf,arg,start,end,dospace);
        fd_decref(word);
        u8_free(out.u8_outbuf);
        return fd_simplify_choice(result);}
      else if ( (FALSEP(word)) || (EMPTYP(word)) || (NILP(word)) ) {
        start++;
        continue;}
      else if (dospace) {u8_putc(&out,' ');} else dospace = 1;
      if (STRINGP(word)) u8_puts(&out,FD_STRING_DATA(word));
      else u8_printf(&out,"%q",word);
      fd_decref(word);
      start++;}
    return fd_stream2string(&out);}
}

static lispval seq2phrase_ndhelper
(u8_string base,lispval seq,int start,int end,int dospace)
{
  if (start == end)
    return fd_lispstring(u8_strdup(base));
  else {
    lispval elt = fd_seq_elt(seq,start), results = EMPTY;
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
    DO_CHOICES(s,elt) {
      lispval result;
      if (!(STRINGP(s))) {
        fd_decref(elt); fd_decref(results);
        u8_free(out.u8_outbuf);
        return fd_type_error(_("string"),"seq2phrase_ndhelper",s);}
      out.u8_write = out.u8_outbuf;
      u8_puts(&out,base);
      if (dospace) u8_putc(&out,' ');
      u8_puts(&out,CSTRING(s));
      result = seq2phrase_ndhelper
        (out.u8_outbuf,seq,forward_char(base,start),end,1);
      if (FD_ABORTP(result)) {
        fd_decref(elt); fd_decref(results);
        u8_free(out.u8_outbuf);
        return result;}
      CHOICE_ADD(results,result);}
    fd_decref(elt);
    u8_free(out.u8_outbuf);
    return results;}
}

/* String predicates */

static lispval isspace_percentage(lispval string)
{
  u8_string scan = CSTRING(string);
  if (*scan=='\0') return FD_INT(0);
  else {
    int non_space = 0, space = 0, c;
    while ((c = egetc(&scan))>=0)
      if (u8_isspace(c)) space++;
      else non_space++;
    return FD_INT((space*100)/(space+non_space));}
}

static lispval isalpha_percentage(lispval string)
{
  u8_string scan = CSTRING(string);
  if (*scan=='\0') return FD_INT(0);
  else {
    int non_alpha = 0, alpha = 0, c;
    while ((c = egetc(&scan))>0)
      if (u8_isalpha(c)) alpha++;
      else non_alpha++;
    return FD_INT((alpha*100)/(alpha+non_alpha));}
}

static lispval isalphalen(lispval string)
{
  u8_string scan = CSTRING(string);
  if (*scan=='\0') return FD_INT(0);
  else {
    int non_alpha = 0, alpha = 0, c;
    while ((c = egetc(&scan))>0)
      if (u8_isalpha(c)) alpha++;
      else non_alpha++;
    return FD_INT(alpha);}
}

static lispval count_words(lispval string)
{
  u8_string scan = CSTRING(string);
  int c = egetc(&scan), word_count = 0;
  while (u8_isspace(c)) c = egetc(&scan);
  if (c<0) return FD_INT(0);
  else while (c>0) {
      while ((c>0) && (!(u8_isspace(c)))) c = egetc(&scan);
      word_count++;
      while ((c>0) && (u8_isspace(c))) c = egetc(&scan);}
  return FD_INT(word_count);
}

static lispval ismarkup_percentage(lispval string)
{
  u8_string scan = CSTRING(string);
  if (*scan=='\0') return FD_INT(0);
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
    else return FD_INT((markup*100)/(content+markup));}
}

/* Stemming */

FD_EXPORT u8_byte *fd_stem_english_word(const u8_byte *original);

static lispval stem_prim(lispval arg)
{
  u8_byte *stemmed = fd_stem_english_word(CSTRING(arg));
  return fd_lispstring(stemmed);
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

static lispval disemvowel(lispval string,lispval vowels)
{
  struct U8_OUTPUT out; struct U8_INPUT in; 
  U8_INIT_STRING_INPUT(&in,STRLEN(string),CSTRING(string));
  U8_INIT_OUTPUT(&out,FD_STRING_LENGTH(string));
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
  return fd_stream2string(&out);
}

/* Depuncting strings (removing punctuation and whitespace) */

static lispval depunct(lispval string)
{
  struct U8_OUTPUT out; struct U8_INPUT in; 
  U8_INIT_STRING_INPUT(&in,STRLEN(string),CSTRING(string));
  U8_INIT_OUTPUT(&out,FD_STRING_LENGTH(string));
  int c = u8_getc(&in);
  while (c>=0) {
    if (!((u8_isspace(c)) || (u8_ispunct(c))))
      u8_putc(&out,c);
    c = u8_getc(&in);}
  return fd_stream2string(&out);
}

/* Skipping markup */

static lispval strip_markup(lispval string,lispval insert_space_arg)
{
  int c, insert_space = FD_TRUEP(insert_space_arg);
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
    return fd_stream2string(&out);}
  else return fd_incref(string);
}

/* Columnizing */

static lispval columnize_prim(lispval string,lispval cols,lispval parse)
{
  u8_string scan = CSTRING(string), limit = scan+STRLEN(string);
  u8_byte *buf;
  int i = 0, field = 0, n_fields = fd_seq_length(cols), parselen = 0;
  lispval *fields;
  while (i<n_fields) {
    lispval elt = fd_seq_elt(cols,i);
    if (FIXNUMP(elt)) i++;
    else return fd_type_error(_("column width"),"columnize_prim",elt);}
  if (FD_SEQUENCEP(parse)) parselen = fd_seq_length(parselen);
  fields = u8_alloc_n(n_fields,lispval);
  buf = u8_malloc(STRLEN(string)+1);
  while (field<n_fields) {
    lispval parsefn;
    int j = 0, width = fd_getint(fd_seq_elt(cols,field));
    u8_string start = scan;
    /* Get the parse function */
    if (field<parselen)
      parsefn = fd_seq_elt(parse,field);
    else parsefn = parse;
    /* Skip over the field */
    while (j<width)
      if (scan>=limit) break;
      else {u8_sgetc(&scan); j++;}
    /* If you're at the end, set the field to a default. */
    if (scan == start)
      if (FALSEP(parsefn))
        fields[field++]=fd_init_string(NULL,0,NULL);
      else fields[field++]=FD_FALSE;
    /* If the parse function is false, make a string. */
    else if (FALSEP(parsefn))
      fields[field++]=fd_substring(start,scan);
    /* If the parse function is #t, use the lisp parser */
    else if (FD_TRUEP(parsefn)) {
      lispval value;
      strncpy(buf,start,scan-start); buf[scan-start]='\0';
      value = fd_parse_arg(buf);
      if (FD_ABORTP(value)) {
        int k = 0; while (k<field) {fd_decref(fields[k]); k++;}
        u8_free(fields);
        u8_free(buf);
        return value;}
      else fields[field++]=value;}
    /* If the parse function is applicable, make a string
       and apply the parse function. */
    else if (FD_APPLICABLEP(parsefn)) {
      lispval stringval = fd_substring(start,scan);
      lispval value = fd_apply(parse,1,&stringval);
      if (field<parselen) fd_decref(parsefn);
      if (FD_ABORTP(value)) {
        int k = 0; while (k<field) {fd_decref(fields[k]); k++;}
        u8_free(fields);
        u8_free(buf);
        fd_decref(stringval);
        return value;}
      else {
        fd_decref(stringval);
        fields[field++]=value;}}
    else {
      int k = 0; while (k<field) {fd_decref(fields[k]); k++;}
      u8_free(fields);
      u8_free(buf);
      return fd_type_error(_("column parse function"),
                           "columnize_prim",parsefn);}}
  u8_free(buf);
  if (FALSEP(parse))
    while (field<n_fields)
      fields[field++]=fd_init_string(NULL,0,NULL);
  else while (field<n_fields) fields[field++]=FD_FALSE;
  lispval result=fd_makeseq(FD_PTR_TYPE(cols),n_fields,fields);
  u8_free(fields);
  return result;
}

/* The Matcher */

static lispval return_offsets(u8_string s,lispval results)
{
  lispval final_results = EMPTY;
  DO_CHOICES(off,results)
    if (FD_UINTP(off)) {
      u8_charoff charoff = u8_charoffset(s,FIX2INT(off));
      CHOICE_ADD(final_results,FD_INT(charoff));}
    else {}
  fd_decref(results);
  return final_results;
}

#define convert_arg(off,string,max) u8_byteoffset(string,off,max)

static void convert_offsets
(lispval string,lispval offset,lispval limit,u8_byteoff *off,u8_byteoff *lim)
{
  u8_charoff offval = fd_getint(offset);
  if (FD_INTP(limit)) {
    int intlim = FIX2INT(limit), len = STRLEN(string);
    u8_string data = CSTRING(string);
    if (intlim<0) {
      int char_len = u8_strlen(data);
      *lim = u8_byteoffset(data,char_len+intlim,len);}
    else *lim = u8_byteoffset(CSTRING(string),intlim,len);}
  else *lim = STRLEN(string);
  *off = u8_byteoffset(CSTRING(string),offval,*lim);
}

static lispval textmatcher(lispval pattern,lispval string,
                          lispval offset,lispval limit)
{
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textmatcher",NULL,VOID);
  else {
    lispval match_result = fd_text_matcher
      (pattern,NULL,CSTRING(string),off,lim,0);
    if (FD_ABORTP(match_result))
      return match_result;
    else return return_offsets(CSTRING(string),match_result);}
}

static lispval textmatch(lispval pattern,lispval string,
                        lispval offset,lispval limit)
{
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textmatch",NULL,VOID);
  else {
    int retval=
      fd_text_match(pattern,NULL,CSTRING(string),off,lim,0);
    if (retval<0) return FD_ERROR;
    else if (retval) return FD_TRUE;
    else return FD_FALSE;}
}

static lispval textsearch(lispval pattern,lispval string,
                         lispval offset,lispval limit)
{
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textsearch",NULL,VOID);
  else {
    int pos = fd_text_search(pattern,NULL,CSTRING(string),off,lim,0);
    if (pos<0)
      if (pos== -2) return FD_ERROR;
      else return FD_FALSE;
    else return FD_INT(u8_charoffset(CSTRING(string),pos));}
}

static lispval textract(lispval pattern,lispval string,
                        lispval offset,lispval limit)
{
  lispval results = EMPTY;
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textract",NULL,VOID);
  else {
    lispval extract_results = fd_text_extract
      (pattern,NULL,CSTRING(string),off,lim,0);
    if (FD_ABORTP(extract_results))
      return extract_results;
    else {
      DO_CHOICES(extraction,extract_results) {
        if (FD_ABORTP(extraction)) {
          fd_decref(results);
          fd_incref(extraction);
          fd_decref(extract_results);
          return extraction;}
        else if (PAIRP(extraction))
          if (fd_getint(FD_CAR(extraction)) == lim) {
            lispval extract = fd_incref(FD_CDR(extraction));
            CHOICE_ADD(results,extract);}
          else {}
        else {}}
      fd_decref(extract_results);
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
    return fd_err(fd_RangeError,"textgather",NULL,VOID);
  else {
    int start = fd_text_search(pattern,NULL,data,off,lim,0);
    while ((start>=0)&&(start<lim)) {
      lispval substring, match_result=
        fd_text_matcher(pattern,NULL,CSTRING(string),start,lim,0);
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
          int pt = fd_getint(match);
          if ((pt>maxpoint)&&(pt<=lim)) {
            maxpoint = pt;}}
        fd_decref(match_result);}
      if (maxpoint<0)
        return results;
      else if (maxpoint>start) {
        substring = fd_substring(data+start,data+maxpoint);
        CHOICE_ADD(results,substring);}
      if (star)
        start = fd_text_search(pattern,NULL,data,forward_char(data,start),lim,0);
      else if (maxpoint == lim) return results;
      else if (maxpoint>start)
        start = fd_text_search(pattern,NULL,data,maxpoint,lim,0);
      else start = fd_text_search(pattern,NULL,data,forward_char(data,start),lim,0);}
    if (start== -2) {
      fd_decref(results);
      return FD_ERROR;}
    else return results;}
}

static lispval textgather(lispval pattern,lispval string,
                         lispval offset,lispval limit)
{
  return textgather_base(pattern,string,offset,limit,0);
}

static lispval textgather_star(lispval pattern,lispval string,
                              lispval offset,lispval limit)
{
  return textgather_base(pattern,string,offset,limit,1);
}

static lispval textgather2list(lispval pattern,lispval string,
                               lispval offset,lispval limit)
{
  lispval head = NIL, *tail = &head;
  u8_string data = CSTRING(string);
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textgather",NULL,VOID);
  else {
    int start = fd_text_search(pattern,NULL,data,off,lim,0);
    while (start>=0) {
      lispval match_result=
        fd_text_matcher(pattern,NULL,CSTRING(string),start,lim,0);
      int end = -1;
      if (EMPTYP(match_result)) {}
      else if (FIXNUMP(match_result)) {
        int point = FIX2INT(match_result);
        if (point>end) end = point;}
      else if (FD_ABORTP(match_result)) {
        fd_decref(head);
        return match_result;}
      else {
        DO_CHOICES(match,match_result) {
          int point = fd_getint(match); if (point>end) end = point;}
        fd_decref(match_result);}
      if (end<0) return head;
      else if (end>start) {
        lispval newpair=
          fd_conspair(fd_substring(data+start,data+end),NIL);
        *tail = newpair; tail = &(FD_CDR(newpair));
        start = fd_text_search(pattern,NULL,data,end,lim,0);}
      else if (end == lim)
        return head;
      else start = fd_text_search
             (pattern,NULL,data,forward_char(data,end),lim,0);}
    if (start== -2) {
      fd_decref(head);
      return FD_ERROR;}
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
    lispval sym = FD_CAR(xtract);
    if ((sym == FDSYM_STAR) || (sym == FDSYM_PLUS) || (sym == FDSYM_OPT)) {
      lispval elts = FD_CDR(xtract);
      if (NILP(elts)) {}
      else {
        FD_DOLIST(elt,elts) {
          int retval = dorewrite(out,elt);
          if (retval<0) return retval;}}}
    else if (sym == FDSYM_LABEL) {
      lispval content = fd_get_arg(xtract,2);
      if (VOIDP(content)) {
        fd_seterr(fd_BadExtractData,"dorewrite",NULL,xtract);
        return -1;}
      else if (dorewrite(out,content)<0) return -1;}
    else if (sym == subst_symbol) {
      lispval args = FD_CDR(FD_CDR(xtract)), content, head, params;
      int free_head = 0;
      if (NILP(args)) return 1;
      content = FD_CAR(FD_CDR(xtract)); head = FD_CAR(args); params = FD_CDR(args);
      if (SYMBOLP(head)) {
        lispval probe = fd_get(texttools_module,head,VOID);
        if (VOIDP(probe)) probe = fd_get(fd_scheme_module,head,VOID);
        if (VOIDP(probe)) {
          fd_seterr(_("Unknown subst function"),"dorewrite",NULL,head);
          return -1;}
        head = probe; free_head = 1;}
      if ((STRINGP(head))&&(NILP(params))) {
        u8_putn(out,CSTRING(head),STRLEN(head));
        if (free_head) fd_decref(head);}
      else if (FD_APPLICABLEP(head)) {
        lispval xformed = rewrite_apply(head,content,params);
        if (FD_ABORTP(xformed)) {
          if (free_head) fd_decref(head);
          return -1;}
        u8_putn(out,CSTRING(xformed),STRLEN(xformed));
        fd_decref(xformed);
        return 1;}
      else {
        FD_DOLIST(elt,args)
          if (STRINGP(elt))
            u8_putn(out,CSTRING(elt),STRLEN(elt));
          else if (FD_TRUEP(elt))
            u8_putn(out,CSTRING(content),STRLEN(content));
          else if (FD_APPLICABLEP(elt)) {
            lispval xformed = rewrite_apply(elt,content,NIL);
            if (FD_ABORTP(xformed)) {
              if (free_head) fd_decref(head);
              return -1;}
            u8_putn(out,CSTRING(xformed),STRLEN(xformed));
            fd_decref(xformed);}
          else {}
        if (free_head) fd_decref(head);
        return 1;}}
    else {
      fd_seterr(fd_BadExtractData,"dorewrite",NULL,xtract);
      return -1;}}
  else {
    fd_seterr(fd_BadExtractData,"dorewrite",NULL,xtract);
    return -1;}
  return 1;
}

static lispval rewrite_apply(lispval fcn,lispval content,lispval args)
{
  if (NILP(args))
    return fd_apply(fcn,1,&content);
  else {
    lispval argvec[16]; int i = 1;
    FD_DOLIST(arg,args) {
      if (i>=16) return fd_err(fd_TooManyArgs,"rewrite_apply",NULL,fcn);
      else argvec[i++]=arg;}
    argvec[0]=content;
    return fd_apply(fcn,i,argvec);}
}

static lispval textrewrite(lispval pattern,lispval string,
                           lispval offset,lispval limit)
{
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textrewrite",NULL,VOID);
  else if ((lim-off)==0)
    return lispval_string("");
  else {
    lispval extract_results = fd_text_extract
      (pattern,NULL,CSTRING(string),off,lim,0);
    if (FD_ABORTP(extract_results))
      return extract_results;
    else {
      lispval subst_results = EMPTY;
      DO_CHOICES(extraction,extract_results)
        if ((fd_getint(FD_CAR(extraction))) == lim) {
          struct U8_OUTPUT out; lispval stringval;
          U8_INIT_OUTPUT(&out,(lim-off)*2);
          if (dorewrite(&out,FD_CDR(extraction))<0) {
            fd_decref(subst_results); fd_decref(extract_results);
            u8_free(out.u8_outbuf); return FD_ERROR;}
          stringval = fd_stream2string(&out);
          CHOICE_ADD(subst_results,stringval);}
      fd_decref(extract_results);
      return subst_results;}}
}

static lispval textsubst(lispval string,
                        lispval pattern,lispval replace,
                        lispval offset,lispval limit)
{
  u8_byteoff off, lim;
  u8_string data = CSTRING(string);
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textsubst",NULL,VOID);
  else if ((lim-off)==0)
    return lispval_string("");
  else {
    int start = fd_text_search(pattern,NULL,data,off,lim,0), last = off;
    if (start>=0) {
      U8_OUTPUT out; U8_INIT_OUTPUT(&out,2*(lim-off));
      while (start>=0) {
        lispval match_result=
          fd_text_matcher(pattern,NULL,CSTRING(string),start,lim,0);
        int end = -1;
        if (FD_ABORTP(match_result))
          return match_result;
        else if (FIXNUMP(match_result)) {
          int point = FIX2INT(match_result);
          if (point>end) end = point;}
        else {
          DO_CHOICES(match,match_result) {
            int point = fd_getint(match);
            if (point>end) end = point;}
          fd_decref(match_result);}
        if (end<0) {
          u8_puts(&out,data+last);
          return fd_stream2string(&out);}
        else if (end>start) {
          u8_putn(&out,data+last,start-last);
          if (STRINGP(replace))
            u8_puts(&out,CSTRING(replace));
          else {
            u8_string stringdata = CSTRING(string);
            lispval lisp_lim = FD_INT(u8_charoffset(stringdata,lim));
            lispval replace_pat, xtract;
            if (VOIDP(replace)) replace_pat = pattern;
            else replace_pat = replace;
            xtract = fd_text_extract(replace_pat,NULL,stringdata,start,lim,0);
            if (FD_ABORTP(xtract)) {
              u8_free(out.u8_outbuf);
              return xtract;}
            else if (EMPTYP(xtract)) {
              /* This is the incorrect case where the matcher works
                 but extraction does not.  We simply report an error. */
              u8_byte buf[256];
              int showlen = (((end-start)<256)?(end-start):(255));
              strncpy(buf,data+start,showlen); buf[showlen]='\0';
              u8_log(LOG_WARN,fd_BadExtractData,
                     "Pattern %q matched '%s' but couldn't extract",
                     pattern,buf);
              u8_putn(&out,data+start,end-start);}
            else if ((CHOICEP(xtract)) || (PRECHOICEP(xtract))) {
              lispval results = EMPTY;
              DO_CHOICES(xt,xtract) {
                u8_byteoff newstart = fd_getint(FD_CAR(xt));
                if (newstart == lim) {
                  lispval stringval;
                  struct U8_OUTPUT tmpout; 
                  U8_INIT_OUTPUT(&tmpout,512);
                  u8_puts(&tmpout,out.u8_outbuf);
                  if (dorewrite(&tmpout,FD_CDR(xt))<0) {
                    u8_free(tmpout.u8_outbuf); u8_free(out.u8_outbuf);
                    fd_decref(results); results = FD_ERROR;
                    FD_STOP_DO_CHOICES; break;}
                  stringval = fd_stream2string(&tmpout);
                  CHOICE_ADD(results,stringval);}
                else {
                  u8_charoff new_char_off = u8_charoffset(stringdata,newstart);
                  lispval remainder = textsubst
                    (string,pattern,replace,
                     FD_INT(new_char_off),lisp_lim);
                  if (FD_ABORTP(remainder)) return remainder;
                  else {
                    DO_CHOICES(rem,remainder) {
                      lispval stringval;
                      struct U8_OUTPUT tmpout; 
                      U8_INIT_OUTPUT(&tmpout,512);
                      u8_puts(&tmpout,out.u8_outbuf);
                      if (dorewrite(&tmpout,FD_CDR(xt))<0) {
                        u8_free(tmpout.u8_outbuf); u8_free(out.u8_outbuf);
                        fd_decref(results); results = FD_ERROR;
                        FD_STOP_DO_CHOICES; break;}
                      u8_puts(&tmpout,CSTRING(rem));
                      stringval = fd_stream2string(&tmpout);
                      CHOICE_ADD(results,stringval);}}
                  fd_decref(remainder);}}
              u8_free(out.u8_outbuf);
              fd_decref(xtract);
              return results;}
            else {
              if (dorewrite(&out,FD_CDR(xtract))<0) {
                u8_free(out.u8_outbuf); fd_decref(xtract);
                return FD_ERROR;}
              fd_decref(xtract);}}
          last = end; start = fd_text_search(pattern,NULL,data,last,lim,0);}
        else if (end == lim) break;
        else start = fd_text_search
               (pattern,NULL,data,forward_char(data,end),lim,0);}
      u8_puts(&out,data+last);
      return fd_stream2string(&out);}
    else if (start== -2) 
      return FD_ERROR;
    else return fd_substring(data+off,data+lim);}
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
    return fd_err(fd_RangeError,"textgather",NULL,VOID);
  else {
    int start = fd_text_search(pattern,NULL,data,off,lim,0);
    while ((start>=0)&&(start<lim)) {
      lispval result, extract_result=
        fd_text_extract(pattern,NULL,CSTRING(string),start,lim,0);
      int end = -1; lispval longest = VOID;
      if (FD_ABORTP(extract_result)) {
        fd_decref(results);
        return extract_result;}
      else {
        DO_CHOICES(extraction,extract_result) {
          int point = fd_getint(FD_CAR(extraction));
          if ((point>end)&&(point<=lim)) {
            end = point; longest = FD_CDR(extraction);}}}
      fd_incref(longest);
      fd_decref(extract_result);
      if (end<0) return results;
      else if (end>start) {
        struct U8_OUTPUT tmpout; U8_INIT_OUTPUT(&tmpout,128);
        dorewrite(&tmpout,longest);
        result = fd_stream2string(&tmpout);
        CHOICE_ADD(results,result);
        fd_decref(longest);}
      if (star)
        start = fd_text_search(pattern,NULL,data,forward_char(data,start),lim,0);
      else if (end == lim)
        return results;
      else if (end>start)
        start = fd_text_search(pattern,NULL,data,end,lim,0);
      else start = fd_text_search(pattern,NULL,data,forward_char(data,end),lim,0);}
    if (start== -2) {
      fd_decref(results);
      return FD_ERROR;}
    else return results;}
}

static lispval gathersubst(lispval pattern,lispval string,
                          lispval offset,lispval limit)
{
  return gathersubst_base(pattern,string,offset,limit,0);
}

static lispval gathersubst_star(lispval pattern,lispval string,
                               lispval offset,lispval limit)
{
  return gathersubst_base(pattern,string,offset,limit,1);
}

/* Handy filtering functions */

static lispval textfilter(lispval strings,lispval pattern)
{
  lispval results = EMPTY;
  DO_CHOICES(string,strings)
    if (STRINGP(string)) {
      int rv = fd_text_match(pattern,NULL,CSTRING(string),0,STRLEN(string),0);
      if (rv<0) return FD_ERROR;
      else if (rv) {
        string = fd_incref(string);
        CHOICE_ADD(results,string);}
      else {}}
    else {
      fd_decref(results);
      return fd_type_error("string","textfiler",string);}
  return results;
}

/* PICK/REJECT predicates */

/* These are matching functions with the arguments reversed
   to be especially useful as arguments to PICK and REJECT. */

static int getnonstring(lispval choice)
{
  DO_CHOICES(x,choice) {
    if (!(STRINGP(x))) {
      FD_STOP_DO_CHOICES;
      return x;}
    else {}}
  return VOID;
}

static lispval string_matches(lispval string,lispval pattern,
                             lispval start_arg,lispval end_arg)
{
  int retval;
  u8_byteoff off, lim;
  lispval notstring;
  if (QCHOICEP(pattern)) pattern = (FD_XQCHOICE(pattern))->qchoiceval;
  if ((EMPTYP(pattern))||(EMPTYP(string)))
    return FD_FALSE;
  notstring = ((STRINGP(string))?(VOID):
             (FD_AMBIGP(string))?(getnonstring(string)):
             (string));
  if (!(VOIDP(notstring)))
    return fd_type_error("string","string_matches",notstring);
  else if (FD_AMBIGP(string)) {
    DO_CHOICES(s,string) {
      convert_offsets(s,start_arg,end_arg,&off,&lim);
      if ((off<0) || (lim<0)) {
        FD_STOP_DO_CHOICES;
        return fd_err(fd_RangeError,"textmatcher",NULL,VOID);}
      else retval = fd_text_match(pattern,NULL,CSTRING(s),off,lim,0);
      if (retval!=0) {
        FD_STOP_DO_CHOICES;
        if (retval<0) return FD_ERROR;
        else return FD_TRUE;}}
    return FD_FALSE;}
  else {
    convert_offsets(string,start_arg,end_arg,&off,&lim);
    if ((off<0) || (lim<0)) 
      return fd_err(fd_RangeError,"textmatcher",NULL,VOID);
    else retval = fd_text_match(pattern,NULL,CSTRING(string),off,lim,0);
    if (retval<0) return FD_ERROR;
    else if (retval) return FD_TRUE;
    else return FD_FALSE;}
}

static lispval string_contains(lispval string,lispval pattern,
                              lispval start_arg,lispval end_arg)
{
  int retval;
  u8_byteoff off, lim;
  lispval notstring;
  if (QCHOICEP(pattern)) pattern = (FD_XQCHOICE(pattern))->qchoiceval;
  if ((EMPTYP(pattern))||(EMPTYP(string)))
    return FD_FALSE;
  notstring = ((STRINGP(string))?(VOID):
             (FD_AMBIGP(string))?(getnonstring(string)):
             (string));
  if (!(VOIDP(notstring)))
    return fd_type_error("string","string_matches",notstring);
  else if (FD_AMBIGP(string)) {
    DO_CHOICES(s,string) {
      convert_offsets(s,start_arg,end_arg,&off,&lim);
      if ((off<0) || (lim<0)) {
        FD_STOP_DO_CHOICES;
        return fd_err(fd_RangeError,"textmatcher",NULL,VOID);}
      else retval = fd_text_search(pattern,NULL,CSTRING(s),off,lim,0);
      if (retval== -1) {}
      else if (retval<0) {
        FD_STOP_DO_CHOICES;
        return FD_ERROR;}
      else {
        FD_STOP_DO_CHOICES;
        return FD_TRUE;}}
    return FD_FALSE;}
  else {
    convert_offsets(string,start_arg,end_arg,&off,&lim);
    if ((off<0) || (lim<0)) 
      return fd_err(fd_RangeError,"textmatcher",NULL,VOID);
    else retval = fd_text_search(pattern,NULL,CSTRING(string),off,lim,0);
    if (retval<-1) return FD_ERROR;
    else if (retval<0) return FD_FALSE;
    else return FD_TRUE;}
}

static lispval string_starts_with(lispval string,lispval pattern,
                                 lispval start_arg,lispval end_arg)
{
  u8_byteoff off, lim;
  lispval match_result, notstring;
  if (QCHOICEP(pattern))
    pattern = (FD_XQCHOICE(pattern))->qchoiceval;
  if ((EMPTYP(pattern))||(EMPTYP(string)))
    return FD_FALSE;
  notstring = ((STRINGP(string))?(VOID):
             (FD_AMBIGP(string))?(getnonstring(string)):
             (string));
  if (!(VOIDP(notstring)))
    return fd_type_error("string","string_matches",notstring);
  else if (FD_AMBIGP(string)) {
    DO_CHOICES(s,string) {
      convert_offsets(s,start_arg,end_arg,&off,&lim);
      if ((off<0) || (lim<0)) {
        FD_STOP_DO_CHOICES;
        return fd_err(fd_RangeError,"textmatcher",NULL,VOID);}
      match_result = fd_text_matcher(pattern,NULL,CSTRING(s),off,lim,0);
      if (FD_ABORTP(match_result)) {
        FD_STOP_DO_CHOICES;
        return FD_ERROR;}
      else if (EMPTYP(match_result)) {}
      else {
        FD_STOP_DO_CHOICES;
        fd_decref(match_result);
        return FD_TRUE;}}
    return FD_FALSE;}
  else {
    convert_offsets(string,start_arg,end_arg,&off,&lim);
    if ((off<0) || (lim<0))
      return fd_err(fd_RangeError,"textmatcher",NULL,VOID);
    match_result = fd_text_matcher(pattern,NULL,CSTRING(string),off,lim,0);
    if (FD_ABORTP(match_result))
      return match_result;
    else if (EMPTYP(match_result))
      return FD_FALSE;
    else {
      fd_decref(match_result);
      return FD_TRUE;}}
}

static lispval string_ends_with_test(lispval string,lispval pattern,
                                    int off,int lim)
{
  u8_string data = CSTRING(string); int start;
  lispval end = FD_INT(lim);
  if (QCHOICEP(pattern)) pattern = (FD_XQCHOICE(pattern))->qchoiceval;
  if (EMPTYP(pattern)) return FD_FALSE;
  start = fd_text_search(pattern,NULL,data,off,lim,0);
  /* -2 is an error, -1 is not found */
  if (start<-1) return -1;
  while (start>=0) {
    lispval matches = fd_text_matcher(pattern,NULL,data,start,lim,0);
    if (FD_ABORTP(matches))
      return -1;
    else if (matches == end)
      return 1;
    else if (FIXNUMP(matches)) {}
    else {
      DO_CHOICES(match,matches)
        if (match == end) {
          fd_decref(matches);
          FD_STOP_DO_CHOICES;
          return 1;}
      fd_decref(matches);}
    start = fd_text_search(pattern,NULL,data,forward_char(data,start),lim,0);
    if (start<-1) return -1;}
  return 0;
}

static lispval string_ends_with(lispval string,lispval pattern,
                               lispval start_arg,lispval end_arg)
{
  int retval;
  u8_byteoff off, lim;
  lispval notstring;
  if (EMPTYP(string)) return FD_FALSE;
  notstring = ((STRINGP(string))?(VOID):
             (FD_AMBIGP(string))?(getnonstring(string)):
             (string));
  if (!(VOIDP(notstring)))
    return fd_type_error("string","string_matches",notstring);
  convert_offsets(string,start_arg,end_arg,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textmatcher",NULL,VOID);
  else if (FD_AMBIGP(string)) {
    DO_CHOICES(s,string) {
      convert_offsets(s,start_arg,end_arg,&off,&lim);
      if ((off<0) || (lim<0)) {
        FD_STOP_DO_CHOICES;
        return fd_err(fd_RangeError,"textmatcher",NULL,VOID);}
      retval = string_ends_with_test(s,pattern,off,lim);
      if (retval<0) return FD_ERROR;
      else if (retval) {
        FD_STOP_DO_CHOICES; return FD_TRUE;}
      else {}}
    return FD_FALSE;}
  else {
    convert_offsets(string,start_arg,end_arg,&off,&lim);
    if ((off<0) || (lim<0))
      return fd_err(fd_RangeError,"textmatcher",NULL,VOID);
    if (string_ends_with_test(string,pattern,off,lim))
      return FD_TRUE;
    else return FD_FALSE;}
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
    lispval sym = FD_CAR(xtract);
    if ((sym == FDSYM_STAR) || (sym == FDSYM_PLUS) || (sym == FDSYM_OPT)) {
      lispval elts = FD_CDR(xtract);
      if (NILP(elts)) {}
      else {
        FD_DOLIST(elt,elts) {
          int retval = framify(f,out,elt);
          if (retval<0) return retval;}}}
    else if (sym == FDSYM_LABEL) {
      lispval slotid = fd_get_arg(xtract,1);
      lispval content = fd_get_arg(xtract,2);
      if (VOIDP(content)) {
        fd_seterr(fd_BadExtractData,"framify",NULL,xtract);
        return -1;}
      else if (!((SYMBOLP(slotid)) || (OIDP(slotid)))) {
        fd_seterr(fd_BadExtractData,"framify",NULL,xtract);
        return -1;}
      else {
        lispval parser = fd_get_arg(xtract,3);
        struct U8_OUTPUT _out; int retval;
        U8_INIT_OUTPUT(&_out,128);
        retval = framify(f,&_out,content);
        if (retval<0) return -1;
        else if (out)
          u8_putn(out,_out.u8_outbuf,_out.u8_write-_out.u8_outbuf);
        if (VOIDP(parser)) {
          lispval stringval = fd_stream2string(&_out);
          fd_add(f,slotid,stringval);
          fd_decref(stringval);}
        else if (FD_APPLICABLEP(parser)) {
          lispval stringval = fd_stream2string(&_out);
          lispval parsed_val = fd_finish_call(fd_dapply(parser,1,&stringval));
          if (!(FD_ABORTP(parsed_val))) fd_add(f,slotid,parsed_val);
          fd_decref(parsed_val);
          fd_decref(stringval);
          if (FD_ABORTP(parsed_val)) return -1;}
        else if (FD_TRUEP(parser)) {
          lispval parsed_val = fd_parse(_out.u8_outbuf);
          fd_add(f,slotid,parsed_val);
          fd_decref(parsed_val);
          u8_free(_out.u8_outbuf);}
        else {
          lispval stringval = fd_stream2string(&_out);
          fd_add(f,slotid,stringval);
          fd_decref(stringval);}
        return 1;}}
    else if (sym == subst_symbol) {
      lispval content = fd_get_arg(xtract,2);
      if (VOIDP(content)) {
        fd_seterr(fd_BadExtractData,"framify",NULL,xtract);
        return -1;}
      else if (framify(f,out,content)<0) return -1;}
    else {
      fd_seterr(fd_BadExtractData,"framify",NULL,xtract);
      return -1;}}
  else {
    fd_seterr(fd_BadExtractData,"framify",NULL,xtract);
    return -1;}
  return 1;
}

static lispval text2frame(lispval pattern,lispval string,
                          lispval offset,lispval limit)
{
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"text2frame",NULL,VOID);
  else {
    lispval extract_results=
      fd_text_extract(pattern,NULL,CSTRING(string),off,lim,0);
    if (FD_ABORTP(extract_results)) return extract_results;
    else {
      lispval frame_results = EMPTY;
      DO_CHOICES(extraction,extract_results) {
        if (fd_getint(FD_CAR(extraction)) == lim) {
          lispval frame = fd_empty_slotmap();
          if (framify(frame,NULL,FD_CDR(extraction))<0) {
            fd_decref(frame);
            fd_decref(frame_results);
            fd_decref(extract_results);
            return FD_ERROR;}
          CHOICE_ADD(frame_results,frame);}}
      fd_decref(extract_results);
      return frame_results;}}
}

static lispval text2frames(lispval pattern,lispval string,
                           lispval offset,lispval limit)
{
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"text2frames",NULL,VOID);
  else {
    lispval results = EMPTY;
    u8_string data = CSTRING(string);
    int start = fd_text_search(pattern,NULL,data,off,lim,0);
    while (start>=0) {
      lispval extractions = fd_text_extract
        (pattern,NULL,CSTRING(string),start,lim,0);
      lispval longest = EMPTY;
      int max = -1;
      if (FD_ABORTP(extractions)) {
        fd_decref(results);
        return extractions;}
      else if ((CHOICEP(extractions)) || (PRECHOICEP(extractions))) {
        DO_CHOICES(extraction,extractions) {
          int xlen = fd_getint(FD_CAR(extraction));
          if (xlen == max) {
            lispval cdr = FD_CDR(extraction);
            fd_incref(cdr); CHOICE_ADD(longest,cdr);}
          else if (xlen>max) {
            fd_decref(longest); longest = fd_incref(FD_CDR(extraction));
            max = xlen;}
          else {}}}
      else if (EMPTYP(extractions)) {}
      else {
        max = fd_getint(FD_CAR(extractions));
        longest = fd_incref(FD_CDR(extractions));}
      /* Should we signal an internal error here if longest is empty, 
         since search stopped at start, but we don't have a match? */
      {
        DO_CHOICES(extraction,longest) {
          lispval f = fd_empty_slotmap();
          framify(f,NULL,extraction);
          CHOICE_ADD(results,f);}}
      fd_decref(longest);
      fd_decref(extractions);
      if (max>start)
        start = fd_text_search(pattern,NULL,data,max,lim,0);
      else if (max == lim)
        return results;
      else start = fd_text_search
             (pattern,NULL,data,forward_char(data,max),lim,0);}
    if (start== -2) {
      fd_decref(results);
      return FD_ERROR;}
    else return results;}
}

/* Slicing */

static int interpret_keep_arg(lispval keep_arg)
{
  if (FALSEP(keep_arg)) return 0;
  else if (FD_TRUEP(keep_arg)) return 1;
  else if (FD_INTP(keep_arg))
    return FIX2INT(keep_arg);
  else if (FD_EQ(keep_arg,FDSYM_SUFFIX))
    return 1;
  else if (FD_EQ(keep_arg,FDSYM_PREFIX))
    return -1;
  else if (FD_EQ(keep_arg,FDSYM_SEP))
    return 2;
  else return 0;
}

static lispval textslice(lispval string,lispval sep,lispval keep_arg,
                        lispval offset,lispval limit)
{
  u8_byteoff start, len;
  convert_offsets(string,offset,limit,&start,&len);
  if ((start<0) || (len<0))
    return fd_err(fd_RangeError,"textslice",NULL,VOID);
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
    u8_byteoff scan = fd_text_search(sep,NULL,data,start,len,0);
    while ((scan>=0) && (scan<len)) {
      lispval match_result=
        fd_text_matcher(sep,NULL,data,scan,len,0);
      lispval sepstring = VOID, substring = VOID, newpair;
      int end = -1;
      if (FD_ABORTP(match_result))
        return match_result;
      else if (FIXNUMP(match_result)) {
        int point = FIX2INT(match_result);
        if (point>end) end = point;}
      else {
        /* Figure out how long the sep is, taking the longest result. */
        DO_CHOICES(match,match_result) {
          int point = fd_getint(match); if (point>end) end = point;}
        fd_decref(match_result);}
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
          substring = fd_substring(data+start,data+scan);
        else if (keep == 2) {
          sepstring = fd_substring(data+scan,data+end);
          substring = fd_substring(data+start,data+scan);}
        else if (keep>0)
          substring = fd_substring(data+start,data+end);
        else substring = fd_substring(data+start,data+scan);
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
        scan = fd_text_search(sep,NULL,data,forward_char(data,scan),len,0);
      else scan = fd_text_search(sep,NULL,data,end,len,0);
      /* Push it onto the list. */
      if (!(VOIDP(substring))) {
        newpair = fd_conspair(substring,NIL);
        *tail = newpair; tail = &(FD_CDR(newpair));}
      /* Push the separator if you're keeping it */
      if (!(VOIDP(sepstring))) {
        newpair = fd_conspair(sepstring,NIL);
        *tail = newpair; tail = &(FD_CDR(newpair));}}
    /* scan== -2 indicates a real error, not just a failed search. */
    if (scan== -2) {
      fd_decref(slices);
      return FD_ERROR;}
    else if (start<len) {
      /* If you ran out of separators, just add the tail end to the list. */
      lispval substring = fd_substring(data+start,data+len);
      lispval newpair = fd_conspair(substring,NIL);
      *tail = newpair; tail = &(FD_CDR(newpair));}
    return slices;
  }
}

/* Word has-suffix/prefix */

static lispval has_word_suffix(lispval string,lispval suffix,lispval strictarg)
{
  int strict = (FD_TRUEP(strictarg));
  u8_string string_data = FD_STRING_DATA(string);
  u8_string suffix_data = FD_STRING_DATA(suffix);
  int string_len = FD_STRING_LENGTH(string);
  int suffix_len = FD_STRING_LENGTH(suffix);
  if (suffix_len>string_len) return FD_FALSE;
  else if (suffix_len == string_len)
    if (strict) return FD_FALSE;
    else if (strncmp(string_data,suffix_data,suffix_len) == 0)
      return FD_TRUE;
    else return FD_FALSE;
  else {
    u8_string string_data = FD_STRING_DATA(string);
    u8_string suffix_data = FD_STRING_DATA(suffix);
    if ((strncmp(string_data+(string_len-suffix_len),
                 suffix_data,
                 suffix_len) == 0) &&
        (string_data[(string_len-suffix_len)-1]==' '))
        return FD_TRUE;
    else return FD_FALSE;}
}

static lispval has_word_prefix(lispval string,lispval prefix,lispval strictarg)
{
  int strict = (FD_TRUEP(strictarg));
  u8_string string_data = FD_STRING_DATA(string);
  u8_string prefix_data = FD_STRING_DATA(prefix);
  int string_len = FD_STRING_LENGTH(string);
  int prefix_len = FD_STRING_LENGTH(prefix);
  if (prefix_len>string_len) return FD_FALSE;
  else if (prefix_len == string_len)
    if (strict) return FD_FALSE;
    else if (strncmp(string_data,prefix_data,prefix_len) == 0)
      return FD_TRUE;
    else return FD_FALSE;
  else {
    if ((strncmp(string_data,prefix_data,prefix_len) == 0)  &&
        (string_data[prefix_len]==' '))
      return FD_TRUE;
    else return FD_FALSE;}
}

static lispval firstword_prim(lispval string,lispval sep)
{
  u8_string string_data = CSTRING(string);
  if (STRINGP(sep)) {
    u8_string end = strstr(string_data,CSTRING(sep));
    if (end) return fd_substring(string_data,end);
    else return fd_incref(string);}
  else if ((VOIDP(sep))||(FALSEP(sep))||(FD_TRUEP(sep)))  {
    const u8_byte *scan = (u8_byte *)string_data, *last = scan;
    int c = u8_sgetc(&scan); while ((c>0)&&(!(u8_isspace(c)))) {
      last = scan; c = u8_sgetc(&scan);}
    return fd_substring(string_data,last);}
  else {
    int search = fd_text_search(sep,NULL,string_data,0,STRLEN(string),0);
    if (search<0) return fd_incref(string);
    else return fd_substring(string_data,string_data+search);}
}

static int match_end(lispval sep,u8_string data,int off,int lim);
static lispval lastword_prim(lispval string,lispval sep)
{
  u8_string string_data = CSTRING(string);
  if (STRINGP(sep)) {
    u8_string end = string_data, scan = strstr(string_data,CSTRING(sep));
    if (!(scan)) return fd_incref(string);
    else while (scan) {
        end = scan+STRLEN(sep);
        scan = strstr(scan,CSTRING(sep));}
    return fd_substring(end+STRLEN(string),NULL);}
  else if ((VOIDP(sep))||(FALSEP(sep))||(FD_TRUEP(sep)))  {
    const u8_byte *scan = (u8_byte *)string_data, *last = scan;
    int c = u8_sgetc(&scan); while (c>0) {
      if (u8_isspace(c)) {
        u8_string word = scan; c = u8_sgetc(&scan);
        while ((c>0)&&(u8_isspace(c))) {word = scan; c = u8_sgetc(&scan);}
        if (c>0) last = word;}
      else c = u8_sgetc(&scan);}
    return fd_substring(last,NULL);}
  else {
    int lim = STRLEN(string);
    int end = 0, search = fd_text_search(sep,NULL,string_data,0,lim,0);
    if (search<0) return fd_incref(string);
    else {
      while (search>=0) {
        end = match_end(sep,string_data,search,lim);
        search = fd_text_search(sep,NULL,string_data,end,lim,0);}
      return fd_substring(string_data+end,NULL);}}
}

static int match_end(lispval sep,u8_string data,int off,int lim)
{
  lispval matches = fd_text_matcher(sep,NULL,data,off,lim,FD_MATCH_BE_GREEDY);
  if (FD_ABORTP(matches))
    return -1;
  else if (EMPTYP(matches))
    return forward_char(data,off);
  else if (FD_UINTP(matches))
    return FIX2INT(matches);
  else if (FIXNUMP(matches))
    return forward_char(data,off);
  else {
    int max = forward_char(data,off);
    DO_CHOICES(match,matches) {
      int matchlen = ((FD_UINTP(match))?(FIX2INT(match)):(-1));
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
  if (FD_TRUEP(lexicon)) return string;
  else if (TYPEP(lexicon,fd_hashset_type))
    if (fd_hashset_get((fd_hashset)lexicon,string))
      return string;
    else return EMPTY;
  else if (PAIRP(lexicon)) {
    lispval table = FD_CAR(lexicon);
    lispval key = FD_CDR(lexicon);
    lispval value = fd_get(table,string,EMPTY);
    if (EMPTYP(value)) return EMPTY;
    else {
      lispval subvalue = fd_get(value,key,EMPTY);
      if ((EMPTYP(subvalue)) ||
          (VOIDP(subvalue)) ||
          (FALSEP(subvalue))) {
        fd_decref(value);
        return EMPTY;}
      else {
        fd_decref(value); fd_decref(subvalue);
        return string;}}}
  else if (FD_APPLICABLEP(lexicon)) {
    lispval result = fd_finish_call(fd_dapply(lexicon,1,&string));
    if (FD_ABORTP(result)) return FD_ERROR;
    else if (EMPTYP(result)) return EMPTY;
    else if (FALSEP(result)) return EMPTY;
    else {
      fd_decref(result); return string;}}
  else return 0;
}

static lispval apply_suffixrule
  (lispval string,lispval suffix,lispval replacement,
   lispval lexicon)
{
  if (STRLEN(string)>128) return EMPTY;
  else if (has_suffix(string,suffix))
    if (STRINGP(replacement)) {
      struct FD_STRING stack_string; lispval result;
      U8_OUTPUT out; u8_byte buf[256];
      int slen = STRLEN(string), sufflen = STRLEN(suffix);
      int replen = STRLEN(replacement);
      U8_INIT_STATIC_OUTPUT_BUF(out,256,buf);
      u8_putn(&out,CSTRING(string),(slen-sufflen));
      u8_putn(&out,CSTRING(replacement),replen);
      FD_INIT_STATIC_CONS(&stack_string,fd_string_type);
      stack_string.str_bytes = out.u8_outbuf;
      stack_string.str_bytelen = out.u8_write-out.u8_outbuf;
      result = check_string((lispval)&stack_string,lexicon);
      if (FD_ABORTP(result)) return result;
      else if (EMPTYP(result)) return result;
      else return fd_deep_copy((lispval)&stack_string);}
    else if (FD_APPLICABLEP(replacement)) {
      lispval xform = fd_apply(replacement,1,&string);
      if (FD_ABORTP(xform)) return xform;
      else if (STRINGP(xform)) {
        lispval checked = check_string(xform,lexicon);
        if (STRINGP(checked)) return checked;
        else {
          fd_decref(xform); return checked;}}
      else {fd_decref(xform); return EMPTY;}}
    else if (VECTORP(replacement)) {
      lispval rewrites = textrewrite(replacement,string,FD_INT(0),VOID);
      if (FD_ABORTP(rewrites)) return rewrites;
      else if (FD_TRUEP(lexicon)) return rewrites;
      else if (CHOICEP(rewrites)) {
        lispval accepted = EMPTY;
        DO_CHOICES(rewrite,rewrites) {
          if (STRINGP(rewrite)) {
            lispval checked = check_string(rewrite,lexicon);
            if (FD_ABORTP(checked)) {
              fd_decref(rewrites); return checked;}
            fd_incref(checked);
            CHOICE_ADD(accepted,checked);}}
        fd_decref(rewrites);
        return accepted;}
      else if (STRINGP(rewrites))
        return check_string(rewrites,lexicon);
      else { fd_decref(rewrites); return EMPTY;}}
    else return EMPTY;
  else return EMPTY;
}

static lispval apply_morphrule(lispval string,lispval rule,lispval lexicon)
{
  if (VECTORP(rule)) {
    lispval results = EMPTY;
    lispval candidates = textrewrite(rule,string,FD_INT(0),VOID);
    if (FD_ABORTP(candidates)) return candidates;
    else if (EMPTYP(candidates)) {}
    else if (FD_TRUEP(lexicon))
      return candidates;
    else {
      DO_CHOICES(candidate,candidates)
        if (check_string(candidate,lexicon)) {
          fd_incref(candidate);
          CHOICE_ADD(results,candidate);}
      fd_decref(candidates);
      if (!(EMPTYP(results))) return results;}}
  else if (NILP(rule))
    if (check_string(string,lexicon))
      return fd_incref(string);
    else return EMPTY;
  else if (PAIRP(rule)) {
    lispval suffixes = FD_CAR(rule);
    lispval replacement = FD_CDR(rule);
    lispval results = EMPTY;
    DO_CHOICES(suff,suffixes)
      if (STRINGP(suff)) {
        DO_CHOICES(repl,replacement)
          if ((STRINGP(repl)) || (VECTORP(repl)) ||
              (FD_APPLICABLEP(repl))) {
            lispval result = apply_suffixrule(string,suff,repl,lexicon);
            if (FD_ABORTP(result)) {
              fd_decref(results); return result;}
            else {CHOICE_ADD(results,result);}}
          else {
            fd_decref(results);
            return fd_err(fd_BadMorphRule,"morphrule",NULL,rule);}}
      else return fd_err(fd_BadMorphRule,"morphrule",NULL,rule);
    return results;}
  else if (CHOICEP(rule)) {
    lispval results = EMPTY;
    DO_CHOICES(alternate,rule) {
      lispval result = apply_morphrule(string,alternate,lexicon);
      if (FD_ABORTP(result)) {
        fd_decref(results); 
        return result;}
      CHOICE_ADD(results,result);}
    return results;}
  else return fd_type_error(_("morphrule"),"morphrule",rule);
  return EMPTY;
}

static lispval morphrule(lispval string,lispval rules,lispval lexicon)
{
  if (NILP(rules))
    if (check_string(string,lexicon)) return fd_incref(string);
    else return EMPTY;
  else {
    FD_DOLIST(rule,rules) {
      lispval result = apply_morphrule(string,rule,lexicon);
      if (FD_ABORTP(result)) return result;
      if (!(EMPTYP(result))) return result;}
    return EMPTY;}
    
}

/* textclosure prim */

static lispval textclosure_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval pattern_arg = fd_get_arg(expr,1);
  lispval pattern = fd_eval(pattern_arg,env);
  if (VOIDP(pattern_arg))
    return fd_err(fd_SyntaxError,"textclosure_evalfn",NULL,expr);
  else if (FD_ABORTP(pattern)) return pattern;
  else {
    lispval closure = fd_textclosure(pattern,env);
    fd_decref(pattern);
    return closure;}
}

static lispval textclosurep(lispval arg)
{
  if (TYPEP(arg,fd_txclosure_type))
    return FD_TRUE;
  else return FD_FALSE;
}

/* ISSUFFIX/ISPREFIX */

static lispval is_prefix_prim(lispval prefix,lispval string)
{
  int string_len = FD_STRING_LENGTH(string);
  int prefix_len = FD_STRING_LENGTH(prefix);
  if (prefix_len>string_len) return FD_FALSE;
  else {
    u8_string string_data = FD_STRING_DATA(string);
    u8_string prefix_data = FD_STRING_DATA(prefix);
    if (strncmp(string_data,prefix_data,prefix_len) == 0)
      return FD_TRUE;
    else return FD_FALSE;}
}

static lispval is_suffix_prim(lispval suffix,lispval string)
{
  int string_len = FD_STRING_LENGTH(string);
  int suffix_len = FD_STRING_LENGTH(suffix);
  if (suffix_len>string_len) return FD_FALSE;
  else {
    u8_string string_data = FD_STRING_DATA(string);
    u8_string suffix_data = FD_STRING_DATA(suffix);
    if (strncmp(string_data+(string_len-suffix_len),
                suffix_data,
                suffix_len) == 0)
      return FD_TRUE;
    else return FD_FALSE;}
}

/* Reading matches (streaming GATHER) */

static ssize_t get_more_data(u8_input in,size_t lim);

static lispval read_match(lispval port,lispval pat,lispval limit_arg)
{
  ssize_t lim;
  U8_INPUT *in = get_input_port(port);
  if (in == NULL)
    return fd_type_error(_("input port"),"record_reader",port);
  if (VOIDP(limit_arg)) lim = 0;
  else if (FD_UINTP(limit_arg)) lim = FIX2INT(limit_arg);
  else return fd_type_error(_("fixnum"),"record_reader",limit_arg);
  ssize_t buflen = in->u8_inlim-in->u8_read; int eof = 0;
  off_t start = fd_text_search(pat,NULL,in->u8_read,0,buflen,FD_MATCH_BE_GREEDY);
  lispval ends = ((start>=0)?
                 (fd_text_matcher
                  (pat,NULL,in->u8_read,start,buflen,FD_MATCH_BE_GREEDY)):
                 (EMPTY));
  size_t end = getlongmatch(ends);
  fd_decref(ends);
  if ((start>=0)&&(end>start)&&
      ((lim==0)|(end<lim))&&
      ((end<buflen)||(eof))) {
    lispval result = fd_substring(in->u8_read+start,in->u8_read+end);
    in->u8_read = in->u8_read+end;
    return result;}
  else if ((lim)&&(end>lim))
    return FD_EOF;
  else if (in->u8_fillfn) 
    while (!((start>=0)&&(end>start)&&((end<buflen)||(eof)))) {
      int delta = get_more_data(in,lim); size_t new_end;
      if (delta==0) {eof = 1; break;}
      buflen = in->u8_inlim-in->u8_read;
      if (start<0)
        start = fd_text_search
          (pat,NULL,in->u8_read,0,buflen,FD_MATCH_BE_GREEDY);
      if (start<0) continue;
      ends = ((start>=0)?
              (fd_text_matcher
               (pat,NULL,in->u8_read,start,buflen,FD_MATCH_BE_GREEDY)):
              (EMPTY));
      new_end = getlongmatch(ends);
      if ((lim>0)&&(new_end>lim)) eof = 1;
      else end = new_end;
      fd_decref(ends);}
  if ((start>=0)&&(end>start)&&((end<buflen)||(eof))) {
    lispval result = fd_substring(in->u8_read+start,in->u8_read+end);
    in->u8_read = in->u8_read+end;
    return result;}
  else return FD_EOF;
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

static lispval findsep_prim(lispval string,lispval sep,
                           lispval offset,lispval limit,
                           lispval esc)
{
  int c = FD_CHARCODE(sep), e = FD_CHARCODE(esc);
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"findsep_prim",NULL,VOID);
  else if (c>=0x80)
    return fd_type_error("ascii char","findsep_prim",sep);
  else if (e>=0x80)
    return fd_type_error("ascii char","findsep_prim",esc);
  else {
    const u8_byte *str = CSTRING(string), *start = str+off, *limit = str+lim;
    const u8_byte *scan = start, *pos = strchr(scan,c);
    while ((pos) && (scan<limit)) {
      if (pos == start)
        return FD_INT(u8_charoffset(str,(pos-str)));
      else if (*(pos-1) == e) {
        pos++;
        u8_sgetc(&pos);
        scan = pos;
        pos = strchr(scan,c);}
      else return FD_INT(u8_charoffset(str,(pos-str)));}
    return FD_FALSE;}
}

/* Various custom parsing/extraction functions */

static lispval splitsep_prim(lispval string,lispval sep,
                            lispval offset,lispval limit,
                            lispval esc)
{
  int c = FD_CHARCODE(sep), e = FD_CHARCODE(esc);
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"splitsep_prim",NULL,VOID);
  else if (c>=0x80)
    return fd_type_error("ascii char","splitsep_prim",sep);
  else if (e>=0x80)
    return fd_type_error("ascii char","splitsep_prim",esc);
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
          lispval seg = fd_substring(scan,pos);
          lispval elt = fd_conspair(seg,NIL);
          if (VOIDP(head)) head = pair = elt;
          else {
            FD_RPLACD(pair,elt); pair = elt;}
          if (pos) {scan = pos+1; pos = strchr(scan,c);}
          else scan = NULL;}}
    else head = fd_conspair(fd_incref(string),NIL);
    return head;}
}

static char *stdlib_escapes="ntrfab\\";
static char *stdlib_unescaped="\n\t\r\f\a\b\\";

static lispval unslashify_prim(lispval string,lispval offset,lispval limit_arg,
                              lispval dostd)
{
  u8_string sdata = CSTRING(string), start, limit, split1;
  int handle_stdlib = (!(FALSEP(dostd)));
  u8_byteoff off, lim;
  convert_offsets(string,offset,limit_arg,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"unslashify_prim",NULL,VOID);
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
    return fd_stream2string(&out);}
  else if ((off==0) && (lim == STRLEN(string)))
    return fd_incref(string);
  else return fd_substring(start,limit);
}


/* Phonetic prims */

static lispval soundex_prim(lispval string,lispval packetp)
{
  if (FALSEP(packetp))
    return fd_lispstring(fd_soundex(CSTRING(string)));
  else return fd_init_packet(NULL,4,fd_soundex(CSTRING(string)));
}

static lispval metaphone_prim(lispval string,lispval packetp)
{
  if (FALSEP(packetp))
    return fd_lispstring(fd_metaphone(CSTRING(string),0));
  else {
    u8_string dblm = fd_metaphone(CSTRING(string),0);
    return fd_init_packet(NULL,strlen(dblm),dblm);}
}

static lispval metaphone_plus_prim(lispval string,lispval packetp)
{
  if (FALSEP(packetp))
    return fd_lispstring(fd_metaphone(CSTRING(string),1));
  else {
    u8_string dblm = fd_metaphone(CSTRING(string),1);
    return fd_init_packet(NULL,strlen(dblm),dblm);}
}

/* Digest functions */

static lispval md5_prim(lispval input)
{
  unsigned char *digest = NULL;
  if (STRINGP(input))
    digest = u8_md5(CSTRING(input),STRLEN(input),NULL);
  else if (PACKETP(input))
    digest = u8_md5(FD_PACKET_DATA(input),FD_PACKET_LENGTH(input),NULL);
  else {
    struct FD_OUTBUF out = { 0 };
    FD_INIT_BYTE_OUTPUT(&out,1024);
    fd_write_dtype(&out,input);
    digest = u8_md5(out.buffer,out.bufwrite-out.buffer,NULL);
    fd_close_outbuf(&out);}
  if (digest == NULL)
    return FD_ERROR;
  else return fd_init_packet(NULL,16,digest);

}

static lispval sha1_prim(lispval input)
{
  unsigned char *digest = NULL;
  if (STRINGP(input))
    digest = u8_sha1(CSTRING(input),STRLEN(input),NULL);
  else if (PACKETP(input))
    digest = u8_sha1(FD_PACKET_DATA(input),FD_PACKET_LENGTH(input),NULL);
  else {
    struct FD_OUTBUF out = { 0 };
    FD_INIT_BYTE_OUTPUT(&out,1024);
    fd_write_dtype(&out,input);
    digest = u8_sha1(out.buffer,out.bufwrite-out.buffer,NULL);
    fd_close_outbuf(&out);}
  if (digest == NULL)
    return FD_ERROR;
  else return fd_init_packet(NULL,20,digest);

}

static lispval sha256_prim(lispval input)
{
  unsigned char *digest = NULL;
  if (STRINGP(input))
    digest = u8_sha256(CSTRING(input),STRLEN(input),NULL);
  else if (PACKETP(input))
    digest = u8_sha256(FD_PACKET_DATA(input),FD_PACKET_LENGTH(input),NULL);
  else {
    struct FD_OUTBUF out = { 0 };
    FD_INIT_BYTE_OUTPUT(&out,1024);
    fd_write_dtype(&out,input);
    digest = u8_sha256(out.buffer,out.bufwrite-out.buffer,NULL);
    fd_close_outbuf(&out);}
  if (digest == NULL)
    return FD_ERROR;
  else return fd_init_packet(NULL,32,digest);

}

static lispval sha384_prim(lispval input)
{
  unsigned char *digest = NULL;
  if (STRINGP(input))
    digest = u8_sha384(CSTRING(input),STRLEN(input),NULL);
  else if (PACKETP(input))
    digest = u8_sha384(FD_PACKET_DATA(input),FD_PACKET_LENGTH(input),NULL);
  else {
    struct FD_OUTBUF out = { 0 };
    FD_INIT_BYTE_OUTPUT(&out,1024);
    fd_write_dtype(&out,input);
    digest = u8_sha384(out.buffer,out.bufwrite-out.buffer,NULL);
    fd_close_outbuf(&out);}
  if (digest == NULL)
    return FD_ERROR;
  else return fd_init_packet(NULL,48,digest);

}

static lispval sha512_prim(lispval input)
{
  unsigned char *digest = NULL;
  if (STRINGP(input))
    digest = u8_sha512(CSTRING(input),STRLEN(input),NULL);
  else if (PACKETP(input))
    digest = u8_sha512(FD_PACKET_DATA(input),FD_PACKET_LENGTH(input),NULL);
  else {
    struct FD_OUTBUF out = { 0 };
    FD_INIT_BYTE_OUTPUT(&out,1024);
    fd_write_dtype(&out,input);
    digest = u8_sha512(out.buffer,out.bufwrite-out.buffer,NULL);
    fd_close_outbuf(&out);}
  if (digest == NULL)
    return FD_ERROR;
  else return fd_init_packet(NULL,64,digest);

}

static lispval hmac_sha1_prim(lispval key,lispval input)
{
  const unsigned char *data, *keydata, *digest = NULL;
  int data_len, key_len, digest_len, free_key = 0, free_data = 0;
  if (STRINGP(input)) {
    data = CSTRING(input); data_len = STRLEN(input);}
  else if (PACKETP(input)) {
    data = FD_PACKET_DATA(input); data_len = FD_PACKET_LENGTH(input);}
  else {
    struct FD_OUTBUF out = { 0 };
    FD_INIT_BYTE_OUTPUT(&out,1024);
    fd_write_dtype(&out,input);
    data = out.buffer; data_len = out.bufwrite-out.buffer; free_data = 1;}
  if (STRINGP(key)) {
    keydata = CSTRING(key); key_len = STRLEN(key);}
  else if (PACKETP(key)) {
    keydata = FD_PACKET_DATA(key); key_len = FD_PACKET_LENGTH(key);}
  else {
    struct FD_OUTBUF out = { 0 };
    FD_INIT_BYTE_OUTPUT(&out,1024);
    fd_write_dtype(&out,key);
    keydata = out.buffer; key_len = out.bufwrite-out.buffer; free_key = 1;}
  digest = u8_hmac_sha1(keydata,key_len,data,data_len,NULL,&digest_len);
  if (free_data) u8_free(data);
  if (free_key) u8_free(keydata);
  if (digest == NULL)
    return FD_ERROR;
  else return fd_init_packet(NULL,digest_len,digest);
}

static lispval hmac_sha256_prim(lispval key,lispval input)
{
  const unsigned char *data, *keydata, *digest = NULL;
  int data_len, key_len, digest_len, free_key = 0, free_data = 0;
  if (STRINGP(input)) {
    data = CSTRING(input); data_len = STRLEN(input);}
  else if (PACKETP(input)) {
    data = FD_PACKET_DATA(input); data_len = FD_PACKET_LENGTH(input);}
  else {
    struct FD_OUTBUF out = { 0 };
    FD_INIT_BYTE_OUTPUT(&out,1024);
    fd_write_dtype(&out,input);
    data = out.buffer; data_len = out.bufwrite-out.buffer; free_data = 1;}
  if (STRINGP(key)) {
    keydata = CSTRING(key); key_len = STRLEN(key);}
  else if (PACKETP(key)) {
    keydata = FD_PACKET_DATA(key); key_len = FD_PACKET_LENGTH(key);}
  else {
    struct FD_OUTBUF out = { 0 };
    FD_INIT_BYTE_OUTPUT(&out,1024);
    fd_write_dtype(&out,key);
    keydata = out.buffer; key_len = out.bufwrite-out.buffer; free_key = 1;}
  digest = u8_hmac_sha256(keydata,key_len,data,data_len,NULL,&digest_len);
  if (free_data) u8_free(data);
  if (free_key) u8_free(keydata);
  if (digest == NULL)
    return FD_ERROR;
  else return fd_init_packet(NULL,digest_len,digest);
}

static lispval hmac_sha384_prim(lispval key,lispval input)
{
  const unsigned char *data, *keydata, *digest = NULL;
  int data_len, key_len, digest_len, free_key = 0, free_data = 0;
  if (STRINGP(input)) {
    data = CSTRING(input); data_len = STRLEN(input);}
  else if (PACKETP(input)) {
    data = FD_PACKET_DATA(input); data_len = FD_PACKET_LENGTH(input);}
  else {
    struct FD_OUTBUF out = { 0 };
    FD_INIT_BYTE_OUTPUT(&out,1024);
    fd_write_dtype(&out,input);
    data = out.buffer; data_len = out.bufwrite-out.buffer; free_data = 1;}
  if (STRINGP(key)) {
    keydata = CSTRING(key); key_len = STRLEN(key);}
  else if (PACKETP(key)) {
    keydata = FD_PACKET_DATA(key); key_len = FD_PACKET_LENGTH(key);}
  else {
    struct FD_OUTBUF out = { 0 };
    FD_INIT_BYTE_OUTPUT(&out,1024);
    fd_write_dtype(&out,key);
    keydata = out.buffer; key_len = out.bufwrite-out.buffer; free_key = 1;}
  digest = u8_hmac_sha384(keydata,key_len,data,data_len,NULL,&digest_len);
  if (free_data) u8_free(data);
  if (free_key) u8_free(keydata);
  if (digest == NULL)
    return FD_ERROR;
  else return fd_init_packet(NULL,digest_len,digest);
}

static lispval hmac_sha512_prim(lispval key,lispval input)
{
  const unsigned char *data, *keydata, *digest = NULL;
  int data_len, key_len, digest_len, free_key = 0, free_data = 0;
  if (STRINGP(input)) {
    data = CSTRING(input); data_len = STRLEN(input);}
  else if (PACKETP(input)) {
    data = FD_PACKET_DATA(input); data_len = FD_PACKET_LENGTH(input);}
  else {
    struct FD_OUTBUF out = { 0 };
    FD_INIT_BYTE_OUTPUT(&out,1024);
    fd_write_dtype(&out,input);
    data = out.buffer; data_len = out.bufwrite-out.buffer; free_data = 1;}
  if (STRINGP(key)) {
    keydata = CSTRING(key); key_len = STRLEN(key);}
  else if (PACKETP(key)) {
    keydata = FD_PACKET_DATA(key); key_len = FD_PACKET_LENGTH(key);}
  else {
    struct FD_OUTBUF out = { 0 };
    FD_INIT_BYTE_OUTPUT(&out,1024);
    fd_write_dtype(&out,key);
    keydata = out.buffer; key_len = out.bufwrite-out.buffer; free_key = 1;}
  digest = u8_hmac_sha512(keydata,key_len,data,data_len,NULL,&digest_len);
  if (free_data) u8_free(data);
  if (free_key) u8_free(keydata);
  if (digest == NULL)
    return FD_ERROR;
  else return fd_init_packet(NULL,digest_len,digest);
}

/* Match def */

static lispval matchdef_prim(lispval symbol,lispval value)
{
  int retval = fd_matchdef(symbol,value);
  if (retval<0) return FD_ERROR;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

/* Initialization */

static int texttools_init = 0;

void fd_init_texttools()
{
  int fdscheme_version = fd_init_scheme();
  if (texttools_init) return;
  u8_register_source_file(_FILEINFO);
  texttools_init = fdscheme_version;
  texttools_module =
    fd_new_cmodule("TEXTTOOLS",(FD_MODULE_SAFE),fd_init_texttools);
  fd_init_match_c();
  fd_init_phonetic_c();
  fd_idefn(texttools_module,fd_make_cprim1("MD5",md5_prim,1));
  fd_idefn(texttools_module,fd_make_cprim1("SHA1",sha1_prim,1));
  fd_idefn(texttools_module,fd_make_cprim1("SHA256",sha256_prim,1));
  fd_idefn(texttools_module,fd_make_cprim1("SHA384",sha384_prim,1));
  fd_idefn(texttools_module,fd_make_cprim1("SHA512",sha512_prim,1));
  fd_idefn(texttools_module,fd_make_cprim2("HMAC-SHA1",hmac_sha1_prim,2));
  fd_idefn(texttools_module,fd_make_cprim2("HMAC-SHA256",hmac_sha256_prim,2));
  fd_idefn(texttools_module,fd_make_cprim2("HMAC-SHA384",hmac_sha384_prim,2));
  fd_idefn(texttools_module,fd_make_cprim2("HMAC-SHA512",hmac_sha512_prim,2));
  fd_idefn(texttools_module,
           fd_make_cprim2x("SOUNDEX",soundex_prim,1,
                           fd_string_type,VOID,
                           -1,FD_FALSE));
  fd_idefn(texttools_module,
           fd_make_cprim2x("METAPHONE",metaphone_prim,1,
                           fd_string_type,VOID,
                           -1,FD_FALSE));
  fd_idefn(texttools_module,
           fd_make_cprim2x("METAPHONE+",metaphone_plus_prim,1,
                           fd_string_type,VOID,
                           -1,FD_FALSE));
  
  fd_idefn(texttools_module,fd_make_cprim1x("PORTER-STEM",stem_prim,1,
                                            fd_string_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim2x("DISEMVOWEL",disemvowel,1,
                           fd_string_type,VOID,
                           fd_string_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim1x("DEPUNCT",depunct,1,fd_string_type,VOID));
  fd_idefn(texttools_module,
           fd_make_ndprim(fd_make_cprim2("SEGMENT",segment_prim,1)));
  fd_idefn(texttools_module,
           fd_make_cprim2x("GETWORDS",getwords_prim,1,
                           fd_string_type,VOID,-1,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim2x("WORDS->VECTOR",getwordsv_prim,1,
                           fd_string_type,VOID,-1,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim1("LIST->PHRASE",list2phrase_prim,1));
  fd_idefn(texttools_module,
           fd_make_cprim3x("SEQ->PHRASE",seq2phrase_prim,1,
                           -1,VOID,
                           fd_fixnum_type,FD_INT(0),
                           fd_fixnum_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim3x("VECTOR->FRAGS",vector2frags_prim,1,
                           fd_vector_type,VOID,
                           -1,FD_INT(2),
                           -1,FD_TRUE));
  fd_idefn(texttools_module,
           fd_make_cprim1x("DECODE-ENTITIES",decode_entities_prim,1,
                           fd_string_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim3x("ENCODE-ENTITIES",encode_entities_prim,1,
                           fd_string_type,VOID,-1,VOID,
                           -1,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim3x("COLUMNIZE",columnize_prim,2,
                           fd_string_type,VOID,
                           -1,VOID,
                           -1,FD_FALSE));
  
  fd_idefn(texttools_module,
           fd_make_cprim3x("HAS-WORD-SUFFIX?",has_word_suffix,2,
                           fd_string_type,VOID,
                           fd_string_type,VOID,
                           -1,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim3x("HAS-WORD-PREFIX?",has_word_prefix,2,
                           fd_string_type,VOID,
                           fd_string_type,VOID,
                           -1,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim2x("FIRSTWORD",firstword_prim,1,
                           fd_string_type,VOID,
                           -1,FD_TRUE));
  fd_idefn(texttools_module,
           fd_make_cprim2x("LASTWORD",lastword_prim,1,
                           fd_string_type,VOID,
                           -1,FD_TRUE));

  fd_idefn(texttools_module,
           fd_make_cprim1x("ISSPACE%",isspace_percentage,1,
                           fd_string_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim1x("ISALPHA%",isalpha_percentage,1,
                           fd_string_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim1x("ISALPHALEN",isalphalen,1,
                           fd_string_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim1x("MARKUP%",ismarkup_percentage,1,
                           fd_string_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim1x("COUNT-WORDS",count_words,1,
                           fd_string_type,VOID));

  fd_idefn(texttools_module,
           fd_make_cprim2x("STRIP-MARKUP",strip_markup,1,
                           fd_string_type,VOID,
                           -1,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim4x("TEXTMATCHER",textmatcher,2,
                           -1,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_INT(0),
                           fd_fixnum_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim4x("TEXTMATCH",textmatch,2,
                           -1,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_INT(0),
                           fd_fixnum_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim4x("TEXTSEARCH",textsearch,2,
                           -1,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_INT(0),
                           fd_fixnum_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim4x("TEXTRACT",textract,2,
                           -1,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_INT(0),
                           fd_fixnum_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim4x("TEXTREWRITE",textrewrite,2,
                           -1,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_INT(0),
                           fd_fixnum_type,VOID));
  fd_idefn(texttools_module,
           fd_make_ndprim(fd_make_cprim2("TEXTFILTER",textfilter,2)));
  fd_idefn(texttools_module,
           fd_make_ndprim
           (fd_make_cprim4x
            ("STRING-MATCHES?",string_matches,2,
             -1,VOID,-1,VOID,
             fd_fixnum_type,FD_INT(0),
             fd_fixnum_type,VOID)));
  fd_idefn(texttools_module,
           fd_make_ndprim
           (fd_make_cprim4x
            ("STRING-CONTAINS?",string_contains,2,
             -1,VOID,-1,VOID,
             fd_fixnum_type,FD_INT(0),
             fd_fixnum_type,VOID)));
  fd_idefn(texttools_module,
           fd_make_ndprim
           (fd_make_cprim4x
            ("STRING-STARTS-WITH?",string_starts_with,2,
             -1,VOID,-1,VOID,
             fd_fixnum_type,FD_INT(0),
             fd_fixnum_type,VOID)));
  fd_idefn(texttools_module,
           fd_make_ndprim
           (fd_make_cprim4x
            ("STRING-ENDS-WITH?",string_ends_with,2,
             -1,VOID,-1,VOID,
             fd_fixnum_type,FD_INT(0),
             fd_fixnum_type,VOID)));
  
  fd_idefn(texttools_module,
           fd_make_cprim4x("TEXT->FRAME",text2frame,2,
                           -1,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_INT(0),
                           fd_fixnum_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim4x("TEXT->FRAMES",text2frames,2,
                           -1,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_INT(0),
                           fd_fixnum_type,VOID));

  fd_idefn(texttools_module,
           fd_make_cprim5x("TEXTSUBST",textsubst,2,
                           fd_string_type,VOID,
                           -1,VOID,-1,VOID,
                           fd_fixnum_type,FD_INT(0),
                           fd_fixnum_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim4x("GATHER",textgather,2,
                           -1,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_INT(0),
                           fd_fixnum_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim4x("GATHER*",textgather_star,2,
                           -1,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_INT(0),
                           fd_fixnum_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim4x("GATHERSUBST",gathersubst,2,
                           -1,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_INT(0),
                           fd_fixnum_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim4x("GATHERSUBST*",gathersubst_star,2,
                           -1,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_INT(0),
                           fd_fixnum_type,VOID));

  fd_idefn(texttools_module,
           fd_make_cprim4x("GATHER->LIST",textgather2list,2,
                           -1,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_INT(0),
                           fd_fixnum_type,VOID));
  fd_defalias(texttools_module,"GATHER->SEQ","GATHER->LIST");

  fd_idefn(texttools_module,
           fd_make_cprim5x("TEXTSLICE",textslice,2,
                           fd_string_type,VOID,-1,VOID,
                           -1,FD_TRUE,
                           fd_fixnum_type,FD_INT(0),
                           fd_fixnum_type,VOID));

  fd_def_evalfn(texttools_module,"TEXTCLOSURE","",textclosure_evalfn);
  fd_idefn(texttools_module,fd_make_cprim1("TEXTCLOSURE?",textclosurep,1));

  fd_idefn(texttools_module,
           fd_make_cprim3x("READ-MATCH",read_match,2,
                           fd_port_type,VOID,
                           -1,VOID,
                           -1,VOID));


  fd_idefn(texttools_module,
           fd_make_cprim2x("MATCHDEF!",matchdef_prim,2,
                           fd_symbol_type,VOID,-1,VOID));


  fd_idefn(texttools_module,
           fd_make_cprim3x("MORPHRULE",morphrule,2,
                           fd_string_type,VOID,
                           -1,VOID,
                           -1,FD_TRUE));

  /* Escaped separator parsing */
  fd_idefn(texttools_module,
           fd_make_cprim5x("FINDSEP",findsep_prim,2,
                           fd_string_type,VOID,
                           fd_character_type,VOID,
                           -1,VOID,-1,VOID,
                           fd_character_type,FD_CODE2CHAR('\\')));
  fd_idefn(texttools_module,
           fd_make_cprim5x("SPLITSEP",splitsep_prim,2,
                           fd_string_type,VOID,
                           fd_character_type,VOID,
                           -1,VOID,-1,VOID,
                           fd_character_type,FD_CODE2CHAR('\\')));
  fd_idefn(texttools_module,
           fd_make_cprim4x("UNSLASHIFY",unslashify_prim,1,
                           fd_string_type,VOID,
                           -1,VOID,-1,VOID,
                           -1,FD_FALSE));

  fd_idefn(texttools_module,
           fd_make_cprim2x("IS-PREFIX?",is_prefix_prim,2,
                           fd_string_type,VOID,
                           fd_string_type,VOID));
  fd_idefn(texttools_module,
           fd_make_cprim2x("IS-SUFFIX?",is_suffix_prim,2,
                           fd_string_type,VOID,
                           fd_string_type,VOID));


  subst_symbol = fd_intern("SUBST");

  u8_threadcheck();

  fd_finish_module(texttools_module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
