/* C Mode */

/* texttools.c
   This is the core texttools file for the FDB library
   Copyright (C) 2005-2007 beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define U8_INLINE_IO 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/sequences.h"
#include "fdb/texttools.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>

fd_exception fd_BadExtractData=_("Bad extract data");
fd_exception fd_BadMorphRule=_("Bad morphrule");

/* Utility functions */

static int egetc(u8_string *s)
{
  if (**s=='\0') return -1;
  else if (**s<0x80)
    if (**s=='&')
      if (strncmp(*s,"&nbsp;",6)==0) {*s=*s+6; return ' ';}
      else {
	u8_byte *end=NULL; int code=u8_parse_entity((*s)+1,&end);
	if (code>0) {
	  *s=end; return code;}
	else {(*s)++; return '&';}}
    else {
      int c=**s; (*s)++; return c;}
  else return u8_sgetc(s);
}

static u8_byteoff _forward_char(u8_byte *s,u8_byteoff i)
{
  u8_byte *next=u8_substring(s+i,1);
  if (next) return next-s; else return i+1;
}

#define forward_char(s,i) \
  ((s[i] == 0) ? (i) : (s[i] >= 0x80) ? (_forward_char(s,i)) : (i+1))

/* Segmenting */

static u8_string skip_whitespace(u8_string s)
{
  if (s==NULL) return NULL;
  else if (*s) {
    u8_byte *scan=s, *last=s; int c=egetc(&scan);
    while ((c>0) && (u8_isspace(c))) {last=scan; c=egetc(&scan);}
    return last;}
  else return NULL;
}

static u8_string skip_nonwhitespace(u8_string s)
{
  if (s==NULL) return NULL;
  else if (*s) {
    u8_byte *scan=s, *last=s; int c=egetc(&scan);
    while ((c>0) && (!(u8_isspace(c)))) {last=scan; c=egetc(&scan);}
    return last;}
  else return NULL;
}

static fdtype whitespace_segment(u8_string s)
{
  fdtype result=FD_EMPTY_LIST, *lastp=&result;
  u8_byte *start=skip_whitespace(s), *end=skip_nonwhitespace(start);
  while (start) {
    fdtype newcons=
      fd_init_pair(NULL,fd_extract_string(NULL,start,end),FD_EMPTY_LIST);
    *lastp=newcons; lastp=&(FD_CDR(newcons));
    start=skip_whitespace(end); end=skip_nonwhitespace(start);}
  return result;
}

static fdtype dosegment(u8_string string,fdtype separators)
{
  u8_byte *scan=string;
  fdtype result=FD_EMPTY_LIST, *resultp=&result;
  while (scan) {
    fdtype sepstring=FD_EMPTY_CHOICE, pair;
    u8_byte *brk=NULL;
    FD_DO_CHOICES(sep,separators)
      if (FD_STRINGP(sep)) {
	u8_byte *try=strstr(scan,FD_STRDATA(sep));
	if (try==NULL) {}
	else if ((brk==NULL) || (try<brk)) {
	  sepstring=sep; brk=try;}}
      else return fd_type_error(_("string"),"dosegment",sep);
    if (brk==NULL) {
      pair=fd_init_pair(NULL,fdtype_string(scan),FD_EMPTY_LIST);
      *resultp=pair;
      return result;}
    pair=fd_init_pair(NULL,fd_extract_string(NULL,scan,brk),FD_EMPTY_LIST);
    *resultp=pair;
    resultp=&(((struct FD_PAIR *)pair)->cdr);
    scan=brk+FD_STRLEN(sepstring);}
}

static fdtype segment_prim(fdtype inputs,fdtype separators)
{
  if (FD_EMPTY_CHOICEP(inputs)) return FD_EMPTY_CHOICE;
  else if (FD_CHOICEP(inputs)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(input,inputs) {
      fdtype result=segment_prim(input,separators);
      if (FD_ABORTP(result)) {
	fd_decref(results); return result;}
      FD_ADD_TO_CHOICE(results,result);}
    return results;}
  else if (FD_STRINGP(inputs)) 
    if (FD_VOIDP(separators))
      return whitespace_segment(FD_STRDATA(inputs));
    else return dosegment(FD_STRDATA(inputs),separators);
  else return fd_type_error(_("string"),"dosegment",inputs);
}

static fdtype decode_entities_prim(fdtype input)
{
  if (strchr(FD_STRDATA(input),'&')) {
    struct U8_OUTPUT out; u8_string scan=FD_STRDATA(input); int c=egetc(&scan);
    U8_INIT_OUTPUT(&out,FD_STRLEN(input));
    while (c>=0) {
      u8_putc(&out,c); c=egetc(&scan);}
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
  else return fd_incref(input);
}

/* Breaking up strings into words */

static u8_string skip_word(u8_string start)
{
  if (start==NULL) return NULL;
  else if (*start) {
    u8_string last=start, scan=start; int c=egetc(&scan);
    if (u8_isalnum(c)) while (c>0)
      if (u8_isalnum(c)) {last=scan; c=egetc(&scan);}
      else if (u8_isspace(c)) return last;
      else {
	c=egetc(&scan);
	if (!(u8_isalnum(c))) return last;
	else last=scan;}
    else if (u8_ispunct(c)) while (c>0)
      if (u8_ispunct(c)) {
	last=scan; c=egetc(&scan);}
      else return last;
    else return last;
    return last;}
  else return NULL;
}

static u8_string skip_notword(u8_string start)
{
  if (start==NULL) return NULL;
  else if (*start) {
    u8_string last=start, scan=start; int c=egetc(&scan);
    while (c>=0)
      if (u8_isalnum(c)) return last;
      else {
	if (c=='<') while ((c>=0) && (c!='>')) c=egetc(&scan);
	last=scan; c=egetc(&scan);}
    return last;}
  else return NULL;
}

static u8_string skip_punct(u8_string start)
{
  if (start==NULL) return NULL;
  else if (*start=='<') {
    u8_string scan=start; int c=egetc(&scan);
    while ((c>0) && (c != '>')) c=egetc(&scan);
    return scan;}
  else if (*start) {
    u8_string last=start, scan=start; int c=egetc(&scan);
    while (c>0)
      if (c=='<') return last;
      else if (!(u8_ispunct(c))) return last;
      else {last=scan; c=egetc(&scan);}
    if (c<=0) return NULL;
    else return last;}
  else return NULL;
}

static fdtype getwords(u8_string string)
{
  fdtype result=FD_EMPTY_LIST, *lastp=&result;
  u8_string start=skip_notword(string);
  while (start) {
    fdtype newcons;
    u8_string end=skip_word(start);
    if ((end) && (start<end))
      newcons=fd_init_pair
	(NULL,fd_extract_string(NULL,start,end),FD_EMPTY_LIST);
    else if (*start=='\0') return result;
    else newcons=fd_init_pair(NULL,fdtype_string(start),FD_EMPTY_LIST);
    *lastp=newcons; lastp=&(FD_CDR(newcons));
    start=skip_notword(end);}
  return result;
}

static fdtype getwordsv(u8_string string)
{
  int n=0, max=8;
  fdtype *wordsv=u8_alloc_n(max,fdtype);
  u8_string start=skip_notword(string);
  while (start) {
    u8_string end=skip_word(start);
    if (n>=max) {
      wordsv=u8_realloc_n(wordsv,max*2,fdtype);
      max=max*2;}
    if ((end) && (start<end))
      wordsv[n++]=fd_extract_string(NULL,start,end);
    else if (*start=='\0') break;
    else wordsv[n++]=fdtype_string(start);
    start=skip_notword(end);}
  return fd_init_vector(NULL,n,wordsv);
}

static fdtype getwordspunct(u8_string string)
{
  fdtype result=FD_EMPTY_LIST, *lastp=&result;
  u8_string start=skip_whitespace(string);
  while (start) {
    fdtype newcons; u8_string scan=start, end;
    int c=egetc(&scan);
    if (u8_ispunct(c)) end=skip_punct(start);
    else end=skip_word(start);
    if ((end) && (start<end))
      newcons=fd_init_pair
	(NULL,fd_extract_string(NULL,start,end),FD_EMPTY_LIST);
    else if (*start=='\0') return result;
    else newcons=fd_init_pair(NULL,fdtype_string(start),FD_EMPTY_LIST);
    *lastp=newcons; lastp=&(FD_CDR(newcons));
    start=skip_whitespace(end);}
  return result;
}

static fdtype getwordspunctv(u8_string string)
{
  int n=0, max=8;
  fdtype *wordsv=u8_alloc_n(max,fdtype);
  u8_string start=skip_whitespace(string);
  while (start) {
    u8_string scan=start, end;
    int c=egetc(&scan);
    if (c<0) break;
    else if (n>=max) {
      wordsv=u8_realloc_n(wordsv,max*2,fdtype);
      max=max*2;}
    if (u8_ispunct(c)) end=skip_punct(start);
    else end=skip_word(start);
    if ((end) && (start<end))
      wordsv[n++]=fd_extract_string(NULL,start,end);
    else if (*start=='\0') break;
    else wordsv[n++]=fdtype_string(start);
    start=skip_whitespace(end);}
  return fd_init_vector(NULL,n,wordsv);
}

static fdtype getwords_prim(fdtype arg,fdtype punctflag)
{
  int keep_punct=((!(FD_VOIDP(punctflag))) && (FD_TRUEP(punctflag)));
  if (FD_EMPTY_CHOICEP(arg)) return FD_EMPTY_CHOICE;
  else if (FD_CHOICEP(arg)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(input,arg)
      if (FD_STRINGP(input)) {
	fdtype result=((keep_punct) ? (getwordspunct(FD_STRDATA(input))) :
		       (getwords(FD_STRDATA(input))));
	if (FD_ABORTP(result)) {
	  fd_decref(results); return result;}
	FD_ADD_TO_CHOICE(results,result);}
      else {
	fd_decref(results);
	return fd_type_error(_("string"),"getwords_prim",arg);}
    return results;}
  else if (FD_STRINGP(arg))
    if (keep_punct)
      return getwordspunct(FD_STRDATA(arg));
    else return getwords(FD_STRDATA(arg));
  else return fd_type_error(_("string"),"getwords_prim",arg);
}

static fdtype getwordsv_prim(fdtype arg,fdtype punctflag)
{
  int keep_punct=((!(FD_VOIDP(punctflag))) && (FD_TRUEP(punctflag)));
  if (FD_EMPTY_CHOICEP(arg)) return FD_EMPTY_CHOICE;
  else if (FD_CHOICEP(arg)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(input,arg)
      if (FD_STRINGP(input)) {
	fdtype result=((keep_punct) ? (getwordspunctv(FD_STRDATA(input))) :
			(getwordsv(FD_STRDATA(input))));
	if (FD_ABORTP(result)) {
	  fd_decref(results); return result;}
	FD_ADD_TO_CHOICE(results,result);}
      else {
	fd_decref(results);
	return fd_type_error(_("string"),"getwordsv_prim",arg);}
    return results;}
  else if (FD_STRINGP(arg))
    if (keep_punct)
      return getwordspunctv(FD_STRDATA(arg));
    else return getwordsv(FD_STRDATA(arg));
  else return fd_type_error(_("string"),"getwordsv_prim",arg);
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
static fdtype vector2frags_prim(fdtype vec,fdtype window)
{
  int i=0, n=FD_VECTOR_LENGTH(vec), span=FD_FIX2INT(window);
  fdtype *data=FD_VECTOR_DATA(vec), results=FD_EMPTY_CHOICE;
  if (span<=0)
    return fd_type_error(_("natural number"),"vector2frags",window);
  /* Compute prefix fragments */
  i=span-1; while ((i>=0) && (i<n)) {
    fdtype frag=fd_init_pair(NULL,fd_incref(data[i]),FD_EMPTY_LIST);
    int j=i-1; while (j>=0) {
      frag=fd_init_pair(NULL,fd_incref(data[j]),frag); j--;}
    frag=fd_init_pair(NULL,FD_FALSE,frag);
    FD_ADD_TO_CHOICE(results,frag); i--;}
  /* Compute suffix fragments
     We're a little clever here, because we can use the same sublist
     repeatedly.  */
  {
    fdtype frag=fd_init_pair(NULL,FD_FALSE,FD_EMPTY_LIST);
    int stopat=n-span; if (stopat<0) stopat=0;
    i=n-1; while (i>=stopat) {
      frag=fd_init_pair(NULL,fd_incref(data[i]),frag);
      FD_ADD_TO_CHOICE(results,fd_incref(frag));
      i--;}
    fd_decref(frag);}
  /* Now compute internal spans */
  i=0; while (i<n) {
    fdtype frag=FD_EMPTY_LIST;
    int j=i+span-1; while ((j<n) && (j>=i)) {
      frag=fd_init_pair(NULL,fd_incref(data[j]),frag);
      FD_ADD_TO_CHOICE(results,fd_incref(frag));
      j--;}
    fd_decref(frag);
    i++;}
  return results;
}

static fdtype list2phrase_prim(fdtype arg)
{
  int dospace=0; struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  {FD_DOLIST(word,arg) {
    if (dospace) {u8_putc(&out,' ');} else dospace=1;
    if (FD_STRINGP(word)) u8_puts(&out,FD_STRING_DATA(word));
    else u8_printf(&out,"%q",word);}}
  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
}

static fdtype seq2phrase_ndhelper
  (u8_string base,fdtype seq,int start,int end,int dospace);

static fdtype seq2phrase_prim(fdtype arg,fdtype start_arg,fdtype end_arg)
{
  if (FD_EXPECT_FALSE(!(FD_SEQUENCEP(arg))))
    return fd_type_error("sequence","seq2phrase_prim",arg);
  else {
    int dospace=0, start=FD_FIX2INT(start_arg), end, len=fd_seq_length(arg);
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
    if (start<0) start=len+start;
    if ((start<0) || (start>len)) {
      char buf[32]; sprintf(buf,"%d",FD_FIX2INT(start_arg));
      return fd_err(fd_RangeError,"seq2phrase_prim",buf,arg);}
    if (!(FD_FIXNUMP(end_arg))) end=len;
    else {
      end=FD_FIX2INT(end_arg);
      if (end<0) end=len+end;
      if ((end<0) || (end>len)) {
	char buf[32]; sprintf(buf,"%d",FD_FIX2INT(end_arg));
	return fd_err(fd_RangeError,"seq2phrase_prim",buf,arg);}}
    while (start<end) {
      fdtype word=fd_seq_elt(arg,start);
      if (FD_CHOICEP(word)) {
	fdtype result=
	  seq2phrase_ndhelper(out.u8_outbuf,arg,start,end,dospace);
	fd_decref(word); u8_free(out.u8_outbuf);
	return fd_simplify_choice(result);}
      if (dospace) {u8_putc(&out,' ');} else dospace=1;
      if (FD_STRINGP(word)) u8_puts(&out,FD_STRING_DATA(word));
      else u8_printf(&out,"%q",word);
      fd_decref(word); start++;}
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
}

static fdtype seq2phrase_ndhelper
  (u8_string base,fdtype seq,int start,int end,int dospace)
{
  if (start==end)
    return fd_init_string(NULL,-1,u8_strdup(base));
  else {
    fdtype elt=fd_seq_elt(seq,start), results=FD_EMPTY_CHOICE;
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
    FD_DO_CHOICES(s,elt) {
      fdtype result;
      if (!(FD_STRINGP(s))) {
	fd_decref(elt); u8_free(out.u8_outbuf);
	fd_decref(results);
	return fd_type_error(_("string"),"seq2phrase_ndhelper",s);}
      out.u8_outptr=out.u8_outbuf;
      u8_puts(&out,base);
      if (dospace) u8_putc(&out,' ');
      u8_puts(&out,FD_STRDATA(s));
      result=seq2phrase_ndhelper(out.u8_outbuf,seq,start+1,end,1);
      if (FD_ABORTP(result)) {
	fd_decref(elt); fd_decref(results);
	u8_free(out.u8_outbuf);
	return result;}
      FD_ADD_TO_CHOICE(results,result);}
    u8_free(out.u8_outbuf);
    return results;}
}

/* String predicates */

static fdtype isspace_percentage(fdtype string)
{
  u8_string scan=FD_STRDATA(string);
  if (*scan=='\0') return FD_INT2DTYPE(0);
  else {
    int non_space=0, space=0, c;
    while ((c=egetc(&scan))>=0)
      if (u8_isspace(c)) space++;
      else non_space++;
    return FD_INT2DTYPE((space*100)/(space+non_space));}
}

static fdtype isalpha_percentage(fdtype string)
{
  u8_string scan=FD_STRDATA(string);
  if (*scan=='\0') return FD_INT2DTYPE(0);
  else {
    int non_alpha=0, alpha=0, c;
    while ((c=egetc(&scan))>0)
      if (u8_isalpha(c)) alpha++;
      else non_alpha++;
    return FD_INT2DTYPE((alpha*100)/(alpha+non_alpha));}
}

static fdtype count_words(fdtype string)
{
  u8_string scan=FD_STRDATA(string);
  int c=egetc(&scan), word_count=0;
  while (u8_isspace(c)) c=egetc(&scan);
  if (c<0) return FD_INT2DTYPE(0);
  else while (c>0) {
    while ((c>0) && (!(u8_isspace(c)))) c=egetc(&scan);
    word_count++;
    while ((c>0) && (u8_isspace(c))) c=egetc(&scan);}
  return FD_INT2DTYPE(word_count);
}

static fdtype ismarkup_percentage(fdtype string)
{
  u8_string scan=FD_STRDATA(string);
  if (*scan=='\0') return FD_INT2DTYPE(0);
  else {
    int content=0, markup=0, c=egetc(&scan);
    while (c>0)
      if (c=='<') {
	if (strncmp(scan,"!--",3)==0) {
	  u8_string end=strstr(scan,"-->");
	  if (end) scan=end+3; else break;}
	else while ((c>0) && (c!='>')) {
	  markup++; c=egetc(&scan);}
	if (c>0) {markup++; c=egetc(&scan);}
	else break;}
      else if (u8_isspace(c)) {
	/* Count only one character of whitespace */
	content++; c=egetc(&scan);
	while ((c>0) && (u8_isspace(c))) c=egetc(&scan);}
      else {content++; c=egetc(&scan);}
    if ((content+markup==0)) return 0;
    else return FD_INT2DTYPE((markup*100)/(content+markup));}
}

/* Stemming */

FD_EXPORT u8_byte *fd_stem_english_word(u8_byte *original);

static fdtype stem_prim(fdtype arg)
{
  u8_byte *stemmed=fd_stem_english_word(FD_STRDATA(arg));
  return fd_init_string(NULL,-1,stemmed);
}

/* Disemvoweling */

static u8_string default_vowels="aeiouy";

static int all_asciip(u8_string s)
{
  int c=u8_sgetc(&s);
  while (c>0)
    if (c>=0x80) return 0;
    else c=u8_sgetc(&s);
  return 1;
}

static fdtype disemvowel(fdtype string,fdtype vowels)
{
  struct U8_OUTPUT out; struct U8_INPUT in; 
  U8_INIT_STRING_INPUT(&in,FD_STRLEN(string),FD_STRDATA(string));
  U8_INIT_OUTPUT(&out,FD_STRING_LENGTH(string));
  int c=u8_getc(&in), all_ascii;
  u8_string vowelset;
  if (FD_STRINGP(vowels)) vowelset=FD_STRDATA(vowels);
  else vowelset=default_vowels;
  all_ascii=all_asciip(vowelset);
  while (c>=0) {
    int bc=u8_base_char(c);
    if (bc<0x80) {
      if (strchr(vowelset,bc)==NULL)
	u8_putc(&out,c);}
    else if (all_ascii) {}
    else {
      char buf[16]; U8_OUTPUT tmp;
      U8_INIT_FIXED_OUTPUT(&tmp,16,buf);
      u8_putc(&tmp,bc);
      if (strstr(vowelset,buf)==NULL)
	u8_putc(&out,c);}
    c=u8_getc(&in);}
  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
}

/* Depuncting strings (removing punctuation and whitespace) */

static fdtype depunct(fdtype string)
{
  struct U8_OUTPUT out; struct U8_INPUT in; 
  U8_INIT_STRING_INPUT(&in,FD_STRLEN(string),FD_STRDATA(string));
  U8_INIT_OUTPUT(&out,FD_STRING_LENGTH(string));
  int c=u8_getc(&in);
  while (c>=0) {
    if (!((u8_isspace(c)) || (u8_ispunct(c))))
      u8_putc(&out,c);
    c=u8_getc(&in);}
  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
}

/* Skipping markup */

static fdtype strip_markup(fdtype string,fdtype insert_space_arg)
{
  int c, insert_space=FD_TRUEP(insert_space_arg);
  u8_string start=FD_STRDATA(string), scan=start, last=start;
  if (*start) {
      U8_OUTPUT out; U8_INIT_OUTPUT(&out,FD_STRLEN(string));
      while ((c=egetc(&scan))>0)
	if (c=='<') 
	  if (strncmp(scan,"!--",3)==0) {
	    u8_string end=strstr(scan,"-->");
	    if (end) scan=end+3; else break;}
	  else if (strncmp(scan,"<[CDATA[",8)==0) {
	    u8_string end=strstr(scan,"-->");
	    if (end) scan=end+3; else break;}
	  else {
	    while ((c>0) && (c!='>')) c=egetc(&scan);
	    if (c<=0) break;
	    else start=last=scan;
	    if (insert_space) u8_putc(&out,' ');}
	else u8_putc(&out,c);
      u8_putn(&out,start,last-start);
      return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
  else return fd_incref(string);
}

/* Simple string subst */

static fdtype string_subst_prim(fdtype string,fdtype substring,fdtype with)
{
  if (FD_STRLEN(string)==0) return fd_incref(string);
  else {
    u8_string original=FD_STRDATA(string);
    u8_string search=FD_STRDATA(substring);
    u8_string replace=FD_STRDATA(with);
    int searchlen=FD_STRING_LENGTH(substring);
    u8_string point=strstr(original,search);
    if (point) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,2*FD_STRLEN(string));
      u8_string last=original; while (point) {
	u8_putn(&out,last,point-last); u8_puts(&out,replace);
	last=point+searchlen; point=strstr(last,search);}
      u8_puts(&out,last);
      return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
    else return fd_incref(string);}
}

/* Columnizing */

static fdtype columnize_prim(fdtype string,fdtype cols,fdtype parse)
{
  u8_string scan=FD_STRDATA(string), limit=scan+FD_STRLEN(string), buf;
  int i=0, field=0, n_fields=fd_seq_length(cols), parselen=0;
  fdtype *fields;
  while (i<n_fields) {
    fdtype elt=fd_seq_elt(cols,i);
    if (FD_FIXNUMP(elt)) i++;
    else return fd_type_error(_("column width"),"columnize_prim",elt);}
  if (FD_SEQUENCEP(parse)) parselen=fd_seq_length(parselen);
  fields=u8_alloc_n(n_fields,fdtype);
  buf=u8_malloc(FD_STRLEN(string)+1);
  while (field<n_fields) {
    fdtype parsefn;
    int j=0, width=fd_getint(fd_seq_elt(cols,field));
    u8_string start=scan;
    /* Get the parse function */
    if (field<parselen)
      parsefn=fd_seq_elt(parse,field);
    else parsefn=parse;
    /* Skip over the field */
    while (j<width)
      if (scan>=limit) break;
      else {u8_sgetc(&scan); j++;}
    /* If you're at the end, set the field to a default. */
    if (scan==start)
      if (FD_FALSEP(parsefn))
	fields[field++]=fd_init_string(NULL,0,NULL);
      else fields[field++]=FD_FALSE;
    /* If the parse function is false, make a string. */
    else if (FD_FALSEP(parsefn))
      fields[field++]=fd_extract_string(NULL,start,scan);
    /* If the parse function is #t, use the lisp parser */
    else if (FD_TRUEP(parsefn)) {
      fdtype value;
      strncpy(buf,start,scan-start); buf[scan-start]='\0';
      value=fd_parse(buf);
      if (FD_ABORTP(value)) {
	int k=0; while (k<field) {fd_decref(fields[k]); k++;}
	u8_free(fields); u8_free(buf);
	return value;}
      else fields[field++]=value;}
    /* If the parse function is applicable, make a string
       and apply the parse function. */
    else if (FD_APPLICABLEP(parsefn)) {
      fdtype stringval=fd_extract_string(NULL,start,scan);
      fdtype value=fd_apply(parse,1,&stringval);
      if (field<parselen) fd_decref(parsefn);
      if (FD_ABORTP(value)) {
	int k=0; while (k<field) {fd_decref(fields[k]); k++;}
	u8_free(fields); u8_free(buf); fd_decref(stringval);
	return value;}
      else {
	fd_decref(stringval);
	fields[field++]=value;}}
    else {
      int k=0; while (k<field) {fd_decref(fields[k]); k++;}
      u8_free(fields); u8_free(buf);
      return fd_type_error(_("column parse function"),
			   "columnize_prim",parsefn);}}
  if (FD_FALSEP(parse))
    while (field<n_fields) fields[field++]=fd_init_string(NULL,0,NULL);
  else while (field<n_fields) fields[field++]=FD_FALSE;
  return fd_makeseq(FD_PTR_TYPE(cols),n_fields,fields);
}

/* The Matcher */

static fdtype return_offsets(u8_string s,fdtype results)
{
  fdtype final_results=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(off,results)
    if (FD_FIXNUMP(off)) {
      u8_charoff charoff=u8_charoffset(s,FD_FIX2INT(off));
      FD_ADD_TO_CHOICE(final_results,FD_INT2DTYPE(charoff));}
    else {}
  fd_decref(results);
  return final_results;
}

#define convert_arg(off,string,max) u8_byteoffset(string,off,max)

static void convert_offsets
  (fdtype string,fdtype offset,fdtype limit,u8_byteoff *off,u8_byteoff *lim)
{
  u8_charoff offval=fd_getint(offset), limval;
  if (FD_FIXNUMP(limit)) limval=FD_FIX2INT(limit);
  else limval=FD_STRLEN(string);
  *off=u8_byteoffset(FD_STRDATA(string),offval,limval);
  *lim=u8_byteoffset(FD_STRDATA(string),limval,FD_STRLEN(string));
}

static fdtype textmatcher(fdtype pattern,fdtype string,
			   fdtype offset,fdtype limit)
{
  int off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textmatcher",NULL,FD_VOID);
  else {
    fdtype match_result=fd_text_matcher
      (pattern,NULL,FD_STRDATA(string),off,lim,0);
    if (FD_ABORTP(match_result)) return match_result;
    else return return_offsets(FD_STRDATA(string),match_result);}
}

static fdtype textmatch(fdtype pattern,fdtype string,
			 fdtype offset,fdtype limit)
{
  int off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textmatch",NULL,FD_VOID);
  else {
    int retval=
      fd_text_match(pattern,NULL,FD_STRDATA(string),off,lim,0);
    if (retval<0) return fd_erreify();
    else if (retval) return FD_TRUE;
    else return FD_FALSE;}
}

static fdtype textsearch(fdtype pattern,fdtype string,
			  fdtype offset,fdtype limit)
{
  int off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textsearch",NULL,FD_VOID);
  else {
    int pos=fd_text_search(pattern,NULL,FD_STRDATA(string),off,lim,0);
    if (pos<0)
      if (pos==-2) return fd_erreify();
      else return FD_FALSE;
    else return FD_INT2DTYPE(u8_charoffset(FD_STRDATA(string),pos));}
}

static fdtype textract(fdtype pattern,fdtype string,
			fdtype offset,fdtype limit)
{
  fdtype results=FD_EMPTY_CHOICE;
  int off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textract",NULL,FD_VOID);
  else {
    fdtype extract_results=fd_text_extract
      (pattern,NULL,FD_STRDATA(string),off,lim,0);
    FD_DO_CHOICES(extraction,extract_results) 
      if (FD_ABORTP(extraction)) {
	fd_decref(results); fd_incref(extraction);
	fd_decref(extract_results);
	return extraction;}
      else if (FD_PAIRP(extraction))
	if (fd_getint(FD_CAR(extraction))==lim) {
	  fdtype extract=fd_incref(FD_CDR(extraction));
	  FD_ADD_TO_CHOICE(results,extract);}
	else {}
      else {}
    fd_decref(extract_results);}
  return results;
}

static fdtype textgather(fdtype pattern,fdtype string,
			  fdtype offset,fdtype limit)
{
  fdtype results=FD_EMPTY_CHOICE;
  u8_string data=FD_STRDATA(string);
  int off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textgather",NULL,FD_VOID);
  else {
    int start=fd_text_search(pattern,NULL,data,off,lim,0);
    while (start>=0) {
      fdtype substring, match_result=
	fd_text_matcher(pattern,NULL,FD_STRDATA(string),start,lim,0);
      int end=-1;
      {FD_DO_CHOICES(match,match_result) {
	int point=fd_getint(match); if (point>end) end=point;}}
      fd_decref(match_result);
      if (end<0) return results;
      else if (end>start) {
	substring=fd_extract_string(NULL,data+start,data+end);
	FD_ADD_TO_CHOICE(results,substring);
	start=fd_text_search(pattern,NULL,data,end,lim,0);}
      else if (end==lim)
	return results;
      else start=fd_text_search
	     (pattern,NULL,data,forward_char(data,end),lim,0);}
    if (start==-2) {
      fd_decref(results);
      return fd_erreify();}
    else return results;}
}

static fdtype textgather2list(fdtype pattern,fdtype string,
			       fdtype offset,fdtype limit)
{
  fdtype head=FD_EMPTY_LIST, *tail=&head;
  u8_string data=FD_STRDATA(string);
  int off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textgather",NULL,FD_VOID);
  else {
    int start=fd_text_search(pattern,NULL,data,off,lim,0);
    while (start>=0) {
      fdtype match_result=
	fd_text_matcher(pattern,NULL,FD_STRDATA(string),start,lim,0);
      int end=-1;
      {FD_DO_CHOICES(match,match_result) {
	int point=fd_getint(match); if (point>end) end=point;}}
      fd_decref(match_result);
      if (end<0) return head;
      else if (end>start) {
	fdtype newpair=
	  fd_init_pair(NULL,fd_extract_string(NULL,data+start,data+end),
		       FD_EMPTY_LIST);
	*tail=newpair; tail=&(FD_CDR(newpair));
	start=fd_text_search(pattern,NULL,data,end,lim,0);}
      else if (end==lim)
	return head;
      else start=fd_text_search
	     (pattern,NULL,data,forward_char(data,end),lim,0);}
    if (start==-2) {
      fd_decref(head);
      return fd_erreify();}
    else return head;}
}

/* Text rewrite/subst */

static fdtype star_symbol, plus_symbol, label_symbol, subst_symbol, opt_symbol;
static int dorewrite(u8_output out,fdtype xtract)
{
  if (FD_STRINGP(xtract))
    u8_putn(out,FD_STRDATA(xtract),FD_STRLEN(xtract));
  else if (FD_VECTORP(xtract)) {
    int i=0, len=FD_VECTOR_LENGTH(xtract);
    fdtype *data=FD_VECTOR_DATA(xtract);
    while (i<len) {
      int retval=dorewrite(out,data[i]);
      if (retval<0) return retval; else i++;}}
  else if (FD_PAIRP(xtract)) {
    fdtype sym=FD_CAR(xtract);
    if ((sym==star_symbol) || (sym==plus_symbol) || (sym==opt_symbol)) {
      fdtype elts=FD_CDR(xtract);
      if (FD_EMPTY_LISTP(elts)) {}
      else {
	FD_DOLIST(elt,elts) {
	  int retval=dorewrite(out,elt);
	  if (retval<0) return retval;}}}
    else if ((sym==label_symbol) || (sym==subst_symbol)) {
      fdtype content=fd_get_arg(xtract,2);
      if (FD_VOIDP(content)) {
	fd_seterr(fd_BadExtractData,"dorewrite",NULL,xtract);
	return -1;}
      else if (dorewrite(out,content)<0) return -1;}
    else {
      fd_seterr(fd_BadExtractData,"dorewrite",NULL,xtract);
      return -1;}}
  else {
    fd_seterr(fd_BadExtractData,"dorewrite",NULL,xtract);
    return -1;}
  return 1;
}

static fdtype textrewrite(fdtype pattern,fdtype string,
			   fdtype offset,fdtype limit)
{
  int off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textrewrite",NULL,FD_VOID);
  else if ((lim-off)==0)
    return fdtype_string("");
  else {
    fdtype extract_results=fd_text_extract
      (pattern,NULL,FD_STRDATA(string),off,lim,0);
    if (FD_ABORTP(extract_results)) return extract_results;
    else {
      fdtype subst_results=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(extraction,extract_results)
	if ((fd_getint(FD_CAR(extraction)))==lim) {
	  struct U8_OUTPUT out; fdtype stringval;
	  U8_INIT_OUTPUT(&out,(lim-off)*2);
	  if (dorewrite(&out,FD_CDR(extraction))<0) {
	    fd_decref(subst_results); fd_decref(extract_results);
	    u8_free(out.u8_outbuf); return fd_erreify();}
	  stringval=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
	  FD_ADD_TO_CHOICE(subst_results,stringval);}
      fd_decref(extract_results);
      return subst_results;}}
}

static fdtype textsubst(fdtype string,
			 fdtype pattern,fdtype replace,
			 fdtype offset,fdtype limit)
{
  int off, lim;
  u8_string data=FD_STRDATA(string);
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textsubst",NULL,FD_VOID);
  else if ((lim-off)==0)
    return fdtype_string("");
  else {
    int start=fd_text_search(pattern,NULL,data,off,lim,0), last=off;
    if (start>=0) {
      U8_OUTPUT out; U8_INIT_OUTPUT(&out,2*(lim-off));
      while (start>=0) {
	fdtype match_result=
	  fd_text_matcher(pattern,NULL,FD_STRDATA(string),start,lim,0);
	int end=-1;
	{FD_DO_CHOICES(match,match_result) {
	  int point=fd_getint(match); if (point>end) end=point;}}
	fd_decref(match_result);
	if (end<0) {
	  u8_puts(&out,data+last);
	  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
	else if (end>start) {
	  u8_putn(&out,data+last,start-last);
	  if (FD_STRINGP(replace))
	    u8_puts(&out,FD_STRDATA(replace));
	  else {
	    u8_string stringdata=FD_STRDATA(string);
	    fdtype lisp_lim=FD_INT2DTYPE(u8_charoffset(stringdata,lim));
	    fdtype replace_pat, xtract;
	    if (FD_VOIDP(replace)) replace_pat=pattern; else replace_pat=replace;
	    xtract=fd_text_extract(replace_pat,NULL,stringdata,start,end,0);
	    if (FD_ABORTP(xtract)) {
	      u8_free(out.u8_outbuf);
	      return xtract;}
	    else if ((FD_CHOICEP(xtract)) || (FD_ACHOICEP(xtract))) {
	      fdtype results=FD_EMPTY_CHOICE;
	      FD_DO_CHOICES(xt,xtract) {
		u8_charoff newstart=
		  u8_charoffset(stringdata,(fd_getint(FD_CAR(xt))));
		if (newstart==lim) {
		  fdtype stringval;
		  struct U8_OUTPUT tmpout; U8_INIT_OUTPUT(&tmpout,512);
		  u8_puts(&tmpout,out.u8_outbuf);
		  if (dorewrite(&tmpout,FD_CDR(xt))<0) {
		    u8_free(tmpout.u8_outbuf); u8_free(out.u8_outbuf);
		    fd_decref(results); results=fd_erreify();
		    FD_STOP_DO_CHOICES; break;}
		  stringval=fd_init_string
		    (NULL,tmpout.u8_outptr-tmpout.u8_outbuf,tmpout.u8_outbuf);
		  FD_ADD_TO_CHOICE(results,stringval);}
		else {
		  fdtype remainder=textsubst
		    (string,pattern,replace,FD_INT2DTYPE(newstart),lisp_lim);
		  FD_DO_CHOICES(rem,remainder) {
		    fdtype stringval;
		    struct U8_OUTPUT tmpout; U8_INIT_OUTPUT(&tmpout,512);
		    u8_puts(&tmpout,out.u8_outbuf);
		    if (dorewrite(&tmpout,FD_CDR(xt))<0) {
		      u8_free(tmpout.u8_outbuf); u8_free(out.u8_outbuf);
		      fd_decref(results); results=fd_erreify();
		      FD_STOP_DO_CHOICES; break;}
		    u8_puts(&tmpout,FD_STRDATA(rem));
		    stringval=fd_init_string
		      (NULL,tmpout.u8_outptr-tmpout.u8_outbuf,tmpout.u8_outbuf);
		    FD_ADD_TO_CHOICE(results,stringval);}
		  fd_decref(remainder);}}
	      u8_free(out.u8_outbuf);
	      fd_decref(xtract);
	      return results;}
	    else {
	      if (dorewrite(&out,FD_CDR(xtract))<0) {
		u8_free(out.u8_outbuf); fd_decref(xtract);
		return fd_erreify();}
	      fd_decref(xtract);}}
	  last=end; start=fd_text_search(pattern,NULL,data,last,lim,0);}
	else if (end==lim) break;
	else start=fd_text_search
	       (pattern,NULL,data,forward_char(data,end),lim,0);}
      u8_puts(&out,data+last);
      return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
    else if (start==-2) 
      return fd_erreify();
    else return fd_extract_string(NULL,data+off,data+lim);}
}

/* Handy filtering functions */

static fdtype textfilter(fdtype strings,fdtype pattern)
{
  fdtype results=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(string,strings)
    if (FD_STRINGP(string))
      if (fd_text_match(pattern,NULL,FD_STRDATA(string),0,FD_STRLEN(string),0)) {
	FD_ADD_TO_CHOICE(results,fd_incref(string));}
      else {}
    else {
      fd_decref(results);
      return fd_type_error("string","textfiler",string);}
  return results;
}

/* PICK/REJECT predicates */

/* These are matching functions with the arguments reversed
   to be especially useful as arguments to PICK and REJECT. */

static fdtype string_matches(fdtype string,fdtype pattern,
			     fdtype start_arg,fdtype end_arg)
{
  int off, lim;
  convert_offsets(string,start_arg,end_arg,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textmatcher",NULL,FD_VOID);
  else {
    int retval=fd_text_match(pattern,NULL,FD_STRDATA(string),off,lim,0);
    if (retval<0) return fd_erreify();
    else if (retval) return FD_TRUE;
    else return FD_FALSE;}
}

static fdtype string_contains(fdtype string,fdtype pattern,
			      fdtype start_arg,fdtype end_arg)
{
  int off, lim;
  convert_offsets(string,start_arg,end_arg,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textmatcher",NULL,FD_VOID);
  else {
    int retval=fd_text_search(pattern,NULL,FD_STRDATA(string),off,lim,0);
    if (retval<-1) return fd_erreify();
    else if (retval<0) return FD_FALSE;
    else return FD_TRUE;}
}

static fdtype string_starts_with(fdtype string,fdtype pattern,
				 fdtype start_arg,fdtype end_arg)
{
  int off, lim, retval;
  convert_offsets(string,start_arg,end_arg,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textmatcher",NULL,FD_VOID);
  else {
    fdtype match_result=fd_text_matcher
      (pattern,NULL,FD_STRDATA(string),off,lim,0);
    if (FD_ABORTP(match_result)) return match_result;
    else if (FD_EMPTY_CHOICEP(match_result))
      return FD_FALSE;
    else {
      fd_decref(match_result);
      return FD_TRUE;}}
}

/* text2frame */

static int framify(fdtype f,u8_output out,fdtype xtract)
{
  if (FD_STRINGP(xtract)) {
    if (out) u8_putn(out,FD_STRDATA(xtract),FD_STRLEN(xtract));}
  else if (FD_VECTORP(xtract)) {
    int i=0, len=FD_VECTOR_LENGTH(xtract);
    fdtype *data=FD_VECTOR_DATA(xtract);
    while (i<len) {
      int retval=framify(f,out,data[i]);
      if (retval<0) return retval; else i++;}}
  else if (FD_PAIRP(xtract)) {
    fdtype sym=FD_CAR(xtract);
    if ((sym==star_symbol) || (sym==plus_symbol) || (sym==opt_symbol)) {
      fdtype elts=FD_CDR(xtract);
      if (FD_EMPTY_LISTP(elts)) {}
      else {
	FD_DOLIST(elt,elts) {
	  int retval=framify(f,out,elt);
	  if (retval<0) return retval;}}}
    else if (sym==label_symbol) {
      fdtype slotid=fd_get_arg(xtract,1);
      fdtype content=fd_get_arg(xtract,2);
      if (FD_VOIDP(content)) {
	fd_seterr(fd_BadExtractData,"framify",NULL,xtract);
	return -1;}
      else if (!((FD_SYMBOLP(slotid)) || (FD_OIDP(slotid)))) {
	fd_seterr(fd_BadExtractData,"framify",NULL,xtract);
	return -1;}
      else {
	fdtype parser=fd_get_arg(xtract,3);
	struct U8_OUTPUT _out; int retval;
	U8_INIT_OUTPUT(&_out,32);
	retval=framify(f,&_out,content);
	if (out) u8_putn(out,_out.u8_outbuf,_out.u8_outptr-_out.u8_outbuf);
	if (FD_VOIDP(parser)) {
	  fdtype stringval=fd_init_string(NULL,_out.u8_outptr-_out.u8_outbuf,_out.u8_outbuf);
	  fd_add(f,slotid,stringval);
	  fd_decref(stringval);}
	else if (FD_APPLICABLEP(parser)) {
	  fdtype stringval=fd_init_string(NULL,_out.u8_outptr-_out.u8_outbuf,_out.u8_outbuf);
	  fdtype parsed_val=fd_finish_call(fd_dapply(parser,1,&stringval));
	  fd_add(f,slotid,parsed_val);
	  fd_decref(parsed_val);
	  fd_decref(stringval);}
	else if (FD_TRUEP(parser)) {
	  fdtype parsed_val=fd_parse(_out.u8_outbuf);
	  fd_add(f,slotid,parsed_val);
	  fd_decref(parsed_val); u8_free(_out.u8_outbuf);}
	else {
	  fdtype stringval=fd_init_string(NULL,_out.u8_outptr-_out.u8_outbuf,_out.u8_outbuf);
	  fd_add(f,slotid,stringval);
	  fd_decref(stringval);}
	return 1;}}
    else if (sym==subst_symbol) {
      fdtype content=fd_get_arg(xtract,2);
      if (FD_VOIDP(content)) {
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

static fdtype text2frame(fdtype pattern,fdtype string,
			  fdtype offset,fdtype limit)
{
  int off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textsubst",NULL,FD_VOID);
  else {
    fdtype extract_results=
      fd_text_extract(pattern,NULL,FD_STRDATA(string),off,lim,0);
    if (FD_ABORTP(extract_results)) return extract_results;
    else {
      fdtype frame_results=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(extraction,extract_results)
	if (fd_getint(FD_CAR(extraction))==lim) {
	  fdtype frame=fd_init_slotmap(NULL,0,NULL);
	  if (framify(frame,NULL,FD_CDR(extraction))<0) {
	    fd_decref(frame_results); fd_decref(extract_results);
	    return fd_erreify();}
	  FD_ADD_TO_CHOICE(frame_results,frame);}
      return frame_results;}}
}

static fdtype text2frames(fdtype pattern,fdtype string,
			   fdtype offset,fdtype limit)
{
  int off, lim;
  convert_offsets(string,offset,limit,&off,&lim);
  if ((off<0) || (lim<0))
    return fd_err(fd_RangeError,"textsubst",NULL,FD_VOID);
  else {
    fdtype results=FD_EMPTY_CHOICE;
    u8_string data=FD_STRDATA(string);
    int start=fd_text_search(pattern,NULL,data,off,lim,0);
    while (start>=0) {
      fdtype extractions=fd_text_extract
	(pattern,NULL,FD_STRDATA(string),start,lim,0);
      fdtype longest=FD_EMPTY_CHOICE;
      int max=-1;
      if ((FD_CHOICEP(extractions)) || (FD_ACHOICEP(extractions))) {
	FD_DO_CHOICES(extraction,extractions) {
	  int xlen=fd_getint(FD_CAR(extraction));
	  if (xlen==max) {
	    FD_ADD_TO_CHOICE(longest,fd_incref(FD_CDR(extraction)));}
	  else if (xlen>max) {
	    fd_decref(longest); longest=fd_incref(FD_CDR(extraction));
	    max=xlen;}
	  else {}}}
      else if (FD_EMPTY_CHOICEP(extractions)) {}
      else {
	max=fd_getint(FD_CAR(extractions));
	longest=fd_incref(FD_CDR(extractions));}
      /* Should we signal an internal error here if longest is empty, 
	 since search stopped at start, but we don't have a match? */
      {
	FD_DO_CHOICES(extraction,longest) {
	  fdtype f=fd_init_slotmap(NULL,0,NULL);
	  framify(f,NULL,extraction);
	  FD_ADD_TO_CHOICE(results,f);}}
      fd_decref(longest);
      fd_decref(extractions);
      if (max>start)
	start=fd_text_search(pattern,NULL,data,max,lim,0);
      else if (max==lim)
	return results;
      else start=fd_text_search
	     (pattern,NULL,data,forward_char(data,max),lim,0);}
    if (start==-2) {
      fd_decref(results);
      return fd_erreify();}
    else return results;}
}

/* Slicing */

static fdtype textslice(fdtype string,fdtype prefix,fdtype keep_prefixes)
{
  fdtype slices=FD_EMPTY_LIST, *tail=&slices;
  u8_string data=FD_STRDATA(string);
  int len=FD_STRLEN(string), start=0, keep=FD_TRUEP(keep_prefixes);
  int scan=fd_text_search(prefix,NULL,data,start,len,0);
  /* scan is pointing at a substring matching the prefix,
     start is where we last added a string, and end is the greedy limit
     of the matched prefix. */
  while (scan>=0) {
    fdtype match_result=
      fd_text_matcher(prefix,NULL,FD_STRDATA(string),scan,len,0);
    int end=-1;
    /* Figure out how long the prefix is, taking the longest result. */
    {FD_DO_CHOICES(match,match_result) {
      int point=fd_getint(match); if (point>end) end=point;}}
    fd_decref(match_result);
    /* Add the string up to the prefix if non-empty. */
    if (start<scan) {
      fdtype substring=fd_extract_string(NULL,data+start,data+scan);
      fdtype newpair=fd_init_pair(NULL,substring,FD_EMPTY_LIST);
      *tail=newpair; tail=&(FD_CDR(newpair));}
    /* If we don't seem to have an end or the string is empty, swallow
       a codepoint and move forward. */
    if ((end<0) || (end==scan)) {
      /* You're at the start of an empty but positive match, so just make
	 the first character into a string and start after that. */
      u8_string scanner=data+start; int MAYBE_UNUSED c=u8_sgetc(&scanner);
      fdtype substring=fd_extract_string(NULL,data+start,scanner);
      fdtype newpair=fd_init_pair(NULL,substring,FD_EMPTY_LIST);
      *tail=newpair; tail=&(FD_CDR(newpair));
      start=scanner-data;
      scan=fd_text_search(prefix,NULL,data,start,len,0);}
    else {
      if (keep) start=scan; else start=end;
      scan=fd_text_search(prefix,NULL,data,end,len,0);}}
  if (scan==-2) {
    fd_decref(slices);
    return fd_erreify();}
  else if (start<len) {
    fdtype substring=fdtype_string(data+start);
    fdtype newpair=fd_init_pair(NULL,substring,FD_EMPTY_LIST);
    *tail=newpair; tail=&(FD_CDR(newpair));}
  return slices;
}

/* Word has-suffix/prefix */

static fdtype has_word_suffix(fdtype string,fdtype suffix,fdtype strictarg)
{
  int strict=(FD_TRUEP(strictarg));
  u8_string string_data=FD_STRING_DATA(string);
  u8_string suffix_data=FD_STRING_DATA(suffix);
  int string_len=FD_STRING_LENGTH(string);
  int suffix_len=FD_STRING_LENGTH(suffix);
  if (suffix_len>string_len) return FD_FALSE;
  else if (suffix_len==string_len)
    if (strict) return FD_FALSE;
    else if (strncmp(string_data,suffix_data,suffix_len) == 0)
      return FD_TRUE;
    else return FD_FALSE;
  else {
    u8_string string_data=FD_STRING_DATA(string);
    u8_string suffix_data=FD_STRING_DATA(suffix);
    if ((strncmp(string_data+(string_len-suffix_len),
		 suffix_data,
		 suffix_len) == 0) &&
	(string_data[(string_len-suffix_len)-1]==' '))
	return FD_TRUE;
    else return FD_FALSE;}
}

static fdtype has_word_prefix(fdtype string,fdtype prefix,fdtype strictarg)
{
  int strict=(FD_TRUEP(strictarg));
  u8_string string_data=FD_STRING_DATA(string);
  u8_string prefix_data=FD_STRING_DATA(prefix);
  int string_len=FD_STRING_LENGTH(string);
  int prefix_len=FD_STRING_LENGTH(prefix);
  if (prefix_len>string_len) return FD_FALSE;
  else if (prefix_len==string_len)
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

/* Morphrule */

static int has_suffix(fdtype string,fdtype suffix)
{
  int slen=FD_STRLEN(string), sufflen=FD_STRLEN(suffix);
  if (slen<sufflen) return 0;
  else if (strncmp(FD_STRDATA(string)+(slen-sufflen),
		   FD_STRDATA(suffix),
		   sufflen)==0)
    return 1;
  else return 0;
}

static int check_string(fdtype string,fdtype lexicon)
{
  if (FD_TRUEP(lexicon)) return 1;
  else if (FD_PTR_TYPEP(lexicon,fd_hashset_type))
    return fd_hashset_get((fd_hashset)lexicon,string);
  else if (FD_PAIRP(lexicon)) {
    fdtype table=FD_CAR(lexicon);
    fdtype key=FD_CDR(lexicon);
    fdtype value=fd_get(table,string,FD_EMPTY_CHOICE);
    if (FD_EMPTY_CHOICEP(value)) return 0;
    else {
      fdtype subvalue=fd_get(value,key,FD_EMPTY_CHOICE);
      if ((FD_EMPTY_CHOICEP(subvalue)) ||
	  (FD_VOIDP(subvalue)) ||
	  (FD_FALSEP(subvalue))) {
	fd_decref(value);  return 0;}
      else {
	fd_decref(value); fd_decref(subvalue);
	return 1;}}}
  else if (FD_APPLICABLEP(lexicon)) {
    fdtype result=fd_finish_call(fd_dapply(lexicon,1,&string));
    if (FD_EMPTY_CHOICEP(result)) return 0;
    else if (FD_FALSEP(result)) return 0;
    else {
      fd_decref(result); return 1;}}
  else return 0;
}

static fdtype apply_suffixrule
  (fdtype string,fdtype suffix,fdtype replacement,
   fdtype lexicon)
{
  if (FD_STRLEN(string)>128) return FD_EMPTY_CHOICE;
  else if (FD_STRLEN(replacement)>128)
    return FD_EMPTY_CHOICE;  
  else if (has_suffix(string,suffix)) {
    struct FD_STRING stack_string;
    U8_OUTPUT out; u8_byte buf[256];
    int slen=FD_STRLEN(string), sufflen=FD_STRLEN(suffix);
    int replen=FD_STRLEN(replacement);
    U8_INIT_OUTPUT_BUF(&out,256,buf);
    u8_putn(&out,FD_STRDATA(string),(slen-sufflen));
    u8_putn(&out,FD_STRDATA(replacement),replen);
    FD_INIT_STACK_CONS(&stack_string,fd_string_type);
    stack_string.bytes=out.u8_outbuf;
    stack_string.length=out.u8_outptr-out.u8_outbuf;
    if (check_string((fdtype)&stack_string,lexicon))
      return fd_deep_copy((fdtype)&stack_string);
    else return FD_EMPTY_CHOICE;}
  else return FD_EMPTY_CHOICE;
}

static fdtype apply_morphrule(fdtype string,fdtype rule,fdtype lexicon)
{
  if (FD_VECTORP(rule)) {
    fdtype results=FD_EMPTY_CHOICE;
    fdtype candidates=textrewrite(rule,string,FD_INT2DTYPE(0),FD_VOID);
    if (FD_EMPTY_CHOICEP(candidates)) {}
    else {
      FD_DO_CHOICES(candidate,candidates)
	if (check_string(candidate,lexicon)) {
	  FD_ADD_TO_CHOICE(results,fd_incref(candidate));}
      fd_decref(candidates);
      if (!(FD_EMPTY_CHOICEP(results))) return results;}}
  else if (FD_EMPTY_LISTP(rule))
    if (check_string(string,lexicon))
      return fd_incref(string);
    else return FD_EMPTY_CHOICE;
#if 0
  else if ((FD_PAIRP(rule)) && (FD_PAIRP(FD_CDR(rule)))) {
    FD_DOLIST(subrule,rule) {
      fdtype result=apply_morphrule(string,subrule,lexicon);
      if (FD_EMPTY_CHOICEP(result)) {}
      else return result;}
    return FD_EMPTY_CHOICE;}
#endif
  else if (FD_PAIRP(rule)) {
    fdtype suffixes=FD_CAR(rule);
    fdtype replacement=FD_CDR(rule);
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(suff,suffixes)
      if (FD_STRINGP(suff)) {
	FD_DO_CHOICES(repl,replacement)
	  if (FD_STRINGP(repl)) {
	    fdtype result=apply_suffixrule(string,suff,repl,lexicon);
	    if (FD_ABORTP(result)) {
	      fd_decref(results); return result;}
	    else {FD_ADD_TO_CHOICE(results,result);}}
	  else {
	    fd_decref(results);
	    return fd_err(fd_BadMorphRule,"morphrule",NULL,rule);}}
      else return fd_err(fd_BadMorphRule,"morphrule",NULL,rule);
    return results;}
  else if (FD_CHOICEP(rule)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(alternate,rule) {
      fdtype result=apply_morphrule(string,alternate,lexicon);
      FD_ADD_TO_CHOICE(results,result);}
    return results;}
  else return fd_type_error(_("morphrule"),"morphrule",rule);
  return FD_EMPTY_CHOICE;
}

static fdtype morphrule(fdtype string,fdtype rules,fdtype lexicon)
{
  if (FD_EMPTY_LISTP(rules))
    if (check_string(string,lexicon)) return fd_incref(string);
    else return FD_EMPTY_CHOICE;
  else {
    FD_DOLIST(rule,rules) {
      fdtype result=apply_morphrule(string,rule,lexicon);
      if (!(FD_EMPTY_CHOICEP(result))) return result;}
    return FD_EMPTY_CHOICE;}
    
}

/* textclosure prim */

static fdtype textclosure_handler(fdtype expr,fd_lispenv env)
{
  fdtype pattern_arg=fd_get_arg(expr,1);
  fdtype pattern=fd_eval(pattern_arg,env);
  if (FD_VOIDP(pattern_arg))
    return fd_err(fd_SyntaxError,"textclosure_handler",NULL,expr);
  else if (FD_ABORTP(pattern)) return pattern;
  else {
    fdtype closure=fd_textclosure(pattern,env);
    fd_decref(pattern);
    return closure;}
}

/* textframe procedures
   (under construction) */

static int doadds(fdtype table,u8_output out,fdtype xtract)
{
  if (FD_STRINGP(xtract)) 
    if (out)
      u8_putn(out,FD_STRDATA(xtract),FD_STRLEN(xtract));
    else {}
  else if (FD_VECTORP(xtract)) {
    int i=0, len=FD_VECTOR_LENGTH(xtract);
    fdtype *data=FD_VECTOR_DATA(xtract);
    while (i<len) {
      int retval=doadds(table,out,data[i]);
      if (retval<0) return retval; else i++;}}
  else if (FD_PAIRP(xtract)) {
    fdtype sym=FD_CAR(xtract);
    if ((sym==star_symbol) || (sym==plus_symbol)) {
      fdtype elts=FD_CDR(xtract);
      if (FD_EMPTY_LISTP(elts)) {}
      else {
	FD_DOLIST(elt,elts) {
	  int retval=dorewrite(out,elt);
	  if (retval<0) return retval;}}}
    else if (sym==label_symbol) {
      fdtype label=fd_get_arg(xtract,1);
      fdtype content=fd_get_arg(xtract,2);
      if ((FD_VOIDP(content)) || (FD_VOIDP(label))) {
	fd_seterr(fd_BadExtractData,"dorewrite",NULL,xtract);
	return -1;}
      else {
	U8_OUTPUT newout; U8_INIT_OUTPUT(out,32);
	if (doadds(table,&newout,content)<0) {
	  u8_free(newout.u8_outbuf);
	  return -1;}
	else {
	  int outlen=newout.u8_outptr-newout.u8_outbuf;
	  if (out) u8_putn(out,newout.u8_outbuf,outlen);}}}
    else if (sym==subst_symbol) {
      fdtype content=fd_get_arg(xtract,2);
      if (FD_VOIDP(content)) {
	fd_seterr(fd_BadExtractData,"dorewrite",NULL,xtract);
	return -1;}
      else if (dorewrite(out,content)<0) return -1;}
    else {
      fd_seterr(fd_BadExtractData,"dorewrite",NULL,xtract);
      return -1;}}
  else {
    fd_seterr(fd_BadExtractData,"dorewrite",NULL,xtract);
    return -1;}
  return 1;
}

/* Phonetic prims */

static fdtype soundex_prim(fdtype string,fdtype packetp)
{
  if (FD_FALSEP(packetp))
    return fd_init_string(NULL,-1,fd_soundex(FD_STRDATA(string)));
  else return fd_init_packet(NULL,4,fd_soundex(FD_STRDATA(string)));
}

static fdtype metaphone_prim(fdtype string,fdtype packetp)
{
  if (FD_FALSEP(packetp))
    return fd_init_string(NULL,-1,fd_metaphone(FD_STRDATA(string)));
  else {
    u8_string dblm=fd_metaphone(FD_STRDATA(string));
    return fd_init_packet(NULL,strlen(dblm),dblm);}
}

/* Initialization */

static int texttools_init=0;

void fd_init_texttools()
{
  fdtype texttools_module;
  if (texttools_init) return;
  fd_register_source_file(versionid);
  texttools_init=1;
  texttools_module=fd_new_module("TEXTTOOLS",(FD_MODULE_SAFE));
  fd_init_match_c();
  fd_idefn(texttools_module,fd_make_cprim1("MD5",fd_md5,1));
  fd_idefn(texttools_module,
	   fd_make_cprim2x("SOUNDEX",soundex_prim,1,
			   fd_string_type,FD_VOID,
			   -1,FD_FALSE));
  fd_idefn(texttools_module,
	   fd_make_cprim2x("METAPHONE",metaphone_prim,1,
			   fd_string_type,FD_VOID,
			   -1,FD_FALSE));
  
  fd_idefn(texttools_module,fd_make_cprim1x("PORTER-STEM",stem_prim,1,
					    fd_string_type,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_cprim2x("DISEMVOWEL",disemvowel,1,
			   fd_string_type,FD_VOID,
			   fd_string_type,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_cprim1x("DEPUNCT",depunct,1,fd_string_type,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_ndprim(fd_make_cprim2("SEGMENT",segment_prim,1)));
  fd_idefn(texttools_module,
	   fd_make_ndprim(fd_make_cprim2("GETWORDS",getwords_prim,1)));
  fd_idefn(texttools_module,
	   fd_make_ndprim(fd_make_cprim2("WORDS->VECTOR",getwordsv_prim,1)));
  fd_idefn(texttools_module,fd_make_cprim1("LIST->PHRASE",list2phrase_prim,1));
  fd_idefn(texttools_module,fd_make_cprim3x("SEQ->PHRASE",seq2phrase_prim,1,
					    -1,FD_VOID,
					    fd_fixnum_type,FD_INT2DTYPE(0),
					    fd_fixnum_type,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_cprim2x("VECTOR->FRAGS",vector2frags_prim,1,
			   fd_vector_type,FD_VOID,
			   fd_fixnum_type,FD_INT2DTYPE(2)));
  fd_idefn(texttools_module,
	   fd_make_cprim1x("DECODE-ENTITIES",decode_entities_prim,1,
			   fd_string_type,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_cprim3x("COLUMNIZE",columnize_prim,2,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID,
			   -1,FD_FALSE));
  
  fd_idefn(texttools_module,
	   fd_make_cprim3x("HAS-WORD-SUFFIX?",has_word_suffix,2,
			   fd_string_type,FD_VOID,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_cprim3x("HAS-WORD-PREFIX?",has_word_prefix,2,
			   fd_string_type,FD_VOID,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID));

  fd_idefn(texttools_module,
	   fd_make_cprim1x("ISSPACE%",isspace_percentage,1,
			   fd_string_type,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_cprim1x("ISALPHA%",isalpha_percentage,1,
			   fd_string_type,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_cprim1x("MARKUP%",ismarkup_percentage,1,
			   fd_string_type,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_cprim1x("COUNT-WORDS",count_words,1,
			   fd_string_type,FD_VOID));

  fd_idefn(texttools_module,
	   fd_make_cprim2x("STRIP-MARKUP",strip_markup,1,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_cprim3x("STRING-SUBST",string_subst_prim,3,
			   fd_string_type,FD_VOID,
			   fd_string_type,FD_VOID,
			   fd_string_type,FD_VOID));

  fd_idefn(texttools_module,
	   fd_make_cprim4x("TEXTMATCHER",textmatcher,2,
			   -1,FD_VOID,fd_string_type,FD_VOID,
			   fd_fixnum_type,FD_INT2DTYPE(0),
			   fd_fixnum_type,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_cprim4x("TEXTMATCH",textmatch,2,
			   -1,FD_VOID,fd_string_type,FD_VOID,
			   fd_fixnum_type,FD_INT2DTYPE(0),
			   fd_fixnum_type,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_cprim4x("TEXTSEARCH",textsearch,2,
			   -1,FD_VOID,fd_string_type,FD_VOID,
			   fd_fixnum_type,FD_INT2DTYPE(0),
			   fd_fixnum_type,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_cprim4x("TEXTRACT",textract,2,
			   -1,FD_VOID,fd_string_type,FD_VOID,
			   fd_fixnum_type,FD_INT2DTYPE(0),
			   fd_fixnum_type,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_cprim4x("TEXTREWRITE",textrewrite,2,
			   -1,FD_VOID,fd_string_type,FD_VOID,
			   fd_fixnum_type,FD_INT2DTYPE(0),
			   fd_fixnum_type,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_ndprim(fd_make_cprim2("TEXTFILTER",textfilter,2)));
  fd_idefn(texttools_module,fd_make_cprim4x
	   ("STRING-MATCHES?",string_matches,2,
	    fd_string_type,FD_VOID,-1,FD_VOID,
	    fd_fixnum_type,FD_INT2DTYPE(0),
	    fd_fixnum_type,FD_VOID));
  fd_idefn(texttools_module,fd_make_cprim4x
	   ("STRING-CONTAINS?",string_contains,2,
	    fd_string_type,FD_VOID,-1,FD_VOID,
	    fd_fixnum_type,FD_INT2DTYPE(0),
	    fd_fixnum_type,FD_VOID));
  fd_idefn(texttools_module,fd_make_cprim4x
	   ("STRING-STARTS-WITH?",string_starts_with,2,
	    fd_string_type,FD_VOID,-1,FD_VOID,
	    fd_fixnum_type,FD_INT2DTYPE(0),
	    fd_fixnum_type,FD_VOID));
  
  fd_idefn(texttools_module,
	   fd_make_cprim4x("TEXT->FRAME",text2frame,2,
			   -1,FD_VOID,fd_string_type,FD_VOID,
			   fd_fixnum_type,FD_INT2DTYPE(0),
			   fd_fixnum_type,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_cprim4x("TEXT->FRAMES",text2frames,2,
			   -1,FD_VOID,fd_string_type,FD_VOID,
			   fd_fixnum_type,FD_INT2DTYPE(0),
			   fd_fixnum_type,FD_VOID));

  fd_idefn(texttools_module,
	   fd_make_cprim5x("TEXTSUBST",textsubst,2,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID,-1,FD_VOID,
			   fd_fixnum_type,FD_INT2DTYPE(0),
			   fd_fixnum_type,FD_VOID));
  fd_idefn(texttools_module,
	   fd_make_cprim4x("GATHER",textgather,2,
			   -1,FD_VOID,fd_string_type,FD_VOID,
			   fd_fixnum_type,FD_INT2DTYPE(0),
			   fd_fixnum_type,FD_VOID));

  fd_idefn(texttools_module,
	   fd_make_cprim4x("GATHER->LIST",textgather2list,2,
			   -1,FD_VOID,fd_string_type,FD_VOID,
			   fd_fixnum_type,FD_INT2DTYPE(0),
			   fd_fixnum_type,FD_VOID));
  fd_defalias(texttools_module,"GATHER->SEQ","GATHER->LIST");

  fd_idefn(texttools_module,
	   fd_make_cprim3x("TEXTSLICE",textslice,2,
			   fd_string_type,FD_VOID,-1,FD_VOID,
			   -1,FD_TRUE));

  fd_defspecial(texttools_module,"TEXTCLOSURE",textclosure_handler);

  fd_idefn(texttools_module,
	   fd_make_cprim3x("MORPHRULE",morphrule,2,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID,
			   -1,FD_TRUE));

  star_symbol=fd_intern("*");
  plus_symbol=fd_intern("+");
  label_symbol=fd_intern("LABEL");
  subst_symbol=fd_intern("SUBST");
  opt_symbol=fd_intern("OPT");

  fd_finish_module(texttools_module);
  fd_persist_module(texttools_module);
}
