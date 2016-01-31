/* C Mode */

/* match.c
   Regular expression primitives
   Originally implemented by Ken Haase in the Machine Understanding Group
     at the MIT Media Laboratory.
   Extended exentsively by Ken Haase at beingmeta, inc.

   Copyright (C) 1999 Massachusetts Institute of Technology
   Copyright (C) 1999-2016 beingmeta,inc 

*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

/* MATCHER DOCUMENTATION

   The FramerD text matcher provides a powerful and versatile text
   analysis facility designed to support natural readability and
   writablity and to enable and encourage the development of software
   and layered tools which are readily extensible and maintainable.

   TEXT PATTERNS

   FramerD text patterns are compound LISP objects which describe textual
   patterns and are applied against UTF-8 string representations.  The
   use of compound lisp objects simplifies both execution (since the
   pattern is pre-parsed) and readability, since tokens are clearly
   distinguished.

   The core pattern matcher is based on five core LISP types:
   * strings are used for literal content matching:
       e.g. "foo bar" matches "foo bar"; depending on various flags,
        it can also match "Foo bar", "Foo  bar", "Föo Bâr",
        or other variants (see [STRING MATCHING] below)
   * vectors are used for sequences of patterns
       e.g. #("foo" "bar") matches "foobar"
   * choices (see choices.h) are used for alternate matches
       e.g. {"bar" "baz"} matches "bar" or "baz" and
            #("foo" {"bar" "baz"}) matches "foobar" or "foobaz"
   * symbols are used to refer to other patterns, bound in
      the match environment.
   * pairs are used for custom *matchops*
       e.g. (isalpha+) matches "a", "ab", "ba", "ba" or any
            other string of alphabetic characters.  Custom match
            operators can be quite complicated and there are
            quite a few of them and they can be parameterized by
            other patterns.  For example, (+ *pat*) matches any number
            (> 0) of repeated occurences of *pat*, so that (+ "ab")
            matches "ab", "abab", "ababab", "abababababababab", and so on.

   BASIC OPERATIONS

   The matcher library supports three basic operations over text patterns
   and strings: matching, searching, and extraction.

   * matching identifies the length of possible substrings matching a
     pattern and starting at the beginning of the string;  for example,
     a pattern such as (+ "ab") matches "ababababab" with lengths of
     2, 4, 6, 8,and 10.
   * searching identifies the first position in a string which matches
      a particular pattern; e.g. it identifies that the pattern (+ "ab")
      matches a substring of "blabarab" starting at the third character.
   * extraction is a version of matching which returns a copy of the
      text pattern (a compound lisp object) where patterns are replaced
      with substrings of the input string.  Extraction is a core function
      which is typically used by other procedures.

   All of the match functions take a UTF-8 string and start and end arguments
    (u8_byteoff values).  They also take a typically NULL pointer to a LISP
    environment.  This is used to lookup symbol patterns and is typically only
    bound when matching inside patterns created with TEXTCLOSURE (which closes a
    pattern in the current environment).
   
   Internally, the match and extract functions also receive a (possibly FD_VOID)
    dtype pointer to the 'next pattern' used for easily handling wildcards 
    and remainder arguments.  Finally, a *flags* argument controls various aspects
    of the matching process and is normally passed to operations on subpatterns.

   STRING MATCHING

   All the matching operations pass a set of flags which control the
   matching process.  Three of these flags control string comparison
   and can be dynamically applied by corresponding matchops.  When
   passed as a *flags* argument to the matching procedures, these can
   be ORed together.
     FD_MATCH_IGNORE_CASE ignore case differences in strings
         (IGNORE-CASE <pat>) (IC <pat>) ignores case when matching <pat>
         (MATCH-CASE <pat>) (MC <pat>) matches case when matching <pat>
     FD_MATCH_IGNORE_DIACRITICS ignores diacritic modifications on characters
       This ignores Unicode modifier codepoints and also reduces collapsed
       characters to their base form (e.g. ä goes to a).
         (IGNORE-DIACRTICS <pat>) (ID <pat>) ignores diacritics for <pat>
         (MATCH-DIACRITICS <pat>) (MD <pat>) matches diacritics for <pat>
     FD_MATCH_COLLAPSE_SPACES ignores differences in whitespace, e.g.
       "foo bar" matches "foo  bar", "foo       bar", and "foo
       bar".
         (IGNORE-SPACING <pat>) (IS <pat>)
            ignores whitespace variation for <pat>
         (MATCH-SPACING <pat>) (MS <pat>)
            matches whitespace exactly for <pat>

   MATCHER INTERNALS

   The matcher makes extensive use FramerD's choice data structure
   to represent multiple parses internally and externally.  The basic
   match and extract functions return lisp objects (type fdtype)
   in order to allow them to return ambiguous values.  For example
   matching (+ "ab") to "ababab" normally returns the choice {2 4 6}
   to represent the various match alternatives.  (For the exception to
   this, consult [GREEDY MATCHING] below).

   The matcher internally represents lengths and positions as offsets
   into the UTF-8 string.  Because UTF-8 uses multiple-bytes for
   non-ASCII characters, these may not correspond to *character
   positions*.  The code tries to use the u8_charoff and u8_byteoff,
   which are int typedefs, to distinguish between these cases.
   You can convert between these representations by using:
     u8_charoffset(string,byteoff,maxlen)
     u8_byteoffset(string,charoff)

  Match results are represented by choices of byte offsets encoded
  as lisp FIXNUMs, with int values extracted by FD_FIX2INT and recreated
  by FD_INT. If there is only one result, the representation is a
  single fixnum.

  Search results, because they describe the *first* match of a pattern,
  are simple ints (or u8_byteoff which is the same) or -1 to indicate
  a failed search.
   
  Extract results are internally represented by lisp PAIRS whose CARs
  (FD_CAR) contain the corresponding match length (a FIXNUM) and
  whose CDRs (FD_CDR) contain transformed pattern expressions including
  content from the input string.
  For example, the extraction of (+ "ab") against "abab" consists of
  two elements (because there are two matches):
   (2 . (* "ab"))
   (4 . (* "abab"))

  MATCH ENVIRONMENTS
  
  As mentioned above, 

  SUPPORTING HIGHER LEVEL OPERATIONS

  Most higher level operations are supported by embedding structures in the
  results of extraction.  For example, the SUBST matchop takes the
  form (SUBST <pattern> <with>) and yields (SUBST <content> <with>) when
  extracted.  This allows external functions to use the extracted information
  to generate an output string consisting of the original content with
  the designated substitutions.

  The other primary example of this is the LABEL matchop, which takes the form
  (LABEL <symbol> <pattern>) extracted as (LABEL <symbol> <content>) from
  which an external procedure can create records or bind variable
  based on the <symbol>/<content> bindings.

  In addition, many text analysis functions use combinations of
  searching and matching in order to extract substrings.  Typically,
  these functions work by taking the *largest* match and starting
  a search after its end.  For example,
    (gather '(+ (isdigit)) string)
  extracts all the complete digit strings from string, e.g.
    "A .44 beats a .38" ==> {"44" "38"}
  even though (+ (isdigit)) also matches the single digit strings "4",
  "3", and "8".  This usage pattern, called "greedy matching," is not
  normally used internally because it causes the matcher to miss things
  it shouldn't.  However, it can be turned on by special MATCHOPs.

  GREEDY MATCHING

  Normally, the matcher is "expansive," meaning that it considers
  all possible matches at every point in the matching process.  This can
  occasionally lead to surprises, such as when matching the pattern
    #((label head (+ (islower))) (label tail (* (isalpha))))
  which splits an alphabetic string into a lowercase initial portion
  and a remainder.  Applied to the string "fooBaR", this yields
  three matches (we use the Scheme function text->frame to make
  the match results clearer):
    #[HEAD "f" TAIL "ooBar"]
    #[HEAD "fo" TAIL "oBar"]
    #[HEAD "foo" TAIL "Bar"]
  where each prefix of the actual prefix is returned.  The GREEDY
  matchop matches its argument but only considers the longest result, so
  matching
    #((label head (greedy (+ (islower)))) (label tail (* (isalpha))))
  simply returns:
    #[HEAD "foo" TAIL "BaR"]

   GREEDY can be applied to a top level pattern and disabled for a subpattern
   by the matchop EXPANSIVE.  Note that using GREEDY makes all the subpatterns
   match greedily, which may not be what you want.  The matchop LONGEST does
   a regular match but just returns the largest of its component matches.
   
   Why not always be greedy?  (Need to generate a good example).

*/

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/texttools.h"
#include "framerd/fdregex.h"

#include <libu8/u8printf.h>
#include <libu8/u8ctype.h>

#include <ctype.h>

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

fd_exception fd_InternalMatchError=_("Internal match error");
fd_exception fd_MatchSyntaxError=_("match syntax error");
fd_exception fd_TXInvalidPattern=_("Not a valid TX text pattern");

fd_ptr_type fd_txclosure_type;

static fdtype subst_symbol, label_symbol, star_symbol, plus_symbol, opt_symbol;
static fdtype match_env;

#define string_ref(s) ((*(s) < 0x80) ? (*(s)) : (u8_string_ref(s)))

#define FD_MATCH_SPECIAL \
   (FD_MATCH_IGNORE_CASE|FD_MATCH_IGNORE_DIACRITICS|FD_MATCH_COLLAPSE_SPACES)

static fdtype hashset_strget(fd_hashset h,u8_string s,u8_byteoff len)
{
  struct FD_STRING sval;
  FD_INIT_STATIC_CONS(&sval,fd_string_type);
  sval.length=len; sval.bytes=s;
  return fd_hashset_get(h,(fdtype)&sval);
}

static fdtype match_eval(fdtype symbol,fd_lispenv env)
{
  fdtype value=((env)?(fd_symeval(symbol,env)):(FD_VOID));
  if (FD_VOIDP(value))
    return fd_get(match_env,symbol,FD_VOID);
  else return value;
}
static fdtype match_apply(fdtype method,u8_context cxt,fd_lispenv env,
                          int n,fdtype *args)
{
  if (FD_APPLICABLEP(method))
    return fd_apply(method,n,args);
  else if (FD_SYMBOLP(method)) {
    fdtype methfn=match_eval(method,env), result;
    if (FD_APPLICABLEP(methfn))
      result=fd_apply(methfn,n,args);
    else return fd_err(_("Unknown matcher symbol"),cxt,
                       FD_SYMBOL_NAME(method),method);
    fd_decref(methfn);
    return result;}
  else return fd_err(_("Invalid Match Method"),cxt,NULL,method);
}

/** Utility functions **/

static u8_byteoff _get_char_start(const u8_byte *s,u8_byteoff i)
{
  if (s[i]<0x80) return i-1;
  else if (s[i]>=0xc0) return -1;
  else {
    while ((i>=0) && (s[i]<0xc0)) i--;
    return i;}
}

#define backward_char(s,i) \
  ((i==0) ? (i) : ((s[i-1] >= 0x80) ? (_get_char_start(s,i-1)) : (i-1)))

static u8_byteoff _forward_char(const u8_byte *s,u8_byteoff i)
{
  u8_string next=u8_substring(s+i,1);
  if (next) return next-s; else return i+1;
}

#define forward_char(s,i) \
  ((s[i] == 0) ? (i) : (s[i] >= 0x80) ? (_forward_char(s,i)) : (i+1))

static u8_unichar reduce_char(u8_unichar ch,int flags)
{
  if (flags&FD_MATCH_IGNORE_DIACRITICS) {
    u8_unichar nch=u8_base_char(ch);
    if (nch>0) ch=nch;}
  if (flags&FD_MATCH_IGNORE_CASE) ch=u8_tolower(ch);
  return ch;
}

static u8_unichar get_previous_char(u8_string string,u8_byteoff off)
{
  if (off == 0) return -1;
  else if (string[off-1] < 0x80) return string[off-1];
  else {
    u8_byteoff i=off-1, ch; const u8_byte *scan;
    while ((i>0) && (string[i]>=0x80) && (string[i]<0xC0)) i--;
    scan=string+i; ch=u8_sgetc(&scan);
    return ch;}
}

static u8_byteoff strmatcher
  (int flags,
   const u8_byte *pat,u8_byteoff patlen,
   u8_string string,u8_byteoff off,u8_byteoff lim)
{
  if ((flags&FD_MATCH_SPECIAL) == 0)
    if (strncmp(pat,string+off,patlen) == 0) return off+patlen;
    else return -1;
  else {
    int di=(flags&FD_MATCH_IGNORE_DIACRITICS),
      si=(flags&FD_MATCH_COLLAPSE_SPACES);
    const u8_byte *s1=pat, *s2=string+off, *end=s2, *limit=string+lim;
    u8_unichar c1=u8_sgetc(&s1), c2=u8_sgetc(&s2);
    while ((c1>0) && (c2>0) && (s2 <= limit))
      if ((si) && (u8_isspace(c1)) && (u8_isspace(c2))) {
        while ((c1>0) && (u8_isspace(c1))) c1=u8_sgetc(&s1);
        while ((c2>0) && (u8_isspace(c2))) {
          end=s2; c2=u8_sgetc(&s2);}}
      else if ((di) && (u8_ismodifier(c1)) && (u8_ismodifier(c2))) {
        while ((c1>0) && (u8_ismodifier(c1))) c1=u8_sgetc(&s1);
        while ((c2>0) && (u8_ismodifier(c2))) {
          end=s2; c2=u8_sgetc(&s2);}}
      else if (c1 == c2) {
        c1=u8_sgetc(&s1); end=s2; c2=u8_sgetc(&s2);}
      else if (flags&(FD_MATCH_IGNORE_CASE|FD_MATCH_IGNORE_DIACRITICS))
        if (reduce_char(c1,flags) == reduce_char(c2,flags)) {
          c1=u8_sgetc(&s1); end=s2; c2=u8_sgetc(&s2);}
        else return -1;
      else return -1;
    if (c1 < 0) /* If at end of pat string, you have a match */
      return end-string;
    else return -1;}
}

/** Match operator table **/

static struct FD_TEXTMATCH_OPERATOR *match_operators;

static int n_match_operators, limit_match_operators;

static void init_match_operators_table()
{
  match_operators=u8_alloc_n(16,struct FD_TEXTMATCH_OPERATOR);
  n_match_operators=0; limit_match_operators=16;
}

FD_EXPORT
void fd_add_match_operator
  (u8_string label,
   tx_matchfn matcher,tx_searchfn searcher,tx_extractfn extract)
{
  fdtype sym=fd_intern(label);
  struct FD_TEXTMATCH_OPERATOR *scan=match_operators, *limit=scan+n_match_operators;
  while (scan < limit) if (FD_EQ(scan->symbol,sym)) break; else scan++;
  if (scan < limit) {scan->matcher=matcher; return;}
  if (n_match_operators >= limit_match_operators) {
    match_operators=u8_realloc_n
      (match_operators,(limit_match_operators)*2,
       struct FD_TEXTMATCH_OPERATOR);
    limit_match_operators=limit_match_operators*2;}
  match_operators[n_match_operators].symbol=sym;
  match_operators[n_match_operators].matcher=matcher;
  match_operators[n_match_operators].searcher=searcher;
  match_operators[n_match_operators].extract=extract;
  n_match_operators++;
}

/* This is for greedy matching */
static fdtype get_longest_match(fdtype matches)
{
  if ((FD_CHOICEP(matches)) || (FD_ACHOICEP(matches))) {
    u8_byteoff max=-1;
    FD_DO_CHOICES(match,matches) {
      u8_byteoff ival=fd_getint(match);
      if (ival>max) max=ival;}
    fd_decref(matches);
    if (max<0) return FD_EMPTY_CHOICE;
    else return FD_INT(max);}
  else return matches;
}


/** The Matcher **/

static fdtype match_sequence
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);

FD_EXPORT
fdtype fd_text_domatch
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off > lim) return FD_EMPTY_CHOICE;
  else if (FD_EMPTY_CHOICEP(pat)) return FD_EMPTY_CHOICE;
  else if (FD_STRINGP(pat))
    if (off == lim)
      if (FD_STRLEN(pat) == 0) return FD_INT(off);
      else return FD_EMPTY_CHOICE;
    else {
      u8_byteoff mlen=strmatcher
        (flags,FD_STRDATA(pat),FD_STRLEN(pat),
         string,off,lim);
      if (mlen < 0) return FD_EMPTY_CHOICE;
      else return FD_INT(mlen);}
  else if ((FD_CHOICEP(pat)) || (FD_ACHOICEP(pat))) {
    if (flags&(FD_MATCH_BE_GREEDY)) {
      u8_byteoff max=-1;
      FD_DO_CHOICES(each,pat) {
        fdtype answer=fd_text_domatch(each,next,env,string,off,lim,flags);
        if (FD_EMPTY_CHOICEP(answer)) {}
        else if (FD_ABORTP(answer)) {
          FD_STOP_DO_CHOICES;
          return answer;}
        else if (FD_FIXNUMP(answer)) {
          u8_byteoff val=FD_FIX2INT(answer);
          if (val>max) max=val;}
        else if (FD_CHOICEP(answer)) {
          FD_DO_CHOICES(a,answer)
            if (FD_FIXNUMP(a)) {
              u8_byteoff val=FD_FIX2INT(a);
              if (val>max) max=val;}
            else {
              fdtype err=fd_err(fd_InternalMatchError,"fd_text_domatch",NULL,a);
              fd_decref(answer); answer=err;
              FD_STOP_DO_CHOICES;
              break;}
          if (FD_ABORTP(answer)) {
            FD_STOP_DO_CHOICES; return answer;}
          else fd_decref(answer);}
        else {
          fd_decref(answer); FD_STOP_DO_CHOICES;
          return fd_err(fd_InternalMatchError,"fd_text_domatch",NULL,each);}}
      if (max<0) return FD_EMPTY_CHOICE; else return FD_INT(max);}
    else {
      fdtype answers=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(epat,pat) {
        fdtype answer=fd_text_domatch(epat,next,env,string,off,lim,flags);
        if (FD_ABORTP(answer)) {
          FD_STOP_DO_CHOICES;
          fd_decref(answers);
          return answer;}
        FD_ADD_TO_CHOICE(answers,answer);}
      return answers;}}
  else if (FD_CHARACTERP(pat)) {
    if (off == lim) return FD_EMPTY_CHOICE;
    else {
      u8_unichar code=FD_CHAR2CODE(pat);
      if ((code < 0x7f)?(string[off] == code):
          (code == string_ref(string+off))) {
        int next=forward_char(string,off);
        return FD_INT(next);}
      else return FD_EMPTY_CHOICE;}}
  else if (FD_VECTORP(pat))
    return match_sequence(pat,next,env,string,off,lim,flags);
  else if (FD_PAIRP(pat)) {
    fdtype head=FD_CAR(pat), result;
    struct FD_TEXTMATCH_OPERATOR
      *scan=match_operators, *limit=scan+n_match_operators;
    while (scan < limit)
      if (FD_EQ(scan->symbol,head)) break; else scan++; 
    if (scan < limit)
      result=scan->matcher(pat,next,env,string,off,lim,flags);
    else return fd_err(fd_MatchSyntaxError,"fd_text_domatch",
                       _("unknown match operator"),pat);
    if ((FD_CHOICEP(result)) && (flags&(FD_MATCH_BE_GREEDY)))
      return get_longest_match(result);
    else return result;}
  else if (FD_SYMBOLP(pat)) {
    fdtype v=match_eval(pat,env);
    if (FD_VOIDP(v))
      return fd_err(fd_UnboundIdentifier,"fd_text_domatch",
                    FD_SYMBOL_NAME(pat),pat);
    else {
      fdtype result=fd_text_domatch(v,next,env,string,off,lim,flags);
      fd_decref(v); return result;}}
  else if (FD_PTR_TYPEP(pat,fd_txclosure_type)) {
    struct FD_TXCLOSURE *txc=(fd_txclosure)pat;
    return fd_text_matcher(txc->pattern,txc->env,string,off,lim,flags);}
  else if (FD_PTR_TYPEP(pat,fd_regex_type)) {
    int retval=fd_regex_op(rx_matchlen,pat,string+off,lim-off,0);
    if (retval<-1) 
      return fd_err(fd_InternalMatchError,"fd_text_domatch",NULL,pat);
    else if ((retval>lim)||(retval<0)) return FD_EMPTY_CHOICE;
    else return FD_INT(retval);}
  else return fd_err(fd_MatchSyntaxError,"fd_text_domatch",NULL,pat);
}

FD_EXPORT
fdtype fd_text_matcher
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return fd_text_domatch(pat,FD_VOID,env,string,off,lim,flags);
}

static fdtype match_sequence
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  int i=0, l=FD_VECTOR_LENGTH(pat);
  fdtype state=FD_INT(off);
  while (i < l) {
    fdtype epat=FD_VECTOR_REF(pat,i);
    fdtype npat=((i+1==l)?(next):(FD_VECTOR_REF(pat,i+1)));
    fdtype next=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(pos,state) {
      fdtype npos=
        fd_text_domatch(epat,npat,env,string,fd_getint(pos),lim,flags);
      if (FD_ABORTP(npos)) {
        fd_decref(next);
        FD_STOP_DO_CHOICES;
        return npos;}
      FD_ADD_TO_CHOICE(next,npos);}
    fd_decref(state);
    if (FD_EMPTY_CHOICEP(next)) 
      return FD_EMPTY_CHOICE;
    else {state=next; i++;}}
  return state;
}

/** Extraction **/

static fdtype textract
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
static fdtype extract_sequence
  (fdtype pat,int pat_elt,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
static fdtype lists_to_vectors(fdtype lists);

FD_EXPORT
fdtype fd_text_doextract
   (fdtype pat,fdtype next,fd_lispenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return textract(pat,next,env,string,off,lim,flags);
}

FD_EXPORT
fdtype fd_text_extract
   (fdtype pat,fd_lispenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return textract(pat,FD_VOID,env,string,off,lim,flags);
}

static fdtype extract_text(u8_string string,u8_byteoff start,fdtype ends)
{
  fdtype answers=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(each_end,ends)
    if (FD_FIXNUMP(each_end)) {
      fdtype extraction=fd_conspair(each_end,fd_extract_string
                                    (NULL,string+start,string+fd_getint(each_end)));
      FD_ADD_TO_CHOICE(answers,extraction);}
  return answers;
}

static fdtype get_longest_extractions(fdtype extractions)
{
  if ((FD_CHOICEP(extractions)) || (FD_ACHOICEP(extractions))) {
    fdtype largest=FD_EMPTY_CHOICE; u8_byteoff max=-1;
    FD_DO_CHOICES(extraction,extractions) {
      u8_byteoff ival=fd_getint(FD_CAR(extraction));
      if (ival==max) {
        fd_incref(extraction);
        FD_ADD_TO_CHOICE(largest,extraction);}
      else if (ival<max) {}
      else {
        fd_decref(largest); largest=fd_incref(extraction);
        max=ival;}}
    fd_decref(extractions);
    return largest;}
  else return extractions;
}

static fdtype textract
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off > lim) return FD_EMPTY_CHOICE;
  else if (FD_EMPTY_CHOICEP(pat)) return FD_EMPTY_CHOICE;
  else if (FD_STRINGP(pat))
    if ((FD_STRLEN(pat)) == 0)
      return fd_conspair(FD_INT(off),fdtype_string(""));
    else if (off == lim) return FD_EMPTY_CHOICE;
    else {
      u8_byteoff mlen=
        strmatcher(flags,FD_STRDATA(pat),FD_STRLEN(pat),
                   string,off,lim);
      if (mlen<0) return FD_EMPTY_CHOICE;
      else return extract_text(string,off,FD_INT(mlen));}
  else if ((FD_CHOICEP(pat)) || (FD_QCHOICEP(pat)) || (FD_ACHOICEP(pat))) {
    fdtype answers=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(epat,pat) {
      fdtype extractions=textract(epat,next,env,string,off,lim,flags);
      FD_DO_CHOICES(extraction,extractions) {
        if (FD_ABORTP(extraction)) {
          fd_decref(answers); answers=fd_incref(extraction);
          FD_STOP_DO_CHOICES;
          break;}
        else if (FD_PAIRP(extraction)) {
          fd_incref(extraction);
          FD_ADD_TO_CHOICE(answers,extraction);}
        else {
          fd_decref(answers);
          answers=fd_err(fd_InternalMatchError,"textract",NULL,extraction);
          FD_STOP_DO_CHOICES;
          break;}}
      if (FD_ABORTP(answers)) {
        fd_decref(extractions);
        return answers;}
      fd_decref(extractions);}
    if ((flags&FD_MATCH_BE_GREEDY) &&
        ((FD_CHOICEP(answers)) || (FD_ACHOICEP(answers)))) {
      fdtype result=get_longest_extractions(answers);
      return result;}
    else return answers;}
  else if (FD_CHARACTERP(pat)) {
    if (off == lim) return FD_EMPTY_CHOICE;
    else {
      u8_unichar code=FD_CHAR2CODE(pat);
      if ((code < 0x7f)?(string[off] == code):
          (code == string_ref(string+off))) {
        struct U8_OUTPUT str; u8_byte buf[16];
        int next=forward_char(string,off);
        U8_INIT_FIXED_OUTPUT(&str,16,buf);
        u8_putc(&str,code);
        return fd_conspair(FD_INT(next),fdtype_string(buf));}
      else return FD_EMPTY_CHOICE;}}
  else if (FD_VECTORP(pat)) {
    fdtype seq_matches=extract_sequence(pat,0,next,env,string,off,lim,flags);
    if (FD_ABORTP(seq_matches)) return seq_matches;
    else {
      fdtype result=lists_to_vectors(seq_matches);
      fd_decref(seq_matches);
      return result;}}
  else if (FD_PAIRP(pat)) {
    fdtype head=FD_CAR(pat);
    struct FD_TEXTMATCH_OPERATOR
      *scan=match_operators, *limit=scan+n_match_operators;
    while (scan < limit)
      if (FD_EQ(scan->symbol,head)) break; else scan++; 
    if (scan < limit)
      if (scan->extract)
        return scan->extract(pat,next,env,string,off,lim,flags);
      else {
        fdtype matches=scan->matcher(pat,next,env,string,off,lim,flags);
        fdtype answer=extract_text(string,off,matches);
        fd_decref(matches);
        return answer;}
    else return fd_err(fd_MatchSyntaxError,"textract",NULL,pat);}
  else if (FD_SYMBOLP(pat)) {
    fdtype v=match_eval(pat,env);
    if (FD_VOIDP(v))
      return fd_err(fd_UnboundIdentifier,"textract",
                    FD_SYMBOL_NAME(pat),pat);
    else {
      fdtype lengths=
        get_longest_match(fd_text_domatch(v,next,env,string,off,lim,flags));
      fdtype answers=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(l,lengths) {
        fdtype extraction=
          fd_conspair(l,fd_substring(string+off,string+fd_getint(l)));
        if (FD_ABORTP(extraction)) {
          FD_STOP_DO_CHOICES;
          fd_decref(answers);
          return extraction;}
        FD_ADD_TO_CHOICE(answers,extraction);}
      fd_decref(lengths); fd_decref(v);
      return answers;}}
  else if (FD_PTR_TYPEP(pat,fd_txclosure_type)) {
    struct FD_TXCLOSURE *txc=(fd_txclosure)pat;
    return textract(txc->pattern,next,txc->env,string,off,lim,flags);}
  else if (FD_PTR_TYPEP(pat,fd_regex_type)) {
    struct FD_REGEX *ptr=FD_GET_CONS(pat,fd_regex_type,struct FD_REGEX *);
    regmatch_t results[1]; u8_string base=base+off;
    int retval=regexec(&(ptr->compiled),base,1,results,0);
    if (retval==REG_NOMATCH) return FD_EMPTY_CHOICE;
    else if (retval) {
      u8_byte buf[512];
      regerror(retval,&(ptr->compiled),buf,512);
      u8_unlock_mutex(&(ptr->lock));
      return fd_err(fd_RegexError,"fd_text_extract",
                    u8_strdup(buf),FD_VOID);}
    else {
      fdtype ex=fd_extract_string
        (NULL,base+results[0].rm_so,base+results[0].rm_eo);
      int loc=u8_charoffset(string,off+results[0].rm_so);
      return fd_conspair(FD_INT(loc),ex);}}
  else return fd_err(fd_MatchSyntaxError,"textract",NULL,pat);
}

static fdtype extract_sequence
   (fdtype pat,int pat_elt,fdtype next,fd_lispenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  int l=FD_VECTOR_LENGTH(pat);
  if (pat_elt == l)
    return fd_conspair(FD_INT(off),FD_EMPTY_LIST);
  else {
    fdtype nextpat=
      (((pat_elt+1)==l) ? (next) : (FD_VECTOR_REF(pat,pat_elt+1)));
    fdtype sub_matches=
      textract(FD_VECTOR_REF(pat,pat_elt),nextpat,env,string,off,lim,flags);
    if (FD_ABORTP(sub_matches)) return sub_matches;
    else {
      fdtype results=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(sub_match,sub_matches) {
        if (fd_getint(FD_CAR(sub_match)) <= lim) {
          u8_byteoff noff=fd_getint(FD_CAR(sub_match));
          fdtype remainders=extract_sequence
            (pat,pat_elt+1,next,env,string,noff,lim,flags);
          if (FD_ABORTP(remainders)) {
            FD_STOP_DO_CHOICES;
            fd_decref(sub_matches);
            fd_decref(results);
            return remainders;}
          else {
            FD_DO_CHOICES(remainder,remainders) {
              fdtype result=
                fd_conspair(FD_CAR(remainder),
                            fd_conspair(
                                        fd_incref(FD_CDR(sub_match)),
                                        fd_incref(FD_CDR(remainder))));
              FD_ADD_TO_CHOICE(results,result);}
            fd_decref(remainders);}}}
      fd_decref(sub_matches);
      return results;}}
}

static fdtype lists_to_vectors(fdtype lists)
{
  fdtype answer=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(list,lists) {
    fdtype lsize=FD_CAR(list), scan=FD_CDR(list), vec, elt; int i=0, lim=0;
    while (FD_PAIRP(scan)) {lim++; scan=FD_CDR(scan);}
    vec=fd_init_vector(NULL,lim,NULL);
    scan=FD_CDR(list); while (i < lim) {
      fdtype car=FD_CAR(scan); fd_incref(car);
      FD_VECTOR_SET(vec,i,car);
      i++; scan=FD_CDR(scan);}
    elt=fd_conspair(lsize,vec);
    FD_ADD_TO_CHOICE(answer,elt);}
  return answer;
}

/** Match repeatedly **/

static fdtype match_repeatedly
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,
   int flags,int zero_ok)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  fdtype state=FD_INT(off); int count=0;
  if (zero_ok) {FD_ADD_TO_CHOICE(match_points,FD_INT(off));}
  while (1) {
    fdtype next_state=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(pos,state) {
      fdtype npos=
        fd_text_domatch(pat,next,env,string,fd_getint(pos),lim,flags);
      if (FD_ABORTP(npos)) {
        FD_STOP_DO_CHOICES;
        fd_decref(match_points);
        fd_decref(next_state);
        return npos;}
      else {
        FD_DO_CHOICES(n,npos) {
          if (!((FD_CHOICEP(state)) ? (fd_choice_containsp(n,state)) :
                (FD_EQ(state,n)))) {
            if ((flags&FD_MATCH_BE_GREEDY)==0) { 
              fd_incref(n);
              FD_ADD_TO_CHOICE(match_points,n);}
            fd_incref(n);
            FD_ADD_TO_CHOICE(next_state,n);}}
        fd_decref(npos);}}
    if (flags&FD_MATCH_BE_GREEDY)
      if (FD_EMPTY_CHOICEP(next_state)) {
        fd_decref(state);
        return match_points;}
      else {
        fd_decref(match_points);
        match_points=fd_incref(next_state);
        fd_decref(state);
        state=next_state;
        count++;}
    else if (FD_EMPTY_CHOICEP(next_state))
      if (count == 0)
        if (zero_ok) {
          fd_decref(match_points);
          return state;}
        else {
          fd_decref(state);
          fd_decref(match_points);
          return FD_EMPTY_CHOICE;}
      else {
        fd_decref(state);
        return match_points;}
    else {
      fd_decref(state); count++; state=next_state;}}
}

static fdtype extract_repeatedly
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,
   int flags,int zero_ok)
{
  fdtype choices=FD_EMPTY_CHOICE;
  fdtype top=textract(pat,next,env,string,off,lim,flags);
  if (FD_EMPTY_CHOICEP(top))
    if (zero_ok) return fd_conspair(FD_INT(off),FD_EMPTY_LIST);
    else return FD_EMPTY_CHOICE;
  else {
    FD_DO_CHOICES(each,top)
      if ((FD_PAIRP(each)) && (FD_FIXNUMP(FD_CAR(each))) &&
          ((FD_FIX2INT(FD_CAR(each))) != off)) {
        fdtype size=FD_CAR(each);
        fdtype extraction=FD_CDR(each);
        fdtype remainders=
          extract_repeatedly(pat,next,env,string,fd_getint(size),lim,flags,1);
        if (FD_EMPTY_CHOICEP(remainders)) {
          fdtype last_item=
            fd_conspair(fd_incref(extraction),FD_EMPTY_LIST);
          fdtype with_size=fd_conspair(size,last_item);
          FD_ADD_TO_CHOICE(choices,with_size);}
        else {
          FD_DO_CHOICES(remainder,remainders) {
            fdtype item=fd_conspair(fd_car(remainder),
                                    fd_conspair(fd_incref(extraction),(fd_cdr(remainder))));
            FD_ADD_TO_CHOICE(choices,item);}
          fd_decref(remainders);}
        if ((flags&FD_MATCH_BE_GREEDY)==0) {
          fdtype singleton=fd_make_list(1,fd_incref(extraction));
          fdtype pair=fd_conspair(size,singleton);
          FD_ADD_TO_CHOICE(choices,pair);}}}
  fd_decref(top);
  return choices;
}

/** Match operations **/

#define FD_PAT_ARG(arg,cxt,expr) \
  if (FD_VOIDP(arg))              \
    return fd_err(fd_MatchSyntaxError,cxt,NULL,expr);

static fdtype match_star
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (FD_EMPTY_LISTP(FD_CDR(pat))) {
    u8_byteoff nextpos=((FD_VOIDP(next))?(-1):
                 (fd_text_search(next,env,string,off,lim,flags)));
    if (nextpos==-2) return FD_ERROR_VALUE;
    else if (nextpos<0) return FD_INT(lim);
    else return FD_INT(nextpos);}
  else {
    fdtype pat_arg=fd_get_arg(pat,1);
    FD_PAT_ARG(pat_arg,"match_star",pat);
    return match_repeatedly(pat_arg,next,env,string,off,lim,flags,1);}
}
static u8_byteoff search_star
   (fdtype pat,fd_lispenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return off;
}
static fdtype extract_star
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (FD_EMPTY_LISTP(FD_CDR(pat))) {
    u8_byteoff nextpos=
      ((FD_VOIDP(next)) ? (-1) :
       (fd_text_search(next,env,string,off,lim,flags)));
    if (nextpos==-2) return FD_ERROR_VALUE;
    else if (nextpos<0)
      return fd_conspair(FD_INT(nextpos),
                         fd_substring(string+off,string+lim));
    else return fd_conspair(FD_INT(nextpos),
                            fd_substring(string+off,string+nextpos));}
  else {
    fdtype pat_arg=fd_get_arg(pat,1);
    if (FD_VOIDP(pat_arg))
      return fd_err(fd_MatchSyntaxError,"extract_star",NULL,pat);
    else {
      fdtype extractions=extract_repeatedly
        (pat_arg,next,env,string,off,lim,flags,1);
      fdtype answer=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(extraction,extractions) {
        fdtype size=FD_CAR(extraction), data=FD_CDR(extraction);
        fdtype pair=
          fd_conspair(size,fd_conspair(star_symbol,fd_incref(data)));
        FD_ADD_TO_CHOICE(answer,pair);}
      fd_decref(extractions);
      return answer;}}
}

static fdtype match_plus
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"match_plus",NULL,pat);
  else return match_repeatedly(pat_arg,next,env,string,off,lim,flags,0);
}
static u8_byteoff search_plus
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg)) {
    fd_seterr(fd_MatchSyntaxError,"search_plus",NULL,fd_incref(pat));
    return -2;}
  else return fd_text_search(pat_arg,env,string,off,lim,flags);
}
static fdtype extract_plus
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"extract_plus",NULL,pat);
  else {
    fdtype extractions=extract_repeatedly(pat_arg,next,env,string,off,lim,flags,0);
    fdtype answer=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(extraction,extractions) {
      fdtype size=FD_CAR(extraction), data=FD_CDR(extraction);
      fdtype pair=
        fd_conspair(size,fd_conspair(plus_symbol,fd_incref(data)));
      FD_ADD_TO_CHOICE(answer,pair);}
    fd_decref(extractions);
    return answer;}
}

static fdtype match_opt
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1), match_result;
  FD_PAT_ARG(pat_arg,"match_opt",pat);
  match_result=fd_text_domatch(pat_arg,next,env,string,off,lim,flags);
  if (FD_EMPTY_CHOICEP(match_result)) return FD_INT(off);
  else return match_result;
}
static u8_byteoff search_opt
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return off;
}
static fdtype extract_opt
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"extract_opt",NULL,pat);
  else {
    fdtype extraction=textract(pat_arg,next,NULL,string,off,lim,flags);
    if (FD_EMPTY_CHOICEP(extraction))
      return fd_conspair(FD_INT(off),
                         fd_make_list(1,opt_symbol));
    else return extraction;}
}

/** Match NOT **/

static fdtype match_not
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"match_not",NULL,pat);
  else {
    /* Find where there is a pat_arg starting */
    u8_byteoff pos=fd_text_search(pat_arg,env,string,off,lim,flags);
    if (pos == off) return FD_EMPTY_CHOICE;
    else if (pos == -2) return pos;
    else {
      /* Enumerate every character position between here and there */
      u8_byteoff i=forward_char(string,off), last; fdtype result=FD_EMPTY_CHOICE;
      if ((pos < lim) && (pos > off))
        last=pos;
      else last=lim;
      while (i < last) {
        FD_ADD_TO_CHOICE(result,FD_INT(i));
        i=forward_char(string,i);}
      FD_ADD_TO_CHOICE(result,FD_INT(i));
      return result;}}
}

static u8_byteoff search_not
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"search_not",NULL,pat);
  else {
    fdtype match=fd_text_matcher
      (pat_arg,env,string,off,lim,(flags&(~FD_MATCH_DO_BINDINGS)));
    if (FD_EMPTY_CHOICEP(match)) return off;
    else {
      u8_byteoff largest=off;
      FD_DO_CHOICES(m,match) {
        u8_byteoff mi=fd_getint(m);
        if (mi > largest) largest=mi;}
      return largest;}}
}

/** Match AND **/

static fdtype match_and
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat1=fd_get_arg(pat,1);
  fdtype pat2=fd_get_arg(pat,2);
  /* Find where there is a pat_arg starting */
  fdtype matches[2];
  FD_PAT_ARG(pat1,"match_and",pat);
  FD_PAT_ARG(pat2,"match_and",pat);  
  matches[0]=fd_text_domatch(pat1,next,env,string,off,lim,flags);
  if (FD_EMPTY_CHOICEP(matches[0])) return FD_EMPTY_CHOICE;
  else {
    fdtype combined;
    matches[1]=fd_text_domatch(pat2,next,env,string,off,lim,flags);
    if (FD_EMPTY_CHOICEP(matches[1])) combined=FD_EMPTY_CHOICE;
    else combined=fd_intersection(matches,2);
    fd_decref(matches[0]); fd_decref(matches[1]);
    return combined;}
}

static u8_byteoff search_and
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_byteoff result;
  fdtype pat1=fd_get_arg(pat,1);
  fdtype pat2=fd_get_arg(pat,2);
  FD_PAT_ARG(pat1,"search_and",pat);
  FD_PAT_ARG(pat2,"search_and",pat);  
  result=fd_text_search(pat1,env,string,off,lim,flags);
  while (result>=0) {
    fdtype match_result=match_and(pat,FD_VOID,env,string,result,lim,flags);
    if (FD_EMPTY_CHOICEP(match_result))
      result=fd_text_search(pat,env,string,
                            forward_char(string,result),lim,flags);
    else {fd_decref(match_result); return result;}}
  return result;
}

/** Match NOT> **/

static fdtype match_not_gt
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"match_not_gt",NULL,pat);
  else {
    u8_byteoff pos=fd_text_search(pat_arg,env,string,off,lim,flags);
    if (pos < 0)
      if (pos==-2) return FD_ERROR_VALUE;
      else return FD_INT(lim);
    /* else if (pos == off) return FD_EMPTY_CHOICE; */
    else if (pos >= lim) return FD_INT(lim);
    else return FD_INT(pos);}
}

/** Match BIND **/

static fdtype match_bind
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype sym=fd_get_arg(pat,1);
  fdtype spat=fd_get_arg(pat,2);
  FD_PAT_ARG(sym,"match_bind",pat);
  FD_PAT_ARG(spat,"match_bind",pat);  
  if ((flags)&(FD_MATCH_DO_BINDINGS)) {
    fdtype ends=fd_text_domatch(spat,next,env,string,off,lim,flags);
    FD_DO_CHOICES(end,ends) {
      fdtype substr=fd_substring(string+off,string+fd_getint(end));
      fd_bind_value(sym,substr,env);}
    return ends;}
  else return fd_text_domatch(spat,next,env,string,off,lim,flags);
}
static u8_byteoff search_bind
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype spat=fd_get_arg(pat,2);
  if (FD_VOIDP(spat))
    return fd_err(fd_MatchSyntaxError,"search_bind",NULL,pat);
  else return fd_text_search(spat,env,string,off,lim,flags);
}

static fdtype label_match 
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype spat=fd_get_arg(pat,2);
  if (FD_VOIDP(spat))
    return fd_err(fd_MatchSyntaxError,"label_match",NULL,pat);
  else return fd_text_domatch(spat,next,env,string,off,lim,flags);
}

static u8_byteoff label_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype spat=fd_get_arg(pat,2);
  if (FD_VOIDP(spat))
    return fd_err(fd_MatchSyntaxError,"label_search",NULL,pat);
  else return fd_text_search(spat,env,string,off,lim,flags);
}

static fdtype label_extract
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype sym=fd_get_arg(pat,1);
  fdtype spat=fd_get_arg(pat,2);
  fdtype parser=fd_get_arg(pat,3);
  if ((FD_VOIDP(spat)) || (FD_VOIDP(sym)))
    return fd_err(fd_MatchSyntaxError,"label_extract",NULL,pat);
  else {
    fdtype extractions=textract(spat,next,env,string,off,lim,flags);
    fdtype answers=FD_EMPTY_CHOICE;
    if (FD_ABORTP(extractions)) return extractions;
    else {
      FD_DO_CHOICES(extraction,extractions) {
        fdtype size=FD_CAR(extraction), data=FD_CDR(extraction);    
        fdtype xtract, addval; fd_incref(data);
        if (FD_VOIDP(parser))
          xtract=fd_make_list(3,FD_CAR(pat),sym,data);
        else if (FD_SYMBOLP(parser)) {
          fdtype parser_val=match_eval(parser,env);
          if ((FD_ABORTP(parser_val))||(FD_VOIDP(parser_val))) {
            FD_STOP_DO_CHOICES;
            fd_decref(answers); fd_decref(extractions);
            if (FD_VOIDP(parser_val)) 
              return fd_err(fd_UnboundIdentifier,"label_extract/convert",
                            FD_SYMBOL_NAME(parser),parser);
            else return parser_val;}
          xtract=fd_make_list(4,FD_CAR(pat),sym,data,parser_val);}
        else if ((env) && (FD_PAIRP(parser))) {
          fdtype parser_val=fd_eval(parser,env);
          if ((FD_ABORTP(parser_val))||(FD_VOIDP(parser_val))) {
            FD_STOP_DO_CHOICES;
            fd_decref(answers); fd_decref(extractions);
            return parser_val;}
          xtract=fd_make_list(4,FD_CAR(pat),sym,data,parser_val);}
        else {
          fd_incref(parser);
          xtract=fd_make_list(4,FD_CAR(pat),sym,data,parser);}
        addval=fd_conspair(size,xtract);
        FD_ADD_TO_CHOICE(answers,addval);}
      fd_decref(extractions);
      return answers;}}
}

static fdtype subst_match 
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype spat=fd_get_arg(pat,1);
  if (FD_VOIDP(spat))
    return fd_err(fd_MatchSyntaxError,"subst_match",NULL,pat);
  else return fd_text_domatch(spat,next,env,string,off,lim,flags);
}

static u8_byteoff subst_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype spat=fd_get_arg(pat,1);
  if (FD_VOIDP(spat))
    return fd_err(fd_MatchSyntaxError,"subst_search",NULL,pat);
  else return fd_text_search(spat,env,string,off,lim,flags);
}

static fdtype expand_subst_args(fdtype args,fd_lispenv env);
static fdtype subst_extract
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype spat=fd_get_arg(pat,1);
  if (FD_VOIDP(spat))
    return fd_err(fd_MatchSyntaxError,"subst_extract",NULL,pat);
  else {
    fdtype matches=fd_text_domatch(spat,next,env,string,off,lim,flags);
    if (FD_ABORTP(matches)) return matches;
    else {
      fdtype answers=FD_EMPTY_CHOICE;
      fdtype args=FD_CDR(FD_CDR(pat));
      fdtype expanded=expand_subst_args(args,env);
      FD_DO_CHOICES(match,matches) {
        int matchlen=fd_getint(match);
        fdtype matched=fd_substring(string+off,string+matchlen);
        if ((FD_CHOICEP(expanded))||(FD_ACHOICEP(expanded))) {
          FD_DO_CHOICES(subst_arg,expanded) {
            fdtype new_args=fd_conspair(fd_incref(matched),fd_incref(subst_arg));
            fdtype new_subst=fd_conspair(subst_symbol,new_args);
            fdtype answer=fd_conspair(match,new_subst);
            FD_ADD_TO_CHOICE(answers,answer);}
          fd_decref(matched);}
        else {
          fdtype new_args=fd_conspair(matched,expanded);
          fdtype new_subst=fd_conspair(subst_symbol,new_args);
          fdtype answer=fd_conspair(match,new_subst);
          fd_incref(expanded);
          FD_ADD_TO_CHOICE(answers,answer);}}
      fd_decref(expanded);
      fd_decref(matches);
      return answers;}}
}

static fdtype expand_subst_args(fdtype args,fd_lispenv env)
{
  if (FD_SYMBOLP(args)) {
    fdtype value=match_eval(args,env);
    if (FD_VOIDP(value)) return fd_incref(args);
    else return value;}
  else if (!(FD_CONSP(args))) return args;
  else if (FD_PAIRP(args)) {
    fdtype carchoices=expand_subst_args(FD_CAR(args),env);
    fdtype cdrchoices=expand_subst_args(FD_CDR(args),env);
    /* Avoid the multiplication of conses by reusing ARGS if it
       hasn't exploded or changed. */
    if ((FD_CHOICEP(carchoices))||(FD_ACHOICEP(carchoices))||
        (FD_CHOICEP(cdrchoices))||(FD_ACHOICEP(cdrchoices))) {
      fdtype conses=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(car,carchoices) {
        FD_DO_CHOICES(cdr,cdrchoices) {
          fdtype cons=fd_conspair(car,cdr);
          fd_incref(car); fd_incref(cdr);
          FD_ADD_TO_CHOICE(conses,cons);}}
      fd_decref(carchoices);
      fd_decref(cdrchoices);
      return conses;}
    else if ((FD_EQUAL(carchoices,FD_CAR(args)))&&
             (FD_EQUAL(cdrchoices,FD_CDR(args)))) {
      fd_decref(carchoices); fd_decref(cdrchoices);
      return fd_incref(args);}
    else return fd_conspair(carchoices,cdrchoices);}
  else if (FD_CHOICEP(args)) {
    fdtype changed=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(elt,args) {
      fdtype cv=expand_subst_args(elt,env);
      FD_ADD_TO_CHOICE(changed,cv);}
    return changed;}
  else return fd_incref(args);
}

/* Match preferred */

/* This is just like a choice pattern except that when searching,
   we use the patterns in order and bound subsequent searches by previous
   results. */

static fdtype match_pref
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  FD_DOLIST(epat,FD_CDR(pat)) {
    fdtype answer=fd_text_domatch(epat,next,env,string,off,lim,flags);
    if (FD_ABORTP(answer)) return answer;
    else if (FD_EMPTY_CHOICEP(answer)) {}
    else return answer;}
  return FD_EMPTY_CHOICE;
}

static fdtype extract_pref
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype answers=FD_EMPTY_CHOICE;
  FD_DOLIST(epat,FD_CDR(pat)) {
    fdtype extractions=textract(epat,next,env,string,off,lim,flags);
    if (FD_ABORTP(extractions)) return extractions;
    else if (FD_EMPTY_CHOICEP(extractions)) {}
    else return extractions;}
  return FD_EMPTY_CHOICE;
}

static int search_pref
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  int nlim=lim, loc=-1;
  FD_DOLIST(epat,FD_CDR(pat)) {
    u8_byteoff nxt=fd_text_search(epat,env,string,off,nlim,flags);
    if (nxt < 0) {
      if (nxt==-2) {
        return nxt;}}
    else if (nxt < nlim) {nlim=nxt; loc=nxt;}}
  return loc;
}


/* Word match */

#define apostrophep(x) ((x == '\'')||(x==0x2019))
#define dashp(x) ((x == '-')||(x == '_')||(x==0xAD)||(x==0x2010)||(x==0x2011)||(x=='/')||(x==0x2044))

static int word_startp(u8_string string,u8_byteoff off)
{
  if (off==0) return 1;
  else {
    int new_off=backward_char(string,off);
    u8_string scan=string+new_off;
    u8_unichar ch=u8_sgetc(&scan);
    if (ch < 0) return 1;
    else if (u8_isspace(ch)) return 1;
    else if (u8_ispunct(ch)) {
      if ((apostrophep(ch))||(dashp(ch))) {
        if (new_off==0) return 1;
        else {
          new_off=backward_char(string,new_off);
          scan=string+new_off;}
        ch=u8_sgetc(&scan);
        if ((u8_isspace(ch))||(u8_ispunct(ch))) return 1;
        else return 0;}
      else return 1;}
    else return 0;}
}

static fdtype word_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype wpat=fd_get_arg(pat,1);
  if (FD_VOIDP(wpat))
    return fd_err(fd_MatchSyntaxError,"word_match",NULL,pat);
  else if ((off > 0) && ((word_startp(string,off)) == 0))
    return FD_EMPTY_CHOICE;
  else {
    fdtype final_results=(FD_EMPTY_CHOICE);
    fdtype core_result=fd_text_domatch
      (wpat,next,env,string,off,lim,(flags|FD_MATCH_COLLAPSE_SPACES));
    if (FD_ABORTP(core_result)) {
      fd_decref(final_results);
      return core_result;}
    else {
      FD_DO_CHOICES(offset,core_result) {
        if (FD_FIXNUMP(offset)) {
          u8_byteoff noff=FD_FIX2INT(offset), complete_word=0;
          if (noff == lim) complete_word=1;
          else {
            const u8_byte *ptr=string+noff;
            u8_unichar ch=u8_sgetc(&ptr);
            if ((u8_isspace(ch)) || (u8_ispunct(ch))) complete_word=1;}
          if (complete_word) {
            fd_incref(offset);
            FD_ADD_TO_CHOICE(final_results,offset);}}
        else {
          FD_STOP_DO_CHOICES;
          fd_incref(offset); fd_decref(core_result); 
          return fd_err(fd_InternalMatchError,"word_match",NULL,offset);}}}
    fd_decref(core_result);
    return final_results;}
}

static u8_byteoff get_next_candidate
  (u8_string string,u8_byteoff off,u8_byteoff lim)
{
  const u8_byte *scan=string+off, *limit=string+lim;
  while (scan < limit) {   /* Find another space */
    u8_unichar ch=string_ref(scan);
    if ((u8_isspace(ch)) || (u8_ispunct(ch))) break;
    else if (*scan < 0x80) scan++;
    else u8_sgetc(&scan);}
  if (scan >= limit) return -1;
  else { /* Skip over the space */
    const u8_byte *probe=scan, *prev=scan;
    while (probe < limit) {
      u8_unichar ch=u8_sgetc(&probe);
      if ((u8_isspace(ch)) || (u8_ispunct(ch))) prev=probe;
      else break;}
    scan=prev;}
  if (scan >= limit) return -1;
  else return scan-string;
}

static u8_byteoff word_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype wpat=fd_get_arg(pat,1); u8_byteoff match_result=-1, cand;
  if (FD_VOIDP(wpat))
    return fd_err(fd_MatchSyntaxError,"word_search",NULL,pat);
  else if (word_startp(string,off)) cand=off;
  else cand=get_next_candidate(string,off,lim);
  while ((cand >= 0) && (match_result < 0)) {
    fdtype matches=
      fd_text_matcher(wpat,env,string,cand,lim,(flags|FD_MATCH_COLLAPSE_SPACES));
    if (FD_EMPTY_CHOICEP(matches)) {}
    else {
      FD_DO_CHOICES(match,matches) {
        u8_byteoff n=fd_getint(match), ch;
        if (n == lim) {
          match_result=cand; FD_STOP_DO_CHOICES; break;}
        else {
          const u8_byte *p=string+n; ch=u8_sgetc(&p);
          if ((u8_isspace(ch)) || (u8_ispunct(ch))) {
            match_result=cand; FD_STOP_DO_CHOICES; break;}}}}
    cand=get_next_candidate(string,cand,lim);
    fd_decref(matches);}
  return match_result;
}

static fdtype word_extract
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype wpat=fd_get_arg(pat,1);
  if (FD_VOIDP(wpat))
    return fd_err(fd_MatchSyntaxError,"word_extract",NULL,pat);
  else {
    fdtype ends=fd_text_domatch
      (wpat,next,env,string,off,lim,(flags|FD_MATCH_COLLAPSE_SPACES));
    fdtype answers=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(end,ends) {
      fdtype substring=
        fd_substring(string+off,string+fd_getint(end));
      fdtype pair=fd_conspair(end,substring);
      FD_ADD_TO_CHOICE(answers,pair);}
    fd_decref(ends);
    return answers;}
}

/* Matching chunks */

static fdtype chunk_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype cpat=fd_get_arg(pat,1);
  if (FD_VOIDP(cpat))
    return fd_err(fd_MatchSyntaxError,"chunk_match",NULL,pat);
  else return fd_text_domatch(cpat,next,env,string,off,lim,flags);
}

static u8_byteoff chunk_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype cpat=fd_get_arg(pat,1);
  if (FD_VOIDP(cpat))
    return fd_err(fd_MatchSyntaxError,"chunk_search",NULL,pat);
  else return fd_text_search(cpat,env,string,off,lim,flags);
}

static fdtype chunk_extract
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype cpat=fd_get_arg(pat,1);
  if (FD_VOIDP(cpat))
    return fd_err(fd_MatchSyntaxError,"chunk_extract",NULL,pat);
  else {
    fdtype ends=fd_text_domatch(cpat,next,env,string,off,lim,flags);
    fdtype answers=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(end,ends) {
      fdtype substring=
        fd_substring(string+off,string+fd_getint(end));
      fdtype pair=fd_conspair(end,substring);
      FD_ADD_TO_CHOICE(answers,pair);}
    fd_decref(ends);
    return answers;}
}

/** CHOICE matching **/

/* These methods are equivalent to passing choices, but allow:
    (a) the protection of choices from automatic evaluation, and
    (b) increasing readability by combining multiple choices together,
         e.g. (CHOICE {"tree" "bush"} {"beast" "rodent"})
*/
         
static fdtype match_choice
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if ((FD_PAIRP(FD_CDR(pat))) &&
      (FD_EMPTY_LISTP(FD_CDR(FD_CDR(pat)))))
    return fd_text_domatch
      (FD_CAR(FD_CDR(pat)),next,env,string,off,lim,flags);
  else {
    fdtype choice=FD_EMPTY_CHOICE;
    FD_DOLIST(elt,FD_CDR(pat)) {
      fd_incref(elt);
      FD_ADD_TO_CHOICE(choice,elt);}
    if (FD_EMPTY_CHOICEP(choice)) return FD_EMPTY_CHOICE;
    else {
      fdtype result=fd_text_matcher(choice,env,string,off,lim,flags);
      fd_decref(choice);
      return result;}}
}

static u8_byteoff search_choice
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if ((FD_PAIRP(FD_CDR(pat))) &&
      (FD_EMPTY_LISTP(FD_CDR(FD_CDR(pat)))))
    return fd_text_search(FD_CAR(FD_CDR(pat)),env,string,off,lim,flags);
  else {
    fdtype choice=FD_EMPTY_CHOICE;
    FD_DOLIST(elt,FD_CDR(pat)) {
      fd_incref(elt);
      FD_ADD_TO_CHOICE(choice,elt);}
    if (FD_EMPTY_CHOICEP(choice)) return -1;
    else {
      u8_byteoff result=fd_text_search(choice,env,string,off,lim,flags);
      fd_decref(choice);
      return result;}}
}

static fdtype extract_choice
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if ((FD_PAIRP(FD_CDR(pat))) &&
      (FD_EMPTY_LISTP(FD_CDR(FD_CDR(pat)))))
    return fd_text_doextract
      (FD_CAR(FD_CDR(pat)),next,env,string,off,lim,flags);
  else {
    fdtype choice=FD_EMPTY_CHOICE;
    FD_DOLIST(elt,FD_CDR(pat)) {
      fd_incref(elt);
      FD_ADD_TO_CHOICE(choice,elt);}
    if (FD_EMPTY_CHOICEP(choice)) return FD_EMPTY_CHOICE;
    else {
      fdtype result=textract(choice,next,env,string,off,lim,flags);
      fd_decref(choice);
      return result;}}
}

/** Case sensitive/insensitive **/

static fdtype match_ci
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"match_ci",NULL,pat);
  else return fd_text_matcher
    (pat_arg,env,string,off,lim,(flags|(FD_MATCH_IGNORE_CASE)));
}
static u8_byteoff search_ci
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"search_ci",NULL,pat);
  else return fd_text_search(pat_arg,env,string,off,lim,
                             (flags|(FD_MATCH_IGNORE_CASE)));
}
static fdtype extract_ci
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"extract_ci",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags|(FD_MATCH_IGNORE_CASE)));
}

static fdtype match_cs
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"match_cs",NULL,pat);
  else return fd_text_matcher
    (pat_arg,env,string,off,lim,(flags&(~(FD_MATCH_IGNORE_CASE))));
}
static u8_byteoff search_cs
   (fdtype pat,fd_lispenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"search_cs",NULL,pat);
  else return fd_text_search
         (pat_arg,env,string,off,lim,(flags&(~(FD_MATCH_IGNORE_CASE))));
}
static fdtype extract_cs
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"extract_cs",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags&(~(FD_MATCH_IGNORE_CASE))));
}

/* Diacritic insensitive and sensitive matching */

static fdtype match_di
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"match_di",NULL,pat);
  else return fd_text_matcher
    (pat_arg,env,
     string,off,lim,(flags|(FD_MATCH_IGNORE_DIACRITICS)));
}
static u8_byteoff search_di
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"search_di",NULL,pat);
  else return fd_text_search(pat_arg,env,string,off,lim,
                             (flags|(FD_MATCH_IGNORE_DIACRITICS)));
}
static fdtype extract_di
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"extract_di",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags|(FD_MATCH_IGNORE_DIACRITICS)));
}

static fdtype match_ds
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"match_ds",NULL,pat);
  else return fd_text_matcher
         (pat_arg,env,string,off,lim,(flags&(~(FD_MATCH_IGNORE_DIACRITICS))));
}
static u8_byteoff search_ds
   (fdtype pat,fd_lispenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"search_ds",NULL,pat);
  else return fd_text_search
    (pat_arg,env,string,off,lim,(flags&(~(FD_MATCH_IGNORE_DIACRITICS))));
}
static fdtype extract_ds
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"extract_ds",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags&(~(FD_MATCH_IGNORE_DIACRITICS))));
}

/* Greedy/expansion match/search/etc. */

static fdtype match_greedy
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"match_greedy",NULL,pat);
  else return fd_text_matcher
    (pat_arg,env,string,off,lim,(flags|(FD_MATCH_BE_GREEDY)));
}
static u8_byteoff search_greedy
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"search_greedy",NULL,pat);
  else return fd_text_search(pat_arg,env,string,off,lim,
                             (flags|(FD_MATCH_BE_GREEDY)));
}
static fdtype extract_greedy
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"extract_greedy",NULL,pat);
  else return textract
         (pat_arg,next,env,string,off,lim,(flags|(FD_MATCH_BE_GREEDY)));
}

static fdtype match_expansive
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"match_expansive",NULL,pat);
  else return fd_text_matcher
         (pat_arg,env,string,off,lim,(flags&(~(FD_MATCH_BE_GREEDY))));
}
static u8_byteoff search_expansive
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"search_expansive",NULL,pat);
  else return fd_text_search
    (pat_arg,env,string,off,lim,(flags&(~(FD_MATCH_BE_GREEDY))));
}
static fdtype extract_expansive
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"extract_expansive",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags&(~(FD_MATCH_BE_GREEDY))));
}

static fdtype match_longest
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"match_longest",NULL,pat);
  else return get_longest_match(fd_text_matcher(pat_arg,env,string,off,lim,flags));
}
static u8_byteoff search_longest
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"search_longest",NULL,pat);
  else return fd_text_search(pat_arg,env,string,off,lim,flags);
}
static fdtype extract_longest
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"extract_longeset",NULL,pat);
  else {
    fdtype results=fd_text_matcher(pat_arg,env,string,off,lim,flags);
    if (FD_ABORTP(results)) return results;
    else return get_longest_extractions(results);}
}

/* Space collapsing matching */

static fdtype match_si
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"match_si",NULL,pat);
  else return fd_text_matcher
         (pat_arg,env,string,off,lim,(flags|(FD_MATCH_COLLAPSE_SPACES)));
}

static u8_byteoff search_si
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"search_si",NULL,pat);
  else return fd_text_search(pat_arg,env,string,off,lim,
                             (flags|(FD_MATCH_COLLAPSE_SPACES)));
}
static fdtype extract_si
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"extract_si",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags|(FD_MATCH_COLLAPSE_SPACES)));
}

static fdtype match_ss
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"match_ss",NULL,pat);
  else return fd_text_matcher
         (pat_arg,env,string,off,lim,(flags&(~(FD_MATCH_COLLAPSE_SPACES))));
}
static u8_byteoff search_ss
   (fdtype pat,fd_lispenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"search_ss",NULL,pat);
  else return fd_text_search
    (pat_arg,env,string,off,lim,(flags&(~(FD_MATCH_COLLAPSE_SPACES))));
}
static fdtype extract_ss
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"extract_ss",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags&(~(FD_MATCH_COLLAPSE_SPACES))));
}

/** Canonical matching: ignore spacing, case, and diacritics */

static fdtype match_canonical
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"match_canonical",NULL,pat);
  else return fd_text_matcher
         (pat_arg,env,string,off,lim,(flags|(FD_MATCH_SPECIAL)));
}
static u8_byteoff search_canonical
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"search_canonical",NULL,pat);
  else return fd_text_search(pat_arg,env,string,off,lim,
                             (flags|(FD_MATCH_SPECIAL)));
}
static fdtype extract_canonical
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"extract_canonical",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags|(FD_MATCH_SPECIAL)));
}

/** EOL and BOL **/

/* Matching/finding beginning of lines */
static fdtype match_bol
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == 0)
    return FD_INT(0);
  else if ((string[off-1] == '\n') || (string[off-1] == '\r'))
    return FD_INT(off);
  else return FD_EMPTY_CHOICE;
}

static u8_byteoff search_bol
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == 0) return off;
  else if ((string[off-1] == '\n') || (string[off-1] == '\r')) return off;
  else {
    const u8_byte *scan=strchr(string+off,'\n');
    if (scan) return ((scan-string)+1);
    else {
      const u8_byte *scan=strchr(string+off,'\r');
      if (scan) return ((scan-string)+1);
      else return -1;}}
}

/* Matching/finding end of lines */

static fdtype match_eol
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == lim) return FD_INT(off);
  else if ((string[off] == '\n') || (string[off] == '\r'))
    return FD_INT(off+1);
  else return FD_EMPTY_CHOICE;
}

static u8_byteoff search_eol
    (fdtype pat,fd_lispenv env,
     u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == lim) return off+1;
  else if (off > lim) return -1;
  else {
    const u8_byte *scan=strchr(string+off,'\n');
    if (scan) return ((scan-string));
    else {
      const u8_byte *scan=strchr(string+off,'\r');
      if (scan) return ((scan-string));
      else return lim;}}
}

/* Matching/finding beginning of words */

static fdtype match_bow
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == 0)
    return FD_INT(0);
  else {
    u8_byteoff prev=backward_char(string,off);
    const u8_byte *ptr=string+prev;
    int c=u8_sgetc(&ptr);
    if (u8_isspace(c)) return FD_INT(off);
    else return FD_EMPTY_CHOICE;}
}

static u8_byteoff search_bow
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == 0) return off;
  else {
    const u8_byte *scan=string+off, *scanlim=string+lim, *last=scan;
    int c=u8_sgetc(&scan);
    if (u8_isspace(c))  {
      while ((c>0) && (scan<scanlim) && (u8_isspace(c))) {
        last=scan; c=u8_sgetc(&scan);}
      if (scan<scanlim) return last-string; else return -1;}
    else {
      u8_byteoff prev=backward_char(string,off);
      if (prev<0) return -1; else {
        scan=string+prev; c=u8_sgetc(&scan);}
      if (u8_isspace(c)) return off;
      while ((c>0) && (scan<scanlim) && (!(u8_isspace(c))))
        c=u8_sgetc(&scan);
      if (scan>=scanlim) return -1;
      else while ((c>0) && (scan<scanlim) && (u8_isspace(c))) {
          last=scan; c=u8_sgetc(&scan);}
      if (scan>=scanlim) return -1;
      else return last-string;}}
}


/* Matching/finding the beginning or end of the string */

static fdtype match_bos
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == 0) return FD_INT(off);
  else return FD_EMPTY_CHOICE;
}

static u8_byteoff search_bos
    (fdtype pat,fd_lispenv env,
     u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == 0) return off;
  else return -1;
}

static fdtype match_eos
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == lim) return FD_INT(off);
  else return FD_EMPTY_CHOICE;
}

static u8_byteoff search_eos
    (fdtype pat,fd_lispenv env,
     u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == lim) return off+1;
  else return lim;
}

/* Rest matching */

static fdtype match_rest
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return FD_INT(lim);
}
static u8_byteoff search_rest
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return off;
}
static fdtype extract_rest
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return fd_conspair(FD_INT(lim),
                     fd_substring(string+off,string+lim));
}

/** Character match operations **/

static fdtype match_char_range
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar start=FD_CHAR2CODE(fd_get_arg(pat,1));
  u8_unichar end=FD_CHAR2CODE(fd_get_arg(pat,2));
  u8_unichar actual=string_ref(string+off);
  if (flags&(FD_MATCH_IGNORE_CASE)) {
    start=u8_tolower(start); start=u8_tolower(end);
    actual=u8_tolower(start);}
  if ((actual >= start) && (actual <= end)) {
    const u8_byte *new_off=u8_substring(string+off,1);
    return FD_INT(new_off-string);}
  else return FD_EMPTY_CHOICE;
}

static fdtype match_char_not_core
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string, u8_byteoff off,u8_byteoff lim,
   int flags,int match_null_string)
{
  fdtype arg1=fd_get_arg(pat,1);
  const u8_byte *scan=string+off, *last_scan=scan, *end=string+lim;
  int *break_chars, n_break_chars=0;
  if (FD_VOIDP(pat))
    return fd_err(fd_MatchSyntaxError,"match_char_not_core",NULL,pat);
  else if (!(FD_STRINGP(arg1)))
    return fd_type_error("string","match_char_not_core",arg1);
  else {
    const u8_byte *scan=FD_STRDATA(arg1); int c=u8_sgetc(&scan);
    break_chars=u8_alloc_n(FD_STRLEN(arg1),unsigned int);
    while (!(c<0)) {
      if (flags&(FD_MATCH_IGNORE_CASE))
        break_chars[n_break_chars++]=u8_tolower(c);
      else break_chars[n_break_chars++]=c;
      c=u8_sgetc(&scan);}}
  while (scan<end) {
    int i=0, hit=0; u8_unichar ch=u8_sgetc(&scan);
    if (ch == -1) ch=0;
    if (flags&(FD_MATCH_IGNORE_CASE)) ch=u8_tolower(ch);
    while (i < n_break_chars)
      if (break_chars[i] == ch) {hit=1; break;} else i++;
    if (hit)
      if (last_scan == string+off) {
        u8_free(break_chars);
        if (match_null_string) return FD_INT(last_scan-string);
        else return FD_EMPTY_CHOICE;}
      else {
        u8_free(break_chars);
        return FD_INT(last_scan-string);}
    else last_scan=scan;}
  u8_free(break_chars);
  return FD_INT(lim);
}

static fdtype match_char_not
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"match_char_not",NULL,pat);
  else return match_char_not_core(pat,next,env,string,off,lim,flags,0);
}
static fdtype match_char_not_star
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype pat_arg=fd_get_arg(pat,1);
  if (FD_VOIDP(pat_arg))
    return fd_err(fd_MatchSyntaxError,"match_char_not_star",NULL,pat);
  else return match_char_not_core(pat,next,env,string,off,lim,flags,1);
}

static fdtype isvowel_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off), bch=u8_base_char(ch);
  if (strchr("aeiouAEIOU",bch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}

static fdtype isnotvowel_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off), bch=u8_base_char(ch);
  if (strchr("aeiouAEIOU",bch)) return FD_EMPTY_CHOICE;
  else return FD_INT(forward_char(string,off));
}

static fdtype isspace_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if (u8_isspace(ch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype isspace_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  const u8_byte *scan=string+off, *limit=string+lim, *last=scan;
  u8_unichar ch=u8_sgetc(&scan);
  while (u8_isspace(ch)) 
    if (scan > limit) break;
    else {
      last=scan;
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(last-string);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(last-string));}
      ch=u8_sgetc(&scan);}
  if (last == string) return FD_EMPTY_CHOICE;
  else if ((flags)&(FD_MATCH_BE_GREEDY))
    return get_longest_match(match_points);
  else return match_points;
}
static u8_byteoff isspace_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (u8_isspace(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

static fdtype spaces_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *limit=string+lim, *last=scan;
  u8_unichar ch=u8_sgetc(&scan);
  while (u8_isspace(ch)) 
    if (scan > limit) break; else {last=scan; ch=u8_sgetc(&scan);}
  if (last == string+off) return FD_EMPTY_CHOICE;
  else return FD_INT(last-string);
}
static u8_byteoff spaces_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (u8_isspace(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

static fdtype spaces_star_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *limit=string+lim, *last=scan;
  u8_unichar ch=u8_sgetc(&scan);
  while ((ch>0) && (u8_isspace(ch))) 
    if (scan > limit) break; else {last=scan; ch=u8_sgetc(&scan);}
  if (last == string) return FD_INT(off);
  else return FD_INT(last-string);
}
static u8_byteoff spaces_star_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off > lim) return -1;
  else return off;
}

/* Horizontal space matching */

static fdtype ishspace_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if (u8_ishspace(ch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype ishspace_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  const u8_byte *scan=string+off, *limit=string+lim, *last=scan;
  u8_unichar ch=u8_sgetc(&scan);
  while (u8_ishspace(ch)) 
    if (scan > limit) break;
    else {
      last=scan;
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(last-string);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(last-string));}
      ch=u8_sgetc(&scan);}
  if (last == string) return FD_EMPTY_CHOICE;
  else if ((flags)&(FD_MATCH_BE_GREEDY))
    return get_longest_match(match_points);
  else return match_points;
}
static u8_byteoff ishspace_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (u8_ishspace(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

/* Vertical space matching */

static fdtype isvspace_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if (u8_isvspace(ch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype isvspace_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  const u8_byte *scan=string+off, *limit=string+lim, *last=scan;
  u8_unichar ch=u8_sgetc(&scan);
  while (u8_isvspace(ch)) 
    if (scan > limit) break;
    else {
      last=scan;
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(last-string);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(last-string));}
      ch=u8_sgetc(&scan);}
  if (last == string) return FD_EMPTY_CHOICE;
  else if ((flags)&(FD_MATCH_BE_GREEDY))
    return get_longest_match(match_points);
  else return match_points;
}
static u8_byteoff isvspace_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (u8_isvspace(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

/* Other matchers */

static fdtype isalnum_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if (u8_isalnum(ch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype isalnum_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  u8_unichar ch=string_ref(string+off);
  if (u8_isalnum(ch)) {
    while (u8_isalnum(ch)) {
      off=forward_char(string,off);
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(off);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(off));}
      ch=string_ref(string+off);}
    if ((flags)&(FD_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff isalnum_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (u8_isalnum(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

static fdtype isword_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if ((u8_isalpha(ch)) || (ch == '-') || (ch == '_'))
    return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype isword_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  u8_unichar ch=string_ref(string+off);
  if ((u8_isalpha(ch)) || (ch == '-') || (ch == '_')) {
    while ((u8_isalpha(ch)) || (ch == '-') || (ch == '_')) {
      off=forward_char(string,off);
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(off);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(off));}
      ch=string_ref(string+off);}
    if ((flags)&(FD_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff isword_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if ((u8_isalpha(ch)) || (ch == '-') || (ch == '_')) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

/* Digit matching */

static fdtype isdigit_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if (u8_isdigit(ch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype isdigit_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  u8_unichar ch=string_ref(string+off);
  if (u8_isdigit(ch)) {
    while (u8_isdigit(ch)) {
      off=forward_char(string,off);
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(off);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(off));}
      ch=string_ref(string+off);}
    if ((flags)&(FD_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff isdigit_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (u8_isdigit(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

static fdtype isxdigit_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if (u8_isxdigit(ch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype isxdigit_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  u8_unichar ch=string_ref(string+off);
  if (u8_isxdigit(ch)) {
    while (u8_isxdigit(ch)) {
      off=forward_char(string,off);
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(off);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(off));}
      ch=string_ref(string+off);}
    if ((flags)&(FD_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff isxdigit_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (u8_isxdigit(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

static fdtype isodigit_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if (u8_isodigit(ch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype isodigit_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  u8_unichar ch=string_ref(string+off);
  if (u8_isodigit(ch)) {
    while (u8_isodigit(ch)) {
      off=forward_char(string,off);
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(off);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(off));}
      ch=string_ref(string+off);}
    if ((flags)&(FD_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff isodigit_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (u8_isodigit(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

static fdtype isalpha_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if (u8_isalpha(ch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype isalpha_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  u8_unichar ch=string_ref(string+off);
  if (u8_isalpha(ch)) {
    while ((u8_isalpha(ch)) && (off<lim)) {
      off=forward_char(string,off);
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(off);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(off));}
      ch=string_ref(string+off);}
    if ((flags)&(FD_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff isalpha_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (u8_isalpha(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

static fdtype ispunct_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if (u8_ispunct(ch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype ispunct_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  u8_unichar ch=string_ref(string+off);
  if (u8_ispunct(ch)) {
    while ((u8_ispunct(ch)) && (off<lim)) {
      off=forward_char(string,off);
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(off);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(off));}
      ch=string_ref(string+off);}
    if ((flags)&(FD_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff ispunct_search
   (fdtype pat,fd_lispenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (u8_ispunct(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}


static fdtype isprint_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if (u8_isprint(ch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype isprint_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  u8_unichar ch=string_ref(string+off);
  if (u8_isprint(ch)) {
    while (u8_isprint(ch)) {
      off=forward_char(string,off);
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(off);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(off));}
      ch=string_ref(string+off);}
    if ((flags)&(FD_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff isprint_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (u8_isprint(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

static fdtype iscntrl_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if (u8_isctrl(ch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype iscntrl_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  u8_unichar ch=string_ref(string+off);
  if (u8_isctrl(ch)) {
    while ((u8_isctrl(ch)) && (off<lim)) {
      off=forward_char(string,off);
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(off);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(off));}
      ch=string_ref(string+off);}
    if ((flags)&(FD_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff iscntrl_search
   (fdtype pat,fd_lispenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (u8_isctrl(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

/* ISLOWER methods */

static fdtype islower_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if (u8_islower(ch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype islower_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  u8_unichar ch=string_ref(string+off);
  if (u8_islower(ch)) {
    while ((u8_islower(ch)) && (off<lim)) {
      off=forward_char(string,off);
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(off);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(off));}
      ch=string_ref(string+off);}
    if ((flags)&(FD_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff islower_search
   (fdtype pat,fd_lispenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (u8_islower(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

/* ISNOTLOWER methods */

#define isnotlower(c) ((u8_isprint(c))&&(!(u8_islower(c))))

static fdtype isnotlower_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if (isnotlower(ch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype isnotlower_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  u8_unichar ch=string_ref(string+off);
  if (isnotlower(ch)) {
    while ((isnotlower(ch)) && (off<lim)) {
      off=forward_char(string,off);
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(off);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(off));}
      ch=string_ref(string+off);}
    if ((flags)&(FD_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff isnotlower_search
   (fdtype pat,fd_lispenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (isnotlower(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

/* ISUPPER methods */

static fdtype isupper_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if (u8_isupper(ch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype isupper_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  u8_unichar ch=string_ref(string+off);
  if (u8_isupper(ch)) {
    while ((u8_isupper(ch)) && (off<lim)) {
      off=forward_char(string,off);
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(off);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(off));}
      ch=string_ref(string+off);}
    if ((flags)&(FD_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff isupper_search
   (fdtype pat,fd_lispenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (u8_isupper(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

/* ISNOTUPPER methods */

#define isnotupper(c) ((u8_isprint(c))&&(!(u8_isupper(c))))

static fdtype isnotupper_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch=string_ref(string+off);
  if (isnotupper(ch)) return FD_INT(forward_char(string,off));
  else return FD_EMPTY_CHOICE;
}
static fdtype isnotupper_plus_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype match_points=FD_EMPTY_CHOICE;
  u8_unichar ch=string_ref(string+off);
  if (isnotupper(ch)) {
    while ((isnotupper(ch)) && (off<lim)) {
      off=forward_char(string,off);
      if (flags&FD_MATCH_BE_GREEDY)
        match_points=FD_INT(off);
      else {FD_ADD_TO_CHOICE(match_points,FD_INT(off));}
      ch=string_ref(string+off);}
    if ((flags)&(FD_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff isnotupper_search
   (fdtype pat,fd_lispenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    u8_unichar ch=string_ref(s);
    if (isnotupper(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

/* Matching compound words */

static fdtype compound_word_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype embpunc=fd_get_arg(pat,1);
  const u8_byte *embstr=
    ((FD_STRINGP(embpunc) ? (FD_STRDATA(embpunc)) : ((const u8_byte *)",-/.")));
  const u8_byte *scan=string+off, *limit=string+lim, *end;
  u8_unichar ch=u8_sgetc(&scan);
  if (u8_isalnum(ch)) {
    end=scan;
    while (scan < limit) {
      while (u8_isalnum(ch)) {end=scan; ch=u8_sgetc(&scan);}
      if (strchr(embstr,ch)) {
        ch=u8_sgetc(&scan);
        if (u8_isalnum(ch)) continue;
        else return FD_INT(end-string);}
      else return FD_INT(end-string);}
    return FD_INT(end-string);}
  else return FD_EMPTY_CHOICE;
}

/* Lisp Symbols */

#define islsym(c)                                                  \
   (!((u8_isspace(c)) || (c == '"') || (c == '(') || (c == '{') || \
      (c == '[') || (c == ')') || (c == '}') || (c == ']')))

static int lsymbol_startp(u8_string string,u8_byteoff off)
{
  u8_unichar ch=get_previous_char(string,off);
  if (ch < 0) return 1;
  else if (!(islsym(ch))) return 1;
  else return 0;
}

static fdtype islsym_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_byteoff i=off;
  if (lsymbol_startp(string,off) == 0) return FD_EMPTY_CHOICE;
  while (i < lim) {
    u8_unichar ch=string_ref(string+i);
    if (islsym(ch)) i=forward_char(string,i);
    else break;}
  if (i > off) return FD_INT(i);
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff islsym_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *limit=string+lim;
  int good_start=lsymbol_startp(string,off);
  while (scan < limit) {
    const u8_byte *prev=scan; u8_unichar ch=u8_sgetc(&scan);
    if ((good_start) && (islsym(ch))) return prev-string;
    if (islsym(ch)) good_start=0; else good_start=1;}
  return -1;
}

/* C Symbols */

static u8_byteoff csymbol_startp(u8_string string,u8_byteoff off)
{
  u8_unichar ch=get_previous_char(string,off);
  if (ch < 0) return 1;
  else if ((u8_isalnum(ch)) || (ch == '_')) return 0;
  else return 1;
}

static fdtype iscsym_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_byteoff i=off; u8_unichar ch=string_ref(string+off);
  if (csymbol_startp(string,off) == 0) return FD_EMPTY_CHOICE;
  if ((u8_isalpha(ch)) || (ch == '_'))
    while (i < lim) {
      u8_unichar ch=string_ref(string+i);
      if ((u8_isalnum(ch)) || (ch == '_')) i=forward_char(string,i);
      else break;}
  if (i > off) return FD_INT(i);
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff iscsym_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *limit=string+lim;
  int good_start=csymbol_startp(string,off);
  while (scan < limit) {
    const u8_byte *prev=scan; u8_unichar ch=u8_sgetc(&scan);
    if ((good_start) && (u8_isalpha(ch))) return prev-string;
    if (u8_isalnum(ch) || (ch == '_'))
      good_start=0; else good_start=1;}
  return -1;
}

/* C Symbols */

#define ispathelt(ch) ((u8_isalnum(ch)) || (ch == '_') || (ch == '-') || (ch == '.'))

static fdtype ispathelt_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_byteoff i=off; u8_unichar ch=string_ref(string+off);
  if (ispathelt(ch))
    while (i < lim) {
      u8_unichar ch=string_ref(string+i);
      if (ispathelt(ch)) i=forward_char(string,i);
      else break;}
  if (i > off) return FD_INT(i);
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff ispathelt_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *oscan=scan, *limit=string+lim;
  int ch=u8_sgetc(&scan);
  while (scan<limit) {
    if (ispathelt(ch)) return oscan-string;
    oscan=scan; ch=u8_sgetc(&scan);}
  return -1;
}

/* Mail Identifiers */

#define isprinting(x) ((u8_isalnum(x)) || (u8_ispunct(x)))

#define is_not_mailidp(c) \
  (((c<128) && (!(isprinting(c)))) || \
   (u8_isspace(c)) || (c == '<') || \
   (c == ',') || (c == '(') || (c == '>') || (c == '<'))

static fdtype ismailid_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim; u8_byteoff atsign=0;
  while (s < sl) 
    if (*s == '@') {atsign=s-string; s++;}
    else {
      u8_unichar ch=string_ref(s);
      if (*s < 0x80) s++;
      else s=u8_substring(s,1);
      if (is_not_mailidp(ch)) break;}
  if ((atsign) && (!(s == string+atsign+1)))
    if (s == sl) return FD_INT((s-string)-1);
    else return FD_INT((s-string));
  else return FD_EMPTY_CHOICE;
}

#define ismailid(x) ((x<128) && ((isalnum(x)) || (strchr(".-_",x) != NULL)))

static u8_byteoff ismailid_search
   (fdtype pat,fd_lispenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *start=string+off, *slim=string+lim;
  const u8_byte *atsign=strchr(start,'@');
  while ((atsign) && (atsign < slim)) {
    if (atsign == start) start=atsign+1;
    else if (!(ismailid(atsign[0]))) start=atsign+1;
    else {
      const u8_byte *s=atsign-1; fdtype match;
      while (s > start) 
        if (!(ismailid(*s))) break; else s--;
      if (s != start) s++;
      match=ismailid_match(pat,FD_VOID,NULL,string,s-string,lim,flags);
      if (!(FD_EMPTY_CHOICEP(match))) {
        fd_decref(match); return s-string;}
      else start=atsign+1;}
    atsign=strchr(start,'@');}
  return -1;
}

/* XML matching */

#define xmlnmstartcharp(x)                                       \
  ((x<0x80) ? ((isalpha(x))||(x==':')||(x=='_')) :               \
   (((x>=0xc0) && (x<=0x2FF) && (x!=0xD7) && (x!=0xF7)) ||       \
    ((x>=0x370) && (x<=0x1FFF) && (x!=0x37E)) ||                 \
    ((x>0x2000) && /* For dump compilers */                      \
     ((x==0x200C) || (x==0x200D) ||                              \
      ((x>=0x2070) && (x<=0x218F)) ||                            \
      ((x>=0x2C00) && (x<=0x2FEF)) ||                            \
      ((x>=0x3001) && (x<=0xD7FF)) ||                            \
      ((x>=0xF900) && (x<=0xFDCF)) ||                            \
      ((x>=0xFDF0) && (x<=0xFDCF)) ||                            \
      ((x>=0x10000) && (x<=0xEFFFF))))))

#define xmlnmcharp(x)                                            \
  ((x<0x80) ?                                                    \
   ((isalpha(x))||(x==':')||(x=='_')||(x=='-')||(x=='.')) :      \
   (((x>=0xc0) && (x<=0x2FF) && (x!=0xD7) && (x!=0xF7)) ||       \
    (x==0xB7) || ((x>=0x0300) && (x<=0x36F)) ||                  \
    ((x>=0x370) && (x<=0x1FFF) && (x!=0x37E)) ||                 \
    ((x>0x2000) && /* For dump compilers */                      \
     ((x==0x200C) || (x==0x200D) || (x==0x203F) || (x==0x2040)|| \
      ((x>=0x2070) && (x<=0x218F)) ||                            \
      ((x>=0x2C00) && (x<=0x2FEF)) ||                            \
      ((x>=0x3001) && (x<=0xD7FF)) ||                            \
      ((x>=0xF900) && (x<=0xFDCF)) ||                            \
      ((x>=0xFDF0) && (x<=0xFDCF)) ||                            \
      ((x>=0x10000) && (x<=0xEFFFF))))))

static fdtype xmlname_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *oscan=scan, *limit=string+lim;
  int ch=u8_sgetc(&scan);
  if (!(xmlnmstartcharp(ch))) return FD_EMPTY_CHOICE;
  else while ((scan<limit) && (xmlnmcharp(ch))) {
      oscan=scan; ch=u8_sgetc(&scan);}
  if (xmlnmcharp(ch)) return FD_INT(scan-string);
  else return FD_INT(oscan-string);
}
static u8_byteoff xmlname_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *oscan=scan, *limit=string+lim;
  int ch=u8_sgetc(&scan);
  while (scan<limit) {
    if (xmlnmstartcharp(ch)) break;
    oscan=scan; ch=u8_sgetc(&scan);}
  if (xmlnmstartcharp(ch)) return oscan-string;
  else return -1;
}

static fdtype xmlnmtoken_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *oscan=scan, *limit=string+lim;
  int ch=u8_sgetc(&scan);
  if (!(xmlnmcharp(ch))) return FD_EMPTY_CHOICE;
  else while ((scan<limit) && (xmlnmcharp(ch))) {
      oscan=scan; ch=u8_sgetc(&scan);}
  if (xmlnmcharp(ch)) return FD_INT(scan-string);
  else return FD_INT(oscan-string);
}
static u8_byteoff xmlnmtoken_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *oscan=scan, *limit=string+lim;
  int ch=u8_sgetc(&scan);
  while (scan<limit) {
    if (xmlnmcharp(ch)) break;
    oscan=scan; ch=u8_sgetc(&scan);}
  if (xmlnmcharp(ch))
    if (scan<limit) return FD_INT(scan-string);
    else if (oscan<limit) return FD_INT(oscan-string);
    else return FD_EMPTY_CHOICE;
  else return FD_EMPTY_CHOICE;
}

#define htmlidcharp(ch) \
  ((isalnum(ch)) || (ch=='_') || (ch=='.') || (ch==':') || (ch=='-'))
static fdtype htmlid_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *oscan=scan, *limit=string+lim;
  int ch=u8_sgetc(&scan);
  if ((ch>0x80) || (!(isalpha(ch)))) return FD_EMPTY_CHOICE;
  else while ((scan<limit) && (ch<0x80) && (htmlidcharp(ch))) {
      oscan=scan; ch=u8_sgetc(&scan);}
  if (htmlidcharp(ch)) return FD_INT(scan-string);
  else return FD_INT(oscan-string);
}
static u8_byteoff htmlid_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *oscan=scan, *limit=string+lim;
  int ch=u8_sgetc(&scan);
  while (scan<limit)  {
    if ((ch<0x80) && (isalpha(ch))) break;
    oscan=scan; ch=u8_sgetc(&scan);}
  if ((ch<0x80) && (isalpha(ch)))
    if (scan<limit) return FD_INT(scan-string);
    else if (oscan<limit) return FD_INT(oscan-string);
    else return FD_EMPTY_CHOICE;
  else return FD_EMPTY_CHOICE;
}

/* Word matching */

static fdtype aword_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *slim=string+lim, *last=scan;
  u8_unichar ch=u8_sgetc(&scan), lastch=-1;
  int allupper=u8_isupper(ch), dotcount=0;
  if (word_startp(string,off) == 0) return FD_EMPTY_CHOICE;
  while ((scan<=slim) &&
         ((u8_isalpha(ch)) || (apostrophep(ch)) || (dashp(ch)) ||
          ((allupper)&&(ch=='.')))) {
    const u8_byte *prev=scan; u8_unichar nextch=u8_sgetc(&scan);
    if (u8_islower(nextch)) allupper=0;
    if (nextch=='.') dotcount++;
    if ((!(u8_isalpha(ch)))&&(!(u8_isalpha(nextch)))) {
      if ((nextch=='.')||(apostrophep(nextch))) last=prev;
      break;}
    else {lastch=ch; ch=nextch; last=prev;}}
  if (last > string+off)
    if (lastch=='.')
      if ((allupper)&&(dotcount>1))
        return FD_INT(last-string);
      else return FD_INT((last-1)-string);
    else return FD_INT(last-string);
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff aword_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *limit=string+lim;
  int good_start=word_startp(string,off);
  while (scan < limit) {
    const u8_byte *prev=scan; u8_unichar ch=u8_sgetc(&scan);
    if ((good_start) && (u8_isalpha(ch))) return prev-string;
    if ((u8_isspace(ch))||((u8_ispunct(ch))&&(strchr("-./_",ch)==NULL)))
      good_start=1; else good_start=0;}
  return -1;
}

static fdtype lword_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *slim=string+lim, *last=scan;
  u8_unichar ch=u8_sgetc(&scan);
  if (word_startp(string,off) == 0) return FD_EMPTY_CHOICE;
  if (!(u8_islower(ch))) return FD_EMPTY_CHOICE;
  while ((scan<=slim) && ((u8_isalpha(ch)) || (apostrophep(ch)) || (dashp(ch)))) {
    u8_unichar nextch; const u8_byte *prev=scan; nextch=u8_sgetc(&scan);
    if ((!(u8_isalpha(ch)))&&(!(u8_isalpha(nextch)))) {
      if (apostrophep(ch)) last=prev;
      break;}
    else {ch=nextch; last=prev;}}
  if (last > string+off) return FD_INT(last-string);
  else return FD_EMPTY_CHOICE;
}
static u8_byteoff lword_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *limit=string+lim;
  int good_start=word_startp(string,off);
  while (scan < limit) {
    const u8_byte *prev=scan; u8_unichar ch=u8_sgetc(&scan);
    if ((good_start) && (u8_islower(ch))) return prev-string;
    if ((u8_isspace(ch))||
        ((u8_ispunct(ch))&&
         (strchr("-./_'",ch)==NULL)&&
         (!(apostrophep(ch)))&&(!(dashp(ch)))))
      good_start=1; else good_start=0;}
  return -1;
}

static fdtype capword_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype matches=FD_EMPTY_CHOICE;
  const u8_byte *scan=string+off, *last=scan, *slim=string+lim;
  u8_unichar ch=u8_sgetc(&scan), lastch=-1;
  int greedy=((flags)&(FD_MATCH_BE_GREEDY)), allupper=1, dotcount=0;
  if (word_startp(string,off) == 0) return FD_EMPTY_CHOICE;
  if (!(u8_isupper(ch))) return matches;
  while ((scan<=slim) &&
         ((u8_isalpha(ch)) || (apostrophep(ch)) || (dashp(ch)) ||
          ((allupper)&&(ch=='.')))) {
    const u8_byte *prev=scan; u8_unichar nextch=u8_sgetc(&scan); 
    /* Handle embedded dots */
    if (u8_islower(nextch)) allupper=0;
    if (nextch=='.') dotcount++;
    if ((!(greedy))&&(dashp(ch))) {
      FD_ADD_TO_CHOICE(matches,FD_INT(prev-string));}
    if ((!(u8_isalpha(ch)))&&(!(u8_isalpha(nextch)))) {
      if ((nextch=='.')||(apostrophep(nextch))) last=prev;
      break;}
    else {lastch=ch; ch=nextch; last=prev;}}
  if (last > string+off) {
    if (lastch=='.')
      if ((allupper)&&(dotcount>1)) {
        FD_ADD_TO_CHOICE(matches,FD_INT(last-string));}
      else {FD_ADD_TO_CHOICE(matches,FD_INT((last-1)-string));}
    else {
      FD_ADD_TO_CHOICE(matches,FD_INT(last-string));}}
  if (greedy)
    return get_longest_match(matches);
  else return matches;
}

static u8_byteoff capword_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan=string+off, *limit=string+lim;
  int good_start=word_startp(string,off);
  while (scan < limit) {
    const u8_byte *prev=scan; u8_unichar ch=u8_sgetc(&scan);
    if ((good_start) && (u8_isupper(ch))) return prev-string;
    if ((u8_isspace(ch))||((u8_ispunct(ch))&&(strchr("-./_",ch)==NULL)))
        good_start=1; else good_start=0;}
  return -1;
}

#define isoctdigit(x) ((x<128) && (isdigit(x)) && (x < '8'))

static fdtype anumber_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype base_arg=fd_get_arg(pat,1);
  fdtype sep_arg=fd_get_arg(pat,2);
  int base=((FD_FIXNUMP(base_arg)) ? (FD_FIX2INT(base_arg)) : (10));
  u8_string sepchars=
    ((FD_VOIDP(sep_arg)) ? ((u8_string)".,") :
     (FD_STRINGP(sep_arg)) ? (FD_STRDATA(sep_arg)) : ((u8_string)(NULL)));
  const u8_byte *scan=string+off, *slim=string+lim, *last=scan;
  u8_unichar prev_char=get_previous_char(string,off), ch=u8_sgetc(&scan);
  if (u8_isdigit(prev_char)) return FD_EMPTY_CHOICE;
  if (base == 8)
    while ((scan<=slim) && (ch<0x80) && (isoctdigit(ch))) {
      last=scan; ch=u8_sgetc(&scan);}
  else if (base == 16)
    while ((scan<=slim) && (ch<0x80) && (isxdigit(ch))) {
      last=scan; ch=u8_sgetc(&scan);}
  else if (base == 2)
    while ((scan<=slim) && ((ch == '0') || (ch == '1'))) {
      last=scan; ch=u8_sgetc(&scan);}
  else while ((scan<=slim) && (ch<0x80) &&
              ((u8_isdigit(ch)) || ((sepchars)&&(strchr(sepchars,ch))))) {
    last=scan; ch=u8_sgetc(&scan);}
  if (last > string+off) return FD_INT(last-string);
  else return FD_EMPTY_CHOICE;
}
static int check_digit(u8_unichar ch,int base)
{
  if (ch>0x80) return 0;
  else if (base==10) return isdigit(ch);
  else if (base==8) return isoctdigit(ch);
  else if (base==16) return isxdigit(ch);
  else if (base==2) return ((ch=='1') || (ch=='0'));
  else return 0;
}

static u8_byteoff anumber_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype base_arg=fd_get_arg(pat,1);
  int base=((FD_FIXNUMP(base_arg)) ? (FD_FIX2INT(base_arg)) : (10));
  const u8_byte *scan=string+off, *limit=string+lim;
  while (scan < limit) {
    const u8_byte *prev=scan; u8_unichar ch=u8_sgetc(&scan);
    if (check_digit(ch,base)) return prev-string;}
  return -1;
}

/* Hashset matches */

static fd_hashset to_hashset(fdtype arg)
{
  if (FD_PTR_TYPEP(arg,fd_hashset_type)) 
    return (fd_hashset)arg;
  else return NULL;
}

static fdtype hashset_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype hs=fd_get_arg(pat,1);
  fdtype cpat=fd_get_arg(pat,2);
  fdtype xform=fd_get_arg(pat,3);
  if ((FD_VOIDP(hs)) || (FD_VOIDP(cpat)))
    return fd_err(fd_MatchSyntaxError,"hashset_match",NULL,pat);
  else if (!(FD_PTR_TYPEP(hs,fd_hashset_type)))
    return fd_type_error(_("hashset"),"hashset_match",pat);
  if (FD_VOIDP(xform)) {
    fd_hashset h=to_hashset(hs);
    fdtype iresults=fd_text_domatch(cpat,next,env,string,off,lim,flags);
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(possibility,iresults)
      if (hashset_strget(h,string+off,fd_getint(possibility)-off)) {
        fd_incref(possibility);
        FD_ADD_TO_CHOICE(results,possibility);}
    fd_decref(iresults);
    return get_longest_match(results);}
  else {
    fd_hashset h=to_hashset(hs);
    fdtype iresults=fd_text_domatch(cpat,next,env,string,off,lim,flags);
    fdtype results=FD_EMPTY_CHOICE;
    {FD_DO_CHOICES(possibility,iresults) {
        fdtype origin=fd_extract_string
          (NULL,string+off,string+fd_getint(possibility));
        fdtype xformed=match_apply(xform,"HASHSET-MATCH",env,1,&origin);
        if (fd_hashset_get(h,xformed)) {
          fd_incref(possibility); FD_ADD_TO_CHOICE(results,possibility);}
        fd_decref(xformed); fd_decref(origin);}}
    fd_decref(iresults);
    return get_longest_match(results);}
}

static u8_byteoff hashset_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype hs=fd_get_arg(pat,1);
  fdtype cpat=fd_get_arg(pat,2);
  if ((FD_VOIDP(hs)) || (FD_VOIDP(cpat)))
    return fd_err(fd_MatchSyntaxError,"hashset_search",NULL,pat);
  else if (!(FD_PTR_TYPEP(hs,fd_hashset_type)))
    return fd_type_error(_("hashset"),"hashset_search",pat);
  else {
    u8_byteoff try=fd_text_search(cpat,env,string,off,lim,flags);
    while ((try >= 0) && (try < lim)) {
      fdtype matches=hashset_match(pat,FD_VOID,env,string,try,lim,flags);
      if (!(FD_EMPTY_CHOICEP(matches))) {fd_decref(matches); return try;}
      else try=fd_text_search(cpat,env,string,
                              forward_char(string,try),lim,flags);}
    return try;}
}
  
/* HASHSET-NOT */

static fdtype hashset_not_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype hs=fd_get_arg(pat,1);
  fdtype cpat=fd_get_arg(pat,2);
  fdtype xform=fd_get_arg(pat,3);
  if ((FD_VOIDP(hs)) || (FD_VOIDP(cpat)))
    return fd_err(fd_MatchSyntaxError,"hashset_not_match",NULL,pat);
  else if (!(FD_PTR_TYPEP(hs,fd_hashset_type)))
    return fd_type_error(_("hashset"),"hashset_not_match",pat);
  if (FD_VOIDP(xform)) {
    fd_hashset h=to_hashset(hs);
    fdtype iresults=fd_text_domatch(cpat,next,env,string,off,lim,flags);
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(possibility,iresults)
      if (hashset_strget(h,string+off,fd_getint(possibility)-off)) {}
      else {fd_incref(possibility); FD_ADD_TO_CHOICE(results,possibility);}
    fd_decref(iresults);
    return get_longest_match(results);}
  else {
    fd_hashset h=to_hashset(hs);
    fdtype iresults=fd_text_domatch(cpat,next,env,string,off,lim,flags);
    fdtype results=FD_EMPTY_CHOICE;
    {FD_DO_CHOICES(possibility,iresults) {
        fdtype origin=fd_extract_string
          (NULL,string+off,string+fd_getint(possibility));
        fdtype xformed=match_apply(xform,"HASHSET-NOT-MATCH",env,1,&origin);
        if (FD_ABORTP(xformed)) {
          FD_STOP_DO_CHOICES;
          fd_decref(results);
          return xformed;}
        else if ((!(FD_STRINGP(xformed)))||(!(fd_hashset_get(h,xformed)))) {
          fd_incref(possibility); FD_ADD_TO_CHOICE(results,possibility);}
        fd_decref(xformed); fd_decref(origin);}}
    fd_decref(iresults);
    return get_longest_match(results);}
}

static u8_byteoff hashset_not_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype hs=fd_get_arg(pat,1);
  fdtype cpat=fd_get_arg(pat,2);
  if ((FD_VOIDP(hs)) || (FD_VOIDP(cpat)))
    return fd_err(fd_MatchSyntaxError,"hashset_not_search",NULL,pat);
  else if (!(FD_PTR_TYPEP(hs,fd_hashset_type)))
    return fd_type_error(_("hashset"),"hashset_not_search",pat);
  else {
    u8_byteoff try=fd_text_search(cpat,env,string,off,lim,flags);
    while ((try >= 0) && (try < lim)) {
      fdtype matches=hashset_not_match(pat,FD_VOID,env,string,try,lim,flags);
      if (!(FD_EMPTY_CHOICEP(matches))) {fd_decref(matches); return try;}
      else try=
             fd_text_search(cpat,env,string,
                            forward_char(string,try),lim,flags);}
    return try;}
}

/* Proc matches */

static fdtype applytest_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype cpat=fd_get_arg(pat,1);
  fdtype proc=fd_get_arg(pat,2);
  if ((FD_VOIDP(proc)) || (FD_VOIDP(proc)))
    return fd_err(fd_MatchSyntaxError,"proc_match",NULL,cpat);
  else if (!(FD_APPLICABLEP(proc)))
    return fd_type_error(_("applicable"),"proc_match",proc);
  else {
    fdtype iresults=fd_text_domatch
      (cpat,next,env,string,off,lim,(flags&(~FD_MATCH_BE_GREEDY)));
    fdtype results=FD_EMPTY_CHOICE;
    {FD_DO_CHOICES(possibility,iresults) {
        fdtype substring=fd_extract_string
          (NULL,string+off,string+fd_getint(possibility));
        fdtype match=fd_apply(proc,1,&substring);
        if (!((FD_FALSEP(match))||(FD_EMPTY_CHOICEP(match))||(FD_VOIDP(match)))) {
          fd_incref(possibility); FD_ADD_TO_CHOICE(results,possibility);}
        fd_decref(substring); fd_decref(match);}}
    fd_decref(iresults);
    if (flags&FD_MATCH_BE_GREEDY)
      return get_longest_match(results);
    else return results;}
}

static u8_byteoff applytest_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype cpat=fd_get_arg(pat,1);
  fdtype proc=fd_get_arg(pat,2);
  if ((FD_VOIDP(proc)) || (FD_VOIDP(proc)))
    return fd_err(fd_MatchSyntaxError,"proc_match",NULL,cpat);
  else if (!(FD_APPLICABLEP(proc)))
    return fd_type_error(_("applicable"),"proc_match",proc);
  else {
    u8_byteoff try=fd_text_search(cpat,env,string,off,lim,flags);
    while ((try >= 0) && (try < lim)) {
      fdtype matches=applytest_match(pat,FD_VOID,env,string,try,lim,flags);
      if (!(FD_EMPTY_CHOICEP(matches))) {fd_decref(matches); return try;}
      else try=fd_text_search(cpat,env,string,
                              forward_char(string,try),lim,flags);}
    return try;}
}
  
/* MAXLEN matches */

static fdtype maxlen_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype cpat=fd_get_arg(pat,1);
  fdtype lim_arg=fd_get_arg(pat,2);
  if ((FD_VOIDP(lim_arg)) || (FD_VOIDP(cpat)))
    return fd_err(fd_MatchSyntaxError,"maxlen_match",NULL,pat);
  else if (!(FD_FIXNUMP(lim_arg)))
    return fd_type_error(_("fixnum"),"maxlen_match",pat);
  else {
    int maxlen=FD_FIX2INT(lim_arg);
    int maxbytes=u8_byteoffset(string+off,maxlen,lim);
    int newlim=((maxbytes<0) ? (lim) : (off+maxbytes));
    return fd_text_domatch(cpat,next,env,string,off,newlim,flags);}
}

static u8_byteoff maxlen_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype cpat=fd_get_arg(pat,1);
  fdtype lim_arg=fd_get_arg(pat,2);
  if ((FD_VOIDP(lim_arg)) || (FD_VOIDP(cpat)))
    return fd_err(fd_MatchSyntaxError,"maxlen_search",NULL,pat);
  else if (!(FD_FIXNUMP(lim_arg)))
    return fd_type_error(_("fixnum"),"maxlen_search",pat);
  else {
    u8_byteoff try=fd_text_search(cpat,env,string,off,lim,flags);
    while ((try >= 0) && (try < lim)) {
      fdtype matches=maxlen_match(pat,FD_VOID,env,string,try,lim,flags);
      if (!(FD_EMPTY_CHOICEP(matches))) {fd_decref(matches); return try;}
      else try=fd_text_search(cpat,env,string,
                              forward_char(string,try),
                              lim,flags);}
    return try;}
}

/* MINLEN matches */

static fdtype minlen_match
  (fdtype pat,fdtype next,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype cpat=fd_get_arg(pat,1);
  fdtype lim_arg=fd_get_arg(pat,2);
  fdtype inner_results=FD_EMPTY_CHOICE;
  int min_len=-1;
  if (FD_VOIDP(cpat))
    return fd_err(fd_MatchSyntaxError,"maxlen_match",NULL,pat);
  else if (FD_VOIDP(lim_arg))
    min_len=1;
  else if (!(FD_FIXNUMP(lim_arg)))
    return fd_type_error(_("fixnum"),"maxlen_match",pat);
  else {
    min_len=FD_FIX2INT(lim_arg);
    if (min_len<=0)
      return fd_type_error(_("positive fixnum"),"minlen_match",pat);}
  inner_results=fd_text_domatch(cpat,next,env,string,off,lim,flags);
  if (FD_ABORTP(inner_results)) return inner_results;
  else if (FD_EMPTY_CHOICEP(inner_results)) return inner_results;
  else if (FD_FIXNUMP(inner_results)) {
    if ((FD_FIX2INT(inner_results)-off)<min_len)
      return FD_EMPTY_CHOICE;
    else return inner_results;}
  else {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(r,inner_results) {
      if (FD_FIXNUMP(r)) {
        int rint=FD_FIX2INT(r); int diff=rint-off;
        if (diff>=min_len) {FD_ADD_TO_CHOICE(results,r);}}}
    fd_decref(inner_results);
    return results;}
}

static u8_byteoff minlen_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  fdtype cpat=fd_get_arg(pat,1);
  fdtype lim_arg=fd_get_arg(pat,2);
  if (FD_VOIDP(cpat))
    return fd_err(fd_MatchSyntaxError,"minlen_search",NULL,pat);
  else if (FD_VOIDP(lim_arg)) {}
  else if (!(FD_FIXNUMP(lim_arg)))
    return fd_type_error(_("fixnum"),"minlen_search",pat);
  else if ((FD_FIX2INT(lim_arg))<=0)
    return fd_type_error(_("positive fixnum"),"minlen_search",pat);
  {
    u8_byteoff try=fd_text_search(cpat,env,string,off,lim,flags);
    while ((try >= 0) && (try < lim)) {
      fdtype matches=minlen_match(pat,FD_VOID,env,string,try,lim,flags);
      if (!(FD_EMPTY_CHOICEP(matches))) {
        fd_decref(matches);
        return try;}
      else try=fd_text_search(cpat,env,string,
                              forward_char(string,try),
                              lim,flags);}
    return try;}
}

/** Search functions **/

static const u8_byte *strsearch
  (int flags,
   const u8_byte *pat,u8_byteoff patlen,
   u8_string string,u8_byteoff off,u8_byteoff lim)
{
  u8_byte buf[64];
  const u8_byte *scan=pat, *limit;
  int first_char=u8_sgetc(&scan), use_buf=0, c, cand;
  if ((flags&FD_MATCH_COLLAPSE_SPACES) &&
      ((flags&(FD_MATCH_IGNORE_CASE|FD_MATCH_IGNORE_DIACRITICS)) == 0)) {
    struct U8_OUTPUT os; scan=pat; c=u8_sgetc(&scan);
    U8_INIT_STATIC_OUTPUT_BUF(os,64,buf);
    while ((c>0) && (!(u8_isspace(c)))) {
      u8_putc(&os,c); c=u8_sgetc(&scan);}
    use_buf=1;}
  else first_char=reduce_char(first_char,flags);
  scan=string+off; limit=string+lim; cand=scan-string;
  while (scan < limit) {
    if (use_buf) {
      const u8_byte *next=strstr(scan,buf);
      if (next) {
        cand=next-string; 
        scan=next; 
        u8_sgetc(&scan);}
      else return NULL;}
    else {
      c=u8_sgetc(&scan);
      if (c<0) return NULL;
      c=reduce_char(c,flags);
      while ((c != first_char) && (scan < limit)) {
        cand=scan-string;
        c=u8_sgetc(&scan);
        if (c<0) return NULL;
        c=reduce_char(c,flags);}
      if (c != first_char) return NULL;}
    if (cand > lim) return NULL;
    else {
      u8_byteoff matchlen=
        strmatcher(flags,pat,patlen,string,cand,lim);
      if (matchlen>0) return string+cand;}}
  return NULL;
}    

static u8_byteoff slow_search
   (fdtype pat,fd_lispenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s=string+off, *sl=string+lim;
  while (s < sl) {
    fdtype result=fd_text_matcher(pat,env,string,s-string,lim,flags);
    if (!(FD_EMPTY_CHOICEP(result))) return s-string;
    else if (*s < 0x80) s++;
    else s=u8_substring(s,1);}
  return -1;
}

/* Top level functions */

FD_EXPORT
u8_byteoff fd_text_search
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off > lim) return -1;
  else if (FD_EMPTY_CHOICEP(pat)) return -1;
  else if (FD_STRINGP(pat)) {
    u8_byte c=string[lim]; const u8_byte *next;
    if (flags&(FD_MATCH_SPECIAL))
      next=strsearch(flags,FD_STRDATA(pat),FD_STRLEN(pat),
                     string,off,lim);
    else {
      next=strstr(string+off,FD_STRDATA(pat));
      if (next>=string+lim) next=NULL;}
    if ((next) && (next<string+lim))
      return next-string;
    else return -1;}
  else if (FD_VECTORP(pat)) {
    fdtype initial=FD_VECTOR_REF(pat,0);
    u8_byteoff start=fd_text_search(initial,env,string,off,lim,flags);
    while ((start >= 0) && (start < lim)) {
      fdtype m=fd_text_matcher(pat,env,string,start,lim,flags);
      if (FD_ABORTP(m)) {
        fd_interr(m);
        return -2;}
      if (!(FD_EMPTY_CHOICEP(m))) {
        fd_decref(m); return start;}
      else start=fd_text_search
             (initial,env,string,forward_char(string,start),lim,flags);}
    if (start==-2) return start;
    else if (start<lim) return start;
    else return -1;}
  else if (FD_CHARACTERP(pat)) {
    u8_unichar c=FD_CHAR2CODE(pat);
    if (c < 0x80) {
      u8_byte c=string[lim], *next;
      next=strchr(string+off,c);
      if (next>=string+lim) next=NULL;
      if (next) return next-string;
      else return -1;}
    else {
      const u8_byte *s=string+off, *sl=string+lim;
      while (s < sl) {
        u8_unichar ch=string_ref(s);
        if (ch == c) return s-string;
        else if (*s < 0x80) s++;
        else s=u8_substring(s,1);}
      return -1;}}
  else if ((FD_CHOICEP(pat)) || (FD_ACHOICEP(pat))) {
    int nlim=lim, loc=-1;
    FD_DO_CHOICES(epat,pat) {
      u8_byteoff nxt=fd_text_search(epat,env,string,off,lim,flags);
      if (nxt < 0) {
        if (nxt==-2) {
          FD_STOP_DO_CHOICES;
          return nxt;}}
      else if (nxt < nlim) {nlim=nxt; loc=nxt;}}
    return loc;}
  else if (FD_PAIRP(pat)) {
    fdtype head=FD_CAR(pat);
    struct FD_TEXTMATCH_OPERATOR
      *scan=match_operators, *limit=scan+n_match_operators;
    while (scan < limit)
      if (FD_EQ(scan->symbol,head)) break; else scan++; 
    if (scan < limit)
      if (scan->searcher)
        return scan->searcher(pat,env,string,off,lim,flags);
      else return slow_search(pat,env,string,off,lim,flags);
    else {
      fd_seterr(fd_MatchSyntaxError,"fd_text_search",NULL,fd_incref(pat));
      return -2;}}
  else if (FD_SYMBOLP(pat)) {
    fdtype vpat=match_eval(pat,env);
    if (FD_ABORTP(vpat)) {
      fd_interr(vpat);
      return -2;}
    else if (FD_VOIDP(vpat)) {
      u8_string name=FD_SYMBOL_NAME(pat);
      fd_seterr(fd_UnboundIdentifier,"fd_text_search",
                u8_strdup(name),fd_incref(pat));
      return -2;}
    else {
      u8_byteoff result=fd_text_search(vpat,env,string,off,lim,flags);
      fd_decref(vpat); return result;}}
  else if (FD_PTR_TYPEP(pat,fd_txclosure_type)) {
    struct FD_TXCLOSURE *txc=(fd_txclosure)(pat);
    return fd_text_search(txc->pattern,txc->env,string,off,lim,flags);}
  else if (FD_PTR_TYPEP(pat,fd_regex_type))  {
    int retval=fd_regex_op(rx_search,pat,string+off,lim-off,0);
    if (retval<0) return retval;
    else return retval;}
  else {
    fd_seterr(fd_MatchSyntaxError,"fd_text_search",NULL,fd_incref(pat));
    return -2;}
}

FD_EXPORT
int fd_text_match
  (fdtype pat,fd_lispenv env,
   u8_string string,u8_byteoff off,u8_byteoff len,int flags)
{
  fdtype extents=fd_text_matcher(pat,env,string,off,len,flags);
  if (FD_ABORTP(extents)) 
    return fd_interr(extents);
  else {
    int match=0;
    FD_DO_CHOICES(extent,extents)
      if (FD_FIXNUMP(extent))
        if (FD_FIX2INT(extent) == len) {
          match=1; FD_STOP_DO_CHOICES; break;}
    fd_decref(extents);
    return match;}
}

FD_EXPORT
fdtype fd_textclosure(fdtype expr,fd_lispenv env)
{
  struct FD_TXCLOSURE *txc=u8_alloc(struct FD_TXCLOSURE);
  FD_INIT_CONS(txc,fd_txclosure_type);
  txc->pattern=fd_incref(expr); txc->env=fd_copy_env(env);
  return (fdtype) txc;
}

static int unparse_txclosure(u8_output ss,fdtype x)
{
  struct FD_TXCLOSURE *txc=(fd_txclosure)x;
  u8_printf(ss,"#<TX-CLOSURE %q>",txc->pattern);
  return 1;
}

static void recycle_txclosure(FD_CONS *c)
{
  struct FD_TXCLOSURE *txc=(fd_txclosure)c;
  fd_decref(txc->pattern); fd_decref((fdtype)(txc->env));
  u8_free(txc);
}

/* Defining match symbols */

FD_EXPORT int fd_matchdef(fdtype symbol,fdtype value)
{
  return fd_store(match_env,symbol,value);
}

FD_EXPORT fdtype fd_matchget(fdtype symbol,fd_lispenv env)
{
  return match_eval(symbol,env);
}

/** Initialization **/

void fd_init_match_c()
{
  u8_register_source_file(_FILEINFO);

  match_env=fd_make_hashtable(NULL,1024);

  fd_txclosure_type=fd_register_cons_type("txclosure");
  fd_recyclers[fd_txclosure_type]=recycle_txclosure;
  fd_unparsers[fd_txclosure_type]=unparse_txclosure;

  init_match_operators_table();
  fd_add_match_operator("*",match_star,search_star,extract_star);
  fd_add_match_operator("+",match_plus,search_plus,extract_plus);
  fd_add_match_operator("OPT",match_opt,search_opt,extract_opt);
  fd_add_match_operator("NOT",match_not,search_not,NULL);
  fd_add_match_operator("AND",match_and,search_and,NULL);
  fd_add_match_operator("NOT>",match_not_gt,search_not,NULL);
  fd_add_match_operator("CHOICE",match_choice,search_choice,extract_choice);
  fd_add_match_operator("PREF",match_pref,search_pref,extract_pref);

  fd_add_match_operator("=",match_bind,search_bind,NULL);
  fd_add_match_operator("LABEL",label_match,label_search,label_extract);
  fd_add_match_operator("SUBST",subst_match,subst_search,subst_extract);

  fd_add_match_operator("GREEDY",match_greedy,search_greedy,extract_greedy);
  fd_add_match_operator
    ("EXPANSIVE",match_expansive,search_expansive,extract_expansive);
  fd_add_match_operator
    ("LONGEST",match_longest,search_longest,extract_longest);
  fd_add_match_operator("APPLYTEST",applytest_match,applytest_search,NULL);

  fd_add_match_operator("MATCH-CASE",match_cs,search_cs,extract_cs);
  fd_add_match_operator("MC",match_cs,search_cs,extract_cs);
  fd_add_match_operator("IGNORE-CASE",match_ci,search_ci,extract_ci);
  fd_add_match_operator("IC",match_ci,search_ci,extract_ci);
  fd_add_match_operator("MATCH-DIACRITICS",match_ds,search_ds,extract_ds);
  fd_add_match_operator("MD",match_ds,search_ds,extract_ds);
  fd_add_match_operator("IGNORE-DIACRITICS",match_di,search_di,extract_di);
  fd_add_match_operator("ID",match_di,search_di,extract_di);
  fd_add_match_operator("MATCH-SPACING",match_ss,search_ss,extract_ss);
  fd_add_match_operator("MS",match_ss,search_ss,extract_ss);
  fd_add_match_operator("IGNORE-SPACING",match_si,search_si,extract_si);
  fd_add_match_operator("IS",match_si,search_si,extract_si);
  fd_add_match_operator
    ("CANONICAL",match_canonical,search_canonical,extract_canonical);

  fd_add_match_operator("BOL",match_bol,search_bol,NULL);
  fd_add_match_operator("EOL",match_eol,search_eol,NULL);
  fd_add_match_operator("BOS",match_bos,search_bos,NULL);
  fd_add_match_operator("EOS",match_eos,search_eos,NULL);
  fd_add_match_operator("BOW",match_bow,search_bow,NULL);
  fd_add_match_operator("CHAR-RANGE",match_char_range,NULL,NULL);
  fd_add_match_operator("CHAR-NOT",match_char_not,NULL,NULL);
  fd_add_match_operator("CHAR-NOT*",match_char_not_star,NULL,NULL);
  fd_add_match_operator("ISVOWEL",isvowel_match,NULL,NULL);
  fd_add_match_operator("ISNOTVOWEL",isnotvowel_match,NULL,NULL);
  fd_add_match_operator("ISSPACE",isspace_match,isspace_search,NULL);
  fd_add_match_operator("ISSPACE+",isspace_plus_match,isspace_search,NULL);
  fd_add_match_operator("SPACES",spaces_match,spaces_search,NULL);
  fd_add_match_operator("SPACES*",spaces_star_match,spaces_star_search,NULL);
  fd_add_match_operator("HSPACE",ishspace_match,ishspace_search,NULL);
  fd_add_match_operator("HSPACE+",ishspace_plus_match,ishspace_search,NULL);
  fd_add_match_operator("VSPACE",isvspace_match,isvspace_search,NULL);
  fd_add_match_operator("VSPACE+",isvspace_plus_match,isvspace_search,NULL);
  fd_add_match_operator("ISALNUM",isalnum_match,isalnum_search,NULL);
  fd_add_match_operator("ISALNUM+",isalnum_plus_match,isalnum_search,NULL);
  fd_add_match_operator("ISWORD",isword_match,isword_search,NULL);
  fd_add_match_operator("ISWORD+",isword_plus_match,isword_search,NULL);
  fd_add_match_operator("ISALPHA",isalpha_match,isalpha_search,NULL);
  fd_add_match_operator("ISALPHA+",isalpha_plus_match,isalpha_search,NULL);
  fd_add_match_operator("ISDIGIT",isdigit_match,isdigit_search,NULL);
  fd_add_match_operator("ISDIGIT+",isdigit_plus_match,isdigit_search,NULL);
  fd_add_match_operator("ISPRINT",isprint_match,isprint_search,NULL);
  fd_add_match_operator("ISPRINT+",isprint_plus_match,isprint_search,NULL);
  fd_add_match_operator("ISXDIGIT",isxdigit_match,isxdigit_search,NULL);
  fd_add_match_operator("ISXDIGIT+",isxdigit_plus_match,isxdigit_search,NULL);
  fd_add_match_operator("ISODIGIT",isodigit_match,isodigit_search,NULL);
  fd_add_match_operator("ISODIGIT+",isodigit_plus_match,isodigit_search,NULL);
  fd_add_match_operator("ISPUNCT",ispunct_match,ispunct_search,NULL);
  fd_add_match_operator("ISPUNCT+",ispunct_plus_match,ispunct_search,NULL);
  fd_add_match_operator("ISCNTRL",iscntrl_match,iscntrl_search,NULL);
  fd_add_match_operator("ISCNTRL+",iscntrl_plus_match,iscntrl_search,NULL);
  fd_add_match_operator("ISUPPER",isupper_match,isupper_search,NULL);
  fd_add_match_operator("ISUPPER+",isupper_plus_match,isupper_search,NULL);
  fd_add_match_operator("ISLOWER",islower_match,islower_search,NULL);
  fd_add_match_operator("ISLOWER+",islower_plus_match,islower_search,NULL);
  fd_add_match_operator("ISNOTUPPER",isnotupper_match,isnotupper_search,NULL);
  fd_add_match_operator("ISNOTUPPER+",
                        isnotupper_plus_match,isnotupper_search,NULL);
  fd_add_match_operator("ISNOTLOWER",isnotlower_match,isnotlower_search,NULL);
  fd_add_match_operator("ISNOTLOWER+",
                        isnotlower_plus_match,isnotlower_search,NULL);
  fd_add_match_operator("LSYMBOL",islsym_match,islsym_search,NULL);
  fd_add_match_operator("CSYMBOL",iscsym_match,iscsym_search,NULL);
  fd_add_match_operator("PATHELT",ispathelt_match,ispathelt_search,NULL);
  fd_add_match_operator("XMLNAME",xmlname_match,xmlname_search,NULL);
  fd_add_match_operator("XMLNMTOKEN",xmlnmtoken_match,xmlnmtoken_search,NULL);
  fd_add_match_operator("HTMLID",htmlid_match,htmlid_search,NULL);
  fd_add_match_operator("AWORD",aword_match,aword_search,NULL);
  fd_add_match_operator("LWORD",lword_match,lword_search,NULL);
  fd_add_match_operator("ANUMBER",anumber_match,anumber_search,NULL);
  fd_add_match_operator("CAPWORD",capword_match,capword_search,NULL);
  fd_add_match_operator
    ("COMPOUND-WORD",compound_word_match,isalnum_search,NULL);
  fd_add_match_operator("MAILID",ismailid_match,ismailid_search,NULL);

  fd_add_match_operator("CHUNK",chunk_match,chunk_search,chunk_extract);

  /* Whitespace or punctuatation separated tokens */
  fd_add_match_operator("WORD",word_match,word_search,word_extract);
  fd_add_match_operator("PHRASE",word_match,word_search,word_extract);

  fd_add_match_operator("REST",match_rest,search_rest,extract_rest);
  fd_add_match_operator("HASHSET",hashset_match,hashset_search,NULL);
  fd_add_match_operator
    ("HASHSET-NOT",hashset_not_match,hashset_not_search,NULL);

  fd_add_match_operator("MAXLEN",maxlen_match,maxlen_search,NULL);
  fd_add_match_operator("MINLEN",minlen_match,minlen_search,NULL);

  label_symbol=fd_intern("LABEL");
  subst_symbol=fd_intern("SUBST");
  star_symbol=fd_intern("*");
  plus_symbol=fd_intern("+");
  opt_symbol=fd_intern("OPT");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
