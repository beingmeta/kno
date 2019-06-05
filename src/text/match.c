/* C Mode */

/* match.c
   Regular expression primitives
   Originally implemented by Ken Haase in the Machine Understanding Group
     at the MIT Media Laboratory.
   Extended exentsively by Ken Haase at beingmeta, inc.

   Copyright (C) 1999 Massachusetts Institute of Technology
   Copyright (C) 1999-2019 beingmeta,inc 

*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

/* MATCHER DOCUMENTATION

   The Kno text matcher provides a powerful and versatile text
   analysis facility designed to support natural readability and
   writablity and to enable and encourage the development of software
   and layered tools which are readily extensible and maintainable.

   TEXT PATTERNS

   Kno text patterns are compound LISP objects which describe textual
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
   
   Internally, the match and extract functions also receive a (possibly VOID)
    dtype pointer to the 'next pattern' used for easily handling wildcards 
    and remainder arguments.  Finally, a *flags* argument controls various aspects
    of the matching process and is normally passed to operations on subpatterns.

   STRING MATCHING

   All the matching operations pass a set of flags which control the
   matching process.  Three of these flags control string comparison
   and can be dynamically applied by corresponding matchops.  When
   passed as a *flags* argument to the matching procedures, these can
   be ORed together.
     KNO_MATCH_IGNORE_CASE ignore case differences in strings
         (IGNORE-CASE <pat>) (IC <pat>) ignores case when matching <pat>
         (MATCH-CASE <pat>) (MC <pat>) matches case when matching <pat>
     KNO_MATCH_IGNORE_DIACRITICS ignores diacritic modifications on characters
       This ignores Unicode modifier codepoints and also reduces collapsed
       characters to their base form (e.g. ä goes to a).
         (IGNORE-DIACRTICS <pat>) (ID <pat>) ignores diacritics for <pat>
         (MATCH-DIACRITICS <pat>) (MD <pat>) matches diacritics for <pat>
     KNO_MATCH_COLLAPSE_SPACES ignores differences in whitespace, e.g.
       "foo bar" matches "foo  bar", "foo       bar", and "foo
       bar".
         (IGNORE-SPACING <pat>) (IS <pat>)
            ignores whitespace variation for <pat>
         (MATCH-SPACING <pat>) (MS <pat>)
            matches whitespace exactly for <pat>

   MATCHER INTERNALS

   The matcher makes extensive use Kno's choice data structure
   to represent multiple parses internally and externally.  The basic
   match and extract functions return lisp objects (type lispval)
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
  as lisp FIXNUMs, with int values extracted by FIX2INT and recreated
  by KNO_INT. If there is only one result, the representation is a
  single fixnum.

  Search results, because they describe the *first* match of a pattern,
  are simple ints (or u8_byteoff which is the same) or -1 to indicate
  a failed search.
   
  Extract results are internally represented by lisp PAIRS whose CARs
  (KNO_CAR) contain the corresponding match length (a FIXNUM) and
  whose CDRs (KNO_CDR) contain transformed pattern expressions including
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

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/texttools.h"
#include "kno/knoregex.h"

#include <libu8/u8printf.h>
#include <libu8/u8ctype.h>

#include <ctype.h>

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

u8_condition kno_InternalMatchError=_("Internal match error");
u8_condition kno_MatchSyntaxError=_("match syntax error");
u8_condition kno_TXInvalidPattern=_("Not a valid TX text pattern");
u8_condition UnboundMatchSymbol=_("Unbound matcher symbol");
u8_condition BadMatcherMethod=_("Unknown matcher symbol");

kno_ptr_type kno_txclosure_type;

static lispval subst_symbol;
static lispval match_env;

#define string_ref(s) ((*(s) < 0x80) ? (*(s)) : (u8_string_ref(s)))

#define KNO_MATCH_SPECIAL \
   (KNO_MATCH_IGNORE_CASE|KNO_MATCH_IGNORE_DIACRITICS|KNO_MATCH_COLLAPSE_SPACES)

static lispval hashset_strget(kno_hashset h,u8_string s,u8_byteoff len)
{
  struct KNO_STRING sval;
  KNO_INIT_STATIC_CONS(&sval,kno_string_type);
  sval.str_bytelen = len; sval.str_bytes = s;
  return kno_hashset_get(h,(lispval)&sval);
}

static lispval match_eval(lispval symbol,kno_lexenv env)
{
  lispval value = ((env)?(kno_symeval(symbol,env)):(VOID));
  if (VOIDP(value))
    return kno_get(match_env,symbol,VOID);
  else return value;
}
static lispval match_apply(lispval method,u8_context cxt,kno_lexenv env,
                           int n,lispval *args)
{
  if (KNO_APPLICABLEP(method))
    return kno_apply(method,n,args);
  else if (SYMBOLP(method)) {
    lispval methfn = match_eval(method,env), result;
    if (KNO_APPLICABLEP(methfn))
      result = kno_apply(methfn,n,args);
    else return kno_err(BadMatcherMethod,cxt,SYM_NAME(method),method);
    kno_decref(methfn);
    return result;}
  else return kno_err(BadMatcherMethod,cxt,NULL,method);
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
  u8_string next = u8_substring(s+i,1);
  if (next) return next-s; else return i+1;
}

#define forward_char(s,i) \
  ((s[i] == 0) ? (i) : (s[i] >= 0x80) ? (_forward_char(s,i)) : (i+1))

static u8_unichar reduce_char(u8_unichar ch,int flags)
{
  if (flags&KNO_MATCH_IGNORE_DIACRITICS) {
    u8_unichar nch = u8_base_char(ch);
    if (nch>0) ch = nch;}
  if (flags&KNO_MATCH_IGNORE_CASE) ch = u8_tolower(ch);
  return ch;
}

static u8_unichar get_previous_char(u8_string string,u8_byteoff off)
{
  if (off == 0) return -1;
  else if (string[off-1] < 0x80) return string[off-1];
  else {
    u8_byteoff i = off-1, ch; const u8_byte *scan;
    while ((i>0) && (string[i]>=0x80) && (string[i]<0xC0)) i--;
    scan = string+i; ch = u8_sgetc(&scan);
    return ch;}
}

static u8_byteoff strmatcher
  (int flags,
   const u8_byte *pat,u8_byteoff patlen,
   u8_string string,u8_byteoff off,u8_byteoff lim)
{
  if ((flags&KNO_MATCH_SPECIAL) == 0)
    if (strncmp(pat,string+off,patlen) == 0) return off+patlen;
    else return -1;
  else {
    int di = (flags&KNO_MATCH_IGNORE_DIACRITICS),
      si = (flags&KNO_MATCH_COLLAPSE_SPACES);
    const u8_byte *s1 = pat, *s2 = string+off, *end = s2, *limit = string+lim;
    u8_unichar c1 = u8_sgetc(&s1), c2 = u8_sgetc(&s2);
    while ((c1>0) && (c2>0) && (s2 <= limit))
      if ((si) && (u8_isspace(c1)) && (u8_isspace(c2))) {
        while ((c1>0) && (u8_isspace(c1))) c1 = u8_sgetc(&s1);
        while ((c2>0) && (u8_isspace(c2))) {
          end = s2; c2 = u8_sgetc(&s2);}}
      else if ((di) && (u8_ismodifier(c1)) && (u8_ismodifier(c2))) {
        while ((c1>0) && (u8_ismodifier(c1))) c1 = u8_sgetc(&s1);
        while ((c2>0) && (u8_ismodifier(c2))) {
          end = s2; c2 = u8_sgetc(&s2);}}
      else if (c1 == c2) {
        c1 = u8_sgetc(&s1); end = s2; c2 = u8_sgetc(&s2);}
      else if (flags&(KNO_MATCH_IGNORE_CASE|KNO_MATCH_IGNORE_DIACRITICS))
        if (reduce_char(c1,flags) == reduce_char(c2,flags)) {
          c1 = u8_sgetc(&s1); end = s2; c2 = u8_sgetc(&s2);}
        else return -1;
      else return -1;
    if (c1 < 0) /* If at end of pat string, you have a match */
      return end-string;
    else return -1;}
}

/** Match operator table **/

static struct KNO_TEXTMATCH_OPERATOR *match_operators;

static int n_match_operators, limit_match_operators;

static void init_match_operators_table()
{
  match_operators = u8_alloc_n(16,struct KNO_TEXTMATCH_OPERATOR);
  n_match_operators = 0; limit_match_operators = 16;
}

KNO_EXPORT
void kno_add_match_operator
  (u8_string label,
   tx_matchfn matcher,tx_searchfn searcher,tx_extractfn extract)
{
  lispval sym = kno_getsym(label);
  struct KNO_TEXTMATCH_OPERATOR *scan = match_operators, *limit = scan+n_match_operators;
  while (scan < limit) if (KNO_EQ(scan->kno_matchop,sym)) break; else scan++;
  if (scan < limit) {scan->kno_matcher = matcher; return;}
  if (n_match_operators >= limit_match_operators) {
    match_operators = u8_realloc_n
      (match_operators,(limit_match_operators)*2,
       struct KNO_TEXTMATCH_OPERATOR);
    limit_match_operators = limit_match_operators*2;}
  match_operators[n_match_operators].kno_matchop = sym;
  match_operators[n_match_operators].kno_matcher = matcher;
  match_operators[n_match_operators].kno_searcher = searcher;
  match_operators[n_match_operators].kno_extractor = extract;
  n_match_operators++;
}

/* This is for greedy matching */
static lispval get_longest_match(lispval matches)
{
  if (KNO_ABORTED(matches)) return matches;
  else if ((CHOICEP(matches)) || (PRECHOICEP(matches))) {
    u8_byteoff max = -1;
    DO_CHOICES(match,matches) {
      u8_byteoff ival = kno_getint(match);
      if (ival>max) max = ival;}
    kno_decref(matches);
    if (max<0) return EMPTY;
    else return KNO_INT(max);}
  else return matches;
}


/** The Matcher **/

static lispval match_sequence
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);

KNO_EXPORT
lispval kno_text_domatch
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (KNO_INTERRUPTED())
    return KNO_ERROR;
  else if (off > lim) return EMPTY;
  else if (EMPTYP(pat)) return EMPTY;
  else if (STRINGP(pat))
    if (off == lim)
      if (STRLEN(pat) == 0) return KNO_INT(off);
      else return EMPTY;
    else {
      u8_byteoff mlen = strmatcher
        (flags,CSTRING(pat),STRLEN(pat),
         string,off,lim);
      if (mlen < 0) return EMPTY;
      else return KNO_INT(mlen);}
  else if ((CHOICEP(pat)) || (PRECHOICEP(pat))) {
    if (flags&(KNO_MATCH_BE_GREEDY)) {
      u8_byteoff max = -1;
      DO_CHOICES(each,pat) {
        lispval answer = kno_text_domatch(each,next,env,string,off,lim,flags);
        if (EMPTYP(answer)) {}
        else if (KNO_ABORTED(answer)) {
          KNO_STOP_DO_CHOICES;
          return answer;}
        else if (KNO_UINTP(answer)) {
          u8_byteoff val = FIX2INT(answer);
          if (val>max) max = val;}
        else if (CHOICEP(answer)) {
          DO_CHOICES(a,answer)
            if (KNO_UINTP(a)) {
              u8_byteoff val = FIX2INT(a);
              if (val>max) max = val;}
            else {
              lispval err = kno_err(kno_InternalMatchError,"kno_text_domatch",NULL,a);
              kno_decref(answer); answer = err;
              KNO_STOP_DO_CHOICES;
              break;}
          if (KNO_ABORTED(answer)) {
            KNO_STOP_DO_CHOICES; return answer;}
          else kno_decref(answer);}
        else {
          kno_decref(answer); KNO_STOP_DO_CHOICES;
          return kno_err(kno_InternalMatchError,"kno_text_domatch",NULL,each);}}
      if (max<0) return EMPTY; else return KNO_INT(max);}
    else {
      lispval answers = EMPTY;
      DO_CHOICES(epat,pat) {
        lispval answer = kno_text_domatch(epat,next,env,string,off,lim,flags);
        if (KNO_ABORTED(answer)) {
          KNO_STOP_DO_CHOICES;
          kno_decref(answers);
          return answer;}
        CHOICE_ADD(answers,answer);}
      return answers;}}
  else if (KNO_CHARACTERP(pat)) {
    if (off == lim) return EMPTY;
    else {
      u8_unichar code = KNO_CHAR2CODE(pat);
      if ((code < 0x7f)?(string[off] == code):
          (code == string_ref(string+off))) {
        int next = forward_char(string,off);
        return KNO_INT(next);}
      else return EMPTY;}}
  else if (VECTORP(pat))
    return match_sequence(pat,next,env,string,off,lim,flags);
  else if (PAIRP(pat)) {
    lispval head = KNO_CAR(pat), result;
    struct KNO_TEXTMATCH_OPERATOR
      *scan = match_operators, *limit = scan+n_match_operators;
    while (scan < limit)
      if (KNO_EQ(scan->kno_matchop,head)) break; else scan++; 
    if (scan < limit)
      result = scan->kno_matcher(pat,next,env,string,off,lim,flags);
    else return kno_err(kno_MatchSyntaxError,"kno_text_domatch",
                       _("unknown match operator"),pat);
    if ((CHOICEP(result)) && (flags&(KNO_MATCH_BE_GREEDY)))
      return get_longest_match(result);
    else return result;}
  else if (SYMBOLP(pat)) {
    lispval v = match_eval(pat,env);
    if (VOIDP(v))
      return kno_err(UnboundMatchSymbol,"kno_text_domatch",SYM_NAME(pat),pat);
    else {
      lispval result = kno_text_domatch(v,next,env,string,off,lim,flags);
      kno_decref(v); return result;}}
  else if (TYPEP(pat,kno_txclosure_type)) {
    struct KNO_TXCLOSURE *txc = (kno_txclosure)pat;
    return kno_text_matcher(txc->kno_txpattern,txc->kno_txenv,string,off,lim,flags);}
  else if (TYPEP(pat,kno_regex_type)) {
    int retval = kno_regex_op(rx_matchlen,pat,string+off,lim-off,0);
    if (retval<-1)
      return kno_err(kno_InternalMatchError,"kno_text_domatch",NULL,pat);
    else if ((retval>lim)||(retval<0)) return EMPTY;
    else return KNO_INT(retval+off);}
  else return kno_err(kno_MatchSyntaxError,"kno_text_domatch",NULL,pat);
}

KNO_EXPORT
lispval kno_text_matcher
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return kno_text_domatch(pat,VOID,env,string,off,lim,flags);
}

static lispval match_sequence
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  int i = 0, l = VEC_LEN(pat);
  lispval state = KNO_INT(off);
  while (i < l) {
    lispval epat = VEC_REF(pat,i);
    lispval npat = ((i+1==l)?(next):(VEC_REF(pat,i+1)));
    lispval next = EMPTY;
    DO_CHOICES(pos,state) {
      lispval npos=
        kno_text_domatch(epat,npat,env,string,kno_getint(pos),lim,flags);
      if (KNO_ABORTED(npos)) {
        kno_decref(next);
        KNO_STOP_DO_CHOICES;
        return npos;}
      CHOICE_ADD(next,npos);}
    kno_decref(state);
    if (EMPTYP(next)) 
      return EMPTY;
    else {state = next; i++;}}
  return state;
}

/** Extraction **/

KNO_FASTOP lispval textract
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
KNO_FASTOP lispval extract_sequence
  (lispval pat,int pat_elt,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags);
static lispval lists_to_vectors(lispval lists);

KNO_EXPORT
lispval kno_text_doextract
   (lispval pat,lispval next,kno_lexenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return textract(pat,next,env,string,off,lim,flags);
}

KNO_EXPORT
lispval kno_text_extract
   (lispval pat,kno_lexenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return textract(pat,VOID,env,string,off,lim,flags);
}

static lispval extract_text(u8_string string,u8_byteoff start,lispval ends)
{
  lispval answers = EMPTY;
  DO_CHOICES(each_end,ends)
    if (FIXNUMP(each_end)) {
      lispval extraction=
        kno_conspair(each_end,kno_extract_string
                    (NULL,string+start,string+kno_getint(each_end)));
      CHOICE_ADD(answers,extraction);}
  return answers;
}

static lispval get_longest_extractions(lispval extractions)
{
  if (KNO_ABORTED(extractions)) return extractions;
  else if ((CHOICEP(extractions)) || (PRECHOICEP(extractions))) {
    lispval largest = EMPTY; u8_byteoff max = -1;
    DO_CHOICES(extraction,extractions) {
      u8_byteoff ival = kno_getint(KNO_CAR(extraction));
      if (ival == max) {
        kno_incref(extraction);
        CHOICE_ADD(largest,extraction);}
      else if (ival<max) {}
      else {
        kno_decref(largest); largest = kno_incref(extraction);
        max = ival;}}
    kno_decref(extractions);
    return largest;}
  else return extractions;
}

static lispval textract
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (KNO_INTERRUPTED())
    return KNO_ERROR;
  else if (off > lim) return EMPTY;
  else if (EMPTYP(pat)) return EMPTY;
  else if (STRINGP(pat))
    if ((STRLEN(pat)) == 0) {
      return kno_conspair(KNO_INT(off),lispval_string(""));}
    else if (off == lim)
      return EMPTY;
    else {
      u8_byteoff mlen=
        strmatcher(flags,CSTRING(pat),STRLEN(pat),
                   string,off,lim);
      if (mlen<0) return EMPTY;
      else return extract_text(string,off,KNO_INT(mlen));}
  else if ((CHOICEP(pat)) || (QCHOICEP(pat)) || (PRECHOICEP(pat))) {
    lispval answers = EMPTY;
    DO_CHOICES(epat,pat) {
      lispval extractions = textract(epat,next,env,string,off,lim,flags);
      DO_CHOICES(extraction,extractions) {
        if (KNO_ABORTED(extraction)) {
          kno_decref(answers); answers = kno_incref(extraction);
          KNO_STOP_DO_CHOICES;
          break;}
        else if (PAIRP(extraction)) {
          kno_incref(extraction);
          CHOICE_ADD(answers,extraction);}
        else {
          kno_decref(answers);
          answers = kno_err(kno_InternalMatchError,"textract",NULL,extraction);
          KNO_STOP_DO_CHOICES;
          break;}}
      if (KNO_ABORTED(answers)) {
        kno_decref(extractions);
        return answers;}
      else {kno_decref(extractions);}}
    if ((flags&KNO_MATCH_BE_GREEDY) &&
        ((CHOICEP(answers)) || (PRECHOICEP(answers)))) {
      /* get_longest_extracts frees (decrefs) answers if it doesn't
         return them */
      lispval result = get_longest_extractions(answers);
      return result;}
    else return answers;}
  else if (KNO_CHARACTERP(pat)) {
    if (off == lim) return EMPTY;
    else {
      u8_unichar code = KNO_CHAR2CODE(pat);
      if ((code < 0x7f)?(string[off] == code):
          (code == string_ref(string+off))) {
        struct U8_OUTPUT str; u8_byte buf[16];
        int next = forward_char(string,off);
        U8_INIT_FIXED_OUTPUT(&str,16,buf);
        u8_putc(&str,code);
        return kno_conspair(KNO_INT(next),lispval_string(buf));}
      else return EMPTY;}}
  else if (VECTORP(pat)) {
    lispval seq_matches = extract_sequence
      (pat,0,next,env,string,off,lim,flags);
    if (KNO_ABORTED(seq_matches)) return seq_matches;
    else {
      lispval result = lists_to_vectors(seq_matches);
      kno_decref(seq_matches);
      return result;}}
  else if (PAIRP(pat)) {
    lispval head = KNO_CAR(pat);
    struct KNO_TEXTMATCH_OPERATOR
      *scan = match_operators, *limit = scan+n_match_operators;
    while (scan < limit)
      if (KNO_EQ(scan->kno_matchop,head)) break; else scan++; 
    if (scan < limit) {
      if (scan->kno_extractor)
        return scan->kno_extractor(pat,next,env,string,off,lim,flags);
      else {
        lispval matches = scan->kno_matcher(pat,next,env,string,off,lim,flags);
        if (KNO_ABORTED(matches))
          return matches;
        else {
          lispval answer = extract_text(string,off,matches);
          kno_decref(matches);
          return answer;}}}
    else return kno_err(kno_MatchSyntaxError,"textract",NULL,pat);}
  else if (SYMBOLP(pat)) {
    lispval v = match_eval(pat,env);
    if (VOIDP(v))
      return kno_err(kno_UnboundIdentifier,"textract",
                    SYM_NAME(pat),pat);
    else {
      lispval lengths = get_longest_match
        (kno_text_domatch(v,next,env,string,off,lim,flags));
      if (KNO_ABORTED(lengths)) {
        kno_decref(v);
        return lengths;}
      else {
        lispval answers = EMPTY;
        DO_CHOICES(l,lengths) {
          lispval extraction=
            kno_conspair(l,kno_substring(string+off,string+kno_getint(l)));
          CHOICE_ADD(answers,extraction);}
        kno_decref(lengths);
        kno_decref(v);
        return answers;}}}
  else if (TYPEP(pat,kno_txclosure_type)) {
    struct KNO_TXCLOSURE *txc = (kno_txclosure)pat;
    return textract(txc->kno_txpattern,next,txc->kno_txenv,string,off,lim,flags);}
  else if (TYPEP(pat,kno_regex_type)) {
    struct KNO_REGEX *ptr = kno_consptr(struct KNO_REGEX *,pat,kno_regex_type);
    regmatch_t results[1]; u8_string base = string+off;
    int retval = regexec(&(ptr->rxcompiled),base,1,results,0);
    if (retval == REG_NOMATCH)
      return EMPTY;
    else if (retval) {
      u8_byte buf[512];
      regerror(retval,&(ptr->rxcompiled),buf,512);
      u8_unlock_mutex(&(ptr->rx_lock));
      return kno_err(kno_RegexError,"kno_text_extract",
                    u8_strdup(buf),VOID);}
    else {
      lispval ex = kno_extract_string
        (NULL,base+results[0].rm_so,base+results[0].rm_eo);
      int loc = u8_charoffset(string,off+results[0].rm_eo);
      return kno_conspair(KNO_INT(loc),ex);}}
  else return kno_err(kno_MatchSyntaxError,"textract",NULL,pat);
}

static lispval extract_sequence
   (lispval pat,int pat_elt,lispval next,kno_lexenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  int l = VEC_LEN(pat);
  if (pat_elt == l)
    return kno_conspair(KNO_INT(off),NIL);
  else {
    lispval nextpat=
      (((pat_elt+1) == l) ? (next) : (VEC_REF(pat,pat_elt+1)));
    lispval sub_matches=
      textract(VEC_REF(pat,pat_elt),nextpat,env,string,off,lim,flags);
    if (KNO_ABORTED(sub_matches)) return sub_matches;
    else {
      lispval results = EMPTY;
      DO_CHOICES(sub_match,sub_matches) {
        if (kno_getint(KNO_CAR(sub_match)) <= lim) {
          u8_byteoff noff = kno_getint(KNO_CAR(sub_match));
          lispval remainders = extract_sequence
            (pat,pat_elt+1,next,env,string,noff,lim,flags);
          if (KNO_ABORTED(remainders)) {
            KNO_STOP_DO_CHOICES;
            kno_decref(sub_matches);
            kno_decref(results);
            return remainders;}
          else {
            DO_CHOICES(remainder,remainders) {
              lispval result=
                kno_conspair(KNO_CAR(remainder),
                            kno_conspair(kno_incref(KNO_CDR(sub_match)),
                                        kno_incref(KNO_CDR(remainder))));
              CHOICE_ADD(results,result);}
            kno_decref(remainders);}}}
      kno_decref(sub_matches);
      return results;}}
}

static lispval lists_to_vectors(lispval lists)
{
  lispval answer = EMPTY;
  DO_CHOICES(list,lists) {
    int i = 0, lim = 0;
    lispval lsize = KNO_CAR(list), scan = KNO_CDR(list), vec, elt;
    while (PAIRP(scan)) {lim++; scan = KNO_CDR(scan);}
    vec = kno_empty_vector(lim);
    scan = KNO_CDR(list); while (i < lim) {
      lispval car = KNO_CAR(scan); kno_incref(car);
      KNO_VECTOR_SET(vec,i,car);
      i++; scan = KNO_CDR(scan);}
    elt = kno_conspair(lsize,vec);
    CHOICE_ADD(answer,elt);}
  return answer;
}

/** Match repeatedly **/

static lispval match_repeatedly
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,
   int flags,int zero_ok)
{
  lispval match_points = EMPTY;
  lispval state = KNO_INT(off); int count = 0;
  if (zero_ok) {CHOICE_ADD(match_points,KNO_INT(off));}
  while (1) {
    lispval next_state = EMPTY;
    DO_CHOICES(pos,state) {
      lispval npos=
        kno_text_domatch(pat,next,env,string,kno_getint(pos),lim,flags);
      if (KNO_ABORTED(npos)) {
        KNO_STOP_DO_CHOICES;
        kno_decref(match_points);
        kno_decref(next_state);
        return npos;}
      else {
        DO_CHOICES(n,npos) {
          if (!((CHOICEP(state)) ? (kno_choice_containsp(n,state)) :
                (KNO_EQ(state,n)))) {
            if ((flags&KNO_MATCH_BE_GREEDY)==0) { 
              kno_incref(n);
              CHOICE_ADD(match_points,n);}
            kno_incref(n);
            CHOICE_ADD(next_state,n);}}
        kno_decref(npos);}}
    if (flags&KNO_MATCH_BE_GREEDY)
      if (EMPTYP(next_state)) {
        kno_decref(state);
        return match_points;}
      else {
        kno_decref(match_points);
        match_points = kno_incref(next_state);
        kno_decref(state);
        state = next_state;
        count++;}
    else if (EMPTYP(next_state))
      if (count == 0)
        if (zero_ok) {
          kno_decref(match_points);
          return state;}
        else {
          kno_decref(state);
          kno_decref(match_points);
          return EMPTY;}
      else {
        kno_decref(state);
        return match_points;}
    else {
      kno_decref(state); count++; state = next_state;}}
}

static lispval extract_repeatedly
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,
   int flags,int zero_ok)
{
  lispval choices = EMPTY;
  lispval top = textract(pat,next,env,string,off,lim,flags);
  if (EMPTYP(top))
    if (zero_ok)
      return kno_conspair(KNO_INT(off),NIL);
    else return EMPTY;
  else if (KNO_ABORTP(top)) return top;
  else {
    DO_CHOICES(each,top)
      if ((PAIRP(each)) && (KNO_UINTP(KNO_CAR(each))) &&
          ((FIX2INT(KNO_CAR(each))) != off)) {
        lispval size = KNO_CAR(each);
        lispval extraction = KNO_CDR(each);
        lispval remainders=
          extract_repeatedly(pat,next,env,string,kno_getint(size),lim,flags,1);
        if (KNO_ABORTP(remainders)) {
          kno_decref(choices);
          return remainders;}
        else if (EMPTYP(remainders)) {
          lispval last_item = kno_conspair(kno_incref(extraction),NIL);
          lispval with_size = kno_conspair(size,last_item);
          CHOICE_ADD(choices,with_size);}
        else {
          DO_CHOICES(remainder,remainders) {
            lispval item = kno_conspair
              (kno_refcar(remainder),
               kno_conspair(kno_incref(extraction),(kno_refcdr(remainder))));
            CHOICE_ADD(choices,item);}
          kno_decref(remainders);}
        if ((flags&KNO_MATCH_BE_GREEDY)==0) {
          lispval singleton = kno_make_list(1,kno_incref(extraction));
          lispval pair = kno_conspair(size,singleton);
          CHOICE_ADD(choices,pair);}}}
  kno_decref(top);
  return choices;
}

/** Match operations **/

#define KNO_PAT_ARG(arg,cxt,expr) \
  if (VOIDP(arg))              \
    return kno_err(kno_MatchSyntaxError,cxt,NULL,expr);

static lispval match_star
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (NILP(KNO_CDR(pat))) {
    u8_byteoff nextpos = ((VOIDP(next))?(-1):
                 (kno_text_search(next,env,string,off,lim,flags)));
    if (nextpos== -2) return KNO_ERROR;
    else if (nextpos<0) return KNO_INT(lim);
    else return KNO_INT(nextpos);}
  else {
    lispval pat_arg = kno_get_arg(pat,1);
    KNO_PAT_ARG(pat_arg,"match_star",pat);
    return match_repeatedly(pat_arg,next,env,string,off,lim,flags,1);}
}
static u8_byteoff search_star
   (lispval pat,kno_lexenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return off;
}
static lispval extract_star
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (NILP(KNO_CDR(pat))) {
    u8_byteoff nextpos=
      ((VOIDP(next)) ? (-1) :
       (kno_text_search(next,env,string,off,lim,flags)));
    if (nextpos== -2) return KNO_ERROR;
    else if (nextpos<0)
      return kno_conspair(KNO_INT(nextpos),
                         kno_substring(string+off,string+lim));
    else return kno_conspair(KNO_INT(nextpos),
                            kno_substring(string+off,string+nextpos));}
  else {
    lispval pat_arg = kno_get_arg(pat,1);
    if (VOIDP(pat_arg))
      return kno_err(kno_MatchSyntaxError,"extract_star",NULL,pat);
    else {
      lispval extractions = extract_repeatedly
        (pat_arg,next,env,string,off,lim,flags,1);
      lispval answer = EMPTY;
      DO_CHOICES(extraction,extractions) {
        lispval size = KNO_CAR(extraction), data = KNO_CDR(extraction);
        lispval pair=
          kno_conspair(size,kno_conspair(KNOSYM_STAR,kno_incref(data)));
        CHOICE_ADD(answer,pair);}
      kno_decref(extractions);
      return answer;}}
}

static lispval match_plus
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"match_plus",NULL,pat);
  else return match_repeatedly(pat_arg,next,env,string,off,lim,flags,0);
}
static u8_byteoff search_plus
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg)) {
    kno_seterr(kno_MatchSyntaxError,"search_plus",NULL,kno_incref(pat));
    return -2;}
  else return kno_text_search(pat_arg,env,string,off,lim,flags);
}
static lispval extract_plus
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"extract_plus",NULL,pat);
  else {
    lispval extractions =
      extract_repeatedly(pat_arg,next,env,string,off,lim,flags,0);
    lispval answer = EMPTY;
    DO_CHOICES(extraction,extractions) {
      lispval size = KNO_CAR(extraction), data = KNO_CDR(extraction);
      lispval pair=
        kno_conspair(size,kno_conspair(KNOSYM_PLUS,kno_incref(data)));
      CHOICE_ADD(answer,pair);}
    kno_decref(extractions);
    return answer;}
}

static lispval match_opt
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1), match_result;
  KNO_PAT_ARG(pat_arg,"match_opt",pat);
  match_result = kno_text_domatch(pat_arg,next,env,string,off,lim,flags);
  if (EMPTYP(match_result)) return KNO_INT(off);
  else return match_result;
}
static u8_byteoff search_opt
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return off;
}
static lispval extract_opt
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"extract_opt",NULL,pat);
  else {
    lispval extraction = textract(pat_arg,next,NULL,string,off,lim,flags);
    if (EMPTYP(extraction))
      return kno_conspair(KNO_INT(off),
                         kno_make_list(1,KNOSYM_OPT));
    else return extraction;}
}

/** Match NOT **/

static lispval match_not
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"match_not",NULL,pat);
  else {
    /* Find where there is a pat_arg starting */
    u8_byteoff pos = kno_text_search(pat_arg,env,string,off,lim,flags);
    if (pos == off) return EMPTY;
    else if (pos == -2) return pos;
    else {
      /* Enumerate every character position between here and there */
      u8_byteoff i = forward_char(string,off), last; lispval result = EMPTY;
      if ((pos < lim) && (pos > off))
        last = pos;
      else last = lim;
      while (i < last) {
        CHOICE_ADD(result,KNO_INT(i));
        i = forward_char(string,i);}
      CHOICE_ADD(result,KNO_INT(i));
      return result;}}
}

static u8_byteoff search_not
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"search_not",NULL,pat);
  else {
    lispval match = kno_text_matcher
      (pat_arg,env,string,off,lim,(flags&(~KNO_MATCH_DO_BINDINGS)));
    if (EMPTYP(match)) return off;
    else {
      u8_byteoff largest = off;
      DO_CHOICES(m,match) {
        u8_byteoff mi = kno_getint(m);
        if (mi > largest) largest = mi;}
      return largest;}}
}

/** Match AND **/

static lispval match_and
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat1 = kno_get_arg(pat,1);
  lispval pat2 = kno_get_arg(pat,2);
  /* Find where there is a pat_arg starting */
  lispval matches[2];
  KNO_PAT_ARG(pat1,"match_and",pat);
  KNO_PAT_ARG(pat2,"match_and",pat);  
  matches[0]=kno_text_domatch(pat1,next,env,string,off,lim,flags);
  if (EMPTYP(matches[0])) return EMPTY;
  else {
    lispval combined;
    matches[1]=kno_text_domatch(pat2,next,env,string,off,lim,flags);
    if (EMPTYP(matches[1])) combined = EMPTY;
    else combined = kno_intersection(matches,2);
    kno_decref(matches[0]); kno_decref(matches[1]);
    return combined;}
}

static u8_byteoff search_and
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_byteoff result;
  lispval pat1 = kno_get_arg(pat,1);
  lispval pat2 = kno_get_arg(pat,2);
  KNO_PAT_ARG(pat1,"search_and",pat);
  KNO_PAT_ARG(pat2,"search_and",pat);  
  result = kno_text_search(pat1,env,string,off,lim,flags);
  while (result>=0) {
    lispval match_result = match_and(pat,VOID,env,string,result,lim,flags);
    if (EMPTYP(match_result))
      result = kno_text_search(pat,env,string,
                            forward_char(string,result),lim,flags);
    else {kno_decref(match_result); return result;}}
  return result;
}

/** Match NOT> **/

static lispval match_not_gt
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"match_not_gt",NULL,pat);
  else {
    u8_byteoff pos = kno_text_search(pat_arg,env,string,off,lim,flags);
    if (pos < 0)
      if (pos== -2) return KNO_ERROR;
      else return KNO_INT(lim);
    /* else if (pos == off) return EMPTY; */
    else if (pos >= lim) return KNO_INT(lim);
    else return KNO_INT(pos);}
}

/** Match BIND **/

static lispval match_bind
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval sym = kno_get_arg(pat,1);
  lispval spat = kno_get_arg(pat,2);
  KNO_PAT_ARG(sym,"match_bind",pat);
  KNO_PAT_ARG(spat,"match_bind",pat);  
  if ((flags)&(KNO_MATCH_DO_BINDINGS)) {
    lispval ends = kno_text_domatch(spat,next,env,string,off,lim,flags);
    DO_CHOICES(end,ends) {
      lispval substr = kno_substring(string+off,string+kno_getint(end));
      kno_bind_value(sym,substr,env);}
    return ends;}
  else return kno_text_domatch(spat,next,env,string,off,lim,flags);
}
static u8_byteoff search_bind
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval spat = kno_get_arg(pat,2);
  if (VOIDP(spat))
    return kno_err(kno_MatchSyntaxError,"search_bind",NULL,pat);
  else return kno_text_search(spat,env,string,off,lim,flags);
}

static lispval label_match 
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval spat = kno_get_arg(pat,2);
  if (VOIDP(spat))
    return kno_err(kno_MatchSyntaxError,"label_match",NULL,pat);
  else return kno_text_domatch(spat,next,env,string,off,lim,flags);
}

static u8_byteoff label_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval spat = kno_get_arg(pat,2);
  if (VOIDP(spat))
    return kno_err(kno_MatchSyntaxError,"label_search",NULL,pat);
  else return kno_text_search(spat,env,string,off,lim,flags);
}

static lispval label_extract
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval sym = kno_get_arg(pat,1);
  lispval spat = kno_get_arg(pat,2);
  lispval parser = kno_get_arg(pat,3);
  if ((VOIDP(spat)) || (VOIDP(sym)))
    return kno_err(kno_MatchSyntaxError,"label_extract",NULL,pat);
  else {
    lispval extractions = textract(spat,next,env,string,off,lim,flags);
    lispval answers = EMPTY;
    if (KNO_ABORTED(extractions)) return extractions;
    else {
      DO_CHOICES(extraction,extractions) {
        lispval size = KNO_CAR(extraction), data = KNO_CDR(extraction);
        lispval xtract, addval; kno_incref(data);
        if (VOIDP(parser))
          xtract = kno_make_list(3,KNO_CAR(pat),sym,data);
        else if (SYMBOLP(parser)) {
          lispval parser_val = match_eval(parser,env);
          if ((KNO_ABORTED(parser_val))||(VOIDP(parser_val))) {
            KNO_STOP_DO_CHOICES;
            kno_decref(extractions); kno_decref(answers); kno_decref(data);
            if (VOIDP(parser_val))
              return kno_err(kno_UnboundIdentifier,"label_extract/convert",
                            SYM_NAME(parser),parser);
            else return parser_val;}
          xtract = kno_make_list(4,KNO_CAR(pat),sym,data,parser_val);}
        else if ((env) && (PAIRP(parser))) {
          lispval parser_val = kno_eval(parser,env);
          if ((KNO_ABORTED(parser_val))||(VOIDP(parser_val))) {
            KNO_STOP_DO_CHOICES;
            kno_decref(answers);
            kno_decref(extractions);
            kno_decref(data);
            return parser_val;}
          xtract = kno_make_list(4,KNO_CAR(pat),sym,data,parser_val);}
        else {
          kno_incref(parser);
          xtract = kno_make_list(4,KNO_CAR(pat),sym,data,parser);}
        addval = kno_conspair(size,xtract);
        CHOICE_ADD(answers,addval);}
      kno_decref(extractions);
      return answers;}}
}

static lispval subst_match 
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval spat = kno_get_arg(pat,1);
  if (VOIDP(spat))
    return kno_err(kno_MatchSyntaxError,"subst_match",NULL,pat);
  else return kno_text_domatch(spat,next,env,string,off,lim,flags);
}

static u8_byteoff subst_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval spat = kno_get_arg(pat,1);
  if (VOIDP(spat))
    return kno_err(kno_MatchSyntaxError,"subst_search",NULL,pat);
  else return kno_text_search(spat,env,string,off,lim,flags);
}

static lispval expand_subst_args(lispval args,kno_lexenv env);
static lispval subst_extract
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval spat = kno_get_arg(pat,1);
  if (VOIDP(spat))
    return kno_err(kno_MatchSyntaxError,"subst_extract",NULL,pat);
  else {
    lispval matches = kno_text_domatch(spat,next,env,string,off,lim,flags);
    if (KNO_ABORTED(matches)) return matches;
    else {
      lispval answers = EMPTY;
      lispval args = KNO_CDR(KNO_CDR(pat));
      lispval expanded = expand_subst_args(args,env);
      DO_CHOICES(match,matches) {
        int matchlen = kno_getint(match);
        lispval matched = kno_substring(string+off,string+matchlen);
        if ((CHOICEP(expanded))||(PRECHOICEP(expanded))) {
          DO_CHOICES(subst_arg,expanded) {
            lispval new_args = kno_conspair(kno_incref(matched),kno_incref(subst_arg));
            lispval new_subst = kno_conspair(subst_symbol,new_args);
            lispval answer = kno_conspair(match,new_subst);
            CHOICE_ADD(answers,answer);}
          kno_decref(matched);}
        else {
          lispval new_args = kno_conspair(matched,expanded);
          lispval new_subst = kno_conspair(subst_symbol,new_args);
          lispval answer = kno_conspair(match,new_subst);
          kno_incref(expanded);
          CHOICE_ADD(answers,answer);}}
      kno_decref(expanded);
      kno_decref(matches);
      return answers;}}
}

static lispval expand_subst_args(lispval args,kno_lexenv env)
{
  if (SYMBOLP(args)) {
    lispval value = match_eval(args,env);
    if (VOIDP(value)) return kno_incref(args);
    else return value;}
  else if (!(CONSP(args))) return args;
  else if (PAIRP(args)) {
    lispval carchoices = expand_subst_args(KNO_CAR(args),env);
    lispval cdrchoices = expand_subst_args(KNO_CDR(args),env);
    /* Avoid the multiplication of conses by reusing ARGS if it
       hasn't exploded or changed. */
    if ((CHOICEP(carchoices))||(PRECHOICEP(carchoices))||
        (CHOICEP(cdrchoices))||(PRECHOICEP(cdrchoices))) {
      lispval conses = EMPTY;
      DO_CHOICES(car,carchoices) {
        DO_CHOICES(cdr,cdrchoices) {
          lispval cons = kno_conspair(car,cdr);
          kno_incref(car); kno_incref(cdr);
          CHOICE_ADD(conses,cons);}}
      kno_decref(carchoices);
      kno_decref(cdrchoices);
      return conses;}
    else if ((KNO_EQUAL(carchoices,KNO_CAR(args)))&&
             (KNO_EQUAL(cdrchoices,KNO_CDR(args)))) {
      kno_decref(carchoices);
      kno_decref(cdrchoices);
      return kno_incref(args);}
    else return kno_conspair(carchoices,cdrchoices);}
  else if (CHOICEP(args)) {
    lispval changed = EMPTY;
    DO_CHOICES(elt,args) {
      lispval cv = expand_subst_args(elt,env);
      CHOICE_ADD(changed,cv);}
    return changed;}
  else return kno_incref(args);
}

/* Match preferred */

/* This is just like a choice pattern except that when searching,
   we use the patterns in order and bound subsequent searches by previous
   results. */

static lispval match_pref
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  KNO_DOLIST(epat,KNO_CDR(pat)) {
    lispval answer = kno_text_domatch(epat,next,env,string,off,lim,flags);
    if (KNO_ABORTED(answer)) return answer;
    else if (EMPTYP(answer)) {}
    else return answer;}
  return EMPTY;
}

static lispval extract_pref
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  KNO_DOLIST(epat,KNO_CDR(pat)) {
    lispval extractions = textract(epat,next,env,string,off,lim,flags);
    if (KNO_ABORTED(extractions)) return extractions;
    else if (EMPTYP(extractions)) {}
    else return extractions;}
  return EMPTY;
}

static u8_byteoff search_pref
  (lispval pat,kno_lexenv env,
   u8_string string,
   u8_byteoff off,u8_byteoff lim,
   int flags)
{
  int nlim = lim, loc = -1;
  KNO_DOLIST(epat,KNO_CDR(pat)) {
    u8_byteoff nxt = kno_text_search(epat,env,string,off,nlim,flags);
    if (nxt < 0) {
      if (nxt== -2) {
        return nxt;}}
    else if (nxt < nlim) {nlim = nxt; loc = nxt;}}
  return loc;
}


/* Word match */

#define apostrophep(x) ((x == '\'')||(x==0x2019))
#define dashp(x) ((x == '-')||(x == '_')||(x==0xAD)||(x==0x2010)||(x==0x2011)||(x=='/')||(x==0x2044))

static int word_startp(u8_string string,u8_byteoff off)
{
  if (off==0) return 1;
  else {
    int new_off = backward_char(string,off);
    u8_string scan = string+new_off;
    u8_unichar ch = u8_sgetc(&scan);
    if (ch < 0) return 1;
    else if (u8_isspace(ch)) return 1;
    else if (u8_ispunct(ch)) {
      if ((apostrophep(ch))||(dashp(ch))) {
        if (new_off==0) return 1;
        else {
          new_off = backward_char(string,new_off);
          scan = string+new_off;}
        ch = u8_sgetc(&scan);
        if ((u8_isspace(ch))||(u8_ispunct(ch))) return 1;
        else return 0;}
      else return 1;}
    else return 0;}
}

static lispval word_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval wpat = kno_get_arg(pat,1);
  if (VOIDP(wpat))
    return kno_err(kno_MatchSyntaxError,"word_match",NULL,pat);
  else if ((off > 0) && ((word_startp(string,off)) == 0))
    return EMPTY;
  else {
    lispval final_results = (EMPTY);
    lispval core_result = kno_text_domatch
      (wpat,next,env,string,off,lim,(flags|KNO_MATCH_COLLAPSE_SPACES));
    if (KNO_ABORTED(core_result)) {
      kno_decref(final_results);
      return core_result;}
    else {
      DO_CHOICES(offset,core_result) {
        if (KNO_UINTP(offset)) {
          u8_byteoff noff = FIX2INT(offset), complete_word = 0;
          if (noff == lim) complete_word = 1;
          else {
            const u8_byte *ptr = string+noff;
            u8_unichar ch = u8_sgetc(&ptr);
            if ((u8_isspace(ch)) || (u8_ispunct(ch))) complete_word = 1;}
          if (complete_word) {
            kno_incref(offset);
            CHOICE_ADD(final_results,offset);}}
        else {
          KNO_STOP_DO_CHOICES;
          kno_incref(offset); kno_decref(core_result); 
          return kno_err(kno_InternalMatchError,"word_match",NULL,offset);}}}
    kno_decref(core_result);
    return final_results;}
}

static u8_byteoff get_next_candidate
  (u8_string string,u8_byteoff off,u8_byteoff lim)
{
  const u8_byte *scan = string+off, *limit = string+lim;
  while (scan < limit) {   /* Find another space */
    u8_unichar ch = string_ref(scan);
    if ((u8_isspace(ch)) || (u8_ispunct(ch))) break;
    else if (*scan < 0x80) scan++;
    else u8_sgetc(&scan);}
  if (scan >= limit) return -1;
  else { /* Skip over the space */
    const u8_byte *probe = scan, *prev = scan;
    while (probe < limit) {
      u8_unichar ch = u8_sgetc(&probe);
      if ((u8_isspace(ch)) || (u8_ispunct(ch))) prev = probe;
      else break;}
    scan = prev;}
  if (scan >= limit) return -1;
  else return scan-string;
}

static u8_byteoff word_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval wpat = kno_get_arg(pat,1); u8_byteoff match_result = -1, cand;
  if (VOIDP(wpat))
    return kno_err(kno_MatchSyntaxError,"word_search",NULL,pat);
  else if (word_startp(string,off)) cand = off;
  else cand = get_next_candidate(string,off,lim);
  while ((cand >= 0) && (match_result < 0)) {
    lispval matches=
      kno_text_matcher(wpat,env,string,cand,lim,(flags|KNO_MATCH_COLLAPSE_SPACES));
    if (EMPTYP(matches)) {}
    else {
      DO_CHOICES(match,matches) {
        u8_byteoff n = kno_getint(match), ch;
        if (n == lim) {
          match_result = cand; KNO_STOP_DO_CHOICES; break;}
        else {
          const u8_byte *p = string+n; ch = u8_sgetc(&p);
          if ((u8_isspace(ch)) || (u8_ispunct(ch))) {
            match_result = cand; KNO_STOP_DO_CHOICES; break;}}}}
    cand = get_next_candidate(string,cand,lim);
    kno_decref(matches);}
  return match_result;
}

static lispval word_extract
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval wpat = kno_get_arg(pat,1);
  if (VOIDP(wpat))
    return kno_err(kno_MatchSyntaxError,"word_extract",NULL,pat);
  else {
    lispval ends = kno_text_domatch
      (wpat,next,env,string,off,lim,(flags|KNO_MATCH_COLLAPSE_SPACES));
    lispval answers = EMPTY;
    DO_CHOICES(end,ends) {
      lispval substring=
        kno_substring(string+off,string+kno_getint(end));
      lispval pair = kno_conspair(end,substring);
      CHOICE_ADD(answers,pair);}
    kno_decref(ends);
    return answers;}
}

/* Matching chunks */

static lispval chunk_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval cpat = kno_get_arg(pat,1);
  if (VOIDP(cpat))
    return kno_err(kno_MatchSyntaxError,"chunk_match",NULL,pat);
  else return kno_text_domatch(cpat,next,env,string,off,lim,flags);
}

static u8_byteoff chunk_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval cpat = kno_get_arg(pat,1);
  if (VOIDP(cpat))
    return kno_err(kno_MatchSyntaxError,"chunk_search",NULL,pat);
  else return kno_text_search(cpat,env,string,off,lim,flags);
}

static lispval chunk_extract
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval cpat = kno_get_arg(pat,1);
  if (VOIDP(cpat))
    return kno_err(kno_MatchSyntaxError,"chunk_extract",NULL,pat);
  else {
    lispval ends = kno_text_domatch(cpat,next,env,string,off,lim,flags);
    lispval answers = EMPTY;
    DO_CHOICES(end,ends) {
      lispval substring=
        kno_substring(string+off,string+kno_getint(end));
      lispval pair = kno_conspair(end,substring);
      CHOICE_ADD(answers,pair);}
    kno_decref(ends);
    return answers;}
}

/** CHOICE matching **/

/* These methods are equivalent to passing choices, but allow:
    (a) the protection of choices from automatic evaluation, and
    (b) increasing readability by combining multiple choices together,
         e.g. (CHOICE {"tree" "bush"} {"beast" "rodent"})
*/
         
static lispval match_choice
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if ((PAIRP(KNO_CDR(pat))) &&
      (NILP(KNO_CDR(KNO_CDR(pat)))))
    return kno_text_domatch
      (KNO_CAR(KNO_CDR(pat)),next,env,string,off,lim,flags);
  else {
    lispval choice = EMPTY;
    KNO_DOLIST(elt,KNO_CDR(pat)) {
      kno_incref(elt);
      CHOICE_ADD(choice,elt);}
    if (EMPTYP(choice)) return EMPTY;
    else {
      lispval result = kno_text_matcher(choice,env,string,off,lim,flags);
      kno_decref(choice);
      return result;}}
}

static u8_byteoff search_choice
  (lispval pat,kno_lexenv env,
   u8_string string,
   u8_byteoff off,u8_byteoff lim,
   int flags)
{
  if ((PAIRP(KNO_CDR(pat))) &&
      (NILP(KNO_CDR(KNO_CDR(pat)))))
    return kno_text_search(KNO_CAR(KNO_CDR(pat)),env,string,off,lim,flags);
  else {
    lispval choice = EMPTY;
    KNO_DOLIST(elt,KNO_CDR(pat)) {
      kno_incref(elt);
      CHOICE_ADD(choice,elt);}
    if (EMPTYP(choice)) return -1;
    else {
      u8_byteoff result = kno_text_search(choice,env,string,off,lim,flags);
      kno_decref(choice);
      return result;}}
}

static lispval extract_choice
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if ((PAIRP(KNO_CDR(pat))) &&
      (NILP(KNO_CDR(KNO_CDR(pat)))))
    return kno_text_doextract
      (KNO_CAR(KNO_CDR(pat)),next,env,string,off,lim,flags);
  else {
    lispval choice = EMPTY;
    KNO_DOLIST(elt,KNO_CDR(pat)) {
      kno_incref(elt);
      CHOICE_ADD(choice,elt);}
    if (EMPTYP(choice)) return EMPTY;
    else {
      lispval result = textract(choice,next,env,string,off,lim,flags);
      kno_decref(choice);
      return result;}}
}

/** Case sensitive/insensitive **/

static lispval match_ci
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"match_ci",NULL,pat);
  else return kno_text_matcher
    (pat_arg,env,string,off,lim,(flags|(KNO_MATCH_IGNORE_CASE)));
}
static u8_byteoff search_ci
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"search_ci",NULL,pat);
  else return kno_text_search(pat_arg,env,string,off,lim,
                             (flags|(KNO_MATCH_IGNORE_CASE)));
}
static lispval extract_ci
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"extract_ci",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags|(KNO_MATCH_IGNORE_CASE)));
}

static lispval match_cs
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"match_cs",NULL,pat);
  else return kno_text_matcher
    (pat_arg,env,string,off,lim,(flags&(~(KNO_MATCH_IGNORE_CASE))));
}
static u8_byteoff search_cs
   (lispval pat,kno_lexenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"search_cs",NULL,pat);
  else return kno_text_search
         (pat_arg,env,string,off,lim,(flags&(~(KNO_MATCH_IGNORE_CASE))));
}
static lispval extract_cs
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"extract_cs",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags&(~(KNO_MATCH_IGNORE_CASE))));
}

/* Diacritic insensitive and sensitive matching */

static lispval match_di
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"match_di",NULL,pat);
  else return kno_text_matcher
    (pat_arg,env,
     string,off,lim,(flags|(KNO_MATCH_IGNORE_DIACRITICS)));
}
static u8_byteoff search_di
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"search_di",NULL,pat);
  else return kno_text_search(pat_arg,env,string,off,lim,
                             (flags|(KNO_MATCH_IGNORE_DIACRITICS)));
}
static lispval extract_di
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"extract_di",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags|(KNO_MATCH_IGNORE_DIACRITICS)));
}

static lispval match_ds
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"match_ds",NULL,pat);
  else return kno_text_matcher
         (pat_arg,env,string,off,lim,(flags&(~(KNO_MATCH_IGNORE_DIACRITICS))));
}
static u8_byteoff search_ds
   (lispval pat,kno_lexenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"search_ds",NULL,pat);
  else return kno_text_search
    (pat_arg,env,string,off,lim,(flags&(~(KNO_MATCH_IGNORE_DIACRITICS))));
}
static lispval extract_ds
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"extract_ds",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags&(~(KNO_MATCH_IGNORE_DIACRITICS))));
}

/* Greedy/expansion match/search/etc. */

static lispval match_greedy
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"match_greedy",NULL,pat);
  else return kno_text_matcher
    (pat_arg,env,string,off,lim,(flags|(KNO_MATCH_BE_GREEDY)));
}
static u8_byteoff search_greedy
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"search_greedy",NULL,pat);
  else return kno_text_search(pat_arg,env,string,off,lim,
                             (flags|(KNO_MATCH_BE_GREEDY)));
}
static lispval extract_greedy
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"extract_greedy",NULL,pat);
  else return textract
         (pat_arg,next,env,string,off,lim,(flags|(KNO_MATCH_BE_GREEDY)));
}

static lispval match_expansive
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"match_expansive",NULL,pat);
  else return kno_text_matcher
         (pat_arg,env,string,off,lim,(flags&(~(KNO_MATCH_BE_GREEDY))));
}
static u8_byteoff search_expansive
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"search_expansive",NULL,pat);
  else return kno_text_search
    (pat_arg,env,string,off,lim,(flags&(~(KNO_MATCH_BE_GREEDY))));
}
static lispval extract_expansive
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"extract_expansive",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags&(~(KNO_MATCH_BE_GREEDY))));
}

static lispval match_longest
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"match_longest",NULL,pat);
  else return get_longest_match(kno_text_matcher(pat_arg,env,string,off,lim,flags));
}
static u8_byteoff search_longest
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"search_longest",NULL,pat);
  else return kno_text_search(pat_arg,env,string,off,lim,flags);
}
static lispval extract_longest
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"extract_longeset",NULL,pat);
  else {
    lispval results = kno_text_matcher(pat_arg,env,string,off,lim,flags);
    if (KNO_ABORTED(results)) return results;
    else return get_longest_extractions(results);}
}

/* Space collapsing matching */

static lispval match_si
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"match_si",NULL,pat);
  else return kno_text_matcher
         (pat_arg,env,string,off,lim,(flags|(KNO_MATCH_COLLAPSE_SPACES)));
}

static u8_byteoff search_si
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"search_si",NULL,pat);
  else return kno_text_search(pat_arg,env,string,off,lim,
                             (flags|(KNO_MATCH_COLLAPSE_SPACES)));
}
static lispval extract_si
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"extract_si",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags|(KNO_MATCH_COLLAPSE_SPACES)));
}

static lispval match_ss
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"match_ss",NULL,pat);
  else return kno_text_matcher
         (pat_arg,env,string,off,lim,(flags&(~(KNO_MATCH_COLLAPSE_SPACES))));
}
static u8_byteoff search_ss
   (lispval pat,kno_lexenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"search_ss",NULL,pat);
  else return kno_text_search
    (pat_arg,env,string,off,lim,(flags&(~(KNO_MATCH_COLLAPSE_SPACES))));
}
static lispval extract_ss
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"extract_ss",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags&(~(KNO_MATCH_COLLAPSE_SPACES))));
}

/** Canonical matching: ignore spacing, case, and diacritics */

static lispval match_canonical
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"match_canonical",NULL,pat);
  else return kno_text_matcher
         (pat_arg,env,string,off,lim,(flags|(KNO_MATCH_SPECIAL)));
}
static u8_byteoff search_canonical
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"search_canonical",NULL,pat);
  else return kno_text_search(pat_arg,env,string,off,lim,
                             (flags|(KNO_MATCH_SPECIAL)));
}
static lispval extract_canonical
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"extract_canonical",NULL,pat);
  else return textract
    (pat_arg,next,env,string,off,lim,(flags|(KNO_MATCH_SPECIAL)));
}

/** EOL and BOL **/

/* Matching/finding beginning of lines */
static lispval match_bol
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == 0)
    return KNO_INT(0);
  else if ((string[off-1] == '\n') || (string[off-1] == '\r'))
    return KNO_INT(off);
  else return EMPTY;
}

static u8_byteoff search_bol
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == 0) return off;
  else if ((string[off-1] == '\n') || (string[off-1] == '\r')) return off;
  else {
    const u8_byte *scan = strchr(string+off,'\n');
    if (scan) return ((scan-string)+1);
    else {
      const u8_byte *scan = strchr(string+off,'\r');
      if (scan) return ((scan-string)+1);
      else return -1;}}
}

/* Matching/finding end of lines */

static lispval match_eol
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == lim) return KNO_INT(off);
  else if ((string[off] == '\n') || (string[off] == '\r'))
    return KNO_INT(off+1);
  else return EMPTY;
}

static u8_byteoff search_eol
    (lispval pat,kno_lexenv env,
     u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == lim) return off+1;
  else if (off > lim) return -1;
  else {
    const u8_byte *scan = strchr(string+off,'\n');
    if (scan) return ((scan-string));
    else {
      const u8_byte *scan = strchr(string+off,'\r');
      if (scan) return ((scan-string));
      else return lim;}}
}

/* Matching/finding beginning of words */

static lispval match_bow
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == 0)
    return KNO_INT(0);
  else {
    u8_byteoff prev = backward_char(string,off);
    const u8_byte *ptr = string+prev;
    int c = u8_sgetc(&ptr);
    if (u8_isspace(c)) return KNO_INT(off);
    else return EMPTY;}
}

static u8_byteoff search_bow
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == 0) return off;
  else {
    const u8_byte *scan = string+off, *scanlim = string+lim, *last = scan;
    int c = u8_sgetc(&scan);
    if (u8_isspace(c))  {
      while ((c>0) && (scan<scanlim) && (u8_isspace(c))) {
        last = scan; c = u8_sgetc(&scan);}
      if (scan<scanlim) return last-string; else return -1;}
    else {
      u8_byteoff prev = backward_char(string,off);
      if (prev<0) return -1; else {
        scan = string+prev; c = u8_sgetc(&scan);}
      if (u8_isspace(c)) return off;
      while ((c>0) && (scan<scanlim) && (!(u8_isspace(c))))
        c = u8_sgetc(&scan);
      if (scan>=scanlim) return -1;
      else while ((c>0) && (scan<scanlim) && (u8_isspace(c))) {
          last = scan; c = u8_sgetc(&scan);}
      if (scan>=scanlim) return -1;
      else return last-string;}}
}

/* Matching/finding end of words */

static lispval match_eow
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_string point = string+off;
  int nc = u8_sgetc(&point);
  if ( (nc < 0) || (u8_isspace(nc)) ) {
    u8_byteoff prev_off = backward_char(string,off);
    point = string + prev_off;
    int pc = u8_sgetc(&point);
    if (!(u8_isspace(pc)))
      return KNO_FIX2INT(off);
    else return EMPTY;}
  else return EMPTY;
}

static u8_byteoff search_eow
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *scanlim = string+lim, *last = scan;
  int c = u8_sgetc(&scan);
  while ( (u8_isspace(c)) && (scan < scanlim) ) {
    last = scan;
    c = u8_sgetc(&scan);}
  if ( (scan >= scanlim) || (c < 0) || (u8_isspace(c)) )
    return -1;
  while ( (!(u8_isspace(c))) && (scan <= scanlim) ) {
    last = scan;
    c = u8_sgetc(&scan);}
  if ( (c < 0) || (u8_isspace(c)) )
    return last-string;
  else return -1;
}


/* Matching/finding the beginning or end of the string */

static lispval match_bos
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == 0) return KNO_INT(off);
  else return EMPTY;
}

static u8_byteoff search_bos
    (lispval pat,kno_lexenv env,
     u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == 0) return off;
  else return -1;
}

static lispval match_eos
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == lim) return KNO_INT(off);
  else return EMPTY;
}

static u8_byteoff search_eos
    (lispval pat,kno_lexenv env,
     u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off == lim) return off+1;
  else return lim;
}

/* Rest matching */

static lispval match_rest
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return KNO_INT(lim);
}
static u8_byteoff search_rest
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return off;
}
static lispval extract_rest
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  return kno_conspair(KNO_INT(lim),
                     kno_substring(string+off,string+lim));
}

/** Character match operations **/

static lispval match_char_range
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar start = KNO_CHAR2CODE(kno_get_arg(pat,1));
  u8_unichar end = KNO_CHAR2CODE(kno_get_arg(pat,2));
  u8_unichar actual = string_ref(string+off);
  if (flags&(KNO_MATCH_IGNORE_CASE)) {
    start = u8_tolower(start); start = u8_tolower(end);
    actual = u8_tolower(start);}
  if ((actual >= start) && (actual <= end)) {
    const u8_byte *new_off = u8_substring(string+off,1);
    return KNO_INT(new_off-string);}
  else return EMPTY;
}

static lispval match_char_not_core
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string, u8_byteoff off,u8_byteoff lim,
   int flags,int match_null_string)
{
  lispval arg1 = kno_get_arg(pat,1);
  const u8_byte *scan = string+off, *last_scan = scan, *end = string+lim;
  int *break_chars, n_break_chars = 0;
  if (VOIDP(pat))
    return kno_err(kno_MatchSyntaxError,"match_char_not_core",NULL,pat);
  else if (!(STRINGP(arg1)))
    return kno_type_error("string","match_char_not_core",arg1);
  else {
    const u8_byte *scan = CSTRING(arg1); int c = u8_sgetc(&scan);
    break_chars = u8_alloc_n(STRLEN(arg1),unsigned int);
    while (!(c<0)) {
      if (flags&(KNO_MATCH_IGNORE_CASE))
        break_chars[n_break_chars++]=u8_tolower(c);
      else break_chars[n_break_chars++]=c;
      c = u8_sgetc(&scan);}}
  while (scan<end) {
    int i = 0, hit = 0; u8_unichar ch = u8_sgetc(&scan);
    if (ch == -1) ch = 0;
    if (flags&(KNO_MATCH_IGNORE_CASE)) ch = u8_tolower(ch);
    while (i < n_break_chars)
      if (break_chars[i] == ch) {hit = 1; break;} else i++;
    if (hit)
      if (last_scan == string+off) {
        u8_free(break_chars);
        if (match_null_string) return KNO_INT(last_scan-string);
        else return EMPTY;}
      else {
        u8_free(break_chars);
        return KNO_INT(last_scan-string);}
    else last_scan = scan;}
  u8_free(break_chars);
  return KNO_INT(lim);
}

static lispval match_char_not
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"match_char_not",NULL,pat);
  else return match_char_not_core(pat,next,env,string,off,lim,flags,0);
}
static lispval match_char_not_star
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval pat_arg = kno_get_arg(pat,1);
  if (VOIDP(pat_arg))
    return kno_err(kno_MatchSyntaxError,"match_char_not_star",NULL,pat);
  else return match_char_not_core(pat,next,env,string,off,lim,flags,1);
}

static lispval isvowel_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off), bch = u8_base_char(ch);
  if (strchr("aeiouAEIOU",bch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}

static lispval isnotvowel_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off), bch = u8_base_char(ch);
  if (strchr("aeiouAEIOU",bch)) return EMPTY;
  else return KNO_INT(forward_char(string,off));
}

static lispval isspace_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if (u8_isspace(ch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval isspace_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  const u8_byte *scan = string+off, *limit = string+lim, *last = scan;
  u8_unichar ch = u8_sgetc(&scan);
  while (u8_isspace(ch)) 
    if (scan > limit) break;
    else {
      last = scan;
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(last-string);
      else {CHOICE_ADD(match_points,KNO_INT(last-string));}
      ch = u8_sgetc(&scan);}
  if (last == string) return EMPTY;
  else if ((flags)&(KNO_MATCH_BE_GREEDY))
    return get_longest_match(match_points);
  else return match_points;
}
static u8_byteoff isspace_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (u8_isspace(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

static lispval spaces_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *limit = string+lim, *last = scan;
  u8_unichar ch = u8_sgetc(&scan);
  while (u8_isspace(ch)) 
    if (scan > limit) break; else {last = scan; ch = u8_sgetc(&scan);}
  if (last == string+off) return EMPTY;
  else return KNO_INT(last-string);
}
static u8_byteoff spaces_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (u8_isspace(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

static lispval spaces_star_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *limit = string+lim, *last = scan;
  u8_unichar ch = u8_sgetc(&scan);
  while ((ch>0) && (u8_isspace(ch))) 
    if (scan > limit) break; else {last = scan; ch = u8_sgetc(&scan);}
  if (last == string) return KNO_INT(off);
  else return KNO_INT(last-string);
}
static u8_byteoff spaces_star_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (off > lim) return -1;
  else return off;
}

/* Horizontal space matching */

static lispval ishspace_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if (u8_ishspace(ch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval ishspace_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  const u8_byte *scan = string+off, *limit = string+lim, *last = scan;
  u8_unichar ch = u8_sgetc(&scan);
  while (u8_ishspace(ch)) 
    if (scan > limit) break;
    else {
      last = scan;
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(last-string);
      else {CHOICE_ADD(match_points,KNO_INT(last-string));}
      ch = u8_sgetc(&scan);}
  if (last == string) return EMPTY;
  else if ((flags)&(KNO_MATCH_BE_GREEDY))
    return get_longest_match(match_points);
  else return match_points;
}
static u8_byteoff ishspace_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (u8_ishspace(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

/* Vertical space matching */

static lispval vspace_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if (u8_isvspace(ch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval vspace_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  const u8_byte *scan = string+off, *limit = string+lim, *last = scan;
  u8_unichar ch = u8_sgetc(&scan);
  while (u8_isvspace(ch))
    if (scan > limit) break;
    else {
      last = scan;
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(last-string);
      else {CHOICE_ADD(match_points,KNO_INT(last-string));}
      ch = u8_sgetc(&scan);}
  if (last == string) return EMPTY;
  else if ((flags)&(KNO_MATCH_BE_GREEDY))
    return get_longest_match(match_points);
  else return match_points;
}
static u8_byteoff vspace_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (u8_isvspace(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

static lispval vbreak_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *limit = string+lim, *last = NULL;
  u8_unichar ch = u8_sgetc(&scan);
  while (u8_isvspace(ch)) {
    ch=u8_sgetc(&scan);
    while (u8_ishspace(ch)) {
      if (scan > limit) break;
      else ch=u8_sgetc(&scan);}
    if (scan > limit) break;
    else if (!(u8_isvspace(ch))) break;
    else last=scan;}
  if (last==NULL)
    return EMPTY;
  else if (last<=(string+off))
    return EMPTY;
  else return KNO_INT(last-string);
}

static u8_byteoff vbreak_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_string s = string+off, sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (u8_isvspace(ch)) {
      u8_string scan=s; ch=u8_sgetc(&scan);
      while ((scan<sl) && (u8_ishspace(ch))) {
        ch=u8_sgetc(&scan);}
      if (scan>=sl)
        return -1;
      else if (u8_isvspace(ch))
        return s-string;
      else s=scan;}
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

/* Other matchers */

static lispval isalnum_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if (u8_isalnum(ch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval isalnum_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  u8_unichar ch = string_ref(string+off);
  if (u8_isalnum(ch)) {
    while (u8_isalnum(ch)) {
      off = forward_char(string,off);
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(off);
      else {CHOICE_ADD(match_points,KNO_INT(off));}
      ch = string_ref(string+off);}
    if ((flags)&(KNO_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return EMPTY;
}
static u8_byteoff isalnum_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (u8_isalnum(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

static lispval isword_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if ((u8_isalpha(ch)) || (ch == '-') || (ch == '_'))
    return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval isword_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  u8_unichar ch = string_ref(string+off);
  if ((u8_isalpha(ch)) || (ch == '-') || (ch == '_')) {
    while ((u8_isalpha(ch)) || (ch == '-') || (ch == '_')) {
      off = forward_char(string,off);
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(off);
      else {CHOICE_ADD(match_points,KNO_INT(off));}
      ch = string_ref(string+off);}
    if ((flags)&(KNO_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return EMPTY;
}
static u8_byteoff isword_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if ((u8_isalpha(ch)) || (ch == '-') || (ch == '_')) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

/* Digit matching */

static lispval isdigit_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if (u8_isdigit(ch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval isdigit_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  u8_unichar ch = string_ref(string+off);
  if (u8_isdigit(ch)) {
    while (u8_isdigit(ch)) {
      off = forward_char(string,off);
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(off);
      else {CHOICE_ADD(match_points,KNO_INT(off));}
      ch = string_ref(string+off);}
    if ((flags)&(KNO_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return EMPTY;
}
static u8_byteoff isdigit_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (u8_isdigit(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

static lispval isxdigit_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if (u8_isxdigit(ch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval isxdigit_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  u8_unichar ch = string_ref(string+off);
  if (u8_isxdigit(ch)) {
    while (u8_isxdigit(ch)) {
      off = forward_char(string,off);
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(off);
      else {CHOICE_ADD(match_points,KNO_INT(off));}
      ch = string_ref(string+off);}
    if ((flags)&(KNO_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return EMPTY;
}
static u8_byteoff isxdigit_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (u8_isxdigit(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

static lispval isodigit_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if (u8_isodigit(ch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval isodigit_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  u8_unichar ch = string_ref(string+off);
  if (u8_isodigit(ch)) {
    while (u8_isodigit(ch)) {
      off = forward_char(string,off);
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(off);
      else {CHOICE_ADD(match_points,KNO_INT(off));}
      ch = string_ref(string+off);}
    if ((flags)&(KNO_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return EMPTY;
}
static u8_byteoff isodigit_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (u8_isodigit(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

static lispval isalpha_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if (u8_isalpha(ch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval isalpha_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  u8_unichar ch = string_ref(string+off);
  if (u8_isalpha(ch)) {
    while ((u8_isalpha(ch)) && (off<lim)) {
      off = forward_char(string,off);
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(off);
      else {CHOICE_ADD(match_points,KNO_INT(off));}
      ch = string_ref(string+off);}
    if ((flags)&(KNO_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return EMPTY;
}
static u8_byteoff isalpha_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (u8_isalpha(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

static lispval ispunct_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if (u8_ispunct(ch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval ispunct_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  u8_unichar ch = string_ref(string+off);
  if (u8_ispunct(ch)) {
    while ((u8_ispunct(ch)) && (off<lim)) {
      off = forward_char(string,off);
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(off);
      else {CHOICE_ADD(match_points,KNO_INT(off));}
      ch = string_ref(string+off);}
    if ((flags)&(KNO_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return EMPTY;
}
static u8_byteoff ispunct_search
   (lispval pat,kno_lexenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (u8_ispunct(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}


static lispval isprint_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if (u8_isprint(ch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval isprint_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  u8_unichar ch = string_ref(string+off);
  if (u8_isprint(ch)) {
    while (u8_isprint(ch)) {
      off = forward_char(string,off);
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(off);
      else {CHOICE_ADD(match_points,KNO_INT(off));}
      ch = string_ref(string+off);}
    if ((flags)&(KNO_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return EMPTY;
}
static u8_byteoff isprint_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (u8_isprint(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

static lispval iscntrl_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if (u8_isctrl(ch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval iscntrl_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  u8_unichar ch = string_ref(string+off);
  if (u8_isctrl(ch)) {
    while ((u8_isctrl(ch)) && (off<lim)) {
      off = forward_char(string,off);
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(off);
      else {CHOICE_ADD(match_points,KNO_INT(off));}
      ch = string_ref(string+off);}
    if ((flags)&(KNO_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return EMPTY;
}
static u8_byteoff iscntrl_search
   (lispval pat,kno_lexenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (u8_isctrl(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

/* ISLOWER methods */

static lispval islower_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if (u8_islower(ch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval islower_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  u8_unichar ch = string_ref(string+off);
  if (u8_islower(ch)) {
    while ((u8_islower(ch)) && (off<lim)) {
      off = forward_char(string,off);
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(off);
      else {CHOICE_ADD(match_points,KNO_INT(off));}
      ch = string_ref(string+off);}
    if ((flags)&(KNO_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return EMPTY;
}
static u8_byteoff islower_search
   (lispval pat,kno_lexenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (u8_islower(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

/* ISNOTLOWER methods */

#define isnotlower(c) ((u8_isprint(c))&&(!(u8_islower(c))))

static lispval isnotlower_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if (isnotlower(ch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval isnotlower_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  u8_unichar ch = string_ref(string+off);
  if (isnotlower(ch)) {
    while ((isnotlower(ch)) && (off<lim)) {
      off = forward_char(string,off);
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(off);
      else {CHOICE_ADD(match_points,KNO_INT(off));}
      ch = string_ref(string+off);}
    if ((flags)&(KNO_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return EMPTY;
}
static u8_byteoff isnotlower_search
   (lispval pat,kno_lexenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (isnotlower(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

/* ISUPPER methods */

static lispval isupper_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if (u8_isupper(ch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval isupper_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  u8_unichar ch = string_ref(string+off);
  if (u8_isupper(ch)) {
    while ((u8_isupper(ch)) && (off<lim)) {
      off = forward_char(string,off);
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(off);
      else {CHOICE_ADD(match_points,KNO_INT(off));}
      ch = string_ref(string+off);}
    if ((flags)&(KNO_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return EMPTY;
}
static u8_byteoff isupper_search
   (lispval pat,kno_lexenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (u8_isupper(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

/* ISNOTUPPER methods */

#define isnotupper(c) ((u8_isprint(c))&&(!(u8_isupper(c))))

static lispval isnotupper_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_unichar ch = string_ref(string+off);
  if (isnotupper(ch)) return KNO_INT(forward_char(string,off));
  else return EMPTY;
}
static lispval isnotupper_plus_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval match_points = EMPTY;
  u8_unichar ch = string_ref(string+off);
  if (isnotupper(ch)) {
    while ((isnotupper(ch)) && (off<lim)) {
      off = forward_char(string,off);
      if (flags&KNO_MATCH_BE_GREEDY)
        match_points = KNO_INT(off);
      else {CHOICE_ADD(match_points,KNO_INT(off));}
      ch = string_ref(string+off);}
    if ((flags)&(KNO_MATCH_BE_GREEDY))
      return get_longest_match(match_points);
    else return match_points;}
  else return EMPTY;
}
static u8_byteoff isnotupper_search
   (lispval pat,kno_lexenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    u8_unichar ch = string_ref(s);
    if (isnotupper(ch)) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

/* Matching compound words */

static lispval compound_word_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval embpunc = kno_get_arg(pat,1);
  const u8_byte *embstr=
    ((STRINGP(embpunc) ? (CSTRING(embpunc)) : ((const u8_byte *)",-/.")));
  const u8_byte *scan = string+off, *limit = string+lim, *end;
  u8_unichar ch = u8_sgetc(&scan);
  if (u8_isalnum(ch)) {
    end = scan;
    while (scan < limit) {
      while (u8_isalnum(ch)) {end = scan; ch = u8_sgetc(&scan);}
      if (strchr(embstr,ch)) {
        ch = u8_sgetc(&scan);
        if (u8_isalnum(ch)) continue;
        else return KNO_INT(end-string);}
      else return KNO_INT(end-string);}
    return KNO_INT(end-string);}
  else return EMPTY;
}

/* Lisp Symbols */

#define islsym(c)                                                  \
   (!((u8_isspace(c)) || (c == '"') || (c == '(') || (c == '{') || \
      (c == '[') || (c == ')') || (c == '}') || (c == ']')))

static int lsymbol_startp(u8_string string,u8_byteoff off)
{
  u8_unichar ch = get_previous_char(string,off);
  if (ch < 0) return 1;
  else if (!(islsym(ch))) return 1;
  else return 0;
}

static lispval islsym_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_byteoff i = off;
  if (lsymbol_startp(string,off) == 0) return EMPTY;
  while (i < lim) {
    u8_unichar ch = string_ref(string+i);
    if (islsym(ch)) i = forward_char(string,i);
    else break;}
  if (i > off) return KNO_INT(i);
  else return EMPTY;
}
static u8_byteoff islsym_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *limit = string+lim;
  int good_start = lsymbol_startp(string,off);
  while (scan < limit) {
    const u8_byte *prev = scan; u8_unichar ch = u8_sgetc(&scan);
    if ((good_start) && (islsym(ch))) return prev-string;
    if (islsym(ch)) good_start = 0; else good_start = 1;}
  return -1;
}

/* C Symbols */

static u8_byteoff csymbol_startp(u8_string string,u8_byteoff off)
{
  u8_unichar ch = get_previous_char(string,off);
  if (ch < 0) return 1;
  else if ((u8_isalnum(ch)) || (ch == '_')) return 0;
  else return 1;
}

static lispval iscsym_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_byteoff i = off; u8_unichar ch = string_ref(string+off);
  if (csymbol_startp(string,off) == 0) return EMPTY;
  if ((u8_isalpha(ch)) || (ch == '_'))
    while (i < lim) {
      u8_unichar ch = string_ref(string+i);
      if ((u8_isalnum(ch)) || (ch == '_')) i = forward_char(string,i);
      else break;}
  if (i > off) return KNO_INT(i);
  else return EMPTY;
}
static u8_byteoff iscsym_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *limit = string+lim;
  int good_start = csymbol_startp(string,off);
  while (scan < limit) {
    const u8_byte *prev = scan; u8_unichar ch = u8_sgetc(&scan);
    if ((good_start) && (u8_isalpha(ch))) return prev-string;
    if (u8_isalnum(ch) || (ch == '_'))
      good_start = 0; else good_start = 1;}
  return -1;
}

/* C Symbols */

#define ispathelt(ch) ((u8_isalnum(ch)) || (ch == '_') || (ch == '-') || (ch == '.'))

static lispval ispathelt_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  u8_byteoff i = off; u8_unichar ch = string_ref(string+off);
  if (ispathelt(ch))
    while (i < lim) {
      u8_unichar ch = string_ref(string+i);
      if (ispathelt(ch)) i = forward_char(string,i);
      else break;}
  if (i > off) return KNO_INT(i);
  else return EMPTY;
}
static u8_byteoff ispathelt_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *oscan = scan, *limit = string+lim;
  int ch = u8_sgetc(&scan);
  while (scan<limit) {
    if (ispathelt(ch)) return oscan-string;
    oscan = scan; ch = u8_sgetc(&scan);}
  return -1;
}

/* Mail Identifiers */

#define isprinting(x) ((u8_isalnum(x)) || (u8_ispunct(x)))

#define is_not_mailidp(c) \
  (((c<128) && (!(isprinting(c)))) || \
   (u8_isspace(c)) || (c == '<') || \
   (c == ',') || (c == '(') || (c == '>') || (c == '<'))

static lispval ismailid_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim; u8_byteoff atsign = 0;
  while (s < sl) 
    if (*s == '@') {atsign = s-string; s++;}
    else {
      u8_unichar ch = string_ref(s);
      if (*s < 0x80) s++;
      else s = u8_substring(s,1);
      if (is_not_mailidp(ch)) break;}
  if ((atsign) && (!(s == string+atsign+1)))
    if (s == sl) return KNO_INT((s-string)-1);
    else return KNO_INT((s-string));
  else return EMPTY;
}

#define ismailid(x) ((x<128) && ((isalnum(x)) || (strchr(".-_",x) != NULL)))

static u8_byteoff ismailid_search
   (lispval pat,kno_lexenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *start = string+off, *slim = string+lim;
  const u8_byte *atsign = strchr(start,'@');
  while ((atsign) && (atsign < slim)) {
    if (atsign == start) start = atsign+1;
    else if (!(ismailid(atsign[0]))) start = atsign+1;
    else {
      const u8_byte *s = atsign-1; lispval match;
      while (s > start) 
        if (!(ismailid(*s))) break; else s--;
      if (s != start) s++;
      match = ismailid_match(pat,VOID,NULL,string,s-string,lim,flags);
      if (!(EMPTYP(match))) {
        kno_decref(match); return s-string;}
      else start = atsign+1;}
    atsign = strchr(start,'@');}
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

static lispval xmlname_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *oscan = scan, *limit = string+lim;
  int ch = u8_sgetc(&scan);
  if (!(xmlnmstartcharp(ch))) return EMPTY;
  else while ((scan<limit) && (xmlnmcharp(ch))) {
      oscan = scan; ch = u8_sgetc(&scan);}
  if (xmlnmcharp(ch)) return KNO_INT(scan-string);
  else return KNO_INT(oscan-string);
}
static u8_byteoff xmlname_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *oscan = scan, *limit = string+lim;
  int ch = u8_sgetc(&scan);
  while (scan<limit) {
    if (xmlnmstartcharp(ch)) break;
    oscan = scan; ch = u8_sgetc(&scan);}
  if (xmlnmstartcharp(ch)) return oscan-string;
  else return -1;
}

static lispval xmlnmtoken_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *oscan = scan, *limit = string+lim;
  int ch = u8_sgetc(&scan);
  if (!(xmlnmcharp(ch))) return EMPTY;
  else while ((scan<limit) && (xmlnmcharp(ch))) {
      oscan = scan; ch = u8_sgetc(&scan);}
  if (xmlnmcharp(ch)) return KNO_INT(scan-string);
  else return KNO_INT(oscan-string);
}
static u8_byteoff xmlnmtoken_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *oscan = scan, *limit = string+lim;
  int ch = u8_sgetc(&scan);
  while (scan<limit) {
    if (xmlnmcharp(ch)) break;
    oscan = scan; ch = u8_sgetc(&scan);}
  if (xmlnmcharp(ch))
    if (scan<limit) return KNO_INT(scan-string);
    else if (oscan<limit) return KNO_INT(oscan-string);
    else return EMPTY;
  else return EMPTY;
}

#define htmlidcharp(ch) \
  ((isalnum(ch)) || (ch=='_') || (ch=='.') || (ch==':') || (ch=='-'))
static lispval htmlid_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *oscan = scan, *limit = string+lim;
  int ch = u8_sgetc(&scan);
  if ((ch>0x80) || (!(isalpha(ch)))) return EMPTY;
  else while ((scan<limit) && (ch<0x80) && (htmlidcharp(ch))) {
      oscan = scan; ch = u8_sgetc(&scan);}
  if (htmlidcharp(ch)) return KNO_INT(scan-string);
  else return KNO_INT(oscan-string);
}
static u8_byteoff htmlid_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *oscan = scan, *limit = string+lim;
  int ch = u8_sgetc(&scan);
  while (scan<limit)  {
    if ((ch<0x80) && (isalpha(ch))) break;
    oscan = scan; ch = u8_sgetc(&scan);}
  if ((ch<0x80) && (isalpha(ch)))
    if (scan<limit) return KNO_INT(scan-string);
    else if (oscan<limit) return KNO_INT(oscan-string);
    else return EMPTY;
  else return EMPTY;
}

/* Word matching */

static lispval aword_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *slim = string+lim, *last = scan;
  u8_unichar ch = u8_sgetc(&scan), lastch = -1;
  int allupper = u8_isupper(ch), dotcount = 0;
  if (word_startp(string,off) == 0) return EMPTY;
  while ((scan<=slim) &&
         ((u8_isalpha(ch)) || (apostrophep(ch)) || (dashp(ch)) ||
          ((allupper)&&(ch=='.')))) {
    const u8_byte *prev = scan; u8_unichar nextch = u8_sgetc(&scan);
    if (u8_islower(nextch)) allupper = 0;
    if (nextch=='.') dotcount++;
    if ((!(u8_isalpha(ch)))&&(!(u8_isalpha(nextch)))) {
      if ((nextch=='.')||(apostrophep(nextch))) last = prev;
      break;}
    else {lastch = ch; ch = nextch; last = prev;}}
  if (last > string+off)
    if (lastch=='.')
      if ((allupper)&&(dotcount>1))
        return KNO_INT(last-string);
      else return KNO_INT((last-1)-string);
    else return KNO_INT(last-string);
  else return EMPTY;
}
static u8_byteoff aword_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *limit = string+lim;
  int good_start = word_startp(string,off);
  while (scan < limit) {
    const u8_byte *prev = scan; u8_unichar ch = u8_sgetc(&scan);
    if ((good_start) && (u8_isalpha(ch))) return prev-string;
    if ((u8_isspace(ch))||((u8_ispunct(ch))&&(strchr("-./_",ch) == NULL)))
      good_start = 1; else good_start = 0;}
  return -1;
}

static lispval lword_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *slim = string+lim, *last = scan;
  u8_unichar ch = u8_sgetc(&scan);
  if (word_startp(string,off) == 0) return EMPTY;
  if (!(u8_islower(ch))) return EMPTY;
  while ((scan<=slim) && ((u8_isalpha(ch)) || (apostrophep(ch)) || (dashp(ch)))) {
    u8_unichar nextch; const u8_byte *prev = scan; nextch = u8_sgetc(&scan);
    if ((!(u8_isalpha(ch)))&&(!(u8_isalpha(nextch)))) {
      if (apostrophep(ch)) last = prev;
      break;}
    else {ch = nextch; last = prev;}}
  if (last > string+off) return KNO_INT(last-string);
  else return EMPTY;
}
static u8_byteoff lword_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *limit = string+lim;
  int good_start = word_startp(string,off);
  while (scan < limit) {
    const u8_byte *prev = scan; u8_unichar ch = u8_sgetc(&scan);
    if ((good_start) && (u8_islower(ch))) return prev-string;
    if ((u8_isspace(ch))||
        ((u8_ispunct(ch))&&
         (strchr("-./_'",ch) == NULL)&&
         (!(apostrophep(ch)))&&(!(dashp(ch)))))
      good_start = 1; else good_start = 0;}
  return -1;
}

static lispval capword_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval matches = EMPTY;
  const u8_byte *scan = string+off, *last = scan, *slim = string+lim;
  u8_unichar ch = u8_sgetc(&scan), lastch = -1;
  int greedy = ((flags)&(KNO_MATCH_BE_GREEDY)), allupper = 1, dotcount = 0;
  if (word_startp(string,off) == 0) return EMPTY;
  if (!(u8_isupper(ch))) return matches;
  while ((scan<=slim) &&
         ((u8_isalpha(ch)) || (apostrophep(ch)) || (dashp(ch)) ||
          ((allupper)&&(ch=='.')))) {
    const u8_byte *prev = scan; u8_unichar nextch = u8_sgetc(&scan); 
    /* Handle embedded dots */
    if (u8_islower(nextch)) allupper = 0;
    if (nextch=='.') dotcount++;
    if ((!(greedy))&&(dashp(ch))) {
      CHOICE_ADD(matches,KNO_INT(prev-string));}
    if ((!(u8_isalpha(ch)))&&(!(u8_isalpha(nextch)))) {
      if ((nextch=='.')||(apostrophep(nextch))) last = prev;
      break;}
    else {lastch = ch; ch = nextch; last = prev;}}
  if (last > string+off) {
    if (lastch=='.')
      if ((allupper)&&(dotcount>1)) {
        CHOICE_ADD(matches,KNO_INT(last-string));}
      else {CHOICE_ADD(matches,KNO_INT((last-1)-string));}
    else {
      CHOICE_ADD(matches,KNO_INT(last-string));}}
  if (greedy)
    return get_longest_match(matches);
  else return matches;
}

static u8_byteoff capword_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *scan = string+off, *limit = string+lim;
  int good_start = word_startp(string,off);
  while (scan < limit) {
    const u8_byte *prev = scan; u8_unichar ch = u8_sgetc(&scan);
    if ((good_start) && (u8_isupper(ch))) return prev-string;
    if ((u8_isspace(ch))||((u8_ispunct(ch))&&(strchr("-./_",ch) == NULL)))
        good_start = 1; else good_start = 0;}
  return -1;
}

#define isoctdigit(x) ((x<128) && (isdigit(x)) && (x < '8'))

static lispval anumber_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval base_arg = kno_get_arg(pat,1);
  lispval sep_arg = kno_get_arg(pat,2);
  int base = ((KNO_UINTP(base_arg)) ? (FIX2INT(base_arg)) : (10));
  u8_string sepchars=
    ((VOIDP(sep_arg)) ? ((u8_string)".,") :
     (STRINGP(sep_arg)) ? (CSTRING(sep_arg)) : ((u8_string)(NULL)));
  const u8_byte *scan = string+off, *slim = string+lim, *last = scan;
  u8_unichar prev_char = get_previous_char(string,off), ch = u8_sgetc(&scan);
  if (u8_isdigit(prev_char)) return EMPTY;
  if (base == 8)
    while ((scan<=slim) && (ch<0x80) && (isoctdigit(ch))) {
      last = scan; ch = u8_sgetc(&scan);}
  else if (base == 16)
    while ((scan<=slim) && (ch<0x80) && (isxdigit(ch))) {
      last = scan; ch = u8_sgetc(&scan);}
  else if (base == 2)
    while ((scan<=slim) && ((ch == '0') || (ch == '1'))) {
      last = scan; ch = u8_sgetc(&scan);}
  else while ((scan<=slim) && (ch<0x80) &&
              ((u8_isdigit(ch)) || ((sepchars)&&(strchr(sepchars,ch))))) {
    last = scan; ch = u8_sgetc(&scan);}
  if (last > string+off) return KNO_INT(last-string);
  else return EMPTY;
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
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval base_arg = kno_get_arg(pat,1);
  int base = ((KNO_UINTP(base_arg)) ? (FIX2INT(base_arg)) : (10));
  const u8_byte *scan = string+off, *limit = string+lim;
  while (scan < limit) {
    const u8_byte *prev = scan; u8_unichar ch = u8_sgetc(&scan);
    if (check_digit(ch,base)) return prev-string;}
  return -1;
}

/* Hashset matches */

static kno_hashset to_hashset(lispval arg)
{
  if (TYPEP(arg,kno_hashset_type)) 
    return (kno_hashset)arg;
  else return NULL;
}

static lispval hashset_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval hs = kno_get_arg(pat,1);
  lispval cpat = kno_get_arg(pat,2);
  lispval xform = kno_get_arg(pat,3);
  if ((VOIDP(hs)) || (VOIDP(cpat)))
    return kno_err(kno_MatchSyntaxError,"hashset_match",NULL,pat);
  else if (!(TYPEP(hs,kno_hashset_type)))
    return kno_type_error(_("hashset"),"hashset_match",pat);
  if (VOIDP(xform)) {
    kno_hashset h = to_hashset(hs);
    lispval iresults = kno_text_domatch(cpat,next,env,string,off,lim,flags);
    lispval results = EMPTY;
    DO_CHOICES(possibility,iresults)
      if (hashset_strget(h,string+off,kno_getint(possibility)-off)) {
        kno_incref(possibility);
        CHOICE_ADD(results,possibility);}
    kno_decref(iresults);
    return get_longest_match(results);}
  else {
    kno_hashset h = to_hashset(hs);
    lispval iresults = kno_text_domatch(cpat,next,env,string,off,lim,flags);
    lispval results = EMPTY;
    {DO_CHOICES(possibility,iresults) {
        lispval origin = kno_extract_string
          (NULL,string+off,string+kno_getint(possibility));
        lispval xformed = match_apply(xform,"HASHSET-MATCH",env,1,&origin);
        if (kno_hashset_get(h,xformed)) {
          kno_incref(possibility); CHOICE_ADD(results,possibility);}
        kno_decref(xformed); kno_decref(origin);}}
    kno_decref(iresults);
    return get_longest_match(results);}
}

static u8_byteoff hashset_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval hs = kno_get_arg(pat,1);
  lispval cpat = kno_get_arg(pat,2);
  if ((VOIDP(hs)) || (VOIDP(cpat)))
    return kno_err(kno_MatchSyntaxError,"hashset_search",NULL,pat);
  else if (!(TYPEP(hs,kno_hashset_type)))
    return kno_type_error(_("hashset"),"hashset_search",pat);
  else {
    u8_byteoff try = kno_text_search(cpat,env,string,off,lim,flags);
    while ((try >= 0) && (try < lim)) {
      lispval matches = hashset_match(pat,VOID,env,string,try,lim,flags);
      if (KNO_ABORTED(matches)) return -2;
      else if (!(EMPTYP(matches))) {
        kno_decref(matches); 
        return try;}
      else try = kno_text_search(cpat,env,string,
                              forward_char(string,try),lim,flags);}
    return try;}
}
  
/* HASHSET-NOT */

static lispval hashset_not_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval hs = kno_get_arg(pat,1);
  lispval cpat = kno_get_arg(pat,2);
  lispval xform = kno_get_arg(pat,3);
  if ((VOIDP(hs)) || (VOIDP(cpat)))
    return kno_err(kno_MatchSyntaxError,"hashset_not_match",NULL,pat);
  else if (!(TYPEP(hs,kno_hashset_type)))
    return kno_type_error(_("hashset"),"hashset_not_match",pat);
  if (VOIDP(xform)) {
    kno_hashset h = to_hashset(hs);
    lispval iresults = kno_text_domatch(cpat,next,env,string,off,lim,flags);
    lispval results = EMPTY;
    DO_CHOICES(possibility,iresults)
      if (hashset_strget(h,string+off,kno_getint(possibility)-off)) {}
      else {kno_incref(possibility); CHOICE_ADD(results,possibility);}
    kno_decref(iresults);
    return get_longest_match(results);}
  else {
    kno_hashset h = to_hashset(hs);
    lispval iresults = kno_text_domatch(cpat,next,env,string,off,lim,flags);
    lispval results = EMPTY;
    {DO_CHOICES(possibility,iresults) {
        lispval origin = kno_extract_string
          (NULL,string+off,string+kno_getint(possibility));
        lispval xformed = match_apply(xform,"HASHSET-NOT-MATCH",env,1,&origin);
        if (KNO_ABORTED(xformed)) {
          KNO_STOP_DO_CHOICES;
          kno_decref(results);
          return xformed;}
        else if ((!(STRINGP(xformed)))||(!(kno_hashset_get(h,xformed)))) {
          kno_incref(possibility); CHOICE_ADD(results,possibility);}
        kno_decref(xformed); kno_decref(origin);}}
    kno_decref(iresults);
    return get_longest_match(results);}
}

static u8_byteoff hashset_not_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval hs = kno_get_arg(pat,1);
  lispval cpat = kno_get_arg(pat,2);
  if ((VOIDP(hs)) || (VOIDP(cpat)))
    return kno_err(kno_MatchSyntaxError,"hashset_not_search",NULL,pat);
  else if (!(TYPEP(hs,kno_hashset_type)))
    return kno_type_error(_("hashset"),"hashset_not_search",pat);
  else {
    u8_byteoff try = kno_text_search(cpat,env,string,off,lim,flags);
    while ((try >= 0) && (try < lim)) {
      lispval matches = hashset_not_match(pat,VOID,env,string,try,lim,flags);
      if (KNO_ABORTED(matches)) return -2;
      else if (!(EMPTYP(matches))) {
        kno_decref(matches); 
        return try;}
      else try=
             kno_text_search(cpat,env,string,
                            forward_char(string,try),lim,flags);}
    return try;}
}

/* Proc matches */

static lispval applytest_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval cpat = kno_get_arg(pat,1);
  lispval proc = kno_get_arg(pat,2);
  if ((VOIDP(proc)) || (VOIDP(proc)))
    return kno_err(kno_MatchSyntaxError,"proc_match",NULL,cpat);
  else if (!(KNO_APPLICABLEP(proc)))
    return kno_type_error(_("applicable"),"proc_match",proc);
  else {
    lispval iresults = kno_text_domatch
      (cpat,next,env,string,off,lim,(flags&(~KNO_MATCH_BE_GREEDY)));
    lispval results = EMPTY;
    {DO_CHOICES(possibility,iresults) {
        lispval substring = kno_extract_string
          (NULL,string+off,string+kno_getint(possibility));
        lispval match = kno_apply(proc,1,&substring);
        if (!((FALSEP(match))||(EMPTYP(match))||(VOIDP(match)))) {
          kno_incref(possibility); CHOICE_ADD(results,possibility);}
        kno_decref(substring); kno_decref(match);}}
    kno_decref(iresults);
    if (flags&KNO_MATCH_BE_GREEDY)
      return get_longest_match(results);
    else return results;}
}

static u8_byteoff applytest_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval cpat = kno_get_arg(pat,1);
  lispval proc = kno_get_arg(pat,2);
  if ((VOIDP(proc)) || (VOIDP(proc)))
    return kno_err(kno_MatchSyntaxError,"proc_match",NULL,cpat);
  else if (!(KNO_APPLICABLEP(proc)))
    return kno_type_error(_("applicable"),"proc_match",proc);
  else {
    u8_byteoff try = kno_text_search(cpat,env,string,off,lim,flags);
    while ((try >= 0) && (try < lim)) {
      lispval matches = applytest_match(pat,VOID,env,string,try,lim,flags);
      if (KNO_ABORTED(matches)) return -2;
      else if (!(EMPTYP(matches))) {
        kno_decref(matches); 
        return try;}
      else try = kno_text_search(cpat,env,string,
                              forward_char(string,try),lim,flags);}
    return try;}
}
  
/* MAXLEN matches */

static lispval maxlen_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval cpat = kno_get_arg(pat,1);
  lispval lim_arg = kno_get_arg(pat,2);
  if ((VOIDP(lim_arg)) || (VOIDP(cpat)))
    return kno_err(kno_MatchSyntaxError,"maxlen_match",NULL,pat);
  else if (!(KNO_UINTP(lim_arg)))
    return kno_type_error(_("fixnum"),"maxlen_match",pat);
  else {
    int maxlen = FIX2INT(lim_arg);
    int maxbytes = u8_byteoffset(string+off,maxlen,lim);
    int newlim = ((maxbytes<0) ? (lim) : (off+maxbytes));
    return kno_text_domatch(cpat,next,env,string,off,newlim,flags);}
}

static u8_byteoff maxlen_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval cpat = kno_get_arg(pat,1);
  lispval lim_arg = kno_get_arg(pat,2);
  if ((VOIDP(lim_arg)) || (VOIDP(cpat)))
    return kno_err(kno_MatchSyntaxError,"maxlen_search",NULL,pat);
  else if (!(FIXNUMP(lim_arg)))
    return kno_type_error(_("fixnum"),"maxlen_search",pat);
  else {
    u8_byteoff try = kno_text_search(cpat,env,string,off,lim,flags);
    while ((try >= 0) && (try < lim)) {
      lispval matches = maxlen_match(pat,VOID,env,string,try,lim,flags);
      if (KNO_ABORTED(matches)) return -2;
      else if (!(EMPTYP(matches))) {
        kno_decref(matches); return try;}
      else try = kno_text_search(cpat,env,string,
                              forward_char(string,try),
                              lim,flags);}
    return try;}
}

/* MINLEN matches */

static lispval minlen_match
  (lispval pat,lispval next,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval cpat = kno_get_arg(pat,1);
  lispval lim_arg = kno_get_arg(pat,2);
  lispval inner_results = EMPTY;
  int min_len = -1;
  if (VOIDP(cpat))
    return kno_err(kno_MatchSyntaxError,"maxlen_match",NULL,pat);
  else if (VOIDP(lim_arg))
    min_len = 1;
  else if (!(KNO_UINTP(lim_arg)))
    return kno_type_error(_("fixnum"),"maxlen_match",pat);
  else {
    min_len = FIX2INT(lim_arg);
    if (min_len<=0)
      return kno_type_error(_("positive fixnum"),"minlen_match",pat);}
  inner_results = kno_text_domatch(cpat,next,env,string,off,lim,flags);
  if (KNO_ABORTED(inner_results)) return inner_results;
  else if (EMPTYP(inner_results)) return inner_results;
  else if (KNO_UINTP(inner_results)) {
    if ((FIX2INT(inner_results)-off)<min_len)
      return EMPTY;
    else return inner_results;}
  else {
    lispval results = EMPTY;
    DO_CHOICES(r,inner_results) {
      if (KNO_UINTP(r)) {
        int rint = FIX2INT(r); int diff = rint-off;
        if (diff>=min_len) {CHOICE_ADD(results,r);}}}
    kno_decref(inner_results);
    return results;}
}

static u8_byteoff minlen_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  lispval cpat = kno_get_arg(pat,1);
  lispval lim_arg = kno_get_arg(pat,2);
  if (VOIDP(cpat))
    return kno_err(kno_MatchSyntaxError,"minlen_search",NULL,pat);
  else if (VOIDP(lim_arg)) {}
  else if (!(FIXNUMP(lim_arg)))
    return kno_type_error(_("fixnum"),"minlen_search",pat);
  else if ((FIX2INT(lim_arg))<=0)
    return kno_type_error(_("positive fixnum"),"minlen_search",pat);
  {
    u8_byteoff try = kno_text_search(cpat,env,string,off,lim,flags);
    while ((try >= 0) && (try < lim)) {
      lispval matches = minlen_match(pat,VOID,env,string,try,lim,flags);
      if (KNO_ABORTED(matches)) return -2;
      else if (!(EMPTYP(matches))) {
        kno_decref(matches);
        return try;}
      else try = kno_text_search(cpat,env,string,
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
  const u8_byte *scan = pat, *limit;
  int first_char = u8_sgetc(&scan), use_buf = 0, c, cand;
  if ((flags&KNO_MATCH_COLLAPSE_SPACES) &&
      ((flags&(KNO_MATCH_IGNORE_CASE|KNO_MATCH_IGNORE_DIACRITICS)) == 0)) {
    struct U8_OUTPUT os; scan = pat; c = u8_sgetc(&scan);
    U8_INIT_STATIC_OUTPUT_BUF(os,64,buf);
    while ((c>0) && (!(u8_isspace(c)))) {
      u8_putc(&os,c); c = u8_sgetc(&scan);}
    use_buf = 1;}
  else first_char = reduce_char(first_char,flags);
  scan = string+off; limit = string+lim; cand = scan-string;
  while (scan < limit) {
    if (use_buf) {
      const u8_byte *next = strstr(scan,buf);
      if (next) {
        cand = next-string; 
        scan = next; 
        u8_sgetc(&scan);}
      else return NULL;}
    else {
      c = u8_sgetc(&scan);
      if (c<0) return NULL;
      c = reduce_char(c,flags);
      while ((c != first_char) && (scan < limit)) {
        cand = scan-string;
        c = u8_sgetc(&scan);
        if (c<0) return NULL;
        c = reduce_char(c,flags);}
      if (c != first_char) return NULL;}
    if (cand > lim) return NULL;
    else {
      u8_byteoff matchlen=
        strmatcher(flags,pat,patlen,string,cand,lim);
      if (matchlen>0) return string+cand;}}
  return NULL;
}    

static u8_byteoff slow_search
   (lispval pat,kno_lexenv env,
    u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  const u8_byte *s = string+off, *sl = string+lim;
  while (s < sl) {
    lispval result = kno_text_matcher(pat,env,string,s-string,lim,flags);
    if (KNO_ABORTED(result)) return -2;
    else if (!(EMPTYP(result))) return s-string;
    else if (*s < 0x80) s++;
    else s = u8_substring(s,1);}
  return -1;
}

/* Top level functions */

KNO_EXPORT
u8_byteoff kno_text_search
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff lim,int flags)
{
  if (KNO_INTERRUPTED()) return -2;
  else if (off > lim) return -1;
  else if (EMPTYP(pat)) return -1;
  else if (STRINGP(pat)) {
    const u8_byte *next;
    if (flags&(KNO_MATCH_SPECIAL))
      next = strsearch(flags,CSTRING(pat),STRLEN(pat),
                     string,off,lim);
    else {
      next = strstr(string+off,CSTRING(pat));
      if (next>=string+lim) next = NULL;}
    if ((next) && (next<string+lim))
      return next-string;
    else return -1;}
  else if (VECTORP(pat)) {
    lispval initial = VEC_REF(pat,0);
    u8_byteoff start = kno_text_search(initial,env,string,off,lim,flags);
    while ((start >= 0) && (start < lim)) {
      lispval m = kno_text_matcher(pat,env,string,start,lim,flags);
      if (KNO_ABORTED(m)) {
        kno_interr(m);
        return -2;}
      if (!(EMPTYP(m))) {
        kno_decref(m); return start;}
      else start = kno_text_search
             (initial,env,string,forward_char(string,start),lim,flags);}
    if (start== -2) return start;
    else if (start<lim) return start;
    else return -1;}
  else if (KNO_CHARACTERP(pat)) {
    u8_unichar c = KNO_CHAR2CODE(pat);
    if (c < 0x80) {
      u8_byte c = string[lim], *next;
      next = strchr(string+off,c);
      if (next>=string+lim) next = NULL;
      if (next) return next-string;
      else return -1;}
    else {
      const u8_byte *s = string+off, *sl = string+lim;
      while (s < sl) {
        u8_unichar ch = string_ref(s);
        if (ch == c) return s-string;
        else if (*s < 0x80) s++;
        else s = u8_substring(s,1);}
      return -1;}}
  else if ((CHOICEP(pat)) || (PRECHOICEP(pat))) {
    int nlim = lim, loc = -1;
    DO_CHOICES(epat,pat) {
      u8_byteoff nxt = kno_text_search(epat,env,string,off,lim,flags);
      if (nxt < 0) {
        if (nxt== -2) {
          KNO_STOP_DO_CHOICES;
          return nxt;}}
      else if (nxt < nlim) {nlim = nxt; loc = nxt;}}
    return loc;}
  else if (PAIRP(pat)) {
    lispval head = KNO_CAR(pat);
    struct KNO_TEXTMATCH_OPERATOR
      *scan = match_operators, *limit = scan+n_match_operators;
    while (scan < limit)
      if (KNO_EQ(scan->kno_matchop,head)) break; else scan++; 
    if (scan < limit)
      if (scan->kno_searcher)
        return scan->kno_searcher(pat,env,string,off,lim,flags);
      else return slow_search(pat,env,string,off,lim,flags);
    else {
      kno_seterr(kno_MatchSyntaxError,"kno_text_search",NULL,kno_incref(pat));
      return -2;}}
  else if (SYMBOLP(pat)) {
    lispval vpat = match_eval(pat,env);
    if (KNO_ABORTED(vpat)) {
      kno_interr(vpat);
      return -2;}
    else if (VOIDP(vpat)) {
      u8_string name = SYM_NAME(pat);
      kno_seterr(kno_UnboundIdentifier,"kno_text_search",name,pat);
      return -2;}
    else {
      u8_byteoff result = kno_text_search(vpat,env,string,off,lim,flags);
      kno_decref(vpat); return result;}}
  else if (TYPEP(pat,kno_txclosure_type)) {
    struct KNO_TXCLOSURE *txc = (kno_txclosure)(pat);
    return kno_text_search(txc->kno_txpattern,txc->kno_txenv,string,off,lim,flags);}
  else if (TYPEP(pat,kno_regex_type))  {
    int retval = kno_regex_op(rx_search,pat,string+off,lim-off,0);
    if (retval<0) return retval;
    else return retval+off;}
  else {
    kno_seterr(kno_MatchSyntaxError,"kno_text_search",NULL,kno_incref(pat));
    return -2;}
}

KNO_EXPORT
int kno_text_match
  (lispval pat,kno_lexenv env,
   u8_string string,u8_byteoff off,u8_byteoff len,int flags)
{
  lispval extents = kno_text_matcher(pat,env,string,off,len,flags);
  if (KNO_ABORTED(extents)) 
    return kno_interr(extents);
  else {
    int match = 0;
    DO_CHOICES(extent,extents)
      if (FIXNUMP(extent))
        if (FIX2INT(extent) == len) {
          match = 1; KNO_STOP_DO_CHOICES; break;}
    kno_decref(extents);
    return match;}
}

KNO_EXPORT
lispval kno_textclosure(lispval expr,kno_lexenv env)
{
  struct KNO_TXCLOSURE *txc = u8_alloc(struct KNO_TXCLOSURE);
  KNO_INIT_CONS(txc,kno_txclosure_type);
  txc->kno_txpattern = kno_incref(expr); txc->kno_txenv = kno_copy_env(env);
  return (lispval) txc;
}

static int unparse_txclosure(u8_output ss,lispval x)
{
  struct KNO_TXCLOSURE *txc = (kno_txclosure)x;
  u8_printf(ss,"#<TX-CLOSURE %q>",txc->kno_txpattern);
  return 1;
}

static void recycle_txclosure(struct KNO_RAW_CONS *c)
{
  struct KNO_TXCLOSURE *txc = (kno_txclosure)c;
  kno_decref(txc->kno_txpattern); kno_decref((lispval)(txc->kno_txenv));
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

/* Defining match symbols */

KNO_EXPORT int kno_matchdef(lispval symbol,lispval value)
{
  return kno_store(match_env,symbol,value);
}

KNO_EXPORT lispval kno_matchget(lispval symbol,kno_lexenv env)
{
  return match_eval(symbol,env);
}

/** Initialization **/

void kno_init_match_c()
{
  u8_register_source_file(_FILEINFO);

  match_env = kno_make_hashtable(NULL,1024);

  kno_txclosure_type = kno_register_cons_type("txclosure");
  kno_recyclers[kno_txclosure_type]=recycle_txclosure;
  kno_unparsers[kno_txclosure_type]=unparse_txclosure;

  init_match_operators_table();
  kno_add_match_operator("*",match_star,search_star,extract_star);
  kno_add_match_operator("+",match_plus,search_plus,extract_plus);
  kno_add_match_operator("OPT",match_opt,search_opt,extract_opt);
  kno_add_match_operator("NOT",match_not,search_not,NULL);
  kno_add_match_operator("AND",match_and,search_and,NULL);
  kno_add_match_operator("NOT>",match_not_gt,search_not,NULL);
  kno_add_match_operator("CHOICE",match_choice,search_choice,extract_choice);
  kno_add_match_operator("PREF",match_pref,search_pref,extract_pref);

  kno_add_match_operator("=",match_bind,search_bind,NULL);
  kno_add_match_operator("LABEL",label_match,label_search,label_extract);
  kno_add_match_operator("SUBST",subst_match,subst_search,subst_extract);

  kno_add_match_operator("GREEDY",match_greedy,search_greedy,extract_greedy);
  kno_add_match_operator
    ("EXPANSIVE",match_expansive,search_expansive,extract_expansive);
  kno_add_match_operator
    ("LONGEST",match_longest,search_longest,extract_longest);
  kno_add_match_operator("APPLYTEST",applytest_match,applytest_search,NULL);

  kno_add_match_operator("MATCH-CASE",match_cs,search_cs,extract_cs);
  kno_add_match_operator("MC",match_cs,search_cs,extract_cs);
  kno_add_match_operator("IGNORE-CASE",match_ci,search_ci,extract_ci);
  kno_add_match_operator("IC",match_ci,search_ci,extract_ci);
  kno_add_match_operator("MATCH-DIACRITICS",match_ds,search_ds,extract_ds);
  kno_add_match_operator("MD",match_ds,search_ds,extract_ds);
  kno_add_match_operator("IGNORE-DIACRITICS",match_di,search_di,extract_di);
  kno_add_match_operator("ID",match_di,search_di,extract_di);
  kno_add_match_operator("MATCH-SPACING",match_ss,search_ss,extract_ss);
  kno_add_match_operator("MS",match_ss,search_ss,extract_ss);
  kno_add_match_operator("IGNORE-SPACING",match_si,search_si,extract_si);
  kno_add_match_operator("IS",match_si,search_si,extract_si);
  kno_add_match_operator
    ("CANONICAL",match_canonical,search_canonical,extract_canonical);

  kno_add_match_operator("BOL",match_bol,search_bol,NULL);
  kno_add_match_operator("EOL",match_eol,search_eol,NULL);
  kno_add_match_operator("BOS",match_bos,search_bos,NULL);
  kno_add_match_operator("EOS",match_eos,search_eos,NULL);
  kno_add_match_operator("BOW",match_bow,search_bow,NULL);
  kno_add_match_operator("EOW",match_eow,search_eow,NULL);
  kno_add_match_operator("CHAR-RANGE",match_char_range,NULL,NULL);
  kno_add_match_operator("CHAR-NOT",match_char_not,NULL,NULL);
  kno_add_match_operator("CHAR-NOT*",match_char_not_star,NULL,NULL);
  kno_add_match_operator("ISVOWEL",isvowel_match,NULL,NULL);
  kno_add_match_operator("ISNOTVOWEL",isnotvowel_match,NULL,NULL);
  kno_add_match_operator("ISSPACE",isspace_match,isspace_search,NULL);
  kno_add_match_operator("ISSPACE+",isspace_plus_match,isspace_search,NULL);
  kno_add_match_operator("SPACES",spaces_match,spaces_search,NULL);
  kno_add_match_operator("SPACES*",spaces_star_match,spaces_star_search,NULL);
  kno_add_match_operator("HSPACE",ishspace_match,ishspace_search,NULL);
  kno_add_match_operator("HSPACE+",ishspace_plus_match,ishspace_search,NULL);
  kno_add_match_operator("VSPACE",vspace_match,vspace_search,NULL);
  kno_add_match_operator("VSPACE+",vspace_plus_match,vspace_search,NULL);
  kno_add_match_operator("VBREAK",vbreak_match,vbreak_search,NULL);
  kno_add_match_operator("ISALNUM",isalnum_match,isalnum_search,NULL);
  kno_add_match_operator("ISALNUM+",isalnum_plus_match,isalnum_search,NULL);
  kno_add_match_operator("ISWORD",isword_match,isword_search,NULL);
  kno_add_match_operator("ISWORD+",isword_plus_match,isword_search,NULL);
  kno_add_match_operator("ISALPHA",isalpha_match,isalpha_search,NULL);
  kno_add_match_operator("ISALPHA+",isalpha_plus_match,isalpha_search,NULL);
  kno_add_match_operator("ISDIGIT",isdigit_match,isdigit_search,NULL);
  kno_add_match_operator("ISDIGIT+",isdigit_plus_match,isdigit_search,NULL);
  kno_add_match_operator("ISPRINT",isprint_match,isprint_search,NULL);
  kno_add_match_operator("ISPRINT+",isprint_plus_match,isprint_search,NULL);
  kno_add_match_operator("ISXDIGIT",isxdigit_match,isxdigit_search,NULL);
  kno_add_match_operator("ISXDIGIT+",isxdigit_plus_match,isxdigit_search,NULL);
  kno_add_match_operator("ISODIGIT",isodigit_match,isodigit_search,NULL);
  kno_add_match_operator("ISODIGIT+",isodigit_plus_match,isodigit_search,NULL);
  kno_add_match_operator("ISPUNCT",ispunct_match,ispunct_search,NULL);
  kno_add_match_operator("ISPUNCT+",ispunct_plus_match,ispunct_search,NULL);
  kno_add_match_operator("ISCNTRL",iscntrl_match,iscntrl_search,NULL);
  kno_add_match_operator("ISCNTRL+",iscntrl_plus_match,iscntrl_search,NULL);
  kno_add_match_operator("ISUPPER",isupper_match,isupper_search,NULL);
  kno_add_match_operator("ISUPPER+",isupper_plus_match,isupper_search,NULL);
  kno_add_match_operator("ISLOWER",islower_match,islower_search,NULL);
  kno_add_match_operator("ISLOWER+",islower_plus_match,islower_search,NULL);
  kno_add_match_operator("ISNOTUPPER",isnotupper_match,isnotupper_search,NULL);
  kno_add_match_operator("ISNOTUPPER+",
                        isnotupper_plus_match,isnotupper_search,NULL);
  kno_add_match_operator("ISNOTLOWER",isnotlower_match,isnotlower_search,NULL);
  kno_add_match_operator("ISNOTLOWER+",
                        isnotlower_plus_match,isnotlower_search,NULL);
  kno_add_match_operator("LSYMBOL",islsym_match,islsym_search,NULL);
  kno_add_match_operator("CSYMBOL",iscsym_match,iscsym_search,NULL);
  kno_add_match_operator("PATHELT",ispathelt_match,ispathelt_search,NULL);
  kno_add_match_operator("XMLNAME",xmlname_match,xmlname_search,NULL);
  kno_add_match_operator("XMLNMTOKEN",xmlnmtoken_match,xmlnmtoken_search,NULL);
  kno_add_match_operator("HTMLID",htmlid_match,htmlid_search,NULL);
  kno_add_match_operator("AWORD",aword_match,aword_search,NULL);
  kno_add_match_operator("LWORD",lword_match,lword_search,NULL);
  kno_add_match_operator("ANUMBER",anumber_match,anumber_search,NULL);
  kno_add_match_operator("CAPWORD",capword_match,capword_search,NULL);
  kno_add_match_operator
    ("COMPOUND-WORD",compound_word_match,isalnum_search,NULL);
  kno_add_match_operator("MAILID",ismailid_match,ismailid_search,NULL);

  kno_add_match_operator("CHUNK",chunk_match,chunk_search,chunk_extract);

  /* Whitespace or punctuatation separated tokens */
  kno_add_match_operator("WORD",word_match,word_search,word_extract);
  kno_add_match_operator("PHRASE",word_match,word_search,word_extract);

  kno_add_match_operator("REST",match_rest,search_rest,extract_rest);
  kno_add_match_operator("HASHSET",hashset_match,hashset_search,NULL);
  kno_add_match_operator
    ("HASHSET-NOT",hashset_not_match,hashset_not_search,NULL);

  kno_add_match_operator("MAXLEN",maxlen_match,maxlen_search,NULL);
  kno_add_match_operator("MINLEN",minlen_match,minlen_search,NULL);

  subst_symbol = kno_intern("subst");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
