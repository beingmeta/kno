/* C Mode */

/* stem.c
   This is a version of the Porter stemmer ["Program" 14(3), 1980]
    based on the description (Stemming Algorithms) by Frakes 
     in "Information Retrieval" edited by Frakes & Bazea-Yates 
     Published by Prentice-Hall.
   Minor changes to algorithm are commented.

   Copyright (C) 1998, 1999 Massachusetts Institute of Technology
   Copyright (C) 2000-2020 beingmeta, inc.

    Use, modification, and redistribution of this program is permitted
    under the terms of either (at the developer's discretion) the GNU
    General Public License (GPL) Version 2, the GNU Lesser General Public
    License.

    This program has been ported to beingmeta's Kno library.
*/

#include "kno/knosource.h"
#include "kno/lisp.h"

#include <libu8/u8stringfns.h>

#include <stdio.h>
#include <string.h>
#include <ctype.h>

#define isvowel(c) \
  (((c) == 'a') || ((c) == 'e') || ((c) == 'i') || \
   ((c) == 'o') || ((c) == 'u'))

typedef enum STEM_CONDITION
  { none, /* No condition */
    pos_measure, /* Measure of stem is positive */
    contains_vowel, /* Stem contains vowel */
    double_const, /* Stem ends with double consonant but not L, S, or Z */
    m1cvc, /* Measure is 1 & stem ends with CVC (consonant-vowel-consonant) */
    bigm, /* Measure is greater than 1 */
    bigm_ts, /* Measure is greater than one and stem ends in t or s */
    m1notcvc, /* Measure is 1 and stem doesn't end in CVC */
    big_double_l /* Measure is greater than one and stem ends in double l */
  }
 stem_condition;

struct WORD {char *spelling; int length; int changed;};

/* Checks if a word has a suffix and how long the stem would be if it
   does.  Passing in suffix length avoids having to figure out
   the length. */
static int check_suffix(struct WORD w,char *suffix,int suffix_length)
{
  if ((suffix_length < w.length) &&
      (strcmp(w.spelling+w.length-suffix_length,suffix) == 0))
    return w.length-suffix_length;
  else return 0;
}

/* Gets the *measure* of a word, which is basically the number of
    non-terminal vowel sequences in the word.  This is used in a
    number of rules. */
static int get_measure(char *string,char *end)
{
  int measure = 0;
  enum STATE {start, consonant, vowel} state = start;
  while (string < end) {
    switch (state) {
    case start:
      if (isvowel(*string)) {measure++; state = vowel;}
      else state = consonant;
      string++; break; 
    case vowel:
      if ((isvowel(*string)) || (*string == 'y')) state = vowel;
      else state = consonant;
      string++; break;
    case consonant:
      if ((isvowel(*string)) || (*string == 'y'))
	{state = vowel; measure++;}
      string++; break;}}
  if (state == vowel)
    return measure-1;
  else return measure;
}

/* Applies a rule.  Rather than putting the rules in a table, I
   have the procedure apply_rule take all of the rule parameters
   and then do a series of function calls.  This takes more space,
   but is easier to write and possibly easier to read. */
static struct WORD apply_rule
  (struct WORD w,
   stem_condition cond,char *suffix,int suffix_length,
   char *replacement, int single_letter)
{
  int offset, measure;
  char *scan, *limit;
#if (DEBUGGING)
  fprintf(stderr,"Applying rule %d:%s/%s,%d to %s(%w)\n",
	  cond,suffix,replacement,single_letter,
	  w.spelling,w.length);
#endif
  if (w.changed) return w;
  else offset = check_suffix(w,suffix,suffix_length);
#if (DEBUGGING)
  fprintf(stderr,"Offset=%d\n",offset);
#endif
  if (offset == 0) return w;
  measure = get_measure(w.spelling,w.spelling+offset);
#if (DEBUGGING)
  fprintf(stderr,"Measure=%d\n",measure);
#endif
  scan = w.spelling; limit = scan+offset;
  switch (cond)
    {
    case none: break;
    case pos_measure: if (measure == 0) return w; else break;
    case contains_vowel:
      while (scan < limit)
	if ((isvowel(*scan)) || (*scan == 'y')) break;
	else scan++;
      if ((scan < limit) && ((isvowel(*scan)) || (*scan == 'y')))
	break;
      else return w;
    case double_const:
      if ((offset > 2) &&
	  (w.spelling[offset-1] == w.spelling[offset-2]) &&
	  (isvowel(w.spelling[offset-1])) &&
	  (!((w.spelling[offset-1] == 'l') ||
	     (w.spelling[offset-1] == 's') ||
	     (w.spelling[offset-1] == 'z'))))
	break;
      else return w;
    case m1cvc:
      if ((measure == 1) &&
	  (offset > 3) &&
	  (!(isvowel(w.spelling[offset-1]))) &&
	  ((isvowel(w.spelling[offset-2])) ||
	   (w.spelling[offset-2] == 'y')) &&
	  (!(isvowel(w.spelling[offset-3]))) &&
	  (w.spelling[offset-1] != 'w') &&
	  (w.spelling[offset-1] != 'x') &&
	  (w.spelling[offset-1] != 'y'))
	break;
      else return w;
    case bigm:
      if (measure > 1) break; else return w;
    case bigm_ts:
      if (measure < 2) return w;
      else if ((w.spelling[offset-1] == 's') || (w.spelling[offset-1] == 't'))
	break;
      else return w;
    case m1notcvc:
      if ((measure == 1) &&
	  (offset > 3) &&
	  (!((!(isvowel(w.spelling[offset-1]))) &&
	     ((isvowel(w.spelling[offset-2])) ||
	      (w.spelling[offset-2] == 'y')) &&
	     (!(isvowel(w.spelling[offset-3]))) &&
	     (w.spelling[offset-1] != 'w') &&
	     (w.spelling[offset-1] != 'x') &&
	     (w.spelling[offset-1] != 'y'))))
	break;
      else return w;
    case big_double_l:
      if (measure < 2) return w;
      else if ((offset > 2) &&
	       (w.spelling[offset-1] == 'l') &&
	       (w.spelling[offset-2] == 'l'))
	break;
      else return w;	
    }
  w.changed = 1;
  if (single_letter) {
    w.spelling[offset-1]='\0'; w.length = offset-1;}
  else {
    strcpy(w.spelling+offset,replacement);
    w.length = offset+strlen(replacement);}
#if (DEBUGGING)
  fprintf(stderr,"Rule produces %s(%d)\n",w.spelling,w.length);
#endif
  return w;
}

static int canonicalize_string(const u8_byte *string,char *copy,int space)
{
  const u8_byte *read = string; char *write = copy, *limit = copy+space;
  while ((*read) && (write < limit))
    if (*read < 0x80) *write++=tolower(*read++);
    else {
      int c = u8_sgetc(&read);
      int bc = u8_base_char(c);
      int lc = u8_tolower(bc);
      if (lc < 0x80) *write++=lc;
      else *write++=((lc)&0x7f); /* Yuck */}
  if (write < limit) {*write='\0'; return write-copy;}
  else return 1;
}

KNO_EXPORT
/* kno_stem_english_word:
     Arguments: an ASCII string
     Returns: a stemmed string (malloc'd)

   Applies all the Porter rules for stemming a word.  If the word
    is too long (more than 200 characters) it just gives up.  This
    returns a malloc'd string containing the porter stem.   Note that
    the porter stem is usually not itself a word you would recognize. */
char *kno_stem_english_word(const u8_byte *original)
{
  char *copy; int do1b1 = 0, len = strlen(original);
  struct WORD w;
  /* Initialize word */
  if (len==0) return u8_strdup(original);
  else copy = u8_malloc(len+1);
  memset(copy,0,len+1);
  w.spelling = copy;
  w.length = canonicalize_string(original,copy,len+1);
  w.changed = 0;
  if (w.length == 0) return copy;
  /* Step 1a rules */
  if (w.changed == 0) w = apply_rule(w,none,"sses",4,"ss",0);
  if (w.changed == 0) w = apply_rule(w,none,"ies",3,"i",0);
  if (w.changed == 0) w = apply_rule(w,none,"ss",2,"ss",0);
  if (w.changed == 0) w = apply_rule(w,none,"s",1,"",0);
  /* Step 1b rules */
  w.changed = 0;
  if (w.changed == 0) w = apply_rule(w,pos_measure,"eed",3,"ee",0);
  if (w.changed == 0) {
    w = apply_rule(w,contains_vowel,"ed",2,"",0);
    if (w.changed == 0) w = apply_rule(w,contains_vowel,"ing",3,"",0);
    if (w.changed) do1b1 = 1;}
  /* Step 1b1 rules */
  if (do1b1) {
    w.changed = 0;
    w = apply_rule(w,none,"at",2,"ate",0);
    if (w.changed == 0) w = apply_rule(w,none,"bl",2,"ble",0);
    if (w.changed == 0) w = apply_rule(w,none,"iz",2,"ize",0);
    if (w.changed == 0) w = apply_rule(w,double_const,"",0,"",1);
    if (w.changed == 0) w = apply_rule(w,m1cvc,"",0,"e",0);}
  /* Step 1c rules */
  w.changed = 0;
  w = apply_rule(w,contains_vowel,"y",1,"i",0);
  /* Step 2 rules */
  w.changed = 0;
  w = apply_rule(w,pos_measure,"ational",7,"ate",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"tional",6,"tion",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"anci",4,"ance",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"izer",4,"ize",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"abli",4,"able",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"entli",5,"ent",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"eli",3,"e",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"ousli",5,"ous",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"ization",7,"ize",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"ation",5,"ate",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"ator",4,"ate",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"alism",5,"al",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"iveness",7,"ive",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"fulness",7,"ful",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"ousness",7,"ous",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"aliti",5,"ive",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"iviti",5,"ive",0);
  if (w.changed == 0) w = apply_rule(w,pos_measure,"biliti",6,"bli",0);
  /* Step 3 rules */
  w.changed = 0;
  w = apply_rule(w,bigm,"icate",5,"ic",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ative",5,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"alize",5,"al",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"iciti",5,"ic",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ful",3,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ness",4,"",0);
  /* Step 4 rules */
  w.changed = 0;
  w = apply_rule(w,bigm,"al",2,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ance",4,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ence",4,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"er",2,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ic",2,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"able",4,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ible",4,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ant",3,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"cement",6,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ment",4,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ent",3,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm_ts,"ion",3,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ou",2,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ism",3,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ate",3,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"iti",3,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ous",3,"",0);
  /* Added rule to Porter parser: deductive -> deduc; deduce -> deduc, etc. */
  if (w.changed == 0) w = apply_rule(w,bigm,"tive",4,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ive",3,"",0);
  if (w.changed == 0) w = apply_rule(w,bigm,"ize",3,"",0);
  /* Step 5a rules */
  w.changed = 0;
  w = apply_rule(w,bigm,"e",1,"",0);
  if (w.changed == 0) w = apply_rule(w,m1notcvc,"e",1,"",0);
  /* Step 5b rules */
  w.changed = 0;
  w = apply_rule(w,big_double_l,"",0,"",1);
  fflush(stderr);
  return copy;
}


