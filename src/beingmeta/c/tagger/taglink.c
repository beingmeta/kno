/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* taglink.c
   Copyright (C) 2005-2016 beingmeta, inc.
   This file implements primitives for pulling patterns out of tagged parses
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/dtypestream.h"
#include "framerd/texttools.h"

#include "tagger.h"

#include <libu8/libu8io.h>

static fdtype compound_symbol, star_symbol, plus_symbol;
static fdtype term_symbol, tag_symbol, root_symbol, weight_symbol;
static fdtype prefix_symbol, postfix_symbol;

#define VECREF(v,i,d) \
  ((FD_VECTOR_LENGTH(v)>i)?(FD_VECTOR_REF(v,i)):(d))

static void addlink(fdtype *nodes,int pos,
                    fdtype link,
                    fdtype term,int termpos)
{
  fdtype pair=fd_make_pair(term,FD_INT(termpos));
  fdtype node=nodes[pos];
  fd_add(node,link,pair);
  fd_decref(pair);
}

static fdtype taglink(fdtype sentence,fdtype rules)
{
  int n_words=fd_list_length(sentence);
  fdtype *words=u8_alloc_n(n_words,fdtype);
  fdtype *nodes=u8_alloc_n(n_words,fdtype);
  fdtype result;
  fdtype tagged=(FD_STRINGP(sentence))?
    fd_tag_text(NULL,FD_STRDATA(sentence)):
    fd_incref(sentence);
  int i=0, n=0;
  {FD_DOLIST(word,tagged) {
      if (FD_VECTORP(word)) {
        fdtype term=VECREF(word,0,FD_VOID);
        fdtype tag=VECREF(word,1,FD_VOID);
        fdtype root=VECREF(word,2,term);
        fdtype weight=VECREF(word,3,FD_EMPTY_CHOICE);
        fdtype node=fd_make_slotmap(7,0,NULL);
        words[n]=word; nodes[n]=node;
        fd_store(node,FD_INT(n),term);
        fd_store(node,term_symbol,term);
        fd_store(node,tag_symbol,tag);
        fd_store(node,root_symbol,root);
        if (FD_FIXNUMP(weight))
          fd_store(node,weight_symbol,fd_incref(weight));
        n++;}}}
  {FD_DO_CHOICES(rule,rules) {
      if ((FD_VECTORP(rule))&&(FD_VECTOR_LENGTH(rule)>=3)) {
        fdtype headterm=FD_VOID; int headpos=-1;
        fdtype prefixes=FD_EMPTY_CHOICE;
        fdtype link=VECREF(rule,0,FD_VOID);
        fdtype head_tags=VECREF(rule,1,FD_EMPTY_CHOICE);
        fdtype prefix_tags=VECREF(rule,2,FD_EMPTY_CHOICE);
        fdtype suffix_tags=VECREF(rule,3,FD_EMPTY_CHOICE);
        fdtype wall_tags=VECREF(rule,4,FD_EMPTY_CHOICE);
        int radius=(((FD_VECTOR_LENGTH(rule))>4)&&
                    (FD_FIXNUMP(FD_VECTOR_REF(rule,4))))?
          (FD_FIX2INT(FD_VECTOR_REF(rule,4))):
          (-1);
        i=0; while (i<n) {
          fdtype word=words[i];
          if ((FD_VECTORP(word)) && (FD_VECTOR_LENGTH(word)>2)) {
            fdtype term=FD_VECTOR_REF(word,0);
            fdtype tag=FD_VECTOR_REF(word,1);
            fdtype root=FD_VECTOR_REF(word,2);
            if (fd_overlapp(tag,prefix_tags)) {
              FD_ADD_TO_CHOICE(prefixes,FD_INT(i));}
            if (fd_overlapp(tag,suffix_tags)) {
              if (!(FD_VOIDP(headterm))) {
                if ((radius<0) || ((i-headpos)<=radius)) {
                  addlink(nodes,i,link,headterm,headpos);}}}
            if (fd_overlapp(tag,head_tags)) {
              headterm=term; headpos=i;
              {FD_DO_CHOICES(prefix,prefixes) {
                  int prefix_pos=FD_FIX2INT(prefix);
                  addlink(nodes,prefix_pos,link,headterm,headpos);}}
              prefixes=FD_EMPTY_CHOICE;
              headpos=i;}
            if (fd_overlapp(tag,wall_tags)) {
              headterm=FD_VOID; headpos=-1;
              prefixes=FD_EMPTY_CHOICE;}}
          i++;}}}}
  result=fd_make_vector(n,NULL);
  i=0; while (i<n) {
    fdtype word=words[i];
    fdtype term=FD_VECTOR_REF(word,0);
    fdtype tag=FD_VECTOR_REF(word,1);
    fdtype root=FD_VECTOR_REF(word,2);
    fdtype distance=((FD_VECTOR_LENGTH(word)>3)?(FD_VECTOR_REF(word,3)):
                     (FD_FALSE));
    fdtype node=nodes[i];
    FD_VECTOR_SET(result,i,node);
    i++;}
  u8_free(nodes); u8_free(words);
  fd_decref(tagged);
  return result;
}

static fdtype taglink_prim(fdtype sentence,fdtype rules)
{
  if (FD_STRINGP(sentence)) {
    fdtype tagged=fd_tag_text(NULL,FD_STRDATA(sentence));
    if (FD_ABORTP(tagged)) return tagged;
    else {
      fdtype result=taglink_prim(tagged,rules);
      fd_decref(tagged);
      return result;}}
  else if ((FD_PAIRP(sentence))&&(FD_PAIRP(FD_CAR(sentence)))) {
    fdtype result=FD_EMPTY_LIST;
    FD_DOLIST(s,sentence) {
      fdtype linked=taglink_prim(s,rules);
      if (FD_ABORTP(linked)) {
        fd_decref(result); return linked;}
      else result=fd_init_pair(NULL,linked,result);}
    if (!(FD_PAIRP(result))) return result;
    else if (FD_PAIRP(FD_CDR(result))) {
      int n=fd_list_length(result);
      fdtype vec=fd_make_vector(n,NULL);
      FD_DOLIST(linked,result) {
        FD_VECTOR_SET(vec,--n,linked);
        fd_incref(linked);}
      fd_decref(result);
      return vec;}
    else {
      fdtype linked=FD_CAR(result);
      fdtype vec=fd_make_nvector(1,linked);
      fd_incref(linked); fd_decref(result);
      return vec;}}
  else if ((FD_PAIRP(sentence))&&(FD_VECTORP(FD_CAR(sentence))))
    return taglink(sentence,rules);
  else return fd_type_error("text or parse","taglink_prim",sentence);
}

/* fd_init_tagxtract_c:
      Arguments: none
      Returns: nothing
*/
FD_EXPORT
void fd_init_taglink_c()
{
  fdtype menv=fd_new_module("TAGGER",(FD_MODULE_SAFE));

  u8_register_source_file(_FILEINFO);

  compound_symbol=fd_intern("COMPOUND");
  star_symbol=fd_intern("*");
  plus_symbol=fd_intern("+");
  prefix_symbol=fd_intern("PREFIX");
  postfix_symbol=fd_intern("POSTFIX");
  term_symbol=fd_intern("TERM");
  tag_symbol=fd_intern("TAG");
  root_symbol=fd_intern("ROOT");
  weight_symbol=fd_intern("WEIGHT");

  fd_idefn(menv,fd_make_ndprim(fd_make_cprim2("TAGLINK",taglink_prim,2)));

}



/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
