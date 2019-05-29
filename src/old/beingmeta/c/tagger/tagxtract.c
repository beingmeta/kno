/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* tagxtract.c
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
static fdtype prefix_symbol, postfix_symbol;

static int output_term(u8_output out,fdtype term,int insert_space);

/* Compound phrase extraction */

static int eltmatchp(fdtype pat,fdtype word)
{
  fdtype tag=FD_VECTOR_REF(word,1);
  fdtype root=FD_VECTOR_REF(word,2);
  fdtype term=((FD_STRINGP(root)) ? (root) : (FD_VECTOR_REF(word,0)));
  FD_DO_CHOICES(pat_elt,pat)
    if (FD_EQ(pat_elt,compound_symbol)) {
      if (strchr(FD_STRDATA(term),' ')) return 1;}
    else if (FD_SYMBOLP(pat_elt)) {
      if (FD_EQ(pat_elt,tag)) return 1;}
    else if (FD_STRINGP(pat_elt)) {
      if (FDTYPE_EQUAL(pat_elt,tag)) return 1;}
    else if (FD_VECTORP(pat_elt)) {
      if (fd_text_match(pat_elt,NULL,FD_STRDATA(term),
                        0,FD_STRLEN(term),0))
        return 1;}
  return 0;
}

static int tagmatch(fdtype pat,fdtype tags)
{
  if (FD_EMPTY_LISTP(pat)) return 0;
  else if (FD_EMPTY_LISTP(tags)) return 0;
  else if ((FD_PAIRP(pat)) ? (eltmatchp(FD_CAR(pat),FD_CAR(tags))) :
           (eltmatchp(pat,FD_CAR(tags))))
    if (!(FD_PAIRP(pat))) return 1;
    else if (!(FD_PAIRP(FD_CDR(pat)))) return 1;
    else if ((FD_EQ(FD_CAR(FD_CDR(pat)),star_symbol)) ||
             (FD_EQ(FD_CAR(FD_CDR(pat)),plus_symbol))) {
      fdtype next_pat;
      int matchlen=tagmatch(pat,FD_CDR(tags));
      if (matchlen) return matchlen+1;
      else next_pat=FD_CDR(FD_CDR(pat));
      if (FD_PAIRP(next_pat)) {
        matchlen=tagmatch(FD_CDR(FD_CDR(pat)),FD_CDR(tags));
        if (matchlen) return matchlen+1;
        else return 0;}
      else return 1;}
    else {
      int matchlen=tagmatch(FD_CDR(pat),FD_CDR(tags));
      if (matchlen) return matchlen+1;
      else return 0;}
  else if ((FD_PAIRP(pat)) &&
           (FD_PAIRP(FD_CDR(pat))) &&
           (FD_PAIRP(FD_CDR(FD_CDR(pat)))) &&
           (FD_EQ(FD_CAR(FD_CDR(pat)),star_symbol)))
    return tagmatch(FD_CDR(FD_CDR(pat)),tags);
  else return 0;
}

static fdtype make_compound(fdtype tags,int len)
{
  if (len==1)
    if (FD_STRINGP(FD_VECTOR_REF(FD_CAR(tags),2)))
      return fd_incref(FD_VECTOR_REF(FD_CAR(tags),2));
    else return fd_incref(FD_VECTOR_REF(FD_CAR(tags),0));
  else {
    U8_OUTPUT out; int i=0;
    U8_INIT_OUTPUT(&out,64);
    while (i<len)
      if (FD_PAIRP(tags)) {
        fdtype word=FD_CAR(tags);
        fdtype root=FD_VECTOR_REF(word,2);
        if (!(FD_STRINGP(root))) root=FD_VECTOR_REF(word,0);
        if (i>0) u8_putc(&out,' ');
        if (FD_STRINGP(root)) {
          u8_putn(&out,FD_STRDATA(root),FD_STRLEN(root));}
        else output_term(&out,root,0);
        tags=FD_CDR(tags); i++;}
      else return fd_err(fd_RangeError,"make_compound",NULL,tags);
    return fd_stream2string(&out);}
}

static fdtype find_phrase(fdtype tags,fdtype pat,int *locp,int *lenp)
{
  fdtype scan=tags; int loc=0, len=0;
  while ((len==0) && (FD_PAIRP(scan))) {
    FD_DO_CHOICES(elt,pat) {
      int mlen=tagmatch(elt,scan);
      if (mlen>len) len=mlen;}
    if (len==0) {scan=FD_CDR(scan); loc++;}}
  if (len) {
    if (locp) *locp=loc; if (lenp) *lenp=len;
    return scan;}
  else return FD_EMPTY_CHOICE;

}

static fdtype tag_extract(fdtype tags,fdtype pat)
{
  int len=0; fdtype start=find_phrase(tags,pat,NULL,&len);
  if (len) return make_compound(start,len);
  else return FD_EMPTY_CHOICE;
}

static fdtype tag_gather(fdtype tags,fdtype pat)
{
  if (FD_EMPTY_LISTP(tags)) return FD_EMPTY_CHOICE;
  else if (FD_PAIRP(tags))
    if (FD_PAIRP(FD_CAR(tags))) {
      fdtype results=FD_EMPTY_CHOICE;
      FD_DOLIST(elt,tags) {
        fdtype tmp=tag_gather(elt,pat);
        FD_ADD_TO_CHOICE(results,tmp);}
      return results;}
    else {
      fdtype results=FD_EMPTY_CHOICE, scan=tags;
      while (FD_PAIRP(scan)) {
        int max_len=0;
        FD_DO_CHOICES(eachpat,pat) {
          int len=tagmatch(eachpat,scan);
          if (len>max_len) max_len=len;
          if (len) {
            fdtype compound=make_compound(scan,len);
              FD_ADD_TO_CHOICE(results,compound);}}
        if (max_len==0) scan=FD_CDR(scan);
        else while (max_len>0) {scan=FD_CDR(scan); max_len--;}}
      return results;}
  else return fd_type_error(_("pair"),"tag_gather",tags);
}

/* Getting a tagging from a string. */

static fdtype gettags(u8_string string)
{
  fd_grammar g=fd_default_grammar();
  struct FD_PARSE_CONTEXT parse_context;
  fdtype tagging=FD_VOID;
  fd_init_parse_context(&parse_context,g);
  parse_context.flags=parse_context.flags|FD_TAGGER_GLOM_PHRASES;
  tagging=fd_tag_text(&parse_context,string);
  fd_free_parse_context(&parse_context);
  return tagging;
}


/* Phrase primitives */

static fdtype getphrases_prim(fdtype args,fdtype patterns)
{
  fdtype results=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(arg,args)
    if (FD_EMPTY_LISTP(arg)) {}
    else if (FD_PAIRP(arg)) {
      fdtype gathered=tag_gather(arg,patterns);
      FD_ADD_TO_CHOICE(results,gathered);}
    else if (FD_STRINGP(arg)) {
      fdtype tagging=gettags(FD_STRDATA(arg));
      fdtype gathered=tag_gather(tagging,patterns);
      FD_ADD_TO_CHOICE(results,gathered);
      fd_decref(tagging);}
    else {
      fd_decref(results);
      return fd_type_error(_("parse"),"getphrases_prim",arg);}
  return results;
}

static fdtype getphrase_prim(fdtype args,fdtype patterns)
{
  FD_DO_CHOICES(arg,args)
    if (FD_EMPTY_LISTP(arg)) {}
    else if ((FD_PAIRP(arg)) && (FD_PAIRP(FD_CAR(arg)))) {
      FD_DOLIST(sentence,arg) {
        fdtype gathered=tag_extract(sentence,patterns);
        if (FD_EMPTY_CHOICEP(gathered)) {}
        else return gathered;}
      return FD_EMPTY_CHOICE;}
    else if (FD_PAIRP(arg)) {
      return tag_extract(arg,patterns);}
    else if (FD_STRINGP(arg)) {
      fdtype tagging=gettags(FD_STRDATA(arg));
      FD_DOLIST(sentence,tagging) {
        fdtype gathered=tag_extract(sentence,patterns);
        if (FD_EMPTY_CHOICEP(gathered)) {}
        else {
          fd_decref(tagging);
          return gathered;}}
      fd_decref(tagging);
      return FD_EMPTY_CHOICE;}
    else {
      return fd_type_error(_("parse"),"tagextract_prim",arg);}
  return FD_EMPTY_CHOICE;
}

/* Compound functions */

static int output_term(u8_output out,fdtype term,int insert_space)
{
  if (FD_STRINGP(term)) {
    /* If it's a string and punctuation, don't insert a space after it. */
    u8_string s=FD_STRDATA(term);
    int firstchar=u8_sgetc(&s), punct=u8_ispunct(firstchar);
    if ((insert_space) && (!punct)) u8_putc(out,' ');
    u8_puts(out,FD_STRDATA(term));
    return (punct==0);}
  else if (FD_PAIRP(term)) {
    /* If it's an embedded compound that ends with punctuation,
       do insert a space after it. */
    FD_DOLIST(elt,term)
      insert_space=output_term(out,elt,insert_space);
    return 1;}
  else return 1;
}

static fdtype compound2string(fdtype word)
{
  if (FD_STRINGP(word)) return fd_incref(word);
  else if (FD_PAIRP(word))
    if (FD_PAIRP(FD_CDR(word))) {
      struct U8_OUTPUT out;
      fdtype scan=word; int insert_space=0;
      U8_INIT_OUTPUT(&out,64);
      while (FD_PAIRP(scan)) {
        insert_space=output_term(&out,FD_CAR(scan),insert_space);
        scan=FD_CDR(scan);}
      return fd_stream2string(&out);}
    else return fd_incref(FD_CAR(word));
  else return FD_EMPTY_CHOICE;
}

static fdtype phrase_string_prim(fdtype term)
{
  if (FD_VECTORP(term))
    if (FD_VECTOR_LENGTH(term)<3)
      return fd_type_error(_("tagged word"),"phrase_string_prim",term);
    else {
      fdtype word=FD_VECTOR_REF(term,0);
      return compound2string(word);}
  else if (FD_STRINGP(term)) return fd_incref(term);
  else if (FD_PAIRP(term))
    return compound2string(term);
  else return fd_type_error(_("compound term"),"phrase_string_prim",term);
}

static fdtype phrase_root_prim(fdtype term)
{
  if (FD_VECTORP(term))
    if (FD_VECTOR_LENGTH(term)<3)
      return fd_type_error(_("tagged word"),"phrase_root_prim",term);
    else {
      fdtype word=FD_VECTOR_REF(term,0);
      fdtype root=FD_VECTOR_REF(term,2);
      if (FD_VOIDP(root))
        return compound2string(word);
      else return compound2string(root);}
  else if (FD_STRINGP(term)) return fd_incref(term);
  else if (FD_PAIRP(term))
    return compound2string(term);
  else return fd_type_error(_("compound term"),"phrase_root_prim",term);
}

static fdtype phrase_base_prim(fdtype term)
{
  if (FD_VECTORP(term))
    if (FD_VECTOR_LENGTH(term)<3)
      return fd_type_error(_("tagged word"),"phrase_base_prim",term);
    else {
      fdtype word=FD_VECTOR_REF(term,0);
      fdtype root=FD_VECTOR_REF(term,2);
      if (FD_VOIDP(root))
        return phrase_base_prim(word);
      else return phrase_base_prim(root);}
  else if (FD_STRINGP(term)) return fd_incref(term);
  else if (FD_PAIRP(term)) {
    fdtype last=FD_CAR(term), scan=FD_CDR(term);
    while (FD_PAIRP(scan)) {
      last=FD_CAR(scan); scan=FD_CDR(scan);}
    return fd_incref(last);}
  else return fd_type_error(_("compound term"),"phrase_base_prim",term);
}

static fdtype phrase_modifiers_prim(fdtype term)
{
  if (FD_VECTORP(term))
    if (FD_VECTOR_LENGTH(term)<3)
      return fd_type_error(_("tagged word"),"phrase_modifiers_prim",term);
    else {
      fdtype word=FD_VECTOR_REF(term,0);
      fdtype root=FD_VECTOR_REF(term,2);
      if (FD_VOIDP(root))
        return phrase_base_prim(word);
      else return phrase_base_prim(root);}
  else if (FD_STRINGP(term)) return fd_incref(term);
  else if (FD_PAIRP(term)) {
    fdtype results=FD_EMPTY_CHOICE, last=FD_CAR(term), scan=FD_CDR(term);
    while (FD_PAIRP(scan)) {
      fd_incref(last);
      FD_ADD_TO_CHOICE(results,last);
      last=FD_CAR(scan); scan=FD_CDR(scan);}
    return results;}
  else return fd_type_error(_("compound term"),"phrase_modifiers_prim",term);
}

static fdtype phrase_compounds_prim(fdtype term)
{
  if (FD_VECTORP(term))
    if (FD_VECTOR_LENGTH(term)<3)
      return fd_type_error(_("tagged word"),"phrase_string_prim",term);
    else {
      fdtype word=FD_VECTOR_REF(term,0);
      fdtype root=FD_VECTOR_REF(term,2);
      if (FD_VOIDP(root))
        return phrase_compounds_prim(word);
      else return phrase_compounds_prim(root);}
  else if (FD_STRINGP(term)) {
    u8_string data=FD_STRDATA(term);
    if (strchr(data,' ')) return fd_incref(term);
    else return FD_EMPTY_CHOICE;}
  else if (FD_PAIRP(term))
    if (FD_PAIRP(FD_CDR(term))) {
      fdtype results=compound2string(term);
      fdtype scan=term; while (FD_PAIRP(scan)) {
        if ((FD_STRINGP(FD_CAR(scan))) &&
            (strchr(FD_STRDATA(FD_CAR(scan)),' '))) {
          fdtype car=FD_CAR(scan);
          fd_incref(car); FD_ADD_TO_CHOICE(results,car);}
        scan=FD_CDR(scan);}
      return results;}
    else return phrase_compounds_prim(FD_CAR(term));
  else return fd_type_error(_("compound term"),"phrase_string_prim",term);
}

/* Probing compounds */

static fdtype probe_compound(fd_index lexicon,fdtype *elt,int start,int end)
{
  fdtype compound=FD_EMPTY_LIST, lexentry;
  while (end>=start) {
    compound=fd_conspair(fd_incref(elt[end]),compound);
    end--;}
  lexentry=fd_index_get(lexicon,compound);
  if (FD_EMPTY_CHOICEP(lexentry)) {
    fd_decref(compound); return FD_EMPTY_CHOICE;}
  else {
    fdtype string=compound2string(compound);
    fd_decref(compound);
    return string;}
}

static fdtype probe_compounds(fdtype compound)
{
  fdtype terms[6]; fdtype scan=compound; int n=0;
  while ((n<6) && (FD_PAIRP(scan))) {
    terms[n++]=FD_CAR(scan); scan=FD_CDR(scan);}
  if (n==1) return FD_EMPTY_CHOICE;
  else if (n>6) return FD_EMPTY_CHOICE;
  else {
    fd_grammar g=fd_default_grammar();
    fdtype results=FD_EMPTY_CHOICE;
    int i=0;
    while (i<n) {
      int j=0; while (j<i) {
        fdtype probe=probe_compound(g->lexicon,terms,j,i);
        FD_ADD_TO_CHOICE(results,probe); j++;}
      i++;}
    return results;}
}

static fdtype probe_compounds_prim(fdtype term)
{
  if (FD_VECTORP(term))
    if (FD_VECTOR_LENGTH(term)<3)
      return fd_type_error(_("tagged word"),"probe_compounds_prim",term);
    else {
      fdtype word=FD_VECTOR_REF(term,0);
      fdtype root=FD_VECTOR_REF(term,2);
      if (FD_VOIDP(root))
        return probe_compounds_prim(word);
      else return probe_compounds_prim(root);}
  else if (FD_STRINGP(term)) {
    u8_string data=FD_STRDATA(term);
    if (strchr(data,' ')) return fd_incref(term);
    else return FD_EMPTY_CHOICE;}
  else if (FD_PAIRP(term))
    if (FD_PAIRP(FD_CDR(term)))
      return probe_compounds(term);
    else return probe_compounds_prim(FD_CAR(term));
  else return fd_type_error(_("compound term"),"probe_compounds_prim",term);
}

/* Extracting phrases from parses */

static fdtype getterms_prim(fdtype arg,fdtype tags)
{
  if (FD_VECTORP(arg))
    if (FD_VECTOR_LENGTH(arg)<3)
      return fd_type_error(_("tagged word"),"geterms_prim",arg);
    else if ((FD_VOIDP(tags)) || (fd_overlapp(FD_VECTOR_REF(arg,1),tags)))
      return fd_incref(FD_VECTOR_REF(arg,0));
    else return FD_EMPTY_CHOICE;
  else if (FD_PAIRP(arg)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DOLIST(elt,arg) {
      fdtype tmp=getterms_prim(elt,tags);
      if (FD_ABORTP(tmp)) {
        fd_decref(results); return tmp;}
      FD_ADD_TO_CHOICE(results,tmp);}
    return results;}
  else if (FD_STRINGP(arg)) {
    fdtype tagging=gettags(FD_STRDATA(arg));
    fdtype result=getterms_prim(tagging,tags);
    fd_decref(tagging);
    return result;}
  else if (FD_CHOICEP(arg)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(elt,arg) {
      fdtype tmp=getterms_prim(elt,tags);
      if (FD_ABORTP(tmp)) {
        fd_decref(results); return tmp;}
      FD_ADD_TO_CHOICE(results,tmp);}
    return results;}
  else return FD_EMPTY_CHOICE;
}

static fdtype getroots_prim(fdtype arg,fdtype tags)
{
  if (FD_VECTORP(arg))
    if (FD_VECTOR_LENGTH(arg)<3)
      return fd_type_error(_("tagged word"),"geterms_prim",arg);
    else if ((FD_VOIDP(tags)) || (fd_overlapp(FD_VECTOR_REF(arg,1),tags)))
      if (FD_VOIDP(FD_VECTOR_REF(arg,2)))
        return fd_incref(FD_VECTOR_REF(arg,0));
      else return fd_incref(FD_VECTOR_REF(arg,2));
    else return FD_EMPTY_CHOICE;
  else if (FD_PAIRP(arg)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DOLIST(elt,arg) {
      fdtype tmp=getroots_prim(elt,tags);
      if (FD_ABORTP(tmp)) {
        fd_decref(results); return tmp;}
      FD_ADD_TO_CHOICE(results,tmp);}
    return results;}
  else if (FD_STRINGP(arg)) {
    fdtype tagging=gettags(FD_STRDATA(arg));
    fdtype result=getroots_prim(tagging,tags);
    fd_decref(tagging);
    return result;}
  else if (FD_CHOICEP(arg)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(elt,arg) {
      fdtype tmp=getroots_prim(elt,tags);
      if (FD_ABORTP(tmp)) {
        fd_decref(results); return tmp;}
      FD_ADD_TO_CHOICE(results,tmp);}
    return results;}
  else return FD_EMPTY_CHOICE;
}

static fdtype gettags_prim(fdtype arg,fdtype tags)
{
  if (FD_VECTORP(arg))
    if (FD_VECTOR_LENGTH(arg)<3)
      return fd_type_error(_("tagged word"),"geterms_prim",arg);
    else if ((FD_VOIDP(tags)) || (fd_overlapp(FD_VECTOR_REF(arg,1),tags)))
      return fd_incref(arg);
    else return FD_EMPTY_CHOICE;
  else if (FD_PAIRP(arg)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DOLIST(elt,arg) {
      fdtype tmp=gettags_prim(elt,tags);
      if (FD_ABORTP(tmp)) {
        fd_decref(results); return tmp;}
      FD_ADD_TO_CHOICE(results,tmp);}
    return results;}
  else if (FD_STRINGP(arg)) {
    fdtype tagging=gettags(FD_STRDATA(arg));
    fdtype result=gettags_prim(tagging,tags);
    fd_decref(tagging);
    return result;}
  else if (FD_CHOICEP(arg)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(elt,arg) {
      fdtype tmp=gettags_prim(elt,tags);
      if (FD_ABORTP(tmp)) {
        fd_decref(results); return tmp;}
      FD_ADD_TO_CHOICE(results,tmp);}
    return results;}
  else return FD_EMPTY_CHOICE;
}

/* SUFFIX/PREFIX primitives */

static fdtype get_ixes(u8_string start,u8_string end,int prefix,int suffix)
{
  fdtype results=FD_EMPTY_CHOICE;
  u8_string last=start, scan=start; int c=u8_sgetc(&scan);
  while (c>=0) {
    if (c==' ') {
      if ((prefix) && (last>start)) {
        if (last>start) {
          fdtype xstring=fd_substring(start,last);
          FD_ADD_TO_CHOICE(results,xstring);}}
      if ((suffix) && (scan<end)) {
        fdtype xstring=fd_substring(scan,end);
        FD_ADD_TO_CHOICE(results,xstring);}}
    last=scan; c=u8_sgetc(&scan);}
  return results;
}

static fdtype get_prefixes_prim(fdtype string)
{
  return get_ixes(FD_STRDATA(string),FD_STRDATA(string)+FD_STRLEN(string),1,0);
}

static fdtype get_suffixes_prim(fdtype string)
{
  return get_ixes(FD_STRDATA(string),FD_STRDATA(string)+FD_STRLEN(string),0,1);
}

static fdtype get_ixes_prim(fdtype string)
{
  return get_ixes(FD_STRDATA(string),FD_STRDATA(string)+FD_STRLEN(string),1,1);
}

/* XKEY primitives */

static fdtype term_spectrum(fdtype term)
{
  fdtype spectrum=fd_incref(term);
  if ((FD_STRINGP(term)) && (strchr(FD_STRDATA(term),' '))) {
    u8_string start=FD_STRDATA(term)+(FD_STRLEN(term))-1, scan=start;
    fdtype spec;
    while (*scan!=' ') scan--; scan++;
    spec=fdtype_string(scan);
    FD_ADD_TO_CHOICE(spectrum,spec);}
  else if (FD_PAIRP(term)) {
    fdtype base=phrase_base_prim(term);
    FD_ADD_TO_CHOICE(spectrum,base);
    if ((FD_STRINGP(base)) &&
        (strchr(FD_STRDATA(base),' '))) {
      u8_string start=FD_STRDATA(base)+(FD_STRLEN(base))-1, scan=start;
      fdtype spec;
      while (*scan!=' ') scan--; scan++;
      spec=fdtype_string(scan);
      FD_ADD_TO_CHOICE(spectrum,spec);}}
  return spectrum;
}

static fdtype getxkeys
  (fdtype sentence,fdtype head_tags,fdtype prefix_tags,fdtype suffix_tags,int radius)
{
  int wordpos=0, headpos=-1;
  fdtype xkeys=FD_EMPTY_CHOICE;
  fdtype last_head=FD_EMPTY_CHOICE, prefixes=FD_EMPTY_CHOICE;
  FD_DOLIST(word,sentence)
    if ((FD_VECTORP(word)) && (FD_VECTOR_LENGTH(word)>2)) {
      fdtype tag=FD_VECTOR_REF(word,1);
      fdtype root=FD_VECTOR_REF(word,2);
      fdtype spectrum=FD_VOID;
      if (fd_overlapp(tag,prefix_tags)) {
        fdtype pair;
        if (FD_VOIDP(spectrum)) spectrum=term_spectrum(root);
        pair=fd_conspair(FD_INT(wordpos),fd_incref(spectrum));
        FD_ADD_TO_CHOICE(prefixes,pair);}
      if (fd_overlapp(tag,suffix_tags)) {
        if ((radius<0) || ((headpos>0) && ((wordpos-headpos)<=radius))) {
          FD_DO_CHOICES(head,last_head) {
            if (FD_VOIDP(spectrum)) spectrum=term_spectrum(root);
            {FD_DO_CHOICES(suffix,spectrum) {
                fdtype xkey=fd_conspair(fd_incref(head),fd_incref(suffix));
              FD_ADD_TO_CHOICE(xkeys,xkey);}}}}}
      if (fd_overlapp(tag,head_tags)) {
        if (FD_VOIDP(spectrum)) spectrum=term_spectrum(root);
        {FD_DO_CHOICES(head,spectrum) {
          FD_DO_CHOICES(prefix_entry,prefixes) {
            int pos=fd_getint(FD_CAR(prefix_entry));
            fdtype prefixes=FD_CDR(prefix_entry);
            if ((radius<0) || ((wordpos-pos)<=radius)) {
              FD_DO_CHOICES(prefix,prefixes) {
                fdtype xkey=fd_conspair(fd_incref(prefix),fd_incref(head));
                FD_ADD_TO_CHOICE(xkeys,xkey);}}}}}
        fd_decref(prefixes); prefixes=FD_EMPTY_CHOICE;
        fd_decref(last_head); last_head=spectrum;}
      else fd_decref(spectrum);
      wordpos++;}
    else {
      fd_decref(xkeys); fd_decref(last_head); fd_decref(prefixes);
      return fd_type_error(_("tagged word"),"getxkeys",word);}
  fd_decref(last_head); fd_decref(prefixes);
  return xkeys;
}

static fdtype getxkeys_prim
  (fdtype input,fdtype head_tags,fdtype prefix_tags,fdtype suffix_tags,fdtype radius)
{
  if (FD_CHOICEP(input)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(elt,input) {
      fdtype tmp=getxkeys_prim(elt,head_tags,prefix_tags,suffix_tags,radius);
      if (FD_ABORTP(tmp)) {fd_decref(results); return tmp;}
      FD_ADD_TO_CHOICE(results,tmp);}
    return results;}
  else if (FD_PAIRP(input))
    if (FD_VECTORP(FD_CAR(input)))
      return getxkeys(input,head_tags,prefix_tags,suffix_tags,fd_getint(radius));
    else {
      fdtype results=FD_EMPTY_CHOICE;
      FD_DOLIST(elt,input) {
        fdtype tmp=getxkeys_prim(elt,head_tags,prefix_tags,suffix_tags,radius);
        if (FD_ABORTP(tmp)) {fd_decref(results); return tmp;}
        FD_ADD_TO_CHOICE(results,tmp);}
      return results;}
  else if (FD_STRINGP(input)) {
    fdtype tagging=gettags(FD_STRDATA(input));
    fdtype result=getxkeys_prim(tagging,head_tags,prefix_tags,suffix_tags,radius);
    fd_decref(tagging);
    return result;}
  else return FD_EMPTY_CHOICE;
}

static fdtype word2string(fdtype root,fdtype word)
{
  if (FD_VOIDP(root)) root=word;
  if (FD_STRINGP(root)) return fd_incref(root);
  else if (FD_PAIRP(root)) return compound2string(root);
  else return fd_incref(root);
}

static fdtype getxlinks
  (fdtype sentence,fdtype head_tags,fdtype prefix_tags,fdtype suffix_tags,int radius)
{
  int wordpos=0, headpos=-1;
  fdtype xlinks=FD_EMPTY_CHOICE;
  fdtype last_head=FD_EMPTY_CHOICE, prefixes=FD_EMPTY_CHOICE;
  FD_DOLIST(term,sentence)
    if ((FD_VECTORP(term)) && (FD_VECTOR_LENGTH(term)>2)) {
      fdtype word=FD_VECTOR_REF(term,0);
      fdtype tag=FD_VECTOR_REF(term,1);
      fdtype root=FD_VECTOR_REF(term,2);
      if (fd_overlapp(tag,prefix_tags)) {
        fdtype prefix_entry=
          fd_make_nvector(3,FD_INT(wordpos),fd_incref(tag),word2string(root,word));
        FD_ADD_TO_CHOICE(prefixes,prefix_entry);}
      if (fd_overlapp(tag,suffix_tags)) {
        if ((radius<0) || ((headpos>0) && ((wordpos-headpos)<=radius))) {
          fdtype xlink=
            fd_make_list(3,fd_incref(last_head),fd_incref(tag),word2string(root,word));
          FD_ADD_TO_CHOICE(xlinks,xlink);}}
      if (fd_overlapp(tag,head_tags)) {
        FD_DO_CHOICES(prefix_entry,prefixes) {
          int pos=fd_getint(FD_VECTOR_REF(prefix_entry,0));
          fdtype prefix_tag=FD_VECTOR_REF(prefix_entry,1);
          fdtype prefix_root=FD_VECTOR_REF(prefix_entry,2);
          if ((radius<0) || ((wordpos-pos)<=radius)) {
            fdtype xlink=
              fd_make_list(3,word2string(root,word),
                           fd_incref(prefix_tag),fd_incref(prefix_root));
            FD_ADD_TO_CHOICE(xlinks,xlink);}}
        fd_decref(prefixes); prefixes=FD_EMPTY_CHOICE;
        fd_decref(last_head); last_head=word2string(root,word);}
      wordpos++;}
    else {
      fd_decref(xlinks); fd_decref(last_head); fd_decref(prefixes);
      return fd_type_error(_("tagged word"),"getxlinks",term);}
  fd_decref(last_head); fd_decref(prefixes);
  return xlinks;
}

static fdtype getxlinks_prim
  (fdtype input,fdtype head_tags,fdtype prefix_tags,fdtype suffix_tags,fdtype radius)
{
  if (FD_CHOICEP(input)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(elt,input) {
      fdtype tmp=getxlinks_prim(elt,head_tags,prefix_tags,suffix_tags,radius);
      if (FD_ABORTP(tmp)) {fd_decref(results); return tmp;}
      FD_ADD_TO_CHOICE(results,tmp);}
    return results;}
  else if (FD_PAIRP(input))
    if (FD_VECTORP(FD_CAR(input)))
      return getxlinks(input,head_tags,prefix_tags,suffix_tags,fd_getint(radius));
    else {
      fdtype results=FD_EMPTY_CHOICE;
      FD_DOLIST(elt,input) {
        fdtype tmp=getxlinks_prim(elt,head_tags,prefix_tags,suffix_tags,radius);
        if (FD_ABORTP(tmp)) {fd_decref(results); return tmp;}
        FD_ADD_TO_CHOICE(results,tmp);}
      return results;}
  else if (FD_STRINGP(input)) {
    fdtype tagging=gettags(FD_STRDATA(input));
    fdtype result=getxlinks_prim(tagging,head_tags,prefix_tags,suffix_tags,radius);
    fd_decref(tagging);
    return result;}
  else return FD_EMPTY_CHOICE;
}

/* fd_init_tagxtract_c:
      Arguments: none
      Returns: nothing
*/
FD_EXPORT
void fd_init_tagxtract_c()
{
  fdtype menv=fd_new_module("TAGGER",(FD_MODULE_SAFE));

  u8_register_source_file(_FILEINFO);

  compound_symbol=fd_intern("COMPOUND");
  star_symbol=fd_intern("*");
  plus_symbol=fd_intern("+");
  prefix_symbol=fd_intern("PREFIX");
  postfix_symbol=fd_intern("POSTFIX");

  fd_idefn(menv,fd_make_cprim1("PHRASE-STRING",phrase_string_prim,1));
  fd_idefn(menv,fd_make_cprim1("PHRASE-ROOT",phrase_root_prim,1));
  fd_idefn(menv,fd_make_cprim1("PHRASE-BASE",phrase_base_prim,1));
  fd_idefn(menv,fd_make_cprim1("PHRASE-MODIFIERS",phrase_modifiers_prim,1));
  fd_idefn(menv,fd_make_cprim1("PHRASE-COMPOUNDS",phrase_compounds_prim,1));
  fd_idefn(menv,fd_make_cprim1("PROBE-COMPOUNDS",probe_compounds_prim,1));

  fd_idefn(menv,fd_make_cprim1x("GET-SUFFIXES",get_suffixes_prim,1,
                                fd_string_type,FD_VOID));
  fd_idefn(menv,fd_make_cprim1x("GET-PREFIXES",get_prefixes_prim,1,
                                fd_string_type,FD_VOID));
  fd_idefn(menv,fd_make_cprim1x("GET-IXES",get_ixes_prim,1,
                                fd_string_type,FD_VOID));

  fd_idefn(menv,fd_make_ndprim(fd_make_cprim2("GETTERMS",getterms_prim,1)));
  fd_idefn(menv,fd_make_ndprim(fd_make_cprim2("GETROOTS",getroots_prim,1)));
  fd_idefn(menv,fd_make_ndprim(fd_make_cprim2("GETTAGS",gettags_prim,1)));
  fd_idefn(menv,fd_make_ndprim
           (fd_make_cprim5x("GETXKEYS",getxkeys_prim,4,
                            -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,
                            fd_fixnum_type,FD_INT(-1))));
  fd_idefn(menv,fd_make_ndprim
           (fd_make_cprim5x("GETXLINKS",getxlinks_prim,4,
                            -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,
                            fd_fixnum_type,FD_INT(-1))));

  fd_idefn(menv,fd_make_ndprim
           (fd_make_cprim2("GETPHRASES",getphrases_prim,2)));
  fd_idefn(menv,fd_make_ndprim
           (fd_make_cprim2("GETPHRASE",getphrase_prim,2)));

}



/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
