/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2012-2014 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"

#include <libu8/libu8io.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <hyphen.h>

FD_EXPORT int fd_init_hyphenate(void) FD_LIBINIT_FN;

static u8_string default_hyphenation_file=NULL;
static HyphenDict *default_dict=NULL;
static struct HYPHEN_DICTIONARY {
  char *filename; HyphenDict *dict;
  struct HYPHEN_DICTIONARY *next;} *other_dicts;

static int hyphenate_init=0;
static u8_mutex hyphen_dict_lock;

static fdtype hyphenate_prim(fdtype string_arg)
{
  u8_string string=FD_STRDATA(string_arg);
  int len=FD_STRLEN(string_arg);
  char *hword=u8_malloc(len*2), *hyphens=u8_malloc(len+5);
  char ** rep=NULL; int * pos=NULL; int * cut=NULL;
  int retval=hnj_hyphen_hyphenate2(default_dict,string,len,hyphens,
                                   hword,&rep,&pos,&cut);
  if (retval) {
    u8_free(hyphens); u8_free(hword); fd_incref(string_arg);
    fd_seterr("Hyphenation error","get_hyphen_breaks",NULL,string_arg);
    return FD_ERROR_VALUE;}
  else {
    fdtype result=fd_make_string(NULL,-1,(u8_string)hword);
    u8_free(hyphens); u8_free(hword);
    return result;}
}

static fdtype hyphen_breaks_prim(fdtype string_arg)
{
  u8_string string=FD_STRDATA(string_arg);
  int len=FD_STRLEN(string_arg);
  char *hyphens=u8_malloc(len+5);
  char ** rep=NULL; int * pos=NULL; int * cut=NULL;
  int retval=hnj_hyphen_hyphenate2(default_dict,string,len,hyphens,
                                   NULL,&rep,&pos,&cut);
  if (retval) {
    u8_free(hyphens); fd_incref(string_arg);
    fd_seterr("Hyphenation error","get_hyphen_breaks",NULL,string_arg);
    return FD_ERROR_VALUE;}
  else {
    struct U8_OUTPUT out;
    fdtype result=FD_VOID, *elts; u8_byte *scan=string;
    int i=0, n_breaks=0, seg=0, cpos=0, c=u8_sgetc(&scan); while (i<len) {
      if (hyphens[i++]&1) n_breaks++;}
    result=fd_init_vector(NULL,n_breaks+1,NULL);
    elts=FD_VECTOR_DATA(result);
    U8_INIT_OUTPUT(&out,32);
    while (c>=0) {
      u8_putc(&out,c);
      if (hyphens[cpos]&1) {
        fdtype s=fd_make_string(NULL,out.u8_outptr-out.u8_outbuf,
                                out.u8_outbuf);
        elts[seg++]=s;
        out.u8_outptr=out.u8_outbuf;}
      cpos=scan-string;
      c=u8_sgetc(&scan);}
    if (out.u8_outptr!=out.u8_outbuf) {
      fdtype s=fd_make_string(NULL,out.u8_outptr-out.u8_outbuf,
                              out.u8_outbuf);
      elts[seg++]=s;
      out.u8_outptr=out.u8_outbuf;}
    u8_free(out.u8_outbuf);
    return result;}
}

static fdtype shyphenate_prim(fdtype string_arg)
{
  u8_string string=FD_STRDATA(string_arg);
  int len=FD_STRLEN(string_arg);
  char *hyphens=u8_malloc(len+5);
  char ** rep=NULL; int * pos=NULL; int * cut=NULL;
  int retval=hnj_hyphen_hyphenate2(default_dict,string,len,hyphens,
                                   NULL,&rep,&pos,&cut);
  if (retval) {
    u8_free(hyphens); fd_incref(string_arg);
    fd_seterr("Hyphenation error","get_hyphen_breaks",NULL,string_arg);
    return FD_ERROR_VALUE;}
  else {
    fdtype result=FD_VOID;
    struct U8_OUTPUT out; u8_byte *scan=string;
    int cpos=0, c=u8_sgetc(&scan);
    U8_INIT_OUTPUT(&out,len*3);
    while (c>=0) {
      u8_putc(&out,c);
      if (hyphens[cpos]&1) u8_putc(&out,0xAD);
      cpos=scan-string;
      c=u8_sgetc(&scan);}
    result=fd_make_string(NULL,out.u8_outptr-out.u8_outbuf,
                          out.u8_outbuf);
    u8_free(out.u8_outbuf);
    return result;}
}

static int hyphenout_helper(U8_OUTPUT *out,
                            u8_string string,int len,
                            int hyphen_char)
{
  char *hyphens=u8_malloc(len+5);
  char **rep=NULL; int *pos=NULL; int *cut=NULL;
  int retval=hnj_hyphen_hyphenate2(default_dict,string,len,hyphens,
                                   NULL,&rep,&pos,&cut);
  if (retval) {
    u8_free(hyphens);
    return -1;}
  else {
    fdtype result=FD_VOID;
    u8_byte *scan=string;
    int cpos=0, c=u8_sgetc(&scan), n_hyphens=0;
    while (c>=0) {
      u8_putc(out,c);
      if (hyphens[cpos]&1) {
        u8_putc(out,hyphen_char); n_hyphens++;}
      cpos=scan-string;
      c=u8_sgetc(&scan);}
    return n_hyphens;}
}

static fdtype hyphenout_prim(fdtype string_arg,fdtype hyphen_arg)
{
  U8_OUTPUT *output=u8_current_output;
  u8_string string=FD_STRDATA(string_arg);
  int len=FD_STRLEN(string_arg);
  int hyphen_char=FD_CHAR2CODE(hyphen_arg);
  struct U8_OUTPUT word; u8_byte *scan=string;
  int c=u8_sgetc(&scan);
  U8_INIT_OUTPUT(&word,64);
  while (c>=0) {
    if (u8_isalnum(c)) {
      u8_putc(&word,c); c=u8_sgetc(&scan);}
    else if (u8_isspace(c)) {
      if (word.u8_outptr!=word.u8_outbuf) {
        hyphenout_helper
          (output,word.u8_outbuf,word.u8_outptr-word.u8_outbuf,
           hyphen_char);
        word.u8_outptr=word.u8_outbuf; word.u8_outptr[0]='\0';}
      u8_putc(output,c);
      c=u8_sgetc(&scan);}
    else {
      int nc=u8_sgetc(&scan);
      if ((nc<0)||(!(u8_isalnum(nc)))) {
        if (word.u8_outptr!=word.u8_outbuf) {
          hyphenout_helper
            (output,word.u8_outbuf,word.u8_outptr-word.u8_outbuf,
             hyphen_char);
          word.u8_outptr=word.u8_outbuf; word.u8_outptr[0]='\0';}
        u8_putc(output,c);
        if (nc<0) break;
        u8_putc(output,nc);
        c=u8_sgetc(&scan);}
      else {u8_putc(&word,c); u8_putc(&word,nc); c=u8_sgetc(&scan);}}}
  if (word.u8_outptr!=word.u8_outbuf) {
    hyphenout_helper
      (output,word.u8_outbuf,word.u8_outptr-word.u8_outbuf,
       hyphen_char);}
  u8_free(word.u8_outbuf);
  u8_flush(output);
  return FD_VOID;
}

FD_EXPORT int fd_init_hyphenate()
{
  fdtype hyphenate_module;
  if (hyphenate_init) return 0;

  hyphenate_init=1;

  if (default_hyphenation_file)
    default_dict=hnj_hyphen_load(default_hyphenation_file);
  else {
    u8_string dictfile=u8_mkpath(FD_DATA_DIR,"data/hyph_en_US.dic");
    if (u8_file_existsp(dictfile))
      default_dict=hnj_hyphen_load(dictfile);
    else u8_log(LOG_CRIT,fd_FileNotFound,
                "Hyphenation dictionary %s does not exist!",
                dictfile);}

  hyphenate_module=fd_new_module("HYPHENATE",(FD_MODULE_SAFE));

  fd_idefn(hyphenate_module,
	   fd_make_cprim1x("HYPHENATE",
                           hyphenate_prim,1,fd_string_type,FD_VOID));
  fd_idefn(hyphenate_module,
	   fd_make_cprim1x("HYPHEN-BREAKS",
                           hyphen_breaks_prim,1,fd_string_type,FD_VOID));
  fd_idefn(hyphenate_module,
	   fd_make_cprim1x("SHYPHENATE",
                           shyphenate_prim,1,fd_string_type,FD_VOID));
  fd_idefn(hyphenate_module,
	   fd_make_cprim2x("HYPHENOUT",
                           hyphenout_prim,1,
                           fd_string_type,FD_VOID,
                           fd_character_type,FD_CODE2CHAR(0xAD)));

  fd_finish_module(hyphenate_module);
  fd_persist_module(hyphenate_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
