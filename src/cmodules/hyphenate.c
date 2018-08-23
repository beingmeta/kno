/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2012-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"

#include <libu8/libu8io.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <hyphen.h>

#define EDGE_LEN 1

FD_EXPORT int fd_init_hyphenate(void) FD_LIBINIT_FN;

static u8_string default_hyphenation_file = NULL;
static HyphenDict *default_dict = NULL;

static long long int hyphenate_init = 0;

static lispval hyphenate_word_prim(lispval string_arg)
{
  u8_string string = FD_CSTRING(string_arg);
  int len = FD_STRLEN(string_arg);
  if (len==0) return fd_incref(string_arg);
  else {
    char *hword = u8_malloc(len*2), *hyphens = u8_malloc(len+5);
    char ** rep = NULL; int * pos = NULL; int * cut = NULL;
    int retval = hnj_hyphen_hyphenate2(default_dict,string,len,hyphens,
                                     hword,&rep,&pos,&cut);
    if (retval) {
      u8_free(hyphens); u8_free(hword); fd_incref(string_arg);
      fd_seterr("Hyphenation error","get_hyphen_breaks",NULL,string_arg);
      return FD_ERROR_VALUE;}
    else {
      lispval result = fd_make_string(NULL,-1,(u8_string)hword);
      u8_free(hyphens); u8_free(hword);
      return result;}}
}

static lispval hyphen_breaks_prim(lispval string_arg)
{
  u8_string string = FD_CSTRING(string_arg);
  int len = FD_STRLEN(string_arg);
  if (len==0) return fd_empty_vector(0);
  else {
    char *hyphens = u8_malloc(len+5);
    char ** rep = NULL; int * pos = NULL; int * cut = NULL;
    int retval = hnj_hyphen_hyphenate2(default_dict,string,len,hyphens,
                                     NULL,&rep,&pos,&cut);
    if (retval) {
      u8_free(hyphens); fd_incref(string_arg);
      fd_seterr("Hyphenation error","get_hyphen_breaks",NULL,string_arg);
      return FD_ERROR_VALUE;}
    else {
      struct U8_OUTPUT out;
      const u8_byte *scan = string;
      lispval result = FD_VOID, *elts;
      int i = 0, n_breaks = 0, seg = 0, cpos = 0, c = u8_sgetc(&scan); while (i<len) {
        if (hyphens[i++]&1) n_breaks++;}
      result = fd_empty_vector(n_breaks+1);
      elts = FD_VECTOR_DATA(result);
      U8_INIT_OUTPUT(&out,32);
      while (c>=0) {
        u8_putc(&out,c);
        if (hyphens[cpos]&1) {
          lispval s = fd_make_string(NULL,out.u8_write-out.u8_outbuf,
                                  out.u8_outbuf);
          elts[seg++]=s;
          out.u8_write = out.u8_outbuf;}
        cpos = scan-string;
        c = u8_sgetc(&scan);}
      if (out.u8_write!=out.u8_outbuf) {
        lispval s = fd_make_string(NULL,out.u8_write-out.u8_outbuf,
                                out.u8_outbuf);
        elts[seg++]=s;
        out.u8_write = out.u8_outbuf;}
      u8_free(out.u8_outbuf);
      return result;}}
}

static lispval shyphenate_prim(lispval string_arg)
{
  u8_string string = FD_CSTRING(string_arg);
  int len = FD_STRLEN(string_arg);
  if (len==0) return fd_incref(string_arg);
  else {
    char *hyphens = u8_malloc(len+5);
    char ** rep = NULL; int * pos = NULL; int * cut = NULL;
    int retval = hnj_hyphen_hyphenate2(default_dict,string,len,hyphens,
                                     NULL,&rep,&pos,&cut);
    if (retval) {
      u8_free(hyphens); fd_incref(string_arg);
      fd_seterr("Hyphenation error","get_hyphen_breaks",NULL,string_arg);
      return FD_ERROR_VALUE;}
    else {
      lispval result = FD_VOID;
      const u8_byte *scan = string;
      struct U8_OUTPUT out;
      int cpos = 0, c = u8_sgetc(&scan);
      U8_INIT_OUTPUT(&out,len*3);
      while (c>=0) {
        u8_putc(&out,c);
        if (hyphens[cpos]&1) u8_putc(&out,0xAD);
        cpos = scan-string;
        c = u8_sgetc(&scan);}
      result = fd_make_string(NULL,out.u8_write-out.u8_outbuf,
                            out.u8_outbuf);
      u8_free(out.u8_outbuf);
      return result;}}
}

static int hyphenout_helper(U8_OUTPUT *out,
                            u8_string string,int len,
                            int hyphen_char)
{
  char *hyphens = u8_malloc(len+5);
  char **rep = NULL; int *pos = NULL; int *cut = NULL;
  int retval = hnj_hyphen_hyphenate2(default_dict,string,len,hyphens,
                                   NULL,&rep,&pos,&cut);
  if (retval) {
    u8_free(hyphens);
    return -1;}
  else {
    const u8_byte *scan = string;
    int cpos = 0, c = u8_sgetc(&scan), n_hyphens = 0;
    while (c>=0) {
      u8_putc(out,c);
      if ((cpos>EDGE_LEN)&&(hyphens[cpos]&1)&&(cpos<(len-EDGE_LEN))) {
        u8_putc(out,hyphen_char); n_hyphens++;}
      cpos = scan-string;
      c = u8_sgetc(&scan);}
    u8_free(hyphens);
    return n_hyphens;}
}

static lispval hyphenout_prim(lispval string_arg,lispval hyphen_arg)
{
  U8_OUTPUT *output = u8_current_output;
  u8_string string = FD_CSTRING(string_arg);
  int len = FD_STRLEN(string_arg);
  int hyphen_char = FD_CHAR2CODE(hyphen_arg);
  const u8_byte *scan = string;
  struct U8_OUTPUT word;
  int c = u8_sgetc(&scan);
  if (len==0) return FD_VOID;
  while ((c>=0)&&(!(u8_isalnum(c)))) {
    u8_putc(output,c); c = u8_sgetc(&scan);}
  U8_INIT_OUTPUT(&word,64);
  while (c>=0) {
    if (u8_isalnum(c)) {
      u8_putc(&word,c); c = u8_sgetc(&scan);}
    else if (u8_isspace(c)) {
      if (word.u8_write!=word.u8_outbuf) {
        hyphenout_helper
          (output,word.u8_outbuf,word.u8_write-word.u8_outbuf,
           hyphen_char);
        word.u8_write = word.u8_outbuf; word.u8_write[0]='\0';}
      while ((c>=0)&&(!(u8_isalnum(c)))) {
        u8_putc(output,c);
        c = u8_sgetc(&scan);}}
    else {
      int nc = u8_sgetc(&scan);
      if ((nc<0)||(!(u8_isalnum(nc)))) {
        if (word.u8_write!=word.u8_outbuf) {
          hyphenout_helper
            (output,word.u8_outbuf,word.u8_write-word.u8_outbuf,
             hyphen_char);
          word.u8_write = word.u8_outbuf; word.u8_write[0]='\0';}
        u8_putc(output,c);
        if (nc<0) break; else c = nc;
        while ((c>=0)&&(!(u8_isalnum(c)))) {
          u8_putc(output,c);
          c = u8_sgetc(&scan);}}
      else {u8_putc(&word,c); u8_putc(&word,nc); c = u8_sgetc(&scan);}}}
  if (word.u8_write!=word.u8_outbuf) {
    hyphenout_helper
      (output,word.u8_outbuf,word.u8_write-word.u8_outbuf,
       hyphen_char);}
  u8_free(word.u8_outbuf);
  u8_flush(output);
  return FD_VOID;
}

static lispval hyphenate_prim(lispval string_arg,lispval hyphen_arg)
{
  lispval result;
  struct U8_OUTPUT out; U8_OUTPUT *output = &out;
  u8_string string = FD_CSTRING(string_arg);
  int len = FD_STRLEN(string_arg);
  int hyphen_char = FD_CHAR2CODE(hyphen_arg);
  const u8_byte *scan = string;
  struct U8_OUTPUT word;
  int c = u8_sgetc(&scan);
  if (len==0) return fd_incref(string_arg);
  U8_INIT_OUTPUT(&out,len*2);
  U8_INIT_OUTPUT(&word,64);
  while ((c>=0)&&(!(u8_isalnum(c)))) {
    u8_putc(output,c); c = u8_sgetc(&scan);}
  while (c>=0) {
    if (u8_isalnum(c)) {
      u8_putc(&word,c); c = u8_sgetc(&scan);}
    else if (u8_isspace(c)) {
      if (word.u8_write!=word.u8_outbuf) {
        hyphenout_helper
          (output,word.u8_outbuf,word.u8_write-word.u8_outbuf,
           hyphen_char);
        word.u8_write = word.u8_outbuf; word.u8_write[0]='\0';}
      while ((c>=0)&&(!(u8_isalnum(c)))) {
        u8_putc(output,c);
        c = u8_sgetc(&scan);}
      if (c<0) break;}
    else {
      int nc = u8_sgetc(&scan);
      if ((nc<0)||(!(u8_isalnum(nc)))) {
        if (word.u8_write!=word.u8_outbuf) {
          hyphenout_helper
            (output,word.u8_outbuf,word.u8_write-word.u8_outbuf,
             hyphen_char);
          word.u8_write = word.u8_outbuf; word.u8_write[0]='\0';}
        u8_putc(output,c);
        if (nc<0) break; else c = nc;
        while ((c>=0)&&(!(u8_isalnum(c)))) {
          u8_putc(output,c);
          c = u8_sgetc(&scan);}}
      else {u8_putc(&word,c); u8_putc(&word,nc); c = u8_sgetc(&scan);}}}
  if (word.u8_write!=word.u8_outbuf) {
    hyphenout_helper
      (output,word.u8_outbuf,word.u8_write-word.u8_outbuf,
       hyphen_char);}
  u8_free(word.u8_outbuf);
  result = fd_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
  u8_free(out.u8_outbuf);
  return result;
}

FD_EXPORT int fd_init_hyphenate()
{
  lispval hyphenate_module;
  if (hyphenate_init) return 0;

  hyphenate_init = u8_millitime();

  fd_register_config("HYPHENDICT","Where the hyphenate module gets its data",
                     fd_sconfig_get,fd_sconfig_set,&default_hyphenation_file);

  if (default_hyphenation_file)
    default_dict = hnj_hyphen_load(default_hyphenation_file);
  else {
    u8_string dictfile = u8_mkpath(FD_DATA_DIR,"hyph_en_US.dic");
    if (u8_file_existsp(dictfile))
      default_dict = hnj_hyphen_load(dictfile);
    else {
      u8_log(LOG_CRIT,fd_FileNotFound,
             "Hyphenation dictionary %s does not exist!",
             dictfile);}
    u8_free(dictfile);}

  hyphenate_module =
    fd_new_cmodule("HYPHENATE",(FD_MODULE_SAFE),fd_init_hyphenate);

  fd_idefn(hyphenate_module,
           fd_make_cprim1x("HYPHENATE-WORD",
                           hyphenate_word_prim,1,fd_string_type,FD_VOID));
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
  fd_idefn(hyphenate_module,
           fd_make_cprim2x("HYPHENATE",
                           hyphenate_prim,1,
                           fd_string_type,FD_VOID,
                           fd_character_type,FD_CODE2CHAR(0xAD)));

  fd_finish_module(hyphenate_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
