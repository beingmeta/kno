/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Dealing with DTYPE (dynamically typed) objects */

#ifndef FRAMERD_PPRINT_H
#define FRAMERD_PPRINT_H 1
#ifndef FRAMERD_PPRINT_H_INFO
#define FRAMERD_PPRINT_H_INFO "include/framerd/pprint.h"
#endif

#include "common.h"
#include "ptr.h"
#include "cons.h"

/* -1 = unlimited, 0 means use default */
FD_EXPORT int pprint_maxcol;
FD_EXPORT int pprint_fudge;
FD_EXPORT int pprint_maxchars;
FD_EXPORT int pprint_maxbytes;
FD_EXPORT int pprint_maxelts;
FD_EXPORT int pprint_maxdepth;
FD_EXPORT int pprint_list_max;
FD_EXPORT int pprint_vector_max;
FD_EXPORT int pprint_choice_max;
FD_EXPORT int pprint_maxkeys;
FD_EXPORT u8_string pprint_margin;
FD_EXPORT lispval pprint_default_rules;

typedef struct PPRINT_CONTEXT {
  u8_string pp_margin;
  int pp_margin_len;
  /* -1 = unlimited, 0 means use default */
  int pp_maxcol;
  int pp_maxdepth;
  int pp_fudge;
  int pp_maxelts;
  int pp_maxchars;
  int pp_maxbytes;
  int pp_maxkeys;
  int pp_list_max;
  int pp_vector_max;
  int pp_choice_max;
  /* Reserved for future use */
  void *pp_customfn; void *pp_customdata;
  /* For customization */
  lispval pp_rules;
  /* For other display options */
  lispval pp_opts;}
  *pprint_context;

typedef int (*fd_pprintfn)(u8_output out,lispval x,
			   int indent,int col,int depth,
			   struct PPRINT_CONTEXT *ppcxt,
			   void *data);

FD_EXPORT
int fd_pprinter(u8_output out,lispval x,int indent,int col,int depth,
		fd_pprintfn customfn,void *customdata,
		struct PPRINT_CONTEXT *ppcxt);
FD_EXPORT int fd_pprint_x
(u8_output out,lispval x,u8_string margin,
 int indent,int col,int maxcol,
 fd_pprintfn fn,void *data);
FD_EXPORT int fd_pprint
(u8_output out,lispval x,u8_string margin,
 int indent,int col,int maxcol);

FD_EXPORT
int fd_pprint_table(u8_output out,lispval x,
		    const lispval *keys,size_t n_keys,
		    int indent,int col,int depth,
		    fd_pprintfn customfn,void *customdata,
		    struct PPRINT_CONTEXT *ppcxt);

#endif /* ndef FRAMERD_PPRINT_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
