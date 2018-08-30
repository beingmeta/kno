/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Dealing with DTYPE (dynamically typed) objects */

#ifndef FRAMERD_DTYPE_H
#define FRAMERD_DTYPE_H 1
#ifndef FRAMERD_DTYPE_H_INFO
#define FRAMERD_DTYPE_H_INFO "include/framerd/dtype.h"
#endif

#include "common.h"
#include "ptr.h"
#include "cons.h"
#include "compounds.h"

FD_EXPORT u8_condition fd_NoMethod;
FD_EXPORT u8_condition fd_NotAnOID;

FD_EXPORT double fd_load_start;

FD_EXPORT int fd_major_version, fd_minor_version, fd_release_version;
FD_EXPORT u8_string fd_version, fd_revision;

U8_EXPORT
/** Returns the current FramerD revision string (derived from the GIT branch)
    @returns a utf8 string
**/
u8_string fd_getrevision(void);

U8_EXPORT
/** Returns the current FramerD version string (major.minor.release)
    @returns a utf8 string
**/
u8_string fd_getversion(void);

U8_EXPORT
/** Returns the current FramerD major version number
    @returns an int
**/
int fd_getmajorversion(void);

FD_EXPORT int fd_init_lisp_types(void) FD_LIBINIT_FN;
FD_EXPORT int fd_boot_message(void);
FD_EXPORT void fd_log_status(u8_condition why);
FD_EXPORT int fd_be_vewy_quiet;

FD_EXPORT lispval fd_parse_expr(struct U8_INPUT *);
FD_EXPORT lispval fd_parser(struct U8_INPUT *);
FD_EXPORT lispval fd_parse(u8_string string);
FD_EXPORT lispval fd_parse_arg(u8_string string);
FD_EXPORT u8_string fd_unparse_arg(lispval obj);
FD_EXPORT lispval fd_read_arg(u8_input s);
FD_EXPORT int fd_skip_whitespace(u8_input s);
FD_EXPORT int fd_read_escape(u8_input in);

FD_EXPORT int fd_unparse(u8_output,lispval);
FD_EXPORT u8_string fd_lisp2string(lispval x);
FD_EXPORT u8_string fd_lisp2buf(lispval x,size_t n,u8_byte *buf);

typedef lispval (*fd_listobj_fn)(lispval obj);
FD_EXPORT int fd_list_object(u8_output out,
                             lispval result,
                             u8_string label,
                             u8_string pathref,
                             u8_string indent,
                             fd_listobj_fn listfn,
                             int width,
                             int show_elts);


FD_EXPORT lispval fd_string2number(u8_string string,int base);
FD_EXPORT int fd_output_number(u8_output output,lispval num,int base);

FD_EXPORT int fd_unparse_maxchars, fd_unparse_maxelts;
FD_EXPORT int fd_unparse_hexpacket, fd_packet_outfmt;

FD_EXPORT int fd_register_record_tag
  (lispval symbol,lispval (*recreate)(int n,lispval *v));

FD_EXPORT int fd_parse_pointers;
FD_EXPORT lispval (*_fd_parse_number)(u8_string,int);

FD_EXPORT int (*fd_unparse_error)(U8_OUTPUT *,lispval x,u8_string details);
FD_EXPORT int (*fd_dtype_error)(struct FD_OUTBUF *,lispval x,u8_string details);
FD_EXPORT void fd_set_oid_parser(lispval (*parsefn)(u8_string start,int len));
typedef int (*fd_hashfn)(lispval x,unsigned int (*hf)(lispval));
FD_EXPORT lispval fd_parse_oid_addr(u8_string string,int len);
FD_EXPORT fd_hashfn fd_hashfns[];

FD_EXPORT int fd_add_constname(u8_string s,lispval value);
FD_EXPORT lispval fd_lookup_constname(u8_string s);

FD_EXPORT int pprint_maxchars;
FD_EXPORT int pprint_maxbytes;
FD_EXPORT int pprint_maxelts;
FD_EXPORT int pprint_maxdepth;
FD_EXPORT int pprint_list_max;
FD_EXPORT int pprint_vector_max;
FD_EXPORT int pprint_choice_max;
FD_EXPORT int pprint_keys_max;
FD_EXPORT lispval pprint_default_rules;

FD_EXPORT int fd_pprint
(u8_output out,lispval x,u8_string prefix,
 int indent,int col,int maxcol);

#endif /* ndef FRAMERD_DTYPE_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
