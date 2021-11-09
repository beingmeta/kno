/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

/* Dealing with DTYPE (dynamically typed) objects */

#ifndef KNO_LISP_H
#define KNO_LISP_H 1
#ifndef KNO_LISP_H_INFO
#define KNO_LISP_H_INFO "include/kno/lisp.h"
#endif

#include "common.h"
#include "errors.h"
#include "ptr.h"
#include "cons.h"
#include "compounds.h"

KNO_EXPORT u8_condition kno_NoMethod;
KNO_EXPORT u8_condition kno_NotAnOID;

KNO_EXPORT double kno_load_start;

KNO_EXPORT int kno_major_version, kno_minor_version, kno_release_version;
KNO_EXPORT u8_string kno_version, kno_version;

U8_EXPORT
/** Returns the current Kno version string (derived from the GIT branch)
    @returns a utf8 string
**/
u8_string kno_getversion(void);

U8_EXPORT
/** Returns the current Kno version string (major.minor.release)
    @returns a utf8 string
**/
u8_string kno_getversion(void);

U8_EXPORT
/** Returns the current Kno major version number
    @returns an int
**/
int kno_getmajorversion(void);

KNO_EXPORT int kno_init_lisp_types(void) KNO_LIBINIT_FN;
KNO_EXPORT int kno_boot_message(void);
KNO_EXPORT void kno_log_status(u8_condition why);
KNO_EXPORT int kno_be_vewy_quiet;

KNO_EXPORT lispval kno_parse_expr(struct U8_INPUT *);
KNO_EXPORT lispval kno_parser(struct U8_INPUT *);
KNO_EXPORT lispval kno_parse(u8_string string);
KNO_EXPORT lispval kno_parse_arg(u8_string string);
KNO_EXPORT lispval kno_parse_slotid(u8_string string);
KNO_EXPORT lispval kno_slotid_parser(u8_input in);
KNO_EXPORT u8_string kno_unparse_arg(lispval obj);
KNO_EXPORT lispval kno_read_arg(u8_input s);
KNO_EXPORT int kno_skip_whitespace(u8_input s);
KNO_EXPORT int kno_read_escape(u8_input in);
KNO_EXPORT lispval kno_decode_string(u8_input in);
KNO_EXPORT int kno_escape_string
(U8_OUTPUT *out,lispval x,int ascii,int max_chars);

KNO_EXPORT int kno_unparse(u8_output,lispval);
KNO_EXPORT u8_string kno_lisp2string(lispval x);
KNO_EXPORT u8_string kno_lisp2buf(lispval x,size_t n,u8_byte *buf);

typedef lispval (*kno_listobj_fn)(lispval obj);
KNO_EXPORT int kno_list_object(u8_output out,
                             lispval result,
                             u8_string label,
                             u8_string pathref,
                             u8_string indent,
                             kno_listobj_fn listfn,
                             int width,
                             int show_elts);


KNO_EXPORT lispval kno_string2number(u8_string string,int base);
KNO_EXPORT int kno_output_number(u8_output output,lispval num,int base,int sep);

KNO_EXPORT int kno_unparse_maxchars, kno_unparse_maxelts;
KNO_EXPORT int kno_humane_maxchars, kno_humane_maxelts;
KNO_EXPORT int kno_unparse_hexpacket, kno_packet_outfmt;
KNO_EXPORT int kno_integer_sepchar;

KNO_EXPORT int kno_register_record_tag
  (lispval symbol,lispval (*recreate)(int n,lispval *v));

KNO_EXPORT lispval kno_runtime_features;
KNO_EXPORT int kno_feature_testp(lispval expr);

KNO_EXPORT int kno_parse_pointers;

KNO_EXPORT int (*kno_unparse_error)(U8_OUTPUT *,lispval x,u8_string details);
KNO_EXPORT int (*kno_dtype_error)(struct KNO_OUTBUF *,lispval x,u8_string details);
KNO_EXPORT void kno_set_oid_parser(lispval (*parsefn)(u8_string start,int len));
typedef int (*kno_hashfn)(lispval x,unsigned int (*hf)(lispval));
KNO_EXPORT lispval kno_parse_oid_addr(u8_string string,int len);
KNO_EXPORT lispval kno_parse_oid(u8_input in);
KNO_EXPORT kno_hashfn kno_hashfns[];

KNO_EXPORT int kno_add_constname(u8_string s,lispval value);
KNO_EXPORT lispval kno_lookup_constname(u8_string s);

KNO_EXPORT int pprint_maxchars;
KNO_EXPORT int pprint_maxbytes;
KNO_EXPORT int pprint_maxelts;
KNO_EXPORT int pprint_maxdepth;
KNO_EXPORT int pprint_list_max;
KNO_EXPORT int pprint_vector_max;
KNO_EXPORT int pprint_choice_max;
KNO_EXPORT int pprint_keys_max;
KNO_EXPORT lispval pprint_default_rules;

KNO_EXPORT int kno_pprint
(u8_output out,lispval x,u8_string prefix,
 int indent,int col,int maxcol);

/* Copying LISP values */

KNO_EXPORT void *_kno_lspcpy(lispval *dest,const lispval *src,int n);

#if KNO_CORE
KNO_FASTOP void *kno_lspcpy(lispval *dest,const lispval *src,int n)
{
  return memcpy(dest,src,sizeof(lispval)*n);
}
#else
#define kno_lspcpy _kno_lspcpy
#endif

#define kno_lspset(dest,val,n) kno_init_elts(dest,n,val)

#endif /* ndef KNO_LISP_H */

