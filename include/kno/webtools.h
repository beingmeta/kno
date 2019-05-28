/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2005-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_WEBTOOLS_H
#define KNO_WEBTOOLS_H 1
#ifndef KNO_WEBTOOLS_H_INFO
#define KNO_WEBTOOLS_H_INFO "include/kno/webtools.h"
#endif

KNO_EXPORT void kno_init_webtools(void) KNO_LIBINIT_FN;

/* XML input */

KNO_EXPORT u8_condition kno_XMLParseError;

#define KNO_XML_MALLOCD       2
#define KNO_XML_EMPTY_CLOSE   ((KNO_XML_MALLOCD)<<1) /* Handle </> as generic close */
#define KNO_XML_FOLDCASE      ((KNO_XML_EMPTY_CLOSE)<<1) /* Use foldcase when */
#define KNO_XML_AUTOCLOSE     ((KNO_XML_FOLDCASE)<<1) /* automatically close elements */
#define KNO_XML_BADCLOSE      ((KNO_XML_AUTOCLOSE)<<1) /* ignore dangling closes */
#define KNO_XML_BADATTRIB     ((KNO_XML_BADCLOSE)<<1) /* handle bad attribs */
#define KNO_XML_CLOSE_REPEATS ((KNO_XML_BADATTRIB)<<1)  /* let <p> close <p> */
#define KNO_XML_KEEP_RAW      ((KNO_XML_CLOSE_REPEATS)<<1)
#define KNO_XML_CRUSHSPACE    ((KNO_XML_KEEP_RAW)<<1)
#define KNO_XML_SLOTIFY       ((KNO_XML_CRUSHSPACE)<<1)
#define KNO_XML_NOCONTENTS    ((KNO_XML_SLOTIFY)<<1)
#define KNO_XML_NSFREE        ((KNO_XML_NOCONTENTS)<<1)  /* With respect to John Wayne... */
#define KNO_XML_NOEMPTY       ((KNO_XML_NSFREE)<<1)
/* Whether to decode entities in content */
#define KNO_XML_DECODE_ENTITIES ((KNO_XML_NOEMPTY)<<1)
/* The entries below here are internal state used by the XML parser,
   as opposed to general parsing options */
/* Whether the node has anything special (attributes or content) */
#define KNO_XML_HASDATA       ((KNO_XML_DECODE_ENTITIES)<<1)
/* Whether to handle HTML empty elements like HR, BR, META, LINK, etc. */
#define KNO_XML_ISHTML        ((KNO_XML_HASDATA)<<1)
/* Whether we're in an HTML paragraph */
#define KNO_XML_INPARA        ((KNO_XML_ISHTML)<<1)

#define KNO_XML_INHERIT_BITS                                   \
  ((KNO_XML_EMPTY_CLOSE)|(KNO_XML_AUTOCLOSE)|(KNO_XML_KEEP_RAW)| \
   (KNO_XML_CRUSHSPACE)|(KNO_XML_SLOTIFY)|(KNO_XML_NOCONTENTS)|  \
   (KNO_XML_NSFREE)|(KNO_XML_NOEMPTY)|(KNO_XML_ISHTML)|(KNO_XML_INPARA)| \
   (KNO_XML_FOLDCASE)|(KNO_XML_BADCLOSE)|(KNO_XML_CLOSE_REPEATS)| \
   (KNO_XML_DECODE_ENTITIES)|(KNO_XML_BADATTRIB))

#define KNO_XML_SLOPPY \
  ((KNO_XML_AUTOCLOSE)|(KNO_XML_EMPTY_CLOSE)|\
   (KNO_XML_CLOSE_REPEATS)|(KNO_XML_FOLDCASE)|(KNO_XML_BADCLOSE)|  \
   (KNO_XML_ISHTML)|(KNO_XML_BADATTRIB))
#define KNO_SLOPPY_XML KNO_XML_SLOPPY
#define KNO_XML_DATA \
  ((KNO_XML_CRUSHSPACE)|(KNO_XML_SLOTIFY)|\
   (KNO_XML_NOCONTENTS)|(KNO_XML_NSFREE)|\
   (KNO_XML_NOEMPTY))
#define KNO_DATA_XML KNO_XML_DATA

#define KNO_XML_DEFAULT_BITS 0

typedef struct KNO_XML {
  u8_string xml_eltname; int xml_bits;
  lispval xml_head, xml_attribs;
  u8_string xml_namespace;
  u8_string *xml_nsmap;
  int xml_size, xml_limit;
  struct KNO_PAIR *xml_content_tail;
  void *xml_data;
  struct KNO_XML *xml_parent;} KNO_XML;

typedef enum KNO_XMLELT_TYPE {
  xmlopen, xmlclose, xmlempty, xmlpi, xmldoctype, xmlcomment, xmlcdata }
  kno_xmlelt_type;

KNO_EXPORT void *kno_walk_markup
  (U8_INPUT *in,
   void *(*contentfn)(void *,u8_string),
   void *(*markupfn)(void *,u8_string),
   void *data);
KNO_EXPORT void kno_init_xml_node(KNO_XML *node,KNO_XML *parent,u8_string name);
KNO_EXPORT void kno_init_xml_attribs(KNO_XML *node);
KNO_EXPORT int kno_parse_xmltag
  (u8_byte **scanner,u8_byte *end,
   const u8_byte **elts,int max_elts,int sloppy);
KNO_EXPORT int kno_parse_element
  (u8_byte **scanner,u8_byte *end,
   const u8_byte **elts,int max_elts,int sloppy);
KNO_EXPORT kno_xmlelt_type kno_get_markup_type(u8_string buf,int len,int ishtml);

KNO_EXPORT void kno_default_contentfn(KNO_XML *node,u8_string s,int len);
KNO_EXPORT KNO_XML *kno_default_popfn(KNO_XML *node);
KNO_EXPORT KNO_XML *kno_xml_push
  (KNO_XML *newnode,KNO_XML *node,kno_xmlelt_type type,
   int (*attribfn)(KNO_XML *,u8_string,u8_string,int),
   u8_string *elts,int n_elts);
KNO_EXPORT int kno_default_attribfn(KNO_XML *xml,u8_string name,u8_string val,int quote);

KNO_EXPORT void kno_add_content(struct KNO_XML *node,lispval item);

KNO_EXPORT u8_string kno_xmlns_lookup(KNO_XML *xml,u8_string s,u8_string *nsp);
KNO_EXPORT lispval kno_make_qid(u8_string eltname,u8_string namespace);
KNO_EXPORT void kno_free_xml_node(KNO_XML *node);

KNO_EXPORT void kno_attrib_entify(u8_output out,u8_string value);
KNO_EXPORT u8_string kno_deentify(u8_string value,u8_string lim);

KNO_EXPORT void *kno_walk_xml
  (U8_INPUT *in,
   void (*contentfn)(KNO_XML *,u8_string,int),
   KNO_XML *(*pifn)(U8_INPUT *,KNO_XML *,u8_string,int),
   int (*attribfn)(KNO_XML *,u8_string,u8_string,int),
   KNO_XML *(*pushfn)(KNO_XML *,kno_xmlelt_type,u8_string *,int),
   KNO_XML *(*popfn)(KNO_XML *),
   KNO_XML *node);

KNO_EXPORT int kno_xmlparseoptions(lispval x);

/* XMLEVAL stuff */

KNO_EXPORT int kno_lisp2xml(u8_output out,lispval x,kno_lexenv env);
KNO_EXPORT void kno_xmloid(u8_output out,lispval oid_arg);
KNO_EXPORT lispval kno_xmleval(u8_output out,lispval xml,kno_lexenv env);
KNO_EXPORT lispval kno_xmlevalout(u8_output out,lispval xml,
                               kno_lexenv scheme_env,kno_lexenv xml_env);
KNO_EXPORT lispval kno_xmleval_with(u8_output out,lispval xml,lispval,lispval);

KNO_EXPORT lispval kno_open_xml(lispval xml,kno_lexenv env);
KNO_EXPORT lispval kno_close_xml(lispval xml);
KNO_EXPORT lispval kno_unparse_xml(u8_output out,lispval xml,kno_lexenv env);
KNO_EXPORT lispval kno_xmlout(u8_output out,lispval xml,
                           kno_lexenv scheme_env,
                           kno_lexenv xml_env);
KNO_EXPORT int kno_xmlout_helper(U8_OUTPUT *out,U8_OUTPUT *tmp,lispval x,
                               lispval xmloidfn,kno_lexenv env);

KNO_EXPORT struct KNO_XML *kno_load_knoml(u8_input in,int bits);
KNO_EXPORT struct KNO_XML *kno_read_knoml(u8_input in,int bits);
KNO_EXPORT struct KNO_XML *kno_parse_knoml(u8_input in,int bits);
KNO_EXPORT lispval kno_knoml_arg(lispval arg);

KNO_EXPORT void kno_lisp2html(u8_output s,lispval v,u8_string tag,u8_string cl);

/* CGIEXEC stuff */

KNO_EXPORT int kno_parse_cgidata(lispval data);
KNO_EXPORT lispval kno_cgiexec(lispval proc,lispval cgidata);
KNO_EXPORT void kno_urify(u8_output out,lispval val);
KNO_EXPORT lispval kno_mapurl(lispval uri);

KNO_EXPORT char *kno_sendfile_header;

/* URI stuff */

KNO_EXPORT void kno_uri_output(u8_output out,u8_string uri,int,int,const char *);
KNO_EXPORT lispval kno_parse_uri(u8_string uri,lispval base);

/* XML output stuff */

KNO_EXPORT int kno_open_markup
  (u8_output out,u8_string eltname,lispval attribs,int empty);
KNO_EXPORT void kno_emit_xmlcontent(u8_output out,u8_string content);
KNO_EXPORT void kno_emit_xmlattrib
(u8_output out,u8_output tmp,u8_string name,lispval value,int lower);

KNO_EXPORT void kno_xhtmlerrorpage(u8_output s,u8_exception ex);
KNO_EXPORT void kno_xhtmldebugpage(u8_output s,u8_exception ex);
KNO_EXPORT int kno_output_xhtml_preface(U8_OUTPUT *out,lispval cgidata);
KNO_EXPORT int kno_output_xml_preface(U8_OUTPUT *out,lispval cgidata);
KNO_EXPORT int kno_output_http_headers(U8_OUTPUT *out,lispval cgidata);

KNO_EXPORT int kno_output_http_headers(U8_OUTPUT *out,lispval cgidata);

KNO_EXPORT int kno_cache_markup;

/* URI parsing stuff */

KNO_EXPORT lispval kno_parse_uri(u8_string uri,lispval base);
KNO_EXPORT lispval kno_parse_uri(u8_string uri,lispval base);

/* MIME parsing stuff */

KNO_EXPORT lispval kno_parse_multipart_mime(lispval,const char *,const char *);
KNO_EXPORT lispval kno_parse_mime(const char *,const char *);
KNO_EXPORT lispval kno_handle_compound_mime_field(lispval,lispval,lispval);

/* Init functions */

KNO_EXPORT void kno_init_xmleval_c(void);
KNO_EXPORT void kno_init_xmldata_c(void);
KNO_EXPORT void kno_init_htmlout_c(void);
KNO_EXPORT void kno_init_xmlinput_c(void);
KNO_EXPORT void kno_init_xmloutput_c(void);
KNO_EXPORT void kno_init_mime_c(void);
KNO_EXPORT void kno_init_email_c(void);
KNO_EXPORT void kno_init_cgiexec_c(void);
KNO_EXPORT void kno_init_urifns_c(void);
KNO_EXPORT void kno_init_exif_c(void);
KNO_EXPORT void kno_init_curl_c(void);
KNO_EXPORT void kno_init_json_c(void);

/* Default XML/HTML info */

#define DEFAULT_CONTENT_TYPE \
  "Content-type: text/html; charset = utf-8;"
#define DEFAULT_DOCTYPE \
  "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.1//EN\"\
               \"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd\">"
#define DEFAULT_XMLPI \
  "<?xml version='1.0' charset='utf-8' ?>"

#endif /* KNO_WEBTOOLS_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
