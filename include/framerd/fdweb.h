/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2005-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_FDWEB_H
#define FRAMERD_FDWEB_H 1
#ifndef FRAMERD_FDWEB_H_INFO
#define FRAMERD_FDWEB_H_INFO "include/framerd/fdweb.h"
#endif

FD_EXPORT void fd_init_fdweb(void) FD_LIBINIT_FN;

/* XML input */

FD_EXPORT u8_condition fd_XMLParseError;

#define FD_XML_MALLOCD       2
#define FD_XML_EMPTY_CLOSE   ((FD_XML_MALLOCD)<<1) /* Handle </> as generic close */
#define FD_XML_FOLDCASE      ((FD_XML_EMPTY_CLOSE)<<1) /* Use foldcase when */
#define FD_XML_AUTOCLOSE     ((FD_XML_FOLDCASE)<<1) /* automatically close elements */
#define FD_XML_BADCLOSE      ((FD_XML_AUTOCLOSE)<<1) /* ignore dangling closes */
#define FD_XML_BADATTRIB     ((FD_XML_BADCLOSE)<<1) /* handle bad attribs */
#define FD_XML_CLOSE_REPEATS ((FD_XML_BADATTRIB)<<1)  /* let <p> close <p> */
#define FD_XML_KEEP_RAW      ((FD_XML_CLOSE_REPEATS)<<1)
#define FD_XML_CRUSHSPACE    ((FD_XML_KEEP_RAW)<<1)
#define FD_XML_SLOTIFY       ((FD_XML_CRUSHSPACE)<<1)
#define FD_XML_NOCONTENTS    ((FD_XML_SLOTIFY)<<1)
#define FD_XML_NSFREE        ((FD_XML_NOCONTENTS)<<1)  /* With respect to John Wayne... */
#define FD_XML_NOEMPTY       ((FD_XML_NSFREE)<<1)
/* Whether to decode entities in content */
#define FD_XML_DECODE_ENTITIES ((FD_XML_NOEMPTY)<<1)
/* The entries below here are internal state used by the XML parser,
   as opposed to general parsing options */
/* Whether the node has anything special (attributes or content) */
#define FD_XML_HASDATA       ((FD_XML_DECODE_ENTITIES)<<1)
/* Whether to handle HTML empty elements like HR, BR, META, LINK, etc. */
#define FD_XML_ISHTML        ((FD_XML_HASDATA)<<1)
/* Whether we're in an HTML paragraph */
#define FD_XML_INPARA        ((FD_XML_ISHTML)<<1)

#define FD_XML_INHERIT_BITS                                   \
  ((FD_XML_EMPTY_CLOSE)|(FD_XML_AUTOCLOSE)|(FD_XML_KEEP_RAW)| \
   (FD_XML_CRUSHSPACE)|(FD_XML_SLOTIFY)|(FD_XML_NOCONTENTS)|  \
   (FD_XML_NSFREE)|(FD_XML_NOEMPTY)|(FD_XML_ISHTML)|(FD_XML_INPARA)| \
   (FD_XML_FOLDCASE)|(FD_XML_BADCLOSE)|(FD_XML_CLOSE_REPEATS)| \
   (FD_XML_DECODE_ENTITIES)|(FD_XML_BADATTRIB))

#define FD_XML_SLOPPY \
  ((FD_XML_AUTOCLOSE)|(FD_XML_EMPTY_CLOSE)|\
   (FD_XML_CLOSE_REPEATS)|(FD_XML_FOLDCASE)|(FD_XML_BADCLOSE)|  \
   (FD_XML_ISHTML)|(FD_XML_BADATTRIB))
#define FD_SLOPPY_XML FD_XML_SLOPPY
#define FD_XML_DATA \
  ((FD_XML_CRUSHSPACE)|(FD_XML_SLOTIFY)|\
   (FD_XML_NOCONTENTS)|(FD_XML_NSFREE)|\
   (FD_XML_NOEMPTY))
#define FD_DATA_XML FD_XML_DATA

#define FD_XML_DEFAULT_BITS 0

typedef struct FD_XML {
  u8_string xml_eltname; int xml_bits;
  lispval xml_head, xml_attribs;
  u8_string xml_namespace;
  u8_string *xml_nsmap;
  int xml_size, xml_limit;
  struct FD_PAIR *xml_content_tail;
  void *xml_data;
  struct FD_XML *xml_parent;} FD_XML;

typedef enum FD_XMLELT_TYPE {
  xmlopen, xmlclose, xmlempty, xmlpi, xmldoctype, xmlcomment, xmlcdata }
  fd_xmlelt_type;

FD_EXPORT void *fd_walk_markup
  (U8_INPUT *in,
   void *(*contentfn)(void *,u8_string),
   void *(*markupfn)(void *,u8_string),
   void *data);
FD_EXPORT void fd_init_xml_node(FD_XML *node,FD_XML *parent,u8_string name);
FD_EXPORT void fd_init_xml_attribs(FD_XML *node);
FD_EXPORT int fd_parse_xmltag
  (u8_byte **scanner,u8_byte *end,
   const u8_byte **elts,int max_elts,int sloppy);
FD_EXPORT int fd_parse_element
  (u8_byte **scanner,u8_byte *end,
   const u8_byte **elts,int max_elts,int sloppy);
FD_EXPORT fd_xmlelt_type fd_get_markup_type(u8_string buf,int len,int ishtml);

FD_EXPORT void fd_default_contentfn(FD_XML *node,u8_string s,int len);
FD_EXPORT FD_XML *fd_default_popfn(FD_XML *node);
FD_EXPORT FD_XML *fd_xml_push
  (FD_XML *newnode,FD_XML *node,fd_xmlelt_type type,
   int (*attribfn)(FD_XML *,u8_string,u8_string,int),
   u8_string *elts,int n_elts);
FD_EXPORT int fd_default_attribfn(FD_XML *xml,u8_string name,u8_string val,int quote);

FD_EXPORT void fd_add_content(struct FD_XML *node,lispval item);

FD_EXPORT u8_string fd_xmlns_lookup(FD_XML *xml,u8_string s,u8_string *nsp);
FD_EXPORT lispval fd_make_qid(u8_string eltname,u8_string namespace);
FD_EXPORT void fd_free_xml_node(FD_XML *node);

FD_EXPORT void fd_attrib_entify(u8_output out,u8_string value);
FD_EXPORT u8_string fd_deentify(u8_string value,u8_string lim);

FD_EXPORT void *fd_walk_xml
  (U8_INPUT *in,
   void (*contentfn)(FD_XML *,u8_string,int),
   FD_XML *(*pifn)(U8_INPUT *,FD_XML *,u8_string,int),
   int (*attribfn)(FD_XML *,u8_string,u8_string,int),
   FD_XML *(*pushfn)(FD_XML *,fd_xmlelt_type,u8_string *,int),
   FD_XML *(*popfn)(FD_XML *),
   FD_XML *node);

FD_EXPORT int fd_xmlparseoptions(lispval x);

/* XMLEVAL stuff */

FD_EXPORT int fd_lisp2xml(u8_output out,lispval x,fd_lexenv env);
FD_EXPORT void fd_xmloid(u8_output out,lispval oid_arg);
FD_EXPORT lispval fd_xmleval(u8_output out,lispval xml,fd_lexenv env);
FD_EXPORT lispval fd_xmlevalout(u8_output out,lispval xml,
                               fd_lexenv scheme_env,fd_lexenv xml_env);
FD_EXPORT lispval fd_xmleval_with(u8_output out,lispval xml,lispval,lispval);

FD_EXPORT lispval fd_open_xml(lispval xml,fd_lexenv env);
FD_EXPORT lispval fd_close_xml(lispval xml);
FD_EXPORT lispval fd_unparse_xml(u8_output out,lispval xml,fd_lexenv env);
FD_EXPORT lispval fd_xmlout(u8_output out,lispval xml,
                           fd_lexenv scheme_env,
                           fd_lexenv xml_env);
FD_EXPORT int fd_xmlout_helper(U8_OUTPUT *out,U8_OUTPUT *tmp,lispval x,
                               lispval xmloidfn,fd_lexenv env);

FD_EXPORT struct FD_XML *fd_load_fdxml(u8_input in,int bits);
FD_EXPORT struct FD_XML *fd_read_fdxml(u8_input in,int bits);
FD_EXPORT struct FD_XML *fd_parse_fdxml(u8_input in,int bits);
FD_EXPORT lispval fd_fdxml_arg(lispval arg);

FD_EXPORT void fd_lisp2html(u8_output s,lispval v,u8_string tag,u8_string cl);

/* CGIEXEC stuff */

FD_EXPORT int fd_parse_cgidata(lispval data);
FD_EXPORT lispval fd_cgiexec(lispval proc,lispval cgidata);
FD_EXPORT void fd_urify(u8_output out,lispval val);
FD_EXPORT lispval fd_mapurl(lispval uri);

FD_EXPORT char *fd_sendfile_header;

/* URI stuff */

FD_EXPORT void fd_uri_output(u8_output out,u8_string uri,int,int,const char *);
FD_EXPORT lispval fd_parse_uri(u8_string uri,lispval base);

/* XML output stuff */

FD_EXPORT int fd_open_markup
  (u8_output out,u8_string eltname,lispval attribs,int empty);
FD_EXPORT void fd_emit_xmlcontent(u8_output out,u8_string content);
FD_EXPORT void fd_emit_xmlattrib
(u8_output out,u8_output tmp,u8_string name,lispval value,int lower);

FD_EXPORT void fd_xhtmlerrorpage(u8_output s,u8_exception ex);
FD_EXPORT void fd_xhtmldebugpage(u8_output s,u8_exception ex);
FD_EXPORT int fd_output_xhtml_preface(U8_OUTPUT *out,lispval cgidata);
FD_EXPORT int fd_output_xml_preface(U8_OUTPUT *out,lispval cgidata);
FD_EXPORT int fd_output_http_headers(U8_OUTPUT *out,lispval cgidata);

FD_EXPORT int fd_output_http_headers(U8_OUTPUT *out,lispval cgidata);

FD_EXPORT int fd_cache_markup;

/* URI parsing stuff */

FD_EXPORT lispval fd_parse_uri(u8_string uri,lispval base);
FD_EXPORT lispval fd_parse_uri(u8_string uri,lispval base);

/* MIME parsing stuff */

FD_EXPORT lispval fd_parse_multipart_mime(lispval,const char *,const char *);
FD_EXPORT lispval fd_parse_mime(const char *,const char *);
FD_EXPORT lispval fd_handle_compound_mime_field(lispval,lispval,lispval);

/* Init functions */

FD_EXPORT void fd_init_xmleval_c(void);
FD_EXPORT void fd_init_xmldata_c(void);
FD_EXPORT void fd_init_htmlout_c(void);
FD_EXPORT void fd_init_xmlinput_c(void);
FD_EXPORT void fd_init_xmloutput_c(void);
FD_EXPORT void fd_init_mime_c(void);
FD_EXPORT void fd_init_email_c(void);
FD_EXPORT void fd_init_cgiexec_c(void);
FD_EXPORT void fd_init_urifns_c(void);
FD_EXPORT void fd_init_exif_c(void);
FD_EXPORT void fd_init_curl_c(void);
FD_EXPORT void fd_init_json_c(void);

/* Default XML/HTML info */

#define DEFAULT_CONTENT_TYPE \
  "Content-type: text/html; charset = utf-8;"
#define DEFAULT_DOCTYPE \
  "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.1//EN\"\
               \"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd\">"
#define DEFAULT_XMLPI \
  "<?xml version='1.0' charset='utf-8' ?>"

#endif /* FRAMERD_FDWEB_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
