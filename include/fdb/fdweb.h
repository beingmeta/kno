/* -*- Mode: C; -*- */

/* Copyright (C) 2005-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_FDWEB_H
#define FDB_FDWEB_H 1
#define FDB_FDWEB_H_VERSION \
   "$Id$"

FD_EXPORT void fd_init_fdweb(void) FD_LIBINIT_FN;

/* XML input */

FD_EXPORT fd_exception fd_XMLParseError;

#define FD_XML_MALLOCD 2
#define FD_XML_EMPTY_CLOSE 4 /* Handle </> as generic close */
#define FD_XML_AUTOCLOSE 8 /* automatically close elements */
#define FD_XML_CLOSE_REPEATS 16
#define FD_XML_KEEP_RAW 32
#define FD_XML_CRUSHSPACE 64
#define FD_XML_SLOTIFY 128
#define FD_XML_NOCONTENTS 256
#define FD_XML_NSFREE 512  /* With respect to John Wayne... */ 
#define FD_XML_NOEMPTY 1024
#define FD_XML_HASDATA 2048

#define FD_XML_INHERIT_BITS                                   \
  ((FD_XML_EMPTY_CLOSE)|(FD_XML_AUTOCLOSE)|(FD_XML_KEEP_RAW)| \
   (FD_XML_CRUSHSPACE)|(FD_XML_SLOTIFY)|(FD_XML_NOCONTENTS)|  \
   (FD_XML_NSFREE)|(FD_XML_NOEMPTY))

#define FD_SLOPPY_XML \
  (FD_XML_AUTOCLOSE|FD_XML_EMPTY_CLOSE)
#define FD_DATA_XML \
  (FD_XML_CRUSHSPACE|FD_XML_SLOTIFY|FD_XML_NOCONTENTS|FD_XML_NSFREE|FD_XML_NOEMPTY)

#define FD_XML_DEFAULT_BITS 0

typedef struct FD_XML {
  u8_string eltname; int bits;
  fdtype attribs, head;
  u8_string namespace;
  u8_string *nsmap; int size, limit;
  struct FD_PAIR *tail; void *data;
  struct FD_XML *parent;} FD_XML;

typedef enum FD_XMLELT_TYPE {
  xmlopen, xmlclose, xmlempty, xmlpi, xmldoctype, xmlcomment, xmlcdata }
  fd_xmlelt_type;

FD_EXPORT void *fd_walk_markup
  (U8_INPUT *in,
   void *(*contentfn)(void *,u8_string),
   void *(*markupfn)(void *,u8_string),
   void *data);
FD_EXPORT void fd_init_xml_node(FD_XML *node,FD_XML *parent,u8_string name);
FD_EXPORT int fd_parse_element
  (u8_byte **scanner,u8_byte *end,u8_byte **elts,int max_elts);
FD_EXPORT fd_xmlelt_type fd_get_markup_type(u8_string buf,int len);

FD_EXPORT void fd_default_contentfn(FD_XML *node,u8_string s,int len);
FD_EXPORT FD_XML *fd_default_popfn(FD_XML *node);
FD_EXPORT FD_XML *fd_xml_push
  (FD_XML *newnode,FD_XML *node,fd_xmlelt_type type,
   int (*attribfn)(FD_XML *,u8_string,u8_string,int),
   u8_string *elts,int n_elts);
FD_EXPORT int fd_default_attribfn(FD_XML *xml,u8_string name,u8_string val,int quote);
FD_EXPORT int fd_strict_attribfn(FD_XML *xml,u8_string name,u8_string val,int quote);

FD_EXPORT void fd_add_content(struct FD_XML *node,fdtype item);

FD_EXPORT u8_string fd_xmlns_lookup(FD_XML *xml,u8_string s,u8_string *nsp);
FD_EXPORT fdtype fd_make_qid(u8_string eltname,u8_string namespace);

FD_EXPORT void fd_attrib_entify(u8_output out,u8_string value);

FD_EXPORT void *fd_walk_xml
  (U8_INPUT *in,
   void (*contentfn)(FD_XML *,u8_string,int),
   FD_XML *(*pifn)(U8_INPUT *,FD_XML *,u8_string,int),
   int (*attribfn)(FD_XML *,u8_string,u8_string,int),
   FD_XML *(*pushfn)(FD_XML *,fd_xmlelt_type,u8_string *,int),
   FD_XML *(*popfn)(FD_XML *),
   FD_XML *node);

FD_EXPORT int fd_xmlparseoptions(fdtype x);

/* XMLEVAL stuff */

FD_EXPORT int fd_dtype2xml(u8_output out,fdtype x,fd_lispenv env);
FD_EXPORT void fd_xmloid(u8_output out,fdtype oid_arg);
FD_EXPORT fdtype fd_xmleval(u8_output out,fdtype xml,fd_lispenv env);
FD_EXPORT fdtype fd_unparse_xml(u8_output out,fdtype xml,fd_lispenv env);
FD_EXPORT struct FD_XML *fd_read_fdxml(u8_input in,int bits);

/* CGIEXEC stuff */

FD_EXPORT int fd_parse_cgidata(fdtype data);
FD_EXPORT fdtype fd_cgiexec(fdtype proc,fdtype cgidata);

/* XML output stuff */

FD_EXPORT int fd_open_markup
  (u8_output out,u8_string eltname,fdtype attribs,int empty);

FD_EXPORT void fd_xhtmlerrorpage(u8_output s,fdtype error);
FD_EXPORT int fd_output_xhtml_preface(U8_OUTPUT *out,fdtype cgidata);
FD_EXPORT void fd_output_http_headers(U8_OUTPUT *out,fdtype cgidata);

/* URI parsing stuff */

FD_EXPORT fdtype fd_parse_uri(u8_string uri,fdtype base);
FD_EXPORT fdtype fd_parse_uri(u8_string uri,fdtype base);


/* MIME parsing stuff */

FD_EXPORT fdtype fd_parse_multipart_mime(fdtype,char *,char *);
FD_EXPORT fdtype fd_parse_mime(char *,char *);
FD_EXPORT fdtype fd_handle_compound_mime_field(fdtype,fdtype,fdtype);

/* Init functions */

FD_EXPORT void fd_init_xmldata_c(void);
FD_EXPORT void fd_init_xmlinput_c(void);
FD_EXPORT void fd_init_mime_c(void);
FD_EXPORT void fd_init_xmleval_c(void);
FD_EXPORT void fd_init_cgiexec_c(void);
FD_EXPORT void fd_init_urifns_c(void);
FD_EXPORT void fd_init_exif_c(void);
FD_EXPORT void fd_init_curl_c(void);

#endif /* FDB_FDWEB_H */
