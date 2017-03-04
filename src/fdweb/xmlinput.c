/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/eval.h"
#include "framerd/ports.h"
#include "framerd/fdweb.h"

#include <libu8/u8xfiles.h>

#include <ctype.h>

/* TODO:
    Make XML parsing record the markup string as the %raw slot.
    Record %rawname when parsing.
    Check that PIs get their whole body recorded.
    Handle commments and CDATA. */

fd_exception fd_XMLParseError=_("XML parsing error");

#define hasprefix(px,str) ((strncmp(px,str,strlen(px))==0))

static u8_string xmlsnip(u8_string s)
{
  struct U8_OUTPUT out;
  U8_INIT_OUTPUT(&out,80);
  int c=u8_sgetc(&s);
  while ((c>0)&&((out.u8_outlim-out.u8_write)<8)) {
    u8_putc(&out,c);}
  return out.u8_write;
}

static u8_string deentify(u8_string arg,u8_string lim)
{
  U8_OUTPUT out; const u8_byte *scan=arg; int c=u8_sgetc(&scan);
  U8_INIT_OUTPUT(&out,strlen(arg));
  while ((c>0)&&((lim==NULL)||(scan<=lim)))
    if (c=='&') {
      const u8_byte *end=NULL;
      int code=u8_parse_entity(scan,&end);
      if (code<=0) {
        u8_putc(&out,c); c=u8_sgetc(&scan);}
      else {
        u8_putc(&out,code); scan=end;
        c=u8_sgetc(&scan);}}
    else {u8_putc(&out,c); c=u8_sgetc(&scan);}
  return out.u8_outbuf;
}

FD_EXPORT u8_string fd_deentify(u8_string arg,u8_string lim)
{
  return deentify(arg,lim);
}

FD_EXPORT fdtype fd_convert_entities(u8_string arg,u8_string lim)
{
  U8_OUTPUT out;
  const u8_byte *scan=arg;
  int c=u8_sgetc(&scan);
  U8_INIT_OUTPUT(&out,strlen(arg));
  while ((c>0)&&((lim==NULL)||(scan<=lim)))
    if (c=='&') {
      const u8_byte *end=NULL;
      int code=u8_parse_entity(scan,&end);
      if (code<=0) {
        u8_putc(&out,c); c=u8_sgetc(&scan);}
      else {
        u8_putc(&out,code); scan=end;
        c=u8_sgetc(&scan);}}
    else {u8_putc(&out,c); c=u8_sgetc(&scan);}
  return fd_stream2string(&out);
}

static fdtype rawtag_symbol;
static fdtype namespace_symbol, xmltag_symbol, qname_symbol, xmlns_symbol;
static fdtype attribs_symbol, content_symbol, type_symbol;

static fdtype sloppy_symbol, keepraw_symbol, crushspace_symbol;
static fdtype slotify_symbol, nocontents_symbol, nsfree_symbol;
static fdtype autoclose_symbol;
static fdtype noempty_symbol, data_symbol, decode_symbol, ishtml_symbol;
static fdtype comment_symbol, cdata_symbol;

static fdtype nsref2slotid(u8_string s)
{
  u8_string colon=strchr(s,':');
  u8_string start=(((colon) && (colon[1]!='\0')) ? (colon+1) : (s));
  fdtype v=fd_parse(start);
  if (FD_ABORTP(v)) {
    fd_decref(v);
    return fd_intern(start);}
  else return v;
}

struct CLOSERULES {u8_string outer, inner;};
static struct CLOSERULES html_autoclose[]=
  {{"P","P"},{"LI","LI"},
   {"TD","TD"},{"TH","TH"},{"TH","TD"},{"TD","TH"},
   {"TD","TR"},{"TH","TR"},
   {"DT","DT"},{"DD","DT"},{"DT","DD"},
   {NULL,NULL}};

static int check_pair(u8_string outer,u8_string inner,struct CLOSERULES *rules)
{
  while (rules->inner)
    if ((strcasecmp(inner,rules->inner)==0)&&
        (strcasecmp(outer,rules->outer)==0))
      return 1;
    else rules++;
  return 0;
}

/* Parsing entities */

static int egetc(u8_string *s)
{
  if (**s=='\0') return -1;
  else if (**s<0x80)
    if (**s=='&')
      if (strncmp(*s,"&nbsp;",6)==0) {*s=*s+6; return ' ';}
      else {
        const u8_byte *end=NULL;
        int code=u8_parse_entity((*s)+1,&end);
        if (code>0) {
          *s=end; return code;}
        else {(*s)++; return '&';}}
    else {
      int c=**s; (*s)++; return c;}
  else return u8_sgetc(s);
}

static fdtype decode_entities(fdtype input)
{
  struct U8_OUTPUT out; u8_string scan=FD_STRDATA(input); int c=egetc(&scan);
  U8_INIT_OUTPUT(&out,FD_STRLEN(input));
  while (c>=0) {
    u8_putc(&out,c); c=egetc(&scan);}
  return fd_stream2string(&out);
}

static u8_string block_elts[]=
  {"p","ul","ol","dl","h1","h2","h3","h4","h5","h6",
   "pre","table","div","blockquote",
   "noframes","noscript","menu","isindex",
   NULL};

static int block_elementp(u8_string name)
{
  u8_string *scan=block_elts;
  while (*scan)
    if (strcasecmp(name,*scan)==0) return 1;
    else scan++;
  return 0;
}


/* Reading from a stream while keeping a growing buffer around. */

static u8_string readbuf
  (U8_INPUT *in,u8_byte **bufp,size_t *bufsizp,size_t *sizep,char *eos)
{
  u8_byte *buf=*bufp; size_t bufsiz=*bufsizp; int sz=0;
  u8_string data=u8_gets_x(buf,bufsiz,in,eos,&sz);
  if (sizep) *sizep=sz;
  if ((data==NULL)&&(sz==0)) return NULL;
  else if ((data==NULL)&&(sz<0)) return NULL;
  else if (data==NULL) {
    int new_size=bufsiz, need_size=sz+1;
    u8_string result; u8_byte *newbuf;
    while (new_size<need_size)
      if (new_size<=16384) new_size=new_size*2;
      else new_size=new_size+16384;
    u8_free(buf);
    *bufp=newbuf=u8_malloc(new_size);
    *bufsizp=new_size;
    result=u8_gets_x(newbuf,new_size,in,eos,&sz);
    *sizep=sz;
    return result;}
  else return data;
}

static u8_string read_xmltag(u8_input in,u8_byte **buf,
                             size_t *bufsizep,size_t *sizep)
{
  struct U8_OUTPUT out;
  U8_INIT_OUTPUT_BUF(&out,*bufsizep,*buf);
  int c=u8_probec(in);
  if (c=='?') return readbuf(in,buf,bufsizep,sizep,">");
  else if (c=='!') return readbuf(in,buf,bufsizep,sizep,">");
  else c=u8_getc(in);
  while (c>=0) {
    if (c=='>') break;
    else u8_putc(&out,c);
    if (c=='\'') {
      while ((c=u8_getc(in))) {
        if ((c<0)||(c=='\'')) break;
        else u8_putc(&out,c);}
      if (c<0) return NULL;
      else u8_putc(&out,c);
      c=u8_getc(in);}
    else if (c=='"') {
      while ((c=u8_getc(in))) {
          if ((c<0)||(c=='"')) break;
          else u8_putc(&out,c);}
        if (c<0) return NULL;
        else u8_putc(&out,c);
        c=u8_getc(in);}
    else c=u8_getc(in);}
  *buf=out.u8_outbuf;
  *bufsizep=out.u8_outlim-out.u8_outbuf;
  *sizep=out.u8_write-out.u8_outbuf;
  return out.u8_outbuf;
}

FD_EXPORT
void *fd_walk_markup(U8_INPUT *in,
                     void *(*contentfn)(void *,u8_string),
                     void *(*markupfn)(void *,u8_string),
                     void *data)
{
  u8_byte *buf=u8_malloc(1024); size_t bufsiz=1024, size=bufsiz;
  while (1) {
    if (readbuf(in,&buf,&bufsiz,&size,"<")==NULL) {
      data=contentfn(data,in->u8_read); break;}
    else
      data=contentfn(data,buf);
    if (data==NULL) break;
    if (read_xmltag(in,&buf,&bufsiz,&size)==NULL) {
      data=markupfn(data,in->u8_read); break;}
    data=markupfn(data,buf);
    if (data==NULL) break;}
  u8_free(buf);
  return data;
}

/* XML handling */

static void set_elt_name(FD_XML *,u8_string);

static void init_node(FD_XML *node,FD_XML *parent,u8_string name)
{
  node->fdxml_eltname=name;
  node->fdxml_head=FD_EMPTY_LIST; 
  node->fdxml_content_tail=NULL;
  node->fdxml_attribs=FD_EMPTY_CHOICE;
  node->fdxml_parent=parent;
  if (parent) node->fdxml_bits=((parent->fdxml_bits)&(FD_XML_INHERIT_BITS));
  else node->fdxml_bits=FD_XML_DEFAULT_BITS;
  node->fdxml_namespace=NULL; node->fdxml_nsmap=NULL;
  node->fdxml_size=0; node->fdxml_limit=0; node->fdxml_data=NULL;
  if (((node->fdxml_bits)&(FD_XML_ISHTML)) && (strcasecmp(name,"P")==0))
    node->fdxml_bits=node->fdxml_bits|FD_XML_INPARA;
}

static void init_node_attribs(struct FD_XML *node)
{
  if (FD_EMPTY_CHOICEP(node->fdxml_attribs))
    node->fdxml_attribs=fd_empty_slotmap();
  set_elt_name(node,node->fdxml_eltname);
}

FD_EXPORT
void fd_init_xml_node(FD_XML *node,FD_XML *parent,u8_string name)
{
  init_node(node,parent,name);
}

FD_EXPORT void fd_init_xml_attribs(struct FD_XML *node)
{
  if (FD_EMPTY_CHOICEP(node->fdxml_attribs))
    node->fdxml_attribs=fd_empty_slotmap();
  set_elt_name(node,node->fdxml_eltname);
}

static void free_nsinfo(FD_XML *node)
{
  if (node->fdxml_nsmap) {
    int i=0, lim=node->fdxml_size;
    while (i<lim) u8_free(node->fdxml_nsmap[i++]);
    u8_free(node->fdxml_nsmap);
    node->fdxml_nsmap=NULL; node->fdxml_limit=node->fdxml_size=0;}
  if (node->fdxml_namespace) u8_free(node->fdxml_namespace);
}

static void free_node(FD_XML *node,int full)
{
  free_nsinfo(node);
  fd_decref(node->fdxml_attribs);
  fd_decref(node->fdxml_head);
  if (node->fdxml_eltname) u8_free(node->fdxml_eltname);
  if (full) u8_free(node);
}

static fdtype make_qid(u8_string eltname,u8_string namespace)
{
  if (namespace) {
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,32);
    u8_printf(&out,"{%s}%s",namespace,eltname);
    return fd_stream2string(&out);}
  else return fdtype_string(eltname);
}

FD_EXPORT fdtype fd_make_qid(u8_string eltname,u8_string namespace)
{
  return make_qid(eltname,namespace);
}

FD_EXPORT void fd_free_xml_node(FD_XML *node)
{
  free_nsinfo(node);
  fd_decref(node->fdxml_attribs);
  fd_decref(node->fdxml_head);
  if (node->fdxml_eltname) u8_free(node->fdxml_eltname);
}

static void ns_add(FD_XML *xml,u8_string prefix,u8_string url)
{
  int prefix_len=strlen(prefix), url_len=strlen(url);
  u8_byte *entry=u8_malloc(prefix_len+url_len+2);
  strncpy(entry,prefix,prefix_len);
  entry[prefix_len]=':';
  strcpy(entry+prefix_len+1,url);
  if (xml->fdxml_size>=xml->fdxml_limit) {
    int new_limit=((xml->fdxml_limit) ? (2*xml->fdxml_limit) : (4));
    xml->fdxml_nsmap=u8_realloc_n(xml->fdxml_nsmap,new_limit,u8_string);
    xml->fdxml_limit=new_limit;}
  xml->fdxml_nsmap[xml->fdxml_size++]=entry;
}

static u8_string ns_get(FD_XML *xml,u8_string s,u8_string *nsp)
{
  u8_byte *colon=strchr(s,':');
  if (colon==NULL) {
    FD_XML *scan=xml; while (scan)
      if (scan->fdxml_namespace) {
        *nsp=scan->fdxml_namespace; return s;}
      else scan=scan->fdxml_parent;
    *nsp=NULL;
    return s;}
  else {
    int prefix_len=(colon-s)+1;
    FD_XML *scan=xml; while (scan)
      if (scan->fdxml_nsmap) {
        int i=0, len=scan->fdxml_size;
        u8_string *nsmap=scan->fdxml_nsmap;
        while (i<len)
          if (strncmp(s,nsmap[i],prefix_len)==0) {
            *nsp=nsmap[i]+prefix_len;
            return colon+1;}
          else i++;
        scan=scan->fdxml_parent;}
      else scan=scan->fdxml_parent;
    *nsp=NULL;
    return s;}
}

FD_EXPORT
u8_string fd_xmlns_lookup(FD_XML *xml,u8_string s,u8_string *nsp)
{
  return ns_get(xml,s,nsp);
}

static int process_nsattrib(FD_XML *xml,u8_string name,u8_string val)
{
  u8_byte *nsprefix;
  if (FD_EMPTY_CHOICEP(xml->fdxml_attribs)) init_node_attribs(xml);
  if (hasprefix("xmlns",name))
    if (strlen(name)==5) {
      fdtype nsval=fdtype_string(val);
      xml->fdxml_namespace=u8_strdup(val);
      fd_add(xml->fdxml_attribs,xmlns_symbol,nsval);
      fd_decref(nsval);
      return 1;}
    else if ((nsprefix=(strchr(name,':')))) {
      fdtype entry=
        fd_conspair(fdtype_string(nsprefix+1),fdtype_string(val));
      ns_add(xml,nsprefix+1,val);
      fd_add(xml->fdxml_attribs,xmlns_symbol,entry);
      fd_decref(entry);
      return 1;}
    else return 0;
  else return 0;
}

static int allspacep(u8_string s)
{
  const u8_byte *scan=s; int c=u8_sgetc(&scan);
  while ((c>0) && (u8_isspace(c))) c=u8_sgetc(&scan);
  if (c<0) return 1; else return 0;
}

static fdtype item2list(fdtype item,int parse_entities)
{
  if ((FD_STRINGP(item)) && (parse_entities)) {
    fdtype result=fd_make_list(1,decode_entities(item));
    fd_decref(item);
    return result;}
  else return fd_make_list(1,item);
}

static void add_content(struct FD_XML *node,fdtype item)
{
  if ((FD_STRINGP(item)) &&
      (((FD_STRING_LENGTH(item))==0) ||
       ((node->fdxml_bits&FD_XML_CRUSHSPACE)&&
        (allspacep(FD_STRDATA(item)))))) {
    fd_decref(item);
    return;}
  else {
    fdtype entry;
    if (FD_STRINGP(item)) {
      int parse_entities=
        (((node->fdxml_bits)&(FD_XML_DECODE_ENTITIES)) &&
         (strchr(FD_STRDATA(item),'&')!=NULL));
      entry=item2list(item,parse_entities);}
    else entry=fd_make_list(1,item);
    if (node->fdxml_content_tail==NULL) {
      node->fdxml_head=entry;
      node->fdxml_content_tail=(struct FD_PAIR *)entry;}
    else {
      node->fdxml_content_tail->fd_cdr=entry;
      node->fdxml_content_tail=(struct FD_PAIR *)entry;}}
}
FD_EXPORT void fd_add_content(struct FD_XML *node,fdtype item)
{
  add_content(node,item);
}

static void set_elt_name(FD_XML *xml,u8_string name)
{
  u8_string ns=NULL, eltname=ns_get(xml,name,&ns);
  fdtype lispname=nsref2slotid(eltname);
  fdtype qlispname=nsref2slotid(name);
  if ((ns) && ((xml->fdxml_bits&FD_XML_NSFREE)==0)) {
    fdtype namespace=fdtype_string(ns);
    fdtype qname=make_qid(eltname,ns);
    fd_store(xml->fdxml_attribs,rawtag_symbol,fd_intern(name));
    fd_add(xml->fdxml_attribs,namespace_symbol,namespace);
    fd_add(xml->fdxml_attribs,qname_symbol,qname);
    fd_decref(qname); fd_decref(namespace);}
  fd_add(xml->fdxml_attribs,xmltag_symbol,lispname);
  fd_add(xml->fdxml_attribs,qname_symbol,qlispname);
  fd_decref(qlispname);
  fd_decref(lispname);
}

/* Parsing XML tags */

FD_EXPORT
int fd_parse_xmltag(u8_byte **scanner,u8_byte *end,
                     const u8_byte **attribs,int max_attribs,
                     int sloppy)
{
  int n_attribs=0;
  u8_byte *scan=*scanner, *elt_start=scan;
  /* Accumulate the tag attribs (including the tag name) */
  if ((*scan=='/') || (*scan=='?') || (*scan=='!')) {
    /* Skip post < character */
    scan++; elt_start=scan;}
  while (scan<end) 
    /* Scan to set scan at the end */
    if (isspace(*scan)) {
      u8_byte *item_end=scan;
      /* Skip (and ignore) whitespace; spaces outside of quotes
         or not after or before = are always 'attribute' breaks */
      while ((scan<end) && (isspace(*scan))) scan++;
      /* This = is part of an attribute */
      if (*scan=='=') {
        scan++; /* Skip the whitespace after the = */
        while ((scan<end) && (isspace(*scan))) scan++;
        /* We're still in the attribute */
        continue;}
      else {
        /* The space really ends the attribute, so NUL terminate it and
           add it to *attribs */
        *item_end='\0'; attribs[n_attribs++]=elt_start;
        if (n_attribs>=max_attribs) {
          *scanner=scan; return n_attribs;}
        elt_start=scan;}}
    else if (*scan=='\'') {
      /* Scan a single quoted value, waiting for an unprotected
         single quote */
      u8_byte *aend=strchr(scan+1,'\'');
      if ((aend)&&(aend<end)) scan=aend+1; /* got one */
      else if (sloppy) {
        /* Not closed, but sloppy is okay, so we just go up to the
           first space after the opening quote.  We could be more
           clever (looking for xxx=, for exampe) but let's not.  */
        const u8_byte *lookahead=scan;
        int c=u8_sgetc(&lookahead);
        while ((c>0)&&(!(u8_isspace(c)))&&(lookahead<end)) {
          scan=(u8_byte *)lookahead;
          c=u8_sgetc(&lookahead);}}
      else {
        fd_seterr3(fd_XMLParseError,"unclosed quote in attribute",
                   xmlsnip(*scanner));
        return -1;}}
    else if (*scan=='"') {
      /* This is the exact same logic as above, but scanning for
         a double quote rather than a single quote. */
      u8_byte *aend=strchr(scan+1,'"');
      /* Scan, ignoring escaped single quotes */
      if ((aend)&&(aend<end)) scan=aend+1; /* got one */
      else if (sloppy) {
        /* Not closed, but sloppy is okay, so we just go up to the
           first space after the opening quote.  We could be more
           clever (looking for xxx=, for exampe) but let's not.  */
        const u8_byte *lookahead=scan;
        int c=u8_sgetc(&lookahead);
        if (lookahead==end) scan=(u8_byte *)lookahead;
        else while ((c>0)&&(!(u8_isspace(c)))&&(lookahead<end)) {
            scan=(u8_byte *)lookahead;
            c=u8_sgetc(&lookahead);}}
      else {
        fd_seterr3(fd_XMLParseError,"unclosed quote in attribute",
                   xmlsnip(*scanner));
        return -1;}}
    else if (*scan=='=') {
      /* Skip whitespace after an = sign */
      const u8_byte *start=++scan, *next=start;
      int c=u8_sgetc(&next);
      while ((c>0)&&(u8_isspace(c))&&(scan<end)) {
        scan=(u8_byte *)next;
        c=u8_sgetc(&next);}
      /* If you ran over (no value for =), just take the
         string up to the = */
      if (scan>=end) scan=(u8_byte *)start;}
    else if (*scan=='\0') scan++;
    else u8_sgetc((u8_string *)&scan);
  if (scan>elt_start) {
    *scan='\0'; attribs[n_attribs++]=elt_start;}
  return n_attribs;
}
/* Old name, keeping around until the next ABI update */
FD_EXPORT
int fd_parse_element(u8_byte **scanner,u8_byte *end,
                     const u8_byte **elts,int max_elts,
                     int sloppy)
{
  return fd_parse_xmltag(scanner,end,elts,max_elts,sloppy);
}


static int _tag_matchp(u8_string buf,u8_string tagname,int len)
{
  if (strncasecmp(buf,tagname,len)==0)
    if ((buf[len]=='\0') || (isspace(buf[len])) || (ispunct(buf[len])))
      return 1;
    else return 0;
  else return 0;
}

#define tagmatchp(buf,name) (_tag_matchp((buf),(name),(strlen(name))))

FD_EXPORT
fd_xmlelt_type fd_get_markup_type(u8_string buf,int len,int html)
{
  fd_xmlelt_type elt_type;
  const u8_byte *start=buf, *end=buf+(len-1);
  while ((start<end) && (isspace(*start))) start++;
  while ((end>start) && (isspace(*end))) end--;
  if (*start=='/') elt_type=xmlclose;
  else if (*start=='?') elt_type=xmlpi;
  else if (*start=='!')
    if (strncmp(start,"!--",3)==0) elt_type=xmlcomment;
    else if (strncmp(start,"![CDATA[",8)==0) elt_type=xmlcdata;
    else elt_type=xmldoctype;
  else if (buf[len-1]=='/') elt_type=xmlempty;
  else if ((html) &&
           ((tagmatchp(buf,"img")) || (tagmatchp(buf,"base")) ||
            (tagmatchp(buf,"br")) || (tagmatchp(buf,"hr")) ||
            (tagmatchp(buf,"meta")) || (tagmatchp(buf,"link")) ||
            (tagmatchp(buf,"input")) || (tagmatchp(buf,"textarea")))) {
    elt_type=xmlempty; }
  else if (!(isalpha(*start))) return -1;
  else elt_type=xmlopen;
  if ((elt_type==xmlpi) && (buf[len-1]!='?'))
    return -1;
  else return elt_type;
}

/* Attribute processing */

static void process_attribs(int (*attribfn)(FD_XML *,u8_string,u8_string,int),
                            FD_XML *xml,u8_string *attribs,int n)
{
  int i=0; while (i< n) {
    u8_byte *item=(u8_byte *)attribs[i++], *end=item+strlen(item)-1;
    u8_byte *equals=strchr(item,'='), *name_end=equals; 
    u8_string name=item, val; int quote=-1;
    if (equals) {
      const u8_byte *scan=item; int c=u8_sgetc(&scan);
      /* Find the end of the name, ignoring any whitespace before
         the equals sign. */
      while ((scan<=equals)&&(c>=0)) {
        /* When we exit the loop, we've got the end of the attrib name */
        if (!((c==' ')||(c=='\n')||(c=='\r')||(u8_isspace(c)))) 
          name_end=(u8_byte *)scan;
        c=u8_sgetc(&scan);}
      c=u8_sgetc(&scan); /* c=='=' */
      /* Now get the value, skipping whitespace and unwrapping quotes */
      if (u8_isspace(c)) {
        while ((c==' ')||(c=='\n')||(c=='\r')||(u8_isspace(c))) {
          val=scan; c=u8_sgetc(&scan);}}
      else val=equals+1;
      /* We don't have to worry about where the item ends
         (that was handled by fd_parse_xmltag) but we do need to
         strip off any quotes. */
      if ((c=='"')||(c=='\'')) {quote=c; val++;}
      *name_end='\0';
      if (*end==quote) *end='\0';
      if (process_nsattrib(xml,name,val)) {}
      else if ((attribfn) && (attribfn(xml,name,val,quote))) {}
      else fd_default_attribfn(xml,name,val,quote);}
    else if ((attribfn) && (attribfn(xml,item,NULL,-1))) {}
    else fd_default_attribfn(xml,item,NULL,-1);}
}

/* Defaults */

FD_EXPORT
void fd_default_contentfn(FD_XML *node,u8_string s,int len)
{
  if (strncmp(s,"<!--",4)==0) {
    fdtype cnode=fd_empty_slotmap();
    fdtype comment_string=fd_substring(s+4,s+(len-3));
    fdtype comment_content=fd_conspair(comment_string,FD_EMPTY_LIST);
    fd_store(cnode,xmltag_symbol,comment_symbol);
    fd_store(cnode,content_symbol,comment_content);
    fd_decref(comment_content);
    add_content(node,cnode);}
  else if (strncmp(s,"<![CDATA[",9)==0) {
    fdtype cnode=fd_empty_slotmap();
    fdtype cdata_string=fd_substring(s+9,s+(len-3));
    fdtype cdata_content=fd_conspair(cdata_string,FD_EMPTY_LIST);
    fd_store(cnode,xmltag_symbol,cdata_symbol);
    fd_store(cnode,content_symbol,cdata_content);
    fd_decref(cdata_content);
    add_content(node,cnode);}
  else add_content(node,fd_make_string(NULL,len,s));
}

FD_EXPORT
void fd_default_pifn(FD_XML *node,u8_string s,int len)
{
  u8_byte *buf=u8_malloc(len+3);
  strcpy(buf,"<"); strcat(buf,s); strcat(buf+len,">");
  add_content(node,fd_init_string(NULL,len+2,buf));
}

static int slotify_nodep(FD_XML *node)
{
  if ((node->fdxml_bits&FD_XML_NOEMPTY)==0) return 1;
  else if (FD_PAIRP(node->fdxml_head)) return 1;
  else if (FD_EMPTY_CHOICEP(node->fdxml_attribs)) return 0;
  else if (!(FD_SLOTMAPP(node->fdxml_attribs)))
    /* Not sure this case ever happens */
    return 1;
  else if (FD_SLOTMAP_NUSED(node->fdxml_attribs)>1) return 1;
  else return 0;
}

static void cleanup_attribs(fdtype table);

FD_EXPORT
FD_XML *fd_default_popfn(FD_XML *node)
{
  if (FD_EMPTY_CHOICEP(node->fdxml_attribs)) init_node_attribs(node);
  if (FD_PAIRP(node->fdxml_head))
    fd_add(node->fdxml_attribs,content_symbol,node->fdxml_head);
  cleanup_attribs(node->fdxml_attribs);
  if (((node->fdxml_bits&FD_XML_NOCONTENTS)==0) ||
      (node->fdxml_parent==NULL) ||
      (node->fdxml_parent->fdxml_parent==NULL)) {
    if (node->fdxml_parent!=NULL) {
      add_content(node->fdxml_parent,node->fdxml_attribs);
      node->fdxml_attribs=FD_EMPTY_CHOICE;}}
  if ((node->fdxml_bits&FD_XML_SLOTIFY) && (slotify_nodep(node))) {
    fdtype slotid;
    if (FD_EMPTY_CHOICEP(node->fdxml_parent->fdxml_attribs))
      init_node_attribs(node->fdxml_parent);
    slotid=fd_get(node->fdxml_attribs,xmltag_symbol,FD_EMPTY_CHOICE);
    if ((node->fdxml_bits)&FD_XML_KEEP_RAW)
      add_content(node->fdxml_parent,fd_incref(node->fdxml_attribs));
    node->fdxml_parent->fdxml_bits=node->fdxml_parent->fdxml_bits|FD_XML_HASDATA;
    if ((FD_PAIRP(node->fdxml_head)) &&
        (!((node->fdxml_bits)&FD_XML_HASDATA)) &&
        (!(FD_PAIRP(FD_CDR(node->fdxml_head)))))
      fd_add(node->fdxml_parent->fdxml_attribs,slotid,FD_CAR(node->fdxml_head));
    else fd_add(node->fdxml_parent->fdxml_attribs,slotid,node->fdxml_attribs);}
  return node->fdxml_parent;
}

static void cleanup_attribs(fdtype table)
{
  if (FD_SLOTMAPP(table)) {
    struct FD_SLOTMAP *sm=(struct FD_SLOTMAP *)table;
    struct FD_KEYVAL *scan, *limit; int unlock=0, size;
    if (sm->table_uselock) { u8_read_lock(&sm->table_rwlock); unlock=1;}
    size=FD_XSLOTMAP_NUSED(sm); scan=sm->sm_keyvals; limit=scan+size;
    if (size==0) {
      if (unlock) u8_rw_unlock(&sm->table_rwlock);
      return;}
    while (scan < limit) {
      fdtype val=scan->kv_val;
      if (FD_ACHOICEP(val))
        scan->kv_val=fd_simplify_choice(val);
      scan++;}
    if (unlock) u8_rw_unlock(&sm->table_rwlock);}
}

FD_EXPORT
FD_XML *fd_xml_push
  (FD_XML *newnode,FD_XML *node,fd_xmlelt_type type,
   int (*attribfn)(FD_XML *,u8_string,u8_string,int),
   u8_string *elts,int n_elts)
{
  u8_string name=u8_strdup(elts[0]);
  init_node(newnode,node,name);
  process_attribs(attribfn,newnode,elts+1,n_elts-1);
  init_node_attribs(newnode);
  return newnode;
}

static fdtype fd_lispify(u8_string arg)
{
  if (*arg==':') return fd_parse(arg+1);
  else return fdtype_string(arg);
}

static fdtype parse_attribname(u8_string string)
{
  fdtype parsed=fd_parse(string);
  if ((FD_SYMBOLP(parsed))||(FD_OIDP(parsed))) return parsed;
  else {
    u8_log(LOG_WARNING,"BadAttribName",
           "Trouble parsing attribute name %s",string);
    return fdtype_string(string);}
}

static fdtype attribids;

FD_EXPORT
int fd_default_attribfn(FD_XML *xml,u8_string name,u8_string val,int quote)
{
  u8_string namespace, attrib_name=ns_get(xml,name,&namespace);
  fdtype slotid=parse_attribname(name);
  fdtype slotval=((val)?
                  (((val[0]=='\0')||(val[0]=='#')) ?
                   (fdtype_string(val)) :
                   (quote<0) ? (fd_lispify(val)) :
                   (fd_convert_entities(val,NULL))) :
                  (FD_FALSE));
  fdtype attrib_entry=FD_VOID;
  if (FD_EMPTY_CHOICEP(xml->fdxml_attribs)) init_node_attribs(xml);
  xml->fdxml_bits=xml->fdxml_bits|FD_XML_HASDATA;
  fd_add(xml->fdxml_attribs,slotid,slotval);
  if (namespace) {
    fd_add(xml->fdxml_attribs,parse_attribname(attrib_name),slotval);
    attrib_entry=
      fd_make_nvector(3,fdtype_string(name),make_qid(attrib_name,namespace),
                      slotval);}
  else attrib_entry=
         fd_make_nvector(3,fdtype_string(name),FD_FALSE,slotval);
  fd_add(xml->fdxml_attribs,attribids,slotid);
  fd_add(xml->fdxml_attribs,attribs_symbol,attrib_entry);
  fd_decref(attrib_entry);
  return 1;
}

/* This attribute function always stores strings and never parses the args. */

static FD_XML *autoclose(FD_XML *node,u8_string name,
                         FD_XML *(*popfn)(FD_XML *))
{
  FD_XML *scan=node->fdxml_parent; while (scan)
    if (strcmp(scan->fdxml_eltname,name)==0) {
      FD_XML *freescan=node, *retval, *next;
      while (freescan) {
        if (freescan==scan) break;
        if ((retval=popfn(freescan))!=freescan->fdxml_parent)
          return retval;
        next=freescan->fdxml_parent;
        free_node(freescan,1);
        freescan=next;}
      return scan;}
    else scan=scan->fdxml_parent;
  return scan;
}

/* Processing a single markup element */

static
/* This is the inner step of the XML parsing loop. */
FD_XML *xmlstep(FD_XML *node,fd_xmlelt_type type,
                u8_string *elts,int n_elts,
                int (*attribfn)(FD_XML *,u8_string,u8_string,int),
                FD_XML *(*pushfn)(FD_XML *,fd_xmlelt_type,u8_string *,int),
                FD_XML *(*popfn)(FD_XML *))
{
  switch (type) {
  case xmlempty:
    if (pushfn) {
      struct FD_XML *newnode=pushfn(node,type,elts,n_elts);
      if (FD_EMPTY_CHOICEP(node->fdxml_attribs)) init_node_attribs(node);
      if (newnode != node) {
        struct FD_XML *retnode;
        retnode=popfn(newnode);
        if (retnode!=newnode)
          free_node(newnode,1);
        return retnode;}
      else {}
      return node;}
    else {
      struct FD_XML newnode, *retnode;
      u8_string name=u8_strdup(elts[0]);
      init_node(&newnode,node,name);
      process_attribs(attribfn,&newnode,elts+1,n_elts-1);
      init_node_attribs(&newnode);
      retnode=popfn(&newnode);
      free_node(&newnode,0);
      return retnode;}
  case xmlclose:
    if ((((node->fdxml_bits)&(FD_XML_FOLDCASE)) ?
         (strcasecmp(node->fdxml_eltname,elts[0])) :
         (strcmp(node->fdxml_eltname,elts[0])))
        ==0) {
      struct FD_XML *retnode;
      if (FD_EMPTY_CHOICEP(node->fdxml_attribs)) init_node_attribs(node);
      if ((FD_EMPTY_LISTP(node->fdxml_head)) &&
          (!(node->fdxml_bits&FD_XML_NOEMPTY)))
        node->fdxml_head=fd_conspair(fd_init_string(NULL,0,NULL),FD_EMPTY_LIST);
      retnode=popfn(node);
      if (retnode!=node)
        free_node(node,1);
      return retnode;}
    else if ((node->fdxml_bits&FD_XML_EMPTY_CLOSE) && (elts[0][0]=='\0')) {
      struct FD_XML *retnode;
      if (FD_EMPTY_CHOICEP(node->fdxml_attribs)) init_node_attribs(node);
      retnode=popfn(node);
      if (retnode!=node)
        free_node(node,1);
      return retnode;}
    else {
      if (node->fdxml_bits&FD_XML_AUTOCLOSE) {
        FD_XML *closenode=NULL;
        if (FD_EMPTY_CHOICEP(node->fdxml_attribs)) init_node_attribs(node);
        closenode=autoclose(node,elts[0],popfn);
        if (closenode) {
          struct FD_XML *retnode;
          retnode=popfn(closenode);
          if (retnode!=closenode)
            free_node(closenode,1);
          return retnode;}}
      if ((node->fdxml_bits)&(FD_XML_BADCLOSE))
        return node;
      else fd_seterr(fd_XMLParseError,"inconsistent close tag",
                     u8_mkstring("</%s> closes <%s>",elts[0],node->fdxml_eltname),
                     FD_VOID);
      return NULL;}
  case xmlopen:
    if (pushfn) return pushfn(node,type,elts,n_elts);
    else {
      FD_XML *newnode=u8_alloc(struct FD_XML);
      u8_string name=u8_strdup(elts[0]);
      if ((node->fdxml_bits&FD_XML_ISHTML) &&
          (check_pair(node->fdxml_eltname,elts[0],html_autoclose))) {
        FD_XML *popped=popfn(node);
        if (popped!=node)
          free_node(node,1);
        node=popped;}
      else if (((node->fdxml_bits)&(FD_XML_INPARA))&&
               (block_elementp(elts[0]))) {
        while ((node) && ((node->fdxml_bits)&(FD_XML_INPARA))) {
          FD_XML *popped=popfn(node);
          if (popped!=node) free_node(node,1);
          node=popped;}}
      init_node(newnode,node,name);
      process_attribs(attribfn,newnode,elts+1,n_elts-1);
      init_node_attribs(newnode);
      return newnode;}
  default:
    return node;}
}

FD_EXPORT
void *fd_walk_xml(U8_INPUT *in,
                  void (*contentfn)(FD_XML *,u8_string,int),
                  FD_XML *(*pifn)(U8_INPUT *,FD_XML *,u8_string,int),
                  int (*attribfn)(FD_XML *,u8_string,u8_string,int),
                  FD_XML *(*pushfn)(FD_XML *,fd_xmlelt_type,u8_string *,int),
                  FD_XML *(*popfn)(FD_XML *),
                  FD_XML *node)
{
  ssize_t bufsize=1024, size=bufsize;
  u8_byte *buf=u8_malloc(1024); const u8_byte *rbuf;
  while (1) {
    fd_xmlelt_type type;
    if (FD_INTERRUPTED()) {
      u8_free(buf);
      return NULL;}
    else if ((rbuf=readbuf(in,&buf,&bufsize,&size,"<"))==NULL) {
      if (contentfn)
        contentfn(node,in->u8_read,in->u8_inlim-in->u8_read);
      break;}
    else if (size<0) {
      fd_seterr3(fd_XMLParseError,"end of input",xmlsnip(in->u8_read));
      u8_free(buf);
      return NULL;}
    else if ((size>0)&&(contentfn)) contentfn(node,buf,size);
    else {}
    if ((rbuf=read_xmltag(in,&buf,&bufsize,&size))==NULL) {
      fd_seterr3(fd_XMLParseError,"end of input",xmlsnip(in->u8_read));
      u8_free(buf);
      return NULL;}
    else type=fd_get_markup_type(buf,size,((node->fdxml_bits)&FD_XML_ISHTML));
    if (type == xmlpi) {
      FD_XML *result;
      if (pifn) {
        if ((result=pifn(in,node,buf,size))==NULL)
          return NULL;}
      else result=NULL;
      if (result) node=result;
      else if (contentfn) {
        u8_string reconstituted=
          u8_string_append("<",buf,">",NULL);
        contentfn(node,reconstituted,size+2);
        u8_free(reconstituted);}}
    else if (type == xmlcomment) {
      const u8_byte *remainder=NULL; u8_byte *combined;
      int combined_len, more_data=0;
      if (strcmp((buf+size-2),"--"))
        /* If the markup end isn't --, we still need to find the
           content end (plus more content) */
        remainder=u8_gets_x(NULL,0,in,"-->",&more_data);
      if (more_data)
        combined_len=size+more_data+4;
      else combined_len=size+2;
      combined=u8_malloc(combined_len+1);
      memcpy(combined,"<",1);
      memcpy(combined+1,buf,size);
      if (more_data) {
        memcpy(combined+1+size,remainder,more_data);
        memcpy(combined+1+size+more_data,"-->",4);
        u8_free(remainder);}
      else memcpy(combined+1+size,">",2);
      if (contentfn)
        contentfn(node,combined,combined_len);
      u8_free(combined);}
    else if (type == xmlcdata) {
      const u8_byte *remainder=NULL; u8_byte *combined;
      int more_data=0, combined_len;
      if (strcmp((buf+size-2),"]]"))
        remainder=u8_gets_x(NULL,0,in,"]]>",&more_data);
      if (more_data) combined_len=size+more_data+5;
      else combined_len=size+2;
      combined=u8_malloc(combined_len+1);
      memcpy(combined,"<",1);
      memcpy(combined+1,buf,size);
      if (more_data) {
        memcpy(combined+1+size,">",1);
        memcpy(combined+1+size+1,remainder,more_data);
        memcpy(combined+1+size+1+more_data,"]]>",4);
        u8_free(remainder);}
      else memcpy(combined+1+size,">",2);
      if (contentfn) contentfn(node,combined,combined_len);
      u8_free(combined);}
    else if (type == xmldoctype) {
      if (strchr(buf,'<')) {}
      else if (pifn) node=pifn(in,node,buf,size);
      else if (contentfn) {
        u8_string reconstituted=
          u8_string_append("<",buf,">",NULL);
        contentfn(node,reconstituted,size+2);
        u8_free(reconstituted);}}
    else {
      u8_byte *scan=buf;
      const u8_byte *_elts[32], **elts=_elts;
      int n_elts;
      if ((type == xmlempty)&&(buf[size-1]=='/')) {
        buf[size-1]='\0'; size--;}
      n_elts=fd_parse_xmltag
        (&scan,buf+size,elts,32,((node->fdxml_bits)&(FD_XML_BADATTRIB)));
      if (n_elts<0) {
        fd_seterr3(fd_XMLParseError,"xmlstep",xmlsnip(scan));
        u8_free(buf);
        return NULL;}
      node=xmlstep(node,type,elts,n_elts,attribfn,pushfn,popfn);}
    if (node==NULL) break;}
  while ((node)&&((node->fdxml_bits)&(FD_XML_AUTOCLOSE))) {
    struct FD_XML *next=popfn(node);
    if (next) node=next; else break;}
  u8_free(buf);
  return node;
}

/* A standard XML walker */

FD_EXPORT int fd_xmlparseoptions(fdtype x)
{
  if (FD_FIXNUMP(x)) return FD_FIX2INT(x);
  else if (FD_FALSEP(x)) return FD_SLOPPY_XML;
  else if (FD_TRUEP(x)) return 0;
  else if (FD_VOIDP(x)) return 0;
  else if (FD_SYMBOLP(x))
    if (FD_EQ(x,sloppy_symbol))
      return FD_SLOPPY_XML;
    else if (FD_EQ(x,data_symbol))
      return FD_DATA_XML;
    else if (FD_EQ(x,decode_symbol))
      return FD_XML_DECODE_ENTITIES;
    else if (FD_EQ(x,keepraw_symbol))
      return FD_XML_KEEP_RAW;
    else if (FD_EQ(x,crushspace_symbol))
      return FD_XML_CRUSHSPACE;
    else if (FD_EQ(x,slotify_symbol))
      return FD_XML_SLOTIFY;
    else if (FD_EQ(x,nocontents_symbol))
      return FD_XML_NOCONTENTS;
    else if (FD_EQ(x,ishtml_symbol))
      return FD_XML_ISHTML;
    else if (FD_EQ(x,nsfree_symbol))
      return FD_XML_NSFREE;
    else if (FD_EQ(x,autoclose_symbol))
      return FD_XML_AUTOCLOSE;
    else if (FD_EQ(x,noempty_symbol))
      return FD_XML_NOEMPTY;
    else {
      fd_seterr(fd_TypeError,"xmlparsearg",u8_strdup(_("xmlparse option")),x);
      return -1;}
  else if (FD_CHOICEP(x)) {
    int flags=0;
    FD_DO_CHOICES(opt,x) {
      int flag=fd_xmlparseoptions(opt);
      if (flag<0) return -1;
      else flags=flags|flag;}
    return flags;}
  else if (FD_PAIRP(x)) {
    int flags=0;
    FD_DOLIST(opt,x) {
      int flag=fd_xmlparseoptions(opt);
      if (flag<0) return -1;
      else flags=flags|flag;}
    return flags;}
  else {
    fd_seterr(fd_TypeError,"xmlparsearg",u8_strdup(_("xmlparse option")),x);
    return -1;}
}

static fdtype xmlparse_core(fdtype input,int flags)
{
  struct FD_XML object, *retval;
  struct U8_INPUT *in, _in;
  unsigned int errpos;
  if (flags<0) return FD_ERROR_VALUE;
  if (FD_PORTP(input)) {
    struct FD_PORT *p=fd_consptr(struct FD_PORT *,input,fd_port_type);
    in=p->fd_inport;}
  else if (FD_STRINGP(input)) {
    U8_INIT_STRING_INPUT(&_in,FD_STRLEN(input),FD_STRDATA(input));
    in=&_in;}
  else if (FD_PACKETP(input)) {
    U8_INIT_STRING_INPUT(&_in,FD_PACKET_LENGTH(input),FD_PACKET_DATA(input));
    in=&_in;}
  else return fd_type_error(_("string or port"),"xmlparse",input);
  init_node(&object,NULL,u8_strdup("top")); object.fdxml_bits=flags;
  retval=fd_walk_xml(in,fd_default_contentfn,NULL,NULL,NULL,
                     fd_default_popfn,
                     &object);
  if (retval) {
    fdtype result=fd_incref(object.fdxml_head);
    free_node(&object,0);
    return result;}
  errpos=_in.u8_read-_in.u8_inbuf;
  fd_seterr("XMLPARSE error","xmlparse_core",NULL,FD_INT(errpos));
  return FD_ERROR_VALUE;
}

static fdtype xmlparse(fdtype input,fdtype options)
{
  if (FD_CHOICEP(input)) {
    int flags=fd_xmlparseoptions(options);
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(in,input) {
      fdtype result=xmlparse_core(in,flags);
      FD_ADD_TO_CHOICE(results,result);}
    return results;}
  else if (FD_QCHOICEP(input))
    return xmlparse(FD_XQCHOICE(input)->fd_choiceval,options);
  else return xmlparse_core(input,fd_xmlparseoptions(options));
}

/* Parsing FDXML */

static fdtype fdxml_load(fdtype input,fdtype sloppy)
{
  int flags=FD_XML_KEEP_RAW;
  struct FD_XML *parsed;
  struct U8_INPUT *in, _in;
  if (flags<0) return FD_ERROR_VALUE;
  if (FD_PORTP(input)) {
    struct FD_PORT *p=fd_consptr(struct FD_PORT *,input,fd_port_type);
    in=p->fd_inport;}
  else if (FD_STRINGP(input)) {
    U8_INIT_STRING_INPUT(&_in,FD_STRLEN(input),FD_STRDATA(input));
    in=&_in;}
  else if (FD_PACKETP(input)) {
    U8_INIT_STRING_INPUT(&_in,FD_PACKET_LENGTH(input),FD_PACKET_DATA(input));
    in=&_in;}
  else return fd_type_error(_("string or port"),"xmlparse",input);
  if (FD_FIXNUMP(sloppy))
    flags=FD_FIX2INT(sloppy);
  else if (!((FD_VOIDP(sloppy)) || (FD_FALSEP(sloppy))))
    flags=flags|FD_SLOPPY_XML;
  else {}
  parsed=fd_load_fdxml(in,flags);
  if (parsed) {
    fdtype result=fd_incref(parsed->fdxml_head);
    fdtype lispenv=(fdtype)(parsed->fdxml_data);
    free_node(parsed,1);
    return fd_conspair(lispenv,result);}
  else return FD_ERROR_VALUE;
}

static fdtype fdxml_read(fdtype input,fdtype sloppy)
{
  int flags=FD_XML_KEEP_RAW;
  struct FD_XML *parsed;
  struct U8_INPUT *in, _in;
  if (flags<0) return FD_ERROR_VALUE;
  if (FD_PORTP(input)) {
    struct FD_PORT *p=fd_consptr(struct FD_PORT *,input,fd_port_type);
    in=p->fd_inport;}
  else if ((FD_STRINGP(input))&&(strchr(FD_STRDATA(input),'<')==NULL))
    return fdxml_load(input,sloppy);
 else if (FD_STRINGP(input)) {
    U8_INIT_STRING_INPUT(&_in,FD_STRLEN(input),FD_STRDATA(input));
    in=&_in;}
  else if (FD_PACKETP(input)) {
    U8_INIT_STRING_INPUT(&_in,FD_PACKET_LENGTH(input),FD_PACKET_DATA(input));
    in=&_in;}
  else return fd_type_error(_("string or port"),"xmlparse",input);
  if (FD_FIXNUMP(sloppy))
    flags=FD_FIX2INT(sloppy);
  else if (!((FD_VOIDP(sloppy)) || (FD_FALSEP(sloppy))))
    flags=flags|FD_SLOPPY_XML;
  else {}
  parsed=fd_parse_fdxml(in,flags);
  if (parsed) {
    fdtype result=parsed->fdxml_head;
    fd_incref(result);
    free_node(parsed,1);
    return result;}
  else return FD_ERROR_VALUE;
}

FD_EXPORT fdtype fd_fdxml_arg(fdtype input)
{
  return fdxml_read(input,FD_INT(FD_XML_SLOPPY|FD_XML_KEEP_RAW));
}

/* Initialization functions */

FD_EXPORT void fd_init_xmlinput_c()
{
  fdtype full_module=fd_new_module("FDWEB",0);
  fdtype safe_module=fd_new_module("FDWEB",(FD_MODULE_SAFE));
  fdtype xmlparse_prim=fd_make_ndprim(fd_make_cprim2("XMLPARSE",xmlparse,1));
  fdtype fdxml_load_prim=
    fd_make_ndprim(fd_make_cprim2("FDXML/LOAD",fdxml_load,1));
  fdtype fdxml_read_prim=
    fd_make_ndprim(fd_make_cprim2("FDXML/PARSE",fdxml_read,1));
  fd_defn(full_module,xmlparse_prim); fd_idefn(safe_module,xmlparse_prim);
  fd_defn(full_module,fdxml_read_prim); fd_idefn(safe_module,fdxml_read_prim);
  fd_defn(full_module,fdxml_load_prim);

  fd_defn(full_module,xmlparse_prim); fd_idefn(safe_module,xmlparse_prim);
  fd_defn(full_module,fdxml_read_prim); fd_idefn(safe_module,fdxml_read_prim);

  attribs_symbol=fd_intern("%ATTRIBS");
  content_symbol=fd_intern("%CONTENT");
  type_symbol=fd_intern("%TYPE");

  namespace_symbol=fd_intern("%NAMESPACE");
  xmltag_symbol=fd_intern("%XMLTAG");
  rawtag_symbol=fd_intern("%RAWTAG");
  qname_symbol=fd_intern("%QNAME");
  xmlns_symbol=fd_intern("%XMLNS");

  comment_symbol=fd_intern("%COMMENT");
  cdata_symbol=fd_intern("%CDATA");

  attribids=fd_intern("%ATTRIBIDS");

  sloppy_symbol=fd_intern("SLOPPY");
  decode_symbol=fd_intern("DECODE");
  keepraw_symbol=fd_intern("KEEPRAW");
  crushspace_symbol=fd_intern("CRUSHSPACE");
  slotify_symbol=fd_intern("SLOTIFY");
  nocontents_symbol=fd_intern("NOCONTENTS");
  ishtml_symbol=fd_intern("HTML");
  nsfree_symbol=fd_intern("NSFREE");
  autoclose_symbol=fd_intern("AUTOCLOSE");
  noempty_symbol=fd_intern("NOEMPTY");
  data_symbol=fd_intern("DATA");

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
