/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
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

#include <libu8/xfiles.h>

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
  while ((c>0)&&((out.u8_outlim-out.u8_outptr)<8)) {
    u8_putc(&out,c);}
  return out.u8_outptr;
}

static u8_string deentify(u8_string arg,u8_string lim)
{
  U8_OUTPUT out; u8_byte *scan=arg; int c=u8_sgetc(&scan);
  U8_INIT_OUTPUT(&out,strlen(arg));
  while ((c>0)&&((lim==NULL)||(scan<=lim)))
    if (c=='&') {
      u8_byte *end=NULL; int code=u8_parse_entity(scan,&end);
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
  U8_OUTPUT out; u8_byte *scan=arg; int c=u8_sgetc(&scan);
  U8_INIT_OUTPUT(&out,strlen(arg));
  while ((c>0)&&((lim==NULL)||(scan<=lim)))
    if (c=='&') {
      u8_byte *end=NULL; int code=u8_parse_entity(scan,&end);
      if (code<=0) {
	u8_putc(&out,c); c=u8_sgetc(&scan);}
      else {
	u8_putc(&out,code); scan=end;
	c=u8_sgetc(&scan);}}
    else {u8_putc(&out,c); c=u8_sgetc(&scan);}
  return fd_make_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
}

static fdtype raw_name_symbol;
static fdtype namespace_symbol, name_symbol, qname_symbol, xmlns_symbol;
static fdtype attribs_symbol, content_symbol, type_symbol;

static fdtype sloppy_symbol, keepraw_symbol, crushspace_symbol;
static fdtype slotify_symbol, nocontents_symbol, nsfree_symbol, autoclose_symbol;
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
	u8_byte *end=NULL; int code=u8_parse_entity((*s)+1,&end);
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
  {"p","ul","ol","dl","h1","h2","h3","h4","h5","h6","pre","table","div","blockquote",
   "noframes","noscript","menu","isindex",NULL};

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
  (U8_INPUT *in,u8_byte **bufp,int *bufsizp,int *sizep,char *eos)
{
  u8_byte *buf=*bufp; int bufsiz=*bufsizp, sz;
  u8_string data=u8_gets_x(buf,bufsiz,in,eos,&sz);
  if (sizep) *sizep=sz;
  if ((data==NULL)&&(sz==0)) return NULL;
  else if ((data==NULL)&&(sz<0)) return NULL;
  else if (data==NULL) {
    int new_size=bufsiz, need_size=*sizep+1; u8_byte *newbuf;
    while (new_size<need_size)
      if (new_size<=16384) new_size=new_size*2;
      else new_size=new_size+16384;
    u8_free(buf);
    *bufp=newbuf=u8_malloc(new_size);
    *bufsizp=new_size;
    return u8_gets_x(newbuf,new_size,in,eos,sizep);}
  else return data;
}

FD_EXPORT
void *fd_walk_markup(U8_INPUT *in,
		     void *(*contentfn)(void *,u8_string),
		     void *(*markupfn)(void *,u8_string),
		     void *data)
{
  u8_byte *buf=u8_malloc(1024); int bufsiz=1024, size=bufsiz;
  while (1) {
    if (readbuf(in,&buf,&bufsiz,&size,"<")==NULL) {
      data=contentfn(data,in->u8_inptr); break;}
    else 
      data=contentfn(data,buf);
    if (data==NULL) break;
    if (readbuf(in,&buf,&bufsiz,&size,">")==NULL) {
      data=markupfn(data,in->u8_inptr); break;}
    data=markupfn(data,buf);
    if (data==NULL) break;}
  u8_free(buf);
  return data;
}

/* XML handling */

static void set_elt_name(FD_XML *,u8_string);

static void init_node(FD_XML *node,FD_XML *parent,u8_string name)
{
  node->eltname=name;
  node->head=FD_EMPTY_LIST; node->tail=NULL;
  node->attribs=FD_EMPTY_CHOICE;
  node->parent=parent;
  if (parent) node->bits=((parent->bits)&(FD_XML_INHERIT_BITS));
  else node->bits=FD_XML_DEFAULT_BITS;
  node->namespace=NULL; node->nsmap=NULL;
  node->size=0; node->limit=0; node->data=NULL;
  if (((node->bits)&(FD_XML_ISHTML)) && (strcasecmp(name,"P")==0))
    node->bits=node->bits|FD_XML_INPARA;
}

static void init_node_attribs(struct FD_XML *node)
{
  if (FD_EMPTY_CHOICEP(node->attribs))
    node->attribs=fd_empty_slotmap();
  set_elt_name(node,node->eltname);
}

FD_EXPORT
void fd_init_xml_node(FD_XML *node,FD_XML *parent,u8_string name)
{
  init_node(node,parent,name);
}

FD_EXPORT void fd_init_xml_attribs(struct FD_XML *node)
{
  if (FD_EMPTY_CHOICEP(node->attribs))
    node->attribs=fd_empty_slotmap();
  set_elt_name(node,node->eltname);
}

static void free_nsinfo(FD_XML *node)
{
  if (node->nsmap) {
    int i=0, lim=node->size;
    while (i<lim) u8_free(node->nsmap[i++]);
    u8_free(node->nsmap);
    node->nsmap=NULL; node->limit=node->size=0;}
  if (node->namespace) u8_free(node->namespace);
}

static void free_node(FD_XML *node,int full)
{
  free_nsinfo(node);
  fd_decref(node->attribs);
  fd_decref(node->head);
  if (node->eltname) u8_free(node->eltname);
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

static void ns_add(FD_XML *xml,u8_string prefix,u8_string url)
{
  int prefix_len=strlen(prefix), url_len=strlen(url);
  u8_string entry=u8_malloc(prefix_len+url_len+2);
  strncpy(entry,prefix,prefix_len);
  entry[prefix_len]=':';
  strcpy(entry+prefix_len+1,url);
  if (xml->size>=xml->limit) {
    int new_limit=((xml->limit) ? (2*xml->limit) : (4));
    xml->nsmap=u8_realloc_n(xml->nsmap,new_limit,u8_string);
    xml->limit=new_limit;}
  xml->nsmap[xml->size++]=entry;  
}

static u8_string ns_get(FD_XML *xml,u8_string s,u8_string *nsp)
{
  u8_byte *colon=strchr(s,':');
  if (colon==NULL) {
    FD_XML *scan=xml; while (scan)
      if (scan->namespace) {
	*nsp=scan->namespace; return s;}
      else scan=scan->parent;
    *nsp=NULL;
    return s;}
  else {
    int prefix_len=(colon-s)+1;
    FD_XML *scan=xml; while (scan)
      if (scan->nsmap) {
	int i=0, len=scan->size;
	u8_string *nsmap=scan->nsmap;
	while (i<len)
	  if (strncmp(s,nsmap[i],prefix_len)==0) {
	    *nsp=nsmap[i]+prefix_len;
	    return colon+1;}
	  else i++;
	scan=scan->parent;}
      else scan=scan->parent;
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
  if (FD_EMPTY_CHOICEP(xml->attribs)) init_node_attribs(xml);
  if (hasprefix("xmlns",name))
    if (strlen(name)==5) {
      fdtype nsval=fdtype_string(val);
      xml->namespace=u8_strdup(val);
      fd_add(xml->attribs,xmlns_symbol,nsval);
      fd_decref(nsval);
      return 1;}
    else if ((nsprefix=(strchr(name,':')))) {
      fdtype entry=
	fd_init_pair(NULL,fdtype_string(nsprefix+1),
		     fdtype_string(val));
      ns_add(xml,nsprefix+1,val);
      fd_add(xml->attribs,xmlns_symbol,entry);
      fd_decref(entry);
      return 1;}
    else return 0;
  else return 0;
}

static int allspacep(u8_string s)
{
  u8_byte *scan=s; int c=u8_sgetc(&scan);
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
       ((node->bits&FD_XML_CRUSHSPACE)&&
	(allspacep(FD_STRDATA(item)))))) {
    fd_decref(item);
    return;}
  else {
    fdtype entry;
    if (FD_STRINGP(item)) {
      int parse_entities=
	(((node->bits)&(FD_XML_DECODE_ENTITIES)) &&
	 (strchr(FD_STRDATA(item),'&')!=NULL));
      entry=item2list(item,parse_entities);}
    else entry=fd_make_list(1,item);
    if (node->tail==NULL) {
      node->head=entry;
      node->tail=(struct FD_PAIR *)entry;}
    else {
      node->tail->cdr=entry;
      node->tail=(struct FD_PAIR *)entry;}}
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
  if ((ns) && ((xml->bits&FD_XML_NSFREE)==0)) {
    fdtype namespace=fdtype_string(ns);
    fdtype qname=make_qid(eltname,ns);
    fd_add(xml->attribs,namespace_symbol,namespace);
    fd_add(xml->attribs,qname_symbol,qname);
    fd_decref(qname); fd_decref(namespace);}
  fd_add(xml->attribs,name_symbol,lispname);
  fd_add(xml->attribs,qname_symbol,qlispname);
  fd_decref(lispname);
}

/* Parsing XML tags */

FD_EXPORT
int fd_parse_element(u8_byte **scanner,u8_byte *end,
		     u8_byte **elts,int max_elts,int sloppy)
{
  int n_elts=0;
  u8_byte *scan=*scanner, *elt_start=scan;
  /* Accumulate elements into _elts_ */
  if ((*scan=='/') || (*scan=='?') || (*scan=='!')) {
    /* Skip post < character */
    scan++; elt_start=scan;}
  while (scan<end)
    /* Scan to set scan at the end */
    if (isspace(*scan)) {
      u8_byte *item_end=scan;
      /* Spaces outside of quotes or not after or before = are always
       * breaks */
      while ((scan<end) && (isspace(*scan))) scan++;
      if (*scan=='=') {
	scan++;
	while ((scan<end) && (isspace(*scan))) scan++;
	continue;}
      else {
	*item_end='\0'; elts[n_elts++]=elt_start;
	if (n_elts>=max_elts) {
	  *scanner=scan; return n_elts;}
	elt_start=scan;}}
    else if (*scan=='\'') {
      /* Scan a single quoted value, waiting for an unprotected
	 single quote */
      u8_byte *aend=strchr(scan+1,'\'');
      /* Scan, ignoring escaped single quotes */
      while ((aend)&&(aend<end))
	if (aend[-1]=='\\') aend=strchr(aend+1,'\'');
	else break;
      if ((aend)&&(aend<end)) scan=aend+1; /* got one */
      else if (sloppy) {
	/* Not closed, but sloppy is okay, so we just go up to the
	   first space after the opening quote.  We could be more
	   clever (looking for xxx=, for exampe) but let's not.  */
	u8_byte *lookahead=scan;
	int c=u8_sgetc(&lookahead);
	while ((c>0)&&(!(u8_isspace(c)))&&(lookahead<end)) {
	  scan=lookahead; c=u8_sgetc(&lookahead);}}
      else {
	fd_seterr3(fd_XMLParseError,"unclosed quote in attribute",
		   xmlsnip(*scanner));
	return -1;}}
    else if (*scan=='"') {
      /* This is the exact same logic as above, but scanning for
	 a double quote rather than a single quote. */
      u8_byte *aend=strchr(scan+1,'"');
      /* Scan, ignoring escaped single quotes */
      while ((aend)&&(aend<end))
	if (aend[-1]=='\\') end=strchr(aend+1,'"');
	else break;
      if ((aend)&&(aend<end)) scan=aend+1; /* got one */
      else if (sloppy) {
	/* Not closed, but sloppy is okay, so we just go up to the
	   first space after the opening quote.  We could be more
	   clever (looking for xxx=, for exampe) but let's not.  */
	u8_byte *lookahead=scan;
	int c=u8_sgetc(&lookahead);
	if (lookahead==end) scan=lookahead;
	else while ((c>0)&&(!(u8_isspace(c)))&&(lookahead<end)) {
	    scan=lookahead; c=u8_sgetc(&lookahead);}}
      else {
	fd_seterr3(fd_XMLParseError,"unclosed quote in attribute",
		   xmlsnip(*scanner));
	return -1;}}
    else if (*scan=='=') {
      /* Skip whitespace after an = sign */
      u8_byte *start=++scan; u8_byte *next=start;
      int c=u8_sgetc(&next);
      while ((c>0)&&(u8_isspace(c))&&(scan<end)) {
	scan=next; c=u8_sgetc(&next);}
      /* If you ran over (no value for =), just take the
	 string up to the = */
      if (scan>=end) scan=start;}
    else if (*scan=='\0') scan++;
    else u8_sgetc(&scan);
  if (scan>elt_start) {
    *scan='\0'; elts[n_elts++]=elt_start;}
  return n_elts;
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
  u8_byte *start=buf, *end=buf+(len-1);
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
			    FD_XML *xml,u8_string *items,int n)
{
  int i=0; while (i< n) {
    u8_string item=items[i++]; int quote=-1;
    u8_byte *equals=strchr(item,'=');
    if (equals) {
      u8_byte *valstart=equals+1, *scan=valstart; int c;
      u8_byte *valend=valstart+strlen(valstart)-1;
      u8_string val=NULL;
      *equals='\0'; /* We're going to parse this */
      c=u8_sgetc(&scan);
      /* Skip whitespace */
      while ((c>=0)&&((c==' ')||(c=='\n')||(c=='\r')||(u8_isspace(c)))) {
	valstart=scan; c=u8_sgetc(&scan);}
      /* We don't have to worry about where the item ends
	 (that was handled by parse_element) but we do need to
	 strip off any quotes. */
      if (*valstart=='"') {
	valstart++; quote='"'; if (*valend=='"') *valend='\0';}
      else if (*valstart=='\'') {
	valstart++; quote='\''; if (*valend=='\'') *valend='\0';}
      else {}
      /* if (strchr(valstart,'&')) val=deentify(valstart,NULL); else */
      val=valstart;
      if (process_nsattrib(xml,item,val)) {}
      else if ((attribfn) && (attribfn(xml,item,val,quote))) {}
      else fd_default_attribfn(xml,item,val,quote);
      if (val!=valstart) u8_free(val);}
    else if ((attribfn) && (attribfn(xml,item,NULL,-1))) {}
    else fd_default_attribfn(xml,item,NULL,-1);}
}

/* Defaults */

FD_EXPORT
void fd_default_contentfn(FD_XML *node,u8_string s,int len)
{
  if (strncmp(s,"<!--",4)==0) {
    fdtype cnode=fd_empty_slotmap();
    fd_store(cnode,name_symbol,comment_symbol);
    fd_store(cnode,content_symbol,
	     fd_init_pair(NULL,fd_extract_string(NULL,s+4,s+(len-3)),
			  FD_EMPTY_LIST));
    add_content(node,cnode);}
  else if (strncmp(s,"<![CDATA[",9)==0) {
    fdtype cnode=fd_empty_slotmap();
    fd_store(cnode,name_symbol,cdata_symbol);
    fd_store(cnode,content_symbol,
	     fd_init_pair(NULL,fd_extract_string(NULL,s+9,s+(len-3)),
			  FD_EMPTY_LIST));
    add_content(node,cnode);}
  else add_content(node,fdtype_string(s));
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
  if ((node->bits&FD_XML_NOEMPTY)==0) return 1;
  else if (FD_PAIRP(node->head)) return 1;
  else if (FD_EMPTY_CHOICEP(node->attribs)) return 0;
  else if (!(FD_SLOTMAPP(node->attribs)))
    /* Not sure this case ever happens */
    return 1;
  else if (FD_SLOTMAP_SIZE(node->attribs)>1) return 1;
  else return 0;
}

FD_EXPORT
FD_XML *fd_default_popfn(FD_XML *node)
{
  if (FD_PAIRP(node->head)) {
    if (FD_EMPTY_CHOICEP(node->attribs)) init_node_attribs(node);
    fd_add(node->attribs,content_symbol,node->head);}
  if (((node->bits&FD_XML_NOCONTENTS)==0) ||
      (node->parent==NULL) ||
      (node->parent->parent==NULL)) {
    if (FD_EMPTY_CHOICEP(node->attribs)) init_node_attribs(node);
    if (node->parent!=NULL) {
      add_content(node->parent,node->attribs);
      node->attribs=FD_EMPTY_CHOICE;}}
  if ((node->bits&FD_XML_SLOTIFY) && (slotify_nodep(node))) {
    fdtype slotid;
    if (FD_EMPTY_CHOICEP(node->attribs)) init_node_attribs(node);
    if (FD_EMPTY_CHOICEP(node->parent->attribs))
      init_node_attribs(node->parent);
    slotid=fd_get(node->attribs,name_symbol,FD_EMPTY_CHOICE);
    if ((node->bits)&FD_XML_KEEP_RAW)
      add_content(node->parent,fd_incref(node->attribs));
    node->parent->bits=node->parent->bits|FD_XML_HASDATA;
    if ((FD_PAIRP(node->head)) &&
	(!((node->bits)&FD_XML_HASDATA)) &&
	(!(FD_PAIRP(FD_CDR(node->head)))))
      fd_add(node->parent->attribs,slotid,FD_CAR(node->head));
    else fd_add(node->parent->attribs,slotid,node->attribs);}
  return node->parent;
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
  if (FD_EMPTY_CHOICEP(xml->attribs)) init_node_attribs(xml);
  xml->bits=xml->bits|FD_XML_HASDATA;
  fd_add(xml->attribs,slotid,slotval);
  if (namespace) {
    fd_add(xml->attribs,parse_attribname(attrib_name),slotval);
    attrib_entry=
      fd_make_nvector(3,fdtype_string(name),make_qid(attrib_name,namespace),
		      fd_incref(slotval));}
  else attrib_entry=
	 fd_make_nvector(3,fdtype_string(name),FD_FALSE,fd_incref(slotval));
  fd_add(xml->attribs,attribids,slotid);
  fd_add(xml->attribs,attribs_symbol,attrib_entry);
  fd_decref(attrib_entry); fd_decref(slotval);
  return 1;
}

/* This attribute function always stores strings and never parses the args. */

static FD_XML *autoclose(FD_XML *node,u8_string name,
			 FD_XML *(*popfn)(FD_XML *))
{
  FD_XML *scan=node->parent; while (scan)
    if (strcmp(scan->eltname,name)==0) {
      FD_XML *freescan=node, *retval, *next;
      while (freescan) {
	if (freescan==scan) break;
	if ((retval=popfn(freescan))!=freescan->parent)
	  return retval;
	next=freescan->parent;
	free_node(freescan,1);
	freescan=next;}
      return scan;}
    else scan=scan->parent;
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
      if (FD_EMPTY_CHOICEP(node->attribs)) init_node_attribs(node);
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
    if ((((node->bits)&(FD_XML_FOLDCASE)) ?
	 (strcasecmp(node->eltname,elts[0])) :
	 (strcmp(node->eltname,elts[0])))
	==0) {
      struct FD_XML *retnode;
      if (FD_EMPTY_CHOICEP(node->attribs)) init_node_attribs(node);
      if ((FD_EMPTY_LISTP(node->head)) &&
	  (!(node->bits&FD_XML_NOEMPTY))) 
	node->head=fd_init_pair(NULL,fd_init_string(NULL,0,NULL),
				FD_EMPTY_LIST);
      retnode=popfn(node);
      if (retnode!=node)
	free_node(node,1);
      return retnode;}
    else if ((node->bits&FD_XML_EMPTY_CLOSE) && (elts[0][0]=='\0')) {
      struct FD_XML *retnode;
      if (FD_EMPTY_CHOICEP(node->attribs)) init_node_attribs(node);
      fd_add(node->attribs,raw_name_symbol,fdtype_string(node->eltname));
      retnode=popfn(node);
      if (retnode!=node) 
	free_node(node,1);
      return retnode;}
    else {
      if (node->bits&FD_XML_AUTOCLOSE) {
	FD_XML *closenode=NULL;
	if (FD_EMPTY_CHOICEP(node->attribs)) init_node_attribs(node);
	fd_add(node->attribs,raw_name_symbol,fdtype_string(node->eltname));
	closenode=autoclose(node,elts[0],popfn);
	if (closenode) {
	  struct FD_XML *retnode;
	  retnode=popfn(closenode);	  
	  if (retnode!=closenode)
	    free_node(closenode,1);
	  return retnode;}}
      if ((node->bits)&(FD_XML_BADCLOSE))
	return node;
      else fd_seterr(fd_XMLParseError,"inconsistent close tag",
		     u8_mkstring("</%s> closes <%s>",elts[0],node->eltname),
		     FD_VOID);
      return NULL;}
  case xmlopen:
    if (pushfn) return pushfn(node,type,elts,n_elts);
    else {
      FD_XML *newnode=u8_alloc(struct FD_XML);
      u8_string name=u8_strdup(elts[0]);
      if ((node->bits&FD_XML_ISHTML) &&
	  (check_pair(node->eltname,elts[0],html_autoclose))) {
	FD_XML *popped=popfn(node);
	if (popped!=node)
	  free_node(node,1);
	node=popped;}
      else if (((node->bits)&(FD_XML_INPARA))&&
	       (block_elementp(elts[0]))) {
	while ((node) && ((node->bits)&(FD_XML_INPARA))) {
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
  int bufsize=1024, size=bufsize;
  u8_byte *buf=u8_malloc(1024), *rbuf;
  while (1) {
    fd_xmlelt_type type;
    if ((rbuf=readbuf(in,&buf,&bufsize,&size,"<"))==NULL) {
      if (contentfn)
        contentfn(node,in->u8_inptr,in->u8_inlim-in->u8_inptr);
      break;}
    else if (size<0) {
      fd_seterr3(fd_XMLParseError,"end of input",xmlsnip(in->u8_inptr));
      u8_free(buf);
      return NULL;}
    else if ((size>0)&&(contentfn)) contentfn(node,buf,size);
    else {}
    if ((rbuf=readbuf(in,&buf,&bufsize,&size,">"))==NULL) {
      fd_seterr3(fd_XMLParseError,"end of input",xmlsnip(in->u8_inptr));
      u8_free(buf);
      return NULL;}
    else type=fd_get_markup_type(buf,size,((node->bits)&FD_XML_ISHTML));
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
      u8_byte *remainder=NULL, *combined;
      int combined_len, more_data=0;
      if (strcmp((buf+size-2),"--")) 
	/* If the markup end isn't --, we still need to find the
	   content end (plus more content) */
	remainder=u8_gets_x(NULL,0,in,"-->",&more_data);
      if (more_data)
	combined_len=size+more_data+4;
      else combined_len=size+2;
      combined=u8_malloc(combined_len+1);
      strncpy(combined,"<",1);
      strncpy(combined+1,buf,size);
      if (more_data) {
	strncpy(combined+1+size,remainder,more_data);
	strncpy(combined+1+size+more_data,"-->",4);}
      else strncpy(combined+1+size,">",2);
      if (contentfn)
	contentfn(node,combined,combined_len);
      u8_free(combined);
      if (more_data) u8_free(remainder);}
    else if (type == xmlcdata) {
      u8_byte *remainder=NULL, *combined;
      int more_data=0, combined_len;
      if (strcmp((buf+size-2),"]]")) 
	remainder=u8_gets_x(NULL,0,in,"]]>",&more_data);
      if (more_data) combined_len=size+more_data+4;
      else combined_len=size+2;
      combined=u8_malloc(combined_len+1);
      strncpy(combined,"<",1);
      strncpy(combined+1,buf,size);
      if (more_data) {
	strncpy(combined+1+size,remainder,more_data);
	strncpy(combined+1+size+more_data,"]]>",4);}
      else strncpy(combined+1+size,">",2);
      if (contentfn) contentfn(node,combined,combined_len);
      u8_free(combined); if (more_data) u8_free(remainder);}
    else if (type == xmldoctype) {
      if (strchr(buf,'<')) {}
      else if (pifn) node=pifn(in,node,buf,size);
      else if (contentfn) {
	u8_string reconstituted=
	  u8_string_append("<",buf,">",NULL);
	contentfn(node,reconstituted,size+2);
	u8_free(reconstituted);}}
    else {
      u8_byte *scan=buf, *_elts[32], **elts=_elts;
      int n_elts;
      if ((type == xmlempty)&&(buf[size-1]=='/')) {
	buf[size-1]='\0'; size--;} 
     n_elts=fd_parse_element
	(&scan,buf+size,elts,32,((node->bits)&(FD_XML_BADATTRIB)));
      if (n_elts<0) {
	fd_seterr3(fd_XMLParseError,"xmlstep",xmlsnip(scan));
	u8_free(buf);
	return NULL;}
      node=xmlstep(node,type,elts,n_elts,attribfn,pushfn,popfn);}
    if (node==NULL) break;}
  while ((node)&&((node->bits)&(FD_XML_AUTOCLOSE))) {
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
    struct FD_PORT *p=FD_GET_CONS(input,fd_port_type,struct FD_PORT *);
    in=p->in;}
  else if (FD_STRINGP(input)) {
    U8_INIT_STRING_INPUT(&_in,FD_STRLEN(input),FD_STRDATA(input));
    in=&_in;}
  else if (FD_PACKETP(input)) {
    U8_INIT_STRING_INPUT(&_in,FD_PACKET_LENGTH(input),FD_PACKET_DATA(input));
    in=&_in;}
  else return fd_type_error(_("string or port"),"xmlparse",input);
  init_node(&object,NULL,u8_strdup("top")); object.bits=flags;
  retval=fd_walk_xml(in,fd_default_contentfn,NULL,NULL,NULL,
		     fd_default_popfn,
		     &object);
  if (retval) {
    fdtype result=fd_incref(object.head);
    free_node(&object,0);
    return result;}
  errpos=_in.u8_inptr-_in.u8_inbuf;
  fd_seterr("XMLPARSE error","xmlparse_core",NULL,FD_INT2DTYPE(errpos));
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
    return xmlparse(FD_XQCHOICE(input)->choice,options);
  else return xmlparse_core(input,fd_xmlparseoptions(options));
}

/* Parsing FDXML */

static fdtype fdxmlparse(fdtype input,fdtype sloppy)
{
  int flags=FD_XML_KEEP_RAW;
  struct FD_XML *retval;
  struct U8_INPUT *in, _in;
  if (flags<0) return FD_ERROR_VALUE;
  if (FD_PORTP(input)) {
    struct FD_PORT *p=FD_GET_CONS(input,fd_port_type,struct FD_PORT *);
    in=p->in;}
  else if (FD_STRINGP(input)) {
    U8_INIT_STRING_INPUT(&_in,FD_STRLEN(input),FD_STRDATA(input));
    in=&_in;}
  else if (FD_PACKETP(input)) {
    U8_INIT_STRING_INPUT(&_in,FD_PACKET_LENGTH(input),FD_PACKET_DATA(input));
    in=&_in;}
  else return fd_type_error(_("string or port"),"xmlparse",input);
  if (!((FD_VOIDP(sloppy)) || (FD_FALSEP(sloppy))))
    flags=flags|FD_SLOPPY_XML;
  retval=fd_read_fdxml(in,flags);
  if (retval) {
    fdtype result=fd_incref(retval->head);
    fdtype lispenv=(fdtype)(retval->data);
    fd_incref(lispenv);
    /* free_node(&object,0); */
    return fd_init_pair(NULL,lispenv,result);}
  else return FD_ERROR_VALUE;
}

/* Initialization functions */

FD_EXPORT void fd_init_xmlinput_c()
{
  fdtype full_module=fd_new_module("FDWEB",0);
  fdtype safe_module=fd_new_module("FDWEB",(FD_MODULE_SAFE));
  fdtype xmlparse_prim=fd_make_ndprim(fd_make_cprim2("XMLPARSE",xmlparse,1));
  fdtype fdxmlparse_prim=
    fd_make_ndprim(fd_make_cprim2("FDXML/PARSE",fdxmlparse,1));
  fd_defn(full_module,xmlparse_prim); fd_idefn(safe_module,xmlparse_prim);
  fd_defn(full_module,fdxmlparse_prim); fd_idefn(safe_module,fdxmlparse_prim);

  attribs_symbol=fd_intern("%ATTRIBS");
  content_symbol=fd_intern("%CONTENT");
  type_symbol=fd_intern("%TYPE");

  namespace_symbol=fd_intern("%NAMESPACE");
  name_symbol=fd_intern("%XMLTAG");
  qname_symbol=fd_intern("%QNAME");
  xmlns_symbol=fd_intern("%XMLNS");

  raw_name_symbol=fd_intern("%RAWTAG");
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
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
