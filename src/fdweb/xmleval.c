/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2010 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/fddb.h"
#include "fdb/eval.h"
#include "fdb/apply.h"
#include "fdb/ports.h"
#include "fdb/fdweb.h"
#include "fdb/sequences.h"
#include "fdb/fileprims.h"

#include <libu8/xfiles.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>

#include <ctype.h>

fd_lispenv fdxml_module;

int fd_cache_markup=1;

static fdtype xmleval_tag, xmleval2expr_tag;
static fdtype rawname_slotid, raw_attribs, raw_markup;
static fdtype content_slotid, elt_name, qname_slotid, attribs_slotid;
static fdtype id_symbol, bind_symbol, xml_env_symbol, xmlns_symbol;

static fdtype pblank_symbol, xmlnode_symbol, xmlbody_symbol, env_symbol;
static fdtype pnode_symbol, pbody_symbol, begin_symbol;

static fdtype xattrib_overlay, escape_id, attribids, piescape_symbol;

void *inherit_node_data(FD_XML *node)
{
  FD_XML *scan=node;
  while (node)
    if (scan->data) return scan->data;
    else scan=scan->parent;
  return NULL;
}

/* Accessing xml attributes and elements. */

FD_EXPORT
fdtype fd_xml_get(fdtype xml,fdtype slotid)
{
  fdtype results=fd_get(xml,slotid,FD_EMPTY_CHOICE);
  fdtype content=fd_get(xml,content_slotid,FD_VOID);
  FD_DOLIST(item,content)
    if ((FD_TABLEP(item)) && (fd_test(item,elt_name,slotid))) {
      FD_ADD_TO_CHOICE(results,item);}
  fd_decref(content);
  return results;
}

static int output_attribval(u8_output out,fdtype val,fd_lispenv env,int colon)
{
  if ((FD_PAIRP(val)) &&
      ((FD_EQ(FD_CAR(val),xmleval_tag)) ||
       (FD_EQ(FD_CAR(val),xmleval2expr_tag)))) {
    fdtype value=fd_eval(FD_CDR(val),env); u8_string as_string;
    if (FD_ABORTP(value)) return fd_interr(value);
    else if (FD_VOIDP(value)) return 0;
    else if (FD_EQ(FD_CAR(val),xmleval2expr_tag))
      as_string=fd_dtype2string(value);
    else if (FD_STRINGP(value))
      as_string=u8_strdup(FD_STRDATA(value));
    else if (FD_EMPTY_CHOICEP(value))
      as_string=u8_strdup("");
    else as_string=fd_dtype2string(value);
    if (FD_EQ(FD_CAR(val),xmleval2expr_tag))
      if ((FD_STRINGP(value)) && (FD_STRLEN(value)>0) &&
	  ((isdigit(FD_STRDATA(value)[0]))||(FD_STRDATA(value)[0]==':')))
	u8_putc(out,'\\');
    /* Don't output a preceding colon if the value would be 'self parsing' */
      else if ((FD_FIXNUMP(value)) || (FD_FLONUMP(value)) || (FD_STRINGP(value))) {}
      else u8_putc(out,':');
    fd_attrib_entify(out,as_string);
    fd_decref(value); u8_free(as_string);
    return 0;}
  else if (FD_SLOTMAPP(val)) {
    fdtype value=fd_xmleval(NULL,val,env); u8_string as_string;
    if (FD_ABORTP(value)) return fd_interr(value);
    else as_string=fd_dtype2string(value);
    if (!(FD_STRINGP(value))) u8_putc(out,':');
    fd_attrib_entify(out,as_string);
    fd_decref(value); u8_free(as_string);
    return 0;}
  else if (FD_STRINGP(val)) {
    fd_attrib_entify(out,FD_STRDATA(val));
    return 1;}
  else if (FD_OIDP(val)) {}
  else {
    u8_string as_string=fd_dtype2string(val);
    if (!((FD_FIXNUMP(val)) || (FD_FLONUMP(val)) || (FD_BIGINTP(val))))
      u8_putc(out,':');
    fd_attrib_entify(out,as_string);
    u8_free(as_string);
    return 1;}
}

static int output_markup_sym(u8_output out,fdtype sym)
{
  if (FD_STRINGP(sym)) return u8_puts(out,FD_STRDATA(sym));
  else if (FD_SYMBOLP(sym)) {
    u8_byte *scan=FD_SYMBOL_NAME(sym); 
    int c=u8_sgetc(&scan);
    while (c>=0) {
      int lc=u8_tolower(c);
      int retcode=u8_putc(out,lc);
      if (retcode<0) return retcode;
      else c=u8_sgetc(&scan);}
    return 1;}
  else {
    u8_printf(out,"%q",sym);
    return 1;}
}

static fdtype get_markup_string(fdtype xml,fd_lispenv env)
{
  U8_OUTPUT out; int cache_result=fd_cache_markup;
  fdtype cached; fdtype xmlns=fd_get(xml,xmlns_symbol,FD_VOID);
  if (fd_cache_markup) {
    cached=fd_get(xml,raw_markup,FD_VOID);
    if (!(FD_VOIDP(cached))) return cached;}
  U8_INIT_OUTPUT(&out,32);
  if (fd_test(xml,rawname_slotid,FD_VOID)) {
    fdtype rawname=fd_get(xml,rawname_slotid,FD_VOID);
    fdtype rawattribs=fd_get(xml,raw_attribs,FD_EMPTY_CHOICE);
    if (FD_ABORTP(rawname)) return rawname;
    else if (FD_ABORTP(rawattribs)) {
      fd_decref(rawname); u8_free(out.u8_outbuf);
      return rawattribs;}
    else if (rawname==pblank_symbol) {
      fd_decref(rawattribs); u8_free(out.u8_outbuf);
      return rawname;}
    else if (!(FD_STRINGP(rawname))) {
      fd_decref(rawname); u8_free(out.u8_outbuf);
      return fd_type_error("XML node","get_markup_string",xml);}
    u8_putn(&out,FD_STRDATA(rawname),FD_STRLEN(rawname));
    if (!(FD_VOIDP(xmlns))) {
      FD_DO_CHOICES(nspec,xmlns) {
	if (FD_STRINGP(nspec)) {
	  u8_printf(&out," xmlns=\"%s\"",FD_STRDATA(nspec));}
	else if ((FD_PAIRP(nspec))&&
		 (FD_STRINGP(FD_CAR(nspec)))&&
		 (FD_STRINGP(FD_CDR(nspec)))) {
	  u8_printf(&out," xmlns:%s=\"%s\"",FD_STRDATA(FD_CAR(nspec)),FD_STRDATA(FD_CDR(nspec)));}
	else {}}}
    {FD_DO_CHOICES(attrib,rawattribs) {
	if (FD_STRINGP(attrib))
	  u8_printf(&out," %s",FD_STRDATA(attrib));
	else {
	  fdtype rawaname=FD_VECTOR_REF(attrib,0);
	  fdtype val=FD_VECTOR_REF(attrib,2);
	  int retcode;
	  u8_printf(&out," %s='",FD_STRDATA(rawaname));
	  retcode=output_attribval(&out,val,env,0);
	  if (retcode<0) {
	    fd_decref(rawname); fd_decref(rawattribs);
	    u8_free(out.u8_outbuf);
	    return FD_ERROR_VALUE;}
	  else if (retcode==0) cache_result=0;
	  else {}
	  u8_putc(&out,'\'');}}}
    fd_decref(rawname); fd_decref(rawattribs);}
  else if ((fd_test(xml,elt_name,FD_VOID))&&
	   (fd_test(xml,attribs_slotid,FD_VOID))) {
    fdtype name=fd_get(xml,elt_name,FD_VOID);
    fdtype attribs=fd_get(xml,attribs_slotid,FD_EMPTY_CHOICE);
    if (name==pblank_symbol) {
      fd_decref(attribs); u8_free(out.u8_outbuf);
      return name;}
    else output_markup_sym(&out,name);
    if (!(FD_VOIDP(xmlns))) {
      FD_DO_CHOICES(nspec,xmlns) {
	if (FD_STRINGP(nspec)) {
	  u8_printf(&out," xmlns=\"%s\"",FD_STRDATA(nspec));}
	else if ((FD_PAIRP(nspec))&&
		 (FD_STRINGP(FD_CAR(nspec)))&&
		 (FD_STRINGP(FD_CDR(nspec)))) {
	  u8_printf(&out," xmlns:%s=\"%s\"",FD_STRDATA(FD_CAR(nspec)),FD_STRDATA(FD_CDR(nspec)));}
	else {}}}
    {FD_DO_CHOICES(attrib,attribs) {
	u8_string name=NULL; fdtype value;
	if (FD_PAIRP(attrib)) {
	  name=FD_STRDATA(FD_CAR(attrib));
	  value=FD_CDR(attrib);}
	else if (FD_VECTORP(attrib)) {
	  name=FD_STRDATA(FD_VECTOR_REF(attrib,0));
	  value=FD_VECTOR_REF(attrib,2);}
	else {}
	if (name) {
	  u8_string qbrace=strchr(name,'}');
	  if (qbrace)
	    u8_printf(&out," %s='",qbrace+1);
	  else u8_printf(&out," %s='",name);
	  if (FD_STRINGP(value))
	    fd_attrib_entify(&out,FD_STRDATA(value));
	  else if (FD_FIXNUMP(value))
	    u8_printf(&out,"%d",FD_FIX2INT(value));
	  else {
	    struct U8_OUTPUT subout; U8_INIT_OUTPUT(&subout,64);
	    fd_unparse(&subout,value);
	    u8_putc(&out,':'); fd_attrib_entify(&out,subout.u8_outbuf);
	    u8_free(subout.u8_outbuf);}
	  u8_putc(&out,'\'');}}}
    fd_decref(attribs); fd_decref(name);}
  else if (fd_test(xml,elt_name,FD_VOID)) {
    fdtype name=fd_get(xml,elt_name,FD_VOID);
    fdtype attribnames=fd_get(xml,attribids,FD_EMPTY_CHOICE);
    if (name==pblank_symbol) {
      fd_decref(attribnames); u8_free(out.u8_outbuf);
      return name;}
    else output_markup_sym(&out,name);
    {FD_DO_CHOICES(attribname,attribnames) {
	fdtype val=fd_get(xml,attribname,FD_VOID); int retcode;
	if (!(FD_VOIDP(val))) {
	  u8_putc(&out,' '); output_markup_sym(&out,attribname);
	  u8_puts(&out,"='");
	  retcode=output_attribval(&out,val,env,0);
	  if (retcode<0) {
	    fd_decref(attribnames); fd_decref(name);
	    u8_free(out.u8_outbuf);
	    return FD_ERROR_VALUE;}
	  else if (retcode==0) cache_result=0;
	  else {}
	  u8_putc(&out,'\'');}}}}
  else return fd_type_error("XML node","get_markup_string",xml);
  cached=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
  if (cache_result) fd_store(xml,raw_markup,cached);
  return cached;
}

FD_EXPORT
fdtype fd_unparse_xml(u8_output out,fdtype xml,fd_lispenv env)
{
  if (FD_STRINGP(xml)) {}
  else if (FD_PAIRP(xml)) {}
  else if (FD_TABLEP(xml)) {
    fdtype markup=get_markup_string(xml,env);
    fdtype content=fd_get(xml,content_slotid,FD_VOID);
    if (FD_ABORTP(markup)) {
      fd_decref(content); return markup;}
    else if (FD_ABORTP(content)) {
      fd_decref(markup); return content;}
    else if ((FD_EMPTY_CHOICEP(content)) ||
	     (FD_VOIDP(content)) ||
	     (FD_EMPTY_LISTP(content))) {
      if (FD_STRINGP(markup))
	u8_printf(out,"<%s/>",FD_STRDATA(markup));
      fd_decref(markup);
      return FD_VOID;}
    if (FD_STRINGP(markup))
      u8_printf(out,"<%s>",FD_STRDATA(markup));
    if (FD_PAIRP(content)) {
      FD_DOLIST(item,content) {
	fdtype result=FD_VOID;
	if (FD_STRINGP(item))
	  u8_putn(out,FD_STRDATA(item),FD_STRLEN(item));
	else result=fd_xmleval(out,item,env);
	if (FD_VOIDP(result)) {}
	else if (FD_ABORTP(result)) {
	  fd_decref(content);
	  return result;}
	else if ((FD_TABLEP(result)) &&
		 (fd_test(result,rawname_slotid,FD_VOID))) {
	  fd_unparse_xml(out,result,env);
	  fd_decref(result);}
	else fd_dtype2xml(out,result,env);
	fd_decref(result);}}
    if (FD_STRINGP(markup)) {
      u8_string mstring=FD_STRDATA(markup);
      u8_string atspace=strchr(mstring,' ');
      u8_puts(out,"</");
      if (atspace) u8_putn(out,mstring,atspace-mstring);
      else u8_puts(out,mstring);
      u8_putc(out,'>');}
    fd_decref(markup); fd_decref(content);
    return FD_VOID;}
  else return fd_type_error("XML node","get_markup_string",xml);
}

/* Handling dynamic elements */

static fdtype get_xml_handler(fdtype xml,fd_lispenv env)
{
  fdtype xml_env=fd_symeval(xml_env_symbol,env);
  if (FD_VOIDP(xml_env)) return FD_VOID;
  if (!(FD_PTR_TYPEP(xml_env,fd_environment_type)))
    return fd_type_error("environment","get_xml_handler",xml_env);
  else {
    fdtype qname=fd_get(xml,qname_slotid,FD_VOID);
    fdtype name=fd_get(xml,elt_name,FD_VOID);
    fdtype value=FD_VOID;
    if (FD_STRINGP(qname)) {
      fdtype symbol=fd_probe_symbol(FD_STRDATA(qname),FD_STRLEN(qname));
      if (FD_SYMBOLP(symbol)) value=fd_symeval(symbol,(fd_lispenv)xml_env);}
    if (!(FD_VOIDP(value))) return value;
    else if (FD_SYMBOLP(name)) return fd_symeval(name,(fd_lispenv)xml_env);
    else return FD_VOID;}
}

struct XMLAPPLY { fdtype xml; fd_lispenv env;};

FD_EXPORT fdtype fdxml_get(fdtype xml,fdtype sym,fd_lispenv env)
{
  if ((sym==xmlnode_symbol) || (sym==pnode_symbol)) return fd_incref(xml);
  else if (sym==env_symbol) return (fdtype) fd_copy_env(env);
  else if ((sym==xmlbody_symbol) || (sym==pbody_symbol)) {
    fdtype content=fd_get(xml,content_slotid,FD_VOID);
    if (FD_VOIDP(content)) return FD_EMPTY_CHOICE;
    else {
      struct FD_KEYVAL *kv=u8_alloc_n(2,struct FD_KEYVAL);
      /* This generates a "blank node" which generates its content
	 without any container. */
      kv[0].key=rawname_slotid; kv[0].value=pblank_symbol;
      kv[1].key=content_slotid; kv[1].value=content;
      return fd_init_slotmap(NULL,2,kv);}}
  else {
    fdtype values=fd_get(xml,sym,FD_VOID);
    if (FD_VOIDP(values)) return FD_VOID;
    else if (FD_CHOICEP(values)) {
      fdtype results=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(value,values)
	if (FD_PAIRP(value))
	  if  ((FD_EQ(FD_CAR(value),xmleval_tag)) ||
	       (FD_EQ(FD_CAR(value),xmleval2expr_tag))) {
	    fdtype result=fd_eval(FD_CDR(value),env);
	    FD_ADD_TO_CHOICE(results,result);}
	  else {
	    fd_incref(value);
	    FD_ADD_TO_CHOICE(results,value);}
	else if (FD_TABLEP(value)) {
	  fdtype result=fd_xmleval(NULL,value,env);
	  FD_ADD_TO_CHOICE(results,result);}
	else {
	  fd_incref(value);
	  FD_ADD_TO_CHOICE(results,value);}
      fd_decref(values);
      return results;}
    else if (FD_PAIRP(values))
      if ((FD_EQ(FD_CAR(values),xmleval_tag)) ||
	  (FD_EQ(FD_CAR(values),xmleval2expr_tag))) {
	fdtype result=fd_eval(FD_CDR(values),env);
	fd_decref(values);
	return result;}
      else return values;
    else if (FD_TABLEP(values))
      return fd_xmleval(NULL,values,env);
    else if (FD_QCHOICEP(values)) {
      fdtype result=FD_XQCHOICE(values)->choice;
      fd_incref(result); fd_decref(values);
      return result;}
    else return values;}
}

static fdtype xmlgetarg(void *vcxt,fdtype sym)
{
  struct XMLAPPLY *cxt=(struct XMLAPPLY *)vcxt;
  return fdxml_get(cxt->xml,sym,cxt->env);
}

static fdtype xmlapply(u8_output out,fdtype fn,fdtype xml,fd_lispenv env)
{
  struct XMLAPPLY cxt; cxt.xml=xml; cxt.env=env;
  fdtype bind=fd_get(xml,id_symbol,FD_VOID), result=FD_VOID;
  if (FD_PTR_TYPEP(fn,fd_specform_type)) {
    struct FD_SPECIAL_FORM *sf=
      FD_GET_CONS(fn,fd_specform_type,fd_special_form);
    result=sf->eval(xml,env);}
  else if (FD_PRIM_TYPEP(fn,fd_sproc_type))
    result=fd_xapply_sproc((struct FD_SPROC *)fn,&cxt,xmlgetarg);
  else {
    fd_decref(bind);
    return fd_type_error("function","xmlapply",fn);}

  result=fd_finish_call(result);

  if (FD_ABORTP(result))
    return result;
  else if (FD_VOIDP(bind)) return result;
  else if (FD_SYMBOLP(bind)) {
    fd_bind_value(bind,result,env);
    fd_decref(result);
    result=FD_VOID;}
  else if (FD_STRINGP(bind)) {
    fdtype sym=fd_parse(FD_STRDATA(bind));
    if (FD_SYMBOLP(sym)) {
      fd_bind_value(sym,result,env);
      fd_decref(result);
      result=FD_VOID;}
    fd_decref(sym);}
  else {}
  return result;
}

/* Handling XML attributes */

/* Thoughts on a simple infix syntax for attribute values:
     foo.bar ==> (get foo 'bar)
     foo@slot ==> (get foo slot)
     foo@?es ==> (get foo @?es)
     foo#car ==> (car foo)
     foo[i] ==> (elt foo i)
   All require that the first character be alphabetical.
   We also rule out variable names include the infix characters
   we are using.
 */

static fdtype get_symbol, elt_symbol, quote_symbol;

static fdtype extract_var(u8_string start,u8_string end)
{
  u8_byte _buf[128], *buf; fdtype result;
  if ((end-start)<127) buf=_buf;
  else buf=u8_malloc((end-start)+1);
  strncpy(buf,start,end-start); buf[end-start]='\0';
  result=fd_parse(buf);
  if (buf!=_buf) u8_free(buf);
  return result;
}

static fdtype parse_infix(u8_string start)
{
  u8_string split;
  if ((split=(strchr(start,'.')))) 
    return fd_make_list(3,get_symbol,extract_var(start,split),
			fd_make_list(2,quote_symbol,fd_parse(split+1)));
  else if ((split=(strchr(start,'#'))))
    return fd_make_list(2,fd_parse(split+1),extract_var(start,split));
  else if ((split=(strchr(start,'['))))
    return fd_make_list(3,elt_symbol,extract_var(start,split),
			fd_parse(split+1));
  else if ((split=(strchr(start,'@'))))
    if (split[1]=='?')
      return fd_make_list
	(3,get_symbol,extract_var(start,split),fd_parse(split));
    else return fd_make_list
	   (3,get_symbol,extract_var(start,split),fd_parse(split+1));
  else return fd_parse(start);
}

static fdtype xmlevalify(u8_string string)
{
  if (string[0]==':')
    if (string[1]=='$')
      return fd_init_pair
	(NULL,xmleval2expr_tag,parse_infix(string+2));
    else return fd_parse(string+1);
  else if (string[0]=='$') {
    u8_string start=string+1;
    int c=u8_sgetc(&start);
    if (u8_isalpha(c))
      return fd_init_pair(NULL,xmleval_tag,parse_infix(string+1));
    else return fd_init_pair(NULL,xmleval_tag,fd_parse(string+1));}
  else if (string[0]=='\\') return fdtype_string(string+1);
  else return fdtype_string(string);
}

static fdtype parse_attrib_name(u8_string name)
{
  fdtype v=fd_parse(name);
  if (FD_ABORTP(v)) {
    fd_decref(v); return fd_intern(name);}
  else return v;
}

FD_EXPORT
int fd_xmleval_attribfn(FD_XML *xml,u8_string name,u8_string val,int quote)
{
  u8_string namespace, attrib_name=fd_xmlns_lookup(xml,name,&namespace);
  if (val) {
    fdtype slotid=parse_attrib_name(attrib_name);
    fdtype slotval=((quote>0) ? (xmlevalify(val)) : (fd_parse(val)));
    fdtype qid=fd_make_qid(attrib_name,namespace);
    fdtype qentry=fd_init_pair(NULL,qid,fd_incref(slotval));
    fdtype rawentry=
      fd_make_vector(3,fdtype_string(name),
		     fd_incref(qid),
		     fd_incref(slotval));
    fd_add(xml->attribs,attribs_slotid,qentry);
    fd_add(xml->attribs,attribids,slotid);
    fd_add(xml->attribs,slotid,slotval);
    fd_add(xml->attribs,raw_attribs,rawentry);
    fd_decref(qentry); fd_decref(rawentry); fd_decref(slotval);}
  else {
    fdtype nameval=fdtype_string(name);
    fd_add(xml->attribs,raw_attribs,nameval);
    if (namespace) {
      fdtype entry=
	fd_init_pair(NULL,fdtype_string(attrib_name),
		     fdtype_string(namespace));
      fd_add(xml->attribs,attribs_slotid,entry);
      fd_decref(entry);}
    fd_decref(nameval);}
  return 1;
}

static fdtype xattrib_slotid;

FD_EXPORT
void fd_xmleval_contentfn(FD_XML *node,u8_string s,int len)
{
  fdtype escape=fd_get((fdtype)(inherit_node_data(node)),escape_id,FD_VOID);
  if (len==0) {}
  else if (FD_VOIDP(escape)) 
    fd_add_content(node,fd_extract_string(NULL,s,s+len));
  else if (FD_STRINGP(escape)) {
    int escape_len=FD_STRLEN(escape);
    u8_string escape_string=FD_STRDATA(escape);
    u8_string start=s, scan=strstr(s,escape_string), limit=s+len;
    while ((scan) && (scan<limit)) {
      U8_INPUT in; fdtype expr;
      if (scan>start)
	fd_add_content(node,fd_extract_string(NULL,start,scan));
      scan=scan+escape_len; U8_INIT_STRING_INPUT(&in,limit-scan,scan);
      if (*scan=='$') {
	/* This handles infix expressions, ending at the next $ */
	u8_string start=scan+1, end=strchr(start,'$');
	u8_byte buf[128];
	if ((end==NULL) || (end-start>100)) {
	  /* If there's not a terminating $, or the string is really
	     long (missing close $), just call the parser,
	     skipping the initial $. */
	  u8_getc(&in); expr=fd_parser(&in);}
	else {
	  strncpy(buf,start,end-start); buf[end-start]='\0';
	  expr=parse_infix(buf);
	  in.u8_inptr=end+1;}}
      else expr=fd_parser(&in);
      if (FD_STRINGP(expr)) {
	struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
	fd_unparse(&out,expr); fd_decref(expr);
	expr=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outptr);
	fd_add_content(node,expr);
	fd_decref(expr);}
      else fd_add_content(node,expr);
      start=in.u8_inptr; scan=strstr(start,escape_string);}
    if ((scan==NULL) && (start) && (*start))
      fd_add_content(node,fdtype_string(start));
    else if (scan>start)
      fd_add_content(node,fd_extract_string(NULL,start,scan));}
  else fd_add_content(node,fd_init_string(NULL,len,s));
}

FD_EXPORT
FD_XML *fd_xmleval_popfn(FD_XML *node)
{
  /* Get your content */
  if (FD_PAIRP(node->head)) {
    if (FD_EMPTY_CHOICEP(node->attribs)) fd_init_xml_attribs(node);
    fd_add(node->attribs,content_slotid,node->head);}
  if (node->parent==NULL) return NULL;
  else {
    fdtype cutaway=fd_get((fdtype)(inherit_node_data(node)),
			  xattrib_overlay,xattrib_slotid);
    fdtype xid=fd_get(node->attribs,cutaway,FD_VOID);
    
    /* Check if you go on the parent's attribs or in its body. */
    if (FD_VOIDP(xid)) {
      fd_add_content(node->parent,node->attribs);
      node->attribs=FD_EMPTY_CHOICE;}
    else if (FD_STRINGP(xid)) {
      fdtype slotid=fd_parse(FD_STRING_DATA(xid));
      fd_add(node->parent->attribs,slotid,node->attribs);
      fd_decref(xid);}
    else fd_add(node->parent->attribs,xid,node->attribs);
    return node->parent;}
}

/* Handling the FDXML PI */

static u8_string get_pi_string(u8_string start)
{
  u8_string end=NULL;
  int c=*start;
  if ((c=='\'') || (c=='"')) {
    start++; end=strchr(start,c);}
  else {
    u8_byte *last=start, *scan=start;
    while (c>0) {
      c=u8_sgetc(&scan);
      if ((c<0) || (u8_isspace(c))) break;
      else last=scan;}
    end=last;}
  if (end)
    return u8_slice(start,end);
  else return NULL;
}

static fd_lispenv get_xml_env(FD_XML *xml)
{
  return (fd_lispenv)fd_symeval(xml_env_symbol,(fd_lispenv)(xml->data));
}

static void set_xml_env(FD_XML *xml,fd_lispenv newenv)
{
  fd_set_value(xml_env_symbol,(fdtype)newenv,(fd_lispenv)(xml->data));
}

static int test_piescape(FD_XML *xml,u8_string content,int len)
{
  if ((strncmp(content,"?fdeval ",7)==0)) return 7;
  else {
    fdtype piescape=fd_get((fdtype)(inherit_node_data(xml)),
			   piescape_symbol,FD_VOID);
    u8_string piend=strchr(content,' ');
    int pielen=((piend)?((piend-content)-1):(0));
    FD_DO_CHOICES(pie,piescape) 
      if ((FD_STRINGP(pie)) && (FD_STRLEN(pie)==0)) {
	if (strncmp(content,"? ",2)==0) return 2;
	else if (strncmp(content,"?(",2)==0) return 1;}
      else if ((FD_STRINGP(pie)) && (FD_STRLEN(pie)==pielen)) {
	if (strncmp(content+1,FD_STRDATA(pie),FD_STRLEN(pie))==0) {
	  FD_STOP_DO_CHOICES;
	  return 1+FD_STRLEN(pie);}}
    return 0;}
}

static FD_XML *handle_xmleval_pi
  (u8_input in,FD_XML *xml,u8_string content,int len)
{
  if (strncmp(content,"?fdxml ",6)==0) {
    u8_string copy=u8_strdup(content);
    u8_byte *scan=copy, *attribs[16];
    int i=0, n_attribs=fd_parse_element(&scan,copy+len,attribs,16,0);
    while (i<n_attribs)
      if ((strncmp(attribs[i],"load=",5))==0) {
	u8_string arg=get_pi_string(attribs[i]+5);
	u8_string filename=fd_get_component(arg);
	fd_lispenv env=(fd_lispenv)(xml->data);
	fd_lispenv xml_env;
	if (fd_load_latest(filename,env,NULL)<0) {
	  u8_free(arg); u8_free(filename);
	  return NULL;}
	else xml_env=get_xml_env(xml);
	u8_free(arg); u8_free(filename);
	if (FD_TABLEP(env->exports)) {
	  fd_lispenv new_xml_env=
	    fd_make_export_env(env->exports,xml_env);
	  set_xml_env(xml,new_xml_env);}
	else {
	  fd_lispenv new_xml_env=
	    fd_make_export_env(env->bindings,xml_env);
	  set_xml_env(xml,new_xml_env);}
	i++;}
      else if ((strncmp(attribs[i],"config=",7))==0) {
	u8_string arg=get_pi_string(attribs[i]+7);
	u8_string filename=fd_get_component(arg);
	int retval=fd_load_config(filename);
	if (retval<0) {
	  u8_condition c; u8_context cxt; u8_string details;
	  fdtype irritant;
	  if (fd_poperr(&c,&cxt,&details,&irritant))
	    if ((FD_VOIDP(irritant)) && (details==NULL) && (cxt==NULL))
	      u8_log(LOG_WARN,"FDXML_CONFIG",
		     _("In config '%s' %m"),filename,c);
	    else if ((FD_VOIDP(irritant)) && (details==NULL))
	      u8_log(LOG_WARN,"FDXML_CONFIG",
		     _("In config '%s' %m@%s"),filename,c,cxt);
	    else if (FD_VOIDP(irritant))
	      u8_log(LOG_WARN,"FDXML_CONFIG",
		     _("In config '%s' [%m@%s] %s"),filename,c,cxt,details);
	    else u8_log(LOG_WARN,"FDXML_CONFIG",
			_("In config '%s' [%m@%s] %s %q"),
			filename,c,cxt,details,irritant);
	  else u8_log(LOG_WARN,"FDXML_CONFIG",
		      _("In config '%s', unknown error"),filename);}
	i++;}
      else if ((strncmp(attribs[i],"module=",7))==0) {
	u8_string arg=get_pi_string(attribs[i]+7);
	fdtype module_name=fd_parse(arg);
	fdtype module=fd_find_module(module_name,0,1);
	fd_lispenv xml_env=get_xml_env(xml);
	u8_free(arg); fd_decref(module_name);
	if ((FD_ENVIRONMENTP(module)) &&
	    (FD_TABLEP(((fd_environment)module)->exports))) {
	  fdtype exports=((fd_environment)module)->exports;
	  fd_lispenv new_xml_env=fd_make_export_env(exports,xml_env);
	  set_xml_env(xml,new_xml_env);}
	else if (FD_TABLEP(module)) {
	  fd_lispenv new_xml_env=
	    fd_make_export_env(module,xml_env);
	  set_xml_env(xml,new_xml_env);}
	i++;}
      else if ((strncmp(attribs[i],"scheme_load=",12))==0) {
	u8_string arg=get_pi_string(attribs[i]+12);
	u8_string filename=fd_get_component(arg);
	fd_lispenv env=(fd_lispenv)(xml->data);
	fd_lispenv xml_env;
	if (fd_load_latest(filename,env,NULL)<0) {
	  u8_free(arg); u8_free(filename);
	  return NULL;}
	else xml_env=get_xml_env(xml);
	u8_free(arg); u8_free(filename);
	i++;}
      else if ((strncmp(attribs[i],"scheme_module=",14))==0) {
	u8_string arg=get_pi_string(attribs[i]+14);
	fdtype module_name=fd_parse(arg);
	fdtype module=fd_find_module(module_name,0,1);
	fd_lispenv scheme_env=(fd_lispenv)(xml->data);
	u8_free(arg); fd_decref(module_name);
	if ((FD_ENVIRONMENTP(module)) &&
	    (FD_TABLEP(((fd_environment)module)->exports))) {
	  fdtype exports=((fd_environment)module)->exports;
	  scheme_env->parent=fd_make_export_env(exports,scheme_env->parent);}
	else if (FD_TABLEP(module)) {
	  scheme_env->parent=fd_make_export_env(module,scheme_env->parent);}
	i++;}
      else if ((strncmp(attribs[i],"escape=",7))==0) {
	fdtype arg=fd_init_string(NULL,-1,get_pi_string(attribs[i]+7));
	fd_lispenv xml_env=(fd_lispenv)(xml->data);
	fd_bind_value(escape_id,arg,xml_env);
	fd_decref(arg);
	i++;}
      else if ((strncmp(attribs[i],"piescape=",9))==0) {
	fdtype arg=fd_init_string(NULL,-1,get_pi_string(attribs[i]+9));
	fd_lispenv xml_env=(fd_lispenv)(xml->data);
	fdtype cur=fd_symeval(piescape_symbol,xml_env);
	if (FD_VOIDP(cur))
	  fd_bind_value(piescape_symbol,arg,xml_env);
	else {
	  FD_ADD_TO_CHOICE(cur,arg);
	  fd_set_value(piescape_symbol,arg,xml_env);}
	fd_decref(arg);
	i++;}
      else if ((strncmp(attribs[i],"xattrib=",8))==0) {
	fdtype arg=fd_init_string(NULL,-1,get_pi_string(attribs[i]+7));
	fd_lispenv xml_env=(fd_lispenv)(xml->data);
	fd_bind_value(xattrib_overlay,arg,xml_env);
	fd_decref(arg);
	i++;}
      else i++;
    u8_free(copy);
    return xml;}
  else {
    int pioff=((strncmp(content,"?eval ",7)==0)?(7):
	       (test_piescape(xml,content,len)));
    if (pioff) {
      struct U8_INPUT in;
      fdtype insert=fd_init_pair(NULL,begin_symbol,FD_EMPTY_LIST);
      fdtype *tail=&(FD_CDR(insert)), expr=FD_VOID;
      U8_INIT_STRING_INPUT(&in,len-pioff-1,content+pioff);
      expr=fd_parse_expr(&in);
      while (1) {
	if (FD_ABORTP(expr)) {
	  fd_decref(insert);
	  return NULL;}
	else if ((FD_EOFP(expr)) || (FD_EOXP(expr))) break;
	else {
	  fdtype new_cons=fd_init_pair(NULL,expr,FD_EMPTY_LIST);
	  *tail=new_cons; tail=&(FD_CDR(new_cons));}
	expr=fd_parse_expr(&in);}
      fd_add_content(xml,insert);}
    return xml;}
}

/* The eval function itself */ 

FD_EXPORT
fdtype fd_xmleval(u8_output out,fdtype xml,fd_lispenv env)
{
  fdtype result=FD_VOID;
  if ((FD_PAIRP(xml)) &&
      ((FD_STRINGP(FD_CAR(xml))) || (FD_TABLEP(FD_CAR(xml))))) {
    /* This is the case where it's a node list */
    fdtype value=FD_VOID;
    FD_DOLIST(elt,xml) {
      if (FD_STRINGP(elt)) u8_puts(out,FD_STRDATA(elt));
      else {
	fd_decref(value); value=fd_xmleval(out,elt,env);
	if (FD_ABORTP(value)) return value;}}
    return value;}
  if (FD_NEED_EVALP(xml)) {
    fdtype result=fd_eval(xml,env);
    /* This is where we have a symbol or list embedded in
       the document (via escapes, for instance) */
    if (FD_VOIDP(result)) {}
    else if ((FD_TABLEP(result)) &&
	     (fd_test(result,elt_name,FD_VOID))) {
      /* If the call returns an XML object, unparse it */
      fd_unparse_xml(out,result,env);
      fd_decref(result);}
    else if (FD_ABORTP(result)) {
      fd_clear_errors(1);
      return FD_VOID;}
    else {
      /* Otherwise, output it as XML */
      fd_dtype2xml(out,result,env);
      fd_decref(result);}}
  else if (FD_STRINGP(xml))
    u8_putn(out,FD_STRDATA(xml),FD_STRLEN(xml));
  else if (FD_OIDP(xml))
    if (fd_oid_test(xml,elt_name,FD_VOID)) {
      fdtype handler=get_xml_handler(xml,env);
      if (FD_VOIDP(handler))
	result=fd_unparse_xml(out,xml,env);
      else result=xmlapply(out,handler,xml,env);
      fd_decref(handler);
      return result;}
    else return xml;
  else if (FD_TABLEP(xml)) {
    fdtype handler=get_xml_handler(xml,env);
    if (FD_VOIDP(handler))
      result=fd_unparse_xml(out,xml,env);
    else result=xmlapply(out,handler,xml,env);
    fd_decref(handler);
    return result;}
  return result;
}

/* Breaking up FDXML evaluation */

FD_EXPORT
fdtype fd_open_xml(fdtype xml,fd_lispenv env)
{
  if (FD_TABLEP(xml)) {
    u8_output out=fd_get_default_output();
    fdtype markup=get_markup_string(xml,env);
    if (FD_STRINGP(markup)) u8_printf(out,"<%s>",FD_STRDATA(markup));
    fd_decref(markup);
    return FD_VOID;}
  else return FD_VOID;
}

FD_EXPORT
fdtype fd_xml_opener(fdtype xml,fd_lispenv env)
{
  if (FD_TABLEP(xml)) 
    return get_markup_string(xml,env);
  else return FD_VOID;
}

FD_EXPORT
fdtype fd_close_xml(fdtype xml)
{
  fdtype name=FD_VOID;
  u8_output out=fd_get_default_output();
  if (fd_test(xml,rawname_slotid,FD_VOID)) 
    name=fd_get(xml,rawname_slotid,FD_VOID);
  else if (fd_test(xml,elt_name,FD_VOID)) 
    name=fd_get(xml,elt_name,FD_VOID);
  else {}
  if ((FD_SYMBOLP(name))||(FD_STRINGP(name))) {
    u8_puts(out,"</");
    output_markup_sym(out,name);
    u8_putc(out,'>');}
  return name;
}

/* Reading for evaluation */

FD_EXPORT
struct FD_XML *fd_read_fdxml(u8_input in,int bits)
{
  struct FD_XML *xml=u8_alloc(struct FD_XML), *retval;
  fd_lispenv working_env=fd_working_environment();
  fd_bind_value(xml_env_symbol,(fdtype)fdxml_module,working_env);
  fd_init_xml_node(xml,NULL,"top");
  xml->bits=bits; xml->data=working_env;
  retval=fd_walk_xml(in,fd_xmleval_contentfn,
		     handle_xmleval_pi,
		     fd_xmleval_attribfn,
		     NULL,
		     fd_xmleval_popfn,
		     xml);
  if (retval) return xml;
  else return retval;
}

static fdtype parsefdxml(fdtype input,fdtype sloppy)
{
  int flags=FD_XML_KEEP_RAW;
  struct FD_XML *retval;
  struct U8_INPUT *in, _in;
  if (flags<0) return FD_ERROR_VALUE;
  if (FD_PTR_TYPEP(input,fd_port_type)) {
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
    /* free_node(&object,0); */
    return result;}
  else return FD_ERROR_VALUE;
}

/* FDXML special forms */

static fdtype test_symbol, predicate_symbol, else_symbol, value_symbol;

static fdtype do_body(fdtype expr,fd_lispenv env);
static fdtype do_else(fdtype expr,fd_lispenv env);

/* Simple execution */

static fdtype fdxml_insert(fdtype expr,fd_lispenv env)
{
  fdtype value=fdxml_get(expr,value_symbol,env);
  u8_output out=fd_get_default_output();
  u8_printf(out,"%q",value);
  return FD_VOID;
}

/* Conditionals */

static fdtype fdxml_if(fdtype expr,fd_lispenv env)
{
  fdtype test=fdxml_get(expr,test_symbol,env);
  if (FD_FALSEP(test))
    return do_else(expr,env);
  else {
    fd_decref(test);
    return do_body(expr,env);}
}

static fdtype do_body(fdtype expr,fd_lispenv env)
{
  u8_output out=fd_get_default_output();
  fdtype body=fd_get(expr,content_slotid,FD_VOID), result=FD_VOID;
  if (FD_PAIRP(body)) {
    FD_DOLIST(elt,body) {
      fdtype value=fd_xmleval(out,elt,env);
      if (FD_ABORTP(value)) {
	fd_decref(body);
	return value;}
      else {
	fd_decref(result); result=value;}}}
  fd_decref(body);
  return result;
}

static fdtype do_else(fdtype expr,fd_lispenv env)
{
  u8_output out=fd_get_default_output();
  fdtype body=fd_get(expr,else_symbol,FD_VOID);
  fdtype result=fd_xmleval(out,body,env);
  fd_decref(body);
  return result;
}

/* Choice/Set operations */

static fdtype fdxml_try(fdtype expr,fd_lispenv env)
{
  u8_output out=fd_get_default_output();
  fdtype body=fd_get(expr,content_slotid,FD_VOID), result=FD_EMPTY_CHOICE;
  if (FD_PAIRP(body)) {
    FD_DOLIST(elt,body)
      if (FD_STRINGP(elt)) {}
      else {
	fdtype value=fd_xmleval(out,elt,env);
	if (FD_EMPTY_CHOICEP(result)) {}
	else if (FD_ABORTP(value)) {
	  fd_decref(body);
	  return value;}
	else {
	  fd_decref(body);
	  return value;}}}
  fd_decref(body);
  return result;
}

static fdtype fdxml_union(fdtype expr,fd_lispenv env)
{
  u8_output out=fd_get_default_output();
  fdtype body=fd_get(expr,content_slotid,FD_VOID), result=FD_EMPTY_CHOICE;
  if (FD_PAIRP(body)) {
    FD_DOLIST(elt,body)
      if (FD_STRINGP(elt)) {}
      else {
	fdtype value=fd_xmleval(out,elt,env);
	if (FD_EMPTY_CHOICEP(result)) {}
	else if (FD_ABORTP(value)) {
	  fd_decref(body);
	  return value;}
	else {
	  fd_decref(body);
	  FD_ADD_TO_CHOICE(result,value);}}}
  fd_decref(body);
  return result;
}

static fdtype fdxml_intersection(fdtype expr,fd_lispenv env)
{
  u8_output out=fd_get_default_output();
  fdtype body=fd_get(expr,content_slotid,FD_VOID);
  int len=0, n=0, i=0;
  fdtype _v[16], *v, result=FD_EMPTY_CHOICE;
  {FD_DOLIST(elt,body) len++;}
  if (len<16) v=_v; else v=u8_alloc_n(len,fdtype);
  if (FD_PAIRP(body)) {
    FD_DOLIST(elt,body)
      if (FD_STRINGP(elt)) {}
      else {
	fdtype value=fd_xmleval(out,elt,env);
	if ((FD_EMPTY_CHOICEP(result)) || (FD_ABORTP(value))) {
	  while (i<n) {fd_decref(v[i]); i++;}
	  if (v!=_v) u8_free(v);
	  return result;}
	else {
	  v[n++]=value;}}}
  result=fd_intersection(v,n);
  while (i<n) {fd_decref(v[i]); i++;}
  if (v!=_v) u8_free(v);
  fd_decref(body);
  return result;
}

/* Binding */

static fdtype fdxml_binding(fdtype expr,fd_lispenv env)
{
  u8_output out=fd_get_default_output();
  fdtype body=fd_get(expr,content_slotid,FD_VOID), result=FD_VOID;
  fdtype attribs=fd_get(expr,attribids,FD_VOID), table=fd_init_slotmap(NULL,0,NULL);
  fd_lispenv inner_env=fd_make_env(table,env);
  {FD_DO_CHOICES(attrib,attribs) {
    fdtype val=fdxml_get(expr,attrib,env);
    fd_bind_value(attrib,val,inner_env);
    fd_decref(val);}}
  fd_decref(attribs);
  if (FD_PAIRP(body)) {
    FD_DOLIST(elt,body)
      if (FD_STRINGP(elt))
	u8_putn(out,FD_STRDATA(elt),FD_STRLEN(elt));
      else {
	fdtype value=fd_xmleval(out,elt,inner_env);
	if (FD_ABORTP(value)) {
	  fd_recycle_environment(inner_env);
	  fd_decref(result);
	  return value;}
	else {fd_decref(result); result=value;}}}
  fd_recycle_environment(inner_env);
  fd_decref(body);
  return result;
}

/* Iteration */

static fdtype each_symbol, count_symbol, sequence_symbol;
static fdtype choice_symbol, max_symbol, min_symbol;

static fd_exception MissingAttrib=_("Missing XML attribute");

static fdtype fdxml_seq_loop(fdtype var,fdtype count_var,fdtype xpr,fd_lispenv env);
static fdtype fdxml_choice_loop(fdtype var,fdtype count_var,fdtype xpr,fd_lispenv env);
static fdtype fdxml_range_loop(fdtype var,fdtype count_var,fdtype xpr,fd_lispenv env);

static fdtype fdxml_loop(fdtype expr,fd_lispenv env)
{
  if (!(fd_test(expr,each_symbol,FD_VOID)))
    return fd_err(MissingAttrib,"fdxml:loop",NULL,each_symbol);
  else {
    fdtype each_val=fd_get(expr,each_symbol,FD_VOID);
    fdtype count_val=fd_get(expr,count_symbol,FD_VOID);
    fdtype to_bind=
      ((FD_STRINGP(each_val)) ? (fd_parse(FD_STRDATA(each_val)))
       : (each_val));
    fdtype to_count=
      ((FD_STRINGP(count_val)) ? (fd_parse(FD_STRDATA(count_val)))
       : (count_val));
    if (fd_test(expr,sequence_symbol,FD_VOID))
      return fdxml_seq_loop(to_bind,to_count,expr,env);
    else if (fd_test(expr,choice_symbol,FD_VOID))
      return fdxml_choice_loop(to_bind,to_count,expr,env);
    else if (fd_test(expr,max_symbol,FD_VOID))
      return fdxml_range_loop(to_bind,to_count,expr,env);
    else return fd_err(MissingAttrib,"fdxml:loop",_("no LOOP arg"),FD_VOID);}
}

static fdtype iter_var;

/* These are for returning binding information in the backtrace. */
static fdtype iterenv1(fdtype seq,fdtype var,fdtype val)
{
  struct FD_KEYVAL *keyvals=u8_alloc_n(2,struct FD_KEYVAL);
  keyvals[0].key=iter_var; keyvals[0].value=fd_incref(seq);
  keyvals[1].key=var; keyvals[1].value=fd_incref(val);
  return fd_init_slotmap(NULL,2,keyvals);
}
static fdtype iterenv2
  (fdtype seq, fdtype var,fdtype val,fdtype xvar,fdtype xval)
{
  struct FD_KEYVAL *keyvals=u8_alloc_n(3,struct FD_KEYVAL);
  keyvals[0].key=iter_var; keyvals[0].value=fd_incref(seq);
  keyvals[1].key=var; keyvals[1].value=fd_incref(val);
  keyvals[2].key=xvar; keyvals[2].value=fd_incref(xval);
  return fd_init_slotmap(NULL,3,keyvals);
}

static fdtype retenv1(fdtype var,fdtype val)
{
  struct FD_KEYVAL *keyvals=u8_alloc_n(1,struct FD_KEYVAL);
  keyvals[0].key=var; keyvals[0].value=fd_incref(val);
  return fd_init_slotmap(NULL,1,keyvals);
}
static fdtype retenv2(fdtype var,fdtype val,fdtype xvar,fdtype xval)
{
  struct FD_KEYVAL *keyvals=u8_alloc_n(2,struct FD_KEYVAL);
  keyvals[0].key=var; keyvals[0].value=fd_incref(val);
  keyvals[1].key=xvar; keyvals[1].value=fd_incref(xval);
  return fd_init_slotmap(NULL,2,keyvals);
}

static fdtype fdxml_seq_loop(fdtype var,fdtype count_var,fdtype xpr,fd_lispenv env)
{
  int i=0, lim;
  u8_output out=fd_get_default_output();
  fdtype seq=fdxml_get(xpr,sequence_symbol,env), *iterval=NULL;
  fdtype body=fd_get(xpr,content_slotid,FD_EMPTY_CHOICE);
  fdtype vars[2], vals[2], inner_env;
  struct FD_SCHEMAP bindings;
  struct FD_ENVIRONMENT envstruct;
  if (FD_EMPTY_CHOICEP(seq)) return FD_VOID;
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error("sequence","fdxml:loop sequence",seq);
  else lim=fd_seq_length(seq);
  if (lim==0) {
    fd_decref(seq);
    return FD_VOID;}
  FD_INIT_STACK_CONS(&bindings,fd_schemap_type);
  bindings.flags=FD_SCHEMAP_STACK_SCHEMA;
  bindings.schema=vars; bindings.values=vals; bindings.size=1;
  fd_init_rwlock(&(bindings.rwlock));
  FD_INIT_STACK_CONS(&envstruct,fd_environment_type);
  envstruct.parent=env;  
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  inner_env=(fdtype)(&envstruct); 
  vars[0]=var; vals[0]=FD_VOID;
  if (!(FD_VOIDP(count_var))) {
    vars[1]=count_var; vals[1]=FD_INT2DTYPE(0);
    bindings.size=2; iterval=&(vals[1]);} 
  while (i<lim) {
    fdtype elt=fd_seq_elt(seq,i);
    if (envstruct.copy) {
      fd_set_value(var,elt,envstruct.copy);
      if (iterval)
	fd_set_value(count_var,FD_INT2DTYPE(i),envstruct.copy);}
    else {
      vals[0]=elt;
      if (iterval) *iterval=FD_INT2DTYPE(i);}
    {FD_DOLIST(expr,body) {
      fdtype val=fd_xmleval(out,expr,&envstruct);
      if (FD_ABORTP(val)) {
	fdtype errbind;
	if (iterval) errbind=iterenv1(seq,var,elt);
	else errbind=iterenv2(seq,var,elt,count_var,FD_INT2DTYPE(i));
	fd_destroy_rwlock(&(bindings.rwlock));
	if (envstruct.copy) fd_recycle_environment(envstruct.copy);
	fd_decref(elt); fd_decref(seq);
	fd_push_error_context(":FDXMLSEQ",errbind);
	return val;}
      fd_decref(val);}}
    if (envstruct.copy) {
      fd_recycle_environment(envstruct.copy);
      envstruct.copy=NULL;}
    fd_decref(vals[0]);
    i++;}
  fd_decref(seq);
  fd_destroy_rwlock(&(bindings.rwlock));
  return FD_VOID;
}

static fdtype fdxml_choice_loop(fdtype var,fdtype count_var,fdtype xpr,fd_lispenv env)
{
  u8_output out=fd_get_default_output();
  fdtype choices=fdxml_get(xpr,choice_symbol,env);
  fdtype body=fd_get(xpr,content_slotid,FD_EMPTY_CHOICE);
  fdtype *vloc=NULL, *iloc=NULL;
  fdtype vars[2], vals[2], inner_env;
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTP(var)) return var;
  else if (FD_ABORTP(choices)) return choices;
  else if (FD_VOIDP(count_var)) {
    bindings.size=1;
    vars[0]=var; vals[0]=FD_VOID;
    vloc=&(vals[0]);}
  else {
    bindings.size=2;
    vars[0]=var; vals[0]=FD_VOID; vloc=&(vals[0]);
    vars[1]=count_var; vals[1]=FD_INT2DTYPE(0); iloc=&(vals[1]);}
  FD_INIT_STACK_CONS(&bindings,fd_schemap_type);
  bindings.flags=FD_SCHEMAP_STACK_SCHEMA;
  bindings.schema=vars; bindings.values=vals;
  fd_init_rwlock(&(bindings.rwlock));
  FD_INIT_STACK_CONS(&envstruct,fd_environment_type);
  envstruct.parent=env;
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  inner_env=(fdtype)(&envstruct);
  if (FD_EMPTY_CHOICEP(choices)) return FD_VOID;
  else if (FD_ABORTP(choices))
    return choices;
  else {
    int i=0; FD_DO_CHOICES(elt,choices) {
      fd_incref(elt);
      if (envstruct.copy) {
	fd_set_value(var,elt,envstruct.copy);
	if (iloc) fd_set_value(count_var,FD_INT2DTYPE(i),envstruct.copy);}
      else {
	*vloc=elt;
	if (iloc) *iloc=FD_INT2DTYPE(i);}
      {FD_DOLIST(expr,body) {
	fdtype val=fd_xmleval(out,expr,&envstruct);
	if (FD_ABORTP(val)) {
	  fdtype env;
	  if (iloc) env=retenv2(var,elt,count_var,FD_INT2DTYPE(i));
	  else env=retenv1(var,elt);
	  fd_decref(choices);
	  if (envstruct.copy) fd_recycle_environment(envstruct.copy);
	  fd_push_error_context(":FDXMLCHOICE",env);
	  return val;}
	fd_decref(val);}}
      if (envstruct.copy) {
	fd_recycle_environment(envstruct.copy);
	envstruct.copy=NULL;}
      fd_decref(*vloc);
      i++;}
    fd_decref(choices);
    if (envstruct.copy) fd_recycle_environment(envstruct.copy);
    return FD_VOID;}
}

static fdtype fdxml_range_loop(fdtype var,fdtype count_var,fdtype xpr,fd_lispenv env)
{
  u8_output out=fd_get_default_output(); int i=0, limit;
  fdtype limit_val=fdxml_get(xpr,max_symbol,env);
  fdtype body=fd_get(xpr,content_slotid,FD_EMPTY_CHOICE);
  fdtype vars[2], vals[2], inner_env;
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTP(var)) return var;
  else if (!(FD_FIXNUMP(limit_val)))
    return fd_type_error("fixnum","dotimes_handler",limit_val);
  else limit=FD_FIX2INT(limit_val);
  FD_INIT_STACK_CONS(&bindings,fd_schemap_type);
  bindings.flags=(FD_SCHEMAP_SORTED|FD_SCHEMAP_STACK_SCHEMA);
  bindings.schema=vars; bindings.values=vals; bindings.size=1;
  fd_init_rwlock(&(bindings.rwlock));
  FD_INIT_STACK_CONS(&envstruct,fd_environment_type);
  envstruct.parent=env;  
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  inner_env=(fdtype)(&envstruct); 
  vars[0]=var; vals[0]=FD_INT2DTYPE(0);
  while (i < limit) {
    if (envstruct.copy) 
      fd_set_value(var,FD_INT2DTYPE(i),envstruct.copy);
    else vals[0]=FD_INT2DTYPE(i);
    {FD_DOLIST(expr,body) {
      fdtype val=fd_xmleval(out,expr,&envstruct);
      if (FD_ABORTP(val)) {
	fd_push_error_context(":FXMLRANGE",iterenv1(limit_val,var,FD_INT2DTYPE(i)));
	fd_destroy_rwlock(&(bindings.rwlock));
	if (envstruct.copy) fd_recycle_environment(envstruct.copy);
	return val;}
      fd_decref(val);}}
    if (envstruct.copy) {
      fd_recycle_environment(envstruct.copy);
      envstruct.copy=NULL;}
    i++;}
  fd_destroy_rwlock(&(bindings.rwlock));
  if (envstruct.copy) fd_recycle_environment(envstruct.copy);
  return FD_VOID;
}

/* FDXML find */

static fdtype index_symbol, with_symbol, slot_symbol, value_symbol;

static fdtype fdxml_find(fdtype expr,fd_lispenv env)
{
  fdtype index_arg=fdxml_get(expr,index_symbol,env), results;
  fdtype *slotvals=u8_alloc_n(16,fdtype);
  fdtype content=fd_get(expr,content_slotid,FD_EMPTY_LIST);
  int i=0, n=0, lim=16;
  FD_DOLIST(elt,content) {
    fdtype name=fd_get(elt,elt_name,FD_VOID);
    if (FD_EQ(name,with_symbol)) {
      fdtype slotid=fdxml_get(expr,slot_symbol,env);
      fdtype slotval=fdxml_get(expr,value_symbol,env);
      if (n>=lim) {
	slotvals=u8_realloc_n(slotvals,lim*2,fdtype);
	lim=lim*2;}
      slotvals[n++]=slotid; slotvals[n++]=slotval;}}
  if (FD_VOIDP(index_arg))
    results=fd_bgfinder(n,slotvals);
  else results=fd_finder(index_arg,n,slotvals);
  while (i<n) {fd_decref(slotvals[i]); i++;}
  u8_free(slotvals);
  return results;
}

/* FDXML define */

static fdtype xmlarg_symbol, doseq_symbol, fdxml_define_body;

static fdtype fdxml_define(fdtype expr,fd_lispenv env)
{
  if (!(fd_test(expr,id_symbol,FD_VOID)))
    return fd_err(MissingAttrib,"fdxml:loop",NULL,id_symbol);
  else {
    fdtype id_arg=fd_get(expr,id_symbol,FD_VOID);
    fdtype to_bind=
      ((FD_STRINGP(id_arg)) ? (fd_parse(FD_STRDATA(id_arg)))
       : (id_arg));
    fdtype content=fd_get(expr,content_slotid,FD_VOID);
    fdtype attribs=fd_get(expr,attribids,FD_VOID);
    fdtype xml_env=fd_symeval(xml_env_symbol,env);
    fdtype arglist=FD_EMPTY_LIST;
    fdtype body=FD_EMPTY_LIST;
    fdtype sproc=FD_VOID;

    /* Construct the arglist */
    {FD_DO_CHOICES(slotid,attribs)
      if (slotid!=id_symbol) {
	fdtype v=fd_get(expr,slotid,FD_FALSE);
	fdtype pair=fd_init_pair(NULL,fd_make_list(2,slotid,v),arglist);
	arglist=pair;}}

    /* Construct the body */
    body=fd_make_list(2,quote_symbol,fd_incref(content));
    body=fd_make_list(2,xmlarg_symbol,body);
    body=fd_make_list(3,doseq_symbol,body,fd_incref(fdxml_define_body));
    body=fd_make_list(1,body);

    /* Construct the sproc */
    sproc=fd_make_sproc(u8_mkstring("XML/%s",FD_SYMBOL_NAME(to_bind)),
			arglist,body,env,1,0);

    fd_bind_value(to_bind,sproc,(fd_lispenv)xml_env);
    fd_decref(sproc); fd_decref(body); fd_decref(arglist);

    return FD_VOID;}
}

/* The init procedure */

static int xmleval_initialized=0;

FD_EXPORT void fd_init_xmleval_c()
{
  fdtype module;
  if (xmleval_initialized) return;
  xmleval_initialized=1;
  fd_init_fdscheme();
  fdxml_module=fd_make_env(fd_make_hashtable(NULL,17),NULL);
  module=fd_new_module("FDWEB",(FD_MODULE_SAFE));
  fd_idefn(module,fd_make_cprim2("PARSE-FDXML",parsefdxml,1));

  fd_defspecial((fdtype)fdxml_module,"IF",fdxml_if);
  fd_defspecial((fdtype)fdxml_module,"LOOP",fdxml_loop);
  fd_defspecial((fdtype)fdxml_module,"INSERT",fdxml_insert);
  fd_defspecial((fdtype)fdxml_module,"DEFINE",fdxml_define);
  fd_defspecial((fdtype)fdxml_module,"FIND",fdxml_find);
  fd_defspecial((fdtype)fdxml_module,"TRY",fdxml_try);
  fd_defspecial((fdtype)fdxml_module,"UNION",fdxml_union);
  fd_defspecial((fdtype)fdxml_module,"INTERSECTION",fdxml_intersection);
  fd_defspecial((fdtype)fdxml_module,"BINDING",fdxml_binding);

  xmleval_tag=fd_intern("%XMLEVAL");
  xmleval2expr_tag=fd_intern("%XMLEVAL2EXPR");
  get_symbol=fd_intern("GET");
  elt_symbol=fd_intern("ELT");
  rawname_slotid=fd_intern("%%XMLTAG");
  raw_attribs=fd_intern("%%ATTRIBS");
  raw_markup=fd_intern("%MARKUP");
  content_slotid=fd_intern("%CONTENT");
  elt_name=fd_intern("%XMLTAG");
  qname_slotid=fd_intern("%QNAME");
  attribs_slotid=fd_intern("%ATTRIBS");

  xattrib_slotid=fd_intern("XATTRIB");
  id_symbol=fd_intern("ID");
  bind_symbol=fd_intern("BIND");

  test_symbol=fd_intern("TEST");
  predicate_symbol=fd_intern("PREDICATE");
  else_symbol=fd_intern("ELSE");

  xml_env_symbol=fd_intern("%XMLENV");
  xattrib_overlay=fd_intern("%XATTRIB");
  escape_id=fd_intern("%ESCAPE");
  piescape_symbol=fd_intern("%PIESCAPE");
  xmlns_symbol=fd_intern("%XMLNS");

  iter_var=fd_intern("%ITER");
  value_symbol=fd_intern("VALUE");
  each_symbol=fd_intern("EACH");
  count_symbol=fd_intern("COUNT");
  sequence_symbol=fd_intern("SEQ");
  choice_symbol=fd_intern("CHOICE");
  max_symbol=fd_intern("MAX");
  min_symbol=fd_intern("MIN");

  index_symbol=fd_intern("INDEX");
  with_symbol=fd_intern("WITH");
  slot_symbol=fd_intern("SLOT");
  value_symbol=fd_intern("VALUE");

  pblank_symbol=fd_intern("%BLANK");
  xmlnode_symbol=fd_intern("XMLNODE");
  pnode_symbol=fd_intern("%NODE");
  xmlbody_symbol=fd_intern("XMLBODY");
  pbody_symbol=fd_intern("%BODY");
  env_symbol=fd_intern("%ENV");

  attribids=fd_intern("%ATTRIBIDS");
  
  begin_symbol=fd_intern("BEGIN");
  quote_symbol=fd_intern("QUOTE");
  xmlarg_symbol=fd_intern("%XMLARG");
  doseq_symbol=fd_intern("DOSEQ");
  fdxml_define_body=fd_make_list(2,fd_intern("XMLEVAL"),xmlarg_symbol);

  fd_register_config
    ("CACHEMARKUP",_("Whether to cache markup generated from unparsing XML"),
     fd_boolconfig_get,fd_boolconfig_set,&fd_cache_markup);

  fd_register_source_file(versionid);
}
