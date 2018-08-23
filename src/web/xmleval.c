/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/eval.h"
#include "framerd/apply.h"
#include "framerd/ports.h"
#include "framerd/fdweb.h"
#include "framerd/sequences.h"
#include "framerd/fileprims.h"

#include <libu8/u8xfiles.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>

#include <ctype.h>

fd_lexenv fdxml_module;

int fd_cache_markup = 1;

static lispval xmleval_tag, xmleval2expr_tag;
static lispval rawtag_symbol, raw_markup;
static lispval content_slotid, xmltag_symbol, qname_slotid;
static lispval attribs_slotid, attribids_slotid, if_symbol, pif_symbol;
static lispval id_symbol, bind_symbol, xml_env_symbol, xmlns_symbol;

static lispval pblank_symbol, xmlnode_symbol, xmlbody_symbol, env_symbol;
static lispval pnode_symbol, pbody_symbol, begin_symbol;
static lispval comment_symbol, cdata_symbol;

static lispval xattrib_overlay, attribids, piescape_symbol;

void *inherit_node_data(FD_XML *node)
{
  FD_XML *scan = node;
  while (scan)
    if (scan->xml_data) return scan->xml_data;
    else scan = scan->xml_parent;
  return NULL;
}

fd_lexenv read_xml_env(fd_lexenv env)
{
  lispval xmlenv = fd_symeval(xml_env_symbol,env);
  if (VOIDP(xmlenv))
    return fdxml_module;
  else if (FD_LEXENVP(xmlenv)) {
    fd_decref(xmlenv);
    return (fd_lexenv)xmlenv;}
  else {
    fd_seterr(fd_TypeError,"read_xml_env","XML environment",xmlenv);
    return NULL;}
}

/* Markup output support functions */

FD_INLINE_FCN void entify(u8_output out,u8_string value,int len)
{
  const u8_byte *scan = value, *lim = value+len;
  while (scan<lim) {
    int c = u8_sgetc(&scan);
    if (c<0) {}
    else if (c=='<') u8_puts(out,"&#60;");
    else if (c=='>') u8_puts(out,"&#62;");
    else if (c=='&') u8_puts(out,"&#38;");
    else u8_putc(out,c);}
}

static void attrib_entify_x(u8_output out,u8_string value,u8_string escape)
{
  const u8_byte *scan = value; int c;
  if (!(escape)) escape="'<>&\"";
  while ((c = u8_sgetc(&scan))>=0)
    if (strchr(escape,c)) /* (strchr("'<>&\"!@$%(){}[]",c)) */
      /* For now, we're not escaping +/-, even though some sources suggest
         it would be a good idea. */
      switch(c) {
      case '\'': u8_puts(out,"&#39;"); break;
      case '\"': u8_puts(out,"&#34;"); break;
      case '<': u8_puts(out,"&#60;"); break;
      case '>': u8_puts(out,"&#62;"); break;
      case '&': u8_puts(out,"&#38;"); break;
        /*
          case '(': u8_puts(out,"&#40;"); break;
          case ')': u8_puts(out,"&#41;"); break;
          case '[': u8_puts(out,"&#91;"); break;
          case ']': u8_puts(out,"&#93;"); break;
          case '{': u8_puts(out,"&#123;"); break;
          case '}': u8_puts(out,"&#125;"); break;
          case '@': u8_puts(out,"&#64;"); break;
          case '!': u8_puts(out,"&#33;"); break;
          case '$': u8_puts(out,"&#36;"); break;
          case '%': u8_puts(out,"&#37;"); break;
          case '-': u8_puts(out,"&#45;"); break;
          case '+': u8_puts(out,"&#43;"); break;
        */
      }
        /* u8_printf(out,"&#%d;",c); */
    else u8_putc(out,c);
}

static int output_markup_sym(u8_output out,lispval sym)
{
  if (STRINGP(sym)) return u8_puts(out,CSTRING(sym));
  else if (SYMBOLP(sym)) {
    const u8_byte *scan = SYM_NAME(sym);
    int c = u8_sgetc(&scan);
    while (c>=0) {
      int lc = u8_tolower(c);
      int retcode = u8_putc(out,lc);
      if (retcode<0) return retcode;
      else c = u8_sgetc(&scan);}
    return 1;}
  else {
    u8_printf(out,"%q",sym);
    return 1;}
}

static void start_attrib(u8_output out,lispval name)
{
  u8_putc(out,' ');
  output_markup_sym(out,name);
  u8_putc(out,'=');
}

static int output_attribval(u8_output out,
                            lispval name,lispval val,
                            fd_lexenv scheme_env,
                            fd_lexenv xml_env,
                            int colon)
{
  if ((PAIRP(val)) &&
      ((FD_EQ(FD_CAR(val),xmleval_tag)) ||
       (FD_EQ(FD_CAR(val),xmleval2expr_tag)))) {
    lispval expr = FD_CDR(val); lispval value;
    u8_string as_string;
    if (SYMBOLP(expr)) {
      value = fd_symeval(expr,scheme_env);
      if ((VOIDP(value))&&(xml_env))
        value = fd_symeval(expr,xml_env);
      if (VOIDP(value))
        value = fd_req_get(expr,VOID);}
    else value = fd_eval(expr,scheme_env);
    if (FD_ABORTED(value)) return fd_interr(value);
    else if ((VOIDP(value))&&(SYMBOLP(expr)))
      as_string = u8_strdup("");
    else if ((VOIDP(value))||(EMPTYP(value))) {
      /* This means the caller has already output then name = so there
       * better be a value */
      if (!(name)) u8_puts(out,"\"\"");
      return 0;}
    else if (FD_EQ(FD_CAR(val),xmleval2expr_tag))
      as_string = fd_lisp2string(value);
    else if (STRINGP(value))
      as_string = u8_strdup(CSTRING(value));
    else as_string = fd_lisp2string(value);
    if (name) start_attrib(out,name);
    u8_putc(out,'"');
    if (FD_EQ(FD_CAR(val),xmleval2expr_tag)) {
      if ((STRINGP(value)) && (STRLEN(value)>0) &&
          ((isdigit(CSTRING(value)[0]))||(CSTRING(value)[0]==':')))
        u8_putc(out,'\\');
    /* Don't output a preceding colon if the value would be 'self parsing' */
      else if ((FIXNUMP(value)) ||
               (FD_FLONUMP(value)) ||
               (STRINGP(value))) {}
      else u8_putc(out,':');}
    fd_attrib_entify(out,as_string);
    u8_putc(out,'"');
    fd_decref(value); u8_free(as_string);
    return 0;}
  else if (SLOTMAPP(val)) {
    lispval value = fd_xmlevalout(NULL,val,scheme_env,xml_env);
    u8_string as_string;
    if (FD_ABORTED(value)) return fd_interr(value);
    else if ((name)&&(EMPTYP(value))) return 0;
    else as_string = fd_lisp2string(value);
    if (name) start_attrib(out,name);
    u8_putc(out,'"');
    if (!(STRINGP(value))) u8_putc(out,':');
    fd_attrib_entify(out,as_string);
    u8_putc(out,'"');
    fd_decref(value); u8_free(as_string);
    return 0;}
  else if (STRINGP(val)) {
    if (name) start_attrib(out,name);
    u8_putc(out,'"');
    fd_attrib_entify(out,CSTRING(val));
    u8_putc(out,'"');
    return 1;}
  else if (OIDP(val)) {
    FD_OID addr = FD_OID_ADDR(val);
    if (name) start_attrib(out,name);
    u8_printf(out,"\"@%x/%x\"",FD_OID_HI(addr),FD_OID_LO(addr));
    return 1;}
  else {
    u8_string as_string = fd_lisp2string(val);
    if (name) start_attrib(out,name);
    u8_putc(out,'"');
    if (!((FIXNUMP(val)) || (FD_FLONUMP(val)) || (FD_BIGINTP(val))))
      u8_putc(out,':');
    fd_attrib_entify(out,as_string);
    u8_putc(out,'"');
    u8_free(as_string);
    return 1;}
}

/* Accessing xml attributes and elements. */

FD_EXPORT
lispval fd_xml_get(lispval xml,lispval slotid)
{
  lispval results = fd_get(xml,slotid,EMPTY);
  lispval content = fd_get(xml,content_slotid,VOID);
  FD_DOELTS(item,content,count)
    if ((TABLEP(item)) && (fd_test(item,xmltag_symbol,slotid))) {
      fd_incref(item); CHOICE_ADD(results,item);}
  fd_decref(content);
  return results;
}

static lispval get_markup_string(lispval xml,
                                fd_lexenv scheme_env,
                                fd_lexenv xml_env)
{
  U8_OUTPUT out; int cache_result = fd_cache_markup;
  lispval cached, attribs = EMPTY, attribids = EMPTY;
  if (fd_cache_markup) {
    cached = fd_get(xml,raw_markup,VOID);
    if (!(VOIDP(cached))) return cached;}
  U8_INIT_OUTPUT(&out,32);
  if (fd_test(xml,rawtag_symbol,VOID)) {
    lispval rawname = fd_get(xml,rawtag_symbol,VOID);
    if (FD_ABORTED(rawname)) return rawname;
    else if (rawname == pblank_symbol) {
      u8_free(out.u8_outbuf);
      return rawname;}
    else if (SYMBOLP(rawname)) {
      u8_string pname = SYM_NAME(rawname);
      u8_puts(&out,pname);}
    else if (!(STRINGP(rawname))) {
      fd_decref(rawname); u8_free(out.u8_outbuf);
      return fd_type_error("XML node","get_markup_string",xml);}
    else {
      u8_putn(&out,CSTRING(rawname),STRLEN(rawname));
      fd_decref(rawname);}}
  else if (fd_test(xml,xmltag_symbol,VOID)) {
    lispval name = fd_get(xml,xmltag_symbol,VOID);
    if (name == pblank_symbol) {
      u8_free(out.u8_outbuf);
      return name;}
    else output_markup_sym(&out,name);
    fd_decref(name);}
  else return fd_type_error("XML node","get_markup_string",xml);
  {
    lispval xmlns = fd_get(xml,xmlns_symbol,VOID);
    if (!(VOIDP(xmlns))) {
      DO_CHOICES(nspec,xmlns) {
        if (STRINGP(nspec)) {
          u8_printf(&out," xmlns=\"%s\"",CSTRING(nspec));}
        else if ((PAIRP(nspec))&&
                 (STRINGP(FD_CAR(nspec)))&&
                 (STRINGP(FD_CDR(nspec)))) {
          u8_printf(&out," xmlns:%s=\"%s\"",
                    CSTRING(FD_CAR(nspec)),
                    CSTRING(FD_CDR(nspec)));}
        else {}}}
    fd_decref(xmlns);}
  attribs = fd_get(xml,attribs_slotid,EMPTY);
  if (EMPTYP(attribs))
    attribids = fd_get(xml,attribids_slotid,EMPTY);
  if (!(EMPTYP(attribs))) {
    DO_CHOICES(attrib,attribs) {
      if (!((VECTORP(attrib))&&
            (VEC_LEN(attrib)>=2)&&
            (STRINGP(VEC_REF(attrib,0))))) {
        FD_STOP_DO_CHOICES;
        return fd_type_error("attrib","get_markup_string",attrib);}
      else {
        lispval name = VEC_REF(attrib,0);
        lispval value = VEC_REF(attrib,2);
        if (PAIRP(value)) {
          if (cache_result)
            cache_result = output_attribval
              (&out,name,value,scheme_env,xml_env,1);
          else output_attribval
                 (&out,name,value,scheme_env,xml_env,1);}
        else if ((SYMBOLP(name))||(STRINGP(name))) {
          start_attrib(&out,name); u8_putc(&out,'"');
          if (STRINGP(value))
            fd_attrib_entify(&out,CSTRING(value));
          else if (FIXNUMP(value))
            u8_printf(&out,"\"%lld\"",FIX2INT(value));
          else if (cache_result)
            cache_result = output_attribval
              (&out,FD_NULL,value,scheme_env,xml_env,1);
          else output_attribval
                 (&out,FD_NULL,value,scheme_env,xml_env,1);
          u8_putc(&out,'"');}}}
    fd_decref(attribs);}
  else if (!(EMPTYP(attribids))) {
    int i = 0, n; lispval *data, buf[1];
    lispval to_free = VOID;
    if (VECTORP(attribids)) {
      n = VEC_LEN(attribids);
      data = VEC_DATA(attribids);}
    else if (CHOICEP(attribids)) {
      n = FD_CHOICE_SIZE(attribids);
      data = (lispval *)FD_CHOICE_DATA(attribids);}
    else if (PRECHOICEP(attribids)) {
      to_free = fd_make_simple_choice(attribids);
      data = (lispval *)FD_CHOICE_DATA(to_free);
      n = FD_CHOICE_SIZE(to_free);}
    else {buf[0]=attribids; data = buf; n = 1;}
    while (i<n) {
      lispval attribid = data[i++];
      lispval value = fd_get(xml,attribid,VOID);
      if (!((VOIDP(value))||(EMPTYP(value)))) {
        if (PAIRP(value)) {
          if (cache_result)
            cache_result = output_attribval
              (&out,attribid,value,scheme_env,xml_env,1);
          else output_attribval
                 (&out,attribid,value,scheme_env,xml_env,1);}
        else {
          u8_putc(&out,' ');
          output_markup_sym(&out,attribid);
          u8_putc(&out,'=');
          if (STRINGP(value)) {
            if (strchr(CSTRING(value),'"')) {
              u8_putc(&out,'\'');
              attrib_entify_x(&out,CSTRING(value),"'<>&");
              u8_putc(&out,'\'');}
            else if (strchr(CSTRING(value),'\'')) {
              u8_putc(&out,'"');
              attrib_entify_x(&out,CSTRING(value),"<>\"&");
              u8_putc(&out,'"');}
            else {
              u8_putc(&out,'"');
              attrib_entify_x(&out,CSTRING(value),NULL);
              u8_putc(&out,'"');}}
          else if (FIXNUMP(value))
            u8_printf(&out,"\"%lld\"",FIX2INT(value));
          else if (cache_result)
            cache_result = output_attribval
              (&out,FD_NULL,value,scheme_env,xml_env,1);
          else output_attribval
                 (&out,FD_NULL,value,scheme_env,xml_env,1);}
        fd_decref(value);}}
    fd_decref(to_free);
    fd_decref(attribids);}
  else {}
  cached = fd_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
  if (cache_result) fd_store(xml,raw_markup,cached);
  return cached;
}

static int test_if(lispval xml,fd_lexenv scheme_env,fd_lexenv xml_env)
{
  lispval test = fd_get(xml,pif_symbol,VOID);
  if (VOIDP(test)) return 1;
  if (SYMBOLP(test)) {
    lispval val = fd_symeval(test,scheme_env);
    if (VOIDP(val)) val = fd_req_get(test,VOID);
    if ((VOIDP(val))||(FALSEP(val))) return 0;
    else {fd_decref(val); return 1;}}
  else if (PAIRP(test)) {
    lispval value = fd_eval(test,scheme_env);
    fd_decref(test);
    if (FD_ABORTED(value)) return fd_interr(value);
    else if (VOIDP(value)) return 0;
    else if ((FALSEP(value))||(EMPTYP(value)))
      return 0;
    else return 1;}
  else {
    fd_decref(test);
    return 1;}
}

FD_EXPORT
lispval fd_xmlout(u8_output out,lispval xml,
                 fd_lexenv scheme_env,fd_lexenv xml_env)
{
  if (STRINGP(xml)) {}
  else if (PAIRP(xml)) {}
  else if ((CHOICEP(xml))||(PRECHOICEP(xml))) {
    lispval results = EMPTY;
    DO_CHOICES(e,xml) {
      lispval r = fd_xmlout(out,e,scheme_env,xml_env);
      if (!(VOIDP(r))) {CHOICE_ADD(results,r);}}
    return results;}
  else if (TABLEP(xml))
    if (fd_test(xml,xmltag_symbol,comment_symbol)) {
      lispval content = fd_get(xml,content_slotid,VOID);
      if (!(PAIRP(content))) {
        fd_decref(content);
        return FD_FALSE;}
      u8_puts(out,"<!--");
      {FD_DOELTS(elt,content,count) {
          if (STRINGP(elt)) entify(out,CSTRING(elt),STRLEN(elt));}}
      u8_puts(out,"-->");
      fd_decref(content);
      return VOID;}
    else if (fd_test(xml,xmltag_symbol,cdata_symbol)) {
      lispval content = fd_get(xml,content_slotid,VOID);
      if (!(PAIRP(content))) {
        fd_decref(content);
        return FD_FALSE;}
      u8_puts(out,"<![CDATA[");
      {FD_DOELTS(elt,content,count) {
          if (STRINGP(elt)) u8_putn(out,CSTRING(elt),STRLEN(elt));}}
      u8_puts(out,"]]>");
      fd_decref(content);
      return VOID;}
    else if ((fd_test(xml,pif_symbol,VOID))&&
             (!(test_if(xml,scheme_env,xml_env)))) {
      /* This node is excluded */
      return VOID;}
    else {
      lispval markup = get_markup_string(xml,scheme_env,xml_env);
      lispval content = fd_get(xml,content_slotid,VOID);
      if (FD_ABORTED(markup)) {
        fd_decref(content); return markup;}
      else if (FD_ABORTED(content)) {
        fd_decref(markup); return content;}
      else if ((VOIDP(content)) ||
               (EMPTYP(content)) ||
               (FALSEP(content))) {
        if (STRINGP(markup))
          u8_printf(out,"<%s/>",CSTRING(markup));
        fd_decref(markup);
        return VOID;}
      if (STRINGP(markup))
        u8_printf(out,"<%s>",CSTRING(markup));
      if (PAIRP(content)) {
        FD_DOELTS(item,content,count) {
          lispval result = VOID;
          if (STRINGP(item))
            u8_putn(out,CSTRING(item),STRLEN(item));
          else result = fd_xmlevalout(out,item,scheme_env,xml_env);
          if (VOIDP(result)) {}
          else if (FD_ABORTED(result)) {
            fd_decref(content);
            return result;}
          else if ((TABLEP(result)) &&
                   (fd_test(result,rawtag_symbol,VOID))) {
            lispval tmp = fd_xmlout(out,result,scheme_env,xml_env);
            fd_decref(tmp);}
          else fd_lisp2xml(out,result,scheme_env);
          fd_decref(result);}}
      else if (STRINGP(content)) {
        u8_putn(out,CSTRING(content),STRLEN(content));}
      else {}
      if (STRINGP(markup)) {
        u8_string mstring = CSTRING(markup);
        u8_string atspace = strchr(mstring,' ');
        u8_puts(out,"</");
        if (atspace) u8_putn(out,mstring,atspace-mstring);
        else u8_puts(out,mstring);
        u8_putc(out,'>');}
      fd_decref(markup); fd_decref(content);
      return VOID;}
  else return fd_type_error("XML node","fd_xmlout",xml);
  return VOID;
}

FD_EXPORT
lispval fd_unparse_xml(u8_output out,lispval xml,fd_lexenv env)
{
  return fd_xmlout(out,xml,env,read_xml_env(env));
}

/* Handling dynamic elements */

static lispval get_xml_handler(lispval xml,fd_lexenv xml_env)
{
  if (!(xml_env)) return VOID;
  else {
    lispval qname = fd_get(xml,qname_slotid,VOID);
    lispval value = VOID;
    DO_CHOICES(q,qname) {
      if (STRINGP(q)) {
        lispval symbol = fd_probe_symbol(CSTRING(q),STRLEN(q));
        if (SYMBOLP(symbol)) value = fd_symeval(symbol,xml_env);
        if (!(VOIDP(value))) {
          fd_decref(qname);
          FD_STOP_DO_CHOICES;
          return value;}}}
    fd_decref(qname); {
      lispval name = fd_get(xml,xmltag_symbol,VOID);
      if (SYMBOLP(name)) value = fd_symeval(name,xml_env);
      else {}
      return value;}}
}

struct XMLAPPLY { lispval xml; fd_lexenv env;};

FD_EXPORT lispval fdxml_get(lispval xml,lispval sym,fd_lexenv env)
{
  if ((sym == xmlnode_symbol) || (sym == pnode_symbol)) return fd_incref(xml);
  else if (sym == env_symbol) return (lispval) fd_copy_env(env);
  else if ((sym == xmlbody_symbol) || (sym == pbody_symbol)) {
    lispval content = fd_get(xml,content_slotid,VOID);
    if (VOIDP(content)) return EMPTY;
    else {
      struct FD_KEYVAL *kv = u8_alloc_n(2,struct FD_KEYVAL);
      /* This generates a "blank node" which generates its content
         without any container. */
      kv[0].kv_key = rawtag_symbol; kv[0].kv_val = pblank_symbol;
      kv[1].kv_key = content_slotid; kv[1].kv_val = content;
      return fd_init_slotmap(NULL,2,kv);}}
  else {
    lispval values = fd_get(xml,sym,VOID);
    if (VOIDP(values)) return VOID;
    else if (CHOICEP(values)) {
      lispval results = EMPTY;
      DO_CHOICES(value,values)
        if (PAIRP(value))
          if  ((FD_EQ(FD_CAR(value),xmleval_tag)) ||
               (FD_EQ(FD_CAR(value),xmleval2expr_tag))) {
            lispval result = fd_eval(FD_CDR(value),env);
            if (FD_ABORTED(result)) {
              fd_decref(results);
              FD_STOP_DO_CHOICES;
              return result;}
            CHOICE_ADD(results,result);}
          else {
            fd_incref(value);
            CHOICE_ADD(results,value);}
        else if (TABLEP(value)) {
          lispval result = fd_xmleval(NULL,value,env);
          if (FD_ABORTED(result)) {
            fd_decref(results);
            FD_STOP_DO_CHOICES;
            return result;}
          CHOICE_ADD(results,result);}
        else {
          fd_incref(value);
          CHOICE_ADD(results,value);}
      fd_decref(values);
      return results;}
    else if (PAIRP(values))
      if ((FD_EQ(FD_CAR(values),xmleval_tag)) ||
          (FD_EQ(FD_CAR(values),xmleval2expr_tag))) {
        lispval result = fd_eval(FD_CDR(values),env);
        fd_decref(values);
        return result;}
      else return values;
    else if (TABLEP(values))
      return fd_xmleval(NULL,values,env);
    else if (QCHOICEP(values)) {
      lispval result = FD_XQCHOICE(values)->qchoiceval;
      fd_incref(result); fd_decref(values);
      return result;}
    else return values;}
}

static lispval xmlgetarg(void *vcxt,lispval sym)
{
  struct XMLAPPLY *cxt = (struct XMLAPPLY *)vcxt;
  return fdxml_get(cxt->xml,sym,cxt->env);
}

static lispval xmlapply(u8_output out,lispval fn,lispval xml,
                       fd_lexenv scheme_env,fd_lexenv xml_env)
{
  struct XMLAPPLY cxt; cxt.xml = xml; cxt.env = scheme_env;
  lispval bind = fd_get(xml,id_symbol,VOID), result = VOID;
  if (TYPEP(fn,fd_evalfn_type)) {
    struct FD_EVALFN *sf=
      fd_consptr(fd_evalfn,fn,fd_evalfn_type);
    result = sf->evalfn_handler(xml,scheme_env,fd_stackptr);}
  else if (FD_LAMBDAP(fn))
    result = fd_xapply_lambda((struct FD_LAMBDA *)fn,&cxt,xmlgetarg);
  else {
    fd_decref(bind);
    return fd_type_error("function","xmlapply",fn);}

  result = fd_finish_call(result);

  if (FD_ABORTED(result))
    return result;
  else if (VOIDP(bind)) return result;
  else if (SYMBOLP(bind)) {
    fd_bind_value(bind,result,scheme_env);
    fd_decref(result);
    result = VOID;}
  else if (STRINGP(bind)) {
    lispval sym = fd_parse(CSTRING(bind));
    if (SYMBOLP(sym)) {
      fd_bind_value(sym,result,scheme_env);
      fd_decref(result);
      result = VOID;}
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

static lispval get_symbol, elt_symbol, quote_symbol;

static lispval extract_var(u8_string start,u8_string end)
{
  u8_byte _buf[128], *buf; lispval result;
  if ((end-start)<127) buf=_buf;
  else buf = u8_malloc((end-start)+1);
  strncpy(buf,start,end-start); buf[end-start]='\0';
  result = fd_parse(buf);
  if (buf!=_buf) u8_free(buf);
  return result;
}

static lispval parse_infix(u8_string start)
{
  u8_string split;
  if ((split = (strchr(start,'.')))) {
    if (split == start) return fd_parse(start);
   /* Record form x.y ==> (get x 'y) */
    return fd_make_list(3,get_symbol,extract_var(start,split),
                        fd_make_list(2,quote_symbol,fd_parse(split+1)));}
  else if ((split = (strchr(start,'#')))) {
    if (split == start) return fd_parse(start);
    /* Call form x#y ==> (y x) */
    return fd_make_list(2,fd_parse(split+1),extract_var(start,split));}
  else if ((split = (strchr(start,'[')))) {
    if (split == start) return fd_parse(start);
    /* Vector form x[y] ==> (elt x y) */
    return fd_make_list(3,elt_symbol,extract_var(start,split),
                        fd_parse(split+1));}
  else return fd_parse(start);
}

static lispval xmlevalify(u8_string encoded)
{
  lispval result = VOID;
  u8_string string=
    ((strchr(encoded,'&') == NULL)?(encoded):
     (fd_deentify(encoded,NULL)));
  if (string[0]==':')
    if (string[1]=='$')
      result = fd_conspair(xmleval2expr_tag,parse_infix(string+2));
    else result = fd_parse(string+1);
  else if (string[0]=='$') {
    u8_string start = string+1;
    int c = u8_sgetc(&start);
    if (u8_isalpha(c))
      result = fd_conspair(xmleval_tag,parse_infix(string+1));
    else result = fd_conspair(xmleval_tag,fd_parse(string+1));}
  else if (string[0]=='\\') result = lispval_string(string+1);
  else result = lispval_string(string);
  if (string!=encoded) u8_free(string);
  return result;
}

static lispval xmldtypify(u8_string string)
{
  if (string[0]==':') return fd_parse(string+1);
  else if (string[0]=='\\') return lispval_string(string+1);
  else return lispval_string(string);
}

static lispval parse_attribname(u8_string string)
{
  lispval parsed = fd_parse(string);
  if ((SYMBOLP(parsed))||(OIDP(parsed))) return parsed;
  else {
    u8_log(LOG_WARNING,"BadAttribName",
           "Trouble parsing attribute name %s",string);
    fd_decref(parsed);
    return fd_intern(string);}
}

FD_EXPORT int fd_xmleval_attribfn
   (FD_XML *xml,u8_string name,u8_string val,int quote)
{
  u8_string namespace, attrib_name = fd_xmlns_lookup(xml,name,&namespace);
  lispval slotid = parse_attribname(name);
  lispval slotval = ((!(val))?(slotid):
                  ((val[0]=='\0')||(val[0]=='#')||
                   (slotid == if_symbol))?
                  (lispval_string(val)):
                  (quote>0) ? (xmlevalify(val)) :
                  (xmldtypify(val)));
  lispval attrib_entry = VOID;
  if ((FD_ABORTED(slotval))||(VOIDP(slotval))||
      (FD_EOFP(slotval))||(FD_EODP(slotval))||
      (FD_EOXP(slotval)))
    slotval = lispval_string(val);
  if (EMPTYP(xml->xml_attribs)) fd_init_xml_attribs(xml);
  xml->xml_bits = xml->xml_bits|FD_XML_HASDATA;
  if (slotid == if_symbol) {
    u8_string sv = CSTRING(slotval);
    lispval sval = ((sv[0]=='$')?(fd_parse(sv+1)):(fd_parse(sv)));
    fd_add(xml->xml_attribs,pif_symbol,sval);
    fd_decref(sval);}
  fd_add(xml->xml_attribs,slotid,slotval);
  if (namespace) {
    fd_add(xml->xml_attribs,parse_attribname(attrib_name),slotval);
    attrib_entry=
      fd_make_nvector(3,lispval_string(name),
                      fd_make_qid(attrib_name,namespace),
                      fd_incref(slotval));}
  else attrib_entry=
         fd_make_nvector(3,lispval_string(name),FD_FALSE,fd_incref(slotval));
  fd_add(xml->xml_attribs,attribids,slotid);
  fd_add(xml->xml_attribs,attribs_slotid,attrib_entry);
  fd_decref(attrib_entry); fd_decref(slotval);
  return 1;
}

static lispval xattrib_slotid;

static int check_symbol_entity(const u8_byte *start,const u8_byte *end);

FD_EXPORT
void fd_xmleval_contentfn(FD_XML *node,u8_string s,int len)
{
  if (len==0) {}
  else if ((strchr(s,'&')) == NULL)
    fd_add_content(node,fd_substring(s,s+len));
  else {
    const u8_byte *start = s, *scan = strchr(s,'&'), *lim = s+len;
    while ((scan)&&(scan<lim)) {
      /* Definitely a character entity */
      if (scan[1]=='#') scan = strchr(scan+1,'&');
      else {
        u8_byte *semi = strchr(scan,';');
        /* A symbol entity is just included as a symbol, but we
           don't want to override any valid character entities,
           so we check. */
        if ((semi)&&((semi-scan)<40)&&
            (check_symbol_entity(scan+1,semi+1))) {
          /* Make a different kind of node to be evaluated */
          struct U8_OUTPUT out; u8_byte buf[64];
          lispval symbol = VOID;
          const u8_byte *as = scan+1, *end = semi;
          U8_INIT_FIXED_OUTPUT(&out,64,buf);
          while (as<end) {
            int c = u8_sgetc(&as); c = u8_toupper(c);
            u8_putc(&out,c);}
          symbol = fd_intern(out.u8_outbuf);
          if (start<scan)
            fd_add_content(node,fd_substring(start,scan));
          fd_add_content(node,symbol);
          start = semi+1; scan = strchr(start,'&');}
        else if (semi)
          scan = strchr(semi+1,'&');
        else scan = strchr(scan+1,'&');}}
    if (start<(s+len))
      fd_add_content(node,fd_substring(start,lim));}
}

static int check_symbol_entity(const u8_byte *start,const u8_byte *end)
{
  const u8_byte *scan = start, *chref_end = end;
  if (((end-start)<=16)&&(u8_parse_entity(scan,&chref_end)>=0))
    return 0;
  else while (scan<end) {
    int c = u8_sgetc(&scan);
    if (u8_isspace(c)) return 0;
    else if ((u8_isalnum(c))||(u8_ispunct(c))) continue;
    else return 0;}
  return 1;
}

FD_EXPORT
FD_XML *fd_xmleval_popfn(FD_XML *node)
{
  /* Get your content */
  if (EMPTYP(node->xml_attribs)) fd_init_xml_attribs(node);
  if (PAIRP(node->xml_head)) {
    fd_add(node->xml_attribs,content_slotid,node->xml_head);}
  if (node->xml_parent == NULL) return NULL;
  else {
    lispval data = (lispval)(inherit_node_data(node));
    lispval cutaway = (((data == FD_NULL)||(FD_IMMEDIATEP(data)))?
                    (xattrib_slotid):
                    (fd_get(data,xattrib_overlay,xattrib_slotid)));
    lispval xid = fd_get(node->xml_attribs,cutaway,VOID);

    /* Check if you go on the parent's attribs or in its body. */
    if (VOIDP(xid)) {
      fd_add_content(node->xml_parent,node->xml_attribs);
      node->xml_attribs = EMPTY;}
    else if (STRINGP(xid)) {
      lispval slotid = fd_parse(FD_STRING_DATA(xid));
      fd_add(node->xml_parent->xml_attribs,slotid,node->xml_attribs);
      fd_decref(node->xml_attribs);
      node->xml_attribs = EMPTY;
      fd_decref(xid);}
    else {
      fd_add(node->xml_parent->xml_attribs,xid,node->xml_attribs);
      fd_decref(node->xml_attribs);
      node->xml_attribs = EMPTY;}
    return node->xml_parent;}
}
/* Handling the FDXML PI */

static u8_string get_pi_string(u8_string start)
{
  u8_string end = NULL;
  int c = *start;
  if ((c=='\'') || (c=='"')) {
    start++; end = strchr(start,c);}
  else {
    const u8_byte *last = start, *scan = start;
    while (c>0) {
      c = u8_sgetc(&scan);
      if ((c<0) || (u8_isspace(c))) break;
      else last = scan;}
    end = last;}
  if (end)
    return u8_slice(start,end);
  else return NULL;
}

static fd_lexenv get_xml_env(FD_XML *xml)
{
  return (fd_lexenv)fd_symeval(xml_env_symbol,(fd_lexenv)(xml->xml_data));
}

static void set_xml_env(FD_XML *xml,fd_lexenv newenv)
{
  fd_assign_value(xml_env_symbol,(lispval)newenv,(fd_lexenv)(xml->xml_data));
}

static int test_piescape(FD_XML *xml,u8_string content,int len)
{
  if ((strncmp(content,"?fdeval ",7)==0)) return 7;
  else {
    lispval piescape = fd_get((lispval)(inherit_node_data(xml)),
                           piescape_symbol,VOID);
    if (VOIDP(piescape)) {
      if (strncmp(content,"?=",2)==0) return 2;
      else return 0;}
    else {
      u8_string piend = strchr(content,' ');
      int pielen = ((piend)?((piend-content)-1):(0));
      DO_CHOICES(pie,piescape)
        if ((STRINGP(pie)) && (STRLEN(pie)==0)) {
          if (strncmp(content,"? ",2)==0) return 2;
          else if (strncmp(content,"?(",2)==0) return 1;}
        else if ((STRINGP(pie)) && (STRLEN(pie) == pielen)) {
          if (strncmp(content+1,CSTRING(pie),STRLEN(pie))==0) {
            FD_STOP_DO_CHOICES;
            return 1+STRLEN(pie);}}
      return 0;}}
}

static FD_XML *handle_fdxml_pi
  (u8_input in,FD_XML *xml,u8_string content,int len)
{
  fd_lexenv env = (fd_lexenv)(xml->xml_data), xml_env = NULL;
  if (strncmp(content,"?fdxml ",6)==0) {
    u8_byte *copy = (u8_byte *)u8_strdup(content);
    u8_byte *scan = copy; u8_string attribs[16];
    int i = 0, n_attribs = fd_parse_xmltag(&scan,copy+len,attribs,16,0);
    while (i<n_attribs)
      if ((strncmp(attribs[i],"load=",5))==0) {
        u8_string arg = get_pi_string(attribs[i]+5);
        u8_string filename = fd_get_component(arg);
        if (fd_load_latest(filename,env,NULL)<0) {
          u8_free(arg); u8_free(filename);
          return NULL;}
        else xml_env = get_xml_env(xml);
        u8_free(arg); u8_free(filename);
        if (TABLEP(env->env_exports)) {
          fd_lexenv new_xml_env=
            fd_make_export_env(env->env_exports,xml_env);
          set_xml_env(xml,new_xml_env);
          fd_decref((lispval)new_xml_env);}
        else {
          fd_lexenv new_xml_env=
            fd_make_export_env(env->env_bindings,xml_env);
          set_xml_env(xml,new_xml_env);
          fd_decref((lispval)new_xml_env);}
        if (xml_env) fd_decref((lispval)xml_env);
        i++;}
      else if ((strncmp(attribs[i],"config=",7))==0) {
        u8_string arg = get_pi_string(attribs[i]+7); i++;
        if (strchr(arg,'='))
          fd_config_assignment(arg);
        else {
          u8_string filename = fd_get_component(arg);
          int retval = fd_load_config(filename);
          if (retval<0) {
            u8_condition c = NULL; u8_context cxt = NULL;
            u8_string details = NULL;
            lispval irritant = VOID;
            if (fd_poperr(&c,&cxt,&details,&irritant)) {
              if ((VOIDP(irritant)) && (details == NULL) && (cxt == NULL))
                u8_log(LOG_WARN,"FDXML_CONFIG",
                       _("In config '%s' %m"),filename,c);
              else if ((VOIDP(irritant)) && (details == NULL))
                u8_log(LOG_WARN,"FDXML_CONFIG",
                       _("In config '%s' %m@%s"),filename,c,cxt);
              else if (VOIDP(irritant))
                u8_log(LOG_WARN,"FDXML_CONFIG",
                       _("In config '%s' [%m@%s] %s"),filename,c,cxt,details);
              else u8_log(LOG_WARN,"FDXML_CONFIG",
                          _("In config '%s' [%m@%s] %s %q"),
                          filename,c,cxt,details,irritant);
              if (details) u8_free(details);
              fd_decref(irritant);}
            else u8_log(LOG_WARN,"FDXML_CONFIG",
                        _("In config '%s', unknown error"),filename);}}}
      else if ((strncmp(attribs[i],"module=",7))==0) {
        u8_string arg = get_pi_string(attribs[i]+7);
        lispval module_name = fd_parse(arg);
        lispval module = fd_find_module(module_name,0,1);
        fd_lexenv xml_env = get_xml_env(xml);
        u8_free(arg); fd_decref(module_name);
        if ((FD_LEXENVP(module)) &&
            (TABLEP(((fd_lexenv)module)->env_exports))) {
          lispval exports = ((fd_lexenv)module)->env_exports;
          fd_lexenv new_xml_env = fd_make_export_env(exports,xml_env);
          set_xml_env(xml,new_xml_env);
          fd_decref((lispval)new_xml_env);}
        else if (TABLEP(module)) {
          fd_lexenv new_xml_env = fd_make_export_env(module,xml_env);
          set_xml_env(xml,new_xml_env);
          fd_decref((lispval)new_xml_env);}
        if (xml_env) fd_decref((lispval)xml_env);
        i++;}
      else if ((strncmp(attribs[i],"scheme_load=",12))==0) {
        u8_string arg = get_pi_string(attribs[i]+12);
        u8_string filename = fd_get_component(arg);
        fd_lexenv env = (fd_lexenv)(xml->xml_data);
        if (fd_load_latest(filename,env,NULL)<0) {
          u8_free(arg); u8_free(filename);
          return NULL;}
        u8_free(arg); u8_free(filename);
        i++;}
      else if ((strncmp(attribs[i],"scheme_module=",14))==0) {
        u8_string arg = get_pi_string(attribs[i]+14);
        lispval module_name = fd_parse(arg);
        lispval module = fd_find_module(module_name,0,1);
        fd_lexenv scheme_env = (fd_lexenv)(xml->xml_data);
        u8_free(arg); fd_decref(module_name);
        if ((FD_LEXENVP(module)) &&
            (TABLEP(((fd_lexenv)module)->env_exports))) {
          lispval exports = ((fd_lexenv)module)->env_exports;
          scheme_env->env_parent = fd_make_export_env(exports,scheme_env->env_parent);}
        else if (TABLEP(module)) {
          scheme_env->env_parent = fd_make_export_env(module,scheme_env->env_parent);}
        i++;}
      else if ((strncmp(attribs[i],"piescape=",9))==0) {
        lispval arg = fd_lispstring(get_pi_string(attribs[i]+9));
        fd_lexenv xml_env = get_xml_env(xml);
        lispval cur = fd_symeval(piescape_symbol,xml_env);
        if (VOIDP(cur))
          fd_bind_value(piescape_symbol,arg,xml_env);
        else {
          CHOICE_ADD(cur,arg);
          fd_assign_value(piescape_symbol,arg,xml_env);}
        fd_decref(arg);
        if (xml_env) fd_decref((lispval)xml_env);
        i++;}
      else if ((strncmp(attribs[i],"xattrib=",8))==0) {
        lispval arg = fd_lispstring(get_pi_string(attribs[i]+7));
        fd_lexenv xml_env = get_xml_env(xml);
        fd_bind_value(xattrib_overlay,arg,xml_env);
        fd_decref(arg);
        if (xml_env) fd_decref((lispval)xml_env);
        i++;}
      else i++;
    u8_free(copy);
    return xml;}
  else {
    int pioff = ((strncmp(content,"?eval ",6)==0)?(6):
               (test_piescape(xml,content,len)));
    u8_string xcontent = NULL; int free_xcontent = 0;
    if (pioff<=0) {
      u8_string restored = u8_string_append("<",content,">",NULL);
      fd_add_content(xml,fd_init_string(NULL,len+2,restored));
      return xml;}
    else if ((len>pioff)&&(content[len-1]=='?')) len--;
    else {}
    if (strchr(content,'&')) {
      xcontent = fd_deentify(content+pioff,content+len);
      len = u8_strlen(xcontent);
      free_xcontent = 1;}
    else {
      xcontent = content+pioff;
      len = len-pioff;}
    { struct U8_INPUT in;
      lispval insert = fd_conspair(begin_symbol,NIL);
      lispval *tail = &(FD_CDR(insert)), expr = VOID;
      U8_INIT_STRING_INPUT(&in,len,xcontent);
      expr = fd_parse_expr(&in);
      while (1) {
        if (FD_ABORTED(expr)) {
          fd_decref(insert);
          return NULL;}
        else if ((FD_EOFP(expr)) || (FD_EOXP(expr))) break;
        else {
          lispval new_cons = fd_conspair(expr,NIL);
          *tail = new_cons; tail = &(FD_CDR(new_cons));}
        expr = fd_parse_expr(&in);}
      fd_add_content(xml,insert);}
    if (free_xcontent) u8_free(xcontent);
    return xml;}
}

static FD_XML *handle_eval_pi(u8_input in,FD_XML *xml,u8_string content,int len)
{
  int pioff = ((strncmp(content,"?eval ",6)==0)?(6):
             (strncmp(content,"?=",2)==0)?(2):
             (0));
  u8_string xcontent = NULL; int free_xcontent = 0;
  if (pioff<=0) {
    u8_string restored = u8_string_append("<",content,">",NULL);
    fd_add_content(xml,fd_init_string(NULL,len+2,restored));
    return xml;}
  else if ((len>pioff)&&(content[len-1]=='?')) len--;
  else {}
  if (strchr(content,'&')) {
    xcontent = fd_deentify(content+pioff,content+len);
    len = u8_strlen(xcontent);
    free_xcontent = 1;}
  else {
    xcontent = content+pioff;
    len = len-pioff;}
  { struct U8_INPUT in;
    lispval insert = fd_conspair(begin_symbol,NIL);
    lispval *tail = &(FD_CDR(insert)), expr = VOID;
    U8_INIT_STRING_INPUT(&in,len,xcontent);
    expr = fd_parse_expr(&in);
    while (1) {
      if (FD_ABORTED(expr)) {
        fd_decref(insert);
        return NULL;}
      else if ((FD_EOFP(expr)) || (FD_EOXP(expr))) break;
      else {
        lispval new_cons = fd_conspair(expr,NIL);
        *tail = new_cons; tail = &(FD_CDR(new_cons));}
      expr = fd_parse_expr(&in);}
    fd_add_content(xml,insert);}
  if (free_xcontent) u8_free(xcontent);
  return xml;
}

/* The eval function itself */

FD_EXPORT
lispval fd_xmlevalout(u8_output out,lispval xml,
                     fd_lexenv scheme_env,fd_lexenv xml_env)
{
  lispval result = VOID;
  if ((PAIRP(xml)) &&
      ((STRINGP(FD_CAR(xml))) || (TABLEP(FD_CAR(xml))))) {
    /* This is the case where it's a node list */
    lispval value = VOID;
    FD_DOELTS(elt,xml,count) {
      if (STRINGP(elt)) u8_puts(out,CSTRING(elt));
      else {
        fd_decref(value);
        value = fd_xmlevalout(out,elt,scheme_env,xml_env);
        if (FD_ABORTED(value)) return value;}}
    return value;}
  if (FD_NEED_EVALP(xml)) {
    lispval result;
    if (SYMBOLP(xml)) {
      /* We look up symbols in both the XML env (first) and
         the Scheme env (second), and the current request (third). */
      lispval val = VOID;
      if (xml_env)
        val = fd_symeval(xml,(fd_lexenv)xml_env);
      else val = fd_req_get(xml,VOID);
      if ((FD_TROUBLEP(val))||(VOIDP(val)))
        result = fd_eval(xml,scheme_env);
      else result = val;}
    /* Non-symbols always get evaluated in the scheme environment */
    else result = fd_eval(xml,scheme_env);
    /* This is where we have a symbol or list embedded in
       the document (via escapes, for instance) */
    if (VOIDP(result)) {}
    else if ((TABLEP(result)) &&
             (fd_test(result,xmltag_symbol,VOID))) {
      /* If the call returns an XML object, unparse it */
      fd_unparse_xml(out,result,scheme_env);
      fd_decref(result);}
    else if (FD_ABORTED(result)) {
      fd_clear_errors(1);
      return VOID;}
    else if (STRINGP(result)) {
      u8_putn(out,CSTRING(result),STRLEN(result));
      fd_decref(result);}
    else {
      /* Otherwise, output it as XML */
      fd_lisp2xml(out,result,scheme_env);
      fd_decref(result);}}
  else if (STRINGP(xml))
    u8_putn(out,CSTRING(xml),STRLEN(xml));
  else if (OIDP(xml))
    if (fd_oid_test(xml,xmltag_symbol,VOID)) {
      lispval handler = get_xml_handler(xml,xml_env);
      if (VOIDP(handler))
        result = fd_xmlout(out,xml,scheme_env,xml_env);
      else result = xmlapply(out,handler,xml,scheme_env,xml_env);
      fd_decref(handler);
      return result;}
    else return xml;
  else if (TABLEP(xml)) {
    lispval handler = get_xml_handler(xml,xml_env);
    if (VOIDP(handler))
      result = fd_xmlout(out,xml,scheme_env,xml_env);
    else result = xmlapply(out,handler,xml,scheme_env,xml_env);
    fd_decref(handler);
    return result;}
  return result;
}

FD_EXPORT
lispval fd_xmleval(u8_output out,lispval xml,fd_lexenv env)
{
  return fd_xmlevalout(out,xml,env,read_xml_env(env));
}

FD_EXPORT
lispval fd_xmleval_with(U8_OUTPUT *out,lispval xml,
                       lispval given_env,lispval given_xml_env)
{
  lispval result = VOID;
  fd_lexenv scheme_env = NULL, xml_env = NULL;
  if (!(out)) out = u8_current_output;
  if ((PAIRP(xml))&&(FD_LEXENVP(FD_CAR(xml)))) {
    /* This is returned by FDXML parsing */
    scheme_env = (fd_lexenv)fd_refcar(xml); xml = FD_CDR(xml);}
  else scheme_env = fd_working_lexenv();
  { lispval implicit_xml_env = fd_symeval(xml_env_symbol,scheme_env);
    if (VOIDP(implicit_xml_env)) {
      xml_env = fd_make_env(fd_make_hashtable(NULL,17),fdxml_module);}
    else if (FD_LEXENVP(implicit_xml_env)) {
      xml_env = (fd_lexenv)implicit_xml_env;}
    else if (TABLEP(implicit_xml_env)) {
      xml_env = fd_make_env(fd_make_hashtable(NULL,17),
                          fd_make_env(implicit_xml_env,fdxml_module));}
    else {}}
  {DO_CHOICES(given,given_env){
      if ((SYMBOLP(given))||(TABLEP(given))||
          (FD_LEXENVP(given)))
        fd_use_module(scheme_env,given);}}
  {DO_CHOICES(given,given_xml_env){
      if ((SYMBOLP(given))||(TABLEP(given))||
          (FD_LEXENVP(given)))
        fd_use_module(xml_env,given);}}
  result = fd_xmlevalout(out,xml,scheme_env,xml_env);
  fd_decref((lispval)scheme_env);
  fd_decref((lispval)xml_env);
  return result;
}

/* Breaking up FDXML evaluation */

FD_EXPORT
lispval fd_open_xml(lispval xml,fd_lexenv env)
{
  if (TABLEP(xml)) {
    u8_output out = u8_current_output;
    lispval markup; fd_lexenv xml_env = read_xml_env(env);
    if (xml_env) markup = get_markup_string(xml,env,xml_env);
    else markup = FD_ERROR;
    if (FD_ABORTED(markup)) return markup;
    else if (STRINGP(markup)) {
      if ((!(fd_test(xml,content_slotid,VOID)))||
          (fd_test(xml,content_slotid,EMPTY))||
          (fd_test(xml,content_slotid,FD_FALSE)))
        u8_printf(out,"<%s/>",CSTRING(markup));
      else u8_printf(out,"<%s>",CSTRING(markup));}
    fd_decref(markup);
    return VOID;}
  else return VOID;
}

FD_EXPORT
lispval fd_xml_opener(lispval xml,fd_lexenv env)
{
  if (TABLEP(xml)) {
    fd_lexenv xml_env = read_xml_env(env);
    if (xml_env)
      return get_markup_string(xml,env,xml_env);
    else return FD_ERROR;}
  else return VOID;
}

FD_EXPORT
lispval fd_close_xml(lispval xml)
{
  lispval name = VOID;
  u8_output out = u8_current_output;
  if ((!(fd_test(xml,content_slotid,VOID)))||
      (fd_test(xml,content_slotid,EMPTY)))
    return VOID;
  if (fd_test(xml,rawtag_symbol,VOID))
    name = fd_get(xml,rawtag_symbol,VOID);
  else if (fd_test(xml,xmltag_symbol,VOID))
    name = fd_get(xml,xmltag_symbol,VOID);
  else {}
  if ((SYMBOLP(name))||(STRINGP(name))) {
    u8_puts(out,"</");
    output_markup_sym(out,name);
    u8_putc(out,'>');}
  return name;
}

/* Reading for evaluation */

FD_EXPORT
struct FD_XML *fd_load_fdxml(u8_input in,int bits)
{
  struct FD_XML *xml = u8_alloc(struct FD_XML), *retval;
  fd_lexenv working_env = fd_working_lexenv();
  fd_bind_value(xml_env_symbol,(lispval)fdxml_module,working_env);
  fd_init_xml_node(xml,NULL,u8_strdup("top"));
  xml->xml_bits = bits; xml->xml_data = working_env;
  retval = fd_walk_xml(in,fd_xmleval_contentfn,
                     handle_fdxml_pi,
                     fd_xmleval_attribfn,
                     NULL,
                     fd_xmleval_popfn,
                     xml);
  if (retval) return xml;
  else return retval;
}
FD_EXPORT
struct FD_XML *fd_read_fdxml(u8_input in,int bits){
  return fd_load_fdxml(in,bits); }

FD_EXPORT
struct FD_XML *fd_parse_fdxml(u8_input in,int bits)
{
  struct FD_XML *xml = u8_alloc(struct FD_XML), *retval;
  fd_init_xml_node(xml,NULL,u8_strdup("top"));
  xml->xml_bits = bits; xml->xml_data = NULL;
  retval = fd_walk_xml(in,fd_xmleval_contentfn,
                     handle_eval_pi,
                     fd_xmleval_attribfn,
                     NULL,
                     fd_xmleval_popfn,
                     xml);
  if (retval) return xml;
  else return retval;
}

/* FDXML evalfns */

static lispval test_symbol, predicate_symbol, else_symbol, value_symbol;

static lispval do_body(lispval expr,fd_lexenv env);
static lispval do_else(lispval expr,fd_lexenv env);

/* Simple execution */

static lispval fdxml_insert(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval value = fdxml_get(expr,value_symbol,env);
  u8_output out = u8_current_output;
  u8_printf(out,"%q",value);
  return VOID;
}

/* Conditionals */

static lispval fdxml_if(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval test = fdxml_get(expr,test_symbol,env);
  if (FALSEP(test))
    return do_else(expr,env);
  else {
    fd_decref(test);
    return do_body(expr,env);}
}

static lispval fdxml_alt(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval content = fd_get(expr,content_slotid,VOID);
  if ((PAIRP(content))||(VECTORP(content))) {
    FD_DOELTS(x,content,count) {
      if (STRINGP(x)) {}
      else if (fd_test(x,test_symbol,VOID)) {
        lispval test = fdxml_get(x,test_symbol,env);
        if (!((FALSEP(test))||(EMPTYP(test)))) {
          lispval result = fd_xmleval(u8_current_output,x,env);
          fd_decref(result);}
        fd_decref(test);}
      else {}}}
  fd_decref(content);
  return VOID;
}

static lispval fdxml_ifreq(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval test = fd_get(expr,test_symbol,VOID);
  lispval value = fdxml_get(expr,value_symbol,env);
  lispval var = ((SYMBOLP(test))?(test):
              (STRINGP(test))?(fd_parse(CSTRING(test))):
              (VOID));
  if (VOIDP(test)) {
    u8_log(LOG_WARN,"Missing XML attribute","IFREQ missing TEST");
    return VOID;}
  else if (VOIDP(var)) {
    u8_log(LOG_WARN,"Bad XML attribute","IFReq TEST=%q",test);
    return VOID;}
  else if (fd_req_test(test,value))
    return do_body(expr,env);
  else return do_else(expr,env);
}

static lispval do_body(lispval expr,fd_lexenv env)
{
  u8_output out = u8_current_output;
  lispval body = fd_get(expr,content_slotid,VOID), result = VOID;
  if ((PAIRP(body))||(VECTORP(body))) {
    FD_DOELTS(elt,body,count) {
      lispval value = fd_xmleval(out,elt,env);
      if (FD_ABORTED(value)) {
        fd_decref(body);
        return value;}
      else {
        fd_decref(result); result = value;}}}
  fd_decref(body);
  return result;
}

static lispval do_else(lispval expr,fd_lexenv env)
{
  u8_output out = u8_current_output;
  lispval body = fd_get(expr,else_symbol,VOID);
  lispval result = fd_xmleval(out,body,env);
  fd_decref(body);
  return result;
}

/* Choice/Set operations */

static lispval fdxml_try(lispval expr,fd_lexenv env,fd_stack _stack)
{
  u8_output out = u8_current_output;
  lispval body = fd_get(expr,content_slotid,VOID), result = EMPTY;
  if ((PAIRP(body))||(VECTORP(body))) {
    FD_DOELTS(elt,body,count) {
      if (STRINGP(elt)) {}
      else {
        lispval value = fd_xmleval(out,elt,env);
        if (EMPTYP(result)) {}
        else if (FD_ABORTED(value)) {
          fd_decref(body);
          return value;}
        else {
          fd_decref(body);
          return value;}}}}
  fd_decref(body);
  return result;
}

static lispval fdxml_union(lispval expr,fd_lexenv env,fd_stack _stack)
{
  u8_output out = u8_current_output;
  lispval body = fd_get(expr,content_slotid,VOID), result = EMPTY;
  if ((PAIRP(body))||(VECTORP(body))) {
    FD_DOELTS(elt,body,count) {
      if (STRINGP(elt)) {}
      else {
        lispval value = fd_xmleval(out,elt,env);
        if (EMPTYP(result)) {}
        else if (FD_ABORTED(value)) {
          fd_decref(body);
          return value;}
        else {
          fd_decref(body);
          CHOICE_ADD(result,value);}}}}
  fd_decref(body);
  return result;
}

static lispval fdxml_intersection(lispval expr,fd_lexenv env,fd_stack _stack)
{
  u8_output out = u8_current_output;
  lispval body = fd_get(expr,content_slotid,VOID);
  int len = 0, n = 0, i = 0;
  lispval _v[16], *v, result = EMPTY;
  if (PAIRP(body)) {
    FD_DOLIST(elt,body) {(void)elt; len++;}}
  else if (VECTORP(body))
    len = VEC_LEN(body);
  else return FD_ERROR;
  if (len<16) v=_v; else v = u8_alloc_n(len,lispval);
  if ((PAIRP(body))||(VECTORP(body))) {
    FD_DOELTS(elt,body,count) {
      if (STRINGP(elt)) {}
      else {
        lispval value = fd_xmleval(out,elt,env);
        if ((EMPTYP(result)) || (FD_ABORTED(value))) {
          while (i<n) {fd_decref(v[i]); i++;}
          if (v!=_v) u8_free(v);
          return result;}
        else {
          v[n++]=value;}}}}
  result = fd_intersection(v,n);
  while (i<n) {fd_decref(v[i]); i++;}
  if (v!=_v) u8_free(v);
  fd_decref(body);
  return result;
}

/* Binding */

static lispval fdxml_binding(lispval expr,fd_lexenv env,fd_stack _stack)
{
  u8_output out = u8_current_output;
  lispval body = fd_get(expr,content_slotid,VOID), result = VOID;
  lispval attribs = fd_get(expr,attribids,VOID), table = fd_empty_slotmap();
  fd_lexenv inner_env = fd_make_env(table,env);
  /* Handle case of vector attribids */
  if (VECTORP(attribs)) {
    lispval idchoice = EMPTY;
    int i = 0; int lim = VEC_LEN(attribs);
    lispval *data = VEC_DATA(attribs);
    while (i<lim) {
      lispval v = data[i++]; fd_incref(v);
      CHOICE_ADD(idchoice,v);}
    fd_decref(attribs); attribs = idchoice;}

  {DO_CHOICES(attrib,attribs) {
    lispval val = fdxml_get(expr,attrib,env);
    fd_bind_value(attrib,val,inner_env);
    fd_decref(val);}}
  fd_decref(attribs);
  if ((PAIRP(body))||(VECTORP(body))) {
    FD_DOELTS(elt,body,counter) {
      if (STRINGP(elt))
        entify(out,CSTRING(elt),STRLEN(elt));
      else {
        lispval value = fd_xmleval(out,elt,inner_env);
        if (FD_ABORTED(value)) {
          fd_recycle_lexenv(inner_env);
          fd_decref(result);
          return value;}
        else {fd_decref(result); result = value;}}}}
  fd_recycle_lexenv(inner_env);
  fd_decref(body);
  return result;
}

/* Iteration */

static lispval each_symbol, count_symbol, sequence_symbol;
static lispval choice_symbol, max_symbol, min_symbol;

static u8_condition MissingAttrib=_("Missing XML attribute");

static lispval fdxml_seq_loop(lispval var,lispval count_var,lispval xpr,fd_lexenv env);
static lispval fdxml_choice_loop(lispval var,lispval count_var,lispval xpr,fd_lexenv env);
static lispval fdxml_range_loop(lispval var,lispval count_var,lispval xpr,fd_lexenv env);

static lispval fdxml_loop(lispval expr,fd_lexenv env,fd_stack _stack)
{
  if (!(fd_test(expr,each_symbol,VOID)))
    return fd_err(MissingAttrib,"fdxml:loop",NULL,each_symbol);
  else {
    lispval each_val = fd_get(expr,each_symbol,VOID);
    lispval count_val = fd_get(expr,count_symbol,VOID);
    lispval to_bind=
      ((STRINGP(each_val)) ? (fd_parse(CSTRING(each_val)))
       : (each_val));
    lispval to_count=
      ((STRINGP(count_val)) ? (fd_parse(CSTRING(count_val)))
       : (count_val));
    if (fd_test(expr,sequence_symbol,VOID))
      return fdxml_seq_loop(to_bind,to_count,expr,env);
    else if (fd_test(expr,choice_symbol,VOID))
      return fdxml_choice_loop(to_bind,to_count,expr,env);
    else if (fd_test(expr,max_symbol,VOID))
      return fdxml_range_loop(to_bind,to_count,expr,env);
    else return fd_err(MissingAttrib,"fdxml:loop",_("no LOOP arg"),VOID);}
}

static lispval iter_var;

static lispval fdxml_seq_loop(lispval var,lispval count_var,lispval xpr,fd_lexenv env)
{
  int i = 0, lim;
  u8_output out = u8_current_output;
  lispval seq = fdxml_get(xpr,sequence_symbol,env), *iterval = NULL;
  lispval body = fd_get(xpr,content_slotid,EMPTY);
  lispval vars[2], vals[2];
  struct FD_SCHEMAP bindings;
  struct FD_LEXENV envstruct;
  if (EMPTYP(seq)) return VOID;
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error("sequence","fdxml:loop sequence",seq);
  else lim = fd_seq_length(seq);
  if (lim==0) {
    fd_decref(seq);
    return VOID;}
  FD_INIT_STATIC_CONS(&envstruct,fd_lexenv_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  bindings.table_schema = vars; bindings.schema_values = vals;
  bindings.schema_length = 1; bindings.schemap_onstack = 1;
  u8_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent = env;
  envstruct.env_bindings = (lispval)(&bindings); envstruct.env_exports = VOID;
  envstruct.env_copy = NULL;
  vars[0]=var; vals[0]=VOID;
  if (!(VOIDP(count_var))) {
    vars[1]=count_var; vals[1]=FD_INT(0);
    bindings.schema_length = 2; iterval = &(vals[1]);}
  while (i<lim) {
    lispval elt = fd_seq_elt(seq,i);
    if (envstruct.env_copy) {
      fd_assign_value(var,elt,envstruct.env_copy);
      if (iterval)
        fd_assign_value(count_var,FD_INT(i),envstruct.env_copy);}
    else {
      vals[0]=elt;
      if (iterval) *iterval = FD_INT(i);}
    {FD_DOELTS(expr,body,count) {
      lispval val = fd_xmleval(out,expr,&envstruct);
      if (FD_ABORTED(val)) {
        u8_destroy_rwlock(&(bindings.table_rwlock));
        if (envstruct.env_copy) fd_recycle_lexenv(envstruct.env_copy);
        fd_decref(elt); fd_decref(seq);
        return val;}
      fd_decref(val);}}
    if (envstruct.env_copy) {
      fd_recycle_lexenv(envstruct.env_copy);
      envstruct.env_copy = NULL;}
    fd_decref(vals[0]);
    i++;}
  fd_decref(seq);
  u8_destroy_rwlock(&(bindings.table_rwlock));
  return VOID;
}

static lispval fdxml_choice_loop(lispval var,lispval count_var,lispval xpr,fd_lexenv env)
{
  u8_output out = u8_current_output;
  lispval choices = fdxml_get(xpr,choice_symbol,env);
  lispval body = fd_get(xpr,content_slotid,EMPTY);
  lispval *vloc = NULL, *iloc = NULL;
  lispval vars[2], vals[2];
  struct FD_SCHEMAP bindings; struct FD_LEXENV envstruct;
  if (FD_ABORTED(var)) return var;
  else if (FD_ABORTED(choices)) return choices;
  FD_INIT_STATIC_CONS(&envstruct,fd_lexenv_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  if (VOIDP(count_var)) {
    bindings.schema_length = 1;
    vars[0]=var; vals[0]=VOID;
    vloc = &(vals[0]);}
  else {
    bindings.schema_length = 2;
    vars[0]=var; vals[0]=VOID; vloc = &(vals[0]);
    vars[1]=count_var; vals[1]=FD_INT(0); iloc = &(vals[1]);}
  bindings.table_schema = vars; bindings.schema_values = vals;
  bindings.schemap_onstack = 1;
  u8_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent = env;
  envstruct.env_bindings = (lispval)(&bindings); envstruct.env_exports = VOID;
  envstruct.env_copy = NULL;
  if (EMPTYP(choices)) return VOID;
  else if (FD_ABORTED(choices))
    return choices;
  else {
    int i = 0; DO_CHOICES(elt,choices) {
      fd_incref(elt);
      if (envstruct.env_copy) {
        fd_assign_value(var,elt,envstruct.env_copy);
        if (iloc) fd_assign_value(count_var,FD_INT(i),envstruct.env_copy);}
      else {
        *vloc = elt;
        if (iloc) *iloc = FD_INT(i);}
      {FD_DOELTS(expr,body,count) {
        lispval val = fd_xmleval(out,expr,&envstruct);
        if (FD_ABORTED(val)) {
          fd_decref(choices);
          if (envstruct.env_copy)
            fd_recycle_lexenv(envstruct.env_copy);
          return val;}
        fd_decref(val);}}
      if (envstruct.env_copy) {
        fd_recycle_lexenv(envstruct.env_copy);
        envstruct.env_copy = NULL;}
      fd_decref(*vloc);
      i++;}
    fd_decref(choices);
    if (envstruct.env_copy) fd_recycle_lexenv(envstruct.env_copy);
    return VOID;}
}

static lispval fdxml_range_loop(lispval var,lispval count_var,
                               lispval xpr,fd_lexenv env)
{
  u8_output out = u8_current_output; int i = 0, limit;
  lispval limit_val = fdxml_get(xpr,max_symbol,env);
  lispval body = fd_get(xpr,content_slotid,EMPTY);
  lispval vars[2], vals[2];
  struct FD_SCHEMAP bindings; struct FD_LEXENV envstruct;
  if (FD_ABORTED(var)) return var;
  else if (!(FD_UINTP(limit_val)))
    return fd_type_error("fixnum","dotimes_handler",limit_val);
  else limit = FIX2INT(limit_val);
  FD_INIT_STATIC_CONS(&envstruct,fd_lexenv_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  bindings.table_schema = vars;
  bindings.schema_values = vals;
  bindings.schema_length = 1;
  bindings.schemap_onstack = 1;
  u8_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent = env;
  envstruct.env_bindings = (lispval)(&bindings); envstruct.env_exports = VOID;
  envstruct.env_copy = NULL;
  vars[0]=var; vals[0]=FD_INT(0);
  while (i < limit) {
    if (envstruct.env_copy)
      fd_assign_value(var,FD_INT(i),envstruct.env_copy);
    else vals[0]=FD_INT(i);
    {FD_DOELTS(expr,body,count) {
      lispval val = fd_xmleval(out,expr,&envstruct);
      if (FD_ABORTED(val)) {
        u8_destroy_rwlock(&(bindings.table_rwlock));
        if (envstruct.env_copy) fd_recycle_lexenv(envstruct.env_copy);
        return val;}
      fd_decref(val);}}
    if (envstruct.env_copy) {
      fd_recycle_lexenv(envstruct.env_copy);
      envstruct.env_copy = NULL;}
    i++;}
  u8_destroy_rwlock(&(bindings.table_rwlock));
  if (envstruct.env_copy) fd_recycle_lexenv(envstruct.env_copy);
  return VOID;
}

/* FDXML find */

static lispval index_symbol, with_symbol, slot_symbol, value_symbol;

static lispval fdxml_find(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval index_arg = fdxml_get(expr,index_symbol,env), results;
  lispval *slotvals = u8_alloc_n(16,lispval);
  lispval content = fd_get(expr,content_slotid,NIL);
  int i = 0, n = 0, lim = 16;
  FD_DOELTS(elt,content,count) {
    lispval name = fd_get(elt,xmltag_symbol,VOID);
    if (FD_EQ(name,with_symbol)) {
      lispval slotid = fdxml_get(expr,slot_symbol,env);
      lispval slotval = fdxml_get(expr,value_symbol,env);
      if (n>=lim) {
        slotvals = u8_realloc_n(slotvals,lim*2,lispval);
        lim = lim*2;}
      slotvals[n++]=slotid; slotvals[n++]=slotval;}}
  if (VOIDP(index_arg))
    results = fd_bgfinder(n,slotvals);
  else results = fd_finder(index_arg,n,slotvals);
  while (i<n) {fd_decref(slotvals[i]); i++;}
  u8_free(slotvals);
  return results;
}

/* FDXML define */

static lispval xmlarg_symbol, doseq_symbol, fdxml_define_body;

static lispval fdxml_define(lispval expr,fd_lexenv env,fd_stack _stack)
{
  if (!(fd_test(expr,id_symbol,VOID)))
    return fd_err(MissingAttrib,"fdxml:loop",NULL,id_symbol);
  else {
    lispval id_arg = fd_get(expr,id_symbol,VOID);
    lispval to_bind=
      ((STRINGP(id_arg)) ? (fd_parse(CSTRING(id_arg)))
       : (id_arg));
    lispval content = fd_get(expr,content_slotid,VOID);
    lispval attribs = fd_get(expr,attribids,VOID);
    lispval xml_env = fd_symeval(xml_env_symbol,env);
    lispval arglist = NIL;
    lispval body = NIL;
    lispval lambda = VOID;

    /* Handle case of vector attribids */
    if (VECTORP(attribs)) {
      lispval idchoice = EMPTY;
      int i = 0; int lim = VEC_LEN(attribs);
      lispval *data = VEC_DATA(attribs);
      while (i<lim) {
        lispval v = data[i++]; fd_incref(v);
        CHOICE_ADD(idchoice,v);}
      fd_decref(attribs); attribs = idchoice;}

    /* Construct the arglist */
    {DO_CHOICES(slotid,attribs)
        if (slotid!=id_symbol) {
          lispval v = fd_get(expr,slotid,FD_FALSE);
          lispval pair = fd_conspair(fd_make_list(2,slotid,v),arglist);
          arglist = pair;}}

    /* Construct the body */
    body = fd_make_list(2,quote_symbol,fd_incref(content));
    body = fd_make_list(2,xmlarg_symbol,body);
    body = fd_make_list(3,doseq_symbol,body,fd_incref(fdxml_define_body));
    body = fd_make_list(1,body);

    /* Construct the lambda */
    lambda = fd_make_lambda(u8_mkstring("XML/%s",SYM_NAME(to_bind)),
                        arglist,body,env,1,0);

    fd_bind_value(to_bind,lambda,(fd_lexenv)xml_env);
    fd_decref(lambda);
    fd_decref(body);
    fd_decref(arglist);
    fd_decref(attribs);

    return VOID;}
}

/* The init procedure */

static int xmleval_initialized = 0;

FD_EXPORT void fd_init_xmleval_c()
{
  if (xmleval_initialized) return;
  xmleval_initialized = 1;
  fd_init_scheme();
  fdxml_module = fd_make_env(fd_make_hashtable(NULL,17),NULL);
  lispval addtomod = (lispval) fdxml_module;

  fd_def_evalfn(addtomod,"IF","",fdxml_if);
  fd_def_evalfn(addtomod,"ALT","",fdxml_alt);
  fd_def_evalfn(addtomod,"IFREQ","",fdxml_ifreq);
  fd_def_evalfn(addtomod,"LOOP","",fdxml_loop);
  fd_def_evalfn(addtomod,"INSERT","",fdxml_insert);
  fd_def_evalfn(addtomod,"DEFINE","",fdxml_define);
  fd_def_evalfn(addtomod,"FIND","",fdxml_find);
  fd_def_evalfn(addtomod,"TRY","",fdxml_try);
  fd_def_evalfn(addtomod,"UNION","",fdxml_union);
  fd_def_evalfn(addtomod,"INTERSECTION","",fdxml_intersection);
  fd_def_evalfn(addtomod,"BINDING","",fdxml_binding);

  xmleval_tag = fd_intern("%XMLEVAL");
  xmleval2expr_tag = fd_intern("%XMLEVAL2EXPR");
  get_symbol = fd_intern("GET");
  elt_symbol = fd_intern("ELT");
  raw_markup = fd_intern("%MARKUP");
  content_slotid = fd_intern("%CONTENT");
  xmltag_symbol = fd_intern("%XMLTAG");
  rawtag_symbol = fd_intern("%RAWTAG");
  qname_slotid = fd_intern("%QNAME");
  attribs_slotid = fd_intern("%ATTRIBS");
  attribids_slotid = fd_intern("%ATTRIBIDS");
  comment_symbol = fd_intern("%COMMENT");
  cdata_symbol = fd_intern("%CDATA");

  xattrib_slotid = fd_intern("XATTRIB");
  id_symbol = fd_intern("ID");
  bind_symbol = fd_intern("BIND");

  test_symbol = fd_intern("TEST");
  if_symbol = fd_intern("IF");
  pif_symbol = fd_intern("%IF");
  predicate_symbol = fd_intern("PREDICATE");
  else_symbol = fd_intern("ELSE");

  xml_env_symbol = fd_intern("%XMLENV");
  xattrib_overlay = fd_intern("%XATTRIB");
  piescape_symbol = fd_intern("%PIESCAPE");
  xmlns_symbol = fd_intern("%XMLNS");

  iter_var = fd_intern("%ITER");
  value_symbol = fd_intern("VALUE");
  each_symbol = fd_intern("EACH");
  count_symbol = fd_intern("COUNT");
  sequence_symbol = fd_intern("SEQ");
  choice_symbol = fd_intern("CHOICE");
  max_symbol = fd_intern("MAX");
  min_symbol = fd_intern("MIN");

  index_symbol = fd_intern("INDEX");
  with_symbol = fd_intern("WITH");
  slot_symbol = fd_intern("SLOT");
  value_symbol = fd_intern("VALUE");

  pblank_symbol = fd_intern("%BLANK");
  xmlnode_symbol = fd_intern("XMLNODE");
  pnode_symbol = fd_intern("%NODE");
  xmlbody_symbol = fd_intern("XMLBODY");
  pbody_symbol = fd_intern("%BODY");
  env_symbol = fd_intern("%ENV");

  attribids = fd_intern("%ATTRIBIDS");

  begin_symbol = fd_intern("BEGIN");
  quote_symbol = fd_intern("QUOTE");
  xmlarg_symbol = fd_intern("%XMLARG");
  doseq_symbol = fd_intern("DOSEQ");
  fdxml_define_body = fd_make_list(2,fd_intern("XMLEVAL"),xmlarg_symbol);

  fd_register_config
    ("CACHEMARKUP",_("Whether to cache markup generated from unparsing XML"),
     fd_boolconfig_get,fd_boolconfig_set,&fd_cache_markup);

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/

