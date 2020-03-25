/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_EVAL 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/eval.h"
#include "kno/apply.h"
#include "kno/ports.h"
#include "kno/webtools.h"
#include "kno/sequences.h"
#include "kno/fileprims.h"

#include <libu8/u8xfiles.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>

#include <ctype.h>

kno_lexenv knoml_module;

int kno_cache_markup = 1;

static lispval xmleval_tag, xmleval2expr_tag;
static lispval rawtag_symbol, raw_markup;
static lispval content_slotid, xmltag_symbol, qname_slotid;
static lispval attribs_slotid, attribids_slotid, if_symbol, pif_symbol;
static lispval id_symbol, bind_symbol, xml_env_symbol, xmlns_symbol;

static lispval pblank_symbol, xmlnode_symbol, xmlbody_symbol, env_symbol;
static lispval pnode_symbol, pbody_symbol, begin_symbol;
static lispval comment_symbol, cdata_symbol;

static lispval xattrib_overlay, attribids, piescape_symbol;

void *inherit_node_data(KNO_XML *node)
{
  KNO_XML *scan = node;
  while (scan)
    if (scan->xml_data) return scan->xml_data;
    else scan = scan->xml_parent;
  return NULL;
}

kno_lexenv read_xml_env(kno_lexenv env)
{
  lispval xmlenv = kno_symeval(xml_env_symbol,env);
  if (VOIDP(xmlenv))
    return knoml_module;
  else if (KNO_LEXENVP(xmlenv)) {
    kno_decref(xmlenv);
    return (kno_lexenv)xmlenv;}
  else {
    kno_seterr(kno_TypeError,"read_xml_env","XML environment",xmlenv);
    return NULL;}
}

/* Markup output support functions */

KNO_INLINE_FCN void entify(u8_output out,u8_string value,int len)
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
                            kno_lexenv scheme_env,
                            kno_lexenv xml_env,
                            int colon)
{
  if ((PAIRP(val)) &&
      ((KNO_EQ(KNO_CAR(val),xmleval_tag)) ||
       (KNO_EQ(KNO_CAR(val),xmleval2expr_tag)))) {
    lispval expr = KNO_CDR(val); lispval value;
    u8_string as_string;
    if (SYMBOLP(expr)) {
      value = kno_symeval(expr,scheme_env);
      if ((VOIDP(value))&&(xml_env))
        value = kno_symeval(expr,xml_env);
      if (VOIDP(value))
        value = kno_req_get(expr,VOID);}
    else value = kno_eval(expr,scheme_env);
    if (KNO_ABORTED(value)) return kno_interr(value);
    else if ((VOIDP(value))&&(SYMBOLP(expr)))
      as_string = u8_strdup("");
    else if ((VOIDP(value))||(EMPTYP(value))) {
      /* This means the caller has already output then name = so there
       * better be a value */
      if (!(name)) u8_puts(out,"\"\"");
      return 0;}
    else if (KNO_EQ(KNO_CAR(val),xmleval2expr_tag))
      as_string = kno_lisp2string(value);
    else if (STRINGP(value))
      as_string = u8_strdup(CSTRING(value));
    else as_string = kno_lisp2string(value);
    if (name) start_attrib(out,name);
    u8_putc(out,'"');
    if (KNO_EQ(KNO_CAR(val),xmleval2expr_tag)) {
      if ((STRINGP(value)) && (STRLEN(value)>0) &&
          ((isdigit(CSTRING(value)[0]))||(CSTRING(value)[0]==':')))
        u8_putc(out,'\\');
    /* Don't output a preceding colon if the value would be 'self parsing' */
      else if ((FIXNUMP(value)) ||
               (KNO_FLONUMP(value)) ||
               (STRINGP(value))) {}
      else u8_putc(out,':');}
    kno_attrib_entify(out,as_string);
    u8_putc(out,'"');
    kno_decref(value); u8_free(as_string);
    return 0;}
  else if (SLOTMAPP(val)) {
    lispval value = kno_xmlevalout(NULL,val,scheme_env,xml_env);
    u8_string as_string;
    if (KNO_ABORTED(value)) return kno_interr(value);
    else if ((name)&&(EMPTYP(value))) return 0;
    else as_string = kno_lisp2string(value);
    if (name) start_attrib(out,name);
    u8_putc(out,'"');
    if (!(STRINGP(value))) u8_putc(out,':');
    kno_attrib_entify(out,as_string);
    u8_putc(out,'"');
    kno_decref(value); u8_free(as_string);
    return 0;}
  else if (STRINGP(val)) {
    if (name) start_attrib(out,name);
    u8_putc(out,'"');
    kno_attrib_entify(out,CSTRING(val));
    u8_putc(out,'"');
    return 1;}
  else if (OIDP(val)) {
    KNO_OID addr = KNO_OID_ADDR(val);
    if (name) start_attrib(out,name);
    u8_printf(out,"\"@%x/%x\"",KNO_OID_HI(addr),KNO_OID_LO(addr));
    return 1;}
  else {
    u8_string as_string = kno_lisp2string(val);
    if (name) start_attrib(out,name);
    u8_putc(out,'"');
    if (!((FIXNUMP(val)) || (KNO_FLONUMP(val)) || (KNO_BIGINTP(val))))
      u8_putc(out,':');
    kno_attrib_entify(out,as_string);
    u8_putc(out,'"');
    u8_free(as_string);
    return 1;}
}

/* Accessing xml attributes and elements. */

KNO_EXPORT
lispval kno_xml_get(lispval xml,lispval slotid)
{
  lispval results = kno_get(xml,slotid,EMPTY);
  lispval content = kno_get(xml,content_slotid,VOID);
  KNO_DOELTS(item,content,count)
    if ((TABLEP(item)) && (kno_test(item,xmltag_symbol,slotid))) {
      kno_incref(item); CHOICE_ADD(results,item);}
  kno_decref(content);
  return results;
}

static lispval get_markup_string(lispval xml,
                                kno_lexenv scheme_env,
                                kno_lexenv xml_env)
{
  U8_OUTPUT out; int cache_result = kno_cache_markup;
  lispval cached, attribs = EMPTY, attribids = EMPTY;
  if (kno_cache_markup) {
    cached = kno_get(xml,raw_markup,VOID);
    if (!(VOIDP(cached))) return cached;}
  U8_INIT_OUTPUT(&out,32);
  if (kno_test(xml,rawtag_symbol,VOID)) {
    lispval rawname = kno_get(xml,rawtag_symbol,VOID);
    if (KNO_ABORTED(rawname)) return rawname;
    else if (rawname == pblank_symbol) {
      u8_free(out.u8_outbuf);
      return rawname;}
    else if (SYMBOLP(rawname)) {
      u8_string pname = SYM_NAME(rawname);
      u8_puts(&out,pname);}
    else if (!(STRINGP(rawname))) {
      kno_decref(rawname); u8_free(out.u8_outbuf);
      return kno_type_error("XML node","get_markup_string",xml);}
    else {
      u8_putn(&out,CSTRING(rawname),STRLEN(rawname));
      kno_decref(rawname);}}
  else if (kno_test(xml,xmltag_symbol,VOID)) {
    lispval name = kno_get(xml,xmltag_symbol,VOID);
    if (name == pblank_symbol) {
      u8_free(out.u8_outbuf);
      return name;}
    else output_markup_sym(&out,name);
    kno_decref(name);}
  else return kno_type_error("XML node","get_markup_string",xml);
  {
    lispval xmlns = kno_get(xml,xmlns_symbol,VOID);
    if (!(VOIDP(xmlns))) {
      DO_CHOICES(nspec,xmlns) {
        if (STRINGP(nspec)) {
          u8_printf(&out," xmlns=\"%s\"",CSTRING(nspec));}
        else if ((PAIRP(nspec))&&
                 (STRINGP(KNO_CAR(nspec)))&&
                 (STRINGP(KNO_CDR(nspec)))) {
          u8_printf(&out," xmlns:%s=\"%s\"",
                    CSTRING(KNO_CAR(nspec)),
                    CSTRING(KNO_CDR(nspec)));}
        else {}}}
    kno_decref(xmlns);}
  attribs = kno_get(xml,attribs_slotid,EMPTY);
  if (EMPTYP(attribs))
    attribids = kno_get(xml,attribids_slotid,EMPTY);
  if (!(EMPTYP(attribs))) {
    DO_CHOICES(attrib,attribs) {
      if (!((VECTORP(attrib))&&
            (VEC_LEN(attrib)>=2)&&
            (STRINGP(VEC_REF(attrib,0))))) {
        KNO_STOP_DO_CHOICES;
        return kno_type_error("attrib","get_markup_string",attrib);}
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
            kno_attrib_entify(&out,CSTRING(value));
          else if (FIXNUMP(value))
            u8_printf(&out,"\"%lld\"",FIX2INT(value));
          else if (cache_result)
            cache_result = output_attribval
              (&out,KNO_NULL,value,scheme_env,xml_env,1);
          else output_attribval
                 (&out,KNO_NULL,value,scheme_env,xml_env,1);
          u8_putc(&out,'"');}}}
    kno_decref(attribs);}
  else if (!(EMPTYP(attribids))) {
    int i = 0, n; lispval *data, buf[1];
    lispval to_free = VOID;
    if (VECTORP(attribids)) {
      n = VEC_LEN(attribids);
      data = VEC_DATA(attribids);}
    else if (CHOICEP(attribids)) {
      n = KNO_CHOICE_SIZE(attribids);
      data = (lispval *)KNO_CHOICE_DATA(attribids);}
    else if (PRECHOICEP(attribids)) {
      to_free = kno_make_simple_choice(attribids);
      data = (lispval *)KNO_CHOICE_DATA(to_free);
      n = KNO_CHOICE_SIZE(to_free);}
    else {buf[0]=attribids; data = buf; n = 1;}
    while (i<n) {
      lispval attribid = data[i++];
      lispval value = kno_get(xml,attribid,VOID);
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
              (&out,KNO_NULL,value,scheme_env,xml_env,1);
          else output_attribval
                 (&out,KNO_NULL,value,scheme_env,xml_env,1);}
        kno_decref(value);}}
    kno_decref(to_free);
    kno_decref(attribids);}
  else {}
  cached = kno_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
  if (cache_result) kno_store(xml,raw_markup,cached);
  return cached;
}

static int test_if(lispval xml,kno_lexenv scheme_env,kno_lexenv xml_env)
{
  lispval test = kno_get(xml,pif_symbol,VOID);
  if (VOIDP(test)) return 1;
  if (SYMBOLP(test)) {
    lispval val = kno_symeval(test,scheme_env);
    if (VOIDP(val)) val = kno_req_get(test,VOID);
    if ((VOIDP(val))||(FALSEP(val))) return 0;
    else {kno_decref(val); return 1;}}
  else if (PAIRP(test)) {
    lispval value = kno_eval(test,scheme_env);
    kno_decref(test);
    if (KNO_ABORTED(value)) return kno_interr(value);
    else if (VOIDP(value)) return 0;
    else if ((FALSEP(value))||(EMPTYP(value)))
      return 0;
    else return 1;}
  else {
    kno_decref(test);
    return 1;}
}

KNO_EXPORT
lispval kno_xmlout(u8_output out,lispval xml,
                 kno_lexenv scheme_env,kno_lexenv xml_env)
{
  if (STRINGP(xml)) {}
  else if (PAIRP(xml)) {}
  else if ((CHOICEP(xml))||(PRECHOICEP(xml))) {
    lispval results = EMPTY;
    DO_CHOICES(e,xml) {
      lispval r = kno_xmlout(out,e,scheme_env,xml_env);
      if (!(VOIDP(r))) {CHOICE_ADD(results,r);}}
    return results;}
  else if (TABLEP(xml))
    if (kno_test(xml,xmltag_symbol,comment_symbol)) {
      lispval content = kno_get(xml,content_slotid,VOID);
      if (!(PAIRP(content))) {
        kno_decref(content);
        return KNO_FALSE;}
      u8_puts(out,"<!--");
      {KNO_DOELTS(elt,content,count) {
          if (STRINGP(elt)) entify(out,CSTRING(elt),STRLEN(elt));}}
      u8_puts(out,"-->");
      kno_decref(content);
      return VOID;}
    else if (kno_test(xml,xmltag_symbol,cdata_symbol)) {
      lispval content = kno_get(xml,content_slotid,VOID);
      if (!(PAIRP(content))) {
        kno_decref(content);
        return KNO_FALSE;}
      u8_puts(out,"<![CDATA[");
      {KNO_DOELTS(elt,content,count) {
          if (STRINGP(elt)) u8_putn(out,CSTRING(elt),STRLEN(elt));}}
      u8_puts(out,"]]>");
      kno_decref(content);
      return VOID;}
    else if ((kno_test(xml,pif_symbol,VOID))&&
             (!(test_if(xml,scheme_env,xml_env)))) {
      /* This node is excluded */
      return VOID;}
    else {
      lispval markup = get_markup_string(xml,scheme_env,xml_env);
      lispval content = kno_get(xml,content_slotid,VOID);
      if (KNO_ABORTED(markup)) {
        kno_decref(content); return markup;}
      else if (KNO_ABORTED(content)) {
        kno_decref(markup); return content;}
      else if ((VOIDP(content)) ||
               (EMPTYP(content)) ||
               (FALSEP(content))) {
        if (STRINGP(markup))
          u8_printf(out,"<%s/>",CSTRING(markup));
        kno_decref(markup);
        return VOID;}
      if (STRINGP(markup))
        u8_printf(out,"<%s>",CSTRING(markup));
      if (PAIRP(content)) {
        KNO_DOELTS(item,content,count) {
          lispval result = VOID;
          if (STRINGP(item))
            u8_putn(out,CSTRING(item),STRLEN(item));
          else result = kno_xmlevalout(out,item,scheme_env,xml_env);
          if (VOIDP(result)) {}
          else if (KNO_ABORTED(result)) {
            kno_decref(content);
            return result;}
          else if ((TABLEP(result)) &&
                   (kno_test(result,rawtag_symbol,VOID))) {
            lispval tmp = kno_xmlout(out,result,scheme_env,xml_env);
            kno_decref(tmp);}
          else kno_lisp2xml(out,result,scheme_env);
          kno_decref(result);}}
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
      kno_decref(markup); kno_decref(content);
      return VOID;}
  else return kno_type_error("XML node","kno_xmlout",xml);
  return VOID;
}

KNO_EXPORT
lispval kno_unparse_xml(u8_output out,lispval xml,kno_lexenv env)
{
  return kno_xmlout(out,xml,env,read_xml_env(env));
}

/* Handling dynamic elements */

static lispval get_xml_handler(lispval xml,kno_lexenv xml_env)
{
  if (!(xml_env)) return VOID;
  else {
    lispval qname = kno_get(xml,qname_slotid,VOID);
    lispval value = VOID;
    DO_CHOICES(q,qname) {
      if (STRINGP(q)) {
        lispval symbol = kno_probe_symbol(CSTRING(q),STRLEN(q));
        if (SYMBOLP(symbol)) value = kno_symeval(symbol,xml_env);
        if (!(VOIDP(value))) {
          kno_decref(qname);
          KNO_STOP_DO_CHOICES;
          return value;}}}
    kno_decref(qname); {
      lispval name = kno_get(xml,xmltag_symbol,VOID);
      if (SYMBOLP(name)) value = kno_symeval(name,xml_env);
      else {}
      return value;}}
}

struct XMLAPPLY { lispval xml; kno_lexenv env;};

KNO_EXPORT lispval knoml_get(lispval xml,lispval sym,kno_lexenv env)
{
  if ((sym == xmlnode_symbol) || (sym == pnode_symbol)) return kno_incref(xml);
  else if (sym == env_symbol) return (lispval) kno_copy_env(env);
  else if ((sym == xmlbody_symbol) || (sym == pbody_symbol)) {
    lispval content = kno_get(xml,content_slotid,VOID);
    if (VOIDP(content)) return EMPTY;
    else {
      struct KNO_KEYVAL *kv = u8_alloc_n(2,struct KNO_KEYVAL);
      /* This generates a "blank node" which generates its content
         without any container. */
      kv[0].kv_key = rawtag_symbol; kv[0].kv_val = pblank_symbol;
      kv[1].kv_key = content_slotid; kv[1].kv_val = content;
      return kno_init_slotmap(NULL,2,kv);}}
  else {
    lispval values = kno_get(xml,sym,VOID);
    if (VOIDP(values)) return VOID;
    else if (CHOICEP(values)) {
      lispval results = EMPTY;
      DO_CHOICES(value,values)
        if (PAIRP(value))
          if  ((KNO_EQ(KNO_CAR(value),xmleval_tag)) ||
               (KNO_EQ(KNO_CAR(value),xmleval2expr_tag))) {
            lispval result = kno_eval(KNO_CDR(value),env);
            if (KNO_ABORTED(result)) {
              kno_decref(results);
              KNO_STOP_DO_CHOICES;
              return result;}
            CHOICE_ADD(results,result);}
          else {
            kno_incref(value);
            CHOICE_ADD(results,value);}
        else if (TABLEP(value)) {
          lispval result = kno_xmleval(NULL,value,env);
          if (KNO_ABORTED(result)) {
            kno_decref(results);
            KNO_STOP_DO_CHOICES;
            return result;}
          CHOICE_ADD(results,result);}
        else {
          kno_incref(value);
          CHOICE_ADD(results,value);}
      kno_decref(values);
      return results;}
    else if (PAIRP(values))
      if ((KNO_EQ(KNO_CAR(values),xmleval_tag)) ||
          (KNO_EQ(KNO_CAR(values),xmleval2expr_tag))) {
        lispval result = kno_eval(KNO_CDR(values),env);
        kno_decref(values);
        return result;}
      else return values;
    else if (TABLEP(values))
      return kno_xmleval(NULL,values,env);
    else if (QCHOICEP(values)) {
      lispval result = KNO_XQCHOICE(values)->qchoiceval;
      kno_incref(result); kno_decref(values);
      return result;}
    else return values;}
}

static lispval xmlgetarg(void *vcxt,lispval sym)
{
  struct XMLAPPLY *cxt = (struct XMLAPPLY *)vcxt;
  return knoml_get(cxt->xml,sym,cxt->env);
}

static lispval xmlapply(u8_output out,lispval fn,lispval xml,
                       kno_lexenv scheme_env,kno_lexenv xml_env)
{
  struct XMLAPPLY cxt; cxt.xml = xml; cxt.env = scheme_env;
  lispval bind = kno_get(xml,id_symbol,VOID), result = VOID;
  if (TYPEP(fn,kno_evalfn_type)) {
    struct KNO_EVALFN *sf=
      kno_consptr(kno_evalfn,fn,kno_evalfn_type);
    result = sf->evalfn_handler(xml,scheme_env,kno_stackptr);}
  else if (KNO_LAMBDAP(fn))
    result = kno_xapply_lambda((struct KNO_LAMBDA *)fn,&cxt,xmlgetarg);
  else {
    kno_decref(bind);
    return kno_type_error("function","xmlapply",fn);}

  if (KNO_ABORTED(result))
    return result;
  else if (VOIDP(bind)) return result;
  else if (SYMBOLP(bind)) {
    kno_bind_value(bind,result,scheme_env);
    kno_decref(result);
    result = VOID;}
  else if (STRINGP(bind)) {
    lispval sym = kno_parse(CSTRING(bind));
    if (SYMBOLP(sym)) {
      kno_bind_value(sym,result,scheme_env);
      kno_decref(result);
      result = VOID;}
    kno_decref(sym);}
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
  result = kno_parse(buf);
  if (buf!=_buf) u8_free(buf);
  return result;
}

static lispval parse_infix(u8_string start)
{
  u8_string split;
  if ((split = (strchr(start,'.')))) {
    if (split == start) return kno_parse(start);
   /* Record form x.y ==> (get x 'y) */
    return kno_make_list(3,get_symbol,extract_var(start,split),
                        kno_make_list(2,quote_symbol,kno_parse(split+1)));}
  else if ((split = (strchr(start,'#')))) {
    if (split == start) return kno_parse(start);
    /* Call form x#y ==> (y x) */
    return kno_make_list(2,kno_parse(split+1),extract_var(start,split));}
  else if ((split = (strchr(start,'[')))) {
    if (split == start) return kno_parse(start);
    /* Vector form x[y] ==> (elt x y) */
    return kno_make_list(3,elt_symbol,extract_var(start,split),
                        kno_parse(split+1));}
  else return kno_parse(start);
}

static lispval xmlevalify(u8_string encoded)
{
  lispval result = VOID;
  u8_string string=
    ((strchr(encoded,'&') == NULL)?(encoded):
     (kno_deentify(encoded,NULL)));
  if (string[0]==':')
    if (string[1]=='$')
      result = kno_conspair(xmleval2expr_tag,parse_infix(string+2));
    else result = kno_parse(string+1);
  else if (string[0]=='$') {
    u8_string start = string+1;
    int c = u8_sgetc(&start);
    if (u8_isalpha(c))
      result = kno_conspair(xmleval_tag,parse_infix(string+1));
    else result = kno_conspair(xmleval_tag,kno_parse(string+1));}
  else if (string[0]=='\\') result = kno_mkstring(string+1);
  else result = kno_mkstring(string);
  if (string!=encoded) u8_free(string);
  return result;
}

static lispval xmldtypify(u8_string string)
{
  if (string[0]==':') return kno_parse(string+1);
  else if (string[0]=='\\') return kno_mkstring(string+1);
  else return kno_mkstring(string);
}

static lispval parse_attribname(u8_string string)
{
  lispval parsed = kno_parse(string);
  if ((SYMBOLP(parsed))||(OIDP(parsed))) return parsed;
  else {
    u8_log(LOG_WARNING,"BadAttribName",
           "Trouble parsing attribute name %s",string);
    kno_decref(parsed);
    return kno_intern(string);}
}

KNO_EXPORT int kno_xmleval_attribfn
   (KNO_XML *xml,u8_string name,u8_string val,int quote)
{
  u8_string namespace, attrib_name = kno_xmlns_lookup(xml,name,&namespace);
  lispval slotid = parse_attribname(name);
  lispval slotval = ((!(val))?(slotid):
                  ((val[0]=='\0')||(val[0]=='#')||
                   (slotid == if_symbol))?
                  (kno_mkstring(val)):
                  (quote>0) ? (xmlevalify(val)) :
                  (xmldtypify(val)));
  lispval attrib_entry = VOID;
  if ((KNO_ABORTED(slotval))||(VOIDP(slotval))||
      (KNO_EOFP(slotval))||(KNO_EODP(slotval))||
      (KNO_EOXP(slotval)))
    slotval = kno_mkstring(val);
  if (EMPTYP(xml->xml_attribs)) kno_init_xml_attribs(xml);
  xml->xml_bits = xml->xml_bits|KNO_XML_HASDATA;
  if (slotid == if_symbol) {
    u8_string sv = CSTRING(slotval);
    lispval sval = ((sv[0]=='$')?(kno_parse(sv+1)):(kno_parse(sv)));
    kno_add(xml->xml_attribs,pif_symbol,sval);
    kno_decref(sval);}
  kno_add(xml->xml_attribs,slotid,slotval);
  if (namespace) {
    kno_add(xml->xml_attribs,parse_attribname(attrib_name),slotval);
    attrib_entry=
      kno_make_nvector(3,kno_mkstring(name),
                      kno_make_qid(attrib_name,namespace),
                      kno_incref(slotval));}
  else attrib_entry=
         kno_make_nvector(3,kno_mkstring(name),KNO_FALSE,kno_incref(slotval));
  kno_add(xml->xml_attribs,attribids,slotid);
  kno_add(xml->xml_attribs,attribs_slotid,attrib_entry);
  kno_decref(attrib_entry); kno_decref(slotval);
  return 1;
}

static lispval xattrib_slotid;

static int check_symbol_entity(const u8_byte *start,const u8_byte *end);

KNO_EXPORT
void kno_xmleval_contentfn(KNO_XML *node,u8_string s,int len)
{
  if (len==0) {}
  else if ((strchr(s,'&')) == NULL)
    kno_add_content(node,kno_substring(s,s+len));
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
            int c = u8_sgetc(&as);
	    c = u8_tolower(c);
            u8_putc(&out,c);}
          symbol = kno_intern(out.u8_outbuf);
          if (start<scan)
            kno_add_content(node,kno_substring(start,scan));
          kno_add_content(node,symbol);
          start = semi+1; scan = strchr(start,'&');}
        else if (semi)
          scan = strchr(semi+1,'&');
        else scan = strchr(scan+1,'&');}}
    if (start<(s+len))
      kno_add_content(node,kno_substring(start,lim));}
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

KNO_EXPORT
KNO_XML *kno_xmleval_popfn(KNO_XML *node)
{
  /* Get your content */
  if (EMPTYP(node->xml_attribs)) kno_init_xml_attribs(node);
  if (PAIRP(node->xml_head)) {
    kno_add(node->xml_attribs,content_slotid,node->xml_head);}
  if (node->xml_parent == NULL) return NULL;
  else {
    lispval data = (lispval)(inherit_node_data(node));
    lispval cutaway = (((data == KNO_NULL)||(KNO_IMMEDIATEP(data)))?
                    (xattrib_slotid):
                    (kno_get(data,xattrib_overlay,xattrib_slotid)));
    lispval xid = kno_get(node->xml_attribs,cutaway,VOID);

    /* Check if you go on the parent's attribs or in its body. */
    if (VOIDP(xid)) {
      kno_add_content(node->xml_parent,node->xml_attribs);
      node->xml_attribs = EMPTY;}
    else if (STRINGP(xid)) {
      lispval slotid = kno_parse(KNO_STRING_DATA(xid));
      kno_add(node->xml_parent->xml_attribs,slotid,node->xml_attribs);
      kno_decref(node->xml_attribs);
      node->xml_attribs = EMPTY;
      kno_decref(xid);}
    else {
      kno_add(node->xml_parent->xml_attribs,xid,node->xml_attribs);
      kno_decref(node->xml_attribs);
      node->xml_attribs = EMPTY;}
    return node->xml_parent;}
}
/* Handling the KNOML PI */

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

static kno_lexenv get_xml_env(KNO_XML *xml)
{
  return (kno_lexenv)kno_symeval(xml_env_symbol,(kno_lexenv)(xml->xml_data));
}

static void set_xml_env(KNO_XML *xml,kno_lexenv newenv)
{
  kno_assign_value(xml_env_symbol,(lispval)newenv,(kno_lexenv)(xml->xml_data));
}

static int test_piescape(KNO_XML *xml,u8_string content,int len)
{
  if ((strncmp(content,"?knoeval ",7)==0)) return 7;
  else {
    lispval piescape = kno_get((lispval)(inherit_node_data(xml)),
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
            KNO_STOP_DO_CHOICES;
            return 1+STRLEN(pie);}}
      return 0;}}
}

static KNO_XML *handle_knoml_pi
  (u8_input in,KNO_XML *xml,u8_string content,int len)
{
  kno_lexenv env = (kno_lexenv)(xml->xml_data), xml_env = NULL;
  if (strncmp(content,"?knoml ",6)==0) {
    u8_byte *copy = (u8_byte *)u8_strdup(content);
    u8_byte *scan = copy; u8_string attribs[16];
    int i = 0, n_attribs = kno_parse_xmltag(&scan,copy+len,attribs,16,0);
    while (i<n_attribs)
      if ((strncmp(attribs[i],"load=",5))==0) {
        u8_string arg = get_pi_string(attribs[i]+5);
        u8_string filename = kno_get_component(arg);
        if (kno_load_latest(filename,env,NULL)<0) {
          u8_free(arg); u8_free(filename);
          return NULL;}
        else xml_env = get_xml_env(xml);
        u8_free(arg); u8_free(filename);
        if (TABLEP(env->env_exports)) {
          kno_lexenv new_xml_env=
            kno_make_export_env(env->env_exports,xml_env);
          set_xml_env(xml,new_xml_env);
          kno_decref((lispval)new_xml_env);}
        else {
          kno_lexenv new_xml_env=
            kno_make_export_env(env->env_bindings,xml_env);
          set_xml_env(xml,new_xml_env);
          kno_decref((lispval)new_xml_env);}
        if (xml_env) kno_decref((lispval)xml_env);
        i++;}
      else if ((strncmp(attribs[i],"config=",7))==0) {
        u8_string arg = get_pi_string(attribs[i]+7); i++;
        if (strchr(arg,'='))
          kno_config_assignment(arg);
        else {
          u8_string filename = kno_get_component(arg);
          int retval = kno_load_config(filename);
          if (retval<0) {
            u8_condition c = NULL; u8_context cxt = NULL;
            u8_string details = NULL;
            lispval irritant = VOID;
            if (kno_poperr(&c,&cxt,&details,&irritant)) {
              if ((VOIDP(irritant)) && (details == NULL) && (cxt == NULL))
                u8_log(LOG_WARN,"KNOML_CONFIG",
                       _("In config '%s' %m"),filename,c);
              else if ((VOIDP(irritant)) && (details == NULL))
                u8_log(LOG_WARN,"KNOML_CONFIG",
                       _("In config '%s' %m@%s"),filename,c,cxt);
              else if (VOIDP(irritant))
                u8_log(LOG_WARN,"KNOML_CONFIG",
                       _("In config '%s' [%m@%s] %s"),filename,c,cxt,details);
              else u8_log(LOG_WARN,"KNOML_CONFIG",
                          _("In config '%s' [%m@%s] %s %q"),
                          filename,c,cxt,details,irritant);
              if (details) u8_free(details);
              kno_decref(irritant);}
            else u8_log(LOG_WARN,"KNOML_CONFIG",
                        _("In config '%s', unknown error"),filename);}}}
      else if ((strncmp(attribs[i],"module=",7))==0) {
        u8_string arg = get_pi_string(attribs[i]+7);
        lispval module_name = kno_parse(arg);
        lispval module = kno_find_module(module_name,1);
        kno_lexenv xml_env = get_xml_env(xml);
        u8_free(arg); kno_decref(module_name);
        if ((KNO_LEXENVP(module)) &&
            (TABLEP(((kno_lexenv)module)->env_exports))) {
          lispval exports = ((kno_lexenv)module)->env_exports;
          kno_lexenv new_xml_env = kno_make_export_env(exports,xml_env);
          set_xml_env(xml,new_xml_env);
          kno_decref((lispval)new_xml_env);}
        else if (TABLEP(module)) {
          kno_lexenv new_xml_env = kno_make_export_env(module,xml_env);
          set_xml_env(xml,new_xml_env);
          kno_decref((lispval)new_xml_env);}
        if (xml_env) kno_decref((lispval)xml_env);
        i++;}
      else if ((strncmp(attribs[i],"scheme_load=",12))==0) {
        u8_string arg = get_pi_string(attribs[i]+12);
        u8_string filename = kno_get_component(arg);
        kno_lexenv env = (kno_lexenv)(xml->xml_data);
        if (kno_load_latest(filename,env,NULL)<0) {
          u8_free(arg); u8_free(filename);
          return NULL;}
        u8_free(arg); u8_free(filename);
        i++;}
      else if ((strncmp(attribs[i],"scheme_module=",14))==0) {
        u8_string arg = get_pi_string(attribs[i]+14);
        lispval module_name = kno_parse(arg);
        lispval module = kno_find_module(module_name,1);
        kno_lexenv scheme_env = (kno_lexenv)(xml->xml_data);
        u8_free(arg); kno_decref(module_name);
        if ((KNO_LEXENVP(module)) &&
            (TABLEP(((kno_lexenv)module)->env_exports))) {
          lispval exports = ((kno_lexenv)module)->env_exports;
          scheme_env->env_parent = kno_make_export_env(exports,scheme_env->env_parent);}
        else if (TABLEP(module)) {
          scheme_env->env_parent = kno_make_export_env(module,scheme_env->env_parent);}
        i++;}
      else if ((strncmp(attribs[i],"piescape=",9))==0) {
        lispval arg = kno_wrapstring(get_pi_string(attribs[i]+9));
        kno_lexenv xml_env = get_xml_env(xml);
        lispval cur = kno_symeval(piescape_symbol,xml_env);
        if (VOIDP(cur))
          kno_bind_value(piescape_symbol,arg,xml_env);
        else {
          CHOICE_ADD(cur,arg);
          kno_assign_value(piescape_symbol,arg,xml_env);}
        kno_decref(arg);
        if (xml_env) kno_decref((lispval)xml_env);
        i++;}
      else if ((strncmp(attribs[i],"xattrib=",8))==0) {
        lispval arg = kno_wrapstring(get_pi_string(attribs[i]+7));
        kno_lexenv xml_env = get_xml_env(xml);
        kno_bind_value(xattrib_overlay,arg,xml_env);
        kno_decref(arg);
        if (xml_env) kno_decref((lispval)xml_env);
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
      kno_add_content(xml,kno_init_string(NULL,len+2,restored));
      return xml;}
    else if ((len>pioff)&&(content[len-1]=='?')) len--;
    else {}
    if (strchr(content,'&')) {
      xcontent = kno_deentify(content+pioff,content+len);
      len = u8_strlen(xcontent);
      free_xcontent = 1;}
    else {
      xcontent = content+pioff;
      len = len-pioff;}
    { struct U8_INPUT in;
      lispval insert = kno_conspair(begin_symbol,NIL);
      lispval *tail = &(KNO_CDR(insert)), expr = VOID;
      U8_INIT_STRING_INPUT(&in,len,xcontent);
      expr = kno_parse_expr(&in);
      while (1) {
        if (KNO_ABORTED(expr)) {
          kno_decref(insert);
          return NULL;}
        else if ((KNO_EOFP(expr)) || (KNO_EOXP(expr))) break;
        else {
          lispval new_cons = kno_conspair(expr,NIL);
          *tail = new_cons; tail = &(KNO_CDR(new_cons));}
        expr = kno_parse_expr(&in);}
      kno_add_content(xml,insert);}
    if (free_xcontent) u8_free(xcontent);
    return xml;}
}

static KNO_XML *handle_eval_pi(u8_input in,KNO_XML *xml,u8_string content,int len)
{
  int pioff = ((strncmp(content,"?eval ",6)==0)?(6):
             (strncmp(content,"?=",2)==0)?(2):
             (0));
  u8_string xcontent = NULL; int free_xcontent = 0;
  if (pioff<=0) {
    u8_string restored = u8_string_append("<",content,">",NULL);
    kno_add_content(xml,kno_init_string(NULL,len+2,restored));
    return xml;}
  else if ((len>pioff)&&(content[len-1]=='?')) len--;
  else {}
  if (strchr(content,'&')) {
    xcontent = kno_deentify(content+pioff,content+len);
    len = u8_strlen(xcontent);
    free_xcontent = 1;}
  else {
    xcontent = content+pioff;
    len = len-pioff;}
  { struct U8_INPUT in;
    lispval insert = kno_conspair(begin_symbol,NIL);
    lispval *tail = &(KNO_CDR(insert)), expr = VOID;
    U8_INIT_STRING_INPUT(&in,len,xcontent);
    expr = kno_parse_expr(&in);
    while (1) {
      if (KNO_ABORTED(expr)) {
        kno_decref(insert);
        return NULL;}
      else if ((KNO_EOFP(expr)) || (KNO_EOXP(expr))) break;
      else {
        lispval new_cons = kno_conspair(expr,NIL);
        *tail = new_cons; tail = &(KNO_CDR(new_cons));}
      expr = kno_parse_expr(&in);}
    kno_add_content(xml,insert);}
  if (free_xcontent) u8_free(xcontent);
  return xml;
}

/* The eval function itself */

KNO_EXPORT
lispval kno_xmlevalout(u8_output out,lispval xml,
                     kno_lexenv scheme_env,kno_lexenv xml_env)
{
  lispval result = VOID;
  if ((PAIRP(xml)) &&
      ((STRINGP(KNO_CAR(xml))) || (TABLEP(KNO_CAR(xml))))) {
    /* This is the case where it's a node list */
    lispval value = VOID;
    KNO_DOELTS(elt,xml,count) {
      if (STRINGP(elt)) u8_puts(out,CSTRING(elt));
      else {
        kno_decref(value);
        value = kno_xmlevalout(out,elt,scheme_env,xml_env);
        if (KNO_ABORTED(value)) return value;}}
    return value;}
  if (KNO_NEED_EVALP(xml)) {
    lispval result;
    if (SYMBOLP(xml)) {
      /* We look up symbols in both the XML env (first) and
         the Scheme env (second), and the current request (third). */
      lispval val = VOID;
      if (xml_env)
        val = kno_symeval(xml,(kno_lexenv)xml_env);
      else val = kno_req_get(xml,VOID);
      if ((KNO_TROUBLEP(val))||(VOIDP(val)))
        result = kno_eval(xml,scheme_env);
      else result = val;}
    /* Non-symbols always get evaluated in the scheme environment */
    else result = kno_eval(xml,scheme_env);
    /* This is where we have a symbol or list embedded in
       the document (via escapes, for instance) */
    if (VOIDP(result)) {}
    else if ((TABLEP(result)) &&
             (kno_test(result,xmltag_symbol,VOID))) {
      /* If the call returns an XML object, unparse it */
      kno_unparse_xml(out,result,scheme_env);
      kno_decref(result);}
    else if (KNO_ABORTED(result)) {
      kno_clear_errors(1);
      return VOID;}
    else if (STRINGP(result)) {
      u8_putn(out,CSTRING(result),STRLEN(result));
      kno_decref(result);}
    else {
      /* Otherwise, output it as XML */
      kno_lisp2xml(out,result,scheme_env);
      kno_decref(result);}}
  else if (STRINGP(xml))
    u8_putn(out,CSTRING(xml),STRLEN(xml));
  else if (OIDP(xml))
    if (kno_oid_test(xml,xmltag_symbol,VOID)) {
      lispval handler = get_xml_handler(xml,xml_env);
      if (VOIDP(handler))
        result = kno_xmlout(out,xml,scheme_env,xml_env);
      else result = xmlapply(out,handler,xml,scheme_env,xml_env);
      kno_decref(handler);
      return result;}
    else return xml;
  else if (TABLEP(xml)) {
    lispval handler = get_xml_handler(xml,xml_env);
    if (VOIDP(handler))
      result = kno_xmlout(out,xml,scheme_env,xml_env);
    else result = xmlapply(out,handler,xml,scheme_env,xml_env);
    kno_decref(handler);
    return result;}
  return result;
}

KNO_EXPORT
lispval kno_xmleval(u8_output out,lispval xml,kno_lexenv env)
{
  return kno_xmlevalout(out,xml,env,read_xml_env(env));
}

KNO_EXPORT
lispval kno_xmleval_with(U8_OUTPUT *out,lispval xml,
                       lispval given_env,lispval given_xml_env)
{
  lispval result = VOID;
  kno_lexenv scheme_env = NULL, xml_env = NULL;
  if (!(out)) out = u8_current_output;
  if ((PAIRP(xml))&&(KNO_LEXENVP(KNO_CAR(xml)))) {
    /* This is returned by KNOML parsing */
    scheme_env = (kno_lexenv)kno_refcar(xml); xml = KNO_CDR(xml);}
  else scheme_env = kno_working_lexenv();
  { lispval implicit_xml_env = kno_symeval(xml_env_symbol,scheme_env);
    if (VOIDP(implicit_xml_env)) {
      xml_env = kno_make_env(kno_make_hashtable(NULL,17),knoml_module);}
    else if (KNO_LEXENVP(implicit_xml_env)) {
      xml_env = (kno_lexenv)implicit_xml_env;}
    else if (TABLEP(implicit_xml_env)) {
      xml_env = kno_make_env(kno_make_hashtable(NULL,17),
                          kno_make_env(implicit_xml_env,knoml_module));}
    else {}}
  {DO_CHOICES(given,given_env){
      if ((SYMBOLP(given))||(TABLEP(given))||
          (KNO_LEXENVP(given)))
        kno_use_module(scheme_env,given);}}
  {DO_CHOICES(given,given_xml_env){
      if ((SYMBOLP(given))||(TABLEP(given))||
          (KNO_LEXENVP(given)))
        kno_use_module(xml_env,given);}}
  result = kno_xmlevalout(out,xml,scheme_env,xml_env);
  kno_decref((lispval)scheme_env);
  kno_decref((lispval)xml_env);
  return result;
}

/* Breaking up KNOML evaluation */

KNO_EXPORT
lispval kno_open_xml(lispval xml,kno_lexenv env)
{
  if (TABLEP(xml)) {
    u8_output out = u8_current_output;
    lispval markup; kno_lexenv xml_env = read_xml_env(env);
    if (xml_env) markup = get_markup_string(xml,env,xml_env);
    else markup = KNO_ERROR;
    if (KNO_ABORTED(markup)) return markup;
    else if (STRINGP(markup)) {
      if ((!(kno_test(xml,content_slotid,VOID)))||
          (kno_test(xml,content_slotid,EMPTY))||
          (kno_test(xml,content_slotid,KNO_FALSE)))
        u8_printf(out,"<%s/>",CSTRING(markup));
      else u8_printf(out,"<%s>",CSTRING(markup));}
    kno_decref(markup);
    return VOID;}
  else return VOID;
}

KNO_EXPORT
lispval kno_xml_opener(lispval xml,kno_lexenv env)
{
  if (TABLEP(xml)) {
    kno_lexenv xml_env = read_xml_env(env);
    if (xml_env)
      return get_markup_string(xml,env,xml_env);
    else return KNO_ERROR;}
  else return VOID;
}

KNO_EXPORT
lispval kno_close_xml(lispval xml)
{
  lispval name = VOID;
  u8_output out = u8_current_output;
  if ((!(kno_test(xml,content_slotid,VOID)))||
      (kno_test(xml,content_slotid,EMPTY)))
    return VOID;
  if (kno_test(xml,rawtag_symbol,VOID))
    name = kno_get(xml,rawtag_symbol,VOID);
  else if (kno_test(xml,xmltag_symbol,VOID))
    name = kno_get(xml,xmltag_symbol,VOID);
  else {}
  if ((SYMBOLP(name))||(STRINGP(name))) {
    u8_puts(out,"</");
    output_markup_sym(out,name);
    u8_putc(out,'>');}
  return name;
}

/* Reading for evaluation */

KNO_EXPORT
struct KNO_XML *kno_load_knoml(u8_input in,int bits)
{
  struct KNO_XML *xml = u8_alloc(struct KNO_XML), *retval;
  kno_lexenv working_env = kno_working_lexenv();
  kno_bind_value(xml_env_symbol,(lispval)knoml_module,working_env);
  kno_init_xml_node(xml,NULL,u8_strdup("top"));
  xml->xml_bits = bits; xml->xml_data = working_env;
  retval = kno_walk_xml(in,kno_xmleval_contentfn,
                     handle_knoml_pi,
                     kno_xmleval_attribfn,
                     NULL,
                     kno_xmleval_popfn,
                     xml);
  if (retval) return xml;
  else return retval;
}
KNO_EXPORT
struct KNO_XML *kno_read_knoml(u8_input in,int bits){
  return kno_load_knoml(in,bits); }

KNO_EXPORT
struct KNO_XML *kno_parse_knoml(u8_input in,int bits)
{
  struct KNO_XML *xml = u8_alloc(struct KNO_XML), *retval;
  kno_init_xml_node(xml,NULL,u8_strdup("top"));
  xml->xml_bits = bits; xml->xml_data = NULL;
  retval = kno_walk_xml(in,kno_xmleval_contentfn,
                     handle_eval_pi,
                     kno_xmleval_attribfn,
                     NULL,
                     kno_xmleval_popfn,
                     xml);
  if (retval) return xml;
  else return retval;
}

/* KNOML evalfns */

static lispval test_symbol, predicate_symbol, else_symbol, value_symbol;

static lispval do_body(lispval expr,kno_lexenv env);
static lispval do_else(lispval expr,kno_lexenv env);

/* Simple execution */

static lispval knoml_insert(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval value = knoml_get(expr,value_symbol,env);
  u8_output out = u8_current_output;
  u8_printf(out,"%q",value);
  return VOID;
}

/* Conditionals */

static lispval knoml_if(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test = knoml_get(expr,test_symbol,env);
  if (FALSEP(test))
    return do_else(expr,env);
  else {
    kno_decref(test);
    return do_body(expr,env);}
}

static lispval knoml_alt(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval content = kno_get(expr,content_slotid,VOID);
  if ((PAIRP(content))||(VECTORP(content))) {
    KNO_DOELTS(x,content,count) {
      if (STRINGP(x)) {}
      else if (kno_test(x,test_symbol,VOID)) {
        lispval test = knoml_get(x,test_symbol,env);
        if (!((FALSEP(test))||(EMPTYP(test)))) {
          lispval result = kno_xmleval(u8_current_output,x,env);
          kno_decref(result);}
        kno_decref(test);}
      else {}}}
  kno_decref(content);
  return VOID;
}

static lispval knoml_ifreq(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test = kno_get(expr,test_symbol,VOID);
  lispval value = knoml_get(expr,value_symbol,env);
  lispval var = ((SYMBOLP(test))?(test):
              (STRINGP(test))?(kno_parse(CSTRING(test))):
              (VOID));
  if (VOIDP(test)) {
    u8_log(LOG_WARN,"Missing XML attribute","IFREQ missing TEST");
    return VOID;}
  else if (VOIDP(var)) {
    u8_log(LOG_WARN,"Bad XML attribute","IFReq TEST=%q",test);
    return VOID;}
  else if (kno_req_test(test,value))
    return do_body(expr,env);
  else return do_else(expr,env);
}

static lispval do_body(lispval expr,kno_lexenv env)
{
  u8_output out = u8_current_output;
  lispval body = kno_get(expr,content_slotid,VOID), result = VOID;
  if ((PAIRP(body))||(VECTORP(body))) {
    KNO_DOELTS(elt,body,count) {
      lispval value = kno_xmleval(out,elt,env);
      if (KNO_ABORTED(value)) {
        kno_decref(body);
        return value;}
      else {
        kno_decref(result); result = value;}}}
  kno_decref(body);
  return result;
}

static lispval do_else(lispval expr,kno_lexenv env)
{
  u8_output out = u8_current_output;
  lispval body = kno_get(expr,else_symbol,VOID);
  lispval result = kno_xmleval(out,body,env);
  kno_decref(body);
  return result;
}

/* Choice/Set operations */

static lispval knoml_try(lispval expr,kno_lexenv env,kno_stack _stack)
{
  u8_output out = u8_current_output;
  lispval body = kno_get(expr,content_slotid,VOID), result = EMPTY;
  if ((PAIRP(body))||(VECTORP(body))) {
    KNO_DOELTS(elt,body,count) {
      if (STRINGP(elt)) {}
      else {
        lispval value = kno_xmleval(out,elt,env);
        if (EMPTYP(result)) {}
        else if (KNO_ABORTED(value)) {
          kno_decref(body);
          return value;}
        else {
          kno_decref(body);
          return value;}}}}
  kno_decref(body);
  return result;
}

static lispval knoml_union(lispval expr,kno_lexenv env,kno_stack _stack)
{
  u8_output out = u8_current_output;
  lispval body = kno_get(expr,content_slotid,VOID), result = EMPTY;
  if ((PAIRP(body))||(VECTORP(body))) {
    KNO_DOELTS(elt,body,count) {
      if (STRINGP(elt)) {}
      else {
        lispval value = kno_xmleval(out,elt,env);
        if (EMPTYP(result)) {}
        else if (KNO_ABORTED(value)) {
          kno_decref(body);
          return value;}
        else {
          kno_decref(body);
          CHOICE_ADD(result,value);}}}}
  kno_decref(body);
  return result;
}

static lispval knoml_intersection(lispval expr,kno_lexenv env,kno_stack _stack)
{
  u8_output out = u8_current_output;
  lispval body = kno_get(expr,content_slotid,VOID);
  int len = 0, n = 0, i = 0;
  lispval _v[16], *v, result = EMPTY;
  if (PAIRP(body)) {
    KNO_DOLIST(elt,body) {(void)elt; len++;}}
  else if (VECTORP(body))
    len = VEC_LEN(body);
  else return KNO_ERROR;
  if (len<16) v=_v; else v = u8_alloc_n(len,lispval);
  if ((PAIRP(body))||(VECTORP(body))) {
    KNO_DOELTS(elt,body,count) {
      if (STRINGP(elt)) {}
      else {
        lispval value = kno_xmleval(out,elt,env);
        if ((EMPTYP(result)) || (KNO_ABORTED(value))) {
          while (i<n) {kno_decref(v[i]); i++;}
          if (v!=_v) u8_free(v);
          return result;}
        else {
          v[n++]=value;}}}}
  result = kno_intersection(v,n);
  while (i<n) {kno_decref(v[i]); i++;}
  if (v!=_v) u8_free(v);
  kno_decref(body);
  return result;
}

/* Binding */

static lispval knoml_binding(lispval expr,kno_lexenv env,kno_stack _stack)
{
  u8_output out = u8_current_output;
  lispval body = kno_get(expr,content_slotid,VOID), result = VOID;
  lispval attribs = kno_get(expr,attribids,VOID), table = kno_empty_slotmap();
  kno_lexenv inner_env = kno_make_env(table,env);
  /* Handle case of vector attribids */
  if (VECTORP(attribs)) {
    lispval idchoice = EMPTY;
    int i = 0; int lim = VEC_LEN(attribs);
    lispval *data = VEC_DATA(attribs);
    while (i<lim) {
      lispval v = data[i++]; kno_incref(v);
      CHOICE_ADD(idchoice,v);}
    kno_decref(attribs); attribs = idchoice;}

  {DO_CHOICES(attrib,attribs) {
    lispval val = knoml_get(expr,attrib,env);
    kno_bind_value(attrib,val,inner_env);
    kno_decref(val);}}
  kno_decref(attribs);
  if ((PAIRP(body))||(VECTORP(body))) {
    KNO_DOELTS(elt,body,counter) {
      if (STRINGP(elt))
        entify(out,CSTRING(elt),STRLEN(elt));
      else {
        lispval value = kno_xmleval(out,elt,inner_env);
        if (KNO_ABORTED(value)) {
          kno_recycle_lexenv(inner_env);
          kno_decref(result);
          return value;}
        else {kno_decref(result); result = value;}}}}
  kno_recycle_lexenv(inner_env);
  kno_decref(body);
  return result;
}

/* Iteration */

static lispval each_symbol, count_symbol, sequence_symbol;
static lispval choice_symbol, max_symbol, min_symbol;

static u8_condition MissingAttrib=_("Missing XML attribute");

static lispval knoml_seq_loop(lispval var,lispval count_var,lispval xpr,kno_lexenv env);
static lispval knoml_choice_loop(lispval var,lispval count_var,lispval xpr,kno_lexenv env);
static lispval knoml_range_loop(lispval var,lispval count_var,lispval xpr,kno_lexenv env);

static lispval knoml_loop(lispval expr,kno_lexenv env,kno_stack _stack)
{
  if (!(kno_test(expr,each_symbol,VOID)))
    return kno_err(MissingAttrib,"knoml:loop",NULL,each_symbol);
  else {
    lispval each_val = kno_get(expr,each_symbol,VOID);
    lispval count_val = kno_get(expr,count_symbol,VOID);
    lispval to_bind=
      ((STRINGP(each_val)) ? (kno_parse(CSTRING(each_val)))
       : (each_val));
    lispval to_count=
      ((STRINGP(count_val)) ? (kno_parse(CSTRING(count_val)))
       : (count_val));
    if (kno_test(expr,sequence_symbol,VOID))
      return knoml_seq_loop(to_bind,to_count,expr,env);
    else if (kno_test(expr,choice_symbol,VOID))
      return knoml_choice_loop(to_bind,to_count,expr,env);
    else if (kno_test(expr,max_symbol,VOID))
      return knoml_range_loop(to_bind,to_count,expr,env);
    else return kno_err(MissingAttrib,"knoml:loop",_("no LOOP arg"),VOID);}
}

static lispval iter_var;

static lispval knoml_seq_loop(lispval var,lispval count_var,lispval xpr,kno_lexenv env)
{
  int i = 0, lim;
  u8_output out = u8_current_output;
  lispval seq = knoml_get(xpr,sequence_symbol,env), *iterval = NULL;
  lispval body = kno_get(xpr,content_slotid,EMPTY);
  lispval vars[2], vals[2];
  struct KNO_SCHEMAP bindings;
  struct KNO_LEXENV envstruct;
  if (EMPTYP(seq)) return VOID;
  else if (!(KNO_SEQUENCEP(seq)))
    return kno_type_error("sequence","knoml:loop sequence",seq);
  else lim = kno_seq_length(seq);
  if (lim==0) {
    kno_decref(seq);
    return VOID;}
  KNO_INIT_STATIC_CONS(&envstruct,kno_lexenv_type);
  KNO_INIT_STATIC_CONS(&bindings,kno_schemap_type);
  bindings.table_schema = vars; bindings.schema_values = vals;
  bindings.schema_length = 1; bindings.schemap_onstack = 1;
  u8_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent = env;
  envstruct.env_bindings = (lispval)(&bindings); envstruct.env_exports = VOID;
  envstruct.env_copy = NULL;
  vars[0]=var; vals[0]=VOID;
  if (!(VOIDP(count_var))) {
    vars[1]=count_var; vals[1]=KNO_INT(0);
    bindings.schema_length = 2; iterval = &(vals[1]);}
  while (i<lim) {
    lispval elt = kno_seq_elt(seq,i);
    if (envstruct.env_copy) {
      kno_assign_value(var,elt,envstruct.env_copy);
      if (iterval)
        kno_assign_value(count_var,KNO_INT(i),envstruct.env_copy);}
    else {
      vals[0]=elt;
      if (iterval) *iterval = KNO_INT(i);}
    {KNO_DOELTS(expr,body,count) {
      lispval val = kno_xmleval(out,expr,&envstruct);
      if (KNO_ABORTED(val)) {
        u8_destroy_rwlock(&(bindings.table_rwlock));
        if (envstruct.env_copy) kno_recycle_lexenv(envstruct.env_copy);
        kno_decref(elt); kno_decref(seq);
        return val;}
      kno_decref(val);}}
    if (envstruct.env_copy) {
      kno_recycle_lexenv(envstruct.env_copy);
      envstruct.env_copy = NULL;}
    kno_decref(vals[0]);
    i++;}
  kno_decref(seq);
  u8_destroy_rwlock(&(bindings.table_rwlock));
  return VOID;
}

static lispval knoml_choice_loop(lispval var,lispval count_var,lispval xpr,kno_lexenv env)
{
  u8_output out = u8_current_output;
  lispval choices = knoml_get(xpr,choice_symbol,env);
  lispval body = kno_get(xpr,content_slotid,EMPTY);
  lispval *vloc = NULL, *iloc = NULL;
  lispval vars[2], vals[2];
  struct KNO_SCHEMAP bindings; struct KNO_LEXENV envstruct;
  if (KNO_ABORTED(var)) return var;
  else if (KNO_ABORTED(choices)) return choices;
  KNO_INIT_STATIC_CONS(&envstruct,kno_lexenv_type);
  KNO_INIT_STATIC_CONS(&bindings,kno_schemap_type);
  if (VOIDP(count_var)) {
    bindings.schema_length = 1;
    vars[0]=var; vals[0]=VOID;
    vloc = &(vals[0]);}
  else {
    bindings.schema_length = 2;
    vars[0]=var; vals[0]=VOID; vloc = &(vals[0]);
    vars[1]=count_var; vals[1]=KNO_INT(0); iloc = &(vals[1]);}
  bindings.table_schema = vars; bindings.schema_values = vals;
  bindings.schemap_onstack = 1;
  u8_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent = env;
  envstruct.env_bindings = (lispval)(&bindings); envstruct.env_exports = VOID;
  envstruct.env_copy = NULL;
  if (EMPTYP(choices)) return VOID;
  else if (KNO_ABORTED(choices))
    return choices;
  else {
    int i = 0; DO_CHOICES(elt,choices) {
      kno_incref(elt);
      if (envstruct.env_copy) {
        kno_assign_value(var,elt,envstruct.env_copy);
        if (iloc) kno_assign_value(count_var,KNO_INT(i),envstruct.env_copy);}
      else {
        *vloc = elt;
        if (iloc) *iloc = KNO_INT(i);}
      {KNO_DOELTS(expr,body,count) {
        lispval val = kno_xmleval(out,expr,&envstruct);
        if (KNO_ABORTED(val)) {
          kno_decref(choices);
          if (envstruct.env_copy)
            kno_recycle_lexenv(envstruct.env_copy);
          return val;}
        kno_decref(val);}}
      if (envstruct.env_copy) {
        kno_recycle_lexenv(envstruct.env_copy);
        envstruct.env_copy = NULL;}
      kno_decref(*vloc);
      i++;}
    kno_decref(choices);
    if (envstruct.env_copy) kno_recycle_lexenv(envstruct.env_copy);
    return VOID;}
}

static lispval knoml_range_loop(lispval var,lispval count_var,
                               lispval xpr,kno_lexenv env)
{
  u8_output out = u8_current_output; int i = 0, limit;
  lispval limit_val = knoml_get(xpr,max_symbol,env);
  lispval body = kno_get(xpr,content_slotid,EMPTY);
  lispval vars[2], vals[2];
  struct KNO_SCHEMAP bindings; struct KNO_LEXENV envstruct;
  if (KNO_ABORTED(var)) return var;
  else if (!(KNO_UINTP(limit_val)))
    return kno_type_error("fixnum","dotimes_handler",limit_val);
  else limit = FIX2INT(limit_val);
  KNO_INIT_STATIC_CONS(&envstruct,kno_lexenv_type);
  KNO_INIT_STATIC_CONS(&bindings,kno_schemap_type);
  bindings.table_schema = vars;
  bindings.schema_values = vals;
  bindings.schema_length = 1;
  bindings.schemap_onstack = 1;
  u8_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent = env;
  envstruct.env_bindings = (lispval)(&bindings); envstruct.env_exports = VOID;
  envstruct.env_copy = NULL;
  vars[0]=var; vals[0]=KNO_INT(0);
  while (i < limit) {
    if (envstruct.env_copy)
      kno_assign_value(var,KNO_INT(i),envstruct.env_copy);
    else vals[0]=KNO_INT(i);
    {KNO_DOELTS(expr,body,count) {
      lispval val = kno_xmleval(out,expr,&envstruct);
      if (KNO_ABORTED(val)) {
        u8_destroy_rwlock(&(bindings.table_rwlock));
        if (envstruct.env_copy) kno_recycle_lexenv(envstruct.env_copy);
        return val;}
      kno_decref(val);}}
    if (envstruct.env_copy) {
      kno_recycle_lexenv(envstruct.env_copy);
      envstruct.env_copy = NULL;}
    i++;}
  u8_destroy_rwlock(&(bindings.table_rwlock));
  if (envstruct.env_copy) kno_recycle_lexenv(envstruct.env_copy);
  return VOID;
}

/* KNOML find */

static lispval index_symbol, with_symbol, slot_symbol, value_symbol;

static lispval knoml_find(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval index_arg = knoml_get(expr,index_symbol,env), results;
  lispval *slotvals = u8_alloc_n(16,lispval);
  lispval content = kno_get(expr,content_slotid,NIL);
  int i = 0, n = 0, lim = 16;
  KNO_DOELTS(elt,content,count) {
    lispval name = kno_get(elt,xmltag_symbol,VOID);
    if (KNO_EQ(name,with_symbol)) {
      lispval slotid = knoml_get(expr,slot_symbol,env);
      lispval slotval = knoml_get(expr,value_symbol,env);
      if (n>=lim) {
        slotvals = u8_realloc_n(slotvals,lim*2,lispval);
        lim = lim*2;}
      slotvals[n++]=slotid; slotvals[n++]=slotval;}}
  if (VOIDP(index_arg))
    results = kno_bgfinder(n,slotvals);
  else results = kno_finder(index_arg,n,slotvals);
  while (i<n) {kno_decref(slotvals[i]); i++;}
  u8_free(slotvals);
  return results;
}

/* KNOML define */

static lispval xmlarg_symbol, doseq_symbol, knoml_define_body;

static lispval knoml_define(lispval expr,kno_lexenv env,kno_stack _stack)
{
  if (!(kno_test(expr,id_symbol,VOID)))
    return kno_err(MissingAttrib,"knoml:loop",NULL,id_symbol);
  else {
    lispval id_arg = kno_get(expr,id_symbol,VOID);
    lispval to_bind=
      ((STRINGP(id_arg)) ? (kno_parse(CSTRING(id_arg)))
       : (id_arg));
    lispval content = kno_get(expr,content_slotid,VOID);
    lispval attribs = kno_get(expr,attribids,VOID);
    lispval xml_env = kno_symeval(xml_env_symbol,env);
    lispval arglist = NIL;
    lispval body = NIL;
    lispval lambda = VOID;

    /* Handle case of vector attribids */
    if (VECTORP(attribs)) {
      lispval idchoice = EMPTY;
      int i = 0; int lim = VEC_LEN(attribs);
      lispval *data = VEC_DATA(attribs);
      while (i<lim) {
        lispval v = data[i++]; kno_incref(v);
        CHOICE_ADD(idchoice,v);}
      kno_decref(attribs); attribs = idchoice;}

    /* Construct the arglist */
    {DO_CHOICES(slotid,attribs)
        if (slotid!=id_symbol) {
          lispval v = kno_get(expr,slotid,KNO_FALSE);
          lispval pair = kno_conspair(kno_make_list(2,slotid,v),arglist);
          arglist = pair;}}

    /* Construct the body */
    body = kno_make_list(2,quote_symbol,kno_incref(content));
    body = kno_make_list(2,xmlarg_symbol,body);
    body = kno_make_list(3,doseq_symbol,body,kno_incref(knoml_define_body));
    body = kno_make_list(1,body);

    /* Construct the lambda */
    lambda = kno_make_lambda(u8_mkstring("XML/%s",SYM_NAME(to_bind)),
                        arglist,body,env,1,0);

    kno_bind_value(to_bind,lambda,(kno_lexenv)xml_env);
    kno_decref(lambda);
    kno_decref(body);
    kno_decref(arglist);
    kno_decref(attribs);

    return VOID;}
}

/* The init procedure */

static int xmleval_initialized = 0;

KNO_EXPORT void kno_init_xmleval_c()
{
  if (xmleval_initialized) return;
  xmleval_initialized = 1;
  kno_init_scheme();
  knoml_module = kno_make_env(kno_make_hashtable(NULL,17),NULL);
  lispval addtomod = (lispval) knoml_module;

  kno_def_evalfn(addtomod,"IF",knoml_if,
		 "*undocumented*");
  kno_def_evalfn(addtomod,"ALT",knoml_alt,
		 "*undocumented*");
  kno_def_evalfn(addtomod,"IFREQ",knoml_ifreq,
		 "*undocumented*");
  kno_def_evalfn(addtomod,"LOOP",knoml_loop,
		 "*undocumented*");
  kno_def_evalfn(addtomod,"INSERT",knoml_insert,
		 "*undocumented*");
  kno_def_evalfn(addtomod,"DEFINE",knoml_define,
		 "*undocumented*");
  kno_def_evalfn(addtomod,"FIND",knoml_find,
		 "*undocumented*");
  kno_def_evalfn(addtomod,"TRY",knoml_try,
		 "*undocumented*");
  kno_def_evalfn(addtomod,"UNION",knoml_union,
		 "*undocumented*");
  kno_def_evalfn(addtomod,"INTERSECTION",knoml_intersection,
		 "*undocumented*");
  kno_def_evalfn(addtomod,"BINDING",knoml_binding,
		 "*undocumented*");

  xmleval_tag = kno_intern("%xmleval");
  xmleval2expr_tag = kno_intern("%xmleval2expr");
  get_symbol = kno_intern("get");
  elt_symbol = kno_intern("elt");
  raw_markup = kno_intern("%markup");
  content_slotid = kno_intern("%content");
  xmltag_symbol = kno_intern("%xmltag");
  rawtag_symbol = kno_intern("%rawtag");
  qname_slotid = kno_intern("%qname");
  attribs_slotid = kno_intern("%attribs");
  attribids_slotid = kno_intern("%attribids");
  comment_symbol = kno_intern("%comment");
  cdata_symbol = kno_intern("%cdata");

  xattrib_slotid = kno_intern("xattrib");
  id_symbol = kno_intern("id");
  bind_symbol = kno_intern("bind");

  test_symbol = kno_intern("test");
  if_symbol = kno_intern("if");
  pif_symbol = kno_intern("%if");
  predicate_symbol = kno_intern("predicate");
  else_symbol = kno_intern("else");

  xml_env_symbol = kno_intern("%xmlenv");
  xattrib_overlay = kno_intern("%xattrib");
  piescape_symbol = kno_intern("%piescape");
  xmlns_symbol = kno_intern("%xmlns");

  iter_var = kno_intern("%iter");
  value_symbol = kno_intern("value");
  each_symbol = kno_intern("each");
  count_symbol = kno_intern("count");
  sequence_symbol = kno_intern("seq");
  choice_symbol = kno_intern("choice");
  max_symbol = kno_intern("max");
  min_symbol = kno_intern("min");

  index_symbol = kno_intern("index");
  with_symbol = kno_intern("with");
  slot_symbol = kno_intern("slot");
  value_symbol = kno_intern("value");

  pblank_symbol = kno_intern("%blank");
  xmlnode_symbol = kno_intern("xmlnode");
  pnode_symbol = kno_intern("%node");
  xmlbody_symbol = kno_intern("xmlbody");
  pbody_symbol = kno_intern("%body");
  env_symbol = kno_intern("%env");

  attribids = kno_intern("%attribids");

  begin_symbol = kno_intern("begin");
  quote_symbol = kno_intern("quote");
  xmlarg_symbol = kno_intern("%xmlarg");
  doseq_symbol = kno_intern("doseq");
  knoml_define_body = kno_make_list(2,kno_intern("xmleval"),xmlarg_symbol);

  kno_register_config
    ("CACHEMARKUP",_("Whether to cache markup generated from unparsing XML"),
     kno_boolconfig_get,kno_boolconfig_set,&kno_cache_markup);

  u8_register_source_file(_FILEINFO);
}


