/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#define U8_INLINE_IO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/tables.h"
#include "kno/eval.h"
#include "kno/ports.h"
#include "kno/webtools.h"
#include "kno/cprims.h"

#include <libu8/u8xfiles.h>
#include <libu8/u8stringfns.h>

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

static lispval name_slotid, content_slotid;

DEFCPRIM("xmlattrib",xmlattrib,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(XMLATTRIB *arg0* *arg1*)` "
	 "**undocumented**",
	 {"doc",kno_any_type,KNO_VOID},
	 {"attrib_id",kno_any_type,KNO_VOID})
static lispval xmlattrib(lispval doc,lispval attrib_id)
{
  if (SLOTMAPP(doc))
    if (SYMBOLP(attrib_id))
      return kno_get(doc,attrib_id,EMPTY);
    else return EMPTY;
  else return EMPTY;
}

static void xmlget_helper(lispval *result,lispval doc,lispval eltid,int cons)
{
  if ((OIDP(doc)) || (SLOTMAPP(doc)))
    if ((kno_test(doc,name_slotid,eltid))) {
      kno_incref(doc);
      if (cons) *result = kno_conspair(doc,*result);
      else {CHOICE_ADD((*result),doc);}}
    else {
      lispval content = kno_get(doc,content_slotid,EMPTY);
      xmlget_helper(result,content,eltid,cons);
      kno_decref(content);}
  else if ((PAIRP(doc))||(VECTORP(doc))) {
    KNO_DOELTS(elt,doc,count) {
      if (STRINGP(elt)) {}
      else xmlget_helper(result,elt,eltid,cons);}}
  else return;
}

DEFCPRIM("xmlget",xmlget,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(XMLGET *arg0* *arg1*)` "
	 "**undocumented**",
	 {"doc",kno_any_type,KNO_VOID},
	 {"attrib_id",kno_any_type,KNO_VOID})
static lispval xmlget(lispval doc,lispval attrib_id)
{
  lispval results = EMPTY;
  xmlget_helper(&results,doc,attrib_id,0);
  return results;
}

static int listlen(lispval l)
{
  if (!(PAIRP(l))) return 0;
  else {
    int len = 0; lispval scan = l;
    while (PAIRP(scan)) {scan = KNO_CDR(scan); len++;}
    return len;}
}

DEFCPRIM("xmlget/sorted",xmlget_sorted,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(XMLGET/SORTED *arg0* *arg1*)` "
	 "**undocumented**",
	 {"doc",kno_any_type,KNO_VOID},
	 {"attrib_id",kno_any_type,KNO_VOID})
static lispval xmlget_sorted(lispval doc,lispval attrib_id)
{
  lispval results = NIL;
  xmlget_helper(&results,doc,attrib_id,1);
  if (NILP(results))
    return kno_make_vector(0,NULL);
  else {
    int len = listlen(results), i = len-1;
    lispval vec = kno_make_vector(len,NULL), scan = results;
    while ((i>=0)&&(PAIRP(scan))) {
      lispval car = KNO_CAR(scan);
      KNO_VECTOR_SET(vec,i,car); kno_incref(car);
      scan = KNO_CDR(scan); i--;}
    kno_decref(results);
    return vec;}
}

DEFCPRIM("xmlget/first",xmlget_first,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(XMLGET/FIRST *arg0* *arg1*)` "
	 "**undocumented**",
	 {"doc",kno_any_type,KNO_VOID},
	 {"attrib_id",kno_any_type,KNO_VOID})
static lispval xmlget_first(lispval doc,lispval attrib_id)
{
  lispval results = NIL;
  xmlget_helper(&results,doc,attrib_id,1);
  if (NILP(results))
    return EMPTY;
  else {
    lispval last_result = VOID;
    KNO_DOELTS(elt,results,count) {last_result = elt;}
    kno_incref(last_result);
    kno_decref(results);
    if (VOIDP(last_result)) return EMPTY;
    else return last_result;}
}

/* This returns the content field as parsed. */

DEFCPRIM("xmlconents",xmlcontents,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(XMLCONENTS *arg0* [*arg1*])` "
	 "**undocumented**",
	 {"doc",kno_any_type,KNO_VOID},
	 {"attrib_id",kno_any_type,KNO_VOID})
static lispval xmlcontents(lispval doc,lispval attrib_id)
{
  if ((CHOICEP(doc)) || (PRECHOICEP(doc))) {
    lispval contents = EMPTY;
    DO_CHOICES(docelt,doc) {
      lispval content = xmlcontents(docelt,attrib_id);
      CHOICE_ADD(contents,content);}
    return contents;}
  else if (VOIDP(attrib_id))
    if (NILP(doc)) return doc;
    else if ((PAIRP(doc)) || (STRINGP(doc))) return kno_incref(doc);
    else if ((OIDP(doc)) || (SLOTMAPP(doc)))
      return kno_get(doc,content_slotid,NIL);
    else return kno_type_error("XML node","xmlcontents",doc);
  else if ((PAIRP(doc))||(VECTORP(doc))) {
    lispval results = EMPTY;
    KNO_DOELTS(docelt,doc,count) {
      lispval contents = xmlcontents(docelt,attrib_id);
      if (!(NILP(contents))) {
	CHOICE_ADD(results,contents);}}
    return results;}
  else {
    lispval value = kno_get(doc,attrib_id,NIL);
    lispval contents = xmlcontents(value,VOID);
    kno_decref(value);
    return contents;}
}

/* This returns the content field as parsed. */

DEFCPRIM("xmlempty?",xmlemptyp,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(XMLEMPTY? *arg0* [*arg1*])` "
	 "**undocumented**",
	 {"elt",kno_any_type,KNO_VOID},
	 {"attribid",kno_any_type,KNO_VOID})
static lispval xmlemptyp(lispval elt,lispval attribid)
{
  if (VOIDP(attribid)) attribid = content_slotid;
  if (!(kno_test(elt,attribid,VOID)))
    return KNO_TRUE;
  else {
    lispval content = kno_get(elt,attribid,NIL);
    if ((NILP(content)) ||
	(EMPTYP(content))||
	((VECTORP(content))&&
	 (VEC_LEN(content)==0)))
      return KNO_TRUE;
    else {
      kno_decref(content);
      return KNO_FALSE;}}
}

/* This returns the content field as a string. */

DEFCPRIM("xmlcontent",xmlcontent,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(XMLCONTENT *arg0* [*arg1*])` "
	 "**undocumented**",
	 {"doc",kno_any_type,KNO_VOID},
	 {"attrib_id",kno_any_type,KNO_VOID})
static lispval xmlcontent(lispval doc,lispval attrib_id)
{
  if ((CHOICEP(doc)) || (PRECHOICEP(doc))) {
    lispval contents = EMPTY;
    DO_CHOICES(docelt,doc) {
      lispval content = xmlcontent(docelt,attrib_id);
      CHOICE_ADD(contents,content);}
    return contents;}
  else if (VOIDP(attrib_id))
    if (NILP(doc)) return kno_init_string(NULL,0,u8_strdup(""));
    else if (STRINGP(doc)) return kno_incref(doc);
    else if ((PAIRP(doc))||(VECTORP(doc))) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
      {KNO_DOELTS(docelt,doc,count) {
	  if (STRINGP(docelt))
	    u8_putn(&out,CSTRING(docelt),STRLEN(docelt));
	  else if ((OIDP(docelt)) || (SLOTMAPP(docelt)))
	    kno_unparse_xml(&out,docelt,NULL);
	  else kno_unparse(&out,docelt);}}
      return kno_stream2string(&out);}
    else if ((OIDP(doc)) || (SLOTMAPP(doc))) {
      lispval content = kno_get(doc,content_slotid,NIL);
      lispval as_string = xmlcontent(content,VOID);
      kno_decref(content);
      return as_string;}
    else return kno_type_error("XML node","xmlcontent",doc);
  else if ((PAIRP(doc))||(VECTORP(doc))) {
    lispval results = EMPTY;
    KNO_DOELTS(docelt,doc,count) {
      lispval contents = xmlcontent(docelt,attrib_id);
      if (!(NILP(contents))) {
	CHOICE_ADD(results,contents);}}
    return results;}
  else {
    lispval value = kno_get(doc,attrib_id,NIL);
    lispval contents = xmlcontent(value,VOID);
    kno_decref(value);
    return contents;}
}

static lispval webtools_module;

KNO_EXPORT
void kno_init_xmldata_c()
{
  webtools_module = kno_new_module("WEBTOOLS",0);

  link_local_cprims();

  name_slotid = kno_intern("%xmltag");
  content_slotid = kno_intern("%content");
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("xmlcontent",xmlcontent,2,webtools_module);
  KNO_LINK_CPRIM("xmlempty?",xmlemptyp,2,webtools_module);
  KNO_LINK_CPRIM("xmlconents",xmlcontents,2,webtools_module);
  KNO_LINK_CPRIM("xmlget/first",xmlget_first,2,webtools_module);
  KNO_LINK_CPRIM("xmlget/sorted",xmlget_sorted,2,webtools_module);
  KNO_LINK_CPRIM("xmlget",xmlget,2,webtools_module);
  KNO_LINK_CPRIM("xmlattrib",xmlattrib,2,webtools_module);
}
