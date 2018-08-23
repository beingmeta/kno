/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/eval.h"
#include "framerd/ports.h"
#include "framerd/fdweb.h"

#include <libu8/u8xfiles.h>
#include <libu8/u8stringfns.h>

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

static lispval name_slotid, content_slotid;

static lispval xmlattrib(lispval doc,lispval attrib_id)
{
  if (SLOTMAPP(doc))
    if (SYMBOLP(attrib_id))
      return fd_get(doc,attrib_id,EMPTY);
    else return EMPTY;
  else return EMPTY;
}

static void xmlget_helper(lispval *result,lispval doc,lispval eltid,int cons)
{
  if ((OIDP(doc)) || (SLOTMAPP(doc)))
    if ((fd_test(doc,name_slotid,eltid))) {
      fd_incref(doc);
      if (cons) *result = fd_conspair(doc,*result);
      else {CHOICE_ADD((*result),doc);}}
    else {
      lispval content = fd_get(doc,content_slotid,EMPTY);
      xmlget_helper(result,content,eltid,cons);
      fd_decref(content);}
  else if ((PAIRP(doc))||(VECTORP(doc))) {
    FD_DOELTS(elt,doc,count) {
      if (STRINGP(elt)) {}
      else xmlget_helper(result,elt,eltid,cons);}}
  else return;
}

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
    while (PAIRP(scan)) {scan = FD_CDR(scan); len++;}
    return len;}
}

static lispval xmlget_sorted(lispval doc,lispval attrib_id)
{
  lispval results = NIL;
  xmlget_helper(&results,doc,attrib_id,1);
  if (NILP(results))
    return fd_make_vector(0,NULL);
  else {
    int len = listlen(results), i = len-1;
    lispval vec = fd_make_vector(len,NULL), scan = results;
    while ((i>=0)&&(PAIRP(scan))) {
      lispval car = FD_CAR(scan);
      FD_VECTOR_SET(vec,i,car); fd_incref(car);
      scan = FD_CDR(scan); i--;}
    fd_decref(results);
    return vec;}
}

static lispval xmlget_first(lispval doc,lispval attrib_id)
{
  lispval results = NIL;
  xmlget_helper(&results,doc,attrib_id,1);
  if (NILP(results))
    return EMPTY;
  else {
    lispval last_result = VOID;
    FD_DOELTS(elt,results,count) {last_result = elt;}
    fd_incref(last_result);
    fd_decref(results);
    if (VOIDP(last_result)) return EMPTY;
    else return last_result;}
}

/* This returns the content field as parsed. */
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
    else if ((PAIRP(doc)) || (STRINGP(doc))) return fd_incref(doc);
    else if ((OIDP(doc)) || (SLOTMAPP(doc)))
      return fd_get(doc,content_slotid,NIL);
    else return fd_type_error("XML node","xmlcontents",doc);
  else if ((PAIRP(doc))||(VECTORP(doc))) {
    lispval results = EMPTY;
    FD_DOELTS(docelt,doc,count) {
      lispval contents = xmlcontents(docelt,attrib_id);
      if (!(NILP(contents))) {
        CHOICE_ADD(results,contents);}}
    return results;}
  else {
    lispval value = fd_get(doc,attrib_id,NIL);
    lispval contents = xmlcontents(value,VOID);
    fd_decref(value);
    return contents;}
}

/* This returns the content field as parsed. */
static lispval xmlemptyp(lispval elt,lispval attribid)
{
  if (VOIDP(attribid)) attribid = content_slotid;
  if (!(fd_test(elt,attribid,VOID)))
    return FD_TRUE;
  else {
    lispval content = fd_get(elt,attribid,NIL);
    if ((NILP(content)) ||
        (EMPTYP(content))||
        ((VECTORP(content))&&
         (VEC_LEN(content)==0)))
      return FD_TRUE;
    else {
      fd_decref(content);
      return FD_FALSE;}}
}

/* This returns the content field as a string. */
static lispval xmlcontent(lispval doc,lispval attrib_id)
{
  if ((CHOICEP(doc)) || (PRECHOICEP(doc))) {
    lispval contents = EMPTY;
    DO_CHOICES(docelt,doc) {
      lispval content = xmlcontent(docelt,attrib_id);
      CHOICE_ADD(contents,content);}
    return contents;}
  else if (VOIDP(attrib_id))
    if (NILP(doc)) return fd_init_string(NULL,0,u8_strdup(""));
    else if (STRINGP(doc)) return fd_incref(doc);
    else if ((PAIRP(doc))||(VECTORP(doc))) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
      {FD_DOELTS(docelt,doc,count) {
          if (STRINGP(docelt))
            u8_putn(&out,CSTRING(docelt),STRLEN(docelt));
          else if ((OIDP(docelt)) || (SLOTMAPP(docelt)))
            fd_unparse_xml(&out,docelt,NULL);
          else fd_unparse(&out,docelt);}}
      return fd_stream2string(&out);}
    else if ((OIDP(doc)) || (SLOTMAPP(doc))) {
      lispval content = fd_get(doc,content_slotid,NIL);
      lispval as_string = xmlcontent(content,VOID);
      fd_decref(content);
      return as_string;}
    else return fd_type_error("XML node","xmlcontent",doc);
  else if ((PAIRP(doc))||(VECTORP(doc))) {
    lispval results = EMPTY;
    FD_DOELTS(docelt,doc,count) {
      lispval contents = xmlcontent(docelt,attrib_id);
      if (!(NILP(contents))) {
        CHOICE_ADD(results,contents);}}
    return results;}
  else {
    lispval value = fd_get(doc,attrib_id,NIL);
    lispval contents = xmlcontent(value,VOID);
    fd_decref(value);
    return contents;}
}

FD_EXPORT
void fd_init_xmldata_c()
{
  lispval safe_module = fd_new_module("FDWEB",(0));
  lispval module = fd_new_module("FDWEB",(FD_MODULE_SAFE));

  fd_idefn(module,fd_make_cprim2("XMLATTRIB",xmlattrib,2));
  fd_idefn(module,fd_make_cprim2("XMLGET",xmlget,2));
  fd_idefn(module,fd_make_cprim2("XMLGET/FIRST",xmlget_first,2));
  fd_idefn(module,fd_make_cprim2("XMLGET/SORTED",xmlget_sorted,2));
  fd_idefn(module,fd_make_cprim2("XMLCONENTS",xmlcontents,1));
  fd_idefn(module,fd_make_cprim2("XMLCONTENT",xmlcontent,1));
  fd_idefn(module,fd_make_cprim2("XMLEMPTY?",xmlemptyp,1));

  fd_idefn(safe_module,fd_make_cprim2("XMLATTRIB",xmlattrib,2));
  fd_idefn(safe_module,fd_make_cprim2("XMLGET",xmlget,2));
  fd_idefn(safe_module,fd_make_cprim2("XMLGET/FIRST",xmlget_first,2));
  fd_idefn(safe_module,fd_make_cprim2("XMLGET/SORTED",xmlget_sorted,2));
  fd_idefn(safe_module,fd_make_cprim2("XMLCONENTS",xmlcontents,1));
  fd_idefn(safe_module,fd_make_cprim2("XMLCONTENT",xmlcontent,1));
  fd_idefn(safe_module,fd_make_cprim2("XMLEMPTY?",xmlemptyp,1));

  name_slotid = fd_intern("%XMLTAG");
  content_slotid = fd_intern("%CONTENT");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
