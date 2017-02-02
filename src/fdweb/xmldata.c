/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
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

static fdtype name_slotid, content_slotid;

static fdtype xmlattrib(fdtype doc,fdtype attrib_id)
{
  if (FD_SLOTMAPP(doc))
    if (FD_SYMBOLP(attrib_id))
      return fd_get(doc,attrib_id,FD_EMPTY_CHOICE);
    else return FD_EMPTY_CHOICE;
  else return FD_EMPTY_CHOICE;
}

static void xmlget_helper(fdtype *result,fdtype doc,fdtype eltid,int cons)
{
  if ((FD_OIDP(doc)) || (FD_SLOTMAPP(doc)))
    if ((fd_test(doc,name_slotid,eltid))) {
      fd_incref(doc);
      if (cons) *result=fd_conspair(doc,*result);
      else {FD_ADD_TO_CHOICE((*result),doc);}}
    else {
      fdtype content=fd_get(doc,content_slotid,FD_EMPTY_CHOICE);
      xmlget_helper(result,content,eltid,cons);
      fd_decref(content);}
  else if ((FD_PAIRP(doc))||(FD_VECTORP(doc))) {
    FD_DOELTS(elt,doc,count) {
      if (FD_STRINGP(elt)) {}
      else xmlget_helper(result,elt,eltid,cons);}}
  else return;
}

static fdtype xmlget(fdtype doc,fdtype attrib_id)
{
  fdtype results=FD_EMPTY_CHOICE;
  xmlget_helper(&results,doc,attrib_id,0);
  return results;
}

static int listlen(fdtype l)
{
  if (!(FD_PAIRP(l))) return 0;
  else {
    int len=0; fdtype scan=l;
    while (FD_PAIRP(scan)) {scan=FD_CDR(scan); len++;}
    return len;}
}

static fdtype xmlget_sorted(fdtype doc,fdtype attrib_id)
{
  fdtype results=FD_EMPTY_LIST;
  xmlget_helper(&results,doc,attrib_id,1);
  if (FD_EMPTY_LISTP(results))
    return fd_make_vector(0,NULL);
  else {
    int len=listlen(results), i=len-1;
    fdtype vec=fd_make_vector(len,NULL), scan=results;
    while ((i>=0)&&(FD_PAIRP(scan))) {
      fdtype car=FD_CAR(scan);
      FD_VECTOR_SET(vec,i,car); fd_incref(car);
      scan=FD_CDR(scan); i--;}
    fd_decref(results);
    return vec;}
}

static fdtype xmlget_first(fdtype doc,fdtype attrib_id)
{
  fdtype results=FD_EMPTY_LIST;
  xmlget_helper(&results,doc,attrib_id,1);
  if (FD_EMPTY_LISTP(results))
    return FD_EMPTY_CHOICE;
  else {
    fdtype last_result=FD_VOID;
    FD_DOELTS(elt,results,count) {last_result=elt;}
    fd_incref(last_result);
    fd_decref(results);
    if (FD_VOIDP(last_result)) return FD_EMPTY_CHOICE;
    else return last_result;}
}

/* This returns the content field as parsed. */
static fdtype xmlcontents(fdtype doc,fdtype attrib_id)
{
  if ((FD_CHOICEP(doc)) || (FD_ACHOICEP(doc))) {
    fdtype contents=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(docelt,doc) {
      fdtype content=xmlcontents(docelt,attrib_id);
      FD_ADD_TO_CHOICE(contents,content);}
    return contents;}
  else if (FD_VOIDP(attrib_id))
    if (FD_EMPTY_LISTP(doc)) return doc;
    else if ((FD_PAIRP(doc)) || (FD_STRINGP(doc))) return fd_incref(doc);
    else if ((FD_OIDP(doc)) || (FD_SLOTMAPP(doc)))
      return fd_get(doc,content_slotid,FD_EMPTY_LIST);
    else return fd_type_error("XML node","xmlcontents",doc);
  else if ((FD_PAIRP(doc))||(FD_VECTORP(doc))) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DOELTS(docelt,doc,count) {
      fdtype contents=xmlcontents(docelt,attrib_id);
      if (!(FD_EMPTY_LISTP(contents))) {
        FD_ADD_TO_CHOICE(results,contents);}}
    return results;}
  else {
    fdtype value=fd_get(doc,attrib_id,FD_EMPTY_LIST);
    fdtype contents=xmlcontents(value,FD_VOID);
    fd_decref(value);
    return contents;}
}

/* This returns the content field as parsed. */
static fdtype xmlemptyp(fdtype elt,fdtype attribid)
{
  if (FD_VOIDP(attribid)) attribid=content_slotid;
  if (!(fd_test(elt,attribid,FD_VOID)))
    return FD_TRUE;
  else {
    fdtype content=fd_get(elt,attribid,FD_EMPTY_LIST);
    if ((FD_EMPTY_LISTP(content)) ||
        (FD_EMPTY_CHOICEP(content))||
        ((FD_VECTORP(content))&&
         (FD_VECTOR_LENGTH(content)==0)))
      return FD_TRUE;
    else {
      fd_decref(content);
      return FD_FALSE;}}
}

/* This returns the content field as a string. */
static fdtype xmlcontent(fdtype doc,fdtype attrib_id)
{
  if ((FD_CHOICEP(doc)) || (FD_ACHOICEP(doc))) {
    fdtype contents=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(docelt,doc) {
      fdtype content=xmlcontent(docelt,attrib_id);
      FD_ADD_TO_CHOICE(contents,content);}
    return contents;}
  else if (FD_VOIDP(attrib_id))
    if (FD_EMPTY_LISTP(doc)) return fd_init_string(NULL,0,u8_strdup(""));
    else if (FD_STRINGP(doc)) return fd_incref(doc);
    else if ((FD_PAIRP(doc))||(FD_VECTORP(doc))) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
      {FD_DOELTS(docelt,doc,count) {
          if (FD_STRINGP(docelt))
            u8_putn(&out,FD_STRDATA(docelt),FD_STRLEN(docelt));
          else if ((FD_OIDP(docelt)) || (FD_SLOTMAPP(docelt)))
            fd_unparse_xml(&out,docelt,NULL);
          else fd_unparse(&out,docelt);}}
      return fd_stream2string(&out);}
    else if ((FD_OIDP(doc)) || (FD_SLOTMAPP(doc))) {
      fdtype content=fd_get(doc,content_slotid,FD_EMPTY_LIST);
      fdtype as_string=xmlcontent(content,FD_VOID);
      fd_decref(content);
      return as_string;}
    else return fd_type_error("XML node","xmlcontent",doc);
  else if ((FD_PAIRP(doc))||(FD_VECTORP(doc))) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DOELTS(docelt,doc,count) {
      fdtype contents=xmlcontent(docelt,attrib_id);
      if (!(FD_EMPTY_LISTP(contents))) {
        FD_ADD_TO_CHOICE(results,contents);}}
    return results;}
  else {
    fdtype value=fd_get(doc,attrib_id,FD_EMPTY_LIST);
    fdtype contents=xmlcontent(value,FD_VOID);
    fd_decref(value);
    return contents;}
}

FD_EXPORT
void fd_init_xmldata_c()
{
  fdtype safe_module=fd_new_module("FDWEB",(0));
  fdtype module=fd_new_module("FDWEB",(FD_MODULE_SAFE));

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

  name_slotid=fd_intern("%XMLTAG");
  content_slotid=fd_intern("%CONTENT");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
