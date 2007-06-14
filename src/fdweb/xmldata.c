/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#define U8_INLINE_IO 1

#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/eval.h"
#include "fdb/ports.h"
#include "fdb/fdweb.h"

#include <libu8/xfiles.h>
#include <libu8/u8stringfns.h>

static MAYBE_UNUSED char versionid[] =
  "$Id$";

static fdtype name_slotid, content_slotid;

static fdtype xmlattrib(fdtype doc,fdtype attrib_id)
{
  if (FD_SLOTMAPP(doc))
    if (FD_SYMBOLP(attrib_id))
      return fd_get(doc,attrib_id,FD_EMPTY_CHOICE);
    else return FD_EMPTY_CHOICE;
  else return FD_EMPTY_CHOICE;
}

static void xmlget_helper(fdtype *result,fdtype doc,fdtype eltid)
{
  if (FD_SLOTMAPP(doc))
    if ((fd_test(doc,name_slotid,eltid))) {
      FD_ADD_TO_CHOICE((*result),fd_incref(doc));}
    else {
      fdtype content=fd_get(doc,content_slotid,FD_EMPTY_CHOICE);
      xmlget_helper(result,content,eltid);
      fd_decref(content);}
  else if (FD_PAIRP(doc)) {
    FD_DOLIST(elt,doc)
      if (FD_STRINGP(elt)) {}
      else xmlget_helper(result,elt,eltid);}
  else return;
}

static fdtype xmlget(fdtype doc,fdtype attrib_id)
{
  fdtype results=FD_EMPTY_CHOICE;
  xmlget_helper(&results,doc,attrib_id);
  return results;
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
    else if (FD_SLOTMAPP(doc)) 
      return fd_get(doc,content_slotid,FD_EMPTY_LIST);
    else return fd_type_error("XML node","xmlcontents",doc);
  else if (FD_PAIRP(doc)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DOLIST(docelt,doc) {
      fdtype contents=xmlcontents(docelt,attrib_id);
      if (!(FD_EMPTY_LISTP(contents))) {
	FD_ADD_TO_CHOICE(results,contents);}
      return results;}}
  else {
    fdtype value=fd_get(doc,attrib_id,FD_EMPTY_LIST);
    fdtype contents=xmlcontents(value,FD_VOID);
    fd_decref(value);
    return contents;}
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
    else if (FD_PAIRP(doc)) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
      {FD_DOLIST(docelt,doc)
	 if (FD_STRINGP(docelt))
	   u8_putn(&out,FD_STRDATA(docelt),FD_STRLEN(docelt));
	 else if (FD_SLOTMAPP(docelt))
	   fd_unparse_xml(&out,docelt,NULL);
	 else fd_unparse(&out,docelt);}
      return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
    else if (FD_SLOTMAPP(doc)) {
      fdtype content=fd_get(doc,content_slotid,FD_EMPTY_LIST);
      fdtype as_string=xmlcontent(content,FD_VOID);
      fd_decref(content);
      return as_string;}
    else return fd_type_error("XML node","xmlcontent",doc);
  else if (FD_PAIRP(doc)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DOLIST(docelt,doc) {
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
  fdtype module=fd_new_module("FDWEB",(FD_MODULE_DEFAULT|FD_MODULE_SAFE));
  fd_idefn(module,fd_make_cprim2("XMLATTRIB",xmlattrib,2));
  fd_idefn(module,fd_make_cprim2("XMLGET",xmlget,2));
  fd_idefn(module,fd_make_cprim2("XMLCONENTS",xmlcontents,1));
  fd_idefn(module,fd_make_cprim2("XMLCONTENT",xmlcontent,1));

  name_slotid=fd_intern("%NAME");
  content_slotid=fd_intern("%CONTENT");
}
