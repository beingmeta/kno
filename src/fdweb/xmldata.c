/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define U8_INLINE_IO 1

#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/eval.h"
#include "fdb/ports.h"
#include "fdb/fdweb.h"

#include <libu8/xfiles.h>
#include <libu8/u8stringfns.h>

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

FD_EXPORT
void fd_init_xmldata_c()
{
  fdtype module=fd_new_module("FDWEB",(FD_MODULE_DEFAULT|FD_MODULE_SAFE));
  fd_idefn(module,fd_make_cprim2("XMLATTRIB",xmlattrib,2));
  fd_idefn(module,fd_make_cprim2("XMLGET",xmlget,2));

  name_slotid=fd_intern("%NAME");
  content_slotid=fd_intern("%CONTENT");
}
