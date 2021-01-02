/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_FCNIDS 1
#define KNO_INLINE_APPLY  1
#define KNO_SOURCE 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/lexenv.h"
#include "kno/stacks.h"
#include "kno/apply.h"
#include "apply_internals.h"

#include <libu8/u8printf.h>
#include <libu8/u8contour.h>
#include <libu8/u8strings.h>

#include <errno.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#if TIME_WITH_SYS_TIME
#include <time.h>
#include <sys/time.h>
#elif HAVE_TIME_H
#include <time.h>
#elif HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

#include <sys/resource.h>
#include "kno/profiles.h"

#include <stdarg.h>

/* Send */

static lispval get_handler(lispval obj,lispval m,lispval interface)
{
  struct KNO_TYPEINFO *typeinfo = kno_taginfo(obj);
  if (typeinfo == NULL) return KNO_VOID;
  lispval handlers = KNO_VOID; int free_handlers = 0;
  if (KNO_VOIDP(interface))
    handlers = typeinfo->type_props;
  else {
    handlers = kno_get(typeinfo->type_props,interface,KNO_VOID);
    if (!(KNO_VOIDP(handlers))) free_handlers=1;
    else handlers = typeinfo->type_props;}
  if (RARELY(KNO_VOIDP(handlers))) return KNO_VOID;
  lispval handler = kno_get(handlers,m,KNO_VOID);
  if ( (KNO_VOIDP(handler)) && (KNO_VOIDP(interface)) )
    handler = kno_get(typeinfo->type_props,m,KNO_VOID);
  if (free_handlers) kno_decref(handlers);
  return handler;
}

KNO_EXPORT lispval kno_handler(lispval obj,lispval m,lispval interface)
{
  return get_handler(obj,m,interface);
}

KNO_EXPORT lispval kno_dispatch(lispval obj,lispval m,lispval interface,
				int err,int n,kno_argvec args)
{
  struct KNO_TYPEINFO *typeinfo = kno_taginfo(obj);
  if (typeinfo == NULL) {
    if (!(err)) return KNO_EMPTY;
    kno_seterr("BadObject","kno_type_handle",
	       (KNO_SYMBOLP(m))?(KNO_SYMBOL_NAME(m)):(NULL),
	       interface);
    return KNO_ERROR;}
  lispval handlers = KNO_VOID; int free_handlers = 0;
  if (KNO_VOIDP(interface))
    handlers = typeinfo->type_props;
  else {
    handlers = kno_get(typeinfo->type_props,interface,KNO_VOID);
    if (!(KNO_VOIDP(handlers))) free_handlers=1;
    else handlers = typeinfo->type_props;}
  if (RARELY(KNO_VOIDP(handlers))) {
    if (free_handlers) kno_decref(handlers);
    if (!(err)) return KNO_EMPTY;
    kno_seterr("NoHandlers","kno_type_handle",
	       (KNO_SYMBOLP(m))?(KNO_SYMBOL_NAME(m)):(NULL),
	       interface);
    return KNO_ERROR;}
  lispval handler = kno_get(handlers,m,KNO_VOID);
  if ( (KNO_VOIDP(handler)) && (KNO_VOIDP(interface)) )
    handler = kno_get(typeinfo->type_props,m,KNO_VOID);
  if (KNO_VOIDP(handler)) {
    if (free_handlers) kno_decref(handlers);
    if (!(err)) return KNO_EMPTY;
    kno_seterr("NoHandler","kno_dispatch",
	       (KNO_SYMBOLP(m))?(KNO_SYMBOL_NAME(m)):(NULL),
	       interface);
    return KNO_ERROR;}
  else if (KNO_APPLICABLEP(handler)) {
    u8_exception ex = u8_current_exception;
    lispval xargs[n+1]; xargs[0] = obj;
    memcpy(xargs+1,args,n*sizeof(lispval));
    lispval result = kno_apply(handler,n+1,xargs);
    if (free_handlers) kno_decref(handlers);
    kno_decref(handler);
    if ( (err==0) && (KNO_ABORTED(result)) ) {
      kno_pop_exceptions(ex,-1);
      return KNO_EMPTY;}
    return result;}
  else {
    if (free_handlers) kno_decref(handlers);
    if (!(err)) return KNO_EMPTY;
    kno_seterr("BadHandler","kno_dispatch",
	       (KNO_SYMBOLP(m))?(KNO_SYMBOL_NAME(m)):(NULL),
	       handler);
    return KNO_ERROR;}
}

DEF_KNOSYM(consfn); DEF_KNOSYM(stringfn);

static lispval default_consfn(int n,lispval *args,kno_typeinfo e)
{
  if (e->type_props) {
    lispval method = kno_get(e->type_props,KNOSYM(consfn),VOID);
    if (VOIDP(method))
      return VOID;
    else {
      lispval result = kno_apply(method,n,args);
      if (! ( (KNO_VOIDP(result)) || (KNO_EMPTYP(result)) ) ) {
	kno_decref(method);
	return result;}}}
  /* This is the default cons method */
  return kno_init_compound_from_elts
    (NULL,e->typetag,KNO_COMPOUND_INCREF,n,args);
}

static int default_unparsefn(u8_output out,lispval obj,kno_typeinfo info)
{
  lispval result = kno_dispatch(obj,KNOSYM(stringfn),KNO_VOID,
				KNO_DISPATCH_NOERR,0,NULL);
  if (KNO_STRINGP(result)) {
    u8_putn(out,KNO_CSTRING(result),KNO_STRLEN(result));
    kno_decref(result);
    return 1;}
  else {
    kno_decref(result);
    return 0;}
}

KNO_EXPORT void kno_init_dispatch_c()
{
  kno_default_unparsefn = default_unparsefn;
  kno_default_consfn = default_consfn;

  KNOSYM(stringfn);
  KNOSYM(consfn);
}

