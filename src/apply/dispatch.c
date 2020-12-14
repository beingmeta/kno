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
#define KNO_INLINE_OBJTYPE 1
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

static lispval get_handler(lispval obj,lispval m)
{
  struct KNO_TYPEINFO *typeinfo = kno_objtype(obj);
  if (typeinfo == NULL) return KNO_VOID;
  return kno_get(typeinfo->type_props,m,KNO_VOID);
}

KNO_EXPORT lispval kno_handler(lispval obj,lispval m)
{
  return get_handler(obj,m);
}

KNO_EXPORT lispval kno_dispatch(lispval obj,lispval m,unsigned int flags,kno_argvec args)
{
  int n = flags&0xFFFF, no_error = (flags & KNO_DISPATCH_NOERR);
  int no_fail = (flags & KNO_DISPATCH_NOFAIL);
  struct KNO_TYPEINFO *typeinfo = kno_objtype(obj);
  lispval handler = (typeinfo) ? (kno_get(typeinfo->type_props,m,KNO_VOID)) : (KNO_VOID);
  if (KNO_VOIDP(handler)) {
    if ( (no_fail) || (no_error) ) return KNO_EMPTY;
    u8_byte buf[50];
    return kno_err("NoHandler","kno_dispatch",
		   (KNO_SYMBOLP(m)) ? (KNO_SYMBOL_NAME(m)) : (u8_bprintf(buf,"%q",m)),
		   obj);}
  else if (KNO_APPLICABLEP(handler)) {
    int include_object =
      ( ! ( (KNO_SYMBOLP(obj)) || (KNO_OIDP(obj)) ||
	    (KNO_TYPEP(obj,kno_ctype_type)) ||
	    (KNO_TYPEP(obj,kno_typeinfo_type)) ) );
    int n_args = (include_object) ? (n+1) : (n);
    u8_exception ex = u8_current_exception;
    lispval xargs[n_args];
    if (include_object) {
      xargs[0] = obj; memcpy(xargs+1,args,n*sizeof(lispval));}
    else {
      memcpy(xargs,args,n*sizeof(lispval));}
    lispval result = kno_apply(handler,n_args,xargs);
    kno_decref(handler);
    if ( (no_error) && (KNO_ABORTED(result)) ) {
      kno_pop_exceptions(ex,-1);
      kno_decref(handler);
      return KNO_EMPTY;}
    return result;}
  else if (no_error) {
    kno_decref(handler);
    return KNO_EMPTY;}
  else {
    u8_byte buf[50];
    kno_seterr("BadHandler","kno_dispatch",
	       (KNO_SYMBOLP(m)) ? (KNO_SYMBOL_NAME(m)) : (u8_bprintf(buf,"%q",m)),
	       handler);
    kno_decref(handler);
    return KNO_ERROR;}
}

DEF_KNOSYM(consfn); DEF_KNOSYM(stringfn);
DEF_KNOSYM(restorefn); DEF_KNOSYM(dumpfn);
DEF_KNOSYM(compound); DEF_KNOSYM(opaque);
DEF_KNOSYM(sequence); DEF_KNOSYM(mutable);
DEF_KNOSYM(annotated);

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
  lispval result = kno_dispatch(obj,KNOSYM(stringfn),KNO_DISPATCH_NOERR,NULL);
  if (KNO_STRINGP(result)) {
    u8_putn(out,KNO_CSTRING(result),KNO_STRLEN(result));
    kno_decref(result);
    return 1;}
  else {
    kno_decref(result);
    return 0;}
}

static lispval default_restorefn(lispval type,lispval dump,kno_typeinfo e)
{
  int flags = KNO_COMPOUND_INCREF;
  if ( (e->type_props) && (KNO_TABLEP(e->type_props)) ) {
    lispval method = kno_get(e->type_props,KNOSYM(restorefn),VOID);
    if (KNO_APPLICABLEP(method)) {
      lispval args[2] = { type, dump };
      lispval result = kno_apply(method,2,args);
      if (! ( (KNO_VOIDP(result)) || (KNO_EMPTYP(result)) ) ) {
	kno_decref(method);
	return result;}
      else if (KNO_ABORTED(result)) {
	u8_log(LOGERR,"RestoreFailed",
	       "For %q restore method %q and data %q",
	       e->typetag,method,dump);
	kno_decref(method);}}}
  if ( (e->type_props) && (KNO_TABLEP(e->type_props)) ) {
    lispval props = e->type_props;
    if (kno_test(props,KNOSYM(compound),KNOSYM(opaque)))
      flags |= KNO_COMPOUND_OPAQUE;
    if (kno_test(props,KNOSYM(compound),KNOSYM(mutable)))
      flags |= KNO_COMPOUND_MUTABLE;
    if (kno_test(props,KNOSYM(compound),KNOSYM(annotated)))
      flags |= KNO_COMPOUND_TABLE;
    if (kno_test(props,KNOSYM(compound),KNOSYM(sequence))) {
      flags |= KNO_COMPOUND_SEQUENCE;
      if (flags & KNO_COMPOUND_TABLE) flags |= (1<<8);}}
  /* This is the default restore method */
  if (KNO_VECTORP(dump))
    return kno_init_compound_from_elts(NULL,e->typetag,flags,
				       KNO_VECTOR_LENGTH(dump),
				       KNO_VECTOR_ELTS(dump));
  else return kno_init_compound_from_elts(NULL,e->typetag,flags,1,&dump);
}

static lispval default_dumpfn(lispval obj,kno_typeinfo e)
{
  if ( (e->type_props) && (KNO_TABLEP(e->type_props)) ) {
    lispval method = kno_get(e->type_props,KNOSYM(dumpfn),VOID);
    if (KNO_APPLICABLEP(method)) {
      lispval result = kno_apply(method,1,&obj);
      if (! ( (KNO_VOIDP(result)) || (KNO_EMPTYP(result)) ) ) {
	kno_decref(method);
	return result;}
      else if (KNO_ABORTED(result)) {
	u8_log(LOGERR,"RestoreFailed",
	       "For %q dump method %q and data %q",
	       e->typetag,method,obj);
	kno_decref(method);}}
    return KNO_FALSE;}
  else return KNO_FALSE;
}

KNO_EXPORT void kno_init_dispatch_c()
{
  kno_default_unparsefn = default_unparsefn;
  kno_default_restorefn = default_restorefn;
  kno_default_dumpfn = default_dumpfn;
  kno_default_consfn = default_consfn;

  KNOSYM(restorefn);
  KNOSYM(dumpfn);
  KNOSYM(stringfn);
  KNOSYM(consfn);
  KNOSYM(compound);
  KNOSYM(opaque);
  KNOSYM(sequence);
  KNOSYM(mutable);
  KNOSYM(annotated);
}


