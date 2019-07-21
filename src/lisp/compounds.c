/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/cons.h"
#include "kno/compounds.h"

#include <libu8/u8printf.h>
#include <libu8/u8timefns.h>

#include <stdarg.h>

lispval kno_compound_descriptor_type;

/* Compounds */

KNO_EXPORT lispval kno_init_compound_from_elts
(struct KNO_COMPOUND *p,lispval tag,int flags,int n,lispval *elts)
{
  int ismutable   = (flags&(KNO_COMPOUND_MUTABLE));
  int isopaque    = (flags&(KNO_COMPOUND_OPAQUE));
  int issequence  = (flags&(KNO_COMPOUND_SEQUENCE));
  int refmask     = (flags&(KNO_COMPOUND_REFMASK));
  int incref      = (refmask==KNO_COMPOUND_INCREF);
  int decref      = (refmask==KNO_COMPOUND_USEREF);
  int copyref     = (refmask==KNO_COMPOUND_COPYREF);
  lispval *write, *limit, *read = elts, initfn = KNO_FALSE;
  if (PRED_FALSE((n<0)))
    return kno_type_error(_("positive byte"),"kno_init_compound_from_elts",
			  KNO_SHORT2LISP(n));
  else if (p == NULL) {
    if (n==0)
      p = u8_malloc(sizeof(struct KNO_COMPOUND));
    else p = u8_malloc(sizeof(struct KNO_COMPOUND)+(n-1)*LISPVAL_LEN);}
  else NO_ELSE;
  struct KNO_TYPEINFO *info = kno_use_typeinfo(tag);
  if (n >= KNO_BIG_COMPOUND_LENGTH)
    u8_log(LOGWARN,"HugeCompound",
	   "Creating a compound of type %q with %d elements",tag,n);
  else NO_ELSE;
  KNO_INIT_CONS(p,kno_compound_type);
  if (ismutable) u8_init_rwlock(&(p->compound_rwlock));
  p->typetag = kno_incref(tag);
  p->typeinfo = info;
  p->compound_ismutable = ismutable;
  p->compound_isopaque = isopaque;
  if (issequence)
    p->compound_seqoff = KNO_COMPOUND_HEADER_LENGTH(flags);
  else p->compound_seqoff = -1;
  p->compound_length = n;
  if (n>0) {
    write = &(p->compound_0);
    limit = write+n;
    while (write<limit) {
      lispval value = *read++;
      if (value == KNO_NULL) {
	lispval *start = &(p->compound_0); int n =0;
	u8_byte buf[64];
	if ( (incref) || (copyref) || (decref) ) {
	  kno_decref_vec(start,write-start);}
	if (decref) {
	  kno_decref_vec(read,((elts+n)-read));}
	u8_free(p);
	return kno_err(kno_NullPtr,"kno_init_compound_from_elts",
		       u8_bprintf(buf,"at elt#%d",read-elts-1),
		       tag);}
      else if (copyref)
	value = kno_copier(value,0);
      else if (incref)
	kno_incref(value);
      else NO_ELSE;
      *write++ = value;}
    if (KNO_ABORTP(initfn)) {
      lispval *scan = &(p->compound_0);
      if ( (incref) || (copyref) || (decref) ) {
	while (scan<write) {
	  kno_decref(*scan);
	  scan++;}}
      return initfn;}
    else return LISP_CONS(p);}
  else return LISP_CONS(p);
}

KNO_EXPORT lispval kno_init_compound
(struct KNO_COMPOUND *p,lispval tag,int flags,int n,...)
{
  lispval elts[n];
  va_list args; int i = 0;
  if (PRED_FALSE((n<0)||(n>=256))) {
    /* Consume the arguments on error, just in case the vararg
       implementation is a little flaky. */
    va_start(args,n);
    while (i<n) {va_arg(args,lispval); i++;}
    return kno_type_error
      (_("positive byte"),"kno_init_compound",KNO_SHORT2LISP(n));}
  va_start(args,n);
  while (i<n) {
    lispval arg = va_arg(args,lispval);
    elts[i++] = arg;}
  return kno_init_compound_from_elts(p,tag,flags,n,elts);
}

KNO_EXPORT lispval kno_compound_ref
(lispval arg,lispval tag,int off,lispval dflt)
{
  struct KNO_COMPOUND *tvec = (struct KNO_COMPOUND *) arg;
  if (! ( (KNO_VOIDP(tag)) || (KNO_DEFAULTP(tag)) || 
	  (tvec->typetag == tag) ) ) {
    U8_STATIC_OUTPUT(details,512);
    kno_unparse(&details,tag);
    lispval errval =
      kno_err(kno_TypeError,"kno_compound_ref",details.u8_outbuf,arg);
    u8_close_output(&details);
    return errval;}
  else if (off >= tvec->compound_length)
    return kno_incref(dflt);
  else {
    lispval v = KNO_COMPOUND_VREF(tvec,off);
    kno_incref(v);
    return v;}
}

/* Init methods */

void kno_init_compounds_c()
{
  u8_register_source_file(_FILEINFO);
}
