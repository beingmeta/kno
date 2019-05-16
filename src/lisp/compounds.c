/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/cons.h"
#include "kno/compounds.h"

#include <libu8/u8printf.h>
#include <libu8/u8timefns.h>

#include <stdarg.h>

lispval kno_compound_descriptor_type;

/* Compounds */

KNO_EXPORT lispval kno_init_compound
  (struct KNO_COMPOUND *p,lispval tag,int flags,int n,...)
{
  lispval *write, *limit, initfn = KNO_FALSE;
  int ismutable   = (flags&(KNO_COMPOUND_MUTABLE));
  int isopaque    = (flags&(KNO_COMPOUND_OPAQUE));
  int issequence  = (flags&(KNO_COMPOUND_SEQUENCE));
  int istable     = (flags&(KNO_COMPOUND_TABLE));
  int refmask     = (flags&(KNO_COMPOUND_REFMASK));
  int incref      = (refmask==KNO_COMPOUND_INCREF);
  int decref      = (refmask==KNO_COMPOUND_USEREF);
  int copyref     = (refmask==KNO_COMPOUND_COPYREF);
  va_list args; int i = 0;
  if (n<0) {
    kno_seterr("NegativeLength","kno_init_compound",NULL,KNO_INT(n));
    return KNO_ERROR;}
  if (PRED_FALSE((n<0)||(n>=256))) {
    /* Consume the arguments on error, just in case the vararg
       implementation is a little flaky. */
    va_start(args,n);
    while (i<n) {va_arg(args,lispval); i++;}
    return kno_type_error
      (_("positive byte"),"kno_init_compound",KNO_SHORT2DTYPE(n));}
  else if (p == NULL) {
    if (n==0) p = u8_malloc(sizeof(struct KNO_COMPOUND));
    else p = u8_malloc(sizeof(struct KNO_COMPOUND)+(n-1)*LISPVAL_LEN);}
  KNO_INIT_CONS(p,kno_compound_type);
  if (ismutable) u8_init_mutex(&(p->compound_lock));
  p->compound_typetag = kno_incref(tag);
  p->compound_ismutable = ismutable;
  p->compound_isopaque = isopaque;
  p->compound_istable = istable;
  if (issequence)
    p->compound_off = KNO_COMPOUND_HEADER_LENGTH(flags);
  else p->compound_off = -1;
  p->compound_length = n;
  if (n > 0) {
    write = &(p->compound_0);
    limit = write+n;
    va_start(args,n);
    while (write<limit) {
      lispval value = va_arg(args,lispval);
      if (copyref)
	value = kno_copier(value,0);
      else if (incref)
	kno_incref(value);
      else NO_ELSE;
      *write = value;
      write++;}
    va_end(args);
    if (KNO_ABORTP(initfn)) {
      lispval *scan = &(p->compound_0);
      if ( (incref) || (copyref) || (decref) ) {
	while (scan<write) {kno_decref(*scan); scan++;}}
      return initfn;}
    else return LISP_CONS(p);}
  else return LISP_CONS(p);
}

KNO_EXPORT lispval kno_init_compound_from_elts
  (struct KNO_COMPOUND *p,lispval tag,int flags,int n,lispval *elts)
{
  int ismutable   = (flags&(KNO_COMPOUND_MUTABLE));
  int isopaque    = (flags&(KNO_COMPOUND_OPAQUE));
  int issequence  = (flags&(KNO_COMPOUND_SEQUENCE));
  int istable     = (flags&(KNO_COMPOUND_TABLE));
  int refmask     = (flags&(KNO_COMPOUND_REFMASK));
  int incref      = (refmask==KNO_COMPOUND_INCREF);
  int decref      = (refmask==KNO_COMPOUND_USEREF);
  int copyref     = (refmask==KNO_COMPOUND_COPYREF);
  lispval *write, *limit, *read = elts, initfn = KNO_FALSE;
  if (PRED_FALSE((n<0)))
    return kno_type_error(_("positive byte"),"kno_init_compound_from_elts",
			 KNO_SHORT2DTYPE(n));
  else if (p == NULL) {
    if (n==0)
      p = u8_malloc(sizeof(struct KNO_COMPOUND));
    else p = u8_malloc(sizeof(struct KNO_COMPOUND)+(n-1)*LISPVAL_LEN);}
  else NO_ELSE;
  if (n >= KNO_BIG_COMPOUND_LENGTH)
    u8_log(LOGWARN,"HugeCompound",
	   "Creating a compound of type %q with %d elements",tag,n);
  else NO_ELSE;
  KNO_INIT_CONS(p,kno_compound_type);
  if (ismutable) u8_init_mutex(&(p->compound_lock));
  p->compound_typetag = kno_incref(tag);
  p->compound_ismutable = ismutable;
  p->compound_isopaque = isopaque;
  p->compound_istable = istable;
  if (issequence)
    p->compound_off = KNO_COMPOUND_HEADER_LENGTH(flags);
  else p->compound_off = -1;
  p->compound_length = n;
  if (n>0) {
    write = &(p->compound_0); limit = write+n;
    while (write<limit) {
      lispval value = *read++;
      if (copyref)
	value = kno_copier(value,0);
      else if (incref)
	kno_incref(value);
      else NO_ELSE;
      *write++ = value;}
    if (KNO_ABORTP(initfn)) {
      lispval *scan = &(p->compound_0);
      if ( (incref) || (copyref) || (decref) ) {
	while (scan<write) {kno_decref(*scan); scan++;}}
      return initfn;}
    else return LISP_CONS(p);}
  else return LISP_CONS(p);
}

KNO_EXPORT lispval kno_compound_ref(lispval arg,lispval tag,int off,lispval dflt)
{
  struct KNO_COMPOUND *tvec = (struct KNO_COMPOUND *) arg;
  if (! ( (KNO_VOIDP(tag)) || (tvec->compound_typetag == tag) ) ) {
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

/* Compound type information */

struct KNO_COMPOUND_TYPEINFO *kno_compound_entries = NULL;
static u8_mutex compound_registry_lock;

KNO_EXPORT
struct KNO_COMPOUND_TYPEINFO
*kno_register_compound(lispval symbol,lispval *datap,int *corep)
{
  struct KNO_COMPOUND_TYPEINFO *scan, *newrec;
  u8_lock_mutex(&compound_registry_lock);
  scan = kno_compound_entries;
  while (scan)
    if (KNO_EQ(scan->compound_typetag,symbol)) {
      if (datap) {
	lispval data = *datap;
	if (VOIDP(scan->compound_metadata)) {
	  scan->compound_metadata = data;
	  kno_incref(data);}
	else {
	  lispval data = *datap; kno_decref(data);
	  data = scan->compound_metadata;
	  kno_incref(data);
	  *datap = data;}}
      if (corep) {
	if (scan->compound_corelen<0)
	  scan->compound_corelen = *corep;
	else *corep = scan->compound_corelen;}
      u8_unlock_mutex(&compound_registry_lock);
      return scan;}
    else scan = scan->compound_nextinfo;
  newrec = u8_alloc(struct KNO_COMPOUND_TYPEINFO);
  memset(newrec,0,sizeof(struct KNO_COMPOUND_TYPEINFO));
  if (datap) {
    lispval data = *datap;
    kno_incref(data);
    newrec->compound_metadata = data;}
  else newrec->compound_metadata = VOID;
  newrec->compound_corelen = ((corep)?(*corep):(-1));
  newrec->compound_nextinfo = kno_compound_entries;
  newrec->compound_typetag = symbol;
  newrec->compound_parser = NULL;
  newrec->compound_dumpfn = NULL;
  newrec->compound_restorefn = NULL;
  newrec->compund_tablefns = NULL;
  kno_compound_entries = newrec;
  u8_unlock_mutex(&compound_registry_lock);
  return newrec;
}

KNO_EXPORT struct KNO_COMPOUND_TYPEINFO
*kno_declare_compound(lispval symbol,lispval data,int core_slots)
{
  struct KNO_COMPOUND_TYPEINFO *scan, *newrec;
  u8_lock_mutex(&compound_registry_lock);
  scan = kno_compound_entries;
  while (scan)
    if (KNO_EQ(scan->compound_typetag,symbol)) {
      if (!(VOIDP(data))) {
	lispval old_data = scan->compound_metadata;
	scan->compound_metadata = kno_incref(data);
	kno_decref(old_data);}
      if (core_slots>0) scan->compound_corelen = core_slots;
      u8_unlock_mutex(&compound_registry_lock);
      return scan;}
    else scan = scan->compound_nextinfo;
  newrec = u8_alloc(struct KNO_COMPOUND_TYPEINFO);
  memset(newrec,0,sizeof(struct KNO_COMPOUND_TYPEINFO));
  newrec->compound_metadata = data;
  newrec->compound_corelen = core_slots;
  newrec->compound_typetag = symbol;
  newrec->compound_nextinfo = kno_compound_entries;
  newrec->compound_parser = NULL;
  newrec->compound_dumpfn = NULL;
  newrec->compound_freefn = NULL;
  newrec->compound_restorefn = NULL;
  newrec->compund_tablefns = NULL;
  kno_compound_entries = newrec;
  u8_unlock_mutex(&compound_registry_lock);
  return newrec;
}

KNO_EXPORT struct KNO_COMPOUND_TYPEINFO *kno_lookup_compound(lispval symbol)
{
  struct KNO_COMPOUND_TYPEINFO *scan = kno_compound_entries;
  while (scan)
    if (KNO_EQ(scan->compound_typetag,symbol)) {
      return scan;}
    else scan = scan->compound_nextinfo;
  return NULL;
}

KNO_EXPORT
int kno_compound_unparser(u8_string pname,kno_compound_unparsefn fn)
{
  lispval sym = kno_intern(pname);
  struct KNO_COMPOUND_TYPEINFO *typeinfo = kno_register_compound(sym,NULL,NULL);
  if (typeinfo) {
    typeinfo->compound_unparser = fn;
    return 1;}
  else return 0;
}

/* Init methods */

void kno_init_compounds_c()
{
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&compound_registry_lock);

  kno_compound_descriptor_type=
    kno_init_compound
    (NULL,VOID,KNO_COMPOUND_MUTABLE,9,
     kno_intern("COMPOUNDTYPE"),0,KNO_INT(9),
     kno_make_nvector(9,FDSYM_TAG,FDSYM_LENGTH,
		     kno_intern("FIELDS"),kno_intern("INITFN"),
		     kno_intern("FREEFN"),kno_intern("COMPAREFN"),
		     kno_intern("STRINGFN"),kno_intern("DUMPFN"),
		     kno_intern("RESTOREFN")),
     KNO_FALSE,KNO_FALSE,KNO_FALSE,KNO_FALSE,
     KNO_FALSE,KNO_FALSE);
  ((kno_compound)kno_compound_descriptor_type)
    ->compound_typetag = kno_compound_descriptor_type;
  kno_incref(kno_compound_descriptor_type);
}
