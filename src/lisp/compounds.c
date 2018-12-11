/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/cons.h"
#include "framerd/compounds.h"

#include <libu8/u8printf.h>
#include <libu8/u8timefns.h>

#include <stdarg.h>

lispval fd_compound_descriptor_type;

/* Compounds */

FD_EXPORT lispval fd_init_compound
  (struct FD_COMPOUND *p,lispval tag,int flags,int n,...)
{
  lispval *write, *limit, initfn = FD_FALSE;
  int ismutable   = (flags&(FD_COMPOUND_MUTABLE));
  int isopaque    = (flags&(FD_COMPOUND_OPAQUE));
  int issequence  = (flags&(FD_COMPOUND_SEQUENCE));
  int istable     = (flags&(FD_COMPOUND_TABLE));
  int refmask     = (flags&(FD_COMPOUND_REFMASK));
  int incref      = (refmask==FD_COMPOUND_INCREF);
  int decref      = (refmask==FD_COMPOUND_USEREF);
  int copyref     = (refmask==FD_COMPOUND_COPYREF);
  va_list args; int i = 0;
  if (n<0) {
    fd_seterr("NegativeLength","fd_init_compound",NULL,FD_INT(n));
    return FD_ERROR;}
  if (PRED_FALSE((n<0)||(n>=256))) {
    /* Consume the arguments on error, just in case the vararg
       implementation is a little flaky. */
    va_start(args,n);
    while (i<n) {va_arg(args,lispval); i++;}
    return fd_type_error
      (_("positive byte"),"fd_init_compound",FD_SHORT2DTYPE(n));}
  else if (p == NULL) {
    if (n==0) p = u8_malloc(sizeof(struct FD_COMPOUND));
    else p = u8_malloc(sizeof(struct FD_COMPOUND)+(n-1)*LISPVAL_LEN);}
  FD_INIT_CONS(p,fd_compound_type);
  if (ismutable) u8_init_mutex(&(p->compound_lock));
  p->compound_typetag = fd_incref(tag);
  p->compound_ismutable = ismutable;
  p->compound_isopaque = isopaque;
  p->compound_istable = istable;
  if (issequence)
    p->compound_off = FD_COMPOUND_HEADER_LENGTH(flags);
  else p->compound_off = -1;
  p->compound_length = n;
  if (n > 0) {
    write = &(p->compound_0);
    limit = write+n;
    va_start(args,n);
    while (write<limit) {
      lispval value = va_arg(args,lispval);
      if (copyref)
	value = fd_copier(value,0);
      else if (incref)
	fd_incref(value);
      else NO_ELSE;
      *write = value;
      write++;}
    va_end(args);
    if (FD_ABORTP(initfn)) {
      lispval *scan = &(p->compound_0);
      if ( (incref) || (copyref) || (decref) ) {
	while (scan<write) {fd_decref(*scan); scan++;}}
      return initfn;}
    else return LISP_CONS(p);}
  else return LISP_CONS(p);
}

FD_EXPORT lispval fd_init_compound_from_elts
  (struct FD_COMPOUND *p,lispval tag,int flags,int n,lispval *elts)
{
  int ismutable   = (flags&(FD_COMPOUND_MUTABLE));
  int isopaque    = (flags&(FD_COMPOUND_OPAQUE));
  int issequence  = (flags&(FD_COMPOUND_SEQUENCE));
  int istable     = (flags&(FD_COMPOUND_TABLE));
  int refmask     = (flags&(FD_COMPOUND_REFMASK));
  int incref      = (refmask==FD_COMPOUND_INCREF);
  int decref      = (refmask==FD_COMPOUND_USEREF);
  int copyref     = (refmask==FD_COMPOUND_COPYREF);
  lispval *write, *limit, *read = elts, initfn = FD_FALSE;
  if (PRED_FALSE((n<0)))
    return fd_type_error(_("positive byte"),"fd_init_compound_from_elts",
			 FD_SHORT2DTYPE(n));
  else if (p == NULL) {
    if (n==0)
      p = u8_malloc(sizeof(struct FD_COMPOUND));
    else p = u8_malloc(sizeof(struct FD_COMPOUND)+(n-1)*LISPVAL_LEN);}
  else NO_ELSE;
  if (n >= FD_BIG_COMPOUND_LENGTH)
    u8_log(LOGWARN,"HugeCompound",
	   "Creating a compound of type %q with %d elements",tag,n);
  else NO_ELSE;
  FD_INIT_CONS(p,fd_compound_type);
  if (ismutable) u8_init_mutex(&(p->compound_lock));
  p->compound_typetag = fd_incref(tag);
  p->compound_ismutable = ismutable;
  p->compound_isopaque = isopaque;
  p->compound_istable = istable;
  if (issequence)
    p->compound_off = FD_COMPOUND_HEADER_LENGTH(flags);
  else p->compound_off = -1;
  p->compound_length = n;
  if (n>0) {
    write = &(p->compound_0); limit = write+n;
    while (write<limit) {
      lispval value = *read++;
      if (copyref)
	value = fd_copier(value,0);
      else if (incref)
	fd_incref(value);
      else NO_ELSE;
      *write++ = value;}
    if (FD_ABORTP(initfn)) {
      lispval *scan = &(p->compound_0);
      if ( (incref) || (copyref) || (decref) ) {
	while (scan<write) {fd_decref(*scan); scan++;}}
      return initfn;}
    else return LISP_CONS(p);}
  else return LISP_CONS(p);
}

FD_EXPORT lispval fd_compound_ref(lispval arg,lispval tag,int off,lispval dflt)
{
  struct FD_COMPOUND *tvec = (struct FD_COMPOUND *) arg;
  if (! ( (FD_VOIDP(tag)) || (tvec->compound_typetag == tag) ) ) {
    U8_STATIC_OUTPUT(details,512);
    fd_unparse(&details,tag);
    lispval errval =
      fd_err(fd_TypeError,"fd_compound_ref",details.u8_outbuf,arg);
    u8_close_output(&details);
    return errval;}
  else if (off >= tvec->compound_length)
    return fd_incref(dflt);
  else {
    lispval v = FD_COMPOUND_VREF(tvec,off);
    fd_incref(v);
    return v;}
}

/* Compound type information */

struct FD_COMPOUND_TYPEINFO *fd_compound_entries = NULL;
static u8_mutex compound_registry_lock;

FD_EXPORT
struct FD_COMPOUND_TYPEINFO
*fd_register_compound(lispval symbol,lispval *datap,int *corep)
{
  struct FD_COMPOUND_TYPEINFO *scan, *newrec;
  u8_lock_mutex(&compound_registry_lock);
  scan = fd_compound_entries;
  while (scan)
    if (FD_EQ(scan->compound_typetag,symbol)) {
      if (datap) {
	lispval data = *datap;
	if (VOIDP(scan->compound_metadata)) {
	  scan->compound_metadata = data;
	  fd_incref(data);}
	else {
	  lispval data = *datap; fd_decref(data);
	  data = scan->compound_metadata;
	  fd_incref(data);
	  *datap = data;}}
      if (corep) {
	if (scan->compound_corelen<0)
	  scan->compound_corelen = *corep;
	else *corep = scan->compound_corelen;}
      u8_unlock_mutex(&compound_registry_lock);
      return scan;}
    else scan = scan->compound_nextinfo;
  newrec = u8_alloc(struct FD_COMPOUND_TYPEINFO);
  memset(newrec,0,sizeof(struct FD_COMPOUND_TYPEINFO));
  if (datap) {
    lispval data = *datap;
    fd_incref(data);
    newrec->compound_metadata = data;}
  else newrec->compound_metadata = VOID;
  newrec->compound_corelen = ((corep)?(*corep):(-1));
  newrec->compound_nextinfo = fd_compound_entries;
  newrec->compound_typetag = symbol;
  newrec->compound_parser = NULL;
  newrec->compound_dumpfn = NULL;
  newrec->compound_restorefn = NULL;
  newrec->compund_tablefns = NULL;
  fd_compound_entries = newrec;
  u8_unlock_mutex(&compound_registry_lock);
  return newrec;
}

FD_EXPORT struct FD_COMPOUND_TYPEINFO
*fd_declare_compound(lispval symbol,lispval data,int core_slots)
{
  struct FD_COMPOUND_TYPEINFO *scan, *newrec;
  u8_lock_mutex(&compound_registry_lock);
  scan = fd_compound_entries;
  while (scan)
    if (FD_EQ(scan->compound_typetag,symbol)) {
      if (!(VOIDP(data))) {
	lispval old_data = scan->compound_metadata;
	scan->compound_metadata = fd_incref(data);
	fd_decref(old_data);}
      if (core_slots>0) scan->compound_corelen = core_slots;
      u8_unlock_mutex(&compound_registry_lock);
      return scan;}
    else scan = scan->compound_nextinfo;
  newrec = u8_alloc(struct FD_COMPOUND_TYPEINFO);
  memset(newrec,0,sizeof(struct FD_COMPOUND_TYPEINFO));
  newrec->compound_metadata = data;
  newrec->compound_corelen = core_slots;
  newrec->compound_typetag = symbol;
  newrec->compound_nextinfo = fd_compound_entries;
  newrec->compound_parser = NULL;
  newrec->compound_dumpfn = NULL;
  newrec->compound_freefn = NULL;
  newrec->compound_restorefn = NULL;
  newrec->compund_tablefns = NULL;
  fd_compound_entries = newrec;
  u8_unlock_mutex(&compound_registry_lock);
  return newrec;
}

FD_EXPORT struct FD_COMPOUND_TYPEINFO *fd_lookup_compound(lispval symbol)
{
  struct FD_COMPOUND_TYPEINFO *scan = fd_compound_entries;
  while (scan)
    if (FD_EQ(scan->compound_typetag,symbol)) {
      return scan;}
    else scan = scan->compound_nextinfo;
  return NULL;
}

FD_EXPORT
int fd_compound_unparser(u8_string pname,fd_compound_unparsefn fn)
{
  lispval sym = fd_intern(pname);
  struct FD_COMPOUND_TYPEINFO *typeinfo = fd_register_compound(sym,NULL,NULL);
  if (typeinfo) {
    typeinfo->compound_unparser = fn;
    return 1;}
  else return 0;
}

/* Init methods */

void fd_init_compounds_c()
{
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&compound_registry_lock);

  fd_compound_descriptor_type=
    fd_init_compound
    (NULL,VOID,FD_COMPOUND_MUTABLE,9,
     fd_intern("COMPOUNDTYPE"),0,FD_INT(9),
     fd_make_nvector(9,FDSYM_TAG,FDSYM_LENGTH,
		     fd_intern("FIELDS"),fd_intern("INITFN"),
		     fd_intern("FREEFN"),fd_intern("COMPAREFN"),
		     fd_intern("STRINGFN"),fd_intern("DUMPFN"),
		     fd_intern("RESTOREFN")),
     FD_FALSE,FD_FALSE,FD_FALSE,FD_FALSE,
     FD_FALSE,FD_FALSE);
  ((fd_compound)fd_compound_descriptor_type)
    ->compound_typetag = fd_compound_descriptor_type;
  fd_incref(fd_compound_descriptor_type);
}
