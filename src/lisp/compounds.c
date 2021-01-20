/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/cons.h"
#include "kno/sequences.h"
#include "kno/tables.h"
#include "kno/compounds.h"

#include <libu8/u8printf.h>
#include <libu8/u8timefns.h>

#include <stdarg.h>

lispval kno_compound_descriptor_type;

/* Compounds */

KNO_EXPORT lispval kno_init_compound_from_elts
(struct KNO_COMPOUND *p,lispval tag,int flags,int n,
 lispval *elts)
{
  int ismutable   = (flags&(KNO_COMPOUND_MUTABLE));
  int isopaque    = (flags&(KNO_COMPOUND_OPAQUE));
  int issequence  = (flags&(KNO_COMPOUND_SEQUENCE));
  int istable     = (flags&(KNO_COMPOUND_TABLE));
  int refmask     = (flags&(KNO_COMPOUND_REFMASK));
  int incref      = (refmask==KNO_COMPOUND_INCREF);
  int decref      = (refmask==KNO_COMPOUND_USEREF);
  int copyref     = (refmask==KNO_COMPOUND_COPYREF);
  lispval *write, *limit;
  lispval *read = elts;
  if (RARELY((n<0)))
    return kno_type_error(_("positive byte"),"kno_init_compound_from_elts",
			  KNO_SHORT2LISP(n));
  if ( (istable) && (!((n>0) && (KNO_TABLEP(elts[0])))) ) {
    u8_log(LOGWARN,"BadXTypeCompound",
	   "The first element of a tabular compound (%q) isn't a table",
	   tag);
    lispval corrected[n+1];
    corrected[0]=kno_make_slotmap(4,0,NULL);
    memcpy(corrected+1,elts,sizeof(lispval)*n);
    return kno_init_compound_from_elts(p,tag,flags,n+1,corrected);}
  if (p == NULL) {
    if (n==0)
      p = u8_zmalloc(sizeof(struct KNO_COMPOUND));
    else p = u8_zmalloc(sizeof(struct KNO_COMPOUND)+(n-1)*LISPVAL_LEN);}
  else NO_ELSE;
  struct KNO_TYPEINFO *info = kno_use_typeinfo(tag);
  if  ( (!(issequence)) && (n >= KNO_BIG_COMPOUND_LENGTH) )
    u8_log(LOGWARN,"HugeCompound",
	   "Creating a non-sequence compound of type %q with %d elements",
	   tag,n);
  else NO_ELSE;
  KNO_INIT_CONS(p,kno_compound_type);
  if (ismutable) u8_init_rwlock(&(p->compound_rwlock));
  if (KNO_CONSP(tag))
    p->typetag = kno_incref(info->typetag);
  else p->typetag = tag;
  p->typeinfo = info;
  p->compound_ismutable = ismutable;
  p->compound_isopaque = isopaque;
  if (issequence)
    p->compound_seqoff = KNO_COMPOUND_HEADER_LENGTH(flags);
  else p->compound_seqoff = -1;
  p->compound_istable = istable;
  p->compound_length = n;
  write = &(p->compound_0);
  if (n>0) {
    if (read) {
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
	*write++ = value;}}
    else {
      int i = 0; while (i<n) {
	*write++ = KNO_FALSE;
	i++;}}}
  if ( (istable) && (!(TABLEP(p->compound_0))) )
    p->compound_0 = kno_make_slotmap(0,0,NULL);
  return LISP_CONS(p);
}

KNO_EXPORT lispval kno_init_compound
(struct KNO_COMPOUND *p,lispval tag,int flags,int n,...)
{
  lispval elts[n];
  va_list args; int i = 0;
  if (RARELY((n<0)||(n>=256))) {
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

/* Table functions */

KNO_FASTOP int compound_tablep(lispval arg)
{
  struct KNO_COMPOUND *co = (kno_compound) arg;
  return ( (co->compound_istable) &&
	   (USUALLY(co->compound_length > 0)) &&
	   (USUALLY(KNO_TABLEP(co->compound_0))) );
}

static lispval compound_table_get(lispval obj,lispval key,lispval dflt)
{
  if (!(compound_tablep(obj)))
    return kno_type_error("tabular_compound","compound_table_get",obj);
  struct KNO_COMPOUND *co = (kno_compound) obj;
  return kno_get(co->compound_0,key,dflt);
}

static int compound_table_store(lispval obj,lispval key,lispval val)
{
  if (!(compound_tablep(obj)))
    return kno_type_error("tabular_compound","compound_table_store",obj);
  struct KNO_COMPOUND *co = (kno_compound) obj;
  return kno_store(co->compound_0,key,val);
}

static int compound_table_add(lispval obj,lispval key,lispval val)
{
  if (!(compound_tablep(obj)))
    return kno_type_error("tabular_compound","compound_table_add",obj);
  struct KNO_COMPOUND *co = (kno_compound) obj;
  return kno_add(co->compound_0,key,val);
}

static int compound_table_drop(lispval obj,lispval key,lispval val)
{
  if (!(compound_tablep(obj)))
    return kno_type_error("tabular_compound","compound_table_drop",obj);
  struct KNO_COMPOUND *co = (kno_compound) obj;
  return kno_drop(co->compound_0,key,val);
}

static int compound_table_test(lispval obj,lispval key,lispval val)
{
  if (!(compound_tablep(obj)))
    return kno_type_error("tabular_compound","compound_table_test",obj);
  struct KNO_COMPOUND *co = (kno_compound) obj;
  return kno_test(co->compound_0,key,val);
}

static int compound_table_readonly(lispval obj,int op)
{
  if (!(compound_tablep(obj)))
    return kno_type_error("tabular_compound","compound_table_readonly",obj);
  struct KNO_COMPOUND *co = (kno_compound) obj;
  if (op < 0)
    return kno_readonlyp(co->compound_0);
  else return kno_set_readonly(co->compound_0,op);
}

static int compound_table_modified(lispval obj,int op)
{
  if (!(compound_tablep(obj)))
    return kno_type_error("tabular_compound","compound_table_modified",obj);
  struct KNO_COMPOUND *co = (kno_compound) obj;
  if (op < 0)
    return kno_modifiedp(co->compound_0);
  else return kno_set_modified(co->compound_0,op);
}

static int compound_table_finished(lispval obj,int op)
{
  if (!(compound_tablep(obj)))
    return kno_type_error("tabular_compound","compound_table_finished",obj);
  struct KNO_COMPOUND *co = (kno_compound) obj;
  if (op < 0)
    return kno_finishedp(co->compound_0);
  else return kno_set_finished(co->compound_0,op);
}

static int compound_table_getsize(lispval obj)
{
  if (!(compound_tablep(obj)))
    return kno_type_error("tabular_compound","compound_table_keys",obj);
  struct KNO_COMPOUND *co = (kno_compound) obj;
  return kno_getsize(co->compound_0);
}


static lispval compound_table_keys(lispval obj)
{
  if (!(compound_tablep(obj)))
    return kno_type_error("tabular_compound","compound_table_keys",obj);
  struct KNO_COMPOUND *co = (kno_compound) obj;
  return kno_getkeys(co->compound_0);
}

/* Init methods */

static struct KNO_SEQFNS compound_seqfns = {
  kno_seq_length,
  kno_seq_elt,
  NULL,
  kno_position,
  kno_search,
  kno_seq_elts,
  NULL};

static struct KNO_TABLEFNS compound_tablefns =
  {
   compound_table_get,
   compound_table_store,
   compound_table_add,
   compound_table_drop,
   compound_table_test,
   compound_table_readonly,
   compound_table_modified,
   compound_table_finished,
   compound_table_getsize,
   compound_table_keys,
   NULL, /* keyvals */
   NULL, /* keyvec */
   compound_tablep
  };

void kno_init_compounds_c()
{
  kno_seqfns[kno_compound_type]= &compound_seqfns;
  kno_tablefns[kno_compound_type]= &compound_tablefns;

  u8_register_source_file(_FILEINFO);
}
