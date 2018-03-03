/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/sequences.h"
#include "framerd/history.h"

static lispval history_symbol, histref_symbol;

#define hashtable_get(tbl,key,dflt) \
  fd_hashtable_get((fd_hashtable)tbl,key,dflt)

/* Data stucture:

*/

int unpack_history(lispval history,lispval *roots,lispval *vals,lispval *refs,
		   lispval *root_refs)
{
  if (FD_COMPOUND_TYPEP(history,history_symbol)) {
    int top = -1;
    if (FD_FIXNUMP(FD_COMPOUND_REF(history,0)))
      top = FD_FIX2INT(FD_COMPOUND_REF(history,0));
    if ( (top >= 0) &&
	 (FD_COMPOUND_LENGTH(history)>=4) &&
	 (FD_HASHTABLEP(FD_COMPOUND_REF(history,1))) &&
	 (FD_HASHTABLEP(FD_COMPOUND_REF(history,2)))) {
    if (roots) *roots = FD_COMPOUND_REF(history,1);
    if (vals) *vals = FD_COMPOUND_REF(history,2);
    if (refs) *refs = FD_COMPOUND_REF(history,3);
    if (root_refs) *root_refs = FD_COMPOUND_REF(history,4);
    return top;}
    else {
      fd_seterr("InvalidHistory","unpack_history",NULL,history);
      return -1;}}
  else {
    fd_seterr("InvalidHistory","unpack_history",NULL,history);
    return -1;}
}

FD_EXPORT lispval fd_history_ref(lispval history,lispval ref)
{
  if (! ( (FD_FIXNUMP(ref)) || (FD_STRINGP(ref)) || (FD_SYMBOLP(ref)) ) )
    return fd_err("InvalidHistoryReference","fd_history_ref",NULL,ref);
  lispval roots=FD_VOID;
  int top = unpack_history(history,&roots,NULL,NULL,NULL);
  if (top < 0)
    return FD_ERROR_VALUE;
  else return hashtable_get(roots,ref,FD_VOID);
}

FD_EXPORT lispval fd_history_getrefs(lispval history,lispval val)
{
  lispval vals=FD_VOID;
  int top = unpack_history(history,NULL,&vals,NULL,NULL);
  if (top<0)
    return FD_ERROR_VALUE;
  else return hashtable_get(vals,val,FD_VOID);
}

#define named_rootp(x) ( (FD_STRINGP(x)) || (FD_SYMBOLP(x)) )

static int better_refp(lispval ref,lispval best)
{
  if (FD_VOIDP(best))
    return 1;
  else if ( (FD_STRINGP(best)) || (FD_SYMBOLP(best)) )
    return 0;
  else if ( (FD_STRINGP(ref)) || (FD_SYMBOLP(ref)) )
    return 1;
  else if (FD_FIXNUMP(ref))
    return 0;
  else if (FD_FIXNUMP(best))
    return 1;
  else if ( (FD_COMPOUND_TYPEP(ref,histref_symbol)) &&
	    (FD_COMPOUND_TYPEP(best,histref_symbol)) ) {
    lispval ref_root = FD_COMPOUND_REF(ref,0);
    lispval best_root = FD_COMPOUND_REF(best,0);
    if	(named_rootp(ref_root)) {
      if (!(named_rootp(best_root)))
	return 1;}
    if (named_rootp(best_root))
      return 0;
    else return ( (FD_COMPOUND_LENGTH(ref)) < (FD_COMPOUND_LENGTH(best)) );}
  else return best;
}

FD_EXPORT lispval fd_history_add(lispval history,lispval val,lispval ref)
{
  lispval roots=FD_VOID, vals=FD_VOID, refs=FD_EMPTY, root_refs=FD_EMPTY;
  int top = unpack_history(history,&roots,&vals,&refs,&root_refs);
  if (top<0)
    return FD_ERROR_VALUE;
  else if ( (FD_SYMBOLP(val)) ||
	    ( (FD_STRINGP(val)) && ( (FD_STRLEN(val)) < 40 ) ) ||
	    ( (FD_NUMBERP(val)) ) )
    return FD_VOID;
  lispval val_key = ( (FD_CHOICEP(val)) || (FD_PRECHOICEP(val)) ) ?
    (fd_make_qchoice(val)) : (val);
  if (!( (FD_VOIDP(ref)) || (FD_FALSEP(ref)) ) ) {
    if ( ! ( (FD_FIXNUMP(ref)) || (FD_STRINGP(ref)) || (FD_SYMBOLP(ref)) ||
	     (FD_COMPOUND_TYPEP(ref,histref_symbol)) ) )
      return fd_err("InvalidHistRef","fd_history_add",NULL,ref);
    lispval overwrite = fd_hashtable_get((fd_hashtable)refs,ref,FD_VOID);
    if ( (!(FD_VOIDP(overwrite))) && (overwrite != val) ) {
      fd_drop(vals,overwrite,ref);
      fd_add(vals,val_key,ref);
      fd_store(refs,ref,val);
      if ( (FD_FIXNUMP(ref)) || (FD_STRINGP(ref)) || (FD_SYMBOLP(ref)) )
	fd_store(roots,ref,val);
      else if (FD_COMPOUND_TYPEP(ref,histref_symbol))
	fd_add(root_refs,FD_COMPOUND_REF(ref,0),ref);
      else NO_ELSE;}}
  else if (! (fd_test(vals,val_key,FD_VOID)) ) {
    struct FD_COMPOUND *histdata = (fd_compound) history;
    fd_store(roots,FD_INT(top),val);
    fd_store(refs,FD_INT(top),val);
    fd_add(vals,val_key,FD_INT(top));
    lispval *elts = &(histdata->compound_0);
    elts[0] = FD_INT(top+1);}
  else NO_ELSE;
  lispval cur = fd_hashtable_get((fd_hashtable)vals,val_key,FD_EMPTY);
  if ( (FD_CHOICEP(val)) || (FD_PRECHOICEP(val)) ) {
    fd_decref(val_key); val_key=FD_VOID;}
  if (FD_PRECHOICEP(cur)) cur = fd_simplify_choice(cur);
  if (FD_CHOICEP(cur)) {
    lispval best = FD_VOID;
    FD_DO_CHOICES(cur_ref,cur) {
      if (better_refp(cur_ref,best)) best = cur_ref;}
    fd_incref(best);
    fd_decref(cur);
    return best;}
  else return cur;
}

FD_EXPORT int fd_histpush(lispval value)
{
  lispval history = fd_thread_get(history_symbol);
  if (FD_ABORTP(history))
    return history;
  else if (VOIDP(history)) {
    fd_seterr("NoActiveHistory","fd_histpush",NULL,value);
    return -1;}
  else {
    lispval ref = fd_history_add(history,value,FD_VOID);
    fd_decref(history);
    return FD_FIX2INT(ref);}
}

FD_EXPORT lispval fd_histref(int ref)
{
  lispval history = fd_thread_get(history_symbol);
  if (FD_ABORTP(history))
    return history;
  else if (VOIDP(history))
    return VOID;
  else {
    lispval result = fd_history_ref(history,FD_INT(ref));
    fd_decref(history);
    return result;}
}

FD_EXPORT lispval fd_histfind(lispval value)
{
  lispval history = fd_thread_get(history_symbol);
  if (VOIDP(history)) return VOID;
  else {
    lispval refs = fd_history_getrefs(history,value);
    fd_decref(history);
    return refs;}
}

FD_EXPORT void fd_histinit(int size)
{
  lispval history = fd_thread_get(history_symbol);
  if (size<=0) {
    lispval configval = fd_config_get("HISTORYSIZE");
    if (FD_UINTP(configval))
      size = FIX2INT(configval);
    else {
      fd_decref(configval);
      size = 128;}}
  if (VOIDP(history)) {
    history = fd_init_compound(NULL,history_symbol,
			       FD_COMPOUND_USEREF,5,
			       FD_INT(1),
			       fd_make_hashtable(NULL,size),
			       fd_make_hashtable(NULL,size),
			       fd_make_hashtable(NULL,size),
			       fd_make_hashtable(NULL,size));
    fd_thread_set(history_symbol,history);}
  fd_decref(history);
}

FD_EXPORT void fd_histclear(int size)
{
  fd_thread_set(history_symbol,VOID);
  fd_histinit(size);
}

static int scheme_history_initialized = 0;

FD_EXPORT void fd_init_history_c()
{
  if (scheme_history_initialized) return;
  else scheme_history_initialized = 1;
  u8_register_source_file(_FILEINFO);

  history_symbol = fd_intern("%HISTORY");
  histref_symbol = fd_intern("%HISTREF");

}

/* Emacs local variables
   ;;;	Local variables: ***
   ;;;	compile-command: "make -C ../.. debug;" ***
   ;;;	indent-tabs-mode: nil ***
   ;;;	End: ***
*/
