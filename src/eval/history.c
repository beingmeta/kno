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
#include "framerd/compounds.h"
#include "framerd/sequences.h"
#include "framerd/history.h"

static lispval history_symbol, histref_symbol;

#define hashtable_get(tbl,key,dflt) \
  fd_hashtable_get((fd_hashtable)tbl,key,dflt)

FD_EXPORT fd_history_resolvefn fd_resolve_histref;
FD_EXPORT lispval fd_oid_value(lispval oid);

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
  else if (FD_CONSTANTP(val))
    return FD_VOID;
#if 0
  else if ( (FD_SYMBOLP(val)) ||
            ( (FD_STRINGP(val)) && ( (FD_STRLEN(val)) < 40 ) ) || 
            ( (FD_NUMBERP(val)) ) )
    return FD_VOID;
#endif
  lispval val_key = ( (FD_CHOICEP(val)) || (FD_PRECHOICEP(val)) ) ?
    (fd_make_qchoice(val)) : (val);
  if (!( (FD_VOIDP(ref)) || (FD_FALSEP(ref)) ) ) {
    if ( ! ( (FD_FIXNUMP(ref)) || (FD_STRINGP(ref)) || (FD_SYMBOLP(ref)) ||
             (FD_COMPOUND_TYPEP(ref,histref_symbol)) ) )
      return fd_err("InvalidHistRef","fd_history_add",NULL,ref);

    lispval overwrite = fd_hashtable_get((fd_hashtable)refs,ref,FD_VOID);
    if ( (!(FD_VOIDP(overwrite))) && (overwrite != val) ) {
      /* Drop pointers from the values we're replacing */
      fd_drop(vals,overwrite,ref);}
    /* Add the ref to the value */
    fd_add(vals,val_key,ref);
    /* Add the value to the refs */
    fd_store(refs,ref,val);
    /* If it can be a root, make it one */
    if ( (FD_FIXNUMP(ref)) || (FD_STRINGP(ref)) || (FD_SYMBOLP(ref)) )
      fd_store(roots,ref,val);
    else if (FD_COMPOUND_TYPEP(ref,histref_symbol))
      fd_add(root_refs,FD_COMPOUND_REF(ref,0),ref);
    else NO_ELSE;}
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

FD_EXPORT lispval fd_history_find(lispval history,lispval val)
{
  lispval roots=FD_VOID, vals=FD_VOID, refs=FD_EMPTY, root_refs=FD_EMPTY;
  int top = unpack_history(history,&roots,&vals,&refs,&root_refs);
  if (top<0)
    return FD_ERROR_VALUE;
  lispval val_key = ( (FD_CHOICEP(val)) || (FD_PRECHOICEP(val)) ) ?
    (fd_make_qchoice(val)) : (val);
  lispval cur = fd_hashtable_get((fd_hashtable)vals,val_key,FD_EMPTY);
  if ( (FD_CHOICEP(val)) || (FD_PRECHOICEP(val)) ) {fd_decref(val_key);}
  return cur;
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
    if (FD_FIXNUMP(ref))
      return FD_FIX2INT(ref);
    else return -1;}
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

FD_EXPORT
lispval fd_get_histref(lispval elts)
{
  lispval history = fd_thread_get(history_symbol);
  if (FD_ABORTP(history))
    return history;
  else if (VOIDP(history)) {
    fd_seterr("NoActiveHistory","histref_evalfn",NULL,FD_VOID);
    return FD_ERROR_VALUE;}
  else NO_ELSE;
  lispval root = FD_CAR(elts);
  int void_root = (FD_FALSEP(root));
  if (void_root) {
    elts = FD_CDR(elts);
    root = FD_CAR(elts);}
  lispval val = fd_history_ref(history,root);
  if ( (FD_ABORTP(root)) || (FD_EMPTYP(root)) || (FD_VOIDP(root)) ) {
    fd_seterr("BadHistref","fd_get_histref",NULL,root);
    return FD_ERROR;}
  lispval paths = FD_CDR(elts);
  lispval scan = fd_incref(val);
  while ( (FD_PAIRP(paths)) && (!(FD_VOIDP(scan))) ) {
    lispval path = FD_CAR(paths); paths = FD_CDR(paths);
    if (FD_FIXNUMP(path)) {
      int rel_off = FD_FIX2INT(path);
      if (FD_CHOICEP(scan)) {
	ssize_t n_choices = FD_CHOICE_SIZE(scan);
	ssize_t off = (rel_off>=0) ?  (rel_off) : (n_choices + rel_off);
	if ( (off < 0) || (off > n_choices) )
	  return fd_err(fd_RangeError,"histref_evalfn",NULL,path);
	else {
	  lispval new_scan = FD_CHOICE_ELTS(scan)[off];
	  fd_incref(new_scan); fd_decref(scan);
	  scan=new_scan;}}
      else if (FD_PAIRP(scan)) {
        lispval base = scan; size_t n_elts = 0;
        int improper=0;
        while (FD_PAIRP(scan)) { scan=FD_CDR(scan); n_elts++; }
        if (scan != FD_EMPTY_LIST) { improper=1; n_elts++; }
        ssize_t off = (rel_off>=0) ?  (rel_off) : (n_elts + rel_off);
	if ( (off < 0) || (off > n_elts) )
	  return fd_err(fd_RangeError,"histref_evalfn",NULL,path);
	else {
          ssize_t i = 0; scan = base; while ( i < off) {
            scan = FD_CDR(scan); i++;}
          if ( (improper) && ((off+1) == n_elts) )
            scan = scan;
          else scan = FD_CAR(scan);}}
      else if (FD_SEQUENCEP(scan)) {
	ssize_t n_elts = fd_seq_length(scan);
	ssize_t off = (rel_off>=0) ?  (rel_off) : (n_elts + rel_off);
	if ( (off < 0) || (off > n_elts) )
	  return fd_err(fd_RangeError,"histref_evalfn",NULL,path);
	else {
	  lispval new_scan = fd_seq_elt(scan,off);
	  fd_decref(scan);
	  scan=new_scan;}}
      else {
        if (FD_OIDP(scan)) scan = fd_oid_value(scan);
        if (FD_SLOTMAPP(scan)) {
          struct FD_SLOTMAP *sm = FD_XSLOTMAP(scan);
          size_t n_slots = sm->n_slots;
          ssize_t off = (rel_off>=0) ?  (rel_off) : (n_slots + rel_off);
          if ( ( off < 0) || ( off >= n_slots ) )
            return fd_err(fd_RangeError,"histref_evalfn",NULL,path);
          else scan = sm->sm_keyvals[off].kv_val;}
        else if (FD_SCHEMAPP(scan)) {
          struct FD_SCHEMAP *sm = FD_XSCHEMAP(scan);
          size_t n_slots = sm->schema_length;
          ssize_t off = (rel_off>=0) ?  (rel_off) : (n_slots + rel_off);
          if ( off >= sm->schema_length )
            return fd_err(fd_RangeError,"histref_evalfn",NULL,path);
          else scan = sm->schema_values[off];}
        else scan = FD_VOID;}}
    else if (FD_STRINGP(path)) {
      if (FD_TABLEP(scan)) {
	lispval v = fd_get(scan,path,FD_VOID);
	if (FD_VOIDP(v)) {
	  u8_string upper = u8_upcase(FD_CSTRING(scan));
	  lispval sym = fd_probe_symbol(upper,-1);
	  if (FD_SYMBOLP(sym))
	    v = fd_get(scan,sym,FD_VOID);}
	if (FD_VOIDP(v))
	  fd_seterr("NoSuchKey","histref_evalfn",FD_CSTRING(path),scan);
	scan = v;}
      else scan = FD_VOID;}
    else if (path == FDSYM_EQUALS) {
      if ( (FD_PAIRP(paths)) &&
           ( (FD_STRINGP(FD_CAR(paths))) ||
             (FD_SYMBOLP(FD_CAR(paths))) ) ) {
        lispval next = FD_CAR(paths);
        fd_history_add(history,scan,next);
        paths=FD_CDR(paths);}
      else {
        fd_seterr("InvalidHistRef","histref_evalfn",NULL,root);
        fd_decref(root);
        return FD_ERROR;}}
    else if (FD_SYMBOLP(path)) {
      if (FD_TABLEP(scan)) {
	lispval v = fd_get(scan,path,FD_VOID);
	if (FD_VOIDP(v))
	  fd_seterr("NoSuchKey","histref_evalfn",FD_CSTRING(path),scan);
	fd_decref(scan);
	scan = v;}
      else scan = FD_VOID;}
    else scan = FD_VOID;}
  if (FD_VOIDP(scan)) {
    fd_seterr("InvalidHistRef","histref_evalfn",NULL,root);
    fd_decref(root);
    return FD_ERROR;}
  else {
    fd_incref(scan);
    fd_decref(val);
    if (void_root)
      return scan;
    else return fd_make_list(2,FDSYM_QUOTE,scan);}
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

  fd_resolve_histref = fd_get_histref;

}

/* Emacs local variables
   ;;;	Local variables: ***
   ;;;	compile-command: "make -C ../.. debug;" ***
   ;;;	indent-tabs-mode: nil ***
   ;;;	End: ***
*/
