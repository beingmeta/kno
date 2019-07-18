/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/apply.h"
#include "kno/lexenv.h"
#include "kno/cprims.h"
#include "kno/eval.h"
#include "kno/compounds.h"
#include "kno/sequences.h"
#include "kno/storage.h"
#include "kno/history.h"

static lispval history_symbol, histref_typetag;

#define hashtable_get(tbl,key,dflt) \
  kno_hashtable_get((kno_hashtable)tbl,key,dflt)

KNO_EXPORT kno_history_resolvefn kno_resolve_histref;
KNO_EXPORT lispval kno_oid_value(lispval oid);

/* Data stucture:

*/

int unpack_history(lispval history,
                   lispval *roots,
                   lispval *vals,
                   lispval *refs,
                   lispval *root_refs)
{
  if (KNO_COMPOUND_TYPEP(history,history_symbol)) {
    int top = -1;
    if (KNO_FIXNUMP(KNO_COMPOUND_REF(history,0)))
      top = KNO_FIX2INT(KNO_COMPOUND_REF(history,0));
    if ( (top >= 0) &&
         (KNO_COMPOUND_LENGTH(history)>=4) &&
         (KNO_HASHTABLEP(KNO_COMPOUND_REF(history,1))) &&
         (KNO_HASHTABLEP(KNO_COMPOUND_REF(history,2)))) {
    if (roots) *roots = KNO_COMPOUND_REF(history,1);
    if (vals) *vals = KNO_COMPOUND_REF(history,2);
    if (refs) *refs = KNO_COMPOUND_REF(history,3);
    if (root_refs) *root_refs = KNO_COMPOUND_REF(history,4);
    return top;}
    else {
      kno_seterr("InvalidHistory","unpack_history",NULL,history);
      return -1;}}
  else {
    kno_seterr("InvalidHistory","unpack_history",NULL,history);
    return -1;}
}

KNO_EXPORT lispval kno_history_ref(lispval history,lispval ref)
{
  if (! (KNO_FIXNUMP(ref)) )
    return kno_err("InvalidHistoryReference","kno_history_ref",NULL,ref);
  lispval roots=KNO_VOID;
  int top = unpack_history(history,&roots,NULL,NULL,NULL);
  if (top < 0)
    return KNO_ERROR_VALUE;
  else return hashtable_get(roots,ref,KNO_VOID);
}

KNO_EXPORT lispval kno_history_getrefs(lispval history,lispval val)
{
  lispval vals=KNO_VOID;
  int top = unpack_history(history,NULL,&vals,NULL,NULL);
  if (top<0)
    return KNO_ERROR_VALUE;
  else return hashtable_get(vals,val,KNO_VOID);
}

KNO_EXPORT lispval kno_history_add(lispval history,lispval val,lispval ref)
{
  lispval roots=KNO_VOID, vals=KNO_VOID, refs=KNO_EMPTY, root_refs=KNO_EMPTY;
  int top = unpack_history(history,&roots,&vals,&refs,&root_refs);
  if (top<0)
    return KNO_ERROR_VALUE;
  else if (KNO_CONSTANTP(val))
    return KNO_VOID;
  lispval val_key = ( (KNO_CHOICEP(val)) || (KNO_PRECHOICEP(val)) ) ?
    (kno_make_qchoice(val)) : (val);
  if (!( (KNO_VOIDP(ref)) || (KNO_FALSEP(ref)) ) ) {
    if ( ! ( (KNO_FIXNUMP(ref)) || (KNO_STRINGP(ref)) || (KNO_SYMBOLP(ref)) ||
             (KNO_COMPOUND_TYPEP(ref,histref_typetag)) ) )
      return kno_err("InvalidHistRef","kno_history_add",NULL,ref);

    lispval overwrite = kno_hashtable_get((kno_hashtable)refs,ref,KNO_VOID);
    if ( (!(KNO_VOIDP(overwrite))) && (overwrite != val) ) {
      /* Drop pointers from the values we're replacing */
      kno_drop(vals,overwrite,ref);}
    /* Add the ref to the value */
    kno_add(vals,val_key,ref);
    /* Add the value to the refs */
    kno_store(refs,ref,val);
    /* If it can be a root, make it one */
    if ( (KNO_FIXNUMP(ref)) || (KNO_STRINGP(ref)) || (KNO_SYMBOLP(ref)) )
      kno_store(roots,ref,val);
    else if (KNO_COMPOUND_TYPEP(ref,histref_typetag))
      kno_add(root_refs,KNO_COMPOUND_REF(ref,0),ref);
    else NO_ELSE;}
  else if (! (kno_test(vals,val_key,KNO_VOID)) ) {
    struct KNO_COMPOUND *histdata = (kno_compound) history;
    kno_store(roots,KNO_INT(top),val);
    kno_store(refs,KNO_INT(top),val);
    kno_add(vals,val_key,KNO_INT(top));
    lispval *elts = &(histdata->compound_0);
    elts[0] = KNO_INT(top+1);}
  else NO_ELSE;
  lispval cur = kno_hashtable_get((kno_hashtable)vals,val_key,KNO_EMPTY);
  if ( (KNO_CHOICEP(val)) || (KNO_PRECHOICEP(val)) ) {
    kno_decref(val_key);
    val_key=KNO_VOID;}
  if (KNO_PRECHOICEP(cur)) cur = kno_simplify_choice(cur);
  if (KNO_CHOICEP(cur)) {
    lispval best = KNO_VOID;
    KNO_DO_CHOICES(cur_ref,cur) {
      if (VOIDP(best))
        best = cur_ref;
      else if ( (FIXNUMP(best)) && (FIXNUMP(cur_ref)) ) {
        long long b_val = KNO_FIX2INT(best);
        long long c_val = KNO_FIX2INT(cur_ref);
        if (c_val < b_val) best = cur_ref;}
      else if (FIXNUMP(cur_ref))
        best = cur_ref;
      else NO_ELSE;}
    kno_incref(best);
    kno_decref(cur);
    return best;}
  else return cur;
}

KNO_EXPORT lispval kno_history_find(lispval history,lispval val)
{
  lispval roots=KNO_VOID, vals=KNO_VOID, refs=KNO_EMPTY, root_refs=KNO_EMPTY;
  int top = unpack_history(history,&roots,&vals,&refs,&root_refs);
  if (top<0)
    return KNO_ERROR_VALUE;
  lispval val_key = ( (KNO_CHOICEP(val)) || (KNO_PRECHOICEP(val)) ) ?
    (kno_make_qchoice(val)) : (val);
  lispval cur = kno_hashtable_get((kno_hashtable)vals,val_key,KNO_EMPTY);
  if ( (KNO_CHOICEP(val)) || (KNO_PRECHOICEP(val)) ) {kno_decref(val_key);}
  return cur;
}

KNO_EXPORT int kno_histpush(lispval value)
{
  lispval history = kno_thread_get(history_symbol);
  if (KNO_ABORTP(history))
    return history;
  else if (VOIDP(history)) {
    kno_seterr("NoActiveHistory","kno_histpush",NULL,value);
    return -1;}
  else {
    lispval ref = kno_history_add(history,value,KNO_VOID);
    kno_decref(history);
    if (KNO_FIXNUMP(ref))
      return KNO_FIX2INT(ref);
    else return -1;}
}

KNO_EXPORT lispval kno_histref(int ref)
{
  lispval history = kno_thread_get(history_symbol);
  if (KNO_ABORTP(history))
    return history;
  else if (VOIDP(history))
    return VOID;
  else {
    lispval result = kno_history_ref(history,KNO_INT(ref));
    kno_decref(history);
    return result;}
}

KNO_EXPORT lispval kno_histfind(lispval value)
{
  lispval history = kno_thread_get(history_symbol);
  if (VOIDP(history)) return VOID;
  else {
    lispval refs = kno_history_getrefs(history,value);
    kno_decref(history);
    return refs;}
}

KNO_EXPORT int kno_historyp()
{
  return kno_thread_probe(history_symbol);
}

KNO_EXPORT
lispval kno_get_histref(lispval elts)
{
  lispval history = kno_thread_get(history_symbol);
  if (KNO_ABORTP(history))
    return history;
  else if ( (VOIDP(history)) || (FALSEP(history)) ) {
    kno_seterr("NoActiveHistory","histref_evalfn",NULL,KNO_VOID);
    return KNO_ERROR_VALUE;}
  else NO_ELSE;
  lispval root = KNO_CAR(elts);
  int void_root = (KNO_FALSEP(root));
  if (void_root) {
    elts = KNO_CDR(elts);
    root = KNO_CAR(elts);}
  lispval val = kno_history_ref(history,root);
  if ( (KNO_ABORTP(root)) || (KNO_EMPTYP(root)) || (KNO_VOIDP(root)) ) {
    kno_seterr("BadHistref","kno_get_histref",NULL,root);
    return KNO_ERROR;}
  lispval paths = KNO_CDR(elts);
  lispval scan = kno_incref(val);
  while ( (KNO_PAIRP(paths)) && (!(KNO_VOIDP(scan))) ) {
    lispval path = KNO_CAR(paths); paths = KNO_CDR(paths);
    if (KNO_FIXNUMP(path)) {
      int rel_off = KNO_FIX2INT(path);
      u8_byte numbuf[64];
      if (KNO_CHOICEP(scan)) {
        ssize_t n_choices = KNO_CHOICE_SIZE(scan);
        ssize_t off = (rel_off>=0) ?  (rel_off) : (n_choices + rel_off);
        if ( (off < 0) || (off > n_choices) )
          return kno_err(kno_RangeError,"histref_evalfn",
                         u8_write_long_long((long long)rel_off,numbuf,64),
                         path);
        else {
          lispval new_scan = KNO_CHOICE_ELTS(scan)[off];
          kno_incref(new_scan);
          kno_decref(scan);
          scan=new_scan;}}
      else if (KNO_PAIRP(scan)) {
        lispval base = scan, lst = base;
        size_t n_elts = 0;
        int improper=0;
        while (KNO_PAIRP(lst)) { lst=KNO_CDR(lst); n_elts++; }
        if (lst != KNO_EMPTY_LIST) { improper=1; n_elts++; }
        ssize_t off = (rel_off>=0) ?  (rel_off) : (n_elts + rel_off);
        if ( (off < 0) || (off > n_elts) )
          return kno_err(kno_RangeError,"histref_evalfn",
                         u8_write_long_long((long long)rel_off,numbuf,64),
                         path);
        else {
          ssize_t i = 0; lst = base; while ( i < off) {
            lst = KNO_CDR(lst); i++;}
          if ( (improper) && ((off+1) == n_elts) ) {
            kno_incref(lst); kno_decref(scan); scan=lst;}
          else {
            lispval new_scan = KNO_CAR(lst);
            kno_incref(new_scan); kno_decref(scan);
            scan = new_scan;}}}
      else if (KNO_SEQUENCEP(scan)) {
        ssize_t n_elts = kno_seq_length(scan);
        ssize_t off = (rel_off>=0) ?  (rel_off) : (n_elts + rel_off);
        if ( (off < 0) || (off > n_elts) )
          return kno_err(kno_RangeError,"histref_evalfn",
                         u8_write_long_long((long long)rel_off,numbuf,64),
                         scan);
        else {
          lispval new_scan = kno_seq_elt(scan,off);
          kno_decref(scan);
          scan=new_scan;}}
      else {
        return kno_err("NotASequence","histref_evalfn",
                       u8_write_long_long((long long)rel_off,numbuf,64),
                       scan);}}
    else if (KNO_STRINGP(path)) {
      if (KNO_TABLEP(scan)) {
        lispval v = kno_get(scan,path,KNO_VOID);
        if (KNO_VOIDP(v)) {
          u8_string upper = u8_upcase(KNO_CSTRING(scan));
          lispval sym = kno_probe_symbol(upper,-1);
          if (KNO_SYMBOLP(sym)) v = kno_get(scan,sym,KNO_VOID);}
        if (KNO_VOIDP(v)) {
          kno_seterr("NoSuchKey","histref_evalfn",KNO_CSTRING(path),scan);
          return KNO_ERROR;}
        kno_decref(scan);
        scan = v;}
      else {
        lispval err = kno_err("NotATable","histref_evalfn",KNO_CSTRING(path),scan);
        kno_decref(scan);
        scan = KNO_VOID;
        return err;}}
    else if ( (KNO_SYMBOLP(path)) || (KNO_OIDP(path)) ) {
      if (KNO_TABLEP(scan)) {
        lispval v = (OIDP(scan)) ? (kno_frame_get(scan,path)) :
          (kno_get(scan,path,KNO_VOID));
        u8_byte keybuf[64];
        if (KNO_VOIDP(v))
          kno_seterr("NoSuchKey","histref_evalfn",
                     ((KNO_SYMBOLP(path)) ? 
                      (KNO_SYMBOL_NAME(path)) :
                      (kno_oid2string(path,keybuf,64))),
                     scan);
        kno_decref(scan);
        scan = v;}
      else scan = KNO_VOID;}
    else scan = KNO_VOID;}
  if (KNO_VOIDP(scan)) {
    kno_seterr("InvalidHistRef","histref_evalfn",NULL,root);
    kno_decref(root);
    return KNO_ERROR;}
  else {
    kno_incref(scan);
    kno_decref(val);
    if (void_root)
      return scan;
    else return kno_make_list(2,KNOSYM_QUOTE,scan);}
}

KNO_EXPORT void kno_histinit(int size)
{
  lispval history = kno_thread_get(history_symbol);
  if (size<=0) {
    lispval configval = kno_config_get("HISTORYSIZE");
    if (KNO_UINTP(configval))
      size = FIX2INT(configval);
    else {
      kno_decref(configval);
      size = 128;}}
  if (VOIDP(history)) {
    history = kno_init_compound(NULL,history_symbol,
                               KNO_COMPOUND_USEREF,5,
                               KNO_INT(1),
                               kno_make_hashtable(NULL,size),
                               kno_make_hashtable(NULL,size),
                               kno_make_hashtable(NULL,size),
                               kno_make_hashtable(NULL,size));
    kno_thread_set(history_symbol,history);}
  kno_decref(history);
}

KNO_EXPORT void kno_histclear(int size)
{
  if (size>0) {
    kno_thread_set(history_symbol,VOID);
    kno_histinit(size);}
  else kno_thread_set(history_symbol,KNO_FALSE);
}

KNO_EXPORT lispval with_history_evalfn
(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval history_arg = kno_stack_eval(kno_get_arg(expr,1),env,stack,0);
  if (KNO_VOIDP(history_arg)) {
    if (u8_current_exception)
      return KNO_ERROR;
    else return kno_err("BadHistoryArg","with_history_evalfn",NULL,expr);}
  lispval outer_history = kno_thread_get(history_symbol);
  kno_thread_set(history_symbol,VOID);
  if (KNO_FIXNUMP(history_arg))
    kno_histinit(KNO_FIX2INT(history_arg));
  else kno_histinit(200);
  lispval body = kno_get_body(expr,2);
  lispval value = kno_eval_exprs(body,env,stack,0);
  kno_thread_set(history_symbol,outer_history);
  kno_decref(outer_history);
  kno_decref(history_arg);
  return value;
}

DEFPRIM("histref",histref_prim,MIN_ARGS(2)|MAX_ARGS(2),
	"`(HISTREF *ref* [*history*])` returns the history item for "
	"*ref*, using the current historical context if *history* is "
	"not provided.");
static lispval histref_prim(lispval ref,lispval history)
{
  if ( (VOIDP(history)) || (FALSEP(history)) || (DEFAULTP(history)) )
    history = kno_thread_get(history_symbol);
  else kno_incref(history);
  if (KNO_COMPOUND_TYPEP(history,history_symbol)) {
    lispval result = kno_history_ref(history,ref);
    kno_decref(history);
    return result;}
  else return KNO_EMPTY;
}

DEFPRIM("histfind",histfind_prim,MIN_ARGS(2)|MAX_ARGS(2),
	"`(HISTFIND *val* [*history*])` returns the history reference for "
	"*value*, using the current historical context if *history* is "
	"not provided.");
static lispval histfind_prim(lispval ref,lispval history)
{
  if ( (VOIDP(history)) || (FALSEP(history)) || (DEFAULTP(history)) )
    history = kno_thread_get(history_symbol);
  else kno_incref(history);
  if (KNO_COMPOUND_TYPEP(history,history_symbol)) {
    lispval result = kno_history_find(history,ref);
    kno_decref(history);
    return result;}
  else return KNO_EMPTY;
}

DEFPRIM("histadd!",histadd_prim,MIN_ARGS(1)|MAX_ARGS(3),
	"`(HISTADD! *val* [*history*] [*ref*])` associates *val* with "
	"the history reference *ref*, creating a new reference if *ref* "
	"is not provided. This uses the current historical context if "
	"*history* is not provided.");
static lispval histadd_prim(lispval val,lispval history,lispval ref)
{
  if ( (VOIDP(history)) || (FALSEP(history)) || (DEFAULTP(history)) )
    history = kno_thread_get(history_symbol);
  else kno_incref(history);
  if (KNO_COMPOUND_TYPEP(history,history_symbol)) {
    lispval result = kno_history_add(history,val,ref);
    kno_decref(history);
    return result;}
  else return KNO_EMPTY;
}

static int scheme_history_initialized = 0;

static lispval history_module ;

KNO_EXPORT void kno_init_history_c()
{
  if (scheme_history_initialized) return;
  else scheme_history_initialized = 1;

  history_module = kno_new_cmodule("history",0,NULL);

  u8_register_source_file(_FILEINFO);

  kno_def_evalfn(history_module,"with-history",
		 "`(with-history *info* *body...*)` creates a new history "
		 "context and evaluates *body* within that context. "
		 "This saves any current history context.",
		 with_history_evalfn);

  init_local_cprims();

  history_symbol = kno_intern("%history");
  histref_typetag = kno_intern("%histref");

  kno_resolve_histref = kno_get_histref;

  kno_finish_module(history_module);

}

static void init_local_cprims()
{
  KNO_LINK_PRIM("histref",histref_prim,2,history_module);
  KNO_LINK_PRIM("histfind",histfind_prim,2,history_module);
  KNO_LINK_PRIM("histadd!",histadd_prim,3,history_module);
}
