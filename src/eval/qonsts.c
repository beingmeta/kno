/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2006-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

/* QONSTS represent values within modules which are (probably)
   constant. They are used by the evaluator and optimizer to avoid
   some read-time lookups. QONSTs are immediate value pointers whose
   underlying value refers to a runtime table of values.

   They are especially used to represent references to functions
   in optimized code.

   
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_QONSTS 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/apply.h"
#include "kno/cprims.h"
#include "kno/eval.h"

#include <libu8/u8printf.h>

#define KNO_QONST_MAX (KNO_QONST_NBLOCKS*KNO_QONST_BLOCKSIZE)

u8_condition kno_InvalidQONST=_("Invalid persistent pointer reference");
u8_condition kno_QONSTOverflow=_("No more valid persistent pointers");

static lispval qonsts_symbol;

struct KNO_CONS **_kno_qonsts[KNO_QONST_NBLOCKS];
struct KNO_HASHTABLE qonst_table;
int _kno_qonst_count = 0;
u8_mutex _kno_qonst_lock;

int _kno_leak_qonsts = 1;

/* Registering qonsts */

KNO_EXPORT lispval kno_register_qonst(lispval key,lispval val)
{
  lispval qonst = kno_hashtable_get(&qonst_table,key,KNO_VOID);
  if (KNO_QONSTP(qonst))
    return qonst;
  else if (KNO_ABORTED(qonst))
    return qonst;
  else if (!(KNO_VOIDP(qonst)))
    return kno_err("CorruptedQonst","kno_register_qonst",NULL,key);
  else NO_ELSE;
  int serialno;
  u8_lock_mutex(&_kno_qonst_lock);
  /* Lookup again, inside the lock */
  qonst = kno_hashtable_get(&qonst_table,key,KNO_VOID);
  if ( (KNO_ABORTED(qonst)) || (KNO_QONSTP(qonst)) ) {
    u8_unlock_mutex(&_kno_qonst_lock);
    return qonst;}
  if (_kno_qonst_count>=KNO_QONST_MAX) {
    u8_unlock_mutex(&_kno_qonst_lock);
    return kno_err(kno_QONSTOverflow,"kno_register_qonst",NULL,key);}
  serialno=_kno_qonst_count++;
  if ((serialno%KNO_QONST_BLOCKSIZE)==0) {
    struct KNO_CONS **block = u8_alloc_n(KNO_QONST_BLOCKSIZE,struct KNO_CONS *);
    int i = 0, n = KNO_QONST_BLOCKSIZE;
    while (i<n) block[i++]=NULL;
    _kno_qonsts[serialno/KNO_QONST_BLOCKSIZE]=block;}
  qonst = LISPVAL_IMMEDIATE(kno_qonst_type,serialno);
  kno_hashtable_store(&qonst_table,qonst,key);
  kno_hashtable_store(&qonst_table,key,qonst);
  _kno_qonsts[serialno/KNO_QONST_BLOCKSIZE][serialno%KNO_QONST_BLOCKSIZE]=
    (struct KNO_CONS *) (kno_make_simple_choice(val));
  u8_unlock_mutex(&_kno_qonst_lock);
  return qonst;
}

KNO_EXPORT lispval kno_probe_qonst(lispval key)
{
  return kno_hashtable_get(&qonst_table,key,KNO_EMPTY);
}

KNO_EXPORT lispval kno_qonst_key(lispval qonst)
{
  return kno_hashtable_get(&qonst_table,qonst,KNO_EMPTY);
}

KNO_EXPORT lispval kno_set_qonst(lispval id,lispval value)
{
  if (!(KNO_QONSTP(id)))
    return kno_type_error("qonst","kno_set_qonst",id);
  else if (!(CONSP(value)))
    return kno_type_error("cons","kno_set_qonst",value);
  else if (!((KNO_FUNCTIONP(value))||
             (TYPEP(value,kno_closure_type))||
	     (TYPEP(value,kno_evalfn_type))))
    return kno_type_error("function/fexpr","kno_set_qonst",value);
  else {
    u8_lock_mutex(&_kno_qonst_lock);
    int serialno = KNO_GET_IMMEDIATE(id,kno_qonst_type);
    int block_num = serialno/KNO_QONST_BLOCKSIZE;
    int block_off = serialno%KNO_QONST_BLOCKSIZE;
    if (serialno>=_kno_qonst_count) {
      u8_unlock_mutex(&_kno_qonst_lock);
      return kno_err(kno_InvalidQONST,"kno_set_qonst",NULL,id);}
    else {
      struct KNO_CONS **block=_kno_qonsts[block_num];
      if (!(block)) {
        /* We should never get here, but let's check anyway */
        u8_unlock_mutex(&_kno_qonst_lock);
        return kno_err(kno_InvalidQONST,"kno_set_qonst",NULL,id);}
      else {
        struct KNO_CONS *current = block[block_off];
        if (current == ((kno_cons)value)) {
          u8_unlock_mutex(&_kno_qonst_lock);
          return id;}
        block[block_off]=(kno_cons) kno_make_simple_choice(value);
        if (!(_kno_leak_qonsts)) {
          /* This is dangerous if, for example, a module is being reloaded
             (and qonst's redefined) while another thread is using the old
             value. If this bothers you, set kno_leak_qonsts to 1. */
          if (current) {kno_decref((lispval)current);}}
        u8_unlock_mutex(&_kno_qonst_lock);
        return id;}}}
}

static int unparse_qonst(u8_output out,lispval x) {
  u8_printf(out,"#<QONST 0x%x>", KNO_IMMEDIATE_DATA(x)); return 1;}

/* QONST/REF */

static lispval getmoduleid(lispval module)
{
  lispval id = kno_get(module,KNOSYM_MODULEID,KNO_VOID);
  if (KNO_SYMBOLP(id)) return id;
  else if (KNO_STRINGP(id)) return id;
  else if (KNO_VOIDP(id)) return KNO_FALSE;
  else if (KNO_CHOICEP(id)) {
    lispval stringid = KNO_FALSE;
    KNO_DO_CHOICES(each,id) {
      if (KNO_SYMBOLP(each)) {
	kno_decref(id);
	return each;}
      else if (KNO_STRINGP(each)) {
	if (KNO_VOIDP(stringid)) stringid=each;}
      else NO_ELSE;}
    kno_decref(id);
    return stringid;}
  else return KNO_FALSE;
}


KNO_EXPORT lispval kno_qonst_ref(lispval sym,lispval from,lispval val)
{
  if (RARELY(!(SYMBOLP(sym))))
    return kno_err("NotASymbol","kno_qonst_ref",NULL,sym);
  lispval moduleid = getmoduleid(from);
  if ( ! ( (SYMBOLP(moduleid)) || (STRINGP(moduleid)) ) )
    return kno_err("NoModuleID","kno_qonst_ref",KNO_SYMBOL_NAME(sym),from);
  if ( (KNO_LEXENVP(from)) || (KNO_HASHTABLEP(from)) ) {
    int decref_val = 0;
    if (KNO_VOIDP(val)) {
      if (KNO_LEXENVP(from)) {
	kno_lexenv env = (kno_lexenv) from;
	if (kno_test(env->env_bindings,sym,KNO_VOID)) {
	  val = kno_get(env->env_bindings,sym,KNO_VOID);
	  if (CONSP(val)) decref_val=1;}
	else {}}
      else if (KNO_HASHTABLEP(from))
	val = kno_get(from,sym,KNO_VOID);
      else NO_ELSE;
      if (KNO_VOIDP(val)) {
	u8_log(LOGWARN,"UnboundOrigin",
	       "The symbol '%q isn't bound in %q",
	       sym,from);
	return KNO_VOID;}}
    if (KNO_QONSTP(val)) return val;
    lispval bindings = (KNO_HASHTABLEP(from)) ? (from) : (((kno_lexenv)from)->env_bindings);
    if (KNO_HASHTABLEP(bindings)) {
      /* Create qonsts if needed */
      lispval qonsts = kno_get(bindings,qonsts_symbol,KNO_VOID);
      if (!(KNO_HASHTABLEP(qonsts))) {
	lispval use_table = kno_make_hashtable(NULL,19);
	kno_hashtable_op((kno_hashtable)bindings,
			 kno_table_default,qonsts_symbol,use_table);
	kno_decref(use_table);
	qonsts = kno_get(bindings,qonsts_symbol,KNO_VOID);}
      lispval qonst = kno_get(qonsts,sym,KNO_VOID);
      if (!(KNO_QONSTP(qonst))) {
	lispval qonst_key = kno_make_pair(sym,moduleid);
	lispval new_qonst = kno_register_qonst(qonst_key,val);
	/* In rare cases, this may leak a qonst reference, but
	   we're not worrying about that. */
	kno_hashtable_op((kno_hashtable)qonsts,
			 kno_table_default,sym,new_qonst);
	qonst = kno_get(qonsts,sym,KNO_VOID);
	if (qonst == new_qonst)
	  kno_hashtable_store((kno_hashtable)qonsts,qonst,sym);
	kno_decref(qonst_key);}
      kno_decref(moduleid);
      kno_set_qonst(qonst,val);
      kno_decref(qonsts);
      if (decref_val) kno_decref(val);
      return qonst;}
    else if (decref_val)
      return val;
    else return kno_incref(val);}
  else return kno_incref(val);
}

static lispval fcnalias_evalfn(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval sym = kno_get_arg(expr,1);
  if (!(KNO_SYMBOLP(sym)))
    return kno_err(kno_SyntaxError,"fcnalias_evalfn",NULL,expr);
  lispval env_expr = kno_get_arg(expr,2), env_arg = KNO_VOID;
  lispval source_table = KNO_VOID;
  kno_lexenv source_env = NULL;
  if (!( (KNO_DEFAULTP(env_expr)) || (KNO_VOIDP(env_expr)) )) {
    env_arg = kno_eval(env_expr,env,stack);
    if (KNO_LEXENVP(env_arg)) {
      kno_lexenv e = (kno_lexenv) env_arg;
      if (kno_test(e->env_bindings,sym,KNO_VOID)) {
	source_env = e;}
      else e = kno_find_binding(e,sym,0);
      if (KNO_HASHTABLEP(e->env_bindings))
	source_env = (kno_lexenv) env_arg;}
    else if (KNO_HASHTABLEP(env_arg))
      source_table = env_arg;
    else if (KNO_SYMBOLP(env_arg)) {
      env_arg = kno_find_module(env_arg,1);
      if (KNO_LEXENVP(env_arg))
	source_env = (kno_lexenv)env_arg;
      else if (KNO_HASHTABLEP(env_arg))
	source_table = env_arg;
      else NO_ELSE;}
    else NO_ELSE;
    if ( (source_env == NULL) && (!(KNO_TABLEP(source_table))) ) {
      kno_seterr("InvalidSourceEnv","fcnalias_evalfn",
		 KNO_SYMBOL_NAME(sym),
		 env_arg);
      kno_decref(env_arg);
      return KNO_ERROR;}}
  else {
    kno_lexenv e = kno_find_binding(env,sym,1);
    if (e == NULL) {
      kno_seterr("UnboundVariable","fcnalias_evalfn",
		 KNO_SYMBOL_NAME(sym),
		 KNO_VOID);
      return KNO_ERROR;}
    source_env = e;}
  if (KNO_TABLEP(source_table)) {
    lispval val = kno_get(source_table,sym,KNO_VOID);
    if (KNO_VOIDP(val)) {
      kno_seterr("UnboundVariable","fcnalias_evalfn",
		 KNO_SYMBOL_NAME(sym),
		 source_table);
      kno_decref(env_arg);
      return KNO_ERROR;}
    lispval ref = kno_qonst_ref(sym,source_table,val);
    kno_decref(val);
    kno_decref(env_arg);
    return ref;}
  lispval val = kno_symeval(sym,source_env);
  if (KNO_ABORTED(val)) {
    kno_decref(env_arg);
    return val;}
  else if ( (KNO_CONSP(val)) &&
	    ( (KNO_FUNCTIONP(val)) ||
	      (KNO_APPLICABLEP(val)) ||
	      (KNO_EVALFNP(val)) ||
	      (KNO_MACROP(val)) ) ) {
    lispval qonst = kno_qonst_ref(sym,(lispval)source_env,val);
    kno_decref(env_arg);
    kno_decref(val);
    return qonst;}
  else {
    kno_decref(env_arg);
    kno_decref(val);
    u8_log(LOG_WARN,"BadAlias",
	   "Can't make alias of the value of '%s: %q",
	   KNO_SYMBOL_NAME(sym),val);
    return val;}
}

DEFC_PRIM("qonst/register",qonst_register_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "tries to return a function alias for *sym* in "
	  "*module*",
	  {"sym",kno_symbol_type,KNO_VOID},
	  {"env_arg",kno_any_type,KNO_VOID},
	  {"val",kno_any_type,KNO_VOID})
static lispval qonst_register_prim(lispval sym,lispval env_arg,lispval val)
{
  lispval use_env = KNO_VOID, result = KNO_VOID;
  if ( (KNO_SYMBOLP(env_arg)) || (KNO_STRINGP(env_arg)) )
    use_env = kno_find_module(env_arg,1);
  else if (KNO_LEXENVP(env_arg))
    use_env = env_arg;
  else if ( (KNO_HASHTABLEP(env_arg)) &&
	    (kno_test(env_arg,KNOSYM_MODULEID,KNO_VOID)) )
    use_env = env_arg;
  else return kno_err("NotAModule","fcn_getalias_prim",
		      KNO_SYMBOL_NAME(sym),
		      env_arg);
  if (KNO_ABORTED(use_env))
    return use_env;
  else if (KNO_VOIDP(use_env))
    result = kno_err("BadEnvArg","fcnalias_prim",
		     KNO_SYMBOL_NAME(sym),
		     env_arg);
  else NO_ELSE;
  if (KNO_VOIDP(result)) {
    if ( (KNO_VOIDP(val)) || (KNO_VOIDP(result)) )
      val = (KNO_LEXENVP(use_env)) ?
	(kno_symeval(sym,(kno_lexenv)env_arg)) :
	(KNO_TABLEP(use_env)) ?
	(kno_get(use_env,sym,KNO_VOID)) :
	(KNO_VOID);
    if (KNO_ABORTED(val))
      result = val;
    else if (KNO_VOIDP(val))
      result = kno_err("Unbound","fcnalias_prim",
		       KNO_SYMBOL_NAME(sym),
		       use_env);
    else if ( (KNO_CONSP(val)) &&
	      ( (KNO_FUNCTIONP(val)) ||
		(KNO_APPLICABLEP(val)) ||
		(KNO_EVALFNP(val)) ||
		(KNO_MACROP(val)) ))
      result = kno_qonst_ref(sym,use_env,val);
    else result = kno_incref(val);}
  if ( use_env != env_arg) kno_decref(use_env);
  return result;
}

/* Initialization */

void init_qonsts_c()
{
  qonsts_symbol=kno_intern("%qonsts");
  kno_type_names[kno_qonst_type]=_("qonst");
  kno_type_docs[kno_qonst_type]=_("function identifier ID");
  kno_unparsers[kno_qonst_type]=unparse_qonst;
  u8_init_mutex(&_kno_qonst_lock);
  u8_register_source_file(_FILEINFO);
  KNO_INIT_STATIC_CONS(&qonst_table,kno_hashtable_type);
  kno_make_hashtable(&qonst_table,256);
  kno_register_config
    ("QONST:LEAK",
     "Leak values stored behind function IDs, to avoid use after free due "
     "to dangling references",
     kno_boolconfig_get,kno_boolconfig_set,&_kno_leak_qonsts);
  link_local_cprims();
}


static void link_local_cprims()
{
  kno_def_evalfn(kno_scheme_module,"fcn/alias",fcnalias_evalfn,
		 "`(fcn/alias *sym*)` returns a *qonst* pointer aliasing the "
		 "definition of *sym* in the current environment.");
  KNO_LINK_CPRIM("qonst/register",qonst_register_prim,3,kno_scheme_module);
  KNO_LINK_ALIAS("fcn/getalias",qonst_register_prim,kno_scheme_module);
}
