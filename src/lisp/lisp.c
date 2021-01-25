/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/tables.h"
#include <libu8/u8rusage.h>
#include <libu8/u8logging.h>
#include <libu8/u8printf.h>
#include <stdarg.h>
#include <time.h>
#include <math.h>

#if ((HAVE_LIBDUMA)&&(HAVE_DUMA_H))
#include <duma.h>
#endif

static int lisp_types_initialized = 0;
double kno_load_start = -1.0;

u8_condition kno_NoMethod=_("Method not supported");

u8_string kno_version = KNO_VERSION;
u8_string kno_revision = KNO_REVISION;
int kno_major_version = KNO_MAJOR_VERSION;
int kno_minor_version = KNO_MINOR_VERSION;
int kno_release_version = KNO_RELEASE_VERSION;

KNO_EXPORT u8_string kno_getversion(){return KNO_VERSION;}
KNO_EXPORT u8_string kno_getrevision(){return KNO_REVISION;}
KNO_EXPORT int kno_getmajorversion(){return KNO_MAJOR_VERSION;}

u8_string kno_type_names[KNO_TYPE_MAX];
u8_string kno_type_docs[KNO_TYPE_MAX];
struct KNO_TABLEFNS *kno_tablefns[KNO_TYPE_MAX] = { NULL };

int kno_lockdown = 0;

#if ((KNO_THREADS_ENABLED)&&(KNO_USE_TLS))
u8_tld_key kno_curthread_key;
#elif ((KNO_THREADS_ENABLED)&&(HAVE_THREAD_STORAGE_CLASS))
__thread lispval kno_current_thread = KNO_VOID;
#else
lispval kno_current_thread = KNO_VOID;
#endif

u8_condition kno_ThreadTerminated=_("Thread terminated");
u8_condition kno_ThreadInterrupted=_("Thread interrupted");

/* External functional versions of common macros */

KNO_EXPORT kno_lisp_type _KNO_TYPEOF(lispval x)
{
  int type_field = (KNO_PTR_MANIFEST_TYPE(x));
  switch (type_field) {
  case kno_cons_type: {
    struct KNO_CONS *cons = (struct KNO_CONS *)x;
    return KNO_CONS_TYPEOF(cons);}
  case kno_immediate_type:
    return KNO_IMMEDIATE_TYPE(x);
  default: return type_field;
  }
}

KNO_EXPORT int _KNO_TYPEP(lispval ptr,int type)
{
  if (type < 0x04)
    return ( ( (ptr) & (0x3) ) == type);
  else if (type >= 0x84)
    if ( (KNO_CONSP(ptr)) && (KNO_CONS_TYPEOF(ptr) == type) )
      return 1;
    else return 0;
  else if (type >= 0x04)
    if ( (KNO_IMMEDIATE_TYPEP(ptr,type) ) )
      return 1;
    else return 0;
  else return 0;
}

KNO_EXPORT lispval _KNO_INT2LISP(long long intval)
{
  if   ( (intval > KNO_MAX_FIXNUM) ||
         (intval < KNO_MIN_FIXNUM) )
    return kno_make_bigint(intval);
  else return KNO_INT2FIX(intval);
}

KNO_EXPORT int _KNO_ABORTP(lispval x)
{
  return ( (KNO_TYPEP(x,kno_constant_type)) &&
           (KNO_GET_IMMEDIATE(x,kno_constant_type)>6) &&
           (KNO_GET_IMMEDIATE(x,kno_constant_type)<=16));
}

KNO_EXPORT int _KNO_ERRORP(lispval x)
{
  return (((KNO_TYPEP(x,kno_constant_type)) &&
           (KNO_GET_IMMEDIATE(x,kno_constant_type)>6) &&
           (KNO_GET_IMMEDIATE(x,kno_constant_type)<15)));
}

KNO_EXPORT int _KNO_SEQUENCEP(lispval x)
{
  if (KNO_CONSP(x))
    if ( (KNO_CONS_TYPEOF(x) >= kno_string_type) &&
         (KNO_CONS_TYPEOF(x) <= kno_pair_type) )
      return 1;
    else if ( (kno_seqfns[KNO_CONS_TYPEOF(x)] != NULL ) &&
              ( (kno_seqfns[KNO_CONS_TYPEOF(x)]->sequencep == NULL ) ||
                (kno_seqfns[KNO_CONS_TYPEOF(x)]->sequencep(x)) ) )
      return 1;
    else return 0;
  else if (x == KNO_EMPTY_LIST)
    return 1;
  else if (KNO_IMMEDIATEP(x))
    if ( (kno_seqfns[KNO_IMMEDIATE_TYPE(x)] != NULL ) &&
	 ( (kno_seqfns[KNO_IMMEDIATE_TYPE(x)]->sequencep == NULL ) ||
	   (kno_seqfns[KNO_IMMEDIATE_TYPE(x)]->sequencep(x)) ) )
      return 1;
    else return 0;
  else return 0;
}

KNO_EXPORT int _KNO_TABLEP(lispval x)
{
  if (KNO_OIDP(x)) return 1;
  else if (KNO_CONSP(x)) {
    if ( ( KNO_CONS_TYPEOF(x) >= kno_slotmap_type) &&
         ( KNO_CONS_TYPEOF(x) <= kno_hashset_type) )
      return 1;
    else if ( (kno_tablefns[KNO_CONS_TYPEOF(x)] != NULL ) &&
              ( (kno_tablefns[KNO_CONS_TYPEOF(x)]->tablep == NULL ) ||
                (kno_tablefns[KNO_CONS_TYPEOF(x)]->tablep(x)) ) )
      return 1;
    else return 0;}
  else if (KNO_IMMEDIATEP(x)) {
    if ( (kno_tablefns[KNO_IMMEDIATE_TYPE(x)] != NULL ) &&
         ( (kno_tablefns[KNO_IMMEDIATE_TYPE(x)]->tablep == NULL ) ||
           (kno_tablefns[KNO_IMMEDIATE_TYPE(x)]->tablep(x)) ) )
      return 1;
    else return 0;}
  else return 0;
}

KNO_EXPORT int _KNO_XTYPEP(lispval x,int type)
{
  if (type <=  kno_opts_type) switch ((kno_lisp_type)type) {
    case kno_number_type: return KNO_NUMBERP(x);
    case kno_sequence_type: return KNO_SEQUENCEP(x);
    case kno_table_type: return KNO_TABLEP(x);
    case kno_type_type:
      if ( (KNO_OIDP(x)) || (KNO_SYMBOLP(x)) ||
	   (KNO_IMMEDIATE_TYPEP(x,kno_ctype_type)) ||
	   (KNO_TYPEP(x,kno_typeinfo_type)) )
	return 1;
      else return 0;
    case kno_keymap_type:
      if (KNO_CONSP(x)) {
	kno_lisp_type ctype = KNO_CONS_TYPEOF(x);
	return ( (ctype && kno_table_type) == ctype );}
      else return 0;
    case kno_opts_type:
      if (KNO_CONSP(x)) {
	kno_lisp_type ctype = KNO_CONS_TYPEOF(x);
	return ( (ctype == kno_pair_type) &&
		 ( (ctype && kno_table_type) == ctype ) );}
      else if ( (x == KNO_FALSE) || (x == KNO_EMPTY_LIST) || (x == KNO_EMPTY_CHOICE) )
	return 1;
      else return 0;
    default:
      if  (type < 0x04) return ( ( (x) & (0x3) ) == type);
      else if (type < 0x84) return (KNO_IMMEDIATE_TYPEP(x,type));
      else if (type < 0x100) return ( (x) && ((KNO_CONS_TYPEOF(x)) == type) );
      else return 0;}
  else return 0;
}

KNO_EXPORT int _KNO_CHECKTYPE(lispval obj,lispval objtype)
{
  if (KNO_IMMEDIATEP(objtype)) {
    if (KNO_IMMEDIATE_TYPEP(objtype,kno_symbol_type))
      return ( ( (KNO_COMPOUNDP(obj)) && ( (KNO_COMPOUND_TAG(obj)) == objtype) ) ||
	       ( (KNO_TYPEP(obj,kno_rawptr_type)) && \
		 ( (KNO_RAWPTR_TAG(obj)) == objtype) ) );
    else if (KNO_IMMEDIATE_TYPEP(objtype,kno_ctype_type)) {
      kno_lisp_type ltype = (KNO_IMMEDIATE_DATA(objtype));
      return (KNO_TYPEP(obj,ltype));}
    else return 0;}
  else if (KNO_OIDP(objtype))
    return ( ( (KNO_COMPOUNDP(obj)) && ( (KNO_COMPOUND_TAG(obj)) == objtype) ) ||
	     ( (KNO_TYPEP(obj,kno_rawptr_type)) && \
	       ( (KNO_RAWPTR_TAG(obj)) == objtype) ) );
  else if (KNO_TYPEP(objtype,kno_typeinfo_type)) {
    struct KNO_TYPEINFO *info = (kno_typeinfo) objtype;
    if ( (info->type_basetype>=0) && (!(KNO_TYPEP(obj,info->type_basetype))) )
      return 0;
    else if (KNO_COMPOUNDP(obj))
      return ( (KNO_COMPOUND_TAG(obj)) == info->typetag) &&
	( (info->type_testfn == NULL) || ((info->type_testfn(obj,info))) );
    else if (KNO_TYPEP(obj,kno_rawptr_type))
      return ( (KNO_RAWPTR_TAG(obj)) == info->typetag) &&
	( (info->type_testfn == NULL) || ((info->type_testfn(obj,info))) );
    else return 0;}
  else return 0;
}

KNO_EXPORT int _KNO_CHOICE_SIZE(lispval x)
{
  if (KNO_EMPTYP(x))
    return 0;
  else if (KNO_CHOICEP(x)) {
    struct KNO_CHOICE *ch = (kno_choice) x;
    return ch->choice_size;}
  else return 1;
}

KNO_EXPORT lispval _kno_return_errcode(lispval x)
{
  return x;
}

/* Errors */

KNO_EXPORT void kno_decref_u8x_xdata(void *ptr)
{
  lispval v = (lispval)ptr;
  kno_decref(v);
}

KNO_EXPORT lispval kno_simple_error
(u8_condition c,u8_context cxt,u8_string details,lispval irritant,
 u8_exception *push)
{
  u8_exception ex = u8_new_exception
    (c,cxt,(details)?(u8_strdup(details)):(NULL),(void *)irritant,
     kno_decref_u8x_xdata);
  if (push) *push = ex;
  else u8_expush(ex);
  return KNO_ERROR;
}

lispval (*_kno_mkerr)
(u8_condition c,u8_context caller,
 u8_string details,lispval irritant,
 u8_exception *push) = kno_simple_error;

KNO_EXPORT void kno_raise
(u8_condition c,u8_context cxt,u8_string details,lispval irritant)
{
  u8_exception ex = NULL;
  _kno_mkerr(c,cxt,details,irritant,&ex);
  u8_raise_exception(ex);
}

KNO_EXPORT void kno_missing_error(u8_string details)
{
  u8_exception ex = u8_current_exception;
  if (ex==NULL)
    u8_seterr("UndeclaredError","kno_missing_error",
	      u8_strdup(details));
}

KNO_EXPORT kno_lisp_type _kno_typeof(lispval x)
{
  return KNO_TYPEOF(x);
}

KNO_EXPORT lispval _kno_debug(lispval x)
{
  return x;
}

static int log_max_refcount = 1, debug_max_refcount = 1;
static long long refcount_max = -1;

KNO_EXPORT void _kno_refcount_overflow(lispval x,long long count,u8_context op)
{
  if ( ( refcount_max > 0 ) && ( count < refcount_max) ) return;
  if (log_max_refcount) {
    kno_lisp_type constype = KNO_TYPEOF(x);
    u8_log(LOGWARN,"MaxRefcount","On %s: count=%lld=0x%llx %llx (%s)",
	   op,count,count,x,kno_type2name(constype));}
  if (debug_max_refcount) _kno_debug(x);
}

/* Initialization procedures */

extern void kno_init_choices_c(void);
extern void kno_init_tables_c(void);

static void register_header_files()
{
  u8_register_source_file(KNO_CONFIG_H_INFO);
  u8_register_source_file(KNO_SUPPORT_H_INFO);
  u8_register_source_file(KNO_MALLOC_H_INFO);
  u8_register_source_file(KNO_COMMON_H_INFO);
  u8_register_source_file(KNO_DEFINES_H_INFO);
  u8_register_source_file(KNO_PTR_H_INFO);
  u8_register_source_file(KNO_CONS_H_INFO);
  u8_register_source_file(KNO_LISP_H_INFO);
  u8_register_source_file(KNO_CHOICES_H_INFO);
  u8_register_source_file(KNO_TABLES_H_INFO);
  u8_register_source_file(KNO_DTYPEIO_H_INFO);
}

static void init_type_names()
{
  memset(kno_type_names,0,sizeof(u8_string)*KNO_TYPE_MAX);
  memset(kno_type_docs,0,sizeof(u8_string)*KNO_TYPE_MAX);

  kno_type_names[kno_oid_type]=_("oid");
  kno_type_docs[kno_oid_type]=_("oid");
  kno_type_names[kno_fixnum_type]=_("fixnum");
  kno_type_docs[kno_fixnum_type]=_("fixnum");
  kno_type_names[kno_cons_type]=_("cons");
  kno_type_docs[kno_cons_type]=_("cons");
  kno_type_names[kno_immediate_type]=_("immediate");
  kno_type_docs[kno_immediate_type]=_("immediate");
  kno_type_names[kno_constant_type]=_("constant");
  kno_type_docs[kno_constant_type]=_("constant");
  kno_type_names[kno_character_type]=_("character");
  kno_type_docs[kno_character_type]=_("character");
  kno_type_names[kno_symbol_type]=_("symbol");
  kno_type_docs[kno_symbol_type]=_("symbol");
  kno_type_names[kno_fcnid_type]=_("fcnid");
  kno_type_docs[kno_fcnid_type]=_("fcnid");
  kno_type_names[kno_lexref_type]=_("lexref");
  kno_type_docs[kno_lexref_type]=_("lexref");
  kno_type_names[kno_opcode_type]=_("opcode");
  kno_type_docs[kno_opcode_type]=_("opcode");
  kno_type_names[kno_typeref_type]=_("typeref");
  kno_type_docs[kno_typeref_type]=_("typeref");
  kno_type_names[kno_coderef_type]=_("coderef");
  kno_type_docs[kno_coderef_type]=_("coderef");
  kno_type_names[kno_pool_type]=_("pool");
  kno_type_docs[kno_pool_type]=_("pool");
  kno_type_names[kno_index_type]=_("index");
  kno_type_docs[kno_index_type]=_("index");
  kno_type_names[kno_histref_type]=_("histref");
  kno_type_docs[kno_histref_type]=_("histref");
  kno_type_names[kno_ctype_type]=_("basetype");
  kno_type_docs[kno_ctype_type]=_("a representation of a primitive C type");

  kno_type_names[kno_string_type]=_("string");
  kno_type_docs[kno_string_type]=_("string");
  kno_type_names[kno_packet_type]=_("packet");
  kno_type_docs[kno_packet_type]=_("packet");
  kno_type_names[kno_secret_type]=_("secret");
  kno_type_docs[kno_secret_type]=_("secret");
  kno_type_names[kno_bigint_type]=_("bigint");
  kno_type_docs[kno_bigint_type]=_("bigint");
  kno_type_names[kno_pair_type]=_("pair");
  kno_type_docs[kno_pair_type]=_("pair");
  kno_type_names[kno_cdrcode_type]=_("cdrcode");
  kno_type_docs[kno_cdrcode_type]=_("cdrcode");

  kno_type_names[kno_typeinfo_type]=_("typeinfo");
  kno_type_docs[kno_typeinfo_type]=_("typeinfo");
  kno_type_names[kno_compound_type]=_("compound");
  kno_type_docs[kno_compound_type]=_("compound");
  kno_type_names[kno_rawptr_type]=_("rawptr");
  kno_type_docs[kno_rawptr_type]=_("rawptr");

  kno_type_names[kno_choice_type]=_("choice");
  kno_type_docs[kno_choice_type]=_("choice");
  kno_type_names[kno_prechoice_type]=_("prechoice");
  kno_type_docs[kno_prechoice_type]=_("prechoice");
  kno_type_names[kno_qchoice_type]=_("qchoice");
  kno_type_docs[kno_qchoice_type]=_("qchoice");

  kno_type_names[kno_vector_type]=_("vector");
  kno_type_docs[kno_vector_type]=_("vector");
  kno_type_names[kno_numeric_vector_type]=_("numeric_vector");
  kno_type_docs[kno_numeric_vector_type]=_("numeric_vector");

  kno_type_names[kno_slotmap_type]=_("slotmap");
  kno_type_docs[kno_slotmap_type]=_("slotmap");
  kno_type_names[kno_schemap_type]=_("schemap");
  kno_type_docs[kno_schemap_type]=_("schemap");
  kno_type_names[kno_hashtable_type]=_("hashtable");
  kno_type_docs[kno_hashtable_type]=_("hashtable");
  kno_type_names[kno_hashset_type]=_("hashset");
  kno_type_docs[kno_hashset_type]=_("hashset");

  kno_type_names[kno_cprim_type]=_("cprim");
  kno_type_docs[kno_cprim_type]=_("cprim");
  kno_type_names[kno_ffi_type]=_("ffi");
  kno_type_docs[kno_ffi_type]=_("ffi");
  kno_type_names[kno_lambda_type]=_("lambda");
  kno_type_docs[kno_lambda_type]=_("lambda");
  kno_type_names[kno_rpc_type]=_("netproc");
  kno_type_docs[kno_rpc_type]=_("netproc");

  kno_type_names[kno_lexenv_type]=_("lexenv");
  kno_type_docs[kno_lexenv_type]=_("lexenv");
  kno_type_names[kno_evalfn_type]=_("evalfn");
  kno_type_docs[kno_evalfn_type]=_("evalfn");
  kno_type_names[kno_macro_type]=_("macro");
  kno_type_docs[kno_macro_type]=_("macro");
  kno_type_names[kno_stackframe_type]=_("stackframe");
  kno_type_docs[kno_stackframe_type]=_("stackframe");
  kno_type_names[kno_exception_type]=_("exception");
  kno_type_docs[kno_exception_type]=_("exception");
  kno_type_names[kno_promise_type]=_("promise");
  kno_type_docs[kno_promise_type]=_("promise");
  kno_type_names[kno_thread_type]=_("thread");
  kno_type_docs[kno_thread_type]=_("thread");
  kno_type_names[kno_synchronizer_type]=_("synchronizer");
  kno_type_docs[kno_synchronizer_type]=_("synchronizer");

  kno_type_names[kno_complex_type]=_("complex");
  kno_type_docs[kno_complex_type]=_("complex");
  kno_type_names[kno_rational_type]=_("rational");
  kno_type_docs[kno_rational_type]=_("rational");
  kno_type_names[kno_flonum_type]=_("flonum");
  kno_type_docs[kno_flonum_type]=_("flonum");

  kno_type_names[kno_timestamp_type]=_("timestamp");
  kno_type_docs[kno_timestamp_type]=_("timestamp");
  kno_type_names[kno_uuid_type]=_("uuid");
  kno_type_docs[kno_uuid_type]=_("uuid");

  kno_type_names[kno_mystery_type]=_("mystery");
  kno_type_docs[kno_mystery_type]=
    _("an object whose representation could not be decoded");
  kno_type_names[kno_ioport_type]=_("ioport");
  kno_type_docs[kno_ioport_type]=_("a textual I/O port");
  kno_type_names[kno_stream_type]=_("stream");
  kno_type_docs[kno_stream_type]=_("a binary I/O stream");

  kno_type_names[kno_regex_type]=_("regex");
  kno_type_docs[kno_regex_type]=_("regex");

  kno_type_names[kno_subjob_type]=_("subjog");
  kno_type_docs[kno_subjob_type]=_("a sub-process (subjob) object");

  kno_type_names[kno_consblock_type]=_("consblock");
  kno_type_docs[kno_consblock_type]=_("consblock");

  kno_type_names[kno_consed_pool_type]=_("raw pool");
  kno_type_docs[kno_consed_pool_type]=
    _("a pointer to an unregistered ('eternal') pool");

  kno_type_names[kno_consed_index_type]=_("raw index");
  kno_type_docs[kno_consed_index_type]=
    _("a pointer to an unregistered ('eternal') index");

  kno_type_names[kno_sqldb_type]=_("sqldb");
  kno_type_docs[kno_sqldb_type]=_("sqldb");
  kno_type_names[kno_sqlproc_type]=_("sqlproc");
  kno_type_docs[kno_sqlproc_type]=_("sqlproc");

  kno_type_names[kno_service_type]=_("service");
  kno_type_docs[kno_service_type]=_("service");

  kno_type_names[kno_bloom_filter_type]=_("bloom_filter");
  kno_type_docs[kno_bloom_filter_type]=_("bloom_filter");

  kno_type_names[kno_pathstore_type]=_("pathstore");
  kno_type_docs[kno_pathstore_type]=_("pathstore");
 
  kno_type_names[kno_type_type]=_("type");
  kno_type_docs[kno_type_type]=
    _("a type reference is a basetype, a tag (symbol or OID) or a typeinfo object");

  kno_type_names[kno_keymap_type]=_("keymap");
  kno_type_docs[kno_keymap_type]=_("a slotmap or schemap");

  kno_type_names[kno_table_type]=_("table");
  kno_type_docs[kno_table_type]=_("a table object");

  kno_type_names[kno_sequence_type]=_("sequence");
  kno_type_docs[kno_sequence_type]=_("a sequence object");

  kno_type_names[kno_opts_type]=_("optsarg");
  kno_type_docs[kno_opts_type]=_("an opts data structure");
}

static int lisp_types_version = 101;

void kno_init_cons_c(void);
void kno_init_typeinfo_c(void);
void kno_init_compare_c(void);
void kno_init_recycle_c(void);
void kno_init_copy_c(void);
void kno_init_compare_c(void);
void kno_init_compounds_c(void);
void kno_init_misctypes_c(void);
void kno_init_oids_c(void);
void kno_init_textio_c(void);
void kno_init_parse_c(void);
void kno_init_unparse_c(void);
void kno_init_pprint_c(void);
void kno_init_ports_c(void);
void kno_init_xtypes_c(void);
void kno_init_dtread_c(void);
void kno_init_dtwrite_c(void);
void kno_init_tables_c(void);
void kno_init_symbols_c(void);
void kno_init_numbers_c(void);
void kno_init_choices_c(void);
void kno_init_support(void);
void kno_init_consblocks_c(void);
void kno_init_sequences_c(void);
void kno_init_fcnids_c(void);
void kno_init_stacks_c(void);
void kno_init_apply_c(void);
void kno_init_build_info(void);

static double format_secs(double secs,char **units)
{
  if (secs<0.001) {*units="us"; return secs*1000000;}
  if (secs<1) {*units="ms"; return secs*1000;}
  if (secs<300) {*units="s"; return secs;}
  if (secs>300) {*units="m"; return secs/60;}
  if (secs>7200) {*units="h"; return secs/3600;}
  if (secs>3600*24) {*units="d"; return secs/3600;}
  return secs;
}
KNO_EXPORT void kno_log_status(u8_condition why)
{
  struct rusage usage;
  int retval = u8_getrusage(0,&usage);
  if (why==NULL) why="Status";
  if (retval<0) {
    u8_log(LOG_CRIT,_("RUSAGE Failed"),
           "During a call to kno_log_status (%s)",why);
    return;}
  else {
    /* long membytes = (usage.ru_idrss+usage.ru_isrss); double memsize; */
    ssize_t heapbytes = u8_memusage(); double heapsize;
    char *stu="s", *utu="s", *etu="s", *heapu="KB";
    double elapsed = format_secs(u8_elapsed_time(),&etu);
    double usertime = format_secs
      (usage.ru_utime.tv_sec+(((double)usage.ru_utime.tv_usec)/1000000),
       &utu);
    double systime = format_secs
      (usage.ru_stime.tv_sec+(((double)usage.ru_stime.tv_usec)/1000000),
       &stu);
    if (heapbytes>10000000000) {
      heapsize = floor(((double)heapbytes)/1000000000); heapu="GB";}
    else if (heapbytes>1500000) {
      heapsize = floor(((double)heapbytes)/1000000); heapu="MB";}
    else {heapsize = floor(((double)heapbytes)/1000); heapu="KB";}
    u8_log(U8_LOG_MSG,why,
           "%s %s<%ld> elapsed %.3f%s (u=%.3f%s,s=%.3f%s), heap=%.0f%s\n",
           KNO_REVISION,u8_appid(),getpid(),
           elapsed,etu,usertime,utu,systime,stu,
           heapsize,heapu);}
}

KNO_EXPORT int kno_init_lisp_types()
{
  int u8_version;
#if ((HAVE_LIBDUMA)&&(HAVE_DUMA_H))
  DUMA_SET_ALIGNMENT(4);
#endif
  if (lisp_types_initialized) return lisp_types_initialized;
  kno_load_start = u8_elapsed_time();
  u8_version = u8_initialize();
  lisp_types_initialized = lisp_types_version*u8_version;

#if ((KNO_THREADS_ENABLED)&&(KNO_USE_TLS))
  u8_new_threadkey(&kno_curthread_key,NULL);
#endif

  u8_register_source_file(_FILEINFO);

  register_header_files();

  u8_init_rwlock(&kno_symbol_lock);
  kno_init_cons_c();
  init_type_names();
  kno_init_typeinfo_c();
  kno_init_recycle_c();
  kno_init_copy_c();
  kno_init_compare_c();
  kno_init_oids_c();
  kno_init_unparse_c();
  kno_init_pprint_c();
  kno_init_parse_c();
  kno_init_tables_c();
  kno_init_symbols_c();
  kno_init_xtypes_c();
  kno_init_support();
  kno_init_dtread_c();
  kno_init_dtwrite_c();
  kno_init_numbers_c();
  kno_init_choices_c();
  kno_init_stacks_c();
  kno_init_apply_c();
  kno_init_misctypes_c();
  kno_init_sequences_c();
  kno_init_compounds_c();
  kno_init_consblocks_c();
  kno_init_fcnids_c();
  kno_init_build_info();

  int typecode = 0; while (typecode < KNO_TYPE_MAX) {
    if (kno_type_names[typecode]) {
      lispval typecode_value = LISPVAL_IMMEDIATE(kno_ctype_type,typecode);
      u8_byte buf[100];
      u8_string hashname = u8_bprintf(buf,"%s_type",kno_type_names[typecode]);
      if (kno_add_constname(hashname,typecode_value)<0)
	u8_log(LOGCRIT,"BadTypeName",
	       "Couldn't register typename '%s' for typecode=%d",
	       hashname,typecode);}
    typecode++;}

  u8_threadcheck();

  return lisp_types_initialized;
}

