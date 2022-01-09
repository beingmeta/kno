/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_LISP_CORE 1
#define KNO_INLINE_QONSTS 1
#define KNO_INLINE_XTYPEP 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/tables.h"
#include <libu8/u8rusage.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8logging.h>
#include <libu8/u8printf.h>
#include <stdarg.h>
#include <time.h>
#include <math.h>

#if ((HAVE_LIBDUMA)&&(HAVE_DUMA_H))
#include <duma.h>
#endif

#if HAVE_DLFCN_H
#include <dlfcn.h>
#endif

static int lisp_types_initialized = 0;
double kno_load_start = -1.0;

u8_condition kno_NoMethod=_("Method not supported");

u8_string kno_version = KNO_VERSION;
int kno_major_version = KNO_MAJOR_VERSION;
int kno_minor_version = KNO_MINOR_VERSION;
int kno_release_version = KNO_PATCHLEVEL;

u8_string kno_sysroot = NULL;
u8_string kno_exec_dir = NULL;

#ifdef KNO_SYSROOT
u8_string config_sysroot = KNO_SYSROOT;
#else
u8_string config_sysroot = KNO_INSTALL_ROOT;
#endif

KNO_EXPORT u8_string kno_getversion(){return KNO_VERSION;}
KNO_EXPORT int kno_getmajorversion(){return KNO_MAJOR_VERSION;}

u8_string kno_type_names[KNO_TYPE_MAX] = { NULL };
u8_string kno_type_docs[KNO_TYPE_MAX] = { NULL };
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

/* Useful functions */

KNO_EXPORT int kno_always_true(lispval x) { return 1; }
KNO_EXPORT int kno_always_false(lispval x) { return 0; }

KNO_EXPORT u8_string kno_syspath(u8_string input)
{
  if (kno_sysroot)
    return u8_string_subst(input,config_sysroot,kno_sysroot);
  else return u8_strdup(input);
}

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
  return KNO_XTYPEP(x,type);
}

KNO_EXPORT int _KNO_CHECKTYPE(lispval obj,lispval objtype)
{
  if (KNO_CHOICEP(objtype)) {
    KNO_ITER_CHOICES(types,limit,objtype);
    while (types<limit) {
      if (KNO_CHECKTYPE(obj,*types)) return 1;
      else types++;}
    return 0;}
  else if (KNO_IMMEDIATEP(objtype)) {
    if ( (KNO_VOIDP(objtype)) || (KNO_FALSEP(objtype)) || (KNO_EMPTYP(objtype)) )
      return 1;
    else if (KNO_IMMEDIATE_TYPEP(objtype,kno_symbol_type))
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
    if ( (info->type_basetype>=0) && (!(KNO_XTYPEP(obj,info->type_basetype))) )
      return 0;
    else if (KNO_COMPOUNDP(obj))
      return ( (KNO_COMPOUND_TAG(obj)) == info->typetag) &&
	( (info->type_testfn == NULL) || ((info->type_testfn(obj,info))) );
    else if (KNO_TYPEP(obj,kno_rawptr_type))
      return ( (KNO_RAWPTR_TAG(obj)) == info->typetag) &&
	( (info->type_testfn == NULL) || ((info->type_testfn(obj,info))) );
    else if (info->type_testfn)
      return (info->type_testfn)(obj,info);
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

KNO_EXPORT void *_kno_lspcpy(lispval *dest,const lispval *src,int n)
{
  return memcpy(dest,src,sizeof(lispval)*n);
}

KNO_EXPORT void *_kno_lspset(lispval *dest,lispval val,int n)
{
  return kno_lspset(dest,val,n);
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
void (*_kno_raise)
(u8_condition c,u8_context caller,
 u8_string details,lispval irritant) = NULL;

KNO_EXPORT void kno_raise
(u8_condition c,u8_context cxt,u8_string details,lispval irritant)
{
  if (_kno_raise)
    _kno_raise(c,cxt,details,irritant);
  else {
    u8_exception ex = NULL;
    _kno_mkerr(c,cxt,details,irritant,&ex);
    u8_raise_exception(ex);}
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

/* QONSTs */

KNO_EXPORT lispval _kno_qonst_val(lispval x)
{
  return kno_qonst_val(x);
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

struct typeinfo_initializer {
  int typecode;
  u8_string name;
  u8_string doc; };
struct typeinfo_initializer init_typeinfo[]=
  {
   { kno_cons_type,_("cons"),_("cons")},
   { kno_immediate_type,_("immediate"),_("immediate")},
   { kno_fixnum_type,_("fixnum"),_("fixnum")},
   { kno_oid_type,_("oid"),_("oid")},
   { kno_constant_type,_("constant"),_("constant")},
   { kno_character_type,_("character"),_("character")},
   { kno_symbol_type,_("symbol"),_("symbol")},
   { kno_qonst_type,_("qonst"),_("qonst")},
   { kno_lexref_type,_("lexref"),_("lexref")},
   { kno_opcode_type,_("opcode"),_("opcode")},
   { kno_typeref_type,_("typeref"),_("typeref")},
   { kno_coderef_type,_("coderef"),_("coderef")},
   { kno_poolref_type,_("poolref"),
     _("a pool ID mapped to a static (eternal) pool")},
   { kno_indexref_type,_("indexref"),
     _("an index ID mapped to a static (eternal) index")},
   { kno_histref_type,_("histref"),_("histref")},
   { kno_ctype_type,_("ctype"),
     _("a representation of a primitive type value used by the runtime")},
   { kno_pooltype_type,_("pooltype"),_("an implemented pool type")},
   { kno_indextype_type,_("indextype"),_("an implemented pool type")},

   { kno_string_type,_("string"),_("a UTF-8 encoded string")},
   { kno_packet_type,_("packet"),_("a byte vector")},
   { kno_vector_type,_("vector"),_("vector")},
   { kno_numeric_vector_type,_("numeric_vector"),_("numeric_vector")},
   { kno_pair_type,_("pair"),_("pair")},
   { kno_cdrcode_type,_("cdrcode"),
     _("a half-cons for compact representations of linked lists")},
   { kno_secret_type,_("secret"),
     _("a byte vector which doesn't easily disclose its data")},
   { kno_bigint_type,_("bigint"),_("an unlimited-precision integer")},
   
   { kno_choice_type,_("choice"),_("choice")},
   { kno_prechoice_type,_("prechoice"),_("prechoice")},
   { kno_qchoice_type,_("qchoice"),_("qchoice")},
   { kno_typeinfo_type,_("typeinfo"),_("a type description")},
   { kno_compound_type,_("compound"),_("compound")},
   { kno_rawptr_type,_("rawptr"),_("a 'wrapped' C pointer value")},

   { kno_ioport_type,_("ioport"),_("a textual I/O port")},
   { kno_regex_type,_("regex"),_("regex")},

   { kno_slotmap_type,_("slotmap"),_("slotmap")},
   { kno_schemap_type,_("schemap"),_("schemap")},
   { kno_hashtable_type,_("hashtable"),_("hashtable")},
   { kno_hashset_type,_("hashset"),_("hashset")},

   { kno_cprim_type,_("cprim"),_("cprim")},
   { kno_lambda_type,_("lambda"),_("lambda")},
   { kno_ffi_type,_("ffi"),_("ffi")},
   { kno_rpc_type,_("netproc"),_("netproc")},
   { kno_closure_type,_("closure"),_("closure")},

   { kno_lexenv_type,_("lexenv"),_("lexenv")},
   { kno_evalfn_type,_("evalfn"),_("evalfn")},
   { kno_macro_type,_("macro"),_("macro")},
   { kno_stackframe_type,_("stackframe"),_("stackframe")},
   { kno_exception_type,_("exception"),_("exception")},
   { kno_promise_type,_("promise"),_("promise")},
   { kno_thread_type,_("thread"),_("thread")},
   { kno_synchronizer_type,_("synchronizer"),_("synchronizer")},

   { kno_consblock_type,_("consblock"),_("consblock")},

   { kno_complex_type,_("complex"),_("complex")},
   { kno_rational_type,_("rational"),_("rational")},
   { kno_flonum_type,_("flonum"),_("flonum")},

   { kno_timestamp_type,_("timestamp"),_("timestamp")},
   { kno_uuid_type,_("uuid"),_("uuid")},
   
   { kno_mystery_type,_("mystery"),
     _("an object whose representation could not be decoded")},
   { kno_stream_type,_("stream"),_("a binary I/O stream")},
   
   { kno_service_type,_("service"),_("service")},
   
   { kno_sqlconn_type,_("sqlconn"),_("sqlconn")},
   { kno_sqlproc_type,_("sqlproc"),_("sqlproc")},
   
    { kno_consed_pool_type,_("raw pool"),
      _("a pointer to an unregistered ('ephemeral') pool")},
    { kno_consed_index_type,_("raw index"),
      _("a pointer to an unregistered ('ephemeral') index")},

   { kno_subproc_type,_("subprocess"),_("a sub-process (subproc) object")},

   { kno_empty_type,_("empty"),_("the empty/fail value")},
   { kno_exists_type,_("exists"),_("the complement of the empty/fail type")},
   { kno_true_type,_("true"),_("the type for non-false values")},
   { kno_error_type,_("error"),_("the type for error values")},
   { kno_void_type,_("void"),_("the type for VOID values")},
   { kno_satisfied_type,_("satisfied"),
     _("the type for non-false non-empty values")},
   { kno_singleton_type,_("singleton"),
     _("the type for non-empty non-ambiguous values")},

   { kno_number_type,_("number"),_("numeric values")},
   { kno_integer_type,_("integer"),_("integral numeric values")},
   { kno_exact_type,_("exact"),_("exact numbers")},
   { kno_inexact_type,_("inexact"),_("inexact numbers")},

   { kno_sequence_type,_("sequence"),_("a sequence object")},
   { kno_table_type,_("table"),_("a table object")},
   { kno_applicable_type,_("applicable"),
     _("an applicable object (prodcure, primitive, etc)")},
   { kno_xfunction_type,_("function"),
     _("applicable objects with metadata")},
   { kno_keymap_type,_("keymap"),_("a slotmap or schemap")},
   { kno_type_type,_("type"),
     _("a ctype, a type tag (symbol or OID) or a typeinfo object")},
   { kno_opts_type,_("optsarg"),_("an option set data structure")},
   { kno_frame_type,_("frame"),_("a `frame`, an OID or keymap")},
   { kno_slotid_type,_("slotid"),_("a `slotid`, a symbol or an OID")},
   { kno_pool_type,_("pool"),
     _("a registered pool id or a consed pool object")},
   { kno_index_type,_("index"),
     _("a registered index id or a consed index object")},
   { -1,NULL,NULL }
  };

static void init_type_names()
{
  struct typeinfo_initializer *scan = init_typeinfo;
  while (scan->typecode >= 0) {
    kno_type_names[scan->typecode] = scan->name;
    kno_type_docs[scan->typecode] = scan->doc;
    scan++;}
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
           "kno-%s %s<%ld> elapsed %.3f%s (u=%.3f%s,s=%.3f%s), heap=%.0f%s\n",
           KNO_VERSION,u8_appid(),getpid(),
           elapsed,etu,usertime,utu,systime,stu,
           heapsize,heapu);}
}

#if KNO_RELOCATION_ENABLED
static void relocate_sysroot()
{
  u8_string sysroot = u8_getenv("KNO_SYSROOT");
  if (sysroot==NULL) sysroot=u8_getenv("KNOROOT");
#if (HAVE_DLADDR)
  if (sysroot==NULL) {
    Dl_info info;
    int rv = dladdr(&kno_sysroot,&info);
    if ( (rv) && (info.dli_fname) ) {
      u8_string modname = u8_fromlibc((char *)info.dli_fname);
      u8_string dirname = u8_dirname(modname);
      u8_string parent = u8_dirname(dirname);
      sysroot = parent;
      u8_free(dirname);
      u8_free(modname);}}
#endif
  if (sysroot==NULL) {}
  else if (u8_has_suffix(sysroot,"/",0))
    kno_sysroot=sysroot;
  else {
    kno_sysroot=u8_string_append(sysroot,"/",NULL);
    u8_free(sysroot);}
  if (strcmp(kno_sysroot,config_sysroot)==0) {
    /* Don't bother translating paths if the configured sysroot
       and the kno_sysroot are the same. */
    u8_free(kno_sysroot);
    kno_sysroot=NULL;}
}
#endif

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

#if KNO_RELOCATION_ENABLED
  relocate_sysroot();
#endif

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
  kno_add_constname("number",KNO_NUMBER_TYPE);
  kno_add_constname("table",KNO_TABLE_TYPE);
  kno_add_constname("sequence",KNO_SEQUENCE_TYPE);
  kno_add_constname("frame",KNO_FRAME_TYPE);
  kno_add_constname("slotid",KNO_SLOTID_TYPE);
  kno_add_constname("pool",KNO_POOL_TYPE);
  kno_add_constname("index",KNO_INDEX_TYPE);

  u8_threadcheck();

  return lisp_types_initialized;
}

