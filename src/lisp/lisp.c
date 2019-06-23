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
#include <libu8/u8rusage.h>
#include <libu8/u8logging.h>
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
  kno_type_names[kno_oid_type]=_("oid");
  kno_type_names[kno_fixnum_type]=_("fixnum");
  kno_type_names[kno_cons_type]=_("cons");
  kno_type_names[kno_immediate_type]=_("immediate");
  kno_type_names[kno_constant_type]=_("constant");
  kno_type_names[kno_character_type]=_("character");
  kno_type_names[kno_symbol_type]=_("symbol");
  kno_type_names[kno_fcnid_type]=_("fcnid");
  kno_type_names[kno_lexref_type]=_("lexref");
  kno_type_names[kno_opcode_type]=_("opcode");
  kno_type_names[kno_type_type]=_("typeref");
  kno_type_names[kno_coderef_type]=_("cdrcode");
  kno_type_names[kno_pool_type]=_("pool");
  kno_type_names[kno_index_type]=_("index");
  kno_type_names[kno_histref_type]=_("histref");

  kno_type_names[kno_string_type]=_("string");
  kno_type_names[kno_packet_type]=_("packet");
  kno_type_names[kno_secret_type]=_("secret");
  kno_type_names[kno_bigint_type]=_("bigint");
  kno_type_names[kno_pair_type]=_("pair");

  kno_type_names[kno_compound_type]=_("compound");
  kno_type_names[kno_choice_type]=_("choice");
  kno_type_names[kno_prechoice_type]=_("prechoice");
  kno_type_names[kno_qchoice_type]=_("quoted choice");
  kno_type_names[kno_vector_type]=_("vector");
  kno_type_names[kno_matrix_type]=_("matrix");
  kno_type_names[kno_numeric_vector_type]=_("numvec");

  kno_type_names[kno_slotmap_type]=_("slotmap");
  kno_type_names[kno_schemap_type]=_("schemap");
  kno_type_names[kno_hashtable_type]=_("hashtable");
  kno_type_names[kno_hashset_type]=_("hashset");

  kno_type_names[kno_cprim_type]=_("builtin function");
  kno_type_names[kno_lexenv_type]=_("environment");
  kno_type_names[kno_evalfn_type]=_("evalfn");
  kno_type_names[kno_macro_type]=_("macro");
  kno_type_names[kno_dtproc_type]=_("dtproc");
  kno_type_names[kno_stackframe_type]=_("stackframe");
  kno_type_names[kno_tailcall_type]=_("tailcall");
  kno_type_names[kno_lambda_type]=_("lambda procedure");
  kno_type_names[kno_ffi_type]=_("foreign function");
  kno_type_names[kno_exception_type]=_("error");

  kno_type_names[kno_complex_type]=_("complex");
  kno_type_names[kno_rational_type]=_("rational");
  kno_type_names[kno_flonum_type]=_("flonum");

  kno_type_names[kno_timestamp_type]=_("timestamp");
  kno_type_names[kno_uuid_type]=_("UUID");

  kno_type_names[kno_mystery_type]=_("mystery");
  kno_type_names[kno_port_type]=_("ioport");
  kno_type_names[kno_stream_type]=_("stream");

  kno_type_names[kno_regex_type]=_("regex");
  kno_type_names[kno_numeric_vector_type]=_("numeric vector");

  kno_type_names[kno_consblock_type]=_("consblock");

  kno_type_names[kno_rawptr_type]=_("raw pointer");
  kno_type_names[kno_dtserver_type]=_("dtype server");
  kno_type_names[kno_bloom_filter_type]=_("bloom filter");
}

static int lisp_types_version = 101;

KNO_EXPORT void kno_init_cons_c(void);
KNO_EXPORT void kno_init_compare_c(void);
KNO_EXPORT void kno_init_recycle_c(void);
KNO_EXPORT void kno_init_copy_c(void);
KNO_EXPORT void kno_init_compare_c(void);
KNO_EXPORT void kno_init_compounds_c(void);
KNO_EXPORT void kno_init_misctypes_c(void);
KNO_EXPORT void kno_init_oids_c(void);
KNO_EXPORT void kno_init_textio_c(void);
KNO_EXPORT void kno_init_parse_c(void);
KNO_EXPORT void kno_init_unparse_c(void);
KNO_EXPORT void kno_init_pprint_c(void);
KNO_EXPORT void kno_init_ports_c(void);
KNO_EXPORT void kno_init_dtread_c(void);
KNO_EXPORT void kno_init_dtwrite_c(void);
KNO_EXPORT void kno_init_tables_c(void);
KNO_EXPORT void kno_init_symbols_c(void);
KNO_EXPORT void kno_init_numbers_c(void);
KNO_EXPORT void kno_init_choices_c(void);
KNO_EXPORT void kno_init_support_c(void);
KNO_EXPORT void kno_init_consblocks_c(void);
KNO_EXPORT void kno_init_sequences_c(void);
KNO_EXPORT void kno_init_fcnids_c(void);
KNO_EXPORT void kno_init_stacks_c(void);
KNO_EXPORT void kno_init_apply_c(void);
KNO_EXPORT void kno_init_build_info(void);

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
  kno_init_recycle_c();
  kno_init_copy_c();
  kno_init_compare_c();
  kno_init_compounds_c();
  kno_init_misctypes_c();
  kno_init_oids_c();
  kno_init_unparse_c();
  kno_init_pprint_c();
  kno_init_parse_c();
  kno_init_tables_c();
  kno_init_symbols_c();
  kno_init_support_c();
  kno_init_dtread_c();
  kno_init_dtwrite_c();
  kno_init_numbers_c();
  kno_init_choices_c();
  kno_init_stacks_c();
  kno_init_apply_c();
  kno_init_sequences_c();
  kno_init_consblocks_c();
  kno_init_fcnids_c();
  kno_init_build_info();

  u8_threadcheck();

  return lisp_types_initialized;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
