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
#include <libu8/u8rusage.h>
#include <libu8/u8logging.h>
#include <stdarg.h>
#include <time.h>
#include <math.h>

#if ((HAVE_LIBDUMA)&&(HAVE_DUMA_H))
#include <duma.h>
#endif

static int lisp_types_initialized = 0;
double fd_load_start = -1.0;

u8_condition fd_NoMethod=_("Method not supported");

u8_string fd_version = FD_VERSION;
u8_string fd_revision = FRAMERD_REVISION;
int fd_major_version = FD_MAJOR_VERSION;
int fd_minor_version = FD_MINOR_VERSION;
int fd_release_version = FD_RELEASE_VERSION;

FD_EXPORT u8_string fd_getversion(){return FD_VERSION;}
FD_EXPORT u8_string fd_getrevision(){return FRAMERD_REVISION;}
FD_EXPORT int fd_getmajorversion(){return FD_MAJOR_VERSION;}

/* Initialization procedures */

extern void fd_init_choices_c(void);
extern void fd_init_tables_c(void);

static void register_header_files()
{
  u8_register_source_file(FRAMERD_CONFIG_H_INFO);
  u8_register_source_file(FRAMERD_SUPPORT_H_INFO);
  u8_register_source_file(FRAMERD_MALLOC_H_INFO);
  u8_register_source_file(FRAMERD_COMMON_H_INFO);
  u8_register_source_file(FRAMERD_DEFINES_H_INFO);
  u8_register_source_file(FRAMERD_PTR_H_INFO);
  u8_register_source_file(FRAMERD_CONS_H_INFO);
  u8_register_source_file(FRAMERD_DTYPE_H_INFO);
  u8_register_source_file(FRAMERD_CHOICES_H_INFO);
  u8_register_source_file(FRAMERD_TABLES_H_INFO);
  u8_register_source_file(FRAMERD_DTYPEIO_H_INFO);
}

static void init_type_names()
{
  fd_type_names[fd_oid_type]=_("oid");
  fd_type_names[fd_fixnum_type]=_("fixnum");
  fd_type_names[fd_cons_type]=_("cons");
  fd_type_names[fd_immediate_type]=_("immediate");
  fd_type_names[fd_constant_type]=_("constant");
  fd_type_names[fd_character_type]=_("character");
  fd_type_names[fd_symbol_type]=_("symbol");
  fd_type_names[fd_fcnid_type]=_("fcnid");
  fd_type_names[fd_lexref_type]=_("lexref");
  fd_type_names[fd_opcode_type]=_("opcode");
  fd_type_names[fd_type_type]=_("typeref");
  fd_type_names[fd_coderef_type]=_("cdrcode");
  fd_type_names[fd_pool_type]=_("pool");
  fd_type_names[fd_index_type]=_("index");
  fd_type_names[fd_histref_type]=_("histref");

  fd_type_names[fd_string_type]=_("string");
  fd_type_names[fd_packet_type]=_("packet");
  fd_type_names[fd_secret_type]=_("secret");
  fd_type_names[fd_bigint_type]=_("bigint");
  fd_type_names[fd_pair_type]=_("pair");

  fd_type_names[fd_compound_type]=_("compound");
  fd_type_names[fd_choice_type]=_("choice");
  fd_type_names[fd_prechoice_type]=_("prechoice");
  fd_type_names[fd_qchoice_type]=_("quoted choice");
  fd_type_names[fd_vector_type]=_("vector");
  fd_type_names[fd_matrix_type]=_("matrix");
  fd_type_names[fd_numeric_vector_type]=_("numvec");

  fd_type_names[fd_slotmap_type]=_("slotmap");
  fd_type_names[fd_schemap_type]=_("schemap");
  fd_type_names[fd_hashtable_type]=_("hashtable");
  fd_type_names[fd_hashset_type]=_("hashset");

  fd_type_names[fd_cprim_type]=_("builtin function");
  fd_type_names[fd_lexenv_type]=_("environment");
  fd_type_names[fd_evalfn_type]=_("evalfn");
  fd_type_names[fd_macro_type]=_("macro");
  fd_type_names[fd_dtproc_type]=_("dtproc");
  fd_type_names[fd_stackframe_type]=_("stackframe");
  fd_type_names[fd_tailcall_type]=_("tailcall");
  fd_type_names[fd_lambda_type]=_("lambda procedure");
  fd_type_names[fd_code_type]=_("bytecode");
  fd_type_names[fd_ffi_type]=_("foreign function");
  fd_type_names[fd_exception_type]=_("error");

  fd_type_names[fd_complex_type]=_("complex");
  fd_type_names[fd_rational_type]=_("rational");
  fd_type_names[fd_flonum_type]=_("flonum");

  fd_type_names[fd_timestamp_type]=_("timestamp");
  fd_type_names[fd_uuid_type]=_("UUID");

  fd_type_names[fd_mystery_type]=_("mystery");
  fd_type_names[fd_port_type]=_("ioport");
  fd_type_names[fd_stream_type]=_("stream");

  fd_type_names[fd_regex_type]=_("regex");
  fd_type_names[fd_numeric_vector_type]=_("numeric vector");

  fd_type_names[fd_consblock_type]=_("consblock");

  fd_type_names[fd_rawptr_type]=_("raw pointer");
  fd_type_names[fd_dtserver_type]=_("dtype server");
  fd_type_names[fd_bloom_filter_type]=_("bloom filter");
}

static int lisp_types_version = 101;

FD_EXPORT void fd_init_cons_c(void);
FD_EXPORT void fd_init_compare_c(void);
FD_EXPORT void fd_init_recycle_c(void);
FD_EXPORT void fd_init_copy_c(void);
FD_EXPORT void fd_init_compare_c(void);
FD_EXPORT void fd_init_compounds_c(void);
FD_EXPORT void fd_init_misctypes_c(void);
FD_EXPORT void fd_init_oids_c(void);
FD_EXPORT void fd_init_textio_c(void);
FD_EXPORT void fd_init_parse_c(void);
FD_EXPORT void fd_init_unparse_c(void);
FD_EXPORT void fd_init_pprint_c(void);
FD_EXPORT void fd_init_ports_c(void);
FD_EXPORT void fd_init_dtread_c(void);
FD_EXPORT void fd_init_dtwrite_c(void);
FD_EXPORT void fd_init_tables_c(void);
FD_EXPORT void fd_init_symbols_c(void);
FD_EXPORT void fd_init_numbers_c(void);
FD_EXPORT void fd_init_choices_c(void);
FD_EXPORT void fd_init_support_c(void);
FD_EXPORT void fd_init_consblocks_c(void);
FD_EXPORT void fd_init_sequences_c(void);
FD_EXPORT void fd_init_fcnids_c(void);
FD_EXPORT void fd_init_stacks_c(void);
FD_EXPORT void fd_init_apply_c(void);
FD_EXPORT void fd_init_build_info(void);

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
FD_EXPORT void fd_log_status(u8_condition why)
{
  struct rusage usage;
  int retval = u8_getrusage(0,&usage);
  if (why==NULL) why="Status";
  if (retval<0) {
    u8_log(LOG_CRIT,_("RUSAGE Failed"),
           "During a call to fd_log_status (%s)",why);
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
           FRAMERD_REVISION,u8_appid(),getpid(),
           elapsed,etu,usertime,utu,systime,stu,
           heapsize,heapu);}
}

FD_EXPORT int fd_init_lisp_types()
{
  int u8_version;
#if ((HAVE_LIBDUMA)&&(HAVE_DUMA_H))
  DUMA_SET_ALIGNMENT(4);
#endif
  if (lisp_types_initialized) return lisp_types_initialized;
  fd_load_start = u8_elapsed_time();
  u8_version = u8_initialize();
  lisp_types_initialized = lisp_types_version*u8_version;

  u8_register_source_file(_FILEINFO);

  register_header_files();

  u8_init_rwlock(&fd_symbol_lock);
  fd_init_cons_c();
  init_type_names();
  fd_init_recycle_c();
  fd_init_copy_c();
  fd_init_compare_c();
  fd_init_compounds_c();
  fd_init_misctypes_c();
  fd_init_oids_c();
  fd_init_unparse_c();
  fd_init_pprint_c();
  fd_init_parse_c();
  fd_init_tables_c();
  fd_init_symbols_c();
  fd_init_support_c();
  fd_init_dtread_c();
  fd_init_dtwrite_c();
  fd_init_numbers_c();
  fd_init_choices_c();
  fd_init_stacks_c();
  fd_init_apply_c();
  fd_init_sequences_c();
  fd_init_consblocks_c();
  fd_init_fcnids_c();
  fd_init_build_info();

  u8_threadcheck();

  return lisp_types_initialized;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
