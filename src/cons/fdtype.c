/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include <libu8/u8rusage.h>
#include <stdarg.h>
#include <time.h>
#include <math.h>

#if ((HAVE_LIBDUMA)&&(HAVE_DUMA_H))
#include <duma.h>
#endif

static int fdtype_initialized=0;
double fd_load_start=-1.0;

fd_exception fd_NoMethod=_("Method not supported");

u8_string fd_version=FD_VERSION;
u8_string fd_revision=FRAMERD_REVISION;
int fd_major_version=FD_MAJOR_VERSION;
int fd_minor_version=FD_MINOR_VERSION;
int fd_release_version=FD_RELEASE_VERSION;

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
  fd_type_names[fd_lexref_type]=_("lexref");
  fd_type_names[fd_opcode_type]=_("opcode");
  fd_type_names[fd_cdrcode_type]=_("cdrcode");
  fd_type_names[fd_string_type]=_("string");
  fd_type_names[fd_packet_type]=_("packet");
  fd_type_names[fd_secret_type]=_("secret");
  fd_type_names[fd_bigint_type]=_("bigint");
  fd_type_names[fd_pair_type]=_("pair");
  fd_type_names[fd_compound_type]=_("compound");
  fd_type_names[fd_choice_type]=_("choice");
  fd_type_names[fd_achoice_type]=_("achoice");
  fd_type_names[fd_vector_type]=_("vector");
  fd_type_names[fd_slotmap_type]=_("slotmap");
  fd_type_names[fd_schemap_type]=_("schemap");
  fd_type_names[fd_hashtable_type]=_("hashtable");
  fd_type_names[fd_flonum_type]=_("flonum");
  fd_type_names[fd_wrapper_type]=_("wrapper");
  fd_type_names[fd_mystery_type]=_("mystery");
  fd_type_names[fd_qchoice_type]=_("quoted choice");
  fd_type_names[fd_hashset_type]=_("hashset");
  fd_type_names[fd_primfcn_type]=_("function");
  fd_type_names[fd_error_type]=_("error");
  fd_type_names[fd_complex_type]=_("complex");
  fd_type_names[fd_rational_type]=_("rational");
  fd_type_names[fd_dtproc_type]=_("dtproc");
  fd_type_names[fd_tailcall_type]=_("tailcall");
  fd_type_names[fd_uuid_type]=_("UUID");
  fd_type_names[fd_rail_type]=_("rail");
  fd_type_names[fd_secret_type]=_("secret");
  fd_type_names[fd_sproc_type]=_("SCHEME procedure");
  fd_type_names[fd_ffi_type]=_("foreign function");
  fd_type_names[fd_regex_type]=_("regex");
  fd_type_names[fd_numeric_vector_type]=_("numeric vector");
  fd_type_names[fd_consblock_type]=_("consblock");
}

static int fdtype_version=101;

FD_EXPORT void fd_init_cons_c(void);
FD_EXPORT void fd_init_compare_c(void);
FD_EXPORT void fd_init_oids_c(void);
FD_EXPORT void fd_init_textio_c(void);
FD_EXPORT void fd_init_tables_c(void);
FD_EXPORT void fd_init_symbols_c(void);
FD_EXPORT void fd_init_numbers_c(void);
FD_EXPORT void fd_init_choices_c(void);
FD_EXPORT void fd_init_support_c(void);
FD_EXPORT void fd_init_ffi_c(void);
FD_EXPORT void fd_init_fcnids_c(void);
FD_EXPORT void fd_init_stream_c(void);
FD_EXPORT void fd_init_hashdtype_c(void);
FD_EXPORT void fd_init_apply_c(void);

FD_EXPORT void fd_boot_message()
{
  u8_message("Copyright (C) beingmeta 2004-2017, all rights reserved");
  u8_message("(%s:%lld) %s %s",
             u8_appid(),(unsigned long long)getpid(),
             fd_getrevision(),u8_getrevision());
}

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
FD_EXPORT void fd_status_message()
{
  struct rusage usage;
  int retval=u8_getrusage(0,&usage);
  if (retval<0) {
    u8_log(LOGCRIT,_("RUSAGE Failed"),
           "During a call to fd_status_message");
    return;}
  else {
    /* long membytes=(usage.ru_idrss+usage.ru_isrss); double memsize; */
    ssize_t heapbytes=u8_memusage(); double heapsize;
    char *stu="s", *utu="s", *etu="s", *heapu="KB";
    double elapsed=format_secs(u8_elapsed_time(),&etu);
    double usertime=format_secs
      (usage.ru_utime.tv_sec+(((double)usage.ru_utime.tv_usec)/1000000),
       &utu);
    double systime=format_secs
      (usage.ru_stime.tv_sec+(((double)usage.ru_stime.tv_usec)/1000000),
       &stu);
    if (heapbytes>10000000000) {
      heapsize=floor(((double)heapbytes)/1000000000); heapu="GB";}
    else if (heapbytes>1500000) {
      heapsize=floor(((double)heapbytes)/1000000); heapu="MB";}
    else {heapsize=floor(((double)heapbytes)/1000); heapu="KB";}
    u8_message
      ("%s %s<%ld> elapsed %.3f%s (u=%.3f%s,s=%.3f%s), heap=%.0f%s",
       FRAMERD_REVISION,u8_appid(),getpid(),
       elapsed,etu,usertime,utu,systime,stu,
       heapsize,heapu);}
}

FD_EXPORT int fd_init_dtypelib()
{
  int u8_version;
#if ((HAVE_LIBDUMA)&&(HAVE_DUMA_H))
  DUMA_SET_ALIGNMENT(4);
#endif
  if (fdtype_initialized) return fdtype_initialized;
  fd_load_start=u8_elapsed_time();
  u8_version=u8_initialize();
  fdtype_initialized=fdtype_version*u8_version;

  u8_register_source_file(_FILEINFO);

  register_header_files();

  fd_init_mutex(&fd_symbol_lock);
  fd_init_cons_c();
  init_type_names();
  fd_init_compare_c();
  fd_init_oids_c();
  fd_init_textio_c();
  fd_init_tables_c();
  fd_init_symbols_c();
  fd_init_numbers_c();
  fd_init_choices_c();
  fd_init_support_c();
  fd_init_apply_c();
  fd_init_ffi_c();
  fd_init_fcnids_c();

  u8_threadcheck();

  return fdtype_initialized;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
