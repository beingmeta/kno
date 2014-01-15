/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/source.h"
#include "framerd/dtype.h"
#include <stdarg.h>
#include <time.h>

static int fdtype_initialized=0;
double fd_load_start=-1.0;

fd_exception fd_NoMethod=_("Method not supported");

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
  fd_type_names[fd_double_type]=_("flonum");
  fd_type_names[fd_wrapper_type]=_("wrapper");
  fd_type_names[fd_mystery_type]=_("mystery");
  fd_type_names[fd_qchoice_type]=_("quoted choice");
  fd_type_names[fd_hashset_type]=_("hashset");
  fd_type_names[fd_function_type]=_("function");
  fd_type_names[fd_error_type]=_("error");
  fd_type_names[fd_complex_type]=_("complex");
  fd_type_names[fd_rational_type]=_("rational");
  fd_type_names[fd_dtproc_type]=_("dtproc");
  fd_type_names[fd_tail_call_type]=_("tailcall");
  fd_type_names[fd_uuid_type]=_("UUID");
  fd_type_names[fd_rail_type]=_("rail");
  fd_type_names[fd_secret_type]=_("secret");
}

static int fdtype_version=101;

FD_EXPORT void fd_init_cons_c(void);
FD_EXPORT void fd_init_oids_c(void);
FD_EXPORT void fd_init_textio_c(void);
FD_EXPORT void fd_init_tables_c(void);
FD_EXPORT void fd_init_symbols_c(void);
FD_EXPORT void fd_init_numbers_c(void);
FD_EXPORT void fd_init_choices_c(void);
FD_EXPORT void fd_init_support_c(void);
FD_EXPORT void fd_init_pptrs_c(void);

FD_EXPORT int fd_init_dtypelib()
{
  int u8_version;
  if (fdtype_initialized) return fdtype_initialized;
  fd_load_start=u8_elapsed_time();
  u8_version=u8_initialize();
  fdtype_initialized=fdtype_version*u8_version;

  u8_register_source_file(_FILEINFO);

  register_header_files();

  fd_init_mutex(&fd_symbol_lock);
  fd_init_cons_c();
  init_type_names();
  fd_init_oids_c();
  fd_init_textio_c();
  fd_init_tables_c();
  fd_init_symbols_c();
  fd_init_numbers_c();
  fd_init_choices_c();
  fd_init_support_c();
  fd_init_pptrs_c();

  u8_threadcheck();

  return fdtype_initialized;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
