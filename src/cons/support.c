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
#include "framerd/numbers.h"
#include "framerd/apply.h"

#include <libu8/u8signals.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8logging.h>

#include <signal.h>
#include <sys/types.h>
#include <pwd.h>

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>
#if FD_FILECONFIG_ENABLED
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#endif

#include <sys/time.h>

#if HAVE_SYS_RESOURCE_H
#include <sys/resource.h>
#endif

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#if HAVE_GRP_H
#include <grp.h>
#endif

int fd_exiting=0;

fd_exception fd_UnknownError=_("Unknown error condition");

fd_exception fd_OutOfMemory=_("Memory apparently exhausted");

/* Req logging */

/* Debugging support functions */

FD_EXPORT fd_ptr_type _fd_ptr_type(fdtype x)
{
  return FD_PTR_TYPE(x);
}

FD_EXPORT fdtype _fd_debug(fdtype x)
{
  return x;
}

/* Getting module locations */

#define LOCAL_MODULES 1
#define LOCAL_SAFE_MODULES 2
#define INSTALLED_MODULES 3
#define INSTALLED_SAFE_MODULES 4
#define SHARED_MODULES 5
#define SHARED_SAFE_MODULES 6
#define BUILTIN_MODULES 7
#define BUILTIN_SAFE_MODULES 8
#define UNPACKAGE_DIR 9

static fdtype config_get_module_loc(fdtype var,void *which_arg)
{
#if (SIZEOF_LONG_LONG == SIZEOF_VOID_P)
  long long which = (long long) which_arg;
#else
  int which = (int) which_arg;
#endif
  switch (which) {
  case LOCAL_MODULES:
    return fdtype_string(FD_LOCAL_MODULE_DIR);
  case LOCAL_SAFE_MODULES:
    return fdtype_string(FD_LOCAL_SAFE_MODULE_DIR);
  case INSTALLED_MODULES:
    return fdtype_string(FD_INSTALLED_MODULE_DIR);
  case INSTALLED_SAFE_MODULES:
    return fdtype_string(FD_INSTALLED_SAFE_MODULE_DIR);
  case SHARED_MODULES:
    return fdtype_string(FD_SHARED_MODULE_DIR);
  case SHARED_SAFE_MODULES:
    return fdtype_string(FD_SHARED_SAFE_MODULE_DIR);
  case BUILTIN_MODULES:
    return fdtype_string(FD_BUILTIN_MODULE_DIR);
  case BUILTIN_SAFE_MODULES:
    return fdtype_string(FD_BUILTIN_SAFE_MODULE_DIR);
  case UNPACKAGE_DIR:
    return fdtype_string(FD_UNPACKAGE_DIR);
  default:
    return fd_err("Bad call","config_get_module_loc",NULL,FD_VOID);}
}

/* Initialization */

void fd_init_config_c(void);
void fd_init_err_c(void);
void fd_init_logging_c(void);
void fd_init_startup_c(void);
void fd_init_getopt_c(void);
void fd_init_fluid_c(void);

void fd_init_support_c()
{

  u8_register_textdomain("FramerD");

  fd_init_config_c();

  fd_init_err_c();

  fd_init_logging_c();
  fd_init_startup_c();
  fd_init_getopt_c();
  fd_init_fluid_c();

  fd_register_config
    ("MERGECHOICES",
     _("Threshold at which to use an external hashset for merging"),
     fd_intconfig_get,fd_intconfig_set,
     &fd_mergesort_threshold);

  fd_register_config
    ("MAXCHARS",_("Max number of chars to show in strings"),
     fd_intconfig_get,fd_intconfig_set,
     &fd_unparse_maxchars);
  fd_register_config
    ("MAXELTS",
     _("Max number of elements to show in vectors/lists/choices, etc"),
     fd_intconfig_get,fd_intconfig_set,
     &fd_unparse_maxelts);
  fd_register_config
    ("NUMVEC:SHOWMAX",_("Max number of elements to show in numeric vectors"),
     fd_intconfig_get,fd_intconfig_set,
     &fd_numvec_showmax);
  fd_register_config
    ("PACKETFMT",
     _("How to dump packets to ASCII (16=hex,64=base64,dflt=ascii-ish)"),
     fd_intconfig_get,fd_intconfig_set,
     &fd_packet_outfmt);

  fd_register_config("LOCAL_MODULES",_("value of LOCAL_MODULES"),
                     config_get_module_loc,NULL,(void *) LOCAL_MODULES);
  fd_register_config("LOCAL_SAFE_MODULES",_("value of LOCAL_SAFE_MODULES"),
                     config_get_module_loc,NULL,(void *) LOCAL_SAFE_MODULES);
  fd_register_config("INSTALLED_MODULES",_("value of INSTALLED_MODULES"),
                     config_get_module_loc,NULL,(void *) INSTALLED_MODULES);
  fd_register_config("INSTALLED_SAFE_MODULES",_("value of INSTALLED_SAFE_MODULES"),
                     config_get_module_loc,NULL,(void *) INSTALLED_SAFE_MODULES);
  fd_register_config("SHARED_MODULES",_("value of SHARED_MODULES"),
                     config_get_module_loc,NULL,(void *) SHARED_MODULES);
  fd_register_config("SHARED_SAFE_MODULES",_("value of SHARED_SAFE_MODULES"),
                     config_get_module_loc,NULL,(void *) SHARED_SAFE_MODULES);
  fd_register_config("BUILTIN_MODULES",_("value of BUILTIN_MODULES"),
                     config_get_module_loc,NULL,(void *) BUILTIN_MODULES);
  fd_register_config("BUILTIN_SAFE_MODULES",_("value of BUILTIN_SAFE_MODULES"),
                     config_get_module_loc,NULL,(void *) BUILTIN_SAFE_MODULES);

  fd_register_config("UNPACKAGE_DIR",_("value of UNPACKAGE_DIR"),
                     config_get_module_loc,NULL,(void *) UNPACKAGE_DIR);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
