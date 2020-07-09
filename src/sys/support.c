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
#include "kno/numbers.h"
#include "kno/apply.h"

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
#if KNO_FILECONFIG_ENABLED
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

u8_condition kno_UnknownError=_("Unknown error condition");
u8_condition kno_OutOfMemory=_("Memory apparently exhausted");
u8_condition kno_FileNotFound=_("File not found");
u8_condition kno_NoSuchFile=_("File does not exist");

/* Req logging */

/* Debugging support functions */

KNO_EXPORT kno_lisp_type _kno_typeof(lispval x)
{
  return KNO_TYPEOF(x);
}

KNO_EXPORT lispval _kno_debug(lispval x)
{
  return x;
}

/* Getting module locations */

#define LOCAL_MODULES 1
#define INSTALLED_MODULES 2
/* #define SHARED_MODULES 3 */
#define STDLIB_MODULES 4
#define UNPACKAGE_DIR 5

static lispval config_get_module_loc(lispval var,void *which_arg)
{
#if (SIZEOF_LONG_LONG == SIZEOF_VOID_P)
  long long which = (long long) which_arg;
#else
  int which = (int) which_arg;
#endif
  switch (which) {
  case LOCAL_MODULES:
    return kno_mkstring(KNO_LOCAL_MODULE_DIR);
  case INSTALLED_MODULES:
    return kno_mkstring(KNO_INSTALLED_MODULE_DIR);
  case STDLIB_MODULES:
    return kno_mkstring(KNO_STDLIB_MODULE_DIR);
  case UNPACKAGE_DIR:
    return kno_mkstring(KNO_UNPACKAGE_DIR);
  default:
    return kno_err("Bad call","config_get_module_loc",NULL,VOID);}
}

/* Resource sensors */

static struct RESOURCE_SENSOR {
  lispval name;
  kno_resource_sensor sensor;
  struct RESOURCE_SENSOR *next;} *resource_sensors;
static u8_mutex resource_sensor_lock;

KNO_EXPORT int kno_add_sensor(lispval name,kno_resource_sensor fn)
{
  u8_lock_mutex(&resource_sensor_lock);
  struct RESOURCE_SENSOR *scan=resource_sensors;
  while (scan) {
    if (scan->name==name) {
      if (fn == scan->sensor) {
        u8_unlock_mutex(&resource_sensor_lock);
        return 0;}
      else {
        scan->sensor=fn;
        u8_unlock_mutex(&resource_sensor_lock);
        return 1;}}
    else scan=scan->next;}
  struct RESOURCE_SENSOR *new=u8_alloc(struct RESOURCE_SENSOR);
  new->name=name; new->sensor=fn;
  new->next=resource_sensors;
  resource_sensors=new;
  u8_unlock_mutex(&resource_sensor_lock);
  return 2;
}
KNO_EXPORT lispval kno_read_sensor(lispval name)
{
  struct RESOURCE_SENSOR *scan=resource_sensors;
  while (scan) {
    if (scan->name==name)
      return scan->sensor();
    else scan=scan->next;}
  return KNO_VOID;
}
KNO_EXPORT lispval kno_read_sensors(lispval into)
{
  if ( (KNO_PAIRP(into)) || (!(KNO_TABLEP(into))) )
    return kno_type_error("table","kno_read_sensors",into);
  struct RESOURCE_SENSOR *scan=resource_sensors;
  while (scan) {
    lispval v=scan->sensor();
    if (KNO_ABORTP(v)) {
      u8_exception ex=u8_erreify();
      u8_log(LOG_WARN,"SensorFailed",
             "Resource sensor %q failed",scan->name);
      kno_log_exception(ex);
      u8_free_exception(ex,1);}
    else {
      kno_store(into,scan->name,v);
      kno_decref(v);}
    scan=scan->next;}
  return into;
}

/* Initialization */

void kno_init_config_c(void);
void kno_init_errobjs_c(void);
void kno_init_logging_c(void);
void kno_init_startup_c(void);
void kno_init_getopt_c(void);
void kno_init_fluid_c(void);
void kno_init_posix_c(void);
void kno_init_signals_c(void);
void kno_init_sourcebase_c(void);
void kno_init_history_c(void);

KNO_EXPORT void kno_init_support_c()
{

  u8_register_textdomain("kno");

  kno_init_lisp_types();

  kno_init_sourcebase_c();
  kno_init_config_c();
  kno_init_errobjs_c();
  kno_init_logging_c();
  kno_init_startup_c();
  kno_init_getopt_c();
  kno_init_fluid_c();
  kno_init_posix_c();
  kno_init_signals_c();
  kno_init_history_c();

  kno_register_config
    ("MAXCHARS",_("Max number of chars to show in strings"),
     kno_intboolconfig_get,kno_intboolconfig_set,
     &kno_unparse_maxchars);
  kno_register_config
    ("MAXELTS",
     _("Max number of elements to show in vectors/lists/choices, etc"),
     kno_intboolconfig_get,kno_intboolconfig_set,
     &kno_unparse_maxelts);
  kno_register_config
    ("NUMVEC:SHOWMAX",_("Max number of elements to show in numeric vectors"),
     kno_intboolconfig_get,kno_intboolconfig_set,
     &kno_numvec_showmax);
  kno_register_config
    ("PACKETFMT",
     _("How to dump packets to ASCII (16 = hex,64 = base64,dflt = ascii-ish)"),
     kno_intconfig_get,kno_intconfig_set,
     &kno_packet_outfmt);
  kno_register_config
    ("OIDS:NUMERIC",_("Whether to use custom formatting for OIDs"),
     kno_boolconfig_get,kno_boolconfig_set,
     &kno_packet_outfmt);

  kno_register_config("PPRINT:MAXCHARS",
                      _("Maximum number of characters when pprinting strings"),
                      kno_intconfig_get,kno_intconfig_set,
                      &pprint_maxchars);
  kno_register_config("PPRINT:MAXBYTE",
                      _("Maximum number of bytes when pprinting packets"),
                      kno_intconfig_get,kno_intconfig_set,
                      &pprint_maxbytes);
  kno_register_config("PPRINT:MAXELTS",
                      _("Maximum number of elements when pprinting sequences "
                        "or choices"),
                      kno_intconfig_get,kno_intconfig_set,
                      &pprint_maxelts);
  kno_register_config("PPRINT:MAXDEPTH",
                      _("Maximum depth for recursive printing"),
                      kno_intconfig_get,kno_intconfig_set,
                      &pprint_maxdepth);
  kno_register_config("PPRINT:LISTMAX",
                      _("Maximum number of elements when pprinting lists"),
                      kno_intconfig_get,kno_intconfig_set,
                      &pprint_list_max);
  kno_register_config("PPRINT:VECTORMAX",
                      _("Maximum number of elements when pprinting vectors"),
                      kno_intconfig_get,kno_intconfig_set,
                      &pprint_vector_max);
  kno_register_config("PPRINT:CHOICEMAX",
                      _("Maximum number of elements when pprinting choices"),
                      kno_intconfig_get,kno_intconfig_set,
                      &pprint_choice_max);
  kno_register_config("PPRINT:KEYSMAX",
                      _("Maximum number of keys when pprinting tables"),
                      kno_intconfig_get,kno_intconfig_set,
                      &pprint_choice_max);

  kno_register_config("PPRINT:INDENTS",
                      _("PPRINT indentation rules"),
                      kno_tblconfig_get,kno_tblconfig_set,
                      &pprint_default_rules);

  kno_register_config("LOCAL_MODULES",_("value of LOCAL_MODULES"),
                      config_get_module_loc,NULL,(void *) LOCAL_MODULES);
  kno_register_config("INSTALLED_MODULES",_("value of INSTALLED_MODULES"),
                      config_get_module_loc,NULL,(void *) INSTALLED_MODULES);
#if 0
  kno_register_config("SHARED_MODULES",_("value of SHARED_MODULES"),
                      config_get_module_loc,NULL,(void *) SHARED_MODULES);
#endif
  kno_register_config("STDLIB_MODULES",_("value of STDLIB_MODULES"),
                      config_get_module_loc,NULL,(void *) STDLIB_MODULES);

  kno_register_config("UNPACKAGE_DIR",_("value of UNPACKAGE_DIR"),
                      config_get_module_loc,NULL,(void *) UNPACKAGE_DIR);
}

