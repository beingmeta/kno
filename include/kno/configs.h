/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_CONFIGS_H
#define KNO_CONFIGS_H 1
#ifndef KNO_CONFIGS_H_INFO
#define KNO_CONFIGS_H_INFO "include/kno/configs.h"
#endif

KNO_EXPORT u8_condition kno_ConfigError, kno_ReadOnlyConfig;

KNO_EXPORT int kno_trace_config, kno_trace_config_load;

#define KNO_CONFIG_MODIFIED	    0x01
#define KNO_CONFIG_SINGLE_VALUE	    0x02
#define KNO_CONFIG_DELAYED	    0x04
#define KNO_CONFIG_LOCKED	    0x08

typedef lispval (*kno_config_getfn)(lispval var,void *data);
typedef int (*kno_config_setfn)(lispval var,lispval val,void *data);

typedef struct KNO_CONFIG_HANDLER {
  lispval configname;
  void *configdata;
  int configflags;
  u8_string configdoc;
  kno_config_getfn config_get_method;
  kno_config_setfn config_set_method;
  struct KNO_CONFIG_HANDLER *config_next;} KNO_CONFIG_HANDLER;
typedef struct KNO_CONFIG_HANDLER *kno_config_handler;

typedef struct KNO_CONFIG_FINDER {
  lispval (*config_lookup)(lispval var,void *data);
  void *config_lookup_data;
  struct KNO_CONFIG_FINDER *next_lookup;} KNO_CONFIG_FINDER;
typedef struct KNO_CONFIG_FINDER *kno_config_finders;

KNO_EXPORT lispval kno_config_get(u8_string var);
KNO_EXPORT int kno_set_config(u8_string var,lispval val);
KNO_EXPORT int kno_handle_config(lispval symbol,lispval val);
KNO_EXPORT int kno_set_default_config(u8_string var,lispval val);
KNO_EXPORT int kno_handle_default_config(lispval symbol,lispval val);
KNO_EXPORT int kno_set_config_consed(u8_string var,lispval val);
#define kno_config_set(var,val) kno_set_config(var,val)
#define kno_config_set_consed(var,val) kno_set_config_consed(var,val)
#define kno_config_default(var,val) kno_set_default_config(var,val)

KNO_EXPORT int kno_configs_initialized;
KNO_EXPORT int kno_init_configs(void);

KNO_EXPORT int kno_readonly_config_set(lispval ignored,lispval v,void *p);

KNO_EXPORT lispval kno_interpret_config(lispval value_expr);

KNO_EXPORT int kno_lconfig_push(lispval,lispval v,void *lispp);
KNO_EXPORT int kno_lconfig_add(lispval,lispval v,void *lispp);
KNO_EXPORT int kno_lconfig_set(lispval,lispval v,void *lispp);
KNO_EXPORT lispval kno_lconfig_get(lispval,void *lispp);
KNO_EXPORT int kno_sconfig_set(lispval,lispval v,void *stringptr);
KNO_EXPORT lispval kno_sconfig_get(lispval,void *stringptr);
KNO_EXPORT int kno_intconfig_set(lispval,lispval v,void *intptr);
KNO_EXPORT lispval kno_longconfig_get(lispval,void *intptr);
KNO_EXPORT int kno_longconfig_set(lispval,lispval v,void *intptr);
KNO_EXPORT lispval kno_intconfig_get(lispval,void *intptr);
KNO_EXPORT int kno_sizeconfig_set(lispval,lispval v,void *intptr);
KNO_EXPORT lispval kno_sizeconfig_get(lispval,void *intptr);
KNO_EXPORT int kno_intboolconfig_set(lispval,lispval v,void *intptr);
KNO_EXPORT lispval kno_intboolconfig_get(lispval,void *intptr);
KNO_EXPORT int kno_boolconfig_set(lispval,lispval v,void *intptr);
KNO_EXPORT lispval kno_boolconfig_get(lispval,void *intptr);
KNO_EXPORT int kno_dblconfig_set(lispval,lispval v,void *dblptr);
KNO_EXPORT lispval kno_dblconfig_get(lispval,void *dblptr);
KNO_EXPORT int kno_loglevelconfig_set(lispval var,lispval val,void *data);

KNO_EXPORT int kno_symconfig_set(lispval,lispval v,void *stringptr);

KNO_EXPORT int kno_realpath_config_set(lispval,lispval v,void *stringptr);
KNO_EXPORT int kno_realdir_config_set(lispval,lispval v,void *stringptr);

KNO_EXPORT int kno_tblconfig_set(lispval var,lispval config_val,void *tblptr);
KNO_EXPORT lispval kno_tblconfig_get(lispval var,void *tblptr);

KNO_EXPORT int kno_config_rlimit_set(lispval ignored,lispval v,void *vptr);
KNO_EXPORT lispval kno_config_rlimit_get(lispval ignored,void *vptr);

KNO_EXPORT int kno_config_assignment(u8_string assign_expr);
KNO_EXPORT int kno_default_config_assignment(u8_string assign_expr);
KNO_EXPORT int kno_read_config(u8_input in);
KNO_EXPORT int kno_read_default_config(u8_input in);

KNO_EXPORT
void kno_register_config_lookup(lispval (*fn)(lispval,void *),void *);

KNO_EXPORT int kno_register_config
(u8_string var,u8_string doc,
   lispval (*getfn)(lispval,void *),
   int (*setfn)(lispval,lispval,void *),
   void *data);
KNO_EXPORT int kno_register_config_x
(u8_string var,u8_string doc,
 lispval (*getfn)(lispval,void *),
 int (*setfn)(lispval,lispval,void *),
 void *data,int flags,
 int (*reuse)(struct KNO_CONFIG_HANDLER *scan));
KNO_EXPORT lispval kno_all_configs(int with_docs);

KNO_EXPORT void kno_config_lock(int lock);

KNO_EXPORT int kno_boolstring(u8_string,int);

typedef struct KNO_CONFIG_RECORD {
  u8_string config_filename;
  struct KNO_CONFIG_RECORD *loaded_after;} KNO_CONFIG_RECORD;

KNO_EXPORT int kno_load_config(u8_string sourceid);
KNO_EXPORT int kno_load_default_config(u8_string sourceid);

#endif /* #ifndef KNO_CONFIGS_H */

