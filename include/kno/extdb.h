/* -*- Mode: C;  Character-encoding: utf-8; -*- */

#ifndef KNO_EXTDB_H
#define KNO_EXTDB_H 1
#ifndef KNO_EXTDB_H_INFO
#define KNO_EXTDB_H_INFO "include/kno/extdb.h"
#endif

KNO_EXPORT kno_ptr_type kno_extdb_type;
KNO_EXPORT kno_ptr_type kno_extdb_proc_type;

#define KNO_EXTDB_FIELDS                         \
  KNO_CONS_HEADER;                               \
  u8_string extdb_spec, extdb_info;             \
  lispval extdb_colinfo, extdb_options;         \
  u8_mutex extdb_proclock;                      \
  int extdb_n_procs, extdb_procslen;            \
  struct KNO_EXTDB_PROC **extdb_procs;           \
  struct KNO_EXTDB_HANDLER *extdb_handler;

typedef struct KNO_EXTDB {KNO_EXTDB_FIELDS;} KNO_EXTDB;
typedef struct KNO_EXTDB *kno_extdb;

#define KNO_EXTDB_PROC_FIELDS                    \
  KNO_FUNCTION_FIELDS;                           \
  u8_string extdb_spec, extdb_qtext;            \
  int fcn_n_params;                             \
  lispval extdbptr, extdb_colinfo;              \
  lispval *extdb_paramtypes;                    \
  struct KNO_EXTDB_HANDLER *extdb_handler;

typedef struct KNO_EXTDB_PROC {KNO_EXTDB_PROC_FIELDS;} KNO_EXTDB_PROC;
typedef struct KNO_EXTDB_PROC *kno_extdb_proc;

typedef struct KNO_EXTDB_HANDLER {
  u8_string name;
  lispval (*execute)(struct KNO_EXTDB *,lispval,lispval);
  lispval (*makeproc)(struct KNO_EXTDB *,u8_string,int,lispval,int,lispval *);
  void (*recycle_db)(struct KNO_EXTDB *c);
  void (*recycle_proc)(struct KNO_EXTDB_PROC *c);
  } KNO_EXTDB_HANDLER;


KNO_EXPORT int kno_register_extdb_handler(struct KNO_EXTDB_HANDLER *h);
KNO_EXPORT int kno_register_extdb_proc(struct KNO_EXTDB_PROC *p);
KNO_EXPORT int kno_release_extdb_proc(struct KNO_EXTDB_PROC *p);

#endif /* ndef KNO_EXTDB_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
