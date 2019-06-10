/* -*- Mode: C;  Character-encoding: utf-8; -*- */

#ifndef KNO_SQLDB_H
#define KNO_SQLDB_H 1
#ifndef KNO_SQLDB_H_INFO
#define KNO_SQLDB_H_INFO "include/kno/sqldb.h"
#endif

KNO_EXPORT kno_ptr_type kno_sqldb_type;
KNO_EXPORT kno_ptr_type kno_sqldb_proc_type;

#define KNO_SQLDB_FIELDS                         \
  KNO_CONS_HEADER;                               \
  u8_string sqldb_spec, sqldb_info;             \
  lispval sqldb_colinfo, sqldb_options;         \
  u8_mutex sqldb_proclock;                      \
  int sqldb_n_procs, sqldb_procslen;            \
  struct KNO_SQLDB_PROC **sqldb_procs;           \
  struct KNO_SQLDB_HANDLER *sqldb_handler;

typedef struct KNO_SQLDB {KNO_SQLDB_FIELDS;} KNO_SQLDB;
typedef struct KNO_SQLDB *kno_sqldb;

#define KNO_SQLDB_PROC_FIELDS                    \
  KNO_FUNCTION_FIELDS;                           \
  u8_string sqldb_spec, sqldb_qtext;            \
  int fcn_n_params;                             \
  lispval sqldbptr, sqldb_colinfo;              \
  lispval *sqldb_paramtypes;                    \
  struct KNO_SQLDB_HANDLER *sqldb_handler;

typedef struct KNO_SQLDB_PROC {KNO_SQLDB_PROC_FIELDS;} KNO_SQLDB_PROC;
typedef struct KNO_SQLDB_PROC *kno_sqldb_proc;

typedef struct KNO_SQLDB_HANDLER {
  u8_string name;
  lispval (*execute)(struct KNO_SQLDB *,lispval,lispval);
  lispval (*makeproc)(struct KNO_SQLDB *,u8_string,int,lispval,int,lispval *);
  void (*recycle_db)(struct KNO_SQLDB *c);
  void (*recycle_proc)(struct KNO_SQLDB_PROC *c);
  } KNO_SQLDB_HANDLER;


KNO_EXPORT int kno_register_sqldb_handler(struct KNO_SQLDB_HANDLER *h);
KNO_EXPORT int kno_register_sqldb_proc(struct KNO_SQLDB_PROC *p);
KNO_EXPORT int kno_release_sqldb_proc(struct KNO_SQLDB_PROC *p);

#endif /* ndef KNO_SQLDB_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
