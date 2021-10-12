/* -*- Mode: C;  Character-encoding: utf-8; -*- */

#ifndef KNO_SQLDB_H
#define KNO_SQLDB_H 1
#ifndef KNO_SQLDB_H_INFO
#define KNO_SQLDB_H_INFO "include/kno/sqldb.h"
#endif

#define KNO_SQLCONN_FIELDS                      \
  KNO_CONS_HEADER;                              \
  u8_string sqlconn_spec, sqlconn_info;         \
  lispval sqlconn_colinfo, sqlconn_options;     \
  u8_mutex sqlconn_procs_lock;                  \
  int sqlconn_n_procs, sqlconn_procs_len;       \
  struct KNO_SQLPROC **sqlconn_procs;           \
  struct KNO_SQLDB_HANDLER *sqldb_handler;

typedef struct KNO_SQLDB {KNO_SQLCONN_FIELDS;} KNO_SQLDB;
typedef struct KNO_SQLDB *kno_sqldb;

#define KNO_SQLPROC_FIELDS                      \
  KNO_FUNCTION_FIELDS;                          \
  u8_string sqlproc_spec, sqlproc_qtext;        \
  int sqlproc_n_params;                         \
  lispval sqlproc_conn, sqlproc_colinfo;        \
  lispval *sqlproc_paramtypes;                  \
  struct KNO_SQLDB_HANDLER *sqldb_handler;

typedef struct KNO_SQLPROC {KNO_SQLPROC_FIELDS;} KNO_SQLPROC;
typedef struct KNO_SQLPROC *kno_sqlproc;

typedef struct KNO_SQLDB_HANDLER {
  u8_string name;
  struct KNO_SQLDB (*open_db)(lispval);
  int (*close_db)(struct KNO_SQLDB *);
  lispval (*execute)(struct KNO_SQLDB *,lispval,lispval);
  lispval (*makeproc)(struct KNO_SQLDB *,u8_string,int,lispval,int,kno_argvec);
  void (*recycle_db)(struct KNO_SQLDB *c);
  void (*recycle_proc)(struct KNO_SQLPROC *c);
  } KNO_SQLDB_HANDLER;

KNO_EXPORT int kno_register_sqldb_handler(struct KNO_SQLDB_HANDLER *h);
KNO_EXPORT int kno_register_sqlproc(struct KNO_SQLPROC *p);
KNO_EXPORT int kno_release_sqlproc(struct KNO_SQLPROC *p);
KNO_EXPORT int kno_recycle_sqlproc(struct KNO_SQLPROC *proc);

#define kno_sqldb_type kno_sqlconn_type
#define KNO_SQLDB_FIELDS KNO_SQLCONN_FIELDS

#endif /* ndef KNO_SQLDB_H */
