/* -*- Mode: C;  Character-encoding: utf-8; -*- */

#ifndef KNO_SQL_H
#define KNO_SQL_H 1
#ifndef KNO_SQL_H_INFO
#define KNO_SQL_H_INFO "include/kno/sql.h"
#endif

#define KNO_SQLCONN_FIELDS                      \
  KNO_CONS_HEADER;                              \
  u8_string sqlconn_spec, sqlconn_info;         \
  lispval sqlconn_options, sqlconn_colinfo;     \
  int sqlconn_bits;				\
  u8_mutex sqlconn_procs_lock;                  \
  int sqlconn_n_procs, sqlconn_procs_len;       \
  struct KNO_SQLPROC **sqlconn_procs;           \
  struct KNO_SQLDB_HANDLER *sqldb_handler;

typedef struct KNO_SQLCONN {KNO_SQLCONN_FIELDS;} KNO_SQLCONN;
typedef struct KNO_SQLCONN *kno_sqlconn;

typedef KNO_SQLCONN KNO_SQLDB;
typedef kno_sqlconn kno_sqldb;

#define SQLCONN_EXEC_OK             0x01

#define KNO_SQLPROC_FIELDS                      \
  KNO_FUNCTION_FIELDS;                          \
  u8_string sqlproc_spec, sqlproc_qtext;        \
  int sqlproc_bits, sqlproc_n_params;		\
  lispval sqlproc_options, sqlproc_colinfo;     \
  lispval *sqlproc_paramtypes;                  \
  lispval sqlproc_conn;				\
  struct KNO_SQLDB_HANDLER *sqldb_handler;

typedef struct KNO_SQLPROC {KNO_SQLPROC_FIELDS;} KNO_SQLPROC;
typedef struct KNO_SQLPROC *kno_sqlproc;

typedef struct KNO_SQLDB_HANDLER {
  u8_string name;
  struct KNO_SQLCONN (*open_db)(u8_string,lispval);
  int (*close_db)(struct KNO_SQLCONN *);
  lispval (*execute)(struct KNO_SQLCONN *,lispval,lispval);
  lispval (*makeproc)(struct KNO_SQLCONN *,u8_string,lispval,lispval,int,kno_argvec);
  void (*recycle_db)(struct KNO_SQLCONN *c);
  void (*recycle_proc)(struct KNO_SQLPROC *c);
  int (*dbctl)(struct KNO_SQLCONN *c,int n,kno_argvec);
  int (*probe_db)(u8_string,lispval);
  } KNO_SQLDB_HANDLER;

typedef lispval (*kno_sqlconn_exec)(struct KNO_SQLCONN *,lispval,lispval);
typedef void (*kno_sqlconn_recycler)(struct KNO_SQLCONN *);
typedef void (*kno_sqlproc_recycler)(struct KNO_SQLPROC *);

KNO_EXPORT lispval KNOSYM_COLINFO;

KNO_EXPORT int kno_register_sqldb_handler(struct KNO_SQLDB_HANDLER *h);
KNO_EXPORT int kno_register_sqlproc(struct KNO_SQLPROC *p);
KNO_EXPORT int kno_release_sqlproc(struct KNO_SQLPROC *p);
KNO_EXPORT int kno_recycle_sqlproc(struct KNO_SQLPROC *proc);

KNO_EXPORT void kno_cleanup_sqlconn(struct KNO_SQLCONN *dbp);
KNO_EXPORT void kno_cleanup_sqlprocs(struct KNO_SQLCONN *dbp);

#define kno_sqldb_type kno_sqlconn_type
#define KNO_SQLDB_FIELDS KNO_SQLCONN_FIELDS

#endif /* ndef KNO_SQL_H */
