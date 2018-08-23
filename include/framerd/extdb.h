/* -*- Mode: C;  Character-encoding: utf-8; -*- */

#ifndef FRAMERD_EXTDB_H
#define FRAMERD_EXTDB_H 1
#ifndef FRAMERD_EXTDB_H_INFO
#define FRAMERD_EXTDB_H_INFO "include/framerd/extdb.h"
#endif

FD_EXPORT fd_ptr_type fd_extdb_type;
FD_EXPORT fd_ptr_type fd_extdb_proc_type;

#define FD_EXTDB_FIELDS                         \
  FD_CONS_HEADER;                               \
  u8_string extdb_spec, extdb_info;             \
  lispval extdb_colinfo, extdb_options;         \
  u8_mutex extdb_proclock;                      \
  int extdb_n_procs, extdb_procslen;            \
  struct FD_EXTDB_PROC **extdb_procs;           \
  struct FD_EXTDB_HANDLER *extdb_handler;

typedef struct FD_EXTDB {FD_EXTDB_FIELDS;} FD_EXTDB;
typedef struct FD_EXTDB *fd_extdb;

#define FD_EXTDB_PROC_FIELDS                    \
  FD_FUNCTION_FIELDS;                           \
  u8_string extdb_spec, extdb_qtext;            \
  int fcn_n_params;                             \
  lispval extdbptr, extdb_colinfo;              \
  lispval *extdb_paramtypes;                    \
  struct FD_EXTDB_HANDLER *extdb_handler;

typedef struct FD_EXTDB_PROC {FD_EXTDB_PROC_FIELDS;} FD_EXTDB_PROC;
typedef struct FD_EXTDB_PROC *fd_extdb_proc;

typedef struct FD_EXTDB_HANDLER {
  u8_string name;
  lispval (*execute)(struct FD_EXTDB *,lispval,lispval);
  lispval (*makeproc)(struct FD_EXTDB *,u8_string,int,lispval,int,lispval *);
  void (*recycle_db)(struct FD_EXTDB *c);
  void (*recycle_proc)(struct FD_EXTDB_PROC *c);
  } FD_EXTDB_HANDLER;


FD_EXPORT int fd_register_extdb_handler(struct FD_EXTDB_HANDLER *h);
FD_EXPORT int fd_register_extdb_proc(struct FD_EXTDB_PROC *p);
FD_EXPORT int fd_release_extdb_proc(struct FD_EXTDB_PROC *p);

#endif /* ndef FRAMERD_EXTDB_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
