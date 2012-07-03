/* C Mode */

#ifndef FDB_EXTDB_H
#define FDB_EXTDB_H 1
#ifndef FDB_EXTDB_H_INFO
#define FDB_EXTDB_H_INFO "include/framerd/extdb.h"
#endif

FD_EXPORT fd_ptr_type fd_extdb_type;
FD_EXPORT fd_ptr_type fd_extdb_proc_type;

#define FD_EXTDB_FIELDS   \
  FD_CONS_HEADER;         \
  u8_string spec, info;   \
  fdtype colinfo;         \
  u8_mutex proclock;      \
  int n_procs, max_procs; \
  struct FD_EXTDB_PROC **procs; \
  struct FD_EXTDB_HANDLER *dbhandler;

typedef struct FD_EXTDB {FD_EXTDB_FIELDS;} FD_EXTDB;
typedef struct FD_EXTDB *fd_extdb;

#define FD_EXTDB_PROC_FIELDS        \
  FD_FUNCTION_FIELDS;               \
  u8_string spec, qtext;            \
  int n_params;                     \
  fdtype db, colinfo;               \
  fdtype *paramtypes;               \
  struct FD_EXTDB_HANDLER *dbhandler;

typedef struct FD_EXTDB_PROC {FD_EXTDB_PROC_FIELDS;} FD_EXTDB_PROC;
typedef struct FD_EXTDB_PROC *fd_extdb_proc;

typedef struct FD_EXTDB_HANDLER {
  u8_string name;
  fdtype (*execute)(struct FD_EXTDB *,fdtype,fdtype);
  fdtype (*makeproc)(struct FD_EXTDB *,u8_string,int,fdtype,int,fdtype *);
  void (*recycle_extdb)(struct FD_EXTDB *c);
  void (*recycle_extdb_proc)(struct FD_EXTDB_PROC *c);
  } FD_EXTDB_HANDLER;


FD_EXPORT int fd_register_extdb_handler(struct FD_EXTDB_HANDLER *h);
FD_EXPORT int fd_register_extdb_proc(struct FD_EXTDB_PROC *p);
FD_EXPORT int fd_release_extdb_proc(struct FD_EXTDB_PROC *p);

#endif /* ndef FDB_EXTDB_H */

