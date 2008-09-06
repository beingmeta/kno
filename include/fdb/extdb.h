/* C Mode */

FD_EXPORT fd_ptr_type fd_extdb_type;
FD_EXPORT fd_ptr_type fd_extdb_proc_type;

typedef struct FD_EXTDB_HANDLER {
  u8_string name;
  fdtype (*execute)(fdtype,fdtype,fdtype);
  fdtype (*makeproc)(fdtype,fdtype,fdtype,int,fdtype *);
  void (*recycle_extdb)(struct FD_CONS *c);
  void (*recycle_extdb_proc)(struct FD_CONS *c);
  } FD_EXTDB_HANDLER;

#define FD_EXTDB_FIELDS   \
  FD_CONS_HEADER;         \
  u8_string spec, cid;    \
  fdtype colinfo;         \
  struct FD_EXTDB_HANDLER *dbhandler;

struct FD_EXTDB {FD_EXTDB_FIELDS;};

#define FD_EXTDB_PROC_FIELDS   \
  FD_FUNCTION_FIELDS;          \
  u8_string spec,cid, qtext;   \
  int n_params;                \
  fdtype colinfo, *paramtypes; \
  struct FD_EXTDB_HANDLER *dbhandler;

struct FD_EXTDB_PROC {FD_EXTDB_PROC_FIELDS;};

FD_EXPORT int fd_register_extdb_handler(struct FD_EXTDB_HANDLER *h);
