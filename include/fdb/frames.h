/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_FRAMES_H
#define FDB_FRAMES_H 1
#define FDB_FRAMES_H_VERSION "$Id$"

/* Adjuncts: these are indices which function as slotids. */

FD_EXPORT int fd_set_adjunct(fd_index ix,fdtype slotid,fd_pool p);
FD_EXPORT fd_index fd_get_adjunct(fdtype slotid,fd_pool p);
FD_EXPORT int fd_adjunctp(fdtype slotid,fd_pool p);

FD_EXPORT fdtype fd_adjunct_slotids;

FD_EXPORT fdtype fd_method_table;

FD_EXPORT int  fd_slot_cache_load(void);
FD_EXPORT void fd_clear_slotcaches(void);
FD_EXPORT void fd_clear_slotcache(fdtype slotid);
FD_EXPORT void fd_clear_slotcache_entry(fdtype slotid,fdtype frame);

enum FDOP {
  fd_getop, fd_addop, fd_dropop, fd_testop, fd_validop };

typedef struct FD_DEPENDENCY_RECORD {
  fdtype frame, slotid, value;} FD_DEPENDENCY_RECORD;
typedef struct FD_DEPENDENCY_RECORD *fd_dependency_record;
  

typedef struct FD_FRAMEOP_STACK {
  enum FDOP op;
  fdtype frame, slotid, value;
  int n_deps, max_deps; struct FD_DEPENDENCY_RECORD *dependencies;
  struct FD_FRAMEOP_STACK *next;} FD_FRAMEOP_STACK;
typedef struct FD_FRAMEOP_STACK fd_frameop_stack;

#define FD_INIT_FRAMEOP_STACK_ENTRY(fse,operation,f,s,v) \
  fse.op=operation; fse.frame=f; fse.slotid=s; fse.value=v; \
  fse.n_deps=fse.max_deps=0; fse.dependencies=NULL; fse.next=NULL

FD_EXPORT int fd_in_progressp(struct FD_FRAMEOP_STACK *op);
FD_EXPORT void fd_push_opstack(struct FD_FRAMEOP_STACK *);
FD_EXPORT int fd_pop_opstack(struct FD_FRAMEOP_STACK *,int normal);

FD_EXPORT fdtype fd_oid_get(fdtype f,fdtype slotid,fdtype dflt);
FD_EXPORT int fd_oid_add(fdtype f,fdtype slotid,fdtype value);
FD_EXPORT int fd_oid_drop(fdtype f,fdtype slotid,fdtype value);
FD_EXPORT int fd_oid_test(fdtype f,fdtype slotid,fdtype value);

FD_EXPORT fdtype fd_frame_get(fdtype f,fdtype slotid);
FD_EXPORT int fd_frame_test(fdtype f,fdtype slotid,fdtype value);
FD_EXPORT int fd_frame_add(fdtype f,fdtype slotid,fdtype value);
FD_EXPORT int fd_frame_drop(fdtype f,fdtype slotid,fdtype value);

/* Finding frames */

FD_EXPORT fdtype fd_prim_find(fdtype indices,fdtype slotid,fdtype value);
FD_EXPORT fdtype fd_finder(fdtype indices,int n,fdtype *slotvals);
FD_EXPORT fdtype fd_bgfinder(int n,fdtype *slotvals);
FD_EXPORT fdtype fd_find_frames(fdtype indices,...);
FD_EXPORT fdtype fd_bgfind(fdtype slotid,fdtype values,...);
FD_EXPORT int fd_bg_prefetch(fdtype keys);

FD_EXPORT 
int fd_index_frame(fd_index ix,fdtype frame,fdtype slotid,fdtype values);


#endif /* FDB_FRAMES_H */


