/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_FRAMES_H
#define FRAMERD_FRAMES_H 1
#ifndef FRAMERD_FRAMES_H_INFO
#define FRAMERD_FRAMES_H_INFO "include/framerd/frames.h"
#endif

FD_EXPORT lispval fd_method_table;

FD_EXPORT int  fd_slot_cache_load(void);
FD_EXPORT void fd_clear_slotcaches(void);
FD_EXPORT void fd_clear_slotcache(lispval slotid);
FD_EXPORT void fd_clear_slotcache_entry(lispval slotid,lispval frame);

enum FDOP {
  fd_getop, fd_addop, fd_dropop, fd_testop, fd_validop };

typedef struct FD_DEPENDENCY_RECORD {
  lispval frame, slotid, value;} FD_DEPENDENCY_RECORD;
typedef struct FD_DEPENDENCY_RECORD *fd_dependency_record;


typedef struct FD_FRAMEOP_STACK {
  enum FDOP op;
  lispval frame, slotid, value;
  int n_deps, max_deps; struct FD_DEPENDENCY_RECORD *dependencies;
  struct FD_FRAMEOP_STACK *next;} FD_FRAMEOP_STACK;
typedef struct FD_FRAMEOP_STACK fd_frameop_stack;

#define FD_INIT_FRAMEOP_STACK_ENTRY(fse,operation,f,s,v) \
  fse.op = operation; fse.frame = f; fse.slotid = s; fse.value = v; \
  fse.n_deps = fse.max_deps = 0; fse.dependencies = NULL; fse.next = NULL

FD_EXPORT int fd_in_progressp(struct FD_FRAMEOP_STACK *op);
FD_EXPORT void fd_push_opstack(struct FD_FRAMEOP_STACK *);
FD_EXPORT int fd_pop_opstack(struct FD_FRAMEOP_STACK *,int normal);

FD_EXPORT lispval fd_oid_get(lispval f,lispval slotid,lispval dflt);
FD_EXPORT int fd_oid_store(lispval f,lispval slotid,lispval value);
FD_EXPORT int fd_oid_add(lispval f,lispval slotid,lispval value);
FD_EXPORT int fd_oid_drop(lispval f,lispval slotid,lispval value);
FD_EXPORT int fd_oid_test(lispval f,lispval slotid,lispval value);

FD_EXPORT lispval fd_frame_get(lispval f,lispval slotid);
FD_EXPORT int fd_frame_test(lispval f,lispval slotid,lispval value);
FD_EXPORT int fd_frame_add(lispval f,lispval slotid,lispval value);
FD_EXPORT int fd_frame_drop(lispval f,lispval slotid,lispval value);

/* Making frames */

FD_EXPORT lispval fd_new_frame(lispval pool_spec,lispval initval,int deepcopy);

/* Finding frames */

FD_EXPORT lispval fd_prim_find(lispval indexes,lispval slotid,lispval value);
FD_EXPORT lispval fd_finder(lispval indexes,int n,lispval *slotvals);
FD_EXPORT lispval fd_bgfinder(int n,lispval *slotvals);
FD_EXPORT lispval fd_find_frames(lispval indexes,...);
FD_EXPORT lispval fd_bgfind(lispval slotid,lispval values,...);
FD_EXPORT int fd_bg_prefetch(lispval keys);

FD_EXPORT int fd_find_prefetch(fd_index ix,lispval slotids,lispval values);

FD_EXPORT
int fd_index_frame(fd_index ix,lispval frame,lispval slotid,lispval values);

FD_EXPORT int fd_slotindex_merge(fd_index into,lispval from);

#endif /* FRAMERD_FRAMES_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
