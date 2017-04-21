/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_FRAMES_H
#define FRAMERD_FRAMES_H 1
#ifndef FRAMERD_FRAMES_H_INFO
#define FRAMERD_FRAMES_H_INFO "include/framerd/frames.h"
#endif

/* Adjuncts: these are indexes which function as slotids. */

FD_EXPORT int fd_set_adjunct(fd_pool p,fdtype slotid,fdtype table);
FD_EXPORT fd_adjunct fd_get_adjunct(fd_pool p,fdtype slotid);
FD_EXPORT int fd_adjunctp(fd_pool p,fdtype slotid);

FD_EXPORT fdtype fd_adjunct_slotids;

FD_EXPORT int fd_pool_setop(fd_pool,fdtype,fdtype,fdtype);

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
  fse.op = operation; fse.frame = f; fse.slotid = s; fse.value = v; \
  fse.n_deps = fse.max_deps = 0; fse.dependencies = NULL; fse.next = NULL

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

FD_EXPORT fdtype fd_overlay_get(fdtype f,fdtype slotid,int);
FD_EXPORT fdtype fd_overlay_add(fdtype f,fdtype slotid,fdtype value,int);
FD_EXPORT fdtype fd_overlay_drop(fdtype f,fdtype slotid,fdtype value,int);
FD_EXPORT fdtype fd_overlay_store(fdtype f,fdtype slotid,fdtype value,int);

FD_EXPORT int fd_overlayp(void);
FD_EXPORT void fd_inhibit_overlays(int flag);

/* Overlay inhibition */

#if FD_USE_TLS
FD_EXPORT u8_tld_key _fd_inhibit_overlay_key;
#define fd_inhibit_overlay ((int)((u8_tld_get(_fd_inhibit_overlay_key))))
#else
FD_EXPORT __thread int fd_inhibit_overlay;
#endif

/* Making frames */

FD_EXPORT fdtype fd_new_frame(fdtype pool_spec,fdtype initval,int deepcopy);

/* Finding frames */

FD_EXPORT fdtype fd_prim_find(fdtype indexes,fdtype slotid,fdtype value);
FD_EXPORT fdtype fd_finder(fdtype indexes,int n,fdtype *slotvals);
FD_EXPORT fdtype fd_bgfinder(int n,fdtype *slotvals);
FD_EXPORT fdtype fd_find_frames(fdtype indexes,...);
FD_EXPORT fdtype fd_bgfind(fdtype slotid,fdtype values,...);
FD_EXPORT int fd_bg_prefetch(fdtype keys);

FD_EXPORT int fd_find_prefetch(fd_index ix,fdtype slotids,fdtype values);

FD_EXPORT
int fd_index_frame(fd_index ix,fdtype frame,fdtype slotid,fdtype values);


#endif /* FRAMERD_FRAMES_H */


