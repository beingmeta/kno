/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_FRAMES_H
#define KNO_FRAMES_H 1
#ifndef KNO_FRAMES_H_INFO
#define KNO_FRAMES_H_INFO "include/kno/frames.h"
#endif

KNO_EXPORT lispval kno_method_table;

KNO_EXPORT int  kno_slot_cache_load(void);
KNO_EXPORT void kno_clear_slotcaches(void);
KNO_EXPORT void kno_clear_slotcache(lispval slotid);
KNO_EXPORT void kno_clear_slotcache_entry(lispval slotid,lispval frame);

enum FDOP {
  kno_getop, kno_addop, kno_dropop, kno_testop, kno_validop };

typedef struct KNO_DEPENDENCY_RECORD {
  lispval frame, slotid, value;} KNO_DEPENDENCY_RECORD;
typedef struct KNO_DEPENDENCY_RECORD *kno_dependency_record;


typedef struct KNO_FRAMEOP_STACK {
  enum FDOP op;
  lispval frame, slotid, value;
  int n_deps, max_deps; struct KNO_DEPENDENCY_RECORD *dependencies;
  struct KNO_FRAMEOP_STACK *next;} KNO_FRAMEOP_STACK;
typedef struct KNO_FRAMEOP_STACK kno_frameop_stack;

#define KNO_INIT_FRAMEOP_STACK_ENTRY(fse,operation,f,s,v) \
  fse.op = operation; fse.frame = f; fse.slotid = s; fse.value = v; \
  fse.n_deps = fse.max_deps = 0; fse.dependencies = NULL; fse.next = NULL

KNO_EXPORT int kno_in_progressp(struct KNO_FRAMEOP_STACK *op);
KNO_EXPORT void kno_push_opstack(struct KNO_FRAMEOP_STACK *);
KNO_EXPORT int kno_pop_opstack(struct KNO_FRAMEOP_STACK *,int normal);

KNO_EXPORT lispval kno_oid_get(lispval f,lispval slotid,lispval dflt);
KNO_EXPORT int kno_oid_store(lispval f,lispval slotid,lispval value);
KNO_EXPORT int kno_oid_add(lispval f,lispval slotid,lispval value);
KNO_EXPORT int kno_oid_drop(lispval f,lispval slotid,lispval value);
KNO_EXPORT int kno_oid_test(lispval f,lispval slotid,lispval value);

KNO_EXPORT lispval kno_frame_get(lispval f,lispval slotid);
KNO_EXPORT int kno_frame_test(lispval f,lispval slotid,lispval value);
KNO_EXPORT int kno_frame_add(lispval f,lispval slotid,lispval value);
KNO_EXPORT int kno_frame_drop(lispval f,lispval slotid,lispval value);

/* Making frames */

KNO_EXPORT lispval kno_new_frame(lispval pool_spec,lispval initval,int deepcopy);

/* Finding frames */

KNO_EXPORT lispval kno_prim_find(lispval indexes,lispval slotid,lispval value);
KNO_EXPORT lispval kno_finder(lispval indexes,int n,lispval *slotvals);
KNO_EXPORT lispval kno_bgfinder(int n,lispval *slotvals);
KNO_EXPORT lispval kno_find_frames(lispval indexes,...);
KNO_EXPORT lispval kno_bgfind(lispval slotid,lispval values,...);
KNO_EXPORT int kno_bg_prefetch(lispval keys);

KNO_EXPORT int kno_find_prefetch(kno_index ix,lispval slotids,lispval values);

KNO_EXPORT
int kno_index_frame(kno_index ix,lispval frame,lispval slotid,lispval values);

KNO_EXPORT int kno_slotindex_merge(kno_index into,lispval from);

#endif /* KNO_FRAMES_H */

