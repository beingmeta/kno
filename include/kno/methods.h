/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_METHODS_H
#define KNO_METHODS_H 1
#ifndef KNO_METHODS_H_INFO
#define KNO_METHODS_H_INFO "include/kno/methods.h"
#endif

#include "storage.h"

/* Data structures */

typedef int (*kno_tree_walkfn)(lispval node,void *data);

/* These are all used when walking lattices to do various
   inferential gathering: kleene closures, value inheritance,
   path finding, etc. */
typedef struct KNO_IVSTRUCT { /* inherit values structure */
  lispval result; lispval slotids;} KNO_IVSTRUCT;
typedef struct KNO_IVSTRUCT *kno_ivstruct;
typedef struct KNO_IVPSTRUCT { /* test inherited values structure */
  lispval value; lispval slotids; int result;} KNO_IVPSTRUCT;
typedef struct KNO_IVPSTRUCT *kno_ivpstruct;
typedef struct KNO_PATHPSTRUCT { /* find paths structure */
  lispval target; int result;} KNO_PATHPSTRUCT;
typedef struct KNO_PATHPSTRUCT *kno_pathpstruct;

/* Prototypes */

KNO_EXPORT int kno_walk_tree
  (lispval roots,lispval slotids,kno_tree_walkfn walk,void *data);
KNO_EXPORT int kno_collect_tree
  (struct KNO_HASHSET *h,lispval roots,lispval slotids);

KNO_EXPORT lispval kno_get_basis
  (lispval collection,lispval lattice);
KNO_EXPORT lispval kno_inherit_values
  (lispval root,lispval slotid,lispval through);
KNO_EXPORT int kno_inherits_valuep
  (lispval root,lispval slotid,lispval through,lispval value);
KNO_EXPORT int kno_pathp(lispval root,lispval slotid,lispval to);

#endif /* KNO_METHODS_H */

