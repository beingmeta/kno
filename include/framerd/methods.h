/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_METHODS_H
#define FRAMERD_METHODS_H 1
#ifndef FRAMERD_METHODS_H_INFO
#define FRAMERD_METHODS_H_INFO "include/framerd/methods.h"
#endif

#include "storage.h"

/* Data structures */

typedef int (*fd_tree_walkfn)(lispval node,void *data);

/* These are all used when walking lattices to do various
   inferential gathering: kleene closures, value inheritance,
   path finding, etc. */
typedef struct FD_IVSTRUCT { /* inherit values structure */
  lispval result; lispval slotids;} FD_IVSTRUCT;
typedef struct FD_IVSTRUCT *fd_ivstruct;
typedef struct FD_IVPSTRUCT { /* test inherited values structure */
  lispval value; lispval slotids; int result;} FD_IVPSTRUCT;
typedef struct FD_IVPSTRUCT *fd_ivpstruct;
typedef struct FD_PATHPSTRUCT { /* find paths structure */
  lispval target; int result;} FD_PATHPSTRUCT;
typedef struct FD_PATHPSTRUCT *fd_pathpstruct;

/* Prototypes */

FD_EXPORT int fd_walk_tree
  (lispval roots,lispval slotids,fd_tree_walkfn walk,void *data);
FD_EXPORT int fd_collect_tree
  (struct FD_HASHSET *h,lispval roots,lispval slotids);

FD_EXPORT lispval fd_get_basis
  (lispval collection,lispval lattice);
FD_EXPORT lispval fd_inherit_values
  (lispval root,lispval slotid,lispval through);
FD_EXPORT int fd_inherits_valuep
  (lispval root,lispval slotid,lispval through,lispval value);
FD_EXPORT int fd_pathp(lispval root,lispval slotid,lispval to);

#endif /* FRAMERD_METHODS_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
