/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2015 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_METHODS_H
#define FRAMERD_METHODS_H 1
#ifndef FRAMERD_METHODS_H_INFO
#define FRAMERD_METHODS_H_INFO "include/framerd/methods.h"
#endif

#include "fddb.h"

/* Data structures */

typedef int (*fd_tree_walkfn)(fdtype node,void *data);

/* These are all used when walking lattices to do various
   inferential gathering: kleene closures, value inheritance,
   path finding, etc. */
typedef struct FD_IVSTRUCT { /* inherit values structure */
  fdtype result; fdtype slotids;} FD_IVSTRUCT;
typedef struct FD_IVSTRUCT *fd_ivstruct;
typedef struct FD_IVPSTRUCT { /* test inherited values structure */
  fdtype value; fdtype slotids; int result;} FD_IVPSTRUCT;
typedef struct FD_IVPSTRUCT *fd_ivpstruct;
typedef struct FD_PATHPSTRUCT { /* find paths structure */
  fdtype target; int result;} FD_PATHPSTRUCT;
typedef struct FD_PATHPSTRUCT *fd_pathpstruct;

/* Prototypes */

FD_EXPORT int fd_walk_tree
  (fdtype roots,fdtype slotids,fd_tree_walkfn walk,void *data);
FD_EXPORT int fd_collect_tree
  (struct FD_HASHSET *h,fdtype roots,fdtype slotids);

FD_EXPORT fdtype fd_get_basis
  (fdtype collection,fdtype lattice);
FD_EXPORT fdtype fd_inherit_values
  (fdtype root,fdtype slotid,fdtype through);
FD_EXPORT int fd_inherits_valuep
  (fdtype root,fdtype slotid,fdtype through,fdtype value);
FD_EXPORT int fd_pathp(fdtype root,fdtype slotid,fdtype to);



#endif /* FRAMERD_METHODS_H */
