/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/components/storage_layer.h"
#define FD_INLINE_BUFIO 1
#define FD_INLINE_CHOICES 1
#define FD_FAST_CHOICE_CONTAINSP 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

static struct FD_INDEX_HANDLER tempindex_handler;

/* The in-memory index */

static lispval tempindex_fetch(fd_index ix,lispval key)
{
  return FD_EMPTY_CHOICE;
}

static int tempindex_fetchsize(fd_index ix,lispval key)
{
  return 0;
}

static lispval *tempindex_fetchn(fd_index ix,int n,const lispval *keys)
{
  lispval *values = u8_big_alloc_n(n,lispval);
  int i = 0; while (i<n) {values[i++] = FD_EMPTY;}
  return values;
}

static lispval *tempindex_fetchkeys(fd_index ix,int *n)
{
  *n = 0;
  return NULL;
}

static struct FD_KEY_SIZE *tempindex_fetchinfo(fd_index ix,fd_choice filter,int *n)
{
  *n = 0;
  return NULL;
}

static fd_index open_tempindex(u8_string name,fd_storage_flags flags,lispval opts)
{
  struct FD_TEMPINDEX *tempindex = u8_alloc(struct FD_TEMPINDEX);
  lispval metadata = fd_getopt(opts,FDSYM_METADATA,FD_VOID);
  fd_init_index((fd_index)tempindex,&tempindex_handler,
                name,name,name,
                flags|FD_STORAGE_NOSWAP,
                metadata,
                opts);
  fd_register_index((fd_index)tempindex);
  fd_decref(metadata);
  return (fd_index)tempindex;
}

static lispval tempindex_ctl(fd_index ix,lispval op,int n,lispval *args)
{
  struct FD_TEMPINDEX *mix = (struct FD_TEMPINDEX *)ix;
  if ( ((n>0)&&(args == NULL)) || (n<0) )
    return fd_err("BadIndexOpCall","hashindex_ctl",
                  mix->indexid,VOID);
  else if (op == fd_cachelevel_op)
    return FD_INT(0);
  else if (op == fd_capacity_op)
    return EMPTY;
  else if (op == fd_swapout_op)
    return FD_FALSE;
  else if (op == fd_load_op)
    return FD_INT(ix->index_adds.table_n_keys);
  else if (op == fd_keycount_op)
    return FD_INT(ix->index_adds.table_n_keys);
  else return fd_default_indexctl(ix,op,n,args);
}

/* The predicate for testing these */

FD_EXPORT int fd_tempindexp(fd_index ix)
{
  return ( (ix) && (ix->index_handler == &tempindex_handler) );
}

FD_EXPORT fd_index fd_make_tempindex
(u8_string name,fd_storage_flags flags,lispval opts)
{
  return open_tempindex(name,flags,opts);
}

/* Initializing the driver module */

static struct FD_INDEX_HANDLER tempindex_handler={
  "tempindex", 1, sizeof(struct FD_TEMPINDEX), 14,
  NULL, /* close */
  NULL, /* commit */
  tempindex_fetch, /* fetch */
  tempindex_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  tempindex_fetchn, /* fetchn */
  tempindex_fetchkeys, /* fetchkeys */
  tempindex_fetchinfo, /* fetchinfo */
  NULL, /* batchadd */
  NULL, /* create */
  NULL, /* walk */
  NULL, /* recycle */
  tempindex_ctl  /* indexctl */
};

FD_EXPORT void fd_init_tempindex_c()
{
  fd_register_index_type("tempindex",
                         &tempindex_handler,
                         open_tempindex,
                         NULL,
                         NULL);

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
