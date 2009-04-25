/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
   "$Id: netpools.c 3399 2009-01-01 22:41:45Z haase $";

#define FD_INLINE_DTYPEIO 1

#include "fdb/dtype.h"
#include "fdb/fddb.h"

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>

#include <stdarg.h>

static struct FD_POOL_HANDLER gpool_handler;

FD_EXPORT
fd_pool fd_make_gpool(FD_OID base,int cap,u8_string id,
		      fdtype fetchfn,fdtype loadfn,
		      fdtype allocfn,fdtype savefn)
{
  struct FD_GPOOL *gp=u8_alloc(struct FD_GPOOL);
  fdtype loadval=fd_apply(loadfn,0,NULL); unsigned int load;
  if (!(FD_FIXNUMP(loadval))) 
    return fd_type_error("fd_make_gpool","pool load (fixnum)",loadval);
  else load=FD_FIX2INT(loadval);
  fd_init_pool((fd_pool)gp,base,cap,&gpool_handler,id,id);
  gp->load=load;
  fd_incref(fetchfn); fd_incref(loadfn);
  fd_incref(allocfn); fd_incref(savefn);
  gp->fetchfn=fetchfn; gp->loadfn=loadfn;
  gp->allocfn=allocfn;  gp->savefn=savefn;
  return gp;

}
		      
static struct FD_POOL_HANDLER gpool_handler={
  "netpool", 1, sizeof(struct FD_GPOOL), 12,
   NULL, /* close */
   NULL, /* setcache */
   NULL, /* setbuf */
   gpool_alloc, /* alloc */
   gpool_fetch, /* fetch */
   NULL, /* fetchn */
   gpool_load, /* getload */
   gpool_lock, /* lock */
   gpool_unlock, /* release */
   gpool_storen, /* storen */
   NULL, /* metadata */
   NULL}; /* sync */

FD_EXPORT void fd_init_gpools_c()
{
  fd_register_source_file(versionid);

}
