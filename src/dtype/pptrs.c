/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2006-2015 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Persistent pointers are immediate values which refer to conses
   maintained in a table.  The idea is that they can be passed around
   without GC operations or the corresponding lock contentions.  They
   especially help speed up the evaluator when used to wrap primitives
   and (to a lesser degree) primitives.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_PPTRS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"

#include <libu8/u8printf.h>

#define FD_PPTR_MAX (FD_PPTR_NBLOCKS*FD_PPTR_BLOCKSIZE)

fd_exception fd_InvalidPPtr=_("Invalid persistent pointer reference");
fd_exception fd_PPtrOverflow=_("No more valid persistent pointers");

struct FD_CONS **_fd_pptrs[FD_PPTR_NBLOCKS];
int _fd_npptrs=0;

#if FD_THREADS_ENABLED
u8_mutex _fd_pptr_lock;
#endif

FD_EXPORT fdtype _fd_pptr_ref(fdtype x)
{
  return fd_pptr_ref(x);
}

FD_EXPORT fdtype fd_pptr_register(fdtype x)
{
  int serialno;
  if (!(FD_CONSP(x)))
    return fd_type_error("cons","fd_pptr_register",x);
  fd_lock_mutex(&_fd_pptr_lock);
  if (_fd_npptrs>=FD_PPTR_MAX) {
    fd_unlock_mutex(&_fd_pptr_lock);
    return fd_err(fd_PPtrOverflow,"fd_register_pptr",NULL,x);}
  serialno=_fd_npptrs++;
  if ((serialno%FD_PPTR_BLOCKSIZE)==0) {
    struct FD_CONS **block=u8_alloc_n(FD_PPTR_BLOCKSIZE,struct FD_CONS *);
    int i=0, n=FD_PPTR_BLOCKSIZE;
    while (i<n) block[i++]=NULL;
    _fd_pptrs[serialno/FD_PPTR_BLOCKSIZE]=block;}
  fd_incref(x);
  _fd_pptrs[serialno/FD_PPTR_BLOCKSIZE][serialno%FD_PPTR_BLOCKSIZE]=
    (struct FD_CONS *)x;
  fd_unlock_mutex(&_fd_pptr_lock);
  return FDTYPE_IMMEDIATE(fd_pptr_type,serialno);
}

static int unparse_pptr(u8_output out,fdtype x)
{
  fdtype lp=fd_pptr_ref(x);
  u8_printf(out,"#~%q",lp);
  return 1;
}

FD_EXPORT void fd_init_pptrs_c()
{
  fd_type_names[fd_pptr_type]=_("persistent pointer");
  fd_unparsers[fd_pptr_type]=unparse_pptr;
#if FD_THREADS_ENABLED
  fd_init_mutex(&_fd_pptr_lock);
#endif
  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
