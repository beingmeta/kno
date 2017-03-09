/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "eval_internals.h"
#include "framerd/fdb.h"

#include <libu8/u8printf.h>

fd_ptr_type fd_macro_type;
static fdtype lambda_symbol;

/* Macros */

FD_EXPORT fdtype fd_make_macro(u8_string name,fdtype xformer)
{
  int xftype=FD_PRIM_TYPE(xformer);
  if ((xftype<FD_TYPE_MAX) && (fd_applyfns[xftype])) {
    struct FD_MACRO *s=u8_alloc(struct FD_MACRO);
    FD_INIT_CONS(s,fd_macro_type);
    s->fd_macro_name=((name) ? (u8_strdup(name)) : (NULL));
    s->fd_macro_transformer=fd_incref(xformer);
    return FDTYPE_CONS(s);}
  else return fd_err(fd_InvalidMacro,NULL,name,xformer);
}

static fdtype macro_handler(fdtype expr,fd_lispenv env)
{
  if ((FD_PAIRP(expr)) && (FD_PAIRP(FD_CDR(expr))) &&
      (FD_SYMBOLP(FD_CADR(expr))) &&
      (FD_PAIRP(FD_CDR(FD_CDR(expr))))) {
    fdtype name=FD_CADR(expr), body=FD_CDR(FD_CDR(expr));
    fdtype lambda_form=
      fd_conspair(lambda_symbol,
                  fd_conspair(fd_make_list(1,name),fd_incref(body)));
    fdtype transformer=fd_eval(lambda_form,env);
    fdtype macro=fd_make_macro(FD_SYMBOL_NAME(name),transformer);
    fd_decref(lambda_form); fd_decref(transformer);
    return macro;}
  else return fd_err(fd_SyntaxError,"MACRO",NULL,expr);
}

FD_EXPORT void recycle_macro(struct FD_RAW_CONS *c)
{
  struct FD_MACRO *mproc=(struct FD_MACRO *)c;
  if (mproc->fd_macro_name) u8_free(mproc->fd_macro_name);
  fd_decref(mproc->fd_macro_transformer);
  if (FD_MALLOCD_CONSP(c)) u8_free(mproc);
}

static int unparse_macro(u8_output out,fdtype x)
{
  struct FD_MACRO *mproc=fd_consptr(struct FD_MACRO *,x,fd_macro_type);
  if (mproc->fd_macro_name)
    u8_printf(out,"#<MACRO %s #!%x>",
              mproc->fd_macro_name,(unsigned long)mproc);
  else u8_printf(out,"#<MACRO #!%x>",(unsigned long)mproc);
  return 1;
}

FD_EXPORT void fd_init_macros_c()
{
  u8_register_source_file(_FILEINFO);

  fd_macro_type=fd_register_cons_type(_("scheme syntactic macro"));

  moduleid_symbol=fd_intern("%MODULEID");
  lambda_symbol=fd_intern("LAMBDA");

  fd_unparsers[fd_macro_type]=unparse_macro;
  fd_recyclers[fd_macro_type]=recycle_macro;

  fd_defspecial(fd_scheme_module,"MACRO",macro_handler);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
