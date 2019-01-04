/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
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
#include "framerd/storage.h"

#include <libu8/u8printf.h>

static lispval lambda_symbol;

/* Macros */

FD_EXPORT lispval fd_make_macro(u8_string name,lispval xformer)
{
  int xftype = FD_PRIM_TYPE(xformer);
  if ((xftype<FD_TYPE_MAX) && (fd_applyfns[xftype])) {
    u8_string sourcebase = fd_sourcebase();
    struct FD_MACRO *s = u8_alloc(struct FD_MACRO);
    FD_INIT_CONS(s,fd_macro_type);
    s->macro_name = ((name) ? (u8_strdup(name)) : (NULL));
    s->macro_transformer = fd_incref(xformer);
    if (sourcebase)
      s->macro_filename = u8_strdup(sourcebase);
    else s->macro_filename = NULL;
    s->macro_moduleid = FD_VOID;
    return LISP_CONS(s);}
  else return fd_err(fd_InvalidMacro,NULL,name,xformer);
}

static lispval macro_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  if ((PAIRP(expr)) && (PAIRP(FD_CDR(expr))) &&
      (SYMBOLP(FD_CADR(expr))) &&
      (PAIRP(FD_CDR(FD_CDR(expr))))) {
    lispval name = FD_CADR(expr), body = FD_CDR(FD_CDR(expr));
    lispval lambda_form=
      fd_conspair(lambda_symbol,
                  fd_conspair(fd_make_list(1,name),fd_incref(body)));
    lispval transformer = fd_eval(lambda_form,env);
    lispval macro = fd_make_macro(SYM_NAME(name),transformer);
    fd_decref(lambda_form);
    fd_decref(transformer);
    return macro;}
  else return fd_err(fd_SyntaxError,"MACRO",NULL,expr);
}

FD_EXPORT void recycle_macro(struct FD_RAW_CONS *c)
{
  struct FD_MACRO *mproc = (struct FD_MACRO *)c;
  if (mproc->macro_name) u8_free(mproc->macro_name);
  if (mproc->macro_filename) u8_free(mproc->macro_filename);
  if (mproc->macro_moduleid) fd_decref(mproc->macro_moduleid);
  fd_decref(mproc->macro_transformer);
  if (FD_MALLOCD_CONSP(c)) u8_free(mproc);
}

static int unparse_macro(u8_output out,lispval x)
{
  struct FD_MACRO *mproc = fd_consptr(struct FD_MACRO *,x,fd_macro_type);
  lispval moduleid = mproc->macro_moduleid;
  u8_string modname =
    (FD_SYMBOLP(moduleid)) ? (FD_SYMBOL_NAME(moduleid)) : (NULL);
  u8_string filename = mproc->macro_filename;
  if ( (modname) && (mproc->macro_name))
    u8_printf(out,"#<MACRO %s %s #!%x%s%s%s>",
              mproc->macro_name,modname,
              (unsigned long)mproc,
              U8OPTSTR(" '",filename,"'"));
  else if (mproc->macro_name)
    u8_printf(out,"#<MACRO %s #!%x %s%s%s>",
              mproc->macro_name,(unsigned long)mproc,
              U8OPTSTR(" '",filename,"'"));
  else u8_printf(out,"#<MACRO #!%x%s%s%s>",
                 (unsigned long)mproc,
                 U8OPTSTR(" '",filename,"'"));
  return 1;
}

static int walk_macro(fd_walker walker,lispval obj,void *walkdata,
                      fd_walk_flags flags,int depth)
{
  struct FD_MACRO *mproc = fd_consptr(struct FD_MACRO *,obj,fd_macro_type);
  if (fd_walk(walker,mproc->macro_transformer,walkdata,flags,depth-1)<0)
    return -1;
  else return 2;
}

FD_EXPORT void fd_init_macros_c()
{
  u8_register_source_file(_FILEINFO);

  moduleid_symbol = fd_intern("%MODULEID");
  lambda_symbol = fd_intern("LAMBDA");

  fd_walkers[fd_macro_type]=walk_macro;
  fd_unparsers[fd_macro_type]=unparse_macro;
  fd_recyclers[fd_macro_type]=recycle_macro;

  fd_def_evalfn(fd_scheme_module,"MACRO","",macro_evalfn);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
