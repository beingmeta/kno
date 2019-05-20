/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_PROVIDE_FASTEVAL 1

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/eval.h"
#include "eval_internals.h"
#include "kno/storage.h"

#include <libu8/u8printf.h>

static lispval lambda_symbol;

/* Macros */

KNO_EXPORT lispval kno_make_macro(u8_string name,lispval xformer)
{
  int xftype = KNO_PRIM_TYPE(xformer);
  if ((xftype<KNO_TYPE_MAX) && (kno_applyfns[xftype])) {
    u8_string sourcebase = kno_sourcebase();
    struct KNO_MACRO *s = u8_alloc(struct KNO_MACRO);
    KNO_INIT_CONS(s,kno_macro_type);
    s->macro_name = ((name) ? (u8_strdup(name)) : (NULL));
    s->macro_transformer = kno_incref(xformer);
    if (sourcebase)
      s->macro_filename = u8_strdup(sourcebase);
    else s->macro_filename = NULL;
    s->macro_moduleid = KNO_VOID;
    return LISP_CONS(s);}
  else return kno_err(kno_InvalidMacro,NULL,name,xformer);
}

static lispval macro_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  if ((PAIRP(expr)) && (PAIRP(KNO_CDR(expr))) &&
      (SYMBOLP(KNO_CADR(expr))) &&
      (PAIRP(KNO_CDR(KNO_CDR(expr))))) {
    lispval name = KNO_CADR(expr), body = KNO_CDR(KNO_CDR(expr));
    lispval lambda_form=
      kno_conspair(lambda_symbol,
                  kno_conspair(kno_make_list(1,name),kno_incref(body)));
    lispval transformer = kno_eval(lambda_form,env);
    lispval macro = kno_make_macro(SYM_NAME(name),transformer);
    kno_decref(lambda_form);
    kno_decref(transformer);
    return macro;}
  else return kno_err(kno_SyntaxError,"MACRO",NULL,expr);
}

KNO_EXPORT void recycle_macro(struct KNO_RAW_CONS *c)
{
  struct KNO_MACRO *mproc = (struct KNO_MACRO *)c;
  if (mproc->macro_name) u8_free(mproc->macro_name);
  if (mproc->macro_filename) u8_free(mproc->macro_filename);
  if (mproc->macro_moduleid) kno_decref(mproc->macro_moduleid);
  kno_decref(mproc->macro_transformer);
  if (KNO_MALLOCD_CONSP(c)) u8_free(mproc);
}

static int unparse_macro(u8_output out,lispval x)
{
  struct KNO_MACRO *mproc = kno_consptr(struct KNO_MACRO *,x,kno_macro_type);
  lispval moduleid = mproc->macro_moduleid;
  u8_string modname =
    (KNO_SYMBOLP(moduleid)) ? (KNO_SYMBOL_NAME(moduleid)) : (NULL);
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

static int walk_macro(kno_walker walker,lispval obj,void *walkdata,
                      kno_walk_flags flags,int depth)
{
  struct KNO_MACRO *mproc = kno_consptr(struct KNO_MACRO *,obj,kno_macro_type);
  if (kno_walk(walker,mproc->macro_transformer,walkdata,flags,depth-1)<0)
    return -1;
  else return 2;
}

KNO_EXPORT void kno_init_macros_c()
{
  u8_register_source_file(_FILEINFO);

  moduleid_symbol = kno_intern("%moduleid");
  lambda_symbol = kno_intern("lambda");

  kno_walkers[kno_macro_type]=walk_macro;
  kno_unparsers[kno_macro_type]=unparse_macro;
  kno_recyclers[kno_macro_type]=recycle_macro;

  kno_def_evalfn(kno_scheme_module,"MACRO","",macro_evalfn);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
