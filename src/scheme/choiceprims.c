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
#include "framerd/fddb.h"
#include "framerd/numbers.h"
#include "framerd/frames.h"
#include "framerd/sorting.h"

/* Choice iteration */

static fdtype parse_control_spec
  (fdtype expr,fdtype *value,fdtype *iter_var,fd_lispenv env)
{
  fdtype control_expr=fd_get_arg(expr,1);
  if (FD_VOIDP(control_expr))
    return fd_err(fd_TooFewExpressions,NULL,NULL,expr);
  else if (FD_SYMBOLP(control_expr)) {
    fdtype values=fd_eval(control_expr,env);
    if (FD_ABORTED(values)) {
      *value=FD_VOID; return values;}
    *value=values; *iter_var=FD_VOID;
    return control_expr;}
  else {
    fdtype var=fd_get_arg(control_expr,0), ivar=fd_get_arg(control_expr,2);
    fdtype val_expr=fd_get_arg(control_expr,1), val;
    if (FD_VOIDP(control_expr))
      return fd_err(fd_TooFewExpressions,NULL,NULL,expr);
    else if (FD_VOIDP(val_expr))
      return fd_err(fd_TooFewExpressions,NULL,NULL,control_expr);
    else if (!(FD_SYMBOLP(var)))
      return fd_err(fd_SyntaxError,
                    _("identifier is not a symbol"),NULL,control_expr);
    else if (!((FD_VOIDP(ivar)) || (FD_SYMBOLP(ivar))))
      return fd_err(fd_SyntaxError,
                    _("identifier is not a symbol"),NULL,control_expr);
    val=fasteval(val_expr,env);
    if (FD_ABORTED(val)) {
      *value=val;
      return FD_VOID;}
    *value=val; if (iter_var) *iter_var=ivar;
    return var;}
}

static fdtype retenv1(fdtype var,fdtype val)
{
  struct FD_KEYVAL keyvals[1];
  keyvals[0].fd_kvkey=var; keyvals[0].fd_keyval=fd_incref(val);
  return fd_make_slotmap(1,1,keyvals);
}
static fdtype retenv2(fdtype var,fdtype val,fdtype xvar,fdtype xval)
{
  struct FD_KEYVAL keyvals[2];
  keyvals[0].fd_kvkey=var; keyvals[0].fd_keyval=fd_incref(val);
  keyvals[1].fd_kvkey=xvar; keyvals[1].fd_keyval=fd_incref(xval);
  return fd_make_slotmap(2,2,keyvals);
}

/* This iterates over a set of choices, evaluating its body for each value.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   It returns VOID. */
static fdtype dochoices_handler(fdtype expr,fd_lispenv env)
{
  fdtype choices, count_var, var=
    parse_control_spec(expr,&choices,&count_var,env);
  fdtype *vloc=NULL, *iloc=NULL;
  fdtype vars[2], vals[2];
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTED(var)) return var;
  else if (FD_ABORTED(choices))
    return choices;
  else if (FD_EMPTY_CHOICEP(choices)) return FD_VOID;
  else if (FD_ABORTED(choices)) return choices;
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  if (FD_VOIDP(count_var)) {
    bindings.schema_length=1;
    vars[0]=var; vals[0]=FD_VOID;
    vloc=&(vals[0]);}
  else {
    bindings.schema_length=2;
    vars[0]=var; vals[0]=FD_VOID; vloc=&(vals[0]);
    vars[1]=count_var; vals[1]=FD_INT(0); iloc=&(vals[1]);}
  bindings.table_schema=vars; bindings.schema_values=vals;
  bindings.schemap_onstack=1;
  fd_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent=env;
  envstruct.env_bindings=(fdtype)(&bindings); envstruct.env_exports=FD_VOID;
  envstruct.env_copy=NULL;
  {
    int i=0; FD_DO_CHOICES(elt,choices) {
      fd_incref(elt);
      if (envstruct.env_copy) {
        fd_set_value(var,elt,envstruct.env_copy);
        if (iloc) fd_set_value(count_var,FD_INT(i),envstruct.env_copy);}
      else {
        *vloc=elt;
        if (iloc) *iloc=FD_INT(i);}
      {fdtype steps=fd_get_body(expr,2);
        FD_DOLIST(step,steps) {
          fdtype val=fasteval(step,&envstruct);
          if (FD_THROWP(val)) {
            fd_decref(choices);
            if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
            return val;}
          else if (FD_ABORTED(val)) {
            fdtype env;
            if (iloc) env=retenv2(var,elt,count_var,FD_INT(i));
            else env=retenv1(var,elt);
            fd_decref(choices);
            if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
            fd_push_error_context(":DO-CHOICES",env);
            return val;}
          fd_decref(val);}}
      if (envstruct.env_copy) {
        fd_recycle_environment(envstruct.env_copy);
        envstruct.env_copy=NULL;}
      fd_decref(*vloc); *vloc=FD_VOID;
      i++;}
    fd_decref(choices);
    fd_destroy_rwlock(&(bindings.table_rwlock));
    if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
    return FD_VOID;}
}

/* This iterates over a set of choices, evaluating its body for each value.
   It returns the first non-empty result of evaluating the body.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   It returns VOID. */
static fdtype trychoices_handler(fdtype expr,fd_lispenv env)
{
  fdtype results=FD_EMPTY_CHOICE;
  fdtype choices, count_var, var=
    parse_control_spec(expr,&choices,&count_var,env);
  fdtype *vloc=NULL, *iloc=NULL;
  fdtype vars[2], vals[2];
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTED(var)) return var;
  else if (FD_ABORTED(choices)) {
    fd_push_error_context("trychoices_handler",expr);
    return choices;}
  else if (FD_EMPTY_CHOICEP(choices)) return FD_EMPTY_CHOICE;
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  if (FD_VOIDP(count_var)) {
    bindings.schema_length=1;
    vars[0]=var; vals[0]=FD_VOID; vloc=&(vals[0]);}
  else {
    bindings.schema_length=2;
    vars[0]=var; vals[0]=FD_VOID; vloc=&(vals[0]);
    vars[1]=count_var; vals[1]=FD_INT(0); iloc=&(vals[1]);}
  bindings.table_schema=vars; bindings.schema_values=vals;
  bindings.schemap_onstack=1;
  fd_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent=env;
  envstruct.env_bindings=(fdtype)(&bindings); envstruct.env_exports=FD_VOID;
  envstruct.env_copy=NULL;
  {
    int i=0; FD_DO_CHOICES(elt,choices) {
      fdtype val=FD_VOID;
      if (envstruct.env_copy) {
        fd_set_value(var,elt,envstruct.env_copy);
        if (iloc) fd_set_value(count_var,FD_INT(i),envstruct.env_copy);}
      else {
        *vloc=elt; fd_incref(elt);
        if (iloc) *iloc=FD_INT(i);}
      {fdtype attempts=fd_get_body(expr,2);
        FD_DOLIST(attempt,attempts) {
          fd_decref(val);
          val=fasteval(attempt,&envstruct);
          if (FD_THROWP(val)) {
            fd_decref(choices);
            if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
            return val;}
          else if (FD_ABORTED(val)) {
            fdtype env;
            if (iloc) env=retenv2(var,elt,count_var,FD_INT(i));
            else env=retenv1(var,elt);
            fd_decref(choices);
            if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
            fd_push_error_context(":TRY-CHOICES",env);
            return val;}}}
      if (!(FD_EMPTY_CHOICEP(val))) {
        FD_STOP_DO_CHOICES;
        fd_decref(choices); fd_decref(*vloc); *vloc=FD_VOID;
        if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
        return val;}
      fd_decref(*vloc); *vloc=FD_VOID;
      i++;}
    fd_decref(choices);
    fd_destroy_rwlock(&(bindings.table_rwlock));
    if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
    return fd_simplify_choice(results);}
}

/* This iterates over a set of choices, evaluating its body for each value, and
    accumulating the results of those evaluations.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   It returns the combined results of its body's execution. */
static fdtype forchoices_handler(fdtype expr,fd_lispenv env)
{
  fdtype results=FD_EMPTY_CHOICE;
  fdtype choices, count_var, var=
    parse_control_spec(expr,&choices,&count_var,env);
  fdtype *vloc=NULL, *iloc=NULL;
  fdtype vars[2], vals[2];
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTED(var)) return var;
  else if (FD_ABORTED(choices))
    return choices;
  else if (FD_EMPTY_CHOICEP(choices))
    return FD_EMPTY_CHOICE;
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  if (FD_VOIDP(count_var)) {
    bindings.schema_length=1;
    vars[0]=var; vals[0]=FD_VOID; vloc=&(vals[0]);}
  else {
    bindings.schema_length=2;
    vars[0]=var; vals[0]=FD_VOID; vloc=&(vals[0]);
    vars[1]=count_var; vals[1]=FD_INT(0); iloc=&(vals[1]);}
  bindings.table_schema=vars; bindings.schema_values=vals;
  bindings.schemap_onstack=1;
  fd_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent=env;
  envstruct.env_bindings=(fdtype)(&bindings); envstruct.env_exports=FD_VOID;
  envstruct.env_copy=NULL;
  {
    int i=0; FD_DO_CHOICES(elt,choices) {
      fdtype val=FD_VOID;
      if (envstruct.env_copy) {
        fd_set_value(var,elt,envstruct.env_copy);
        if (iloc) fd_set_value(count_var,FD_INT(i),envstruct.env_copy);}
      else {
        *vloc=elt; fd_incref(elt);
        if (iloc) *iloc=FD_INT(i);}
      {fdtype body=fd_get_body(expr,2);
        FD_DOLIST(subexpr,body) {
          fd_decref(val);
          val=fasteval(subexpr,&envstruct);
          if (FD_THROWP(val)) {
            fd_decref(choices);
            if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
            fd_decref(results);
            FD_STOP_DO_CHOICES;
            return val;}
          else if (FD_ABORTED(val)) {
            fdtype env;
            if (iloc) env=retenv2(var,elt,count_var,FD_INT(i));
            else env=retenv1(var,elt);
            fd_decref(choices);
            if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
            fd_push_error_context(":FOR-CHOICES",env);
            fd_decref(results);
            FD_STOP_DO_CHOICES;
            return val;}}}
      FD_ADD_TO_CHOICE(results,val);
      if (envstruct.env_copy) {
        fd_recycle_environment(envstruct.env_copy);
        envstruct.env_copy=NULL;}
      fd_decref(*vloc); *vloc=FD_VOID;
      i++;}
    fd_decref(choices);
    fd_destroy_rwlock(&(bindings.table_rwlock));
    if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
    return fd_simplify_choice(results);}
}

/* This iterates over a set of choices, evaluating its third subexpression for each value, and
    accumulating those values for which the body returns true.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   It returns the subset of values which pass the body. */
static fdtype filterchoices_handler(fdtype expr,fd_lispenv env)
{
  fdtype results=FD_EMPTY_CHOICE;
  fdtype choices, count_var, var=
    parse_control_spec(expr,&choices,&count_var,env);
  fdtype test_expr=fd_get_arg(expr,2), *vloc=NULL, *iloc=NULL;
  fdtype vars[2], vals[2];
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTED(choices))
    return choices;
  else if (FD_ABORTED(var)) return var;
  else if (FD_EMPTY_CHOICEP(choices))
    return FD_EMPTY_CHOICE;
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  if (FD_VOIDP(count_var)) {
    bindings.schema_length=1;
    vars[0]=var; vals[0]=FD_VOID;
    vloc=&(vals[0]);}
  else {
    bindings.schema_length=2;
    vars[0]=var; vals[0]=FD_VOID; vloc=&(vals[0]);
    vars[1]=count_var; vals[1]=FD_INT(0); iloc=&(vals[1]);}
  bindings.table_schema=vars; bindings.schema_values=vals;
  bindings.schemap_onstack=1;
  fd_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent=env;
  envstruct.env_bindings=(fdtype)(&bindings); envstruct.env_exports=FD_VOID;
  envstruct.env_copy=NULL;
  {
    int i=0; FD_DO_CHOICES(elt,choices) {
      fdtype val=FD_VOID;
      if (envstruct.env_copy) {
        fd_set_value(var,elt,envstruct.env_copy);
        if (iloc) fd_set_value(count_var,FD_INT(i),envstruct.env_copy);}
      else {
        *vloc=elt; fd_incref(elt);
        if (iloc) *iloc=FD_INT(i);}
      val=fasteval(test_expr,&envstruct);
      if (FD_THROWP(val)) {
        fd_decref(choices);
        fd_decref(results);
        if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
        return val;}
      else if (FD_ABORTED(val)) {
        fdtype env;
        if (iloc) env=retenv2(var,elt,count_var,FD_INT(i));
        else env=retenv1(var,elt);
        fd_decref(choices);
        if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
        fd_push_error_context(":FILTER-CHOICES",env);
        return val;}
      else if (FD_FALSEP(val)) {}
      else {
        fd_decref(val); fd_incref(elt);
        FD_ADD_TO_CHOICE(results,elt);}
      if (envstruct.env_copy) {
        fd_recycle_environment(envstruct.env_copy);
        envstruct.env_copy=NULL;}
      fd_decref(*vloc); *vloc=FD_VOID;
      i++;}
    *vloc=FD_VOID;
    fd_decref(choices);
    fd_destroy_rwlock(&(bindings.table_rwlock));
    if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
    return fd_simplify_choice(results);}
}

/* This iterates over the subsets of a choice and is useful for
    dividing a large dataset into smaller chunks for processing.
   It tries to save effort by not incref'ing elements when creating the subset.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   This returns VOID.  */
static fdtype dosubsets_handler(fdtype expr,fd_lispenv env)
{
  fdtype choices, count_var, var, *vloc=NULL, *iloc=NULL;
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  fdtype vars[2], vals[2];
  fdtype control_spec=fd_get_arg(expr,1);
  fdtype bsize; int blocksize;
  if (!((FD_PAIRP(control_spec)) &&
        (FD_SYMBOLP(FD_CAR(control_spec))) &&
        (FD_PAIRP(FD_CDR(control_spec))) &&
        (FD_PAIRP(FD_CDR(FD_CDR(control_spec))))))
    return fd_err(fd_SyntaxError,"dosubsets_handler",NULL,FD_VOID);
  var=FD_CAR(control_spec);
  count_var=fd_get_arg(control_spec,3);
  if (!((FD_VOIDP(count_var)) || (FD_SYMBOLP(count_var))))
    return fd_err(fd_SyntaxError,"dosubsets_handler",NULL,FD_VOID);
  bsize=fd_eval(FD_CADR(FD_CDR(control_spec)),env);
  if (FD_ABORTED(bsize)) return bsize;
  else if (!(FD_FIXNUMP(bsize)))
    return fd_type_error("fixnum","dosubsets_handler",bsize);
  else blocksize=FD_FIX2INT(bsize);
  choices=fd_eval(FD_CADR(control_spec),env);
  if (FD_ABORTED(choices)) return choices;
  else {FD_SIMPLIFY_CHOICE(choices);}
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  if (FD_VOIDP(count_var)) {
    bindings.schema_length=1;
    vars[0]=var; vals[0]=FD_VOID;
    vloc=&(vals[0]);}
  else {
    bindings.schema_length=2;
    vars[0]=var; vals[0]=FD_VOID; vloc=&(vals[0]);
    vars[1]=count_var; vals[1]=FD_INT(0); iloc=&(vals[1]);}
  bindings.table_schema=vars; bindings.schema_values=vals;
  bindings.schemap_onstack=1;
  fd_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent=env;
  envstruct.env_bindings=(fdtype)(&bindings); envstruct.env_exports=FD_VOID;
  envstruct.env_copy=NULL;
  if (FD_EMPTY_CHOICEP(choices)) return FD_VOID;
  else {
    int i=0, n=FD_CHOICE_SIZE(choices), n_blocks=1+n/blocksize;
    int all_atomicp=((FD_CHOICEP(choices)) ?
                     (FD_ATOMIC_CHOICEP(choices)) : (0));
    const fdtype *data=
      ((FD_CHOICEP(choices))?(FD_CHOICE_DATA(choices)):(NULL));
    if ((n%blocksize)==0) n_blocks--;
    while (i<n_blocks) {
      fdtype v; int free_v=0;
      if ((FD_CHOICEP(choices)) && (n_blocks>1)) {
        const fdtype *read=&(data[i*blocksize]), *limit=read+blocksize;
        struct FD_CHOICE *subset=fd_alloc_choice(blocksize); int atomicp=1;
        fdtype *write=((fdtype *)(FD_XCHOICE_DATA(subset)));
        if (limit>(data+n)) limit=data+n;
        if (all_atomicp)
          while (read<limit) *write++=*read++;
        else while (read<limit) {
          fdtype v=*read++;
          if ((atomicp) && (FD_CONSP(v))) atomicp=0;
          *write++=v;}
        {FD_INIT_XCHOICE(subset,write-FD_XCHOICE_DATA(subset),atomicp);}
        v=(fdtype)subset; free_v=1;}
      else v=choices;
      if (envstruct.env_copy) {
        fd_set_value(var,v,envstruct.env_copy);
        if (iloc) fd_set_value(count_var,FD_INT(i),envstruct.env_copy);}
      else {*vloc=v; if (iloc) *iloc=FD_INT(i);}
      {fdtype body=fd_get_body(expr,2);
        FD_DOLIST(subexpr,body) {
          fdtype val=fasteval(subexpr,&envstruct);
          if (FD_THROWP(val)) {
            fd_decref(choices);
            if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
            if (free_v) fd_decref(v);}
          else if (FD_ABORTED(val)) {
            fdtype env;
            if (iloc) env=retenv2(var,v,count_var,FD_INT(i));
            else env=retenv1(var,v);
            fd_decref(choices);
            if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
            if (free_v) fd_decref(v);
            fd_push_error_context(":DO-SUBSETS",env);
            return val;}
          fd_decref(val);}}
      if (envstruct.env_copy) {
        fd_recycle_environment(envstruct.env_copy);
        envstruct.env_copy=NULL;}
      if (free_v) {
        struct FD_CHOICE *subset=(struct FD_CHOICE *)v;
        /* If our temporary choice ended up being stored somewhere, we
           need to incref its elements, which we didn't do earlier.
           This solution will leak in some (hopefully) rare
           multi-threaded cases where a separate thread grabs this
           value before this test but relinquishes it before it's
           done.  In that case, some elements may be incref'd though
           there is no longer a valid pointer to them.  We could
           shorten that window further by checking
           FD_CONS_REFCOUNT(subset) before each incref below, but
           that's probably overkill. */
        if (FD_CONS_REFCOUNT(subset)>1) {
          if (!(FD_XCHOICE_ATOMICP(subset))) {
            const fdtype *scan=FD_XCHOICE_DATA(subset),
              *limit=scan+FD_XCHOICE_SIZE(subset);
            while (scan<limit) {fdtype v=*scan++; fd_incref(v);}}
          fd_decref(v);}
        else u8_free((struct FD_CHOICE *)v);}
      i++;}}
  fd_decref(choices);
  fd_destroy_rwlock(&(bindings.table_rwlock));
  if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
  return FD_VOID;
}

/* SMALLEST and LARGEST */

static int compare_lisp(fdtype x,fdtype y)
{
  fd_ptr_type xtype=FD_PTR_TYPE(x), ytype=FD_PTR_TYPE(y);
  if (xtype == ytype)
    switch (xtype) {
    case fd_fixnum_type: {
      int xval=FD_FIX2INT(x), yval=FD_FIX2INT(y);
      if (xval < yval) return -1;
      else if (xval > yval) return 1;
      else return 0;}
    case fd_oid_type: {
      FD_OID xval=FD_OID_ADDR(x), yval=FD_OID_ADDR(y);
      return FD_OID_COMPARE(xval,yval);}
    default:
      return FD_FULL_COMPARE(x,y);}
  else if (xtype<ytype) return -1;
  else return 1;
}

static fdtype getmagnitude(fdtype val,fdtype magfn)
{
  if (FD_VOIDP(magfn)) return fd_incref(val);
  else {
    fd_ptr_type magtype=FD_PTR_TYPE(magfn);
    switch (magtype) {
    case fd_hashtable_type: case fd_slotmap_type: case fd_schemap_type:
      return fd_get(magfn,val,FD_EMPTY_CHOICE);
    default:
      if (FD_APPLICABLEP(magfn))
        return fd_finish_call(fd_dapply(magfn,1,&val));
      else return fd_get(val,magfn,FD_EMPTY_CHOICE);}}
}

static fdtype smallest_handler(fdtype elts,fdtype magnitude)
{
  fdtype top=FD_EMPTY_CHOICE, top_score=FD_VOID;
  FD_DO_CHOICES(elt,elts) {
    fdtype score=getmagnitude(elt,magnitude);
    if (FD_ABORTED(score)) return score;
    else if (FD_VOIDP(top_score))
      if (FD_EMPTY_CHOICEP(score)) {}
      else {
        top=fd_incref(elt);
        top_score=score;}
    else if (FD_EMPTY_CHOICEP(score)) {}
    else {
      int comparison=compare_lisp(score,top_score);
      if (comparison>0) {}
      else if (comparison == 0) {
        fd_incref(elt);
        FD_ADD_TO_CHOICE(top,elt);
        fd_decref(score);}
      else {
        fd_decref(top); fd_decref(top_score);
        top=fd_incref(elt);
        top_score=score;}}}
  fd_decref(top_score);
  return top;
}

static fdtype largest_handler(fdtype elts,fdtype magnitude)
{
  fdtype top=FD_EMPTY_CHOICE, top_score=FD_VOID;
  FD_DO_CHOICES(elt,elts) {
    fdtype score=getmagnitude(elt,magnitude);
    if (FD_ABORTED(score)) return score;
    else if (FD_VOIDP(top_score))
      if (FD_EMPTY_CHOICEP(score)) {}
      else {
        top=fd_incref(elt);
        top_score=score;}
    else if (FD_EMPTY_CHOICEP(score)) {}
    else {
      int comparison=compare_lisp(score,top_score);
      if (comparison<0) {}
      else if (comparison == 0) {
        fd_incref(elt);
        FD_ADD_TO_CHOICE(top,elt);
        fd_decref(score);}
      else {
        fd_decref(top); fd_decref(top_score);
        top=fd_incref(elt);
        top_score=score;}}}
  fd_decref(top_score);
  return top;
}

/* Choice functions */

static fdtype fail_prim()
{
  return FD_EMPTY_CHOICE;
}

static fdtype choice_prim(int n,fdtype *args)
{
  int i=0; fdtype results=FD_EMPTY_CHOICE;
  while (i < n) {
    fdtype arg=args[i++]; fd_incref(arg);
    FD_ADD_TO_CHOICE(results,arg);}
  return fd_simplify_choice(results);
}

static fdtype qchoice_prim(int n,fdtype *args)
{
  int i=0; fdtype results=FD_EMPTY_CHOICE, presults;
  while (i < n) {
    fdtype arg=args[i++]; fd_incref(arg);
    FD_ADD_TO_CHOICE(results,arg);}
  presults=fd_simplify_choice(results);
  if ((FD_CHOICEP(presults)) || (FD_EMPTY_CHOICEP(presults)))
    return fd_init_qchoice(NULL,presults);
  else return presults;
}

static fdtype qchoicex_prim(int n,fdtype *args)
{
  int i=0; fdtype results=FD_EMPTY_CHOICE, presults;
  while (i < n) {
    fdtype arg=args[i++]; fd_incref(arg);
    FD_ADD_TO_CHOICE(results,arg);}
  presults=fd_simplify_choice(results);
  if (FD_EMPTY_CHOICEP(presults))
    return presults;
  else if (FD_CHOICEP(presults))
    return fd_init_qchoice(NULL,presults);
  else return presults;
}

/* TRY */

static fdtype try_handler(fdtype expr,fd_lispenv env)
{
  fdtype value=FD_EMPTY_CHOICE;
  fdtype clauses=fd_get_body(expr,1);
  FD_DOLIST(clause,clauses) {
    int ipe_state=fd_ipeval_status();
    fd_decref(value);
    value=fd_eval(clause,env);
    if (FD_ABORTED(value)) {
      fd_incref(clause); fd_push_error_context("TRY",clause);
      fd_incref(expr); fd_push_error_context("TRY",expr);
      return value;}
    else if (FD_VOIDP(value)) {
      fd_seterr(fd_VoidArgument,"try_handler",NULL,clause);
      fd_incref(expr); fd_push_error_context("TRY",expr);
      return FD_ERROR_VALUE;}
    else if (!(FD_EMPTY_CHOICEP(value))) return value;
    else if (fd_ipeval_status()!=ipe_state) return value;}
  return value;
}

/* IFEXISTS */

static fdtype ifexists_handler(fdtype expr,fd_lispenv env)
{
  fdtype value_expr=fd_get_arg(expr,1);
  fdtype value=FD_EMPTY_CHOICE;
  if (FD_VOIDP(value_expr))
    return fd_err(fd_SyntaxError,"ifexists_handler",NULL,expr);
  else if (!(FD_EMPTY_LISTP(FD_CDR(FD_CDR(expr)))))
    return fd_err(fd_SyntaxError,"ifexists_handler",NULL,expr);
  else value=fd_eval(value_expr,env);
  if (FD_ABORTED(value)) {
    fd_incref(expr); fd_push_error_context("ifexists_handler",expr);
    return value;}
  if (FD_EMPTY_CHOICEP(value)) return FD_VOID;
  else return value;
}

/* Predicates */

static fdtype emptyp(fdtype x)
{
  if (FD_EMPTY_CHOICEP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype satisfiedp(fdtype x)
{
  if (FD_EMPTY_CHOICEP(x)) return FD_FALSE;
  else if (FD_FALSEP(x)) return FD_FALSE;
  else return FD_TRUE;
}

static fdtype existsp(fdtype x)
{
  if (FD_EMPTY_CHOICEP(x)) return FD_FALSE; else return FD_TRUE;
}

static fdtype singletonp(fdtype x)
{
  fdtype simple=fd_make_simple_choice(x);
  if (FD_EMPTY_CHOICEP(simple)) return FD_FALSE;
  else if (FD_CHOICEP(simple)) {
    fd_decref(simple); return FD_FALSE;}
  else {
    fd_decref(simple); return FD_TRUE;}
}

static fdtype ambiguousp(fdtype x)
{
  fdtype simple=fd_make_simple_choice(x);
  if (FD_EMPTY_CHOICEP(simple)) return FD_FALSE;
  else if (FD_CHOICEP(simple)) {
    fd_decref(simple); return FD_TRUE;}
  else {
    fd_decref(simple); return FD_FALSE;}
}

static fdtype singleton(fdtype x)
{
  fdtype simple=fd_make_simple_choice(x);
  if (FD_EMPTY_CHOICEP(x)) return x;
  else if (FD_CHOICEP(simple)) {
    fd_decref(simple); return FD_EMPTY_CHOICE;}
  else return simple;
}

static fdtype choice_max(fdtype x,fdtype lim)
{
  fdtype simple=fd_make_simple_choice(x);
  if (FD_EMPTY_CHOICEP(x)) return x;
  else if (FD_CHOICEP(simple)) {
    int max_size=fd_getint(lim);
    if (FD_CHOICE_SIZE(simple)>max_size) {
      fd_decref(simple); return FD_EMPTY_CHOICE;}
    else return simple;}
  else return simple;
}

static fdtype simplify(fdtype x)
{
  return fd_make_simple_choice(x);
}

static fdtype qchoicep_handler(fdtype expr,fd_lispenv env)
{
  /* This is a special form because application often reduces
     qchoices to choices. */
  if (!((FD_PAIRP(expr)) && (FD_PAIRP(FD_CDR(expr)))))
    return fd_err(fd_SyntaxError,"qchoice_handler",NULL,expr);
  else {
    fdtype val=fd_eval(FD_CADR(expr),env);
    if (FD_QCHOICEP(val)) {
      fd_decref(val);
      return FD_TRUE;}
    else {
      fd_decref(val);
      return FD_FALSE;}}
}

/* The exists operation */

static int test_exists(struct FD_FUNCTION *fn,int i,int n,fdtype *nd_args,fdtype *d_args);

static fdtype exists_lexpr(int n,fdtype *nd_args)
{
  fdtype *d_args;
  int i=0; while (i<n)
    if (FD_EMPTY_CHOICEP(nd_args[i])) return FD_FALSE;
    else i++;
  d_args=u8_alloc_n((n-1),fdtype);
  {FD_DO_CHOICES(fcn,nd_args[0])
     if (FD_APPLICABLEP(fcn)) {
       struct FD_FUNCTION *f=(fd_function)fcn;
       int retval=test_exists(f,0,n-1,nd_args+1,d_args);
       if (retval<0) return FD_ERROR_VALUE;
       else if (retval) {
         u8_free(d_args);
         return FD_TRUE;}}
     else {
       u8_free(d_args);
       return fd_type_error(_("function"),"exists_lexpr",nd_args[0]);}
  u8_free(d_args);}
  return FD_FALSE;
}

static int test_exists(struct FD_FUNCTION *fn,int i,int n,fdtype *nd_args,fdtype *d_args)
{
  if (i==n) {
    fdtype val=fd_finish_call(fd_dapply((fdtype)fn,n,d_args));
    if ((FD_FALSEP(val)) || (FD_EMPTY_CHOICEP(val))) {
      return 0;}
    else if (FD_ABORTED(val)) {
      return fd_interr(val);}
    fd_decref(val);
    return 1;}
  else if ((FD_CHOICEP(nd_args[i])) || (FD_ACHOICEP(nd_args[i]))) {
    FD_DO_CHOICES(v,nd_args[i]) {
      int retval;
      d_args[i]=v;
      retval=test_exists(fn,i+1,n,nd_args,d_args);
      if (retval!=0) return retval;}
    return 0;}
  else {
    d_args[i]=nd_args[i];
    return test_exists(fn,i+1,n,nd_args,d_args);}
}

static int test_forall
  (struct FD_FUNCTION *fn,int i,int n,fdtype *nd_args,fdtype *d_args);

static fdtype whenexists_handler(fdtype expr,fd_lispenv env)
{
  fdtype to_eval=fd_get_arg(expr,1), value;
  if (FD_VOIDP(to_eval))
    return fd_err(fd_SyntaxError,"whenexists_handler",NULL,expr);
  else value=fd_eval(to_eval,env);
  if (FD_EMPTY_CHOICEP(value)) return FD_VOID;
  else return value;
}

static fdtype forall_lexpr(int n,fdtype *nd_args)
{
  fdtype *d_args;
  int i=0; while (i<n)
    if (FD_EMPTY_CHOICEP(nd_args[i])) return FD_TRUE;
    else i++;
  d_args=u8_alloc_n(n-1,fdtype);
  {FD_DO_CHOICES(fcn,nd_args[0])
     if (FD_APPLICABLEP(fcn)) {
       struct FD_FUNCTION *f=(fd_function)fcn;
       int retval=test_forall(f,0,n-1,nd_args+1,d_args);
       if (retval<0) return FD_ERROR_VALUE;
       else if (retval) {
         u8_free(d_args);
         return FD_TRUE;}}
     else {
       u8_free(d_args);
       return fd_type_error(_("function"),"exists_lexpr",nd_args[0]);}
  u8_free(d_args);}
  return FD_FALSE;
}

static int test_forall(struct FD_FUNCTION *fn,int i,int n,fdtype *nd_args,fdtype *d_args)
{
  if (i==n) {
    fdtype val=fd_finish_call(fd_dapply((fdtype)fn,n,d_args));
    if (FD_FALSEP(val))
      return 0;
    else if (FD_EMPTY_CHOICEP(val))
      return 1;
    else if (FD_ABORTED(val)) {
      return fd_interr(val);}
    fd_decref(val);
    return 1;}
  else if ((FD_CHOICEP(nd_args[i])) || (FD_ACHOICEP(nd_args[i]))) {
    FD_DO_CHOICES(v,nd_args[i]) {
      int retval;
      d_args[i]=v;
      retval=test_forall(fn,i+1,n,nd_args,d_args);
      if (retval==0) return retval;}
    return 1;}
  else {
    d_args[i]=nd_args[i];
    return test_forall(fn,i+1,n,nd_args,d_args);}
}

/* Set operations */

static fdtype union_lexpr(int n,fdtype *args)
{
  return fd_simplify_choice(fd_union(args,n));
}
static fdtype intersection_lexpr(int n,fdtype *args)
{
  return fd_simplify_choice(fd_intersection(args,n));
}
static fdtype difference_lexpr(int n,fdtype *args)
{
  fdtype result=fd_incref(args[0]); int i=1;
  if (FD_EMPTY_CHOICEP(result)) return result;
  else while (i<n) {
    fdtype new=fd_difference(result,args[i]);
    if (FD_EMPTY_CHOICEP(new)) {
      fd_decref(result); return FD_EMPTY_CHOICE;}
    else {fd_decref(result); result=new;}
    i++;}
  return fd_simplify_choice(result);
}

/* Conversion functions */

static fdtype choice2vector(fdtype x)
{
  int i=0, n=FD_CHOICE_SIZE(x);
  fdtype *elts=u8_alloc_n(n,fdtype);
  FD_DO_CHOICES(elt,x) elts[i++]=fd_incref(elt);
  return fd_init_vector(NULL,i,elts);
}

static fdtype choice2list(fdtype x)
{
  fdtype lst=FD_EMPTY_LIST;
  FD_DO_CHOICES(elt,x) lst=fd_conspair(fd_incref(elt),lst);
  return lst;
}

static fdtype get_part(fdtype x,fdtype part)
{
  if (FD_VOIDP(part)) return fd_incref(x);
  else if (FD_APPLICABLEP(part))
    return fd_apply(part,1,&x);
  else if ((FD_TABLEP(part)) && (!(FD_OIDP(part))))
    return fd_get(part,x,FD_EMPTY_CHOICE);
  else if (FD_OIDP(x))
    return fd_frame_get(x,part);
  else if (FD_TABLEP(x))
    return fd_get(x,part,FD_EMPTY_CHOICE);
  else return FD_EMPTY_CHOICE;
}

#define FD_PARTP(x) \
  ((FD_SYMBOLP(x)) || (FD_OIDP(x)) || (FD_VOIDP(x)) || \
   (FD_TABLEP(x)) || (FD_APPLICABLEP(x)))

static fdtype reduce_choice(fdtype fn,fdtype choice,fdtype start,fdtype part)
{
  if ((FD_APPLICABLEP(fn)) && ((FD_VOIDP(part)) || (FD_PARTP(part)))) {
    fdtype state=fd_incref(start);
    FD_DO_CHOICES(each,choice) {
      fdtype items=get_part(each,part);
      if (FD_ABORTED(items)) {
        FD_STOP_DO_CHOICES;
        fd_decref(state);
        return items;}
      else if (FD_VOIDP(state)) state=fd_incref(items);
      else if (FD_EMPTY_CHOICEP(items)) {}
      else if (FD_CHOICEP(items)) {
        FD_DO_CHOICES(item,items) {
          fdtype rail[2], next_state;
          rail[0]=item; rail[1]=state;
          next_state=fd_apply(fn,2,rail);
          if (FD_ABORTED(next_state)) {
            fd_decref(state); fd_decref(items);
            FD_STOP_DO_CHOICES;
            return next_state;}
          fd_decref(state); state=next_state;}}
      else {
        fdtype item=items;
        fdtype rail[2], next_state;
        rail[0]=item; rail[1]=state;
        next_state=fd_apply(fn,2,rail);
        if (FD_ABORTED(next_state)) {
          fd_decref(state); fd_decref(items);
          FD_STOP_DO_CHOICES;
          return next_state;}
        fd_decref(state); state=next_state;}
      fd_decref(items);}
    return state;}
  else if (!(FD_APPLICABLEP(fn)))
    return fd_type_error(_("function"),"reduce_choice",fn);
  else return fd_type_error(_("part"),"reduce_choice",part);
}

static fdtype apply_map(fdtype fn,fdtype val)
{
  if ((FD_VOIDP(fn)) || (FD_FALSEP(fn)))
    return fd_incref(val);
  else if (FD_APPLICABLEP(fn))
    return fd_apply(fn,1,&val);
  else if (FD_TABLEP(fn))
    return fd_get(fn,val,FD_VOID);
  else return fd_type_error(_("map function"),"xreduce_choice",fn);
}

static fdtype xreduce_choice
  (fdtype choice,fdtype reducefn,fdtype mapfn,fdtype start)
{
  if (FD_CHOICEP(reducefn)) {
    fdtype result=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(rfn,reducefn) {
      fdtype v=xreduce_choice(choice,rfn,mapfn,start);
      if (FD_ABORTED(v)) {
        fd_decref(result); FD_STOP_DO_CHOICES;
        return result;}
      else {FD_ADD_TO_CHOICE(result,v);}}
    return result;}
  else if (FD_CHOICEP(mapfn)) {
    fdtype result=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(mfn,mapfn) {
      fdtype v=xreduce_choice(choice,reducefn,mfn,start);
      if (FD_ABORTED(v)) {
        fd_decref(result); FD_STOP_DO_CHOICES;
        return result;}
      else {FD_ADD_TO_CHOICE(result,v);}}
    return result;}
  else if (FD_APPLICABLEP(reducefn)) {
    fdtype state=((FD_VOIDP(start))?(start):(apply_map(mapfn,start)));
    FD_DO_CHOICES(item,choice)
      if (FD_ABORTED(state)) {
        FD_STOP_DO_CHOICES;
        return state;}
      else if (FD_VOIDP(state))
        state=apply_map(mapfn,item);
      else {
        fdtype item_val=apply_map(mapfn,item);
        if (!((FD_VOIDP(item_val)) || (FD_EMPTY_CHOICEP(item_val)))) {
          fdtype rail[2], next_state;
          rail[0]=item_val; rail[1]=state;
          next_state=fd_apply(reducefn,2,rail);
          fd_decref(item_val); fd_decref(state);
          state=next_state;}}
    return state;}
  else return fd_type_error(_("function"),"xreduce_choice",reducefn);
}

static fdtype choicesize_prim(fdtype x)
{
  int n=FD_CHOICE_SIZE(x);
  return FD_INT(n);
}

static fdtype pickone(fdtype x)
{
  fdtype normal=fd_make_simple_choice(x), chosen=FD_EMPTY_CHOICE;
  if (FD_CHOICEP(normal)) {
    int n=FD_CHOICE_SIZE(normal);
    if (n) {
      int i=u8_random(n);
      const fdtype *data=FD_CHOICE_DATA(normal);
      chosen=data[i];
      fd_incref(data[i]); fd_decref(normal);
      return chosen;}
    else return FD_EMPTY_CHOICE;}
  else return normal;
}

static fdtype samplen(fdtype x,fdtype count)
{
  if (FD_FIXNUMP(count)) {
    fdtype normal=fd_make_simple_choice(x);
    int n=FD_CHOICE_SIZE(normal), howmany=fd_getint(count);
    if (!(FD_CHOICEP(normal))) return normal;
    if (n<=howmany) return normal;
    else if (n) {
      struct FD_HASHSET h;
      const fdtype *data=FD_CHOICE_DATA(normal);
      int j=0; fd_init_hashset(&h,n*3,FD_STACK_CONS);
      while (j<howmany) {
        int i=u8_random(n);
        if (fd_hashset_mod(&h,data[i],1)) j++;}
      fd_decref(normal);
      return fd_hashset_elts(&h,1);}
    else return FD_EMPTY_CHOICE;}
  else return fd_type_error("integer","samplen",count);
}

static fdtype pickn(fdtype x,fdtype count,fdtype offset)
{
  if (FD_FIXNUMP(count)) {
    fdtype normal=fd_make_simple_choice(x);
    int n=FD_CHOICE_SIZE(normal), howmany=fd_getint(count), start;
    if (!(FD_CHOICEP(normal))) return normal;
    if (n<=howmany) return normal;
    else if (FD_FIXNUMP(offset)) {
      start=FD_FIX2INT(offset);
      if ((n-start)<howmany) howmany=n-start;}
    else start=u8_random(n-howmany);
    if (n) {
      struct FD_CHOICE *base=
        (fd_consptr(struct FD_CHOICE *,normal,fd_choice_type));
      struct FD_CHOICE *result=fd_alloc_choice(howmany);
      const fdtype *read=FD_XCHOICE_DATA(base)+start;
      fdtype *write=(fdtype *)FD_XCHOICE_DATA(result);
      if (FD_XCHOICE_ATOMICP(base)) {
        memcpy(write,read,sizeof(fdtype)*howmany);
        fd_decref(normal);
        return fd_init_choice(result,howmany,NULL,FD_CHOICE_ISATOMIC);}
      else {
        int atomicp=1; const fdtype *readlim=read+howmany;
        while (read<readlim) {
          fdtype v=*read++;
          if (FD_ATOMICP(v)) *write++=v;
          else {atomicp=0; fd_incref(v); *write++=v;}}
        fd_decref(normal);
        return fd_init_choice(result,howmany,NULL,
                              ((atomicp)?(FD_CHOICE_ISATOMIC):(0)));}}
    else return FD_EMPTY_CHOICE;}
  else return fd_type_error("integer","topn",count);
}

static fdtype sorted_primfn(fdtype choices,fdtype keyfn,int reverse,int lexsort)
{
  if (FD_EMPTY_CHOICEP(choices))
    return fd_init_vector(NULL,0,NULL);
  else if (FD_CHOICEP(choices)) {
    int i=0, n=FD_CHOICE_SIZE(choices), j=0;
    fdtype *vecdata=u8_alloc_n(n,fdtype);
    struct FD_SORT_ENTRY *entries=u8_alloc_n(n,struct FD_SORT_ENTRY);
    FD_DO_CHOICES(elt,choices) {
      fdtype key=_fd_apply_keyfn(elt,keyfn);
      if (FD_ABORTED(key)) {
        int j=0; while (j<i) {fd_decref(entries[j].fd_sortkey); j++;}
        u8_free(entries); u8_free(vecdata);
        return key;}
      entries[i].fd_sortval=elt;
      entries[i].fd_sortkey=key;
      i++;}
    if (lexsort)
      qsort(entries,n,sizeof(struct FD_SORT_ENTRY),_fd_lexsort_helper);
    else qsort(entries,n,sizeof(struct FD_SORT_ENTRY),_fd_sort_helper);
    i=0; j=n-1; if (reverse) while (i < n) {
      fd_decref(entries[i].fd_sortkey);
      vecdata[j]=fd_incref(entries[i].fd_sortval);
      i++; j--;}
    else while (i < n) {
      fd_decref(entries[i].fd_sortkey);
      vecdata[i]=fd_incref(entries[i].fd_sortval);
      i++;}
    u8_free(entries);
    return fd_init_vector(NULL,n,vecdata);}
  else {
    fdtype *vec=u8_alloc_n(1,fdtype);
    vec[0]=fd_incref(choices);
    return fd_init_vector(NULL,1,vec);}
}

static fdtype sorted_prim(fdtype choices,fdtype keyfn)
{
  return sorted_primfn(choices,keyfn,0,0);
}

static fdtype lexsorted_prim(fdtype choices,fdtype keyfn)
{
  return sorted_primfn(choices,keyfn,0,1);
}

static fdtype rsorted_prim(fdtype choices,fdtype keyfn)
{
  return sorted_primfn(choices,keyfn,1,0);
}

/* Selection */

static struct FD_SORT_ENTRY *sort_alloc(int k,struct FD_SORT_ENTRY **ep);

static fdtype select_helper(fdtype choices,fdtype keyfn,
                            int k,int maximize,
                            struct FD_SORT_ENTRY **ep)
{
#define BETTERP(x) ((maximize)?((x)>0):((x)<0))
#define IS_BETTER(x,y) (BETTERP(FD_QCOMPARE((x),(y))))
  fdtype worst=FD_VOID; 
  int worst_off=(maximize)?(0):(k-1);
  if (FD_EMPTY_CHOICEP(choices))
    return FD_EMPTY_CHOICE;
  else if (k==0)
    return FD_EMPTY_CHOICE;
  else if (FD_CHOICEP(choices)) {
    fdtype candidates=fd_make_simple_choice(choices);
    int n=FD_CHOICE_SIZE(candidates);
    if (k>=n) return candidates;
    else {
      struct FD_SORT_ENTRY *entries=sort_alloc(k,ep);
      int k_len=0, sorted=0;
      FD_DO_CHOICES(elt,choices) {
        fdtype key=_fd_apply_keyfn(elt,keyfn);
        if (FD_ABORTED(key)) {
          int j=0; while (j<k_len) {fd_decref(entries[k_len].fd_sortkey); j++;}
          return key;}
        else if (k_len<k) {
          entries[k_len].fd_sortval=elt;
          entries[k_len].fd_sortkey=key;
          k_len++;}
        else {
          if (sorted==0) {
            qsort(entries,k,sizeof(struct FD_SORT_ENTRY),_fd_sort_helper);
            worst=entries[worst_off].fd_sortkey;
            sorted=1;}
          if (IS_BETTER(key,worst)) {
            fd_decref(worst);
            entries[worst_off].fd_sortval=elt;
            entries[worst_off].fd_sortkey=key;
            /* This could be done faster by either by just finding where to insert it,
               either by iterating O(n) or binary search O(log n). */
            qsort(entries,k,sizeof(struct FD_SORT_ENTRY),_fd_sort_helper);
            worst=entries[worst_off].fd_sortkey;}}}
      return FD_VOID;}}
  else {
    fd_incref(choices);
    return choices;}
#undef BETTERP
#undef IS_BETTER
}

static struct FD_SORT_ENTRY *sort_alloc(int k,struct FD_SORT_ENTRY **ep)
{
  struct FD_SORT_ENTRY *entries=u8_alloc_n(k,struct FD_SORT_ENTRY);
  memset(entries,0,sizeof(struct FD_SORT_ENTRY)*k);
  *ep=entries;
  return entries;
}

static fdtype nmax_prim(fdtype choices,fdtype karg,fdtype keyfn)
{
  if (FD_FIXNUMP(karg)) {
    int k=FD_FIX2INT(karg);
    struct FD_SORT_ENTRY *entries=NULL;
    fdtype results=select_helper(choices,keyfn,k,1,&entries);
    if (FD_VOIDP(results)) {
      int i=0;
      results=FD_EMPTY_CHOICE;
      while (i<k) {
        fdtype elt=entries[i].fd_sortval;
        fd_decref(entries[i].fd_sortkey); fd_incref(elt);
        FD_ADD_TO_CHOICE(results,elt);
        i++;}
      u8_free(entries);
      return results;}
    else return results;}
  else return fd_type_error(_("fixnum"),"nmax_prim",karg);
}

static fdtype nmax2vec_prim(fdtype choices,fdtype karg,fdtype keyfn)
{
  if (FD_FIXNUMP(karg)) {
    int k=FD_FIX2INT(karg);
    struct FD_SORT_ENTRY *entries=NULL;
    fdtype results=select_helper(choices,keyfn,k,1,&entries);
    if (FD_VOIDP(results)) {
      fdtype vec=fd_make_vector(k,NULL);
      int i=0;
      while (i<k) {
        fdtype elt=entries[i].fd_sortval;
        int vec_off=k-1-i;
        fd_incref(elt); fd_decref(entries[i].fd_sortkey);
        FD_VECTOR_SET(vec,vec_off,elt);
        i++;}
      u8_free(entries);
      return vec;}
    else return results;}
  else return fd_type_error(_("fixnum"),"nmax_prim",karg);
}

static fdtype nmin_prim(fdtype choices,fdtype karg,fdtype keyfn)
{
  if (FD_FIXNUMP(karg)) {
    int k=FD_FIX2INT(karg);
    struct FD_SORT_ENTRY *entries=NULL;
    fdtype results=select_helper(choices,keyfn,k,0,&entries);
    if (FD_VOIDP(results)) {
      int i=0;
      results=FD_EMPTY_CHOICE;
      while (i<k) {
        fdtype elt=entries[i].fd_sortval;
        fd_decref(entries[i].fd_sortkey); fd_incref(elt);
        FD_ADD_TO_CHOICE(results,elt);
        i++;}
      u8_free(entries);
      return results;}
    else return results;}
  else return fd_type_error(_("fixnum"),"nmax_prim",karg);
}

static fdtype nmin2vec_prim(fdtype choices,fdtype karg,fdtype keyfn)
{
  if (FD_FIXNUMP(karg)) {
    int k=FD_FIX2INT(karg);
    struct FD_SORT_ENTRY *entries=NULL;
    fdtype results=select_helper(choices,keyfn,k,0,&entries);
    if (FD_VOIDP(results)) {
      fdtype vec=fd_make_vector(k,NULL);
      int i=0;
      while (i<k) {
        fd_decref(entries[i].fd_sortkey);
        fd_incref(entries[i].fd_sortval);
        FD_VECTOR_SET(vec,i,entries[i].fd_sortval);
        i++;}
      u8_free(entries);
      return vec;}
    else return results;}
  else return fd_type_error(_("fixnum"),"nmax_prim",karg);
}

/* GETRANGE */

static fdtype getrange_prim(fdtype arg1,fdtype endval)
{
  int start, end; fdtype results=FD_EMPTY_CHOICE;
  if (FD_VOIDP(endval))
    if (FD_FIXNUMP(arg1)) {
      start=0; end=FD_FIX2INT(arg1);}
    else return fd_type_error(_("fixnum"),"getrange_prim",arg1);
  else if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(endval))) {
    start=FD_FIX2INT(arg1); end=FD_FIX2INT(endval);}
  else if (FD_FIXNUMP(endval))
    return fd_type_error(_("fixnum"),"getrange_prim",arg1);
  else return fd_type_error(_("fixnum"),"getrange_prim",endval);
  if (start>end) {int tmp=start; start=end; end=tmp;}
  while (start<end) {
    FD_ADD_TO_CHOICE(results,FD_INT(start)); start++;}
  return results;
}

static fdtype pick_gt_prim(fdtype items,fdtype num,fdtype checktype)
{
  fdtype lower_bound=FD_VOID;
  FD_DO_CHOICES(n,num) {
    if (!(FD_NUMBERP(n)))
      return fd_type_error("number","pick_gt_prim",n);
    else if (FD_VOIDP(lower_bound)) lower_bound=n;
    else if (fd_numcompare(n,lower_bound)<0) lower_bound=n;}
  {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(item,items)
      if (FD_NUMBERP(item))
        if (fd_numcompare(item,lower_bound)>0) {
          fd_incref(item);
          FD_ADD_TO_CHOICE(results,item);}
        else {}
      else if (checktype==FD_TRUE)
        return fd_type_error("number","pick_gt_prim",item);
      else {}
    return results;
  }
}

static fdtype pick_oids_prim(fdtype items)
{
  fdtype results=FD_EMPTY_CHOICE; int no_change=1;
  FD_DO_CHOICES(item,items)
    if (FD_OIDP(item)) {
      FD_ADD_TO_CHOICE(results,item);}
    else no_change=0;
  if (no_change) {
    fd_decref(results);
    return fd_incref(items);}
  else return results;
}

static fdtype pick_syms_prim(fdtype items)
{
  fdtype results=FD_EMPTY_CHOICE; int no_change=1;
  FD_DO_CHOICES(item,items)
    if (FD_SYMBOLP(item)) {
      FD_ADD_TO_CHOICE(results,item);}
    else no_change=0;
  if (no_change) {
    fd_decref(results);
    return fd_incref(items);}
  else return results;
}

static fdtype pick_strings_prim(fdtype items)
{
  /* I don't think we need to worry about getting an ACHOICE here. */
  fdtype results=FD_EMPTY_CHOICE; int no_change=1;
  FD_DO_CHOICES(item,items)
    if (FD_STRINGP(item)) {
      fd_incref(item);
      FD_ADD_TO_CHOICE(results,item);}
    else no_change=0;
  if (no_change) {
    fd_decref(results);
    return fd_incref(items);}
  else return results;
}

static fdtype pick_vecs_prim(fdtype items)
{
  /* I don't think we need to worry about getting an ACHOICE here. */
  fdtype results=FD_EMPTY_CHOICE; int no_change=1;
  FD_DO_CHOICES(item,items)
    if (FD_VECTORP(item)) {
      fd_incref(item);
      FD_ADD_TO_CHOICE(results,item);}
    else no_change=0;
  if (no_change) {
    fd_decref(results);
    return fd_incref(items);}
  else return results;
}

static fdtype pick_pairs_prim(fdtype items)
{
  /* I don't think we need to worry about getting an ACHOICE here. */
  fdtype results=FD_EMPTY_CHOICE; int no_change=1;
  FD_DO_CHOICES(item,items)
    if (FD_PAIRP(item)) {
      fd_incref(item);
      FD_ADD_TO_CHOICE(results,item);}
    else no_change=0;
  if (no_change) {
    fd_decref(results);
    return fd_incref(items);}
  else return results;
}

static fdtype pick_nums_prim(fdtype items)
{
  fdtype results=FD_EMPTY_CHOICE; int no_change=1;
  FD_DO_CHOICES(item,items)
    if (FD_FIXNUMP(item)) {
      FD_ADD_TO_CHOICE(results,item);}
    else if (FD_NUMBERP(item)) {
      fd_incref(item);
      FD_ADD_TO_CHOICE(results,item);}
    else no_change=0;
  if (no_change) {
    fd_decref(results);
    return fd_incref(items);}
  else return results;
}

/* Initialize functions */

FD_EXPORT void fd_init_choicefns_c()
{
  u8_register_source_file(_FILEINFO);

  fd_defspecial(fd_scheme_module,"DO-CHOICES",dochoices_handler);
  fd_defalias(fd_scheme_module,"DO∀","DO-CHOICES");
  fd_defspecial(fd_scheme_module,"FOR-CHOICES",forchoices_handler);
  fd_defalias(fd_scheme_module,"FOR∀","FOR-CHOICES");
  fd_defspecial(fd_scheme_module,"TRY-CHOICES",trychoices_handler);
  fd_defalias(fd_scheme_module,"TRY∀","TRY-CHOICES");
  fd_defspecial(fd_scheme_module,"FILTER-CHOICES",filterchoices_handler);
  fd_defalias(fd_scheme_module,"?∀","FILTER-CHOICES");

  fd_defspecial(fd_scheme_module,"DO-SUBSETS",dosubsets_handler);

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("CHOICE",choice_prim,0)));
  fd_idefn(fd_scheme_module,fd_make_cprim0("FAIL",fail_prim,0));
  {
    fdtype qc_prim=
      fd_make_ndprim(fd_make_cprimn("QCHOICE",qchoice_prim,0));
    fd_idefn(fd_scheme_module,qc_prim);
    fd_store(fd_scheme_module,fd_intern("QC"),qc_prim);
  }

  {
    fdtype qcx_prim=
      fd_make_ndprim(fd_make_cprimn("QCHOICEX",qchoicex_prim,0));
    fd_idefn(fd_scheme_module,qcx_prim);
    fd_store(fd_scheme_module,fd_intern("QCX"),qcx_prim);
  }

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("INTERSECTION",intersection_lexpr,1)));
  fd_defalias(fd_scheme_module,"∩","INTERSECTION");
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("UNION",union_lexpr,1)));
  fd_defalias(fd_scheme_module,"∪","UNION");
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("DIFFERENCE",difference_lexpr,1)));
  fd_defalias(fd_scheme_module,"∖","DIFFERENCE");

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("SMALLEST",smallest_handler,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("LARGEST",largest_handler,1)));

  fd_defspecial(fd_scheme_module,"TRY",try_handler);

  {
    fdtype empty_prim=
      fd_make_ndprim(fd_make_cprim1("EMPTY?",emptyp,1));
    fd_idefn(fd_scheme_module,empty_prim);
    fd_store(fd_scheme_module,fd_intern("FAIL?"),empty_prim);
    fd_defalias(fd_scheme_module,"∄","FAIL?");
  }

  fd_defspecial(fd_scheme_module,"IFEXISTS",ifexists_handler);

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("SATISFIED?",satisfiedp,1)));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("EXISTS?",existsp,1)));
  fd_defalias(fd_scheme_module,"∃","EXISTS?");

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("CHOICE-SIZE",choicesize_prim,1)));
  fd_defalias(fd_scheme_module,"Ω","CHOICE-SIZE");

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("EXISTS",exists_lexpr,1)));
  fd_defalias(fd_scheme_module,"∃?","EXISTS");
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("FORALL",forall_lexpr,1)));
  fd_defalias(fd_scheme_module,"∀?","FORALL");
  fd_defspecial(fd_scheme_module,"WHENEXISTS",whenexists_handler);

  {
    fdtype unique_prim=
      fd_make_ndprim(fd_make_cprim1("UNIQUE?",singletonp,1));
    fd_idefn(fd_scheme_module,unique_prim);
    fd_store(fd_scheme_module,fd_intern("SINGLETON?"),unique_prim);
    fd_store(fd_scheme_module,fd_intern("SOLE?"),unique_prim);}

  fd_defspecial(fd_scheme_module,"QCHOICE?",qchoicep_handler);

  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim1("AMB?",ambiguousp,1)));
  fd_defalias(fd_scheme_module,"AMBIGUOUS?","AMB?");
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("SINGLETON",singleton,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2x("CHOICE-MAX",choice_max,2,
                                          -1,FD_VOID,fd_fixnum_type,FD_VOID)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("SIMPLIFY",simplify,1)));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("CHOICE->VECTOR",choice2vector,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("CHOICE->LIST",choice2list,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim4("REDUCE-CHOICE",reduce_choice,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim4("XREDUCE",xreduce_choice,3)));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PICK-ONE",pickone,1)));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("SORTED",sorted_prim,1)));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("LEXSORTED",lexsorted_prim,1)));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("RSORTED",rsorted_prim,1)));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3x("PICK>",pick_gt_prim,1,
                                          -1,FD_VOID,
                                          -1,FD_INT(0),
                                          -1,FD_FALSE)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PICKOIDS",pick_oids_prim,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PICKNUMS",pick_nums_prim,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PICKSTRINGS",pick_strings_prim,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PICKSYMS",pick_syms_prim,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PICKPAIRS",pick_pairs_prim,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("PICKVECS",pick_vecs_prim,1)));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2("GETRANGE",getrange_prim,1));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2x
                          ("SAMPLE-N",samplen,1,-1,FD_VOID,
                           fd_fixnum_type,FD_SHORT2DTYPE(10))));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3x("PICK-N",pickn,2,
                                          -1,FD_VOID,fd_fixnum_type,FD_VOID,
                                          fd_fixnum_type,FD_VOID)));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3x("PICK-MAX",nmax_prim,2,
                                          -1,FD_VOID,fd_fixnum_type,FD_VOID,
                                          -1,FD_VOID)));
  fd_defalias(fd_scheme_module,"NMAX","PICK-MAX");

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3x("MAX/SORTED",nmax2vec_prim,2,
                                          -1,FD_VOID,fd_fixnum_type,FD_VOID,
                                          -1,FD_VOID)));
  fd_defalias(fd_scheme_module,"NMAX->VECTOR","MAX/SORTED");

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3x("PICK-MIN",nmin_prim,2,
                                          -1,FD_VOID,fd_fixnum_type,FD_VOID,
                                          -1,FD_VOID)));
  fd_defalias(fd_scheme_module,"NMIN","PICK-MIN");

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3x("MIN/SORTED",nmin2vec_prim,2,
                                          -1,FD_VOID,fd_fixnum_type,FD_VOID,
                                          -1,FD_VOID)));
  fd_defalias(fd_scheme_module,"NMIN->VECTOR","MIN/SORTED");

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
