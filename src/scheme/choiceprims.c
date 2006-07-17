/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"

/* Choice iteration */

static fdtype parse_control_spec
  (fdtype expr,fdtype *value,fdtype *iter_var,fd_lispenv env)
{
  fdtype control_expr=fd_get_arg(expr,1);
  if (FD_VOIDP(control_expr))
    return fd_err(fd_TooFewExpressions,NULL,NULL,expr);
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
    if (FD_EXCEPTIONP(val)) {
      *value=val;
      return FD_VOID;}
    *value=val; if (iter_var) *iter_var=ivar;
    return var;}
}

static fdtype retenv1(fdtype var,fdtype val)
{
  struct FD_KEYVAL *keyvals=u8_malloc(sizeof(struct FD_KEYVAL));
  keyvals[0].key=var; keyvals[0].value=fd_incref(val);
  return fd_init_slotmap(NULL,1,keyvals,NULL);
}
static fdtype retenv2(fdtype var,fdtype val,fdtype xvar,fdtype xval)
{
  struct FD_KEYVAL *keyvals=u8_malloc(sizeof(struct FD_KEYVAL)*2);
  keyvals[0].key=var; keyvals[0].value=fd_incref(val);
  keyvals[1].key=xvar; keyvals[1].value=fd_incref(xval);
  return fd_init_slotmap(NULL,2,keyvals,NULL);
}

/* This iterates over a set of choices, evaluating its body for each value.
   It tries to stack allocate as much as possible for locality and convenience sake.
   Note that this treats a non-choice as a choice of one element.
   It returns VOID. */
static fdtype dochoices_handler(fdtype expr,fd_lispenv env)
{
  fdtype choices, count_var, var=
    parse_control_spec(expr,&choices,&count_var,env);
  fdtype body=fd_get_body(expr,2), *vloc=NULL, *iloc=NULL;
  fdtype vars[2], vals[2], inner_env;
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTP(var)) return var;
  else if (FD_ABORTP(choices)) return choices;
  else if (FD_VOIDP(count_var)) {
    bindings.size=1;
    vars[0]=var; vals[0]=FD_VOID;
    vloc=&(vals[0]);}
  else {
    bindings.size=2;
    vars[0]=var; vals[0]=FD_VOID; vloc=&(vals[0]);
    vars[1]=count_var; vals[1]=FD_INT2DTYPE(0); iloc=&(vals[1]);}
  FD_INIT_STACK_CONS(&bindings,fd_schemap_type);
  bindings.flags=FD_SCHEMAP_STACK_SCHEMA;
  bindings.schema=vars; bindings.values=vals;
  u8_init_mutex(&(bindings.lock));
  FD_INIT_STACK_CONS(&envstruct,fd_environment_type);
  envstruct.parent=env;
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  inner_env=(fdtype)(&envstruct);
  if (FD_EMPTY_CHOICEP(choices)) return FD_VOID;
  else if (FD_EXCEPTIONP(choices))
    return choices;
  else {
    int i=0; FD_DO_CHOICES(elt,choices) {
      fd_incref(elt);
      if (envstruct.copy) {
	fd_set_value(var,elt,envstruct.copy);
	if (iloc) fd_set_value(count_var,FD_INT2DTYPE(i),envstruct.copy);}
      else {
	*vloc=elt;
	if (iloc) *iloc=FD_INT2DTYPE(i);}
      {FD_DOLIST(expr,body) {
	fdtype val=fasteval(expr,&envstruct);
	if (FD_ABORTP(val)) {
	  fdtype env;
	  if (iloc) env=retenv2(var,elt,count_var,FD_INT2DTYPE(i));
	  else env=retenv1(var,elt);
	  fd_decref(choices);
	  if (envstruct.copy) fd_recycle_environment(envstruct.copy);
	  return fd_passerr(val,env);}
	fd_decref(val);}}
      if (envstruct.copy) {
	fd_recycle_environment(envstruct.copy);
	envstruct.copy=NULL;}
      fd_decref(*vloc);
      i++;}
    fd_decref(choices);
    if (envstruct.copy) fd_recycle_environment(envstruct.copy);
    return FD_VOID;}
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
  fdtype body=fd_get_body(expr,2), *vloc=NULL, *iloc=NULL;
  fdtype vars[2], vals[2], inner_env;
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  if (FD_VOIDP(count_var)) {
    bindings.size=1;
    vars[0]=var; vals[0]=FD_VOID; vloc=&(vals[0]);}
  else {
    bindings.size=2;
    vars[0]=var; vals[0]=FD_VOID; vloc=&(vals[0]);
    vars[1]=count_var; vals[1]=FD_INT2DTYPE(0); iloc=&(vals[1]);}
  FD_INIT_STACK_CONS(&bindings,fd_schemap_type); 
  bindings.schema=vars; bindings.values=vals;
  bindings.flags=FD_SCHEMAP_STACK_SCHEMA;
  u8_init_mutex(&(bindings.lock));
  FD_INIT_STACK_CONS(&envstruct,fd_environment_type); 
  envstruct.parent=env;  
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  inner_env=(fdtype)(&envstruct);
  if (FD_EMPTY_CHOICEP(choices)) return FD_EMPTY_CHOICE;
  else if (FD_EXCEPTIONP(choices))
    return choices;
  else {
    int i=0; FD_DO_CHOICES(elt,choices) {
      fdtype val=FD_VOID;
      if (envstruct.copy) {
	fd_set_value(var,elt,envstruct.copy);
	if (iloc) fd_set_value(count_var,FD_INT2DTYPE(i),envstruct.copy);}
      else {
	*vloc=elt; fd_incref(elt);
	if (iloc) *iloc=FD_INT2DTYPE(i);}
      {FD_DOLIST(expr,body) {
	fd_decref(val);
	val=fasteval(expr,&envstruct);
	if (FD_ABORTP(val)) {
	  fdtype env;
	  if (iloc) env=retenv2(var,elt,count_var,FD_INT2DTYPE(i));
	  else env=retenv1(var,elt);
	  fd_decref(choices);
	  if (envstruct.copy) fd_recycle_environment(envstruct.copy);
	  return fd_passerr(val,env);}}}
      FD_ADD_TO_CHOICE(results,val);
      if (envstruct.copy) {
	fd_recycle_environment(envstruct.copy);
	envstruct.copy=NULL;}
      fd_decref(*vloc);
      i++;}
    fd_decref(choices);
    if (envstruct.copy) fd_recycle_environment(envstruct.copy);
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
  fdtype vars[2], vals[2], inner_env;
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  if (FD_VOIDP(count_var)) {
    bindings.size=1;
    vars[0]=var; vals[0]=FD_VOID;
    vloc=&(vals[0]);}
  else {
    bindings.size=2;
    vars[0]=var; vals[0]=FD_VOID; vloc=&(vals[0]);
    vars[1]=count_var; vals[1]=FD_INT2DTYPE(0); iloc=&(vals[1]);}
  FD_INIT_STACK_CONS(&bindings,fd_schemap_type);
  bindings.flags=FD_SCHEMAP_STACK_SCHEMA;
  bindings.schema=vars; bindings.values=vals;
  u8_init_mutex(&(bindings.lock));
  FD_INIT_STACK_CONS(&envstruct,fd_environment_type);
  envstruct.parent=env;  
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  inner_env=(fdtype)(&envstruct);
  if (FD_EMPTY_CHOICEP(choices)) return FD_EMPTY_CHOICE;
  else if (FD_EXCEPTIONP(choices))
    return choices;
  else {
    int i=0; FD_DO_CHOICES(elt,choices) {
      fdtype val=FD_VOID;
      if (envstruct.copy) {
	fd_set_value(var,elt,envstruct.copy);
	if (iloc) fd_set_value(count_var,FD_INT2DTYPE(i),envstruct.copy);}
      else {
	*vloc=elt; fd_incref(elt);
	if (iloc) *iloc=FD_INT2DTYPE(i);}
      val=fasteval(test_expr,&envstruct);
      if (FD_ABORTP(val)) {
	fdtype env;
	if (iloc) env=retenv2(var,elt,count_var,FD_INT2DTYPE(i));
	else env=retenv1(var,elt);
	fd_decref(choices);
	if (envstruct.copy) fd_recycle_environment(envstruct.copy);
	return fd_passerr(val,env);}
      else if (FD_FALSEP(val)) {}
      else {
	fd_decref(val);
	FD_ADD_TO_CHOICE(results,fd_incref(elt));}
      if (envstruct.copy) {
	fd_recycle_environment(envstruct.copy);
	envstruct.copy=NULL;}
      fd_decref(*vloc);
      i++;}
    *vloc=FD_VOID;
    fd_decref(choices);
    if (envstruct.copy) fd_recycle_environment(envstruct.copy);
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
  fdtype vars[2], vals[2], inner_env;
  fdtype control_spec=fd_get_arg(expr,1);
  fdtype body=fd_get_body(expr,2);
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
  if (FD_ABORTP(bsize)) return bsize;
  else if (!(FD_FIXNUMP(bsize)))
    return fd_type_error("fixnum","dosubsets_handler",bsize);
  else blocksize=FD_FIX2INT(bsize);
  choices=fd_eval(FD_CADR(control_spec),env);
  if (FD_ABORTP(choices)) return choices;
  else {FD_SIMPLIFY_CHOICE(choices);}
  if (FD_VOIDP(count_var)) {
    bindings.size=1;
    vars[0]=var; vals[0]=FD_VOID;
    vloc=&(vals[0]);}
  else {
    bindings.size=2;
    vars[0]=var; vals[0]=FD_VOID; vloc=&(vals[0]);
    vars[1]=count_var; vals[1]=FD_INT2DTYPE(0); iloc=&(vals[1]);}
  FD_INIT_STACK_CONS(&bindings,fd_schemap_type);
  bindings.flags=FD_SCHEMAP_STACK_SCHEMA;
  bindings.schema=vars; bindings.values=vals;
  u8_init_mutex(&(bindings.lock));
  FD_INIT_STACK_CONS(&envstruct,fd_environment_type);
  envstruct.parent=env;  
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  inner_env=(fdtype)(&envstruct);
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
	FD_INIT_XCHOICE(subset,write-FD_XCHOICE_DATA(subset),atomicp);
	v=(fdtype)subset; free_v=1;}
      else v=choices;
      if (envstruct.copy) {
	fd_set_value(var,v,envstruct.copy);
	if (iloc) fd_set_value(count_var,FD_INT2DTYPE(i),envstruct.copy);}
      else {*vloc=v; if (iloc) *iloc=FD_INT2DTYPE(i);}
      {FD_DOLIST(expr,body) {
	fdtype val=fasteval(expr,&envstruct);
	if (FD_ABORTP(val)) {
	  fdtype env;
	  if (iloc) env=retenv2(var,v,count_var,FD_INT2DTYPE(i));
	  else env=retenv1(var,v);
	  fd_decref(choices);
	  if (envstruct.copy) fd_recycle_environment(envstruct.copy);
	  if (free_v) u8_free((struct FD_CHOICE *)v);
	  return fd_passerr(val,env);}
	fd_decref(val);}}
      if (envstruct.copy) {
	fd_recycle_environment(envstruct.copy);
	envstruct.copy=NULL;}
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
  if (envstruct.copy) fd_recycle_environment(envstruct.copy);
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
      return FDTYPE_COMPARE(x,y);}
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
	return fd_dapply(((struct FD_FUNCTION *)(magfn)),1,&val);
      else return fd_get(val,magfn,FD_EMPTY_CHOICE);}}
}

static fdtype smallest_handler(fdtype elts,fdtype magnitude)
{
  fdtype top=FD_EMPTY_CHOICE, top_score=FD_VOID;
  FD_DO_CHOICES(elt,elts) {
    fdtype score=getmagnitude(elt,magnitude);
    if (FD_EXCEPTIONP(score)) return score;
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
	FD_ADD_TO_CHOICE(top,fd_incref(elt));
	fd_decref(score);}
      else {
	fd_decref(top);
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
    if (FD_EXCEPTIONP(score)) return score;
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
	FD_ADD_TO_CHOICE(top,fd_incref(elt));
	fd_decref(score);}
      else {
	fd_decref(top);
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
    FD_ADD_TO_CHOICE(results,fd_incref(args[i])); i++;}
  return fd_simplify_choice(results);
}

static fdtype qchoice_prim(int n,fdtype *args)
{
  int i=0; fdtype results=FD_EMPTY_CHOICE, presults;
  while (i < n) {
    FD_ADD_TO_CHOICE(results,fd_incref(args[i])); i++;}
  presults=fd_simplify_choice(results);
  if ((FD_CHOICEP(presults)) || (FD_EMPTY_CHOICEP(presults)))
    return fd_init_qchoice(NULL,presults);
  else return presults;
}

/* TRY */

static fdtype try_handler(fdtype expr,fd_lispenv env)
{
  fdtype value=FD_EMPTY_CHOICE;
  FD_DOLIST(clause,FD_CDR(expr)) {
    fd_decref(value);
    value=fd_eval(clause,env);
    if (!(FD_EMPTY_CHOICEP(value))) return value;}
  return value;
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
  d_args=u8_malloc((n-1)*sizeof(fdtype));
  {FD_DO_CHOICES(fcn,nd_args[0])
     if (FD_APPLICABLEP(fcn)) {
       struct FD_FUNCTION *f=(fd_function)fcn;
       int retval=test_exists(f,0,n-1,nd_args+1,d_args);
       if (retval<0) return fd_erreify();
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
    fdtype val=fd_dapply(fn,n,d_args);
    if ((FD_FALSEP(val)) || (FD_EMPTY_CHOICEP(val))) {
      return 0;}
    else if (FD_ABORTP(val)) {
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

static int test_forall(struct FD_FUNCTION *fn,int i,int n,fdtype *nd_args,fdtype *d_args);

static fdtype forall_lexpr(int n,fdtype *nd_args)
{
  fdtype *d_args;
  int i=0; while (i<n)
    if (FD_EMPTY_CHOICEP(nd_args[i])) return FD_TRUE;
    else i++;
  d_args=u8_malloc((n-1)*sizeof(fdtype));
  {FD_DO_CHOICES(fcn,nd_args[0])
     if (FD_APPLICABLEP(fcn)) {
       struct FD_FUNCTION *f=(fd_function)fcn;
       int retval=test_forall(f,0,n-1,nd_args+1,d_args);
       if (retval<0) return fd_erreify();
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
    fdtype val=fd_dapply(fn,n,d_args);
    if ((FD_FALSEP(val)))
      return 0;
    else if (FD_EMPTY_CHOICEP(val))
      return 1;
    else if (FD_ABORTP(val)) {
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
  fdtype *elts=u8_malloc(sizeof(fdtype)*n);
  FD_DO_CHOICES(elt,x) elts[i++]=fd_incref(elt);
  return fd_init_vector(NULL,i,elts);
}

static fdtype choice2list(fdtype x)
{
  int i=0, n=FD_CHOICE_SIZE(x); fdtype lst=FD_EMPTY_LIST;
  FD_DO_CHOICES(elt,x) lst=fd_init_pair(NULL,fd_incref(elt),lst);
  return lst;
}

static fdtype reduce_choice(fdtype fn,fdtype choice,fdtype start)
{
  if (FD_APPLICABLEP(fn)) {
    fdtype state=fd_incref(start);
    FD_DO_CHOICES(item,choice) 
      if (FD_VOIDP(state)) state=fd_incref(item);
      else {
	fdtype rail[2], next_state;
	rail[0]=item; rail[1]=state;
	next_state=fd_apply((fd_function)fn,2,rail);
	if (FD_ABORTP(next_state)) {
	  fd_decref(state); FD_STOP_DO_CHOICES;
	  return next_state;}
	fd_decref(state); state=next_state;}
    return state;}
  else return fd_type_error(_("function"),"reduce_choice",fn);
}

static fdtype apply_map(fdtype fn,fdtype val)
{
  if ((FD_VOIDP(fn)) || (FD_FALSEP(fn)))
    return fd_incref(val);
  else if (FD_APPLICABLEP(fn))
    return fd_apply((fd_function)fn,1,&val);
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
      if (FD_ABORTP(v)) {
	fd_decref(result); FD_STOP_DO_CHOICES;
	return result;}
      else {FD_ADD_TO_CHOICE(result,v);}}
    return result;}
  else if (FD_CHOICEP(mapfn)) {
    fdtype result=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(mfn,mapfn) {
      fdtype v=xreduce_choice(choice,reducefn,mfn,start);
      if (FD_ABORTP(v)) {
	fd_decref(result); FD_STOP_DO_CHOICES;
	return result;}
      else {FD_ADD_TO_CHOICE(result,v);}}
    return result;}
  else if (FD_APPLICABLEP(reducefn)) {
    fdtype state=((FD_VOIDP(start))?(start):(apply_map(mapfn,start)));
    FD_DO_CHOICES(item,choice) 
      if (FD_ABORTP(state)) {
	FD_STOP_DO_CHOICES;
	return state;}
      else if (FD_VOIDP(state))
	state=apply_map(mapfn,item);
      else {
	fdtype item_val=apply_map(mapfn,item);
	if (!((FD_VOIDP(item_val)) || (FD_EMPTY_CHOICEP(item_val)))) {
	  fdtype rail[2], next_state;
	  rail[0]=item_val; rail[1]=state;
	  next_state=fd_apply((fd_function)reducefn,2,rail);
	  fd_decref(item_val); fd_decref(state);
	  state=next_state;}}
    return state;}
  else return fd_type_error(_("function"),"xreduce_choice",reducefn);
}

static fdtype choicesize_prim(fdtype x)
{
  int i=0, n=FD_CHOICE_SIZE(x);
  return FD_INT2DTYPE(n);
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
  else return fd_incref(normal);
}

static fdtype samplen(fdtype x,fdtype count)
{
  if (FD_FIXNUMP(count)) {
    fdtype normal=fd_make_simple_choice(x), chosen=FD_EMPTY_CHOICE;
    int n=FD_CHOICE_SIZE(normal), howmany=fd_getint(count);
    if (!(FD_CHOICEP(normal))) return normal;
    if (n<=howmany) return normal;
    else if (n) {
      struct FD_HASHSET h;
      const fdtype *data=FD_CHOICE_DATA(normal);
      int j=0; fd_init_hashset(&h,n*3);
      while (j<howmany) {
	int i=u8_random(n);
	if (fd_hashset_mod(&h,data[i],1)) j++;}
      fd_decref(normal);
      return fd_hashset_elts(&h,1);}
    else return FD_EMPTY_CHOICE;}
  else return fd_type_error("integer","samplen",count);
}

static fdtype pickn(fdtype x,fdtype count)
{
  if (FD_FIXNUMP(count)) {
    fdtype normal=fd_make_simple_choice(x), chosen=FD_EMPTY_CHOICE;
    int n=FD_CHOICE_SIZE(normal), howmany=fd_getint(count);
    if (!(FD_CHOICEP(normal))) return normal;
    if (n<=howmany) return normal;
    else if (n) {
      struct FD_CHOICE *base=
	(FD_GET_CONS(normal,fd_choice_type,struct FD_CHOICE *));
      struct FD_CHOICE *result=fd_alloc_choice(howmany);
      const fdtype *read=FD_XCHOICE_DATA(base);
      fdtype *write=(fdtype *)FD_XCHOICE_DATA(result);
      int atomicp=1;
      if (FD_XCHOICE_ATOMICP(base)) {
	fd_init_choice(result,howmany,NULL,FD_CHOICE_ISATOMIC);
	memcpy(write,read,sizeof(fdtype)*howmany);}
      else {
	int atomicp=1; const fdtype *readlim=read+howmany;
	while (read<readlim) {
	  fdtype v=*read++; if (FD_ATOMICP(v))
	    *write++=v;
	  else {atomicp=0; fd_incref(v); *write++=v;}}
	fd_init_choice(result,howmany,
		       FD_XCHOICE_DATA(result),
		       FD_CHOICE_ISATOMIC);}
      fd_decref(normal);
      return FDTYPE_CONS(result);}
    else return FD_EMPTY_CHOICE;}
  else return fd_type_error("integer","topn",count);
}

/* SORTED */

struct FD_SORT_ENTRY {
  fdtype value, key;};

static int sort_helper(const void *vx,const void *vy)
{
  const struct FD_SORT_ENTRY *sx=(struct FD_SORT_ENTRY *)vx;
  const struct FD_SORT_ENTRY *sy=(struct FD_SORT_ENTRY *)vy;
  if (sx->key==sy->key) return 0;
  else {
    fd_ptr_type xtype=FD_PTR_TYPE(sx->key), ytype=FD_PTR_TYPE(sy->key);
    if (xtype==ytype)
      if (FD_OIDP(sx->key)) {
	FD_OID xaddr=FD_OID_ADDR(sx->key);
	FD_OID yaddr=FD_OID_ADDR(sy->key);
	return FD_OID_COMPARE(xaddr,yaddr);}
      else if (FD_FIXNUMP(sx->key)) {
	int xval=FD_FIX2INT(sx->key);
	int yval=FD_FIX2INT(sy->key);
	if (xval<yval) return -1; else return 1;}
      else return FDTYPE_COMPARE(sx->key,sy->key);
    else if ((xtype==fd_fixnum_type) || (xtype==fd_bigint_type) ||
	      (xtype==fd_double_type) || (xtype==fd_rational_type) ||
	      (xtype==fd_complex_type))
      if ((ytype==fd_fixnum_type) || (ytype==fd_bigint_type) ||
	  (ytype==fd_double_type) || (ytype==fd_rational_type) ||
	  (ytype==fd_complex_type)) {
	int cmp=fd_numcompare(sx->key,sy->key);
	if (cmp) return cmp;
	else if (xtype<ytype) return -1;
	else return 1;}
      else return -1;
    else if ((ytype==fd_fixnum_type) || (ytype==fd_bigint_type) ||
	     (ytype==fd_double_type) || (ytype==fd_rational_type) ||
	     (ytype==fd_complex_type))
      return 1;
    else if (xtype<ytype) return -1;
    else if (xtype>ytype) return 1;}
}

static fdtype apply_keyfn(fdtype x,fdtype keyfn)
{
  if ((FD_VOIDP(keyfn)) || (FD_EMPTY_CHOICEP(keyfn)))
    return fd_incref(x);
  else if (FD_OIDP(keyfn)) return fd_frame_get(x,keyfn);
  else if (FD_TABLEP(keyfn)) return fd_get(keyfn,x,FD_EMPTY_CHOICE);
  else if (FD_APPLICABLEP(keyfn)) {
    fd_ptr_type keytype=FD_PTR_TYPE(keyfn);
    return fd_applyfns[keytype](keyfn,1,&x);}
  else if (FD_VECTORP(keyfn)) {
    int i=0, len=FD_VECTOR_LENGTH(keyfn);
    fdtype *keyfns=FD_VECTOR_DATA(keyfn);
    fdtype *vecdata=u8_malloc(sizeof(fdtype)*len);
    while (i<len) {vecdata[i]=apply_keyfn(x,keyfns[i]); i++;}
    return fd_init_vector(NULL,len,vecdata);}
  else if ((FD_OIDP(x)) && (FD_SYMBOLP(keyfn)))
    return fd_frame_get(x,keyfn,FD_EMPTY_CHOICE);
  else if (FD_TABLEP(x))
    return fd_get(x,keyfn,FD_EMPTY_CHOICE);
  else return FD_EMPTY_CHOICE;
}

static fdtype sorted_primfn(fdtype choices,fdtype keyfn,int reverse)
{
  if (FD_EMPTY_CHOICEP(choices))
    return fd_init_vector(NULL,0,NULL);
  else if (FD_CHOICEP(choices)) {
    int i=0, n=FD_CHOICE_SIZE(choices), j=0;
    fdtype *vecdata=u8_malloc(sizeof(fdtype)*n);
    struct FD_SORT_ENTRY *sentries=
      u8_malloc(sizeof(struct FD_SORT_ENTRY)*n), *scan=sentries;
    FD_DO_CHOICES(elt,choices) {
      fdtype value=apply_keyfn(elt,keyfn);
      if (FD_ABORTP(value)) {
	int j=0; while (j<i) {fd_decref(sentries[j].value); j++;}
	u8_free(sentries); u8_free(vecdata);
	return value;}
      sentries[i].value=elt;
      sentries[i].key=value;
      i++;}
    qsort(sentries,n,sizeof(struct FD_SORT_ENTRY),sort_helper);
    i=0; j=n-1; if (reverse) while (i < n) {
      fd_decref(sentries[i].key);
      vecdata[j]=fd_incref(sentries[i].value);
      i++; j--;}
    else while (i < n) {
      fd_decref(sentries[i].key);
      vecdata[i]=fd_incref(sentries[i].value);
      i++;}
    u8_free(sentries);
    return fd_init_vector(NULL,n,vecdata);}
  else {
    fdtype *vec=u8_malloc(sizeof(fdtype));
    vec[0]=fd_incref(choices);
    return fd_init_vector(NULL,1,vec);}
}

static fdtype sorted_prim(fdtype choices,fdtype keyfn)
{
  return sorted_primfn(choices,keyfn,0);
}

static fdtype rsorted_prim(fdtype choices,fdtype keyfn)
{
  return sorted_primfn(choices,keyfn,1);
}

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
    FD_ADD_TO_CHOICE(results,FD_INT2DTYPE(start)); start++;}
  return results;
}

/* Initialize functions */

FD_EXPORT void fd_init_choicefns_c()
{
  fd_register_source_file(versionid);

  fd_defspecial(fd_scheme_module,"DO-CHOICES",dochoices_handler);
  fd_defspecial(fd_scheme_module,"FOR-CHOICES",forchoices_handler);
  fd_defspecial(fd_scheme_module,"FILTER-CHOICES",filterchoices_handler);
  
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

  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprimn("INTERSECTION",intersection_lexpr,1)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprimn("UNION",union_lexpr,1)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprimn("DIFFERENCE",difference_lexpr,1)));


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
  }

  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim1("SATISFIED?",satisfiedp,1)));

  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim1("EXISTS?",existsp,1)));
  
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim1("CHOICE-SIZE",choicesize_prim,1)));

  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprimn("EXISTS",exists_lexpr,1)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprimn("FORALL",forall_lexpr,1)));


  {
    fdtype unique_prim=
      fd_make_ndprim(fd_make_cprim1("UNIQUE?",singletonp,1));
    fd_idefn(fd_scheme_module,unique_prim);
    fd_store(fd_scheme_module,fd_intern("SINGLETON?"),unique_prim);}

  fd_defspecial(fd_scheme_module,"QCHOICE?",qchoicep_handler);

  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim1("AMBIGUOUS?",ambiguousp,1)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim1("SINGLETON",singleton,1)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim1("SIMPLIFY",simplify,1)));

  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim1("CHOICE->VECTOR",choice2vector,1)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim1("CHOICE->LIST",choice2list,1)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim3("REDUCE-CHOICE",reduce_choice,2)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim4("XREDUCE",xreduce_choice,3)));

  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim1("PICK-ONE",pickone,1)));

  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim2("SORTED",sorted_prim,1)));

  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim2("RSORTED",rsorted_prim,1)));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim2("GETRANGE",getrange_prim,1));

  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim2("SAMPLE-N",samplen,2)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim2("PICK-N",pickn,2)));
}


/* The CVS log for this file
   $Log: choicefns.c,v $
   Revision 1.67  2006/02/11 18:11:35  haase
   Internal rename

   Revision 1.66  2006/02/07 03:15:08  haase
   Improved handling of xreduce

   Revision 1.65  2006/02/05 13:55:41  haase
   Cleaned up leak in PICK-N

   Revision 1.64  2006/02/01 15:59:54  haase
   Fixed typo in mapreducechoice name change

   Revision 1.63  2006/02/01 15:57:31  haase
   Added XREDUCE for choices, combining a combining function and a map function

   Revision 1.62  2006/01/31 20:51:44  haase
   Fixed bug in new PICK-N

   Revision 1.61  2006/01/31 12:56:09  haase
   AMade PICK-N be fast, picking the top N choice elements, while keeping SAMPLE-N for random sampling

   Revision 1.60  2006/01/31 03:15:35  haase
   Added REDUCE-CHOICE

   Revision 1.59  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.58  2006/01/18 04:33:06  haase
   Fixed various iteration binding bugs

   Revision 1.57  2006/01/17 19:06:39  haase
   Whitespace changes

   Revision 1.56  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.55  2006/01/02 19:59:30  haase
   Fixed erroneous identity of iterative environments

   Revision 1.54  2006/01/02 19:11:27  haase
   Added DO-SUBSETS

   Revision 1.53  2005/12/22 19:15:50  haase
   Added more comprehensive environment recycling

   Revision 1.52  2005/12/20 19:05:53  haase
   Switched to u8_random and added RANDOMSEED config variable

   Revision 1.51  2005/12/17 17:34:17  haase
   Made SORTED lump numbers together and sort by magnitude

   Revision 1.50  2005/11/21 23:03:02  haase
   Added FORALL

   Revision 1.49  2005/08/29 20:48:27  haase
   Added new EXISTS primitive which does choice iteration and returns true when the first call returns true (not false or fail)

   Revision 1.48  2005/08/27 22:41:27  haase
   Made SORTED return errors returned by keyfns

   Revision 1.47  2005/08/25 20:35:55  haase
   Added RSORTED

   Revision 1.46  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.45  2005/08/07 05:15:59  haase
   Make sorted return an empty vector for an empty choice

   Revision 1.44  2005/07/21 00:19:44  haase
   Fixed some error passing bugs

   Revision 1.43  2005/06/27 16:54:30  haase
   Thread function improvements and addition of THREADJOIN primitive

   Revision 1.42  2005/06/20 13:56:59  haase
   Fixes to regularize CONS header initialization

   Revision 1.41  2005/06/01 13:07:55  haase
   Fixes for less forgiving compilers

   Revision 1.40  2005/05/30 18:04:43  haase
   Removed legacy numeric plugin layer

   Revision 1.39  2005/05/30 17:49:59  haase
   Distinguished FD_QCOMPARE and FDTYPE_COMPARE

   Revision 1.38  2005/05/30 16:07:55  haase
   Made SORTED respect absolute number and OID ordering

   Revision 1.37  2005/05/29 18:27:08  haase
   Defined EXISTS which is the identify for everything but the empty choice, which becomes false

   Revision 1.36  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.35  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.34  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.33  2005/04/14 16:23:57  haase
   Fix sorted to work on sorting OIDs with procedural key functions

   Revision 1.32  2005/04/12 16:07:08  haase
   Fixed choice iterators to handle environments which become dynamic

   Revision 1.31  2005/04/12 01:24:58  haase
   Added SATISFIED? and made DIFFERENCE be a lexpr

   Revision 1.30  2005/04/12 01:15:45  haase
   Added fd_overlapp and its applications

   Revision 1.29  2005/04/08 04:46:30  haase
   Improvements to backtrace accumulation

   Revision 1.28  2005/04/07 19:25:03  haase
   Fix schemap inits for stack schemas

   Revision 1.27  2005/04/07 19:11:30  haase
   Made backtraces include environments, fixed some minor GC issues

   Revision 1.26  2005/04/01 15:12:07  haase
   Cleanup from exports reimplementation

   Revision 1.25  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.24  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.23  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.22  2005/02/25 02:17:09  haase
   Added (FAIL) primitive and UNIQUE? predicate

   Revision 1.21  2005/02/24 19:45:35  haase
   Fix to let largest/smallest use compound procedures

   Revision 1.20  2005/02/17 02:39:16  haase
   Added init of the sorted field in filterchoices_handler

   Revision 1.19  2005/02/13 23:54:26  haase
   Fixed definition of sorted

   Revision 1.18  2005/02/13 23:27:21  haase
   Added first version of SORTED primitive

   Revision 1.17  2005/02/11 03:50:12  haase
   Added some eval branch predictions

   Revision 1.16  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
