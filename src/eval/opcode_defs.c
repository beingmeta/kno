#include "../apply/apply_cprims.h"

static lispval handle_special_opcode(lispval opcode,lispval args,lispval expr,
                                     kno_lexenv env,
                                     kno_stack _stack,
				     int tail);
static lispval get_headval(lispval head,kno_lexenv env,kno_stack eval_stack,
			      int *gc_headval);
KNO_FASTOP lispval op_eval(kno_stack _stack,lispval x,kno_lexenv env,int tail);
static lispval handle_table_opcode(lispval opcode,lispval expr,
                                   kno_lexenv env,
                                   kno_stack _stack);
static lispval handle_numeric_opcode(lispval opcode,lispval expr,
				     kno_lexenv env,
				     kno_stack _stack);
static lispval nd1_call(lispval opcode,lispval arg1);
static lispval nd2_call(lispval opcode,lispval arg1,lispval arg2);
static lispval d1_call(lispval opcode,lispval arg1);
static lispval d2_call(lispval opcode,lispval arg1,lispval arg2);
static lispval d1_loop(lispval opcode,lispval arg1);
static lispval d2_loop(lispval opcode,lispval arg1,lispval arg2);

lispval op_eval_expr(struct KNO_STACK *eval_stack,
		     lispval head,lispval expr,kno_lexenv env,
		     int tail)
{
  lispval result = KNO_VOID;
  int gc_head = 0, n_args = -1;
  u8_string label = NULL;
  if (head == KNO_SOURCEREF_OPCODE) {
    expr = handle_sourcerefs(expr,eval_stack);
    if (KNO_PAIRP(expr)) {
      head = KNO_CAR(expr);
      label = head_label(head);}
    else return op_eval(eval_stack,expr,env,tail);}
  lispval arg_exprs = KNO_CDR(expr), headval =
    (KNO_OPCODEP(head)) ? (head) :
    (KNO_FCNIDP(head)) ? kno_fcnid_ref(head) :
    (KNO_APPLICABLEP(head)) ? (head) :
    (get_headval(head,env,eval_stack,&gc_head));
  if (KNO_FCNIDP(headval)) headval = kno_fcnid_ref(headval);
  if (ABORTED(headval))
    return headval;
  else if (CHOICEP(headval))
    return eval_apply("choice",headval,n_args,arg_exprs,env,eval_stack,tail);
  else if (gc_head) {
    KNO_ADD_TO_CHOICE(eval_stack->stack_vals,headval);}
  else NO_ELSE;
  if (KNO_EMPTYP(headval)) return KNO_EMPTY;
  kno_lisp_type headtype = KNO_TYPEOF(headval);
  kno_function f = (KNO_FUNCTION_TYPEP(headtype)) ?
    ((kno_function)headval) :
    (NULL);
  int isndop = (f) ? (f->fcn_call & KNO_FCN_CALL_NDOP) :
    (KNO_OPCODEP(headval)) ?
    (!((KNO_D1_OPCODEP(headval)) ||
       (KNO_D2_OPCODEP(headval)) ) ) :
    (0);
  lispval restore_op = eval_stack->stack_op;
  kno_lexenv restore_env = eval_stack->stack_env;
  u8_string restore_label = eval_stack->stack_label;
  if (env == restore_env) restore_env = NULL;
  eval_stack->stack_op = restore_op;
  if (restore_env)
    eval_stack->stack_env = restore_env;
  eval_stack->stack_label = restore_label;
  switch (headtype) {
  case kno_evalfn_type: {
    struct KNO_EVALFN *handler = (kno_evalfn)headval;
    KNO_NEW_STACK(eval_stack,kno_evalstack_type,handler->evalfn_name,expr);
    /* These are evalfns which do all the evaluating themselves */
    result = handler->evalfn_handler(expr,env,_stack);
    kno_pop_stack(_stack);
    return result;}
  case kno_macro_type: {
    /* These expand into expressions which are
       then evaluated. */
    struct KNO_MACRO *macrofn=
      kno_consptr(struct KNO_MACRO *,headval,kno_macro_type);
    eval_stack->stack_type="macro";
    lispval xformer = macrofn->macro_transformer;
    lispval new_expr = kno_dcall(eval_stack,xformer,1,&expr);
    if (KNO_ABORTED(new_expr)) {
      u8_string show_name = macrofn->macro_name;
      if (show_name == NULL) show_name = label;
      return kno_err(kno_SyntaxError,_("macro expansion"),show_name,new_expr);}
    result = kno_stack_eval(new_expr,env,eval_stack,tail);
    kno_decref(new_expr);
    eval_stack->stack_op = restore_op;
    if (restore_env) eval_stack->stack_env = restore_env;
    eval_stack->stack_label = restore_label;
    return result;}
  case kno_opcode_type: {
    if (KNO_SPECIAL_OPCODEP(headval))
      return handle_special_opcode(headval,arg_exprs,expr,env,eval_stack,tail);
    else if (KNO_TABLE_OPCODEP(headval))
      return handle_table_opcode(headval,expr,env,eval_stack);
    else if (KNO_NUMERIC_OPCODEP(headval))
      return handle_numeric_opcode(headval,expr,env,eval_stack);
    else if (KNO_APPLY_OPCODEP(headval)) {
      lispval fn_expr = pop_arg(arg_exprs);
      int gc_old_head = gc_head;
      lispval fn_val = (KNO_OPCODEP(fn_expr)) ? (fn_expr) :
	(KNO_FCNIDP(fn_expr)) ? (kno_fcnid_ref(fn_expr)) :
	(KNO_APPLICABLEP(fn_expr)) ? (fn_expr) :
	(get_headval(fn_expr,env,eval_stack,&gc_head));
      if (KNO_ABORTED(fn_val)) return fn_val;
      else if (headval == KNO_APPLY_N_OPCODE) {
	lispval arg_count = pop_arg(arg_exprs);
	int argc = (KNO_FIXNUMP(arg_count)) ? (KNO_FIX2INT(arg_count)) : (-1);
	if (argc < 0)
	  return kno_err(kno_SyntaxError,"apply_op",NULL,expr);
	else n_args = argc;}
      else n_args = ((KNO_IMMEDIATE_DATA(headval))&0xF);
      if (gc_old_head) {kno_decref(head);}
      headval = head = fn_val;
      if (KNO_FUNCTIONP(fn_val)) f = (kno_function) fn_val;
      headtype = KNO_TYPEOF(fn_val);
      isndop = (f) ? (f->fcn_call & KNO_FCN_CALL_NDOP) : (0);}
    else NO_ELSE;
    break;}
  default: {
    if ( (f == NULL) && (!(KNO_APPLICABLEP(headval))) )
      return kno_err(kno_NotAFunction,"op_eval",NULL,headval);}}
  /* When we get there, we're evaluating args and applying headval */
  if (n_args < 0) n_args = list_length(KNO_CDR(expr));
  if ( (f) && (f->fcn_name) )
    eval_stack->stack_label=f->fcn_name;
  int call_width = (n_args<0) ? (0) : (n_args);
  if ( (f) && (f->fcn_call_width > call_width) )
    call_width = f->fcn_call_width;
  lispval args[call_width], free_args[call_width];
  int nd_args = 0, qc_args = 0, gc_args = 0;
  int i=0; while (i < n_args) {
    lispval arg_expr = pop_arg(arg_exprs);
    lispval arg_value = op_eval(eval_stack,arg_expr,env,0);
    if ( (KNO_ABORTED(arg_value)) ||
	 ( (!(isndop)) && (KNO_EMPTYP(arg_value)) ) ) {
      /* Prune the call */
      if (gc_args) {
	int j = 0; while (j<gc_args) {kno_decref(free_args[j]); j++;}}
      return arg_value;}
    if (CONSP(arg_value)) {
      if (KNO_PRECHOICEP(arg_value))
	arg_value = kno_simplify_choice(arg_value);
      if (KNO_MALLOCD_CONSP(arg_value)) {
	free_args[gc_args] = arg_value;
	gc_args++;}
      if (KNO_CHOICEP(arg_value)) nd_args++;
      if (KNO_QCHOICEP(arg_value)) qc_args++;}
    args[i++] = arg_value;}
  while (i < call_width) args[i++] = KNO_VOID;
  if (KNO_OPCODEP(headval)) {
    if (KNO_ND1_OPCODEP(headval)) {
      if (PRED_FALSE(n_args != 1))
	result = kno_err(kno_TooManyArgs,"op_eval_expr",NULL,expr);
      else result = nd1_call(headval,args[0]);}
    else if (KNO_D1_OPCODEP(headval))
      if (nd_args)
	result = d1_loop(headval,args[0]);
      else result = d1_call(headval,args[0]);
    else if (KNO_ND2_OPCODEP(headval))
      result = nd2_call(headval,args[0],args[1]);
    else if (KNO_D2_OPCODEP(headval))
      if (nd_args)
	result = d2_loop(headval,args[0],args[1]);
      else result = d2_call(headval,args[0],args[1]);
    else result = kno_err("BadOpcode","op_eval_expr",NULL,expr);}
  else if (KNO_FUNCTIONP(headval))
    if ( (!nd_args) || (isndop) ) {
      /* This is where we might dispatch directly for C or lambdas  */
      if ( (0) && (headtype == kno_cprim_type) ) {
	lispval argbuf[call_width];
	result = cprim_call(f->fcn_name,(kno_cprim)f,
			    n_args,args,call_width,argbuf,
			    eval_stack);}
      else result = kno_dcall(eval_stack,headval,n_args,args);}
    else result = kno_ndcall(eval_stack,headval,n_args,args);
  else if (KNO_APPLICABLEP(headval))
    result = kno_dcall(eval_stack,headval,n_args,args);
  else result = kno_err(kno_NotAFunction,"op_eval",NULL,expr);
  eval_stack->stack_op = restore_op;
  if (restore_env) eval_stack->stack_env = restore_env;
  eval_stack->stack_label = restore_label;
  if (gc_args) {
    int i = 0; while (i<gc_args) {
      lispval arg = free_args[i++];
      kno_decref(arg);}}
  return result;
}
      
static lispval unbound_error(lispval sym,kno_lexenv env)
{
  /* TODO: Add something to identify env? */
  return kno_err(kno_UnboundIdentifier,"kno_eval",KNO_SYMBOL_NAME(sym),sym);
}

KNO_FASTOP lispval op_eval(kno_stack _stack,lispval x,kno_lexenv env,int tail)
{
  switch (KNO_PTR_MANIFEST_TYPE(x)) {
  case kno_oid_ptr_type: case kno_fixnum_ptr_type:
    return x;
  case kno_immediate_ptr_type: {
    kno_lisp_type itype = KNO_IMMEDIATE_TYPE(x);
    switch (itype) {
    case kno_lexref_type:
      return kno_lexref(x,env);
    case kno_symbol_type: {
      lispval val = kno_symeval(x,env);
      if (KNO_EXPECT_FALSE(KNO_VOIDP(val)))
	return unbound_error(x,env);
      else return val;}
    default:
      return x;}}
  default: {
    kno_lisp_type ctype = KNO_CONSPTR_TYPE(x);
    switch (ctype) {
    case kno_pair_type:
      return op_eval_expr(_stack,KNO_CAR(x),x,env,tail);
    case kno_choice_type:
      return choice_eval(x,env,_stack,tail);
    case kno_schemap_type: {
      lispval result = VOID;
      KNO_PUSH_STACK(eval_stack,kno_evalstack_type,NULL,x);
      result = schemap_eval(x,env,eval_stack);
      kno_pop_stack(eval_stack);
      return result;}
    case kno_slotmap_type:
      return kno_deep_copy(x);
    default:
      return kno_incref(x);}}}
}

KNO_FASTOP lispval op_eval_body(lispval body,kno_lexenv env,kno_stack stack,
				u8_context cxt,u8_string label,
				int tail)
{
  if (KNO_EMPTY_LISTP(body))
    return KNO_VOID;
  else if (!(KNO_PAIRP(body)))
    return kno_err(kno_SyntaxError,
                   ( (cxt) && (label) ) ? (cxt) :
                   ((u8_string)"eval_inner_body"),
                   (label) ? (label) : (cxt) ? (cxt) : (NULL),
                   body);
  lispval scan = body;
  while (PAIRP(scan)) {
      lispval subex = pop_arg(scan);
      if (PAIRP(scan)) {
	lispval v = op_eval(stack,subex,env,0);
	if (KNO_ABORTED(v))
          return v;
	else kno_decref(v);}
      else if (KNO_EMPTY_LISTP(scan))
	return op_eval(stack,subex,env,tail);
      else return kno_err(kno_SyntaxError,
			  ( (cxt) && (label) ) ? (cxt) :
			  ((u8_string)"eval_inner_body"),
			  (label) ? (label) : (cxt) ? (cxt) : (NULL),
			  body);}
  return KNO_VOID;
}

/* Reduce ops */

typedef lispval (*reducefn)(lispval state,lispval arg,int *done);

KNO_FASTOP lispval reduce_op(kno_stack stack,
			     lispval exprs,
			     kno_lexenv env,
			     lispval state,
			     lispval prune,
			     reducefn fn)
{
  int done = 0; kno_incref(state);
  while ( (done == 0) && (KNO_PAIRP(exprs)) ) {
    lispval expr = pop_arg(exprs);
    lispval arg = op_eval(stack,expr,env,0);
    if (KNO_ABORTED(arg)) {
      kno_decref(state);
      return arg;}
    else NO_ELSE;
    lispval reduced = fn(state,arg,&done);
    if (KNO_ABORTP(reduced)) {
      kno_decref(state);
      kno_decref(arg);
      return reduced;}
    if ( (KNO_EMPTYP(reduced)) &&
	 (!(KNO_VOIDP(prune))) ) {
      kno_decref(state);
      state = prune;
      break;}
    else if (state != reduced) {
      lispval prev_state = state;
      state = reduced;
      kno_decref(prev_state);}
    else NO_ELSE;}
  return kno_simplify_choice(state);
}

KNO_FASTOP lispval nd_reduce_op(kno_stack stack,
				lispval exprs,
				kno_lexenv env,
				lispval state,
				reducefn fn)
{
  lispval result = kno_incref(state), arg = KNO_VOID;
  int done = 0;
  while ( (done == 0) && (KNO_PAIRP(exprs)) ) {
    lispval expr = pop_arg(exprs);
    arg = op_eval(stack,expr,env,0);
    if (KNO_ABORTED(arg)) {
      kno_decref(state);
      return arg;}
    else if ( (KNO_CHOICEP(state)) || (KNO_CHOICEP(arg)) ) {
      lispval new_state = KNO_EMPTY;
      if (KNO_CHOICEP(state))
	if (KNO_CHOICEP(arg)) {
	  KNO_DO_CHOICES(state_x,state) {
	    KNO_DO_CHOICES(arg_x,arg) {
	      lispval reduced = fn(state_x,arg_x,&done);
	      if (ABORTED(reduced)) {
		kno_decref(new_state);
		KNO_STOP_DO_CHOICES;
		result = reduced;
		goto error_exit;}
	      else {KNO_ADD_TO_CHOICE(new_state,reduced);}}}}
	else {
	  KNO_DO_CHOICES(state_x,state) {
	    lispval reduced = fn(state_x,arg,&done);
	    if (ABORTED(reduced)) {
	      kno_decref(new_state);
	      KNO_STOP_DO_CHOICES;
	      result = reduced;
	      goto error_exit;}
	    else {KNO_ADD_TO_CHOICE(new_state,reduced);}}}
      else {
	KNO_DO_CHOICES(arg_x,arg) {
	  lispval reduced = fn(state,arg_x,&done);
	  if (ABORTED(reduced)) {
	    kno_decref(new_state);
	    KNO_STOP_DO_CHOICES;
	    result = reduced;
	    goto error_exit;}
	  else {KNO_ADD_TO_CHOICE(new_state,reduced);}}}
      kno_decref(state);
      state = kno_simplify_choice(new_state);}
    else {
      lispval reduced = fn(state,arg,&done);
      if (KNO_ABORTP(reduced)) {
	kno_decref(state);
	kno_decref(arg);
	return reduced;}
      else if (state != reduced) {
	lispval prev_state = state;
	state = kno_simplify_choice(reduced);
	kno_decref(prev_state);}
      else NO_ELSE;}}
  return kno_simplify_choice(state);
 error_exit:
  kno_decref(state);
  kno_decref(arg);
  return result;
}

/* Reduce ops */

KNO_FASTOP lispval try_reduce(lispval state,lispval step,int *done)
{
  if (!(KNO_EMPTYP(step))) *done = 1;
  return step;
}

static lispval try_op(lispval exprs,kno_lexenv env,kno_stack stack,int tail)
{
  return reduce_op(stack,exprs,env,
		   KNO_EMPTY_CHOICE,KNO_VOID,
		   try_reduce);
}

KNO_FASTOP lispval union_reduce(lispval state,lispval step,int *done)
{
  if (VOIDP(state)) return step;
  else if (EMPTYP(step)) return state;
  else {
    if (!(KNO_PRECHOICEP(state))) kno_incref(state);
    KNO_ADD_TO_CHOICE(state,step);
    return state;}
}

static lispval union_op(lispval exprs,kno_lexenv env,kno_stack stack)
{
  return reduce_op(stack,exprs,env,
		   KNO_EMPTY_CHOICE,KNO_VOID,
		   union_reduce);
}

KNO_FASTOP lispval intersect_reduce(lispval state,lispval step,int *done)
{
  if (EMPTYP(step)) {
    *done = 1;
    return KNO_EMPTY;}
  else if (KNO_VOIDP(state))
    return step;
  else {
    lispval combine[2] = { state, step };
    lispval intersection = kno_intersection(combine,2);
    if (KNO_EMPTYP(intersection)) {
      *done=1;
      return intersection;}
    else if (KNO_CHOICE_SIZE(intersection) == KNO_CHOICE_SIZE(state)) {
      kno_decref(intersection);
      return state;}
    else return intersection;}
}

static lispval intersect_op(lispval exprs,kno_lexenv env,kno_stack stack)
{
  return reduce_op(stack,exprs,env,
		   KNO_VOID,KNO_EMPTY_CHOICE,
		   intersect_reduce);
}

KNO_FASTOP lispval difference_reduce(lispval state,lispval step,int *done)
{
  if (EMPTYP(state)) {
    *done = 1;
    return state;}
  else if (VOIDP(state))
    return step;
  else if (EMPTYP(step))
    return state;
  else {
    lispval diff = kno_difference(state,step);
    if (KNO_EMPTYP(diff)) {
      *done=1;
      return diff;}
    else if (KNO_CHOICE_SIZE(diff) == KNO_CHOICE_SIZE(state)) {
      kno_decref(diff);
      return state;}
    else return diff;}
}

static lispval difference_op(lispval exprs,kno_lexenv env,kno_stack stack)
{
  return reduce_op(stack,exprs,env,
		   KNO_VOID,KNO_EMPTY_CHOICE,
		   difference_reduce);
}

KNO_FASTOP lispval and_reduce(lispval state,lispval step,int *done)
{
  /* Evaluate clauses until you get an error or a false/empty value */
  if ( (FALSEP(step)) || (EMPTYP(step)) ) {
    *done=1;
    return step;}
  else return step;
}

static lispval and_op(lispval exprs,kno_lexenv env,kno_stack stack,int tail)
{
  return reduce_op(stack,exprs,env,KNO_TRUE,KNO_EMPTY_CHOICE,and_reduce);
}

KNO_FASTOP lispval or_reduce(lispval state,lispval step,int *done)
{
  /* Evaluate clauses until you get an error or a non-false/non-empty value */
  if (! ( (FALSEP(step)) || (EMPTYP(step)) ) ) {
    *done = 1;
    return step;}
  else return KNO_FALSE;
}

static lispval or_op(lispval exprs,kno_lexenv env,kno_stack stack,int tail)
{
  /* ??: is NO_PRUNE the right thing? */
  return reduce_op(stack,exprs,env,KNO_FALSE,KNO_EMPTY_CHOICE,or_reduce);
}

static lispval xref_type_error(lispval x,lispval tag)
{
  if (VOIDP(tag))
    kno_seterr(kno_TypeError,"XREF_OPCODE","compound",x);
  else {
    u8_string buf=kno_lisp2string(tag);
    kno_seterr(kno_TypeError,"XREF_OPCODE",buf,x);
    u8_free(buf);}
  return KNO_ERROR_VALUE;
}

static lispval xref_op(struct KNO_COMPOUND *c,long long i,lispval tag,int free)
{
  if ((VOIDP(tag)) || ((c->typetag) == tag)) {
    if ((i>=0) && (i<c->compound_length)) {
      lispval *values = &(c->compound_0), value;
      if (c->compound_ismutable)
        u8_read_lock(&(c->compound_rwlock));
      value = values[i];
      kno_incref(value);
      if (c->compound_ismutable)
        u8_rw_unlock(&(c->compound_rwlock));
      if (free) kno_decref((lispval)c);
      return kno_simplify_choice(value);}
    else {
      kno_seterr(kno_RangeError,"xref",NULL,(lispval)c);
      if (free) kno_decref((lispval)c);
      return KNO_ERROR_VALUE;}}
  else {
    lispval err=xref_type_error((lispval)c,tag);
    if (free) kno_decref((lispval)c);
    return err;}
}

static lispval xref_opcode(lispval x,long long i,lispval tag)
{
  if (!(CONSP(x))) {
    if (EMPTYP(x))
      return EMPTY;
    else return xref_type_error(x,tag);}
  else if (KNO_COMPOUNDP(x))
    return xref_op((struct KNO_COMPOUND *)x,i,tag,1);
  else if (CHOICEP(x)) {
    lispval results = EMPTY;
    DO_CHOICES(c,x) {
      lispval r = xref_op((struct KNO_COMPOUND *)c,i,tag,0);
      if (KNO_ABORTED(r)) {
        KNO_STOP_DO_CHOICES;
        kno_decref(results);
	results = r;
        break;}
      else {CHOICE_ADD(results,r);}}
    /* Need to free this */
    kno_decref(x);
    return results;}
  else return kno_err(kno_TypeError,"xref",kno_lisp2string(tag),x);
}

static lispval xpred_opcode(lispval x,lispval tag)
{
  if (KNO_COMPOUNDP(x)) {
    int match = KNO_COMPOUND_TYPEP(x,tag);
    kno_decref(x);
    if (match)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if (KNO_CHOICEP(x)) {
    int match = 0, nomatch = 0;
    KNO_DO_CHOICES(e,x) {
      if (KNO_COMPOUND_TYPEP(e,tag)) {
        match=1; if (nomatch) {
          KNO_STOP_DO_CHOICES;
          break;}}
      else {
        nomatch = 1;
        if (match) {
          KNO_STOP_DO_CHOICES;
          break;}}}
    kno_decref(x);
    if (match) {
      if (nomatch) {
        lispval tmpvec[2] = {KNO_FALSE,KNO_TRUE};
        return kno_init_choice(NULL,2,tmpvec,KNO_CHOICE_ISATOMIC);}
      else return KNO_TRUE;}
    else return KNO_FALSE;}
  else {
    kno_decref(x);
    return KNO_FALSE;}
}

static lispval elt_opcode(lispval arg1,lispval arg2)
{
  if ((KNO_SEQUENCEP(arg1)) && (KNO_INTP(arg2))) {
    lispval result;
    long long off = FIX2INT(arg2), len = kno_seq_length(arg1);
    if (off<0) off = len+off;
    result = kno_seq_elt(arg1,off);
    if (result == KNO_TYPE_ERROR)
      return kno_type_error(_("sequence"),"KNO_OPCODE_ELT",arg1);
    else if (result == KNO_RANGE_ERROR) {
      char buf[32];
      sprintf(buf,"%lld",off);
      return kno_err(kno_RangeError,"KNO_OPCODE_ELT",u8_strdup(buf),arg1);}
    else return result;}
  else if (!(KNO_SEQUENCEP(arg1)))
    return kno_type_error(_("sequence"),"KNO_OPCODE_ELT",arg1);
  else return kno_type_error(_("fixnum"),"KNO_OPCODE_ELT",arg2);
}

static lispval eltn_opcode(lispval arg1,int n,u8_context opname)
{
  if ( ( n == 0) && (KNO_PAIRP(arg1)) )
    return kno_incref(KNO_CAR(arg1));
  else if (VECTORP(arg1))
    if (VEC_LEN(arg1) > n)
      return kno_incref(VEC_REF(arg1,n));
    else return kno_err(kno_RangeError,opname,NULL,arg1);
  else if (COMPOUND_VECTORP(arg1))
    if (COMPOUND_VECLEN(arg1) > n)
      return kno_incref(XCOMPOUND_VEC_REF(arg1,n));
    else return kno_err(kno_RangeError,opname,NULL,arg1);
  else return kno_seq_elt(arg1,n);
}

/* PICK opcodes */

static lispval pickoids_opcode(lispval arg1)
{
  if (OIDP(arg1)) return arg1;
  else if (EMPTYP(arg1)) return arg1;
  else if (CHOICEP(arg1)) {
    lispval results = EMPTY;
    int all_oids = 1;
    {DO_CHOICES(elt,arg1) {
        if (OIDP(elt)) {CHOICE_ADD(results,elt);}
        else if (all_oids)
          all_oids = 0;
        else NO_ELSE;}}
    if (all_oids) {
      kno_decref(results);
      return kno_incref(arg1);}
    else NO_ELSE;
    return kno_simplify_choice(results);}
  else return EMPTY;
}

static lispval pickstrings_opcode(lispval arg1)
{
  if (CHOICEP(arg1)) {
    lispval results = EMPTY;
    int all_strings = 1;
    {DO_CHOICES(elt,arg1) {
        if (STRINGP(elt)) {
          kno_incref(elt);
          CHOICE_ADD(results,elt);}
        else if (all_strings)
          all_strings = 0;
        else NO_ELSE;}}
    if (all_strings) {
      kno_decref(results);
      return kno_incref(arg1);}
    else NO_ELSE;
    return kno_simplify_choice(results);}
  else if (STRINGP(arg1))
    return kno_incref(arg1);
  else return EMPTY;
}

static lispval picknums_opcode(lispval arg1)
{
  if (EMPTYP(arg1)) return arg1;
  else if (CHOICEP(arg1)) {
    lispval results = KNO_EMPTY;
    int all_nums = 1;
    {DO_CHOICES(elt,arg1) {
        if (KNO_FIXNUMP(elt)) {
	  CHOICE_ADD(results,elt);}
	else if (KNO_NUMBERP(elt)) {
	  kno_incref(elt);
	  CHOICE_ADD(results,elt);}
	else if (all_nums)
	  all_nums = 0;
        else NO_ELSE;}}
    if (all_nums) {
      kno_decref(results);
      return kno_incref(arg1);}
    else NO_ELSE;
    return kno_simplify_choice(results);}
  else if (KNO_NUMBERP(arg1))
    return kno_incref(arg1);
  else return EMPTY;
}

static lispval pickmaps_opcode(lispval arg1)
{
  if (EMPTYP(arg1))
    return arg1;
  else if (CHOICEP(arg1)) {
    lispval results = KNO_EMPTY;
    int all_maps = 1;
    {DO_CHOICES(elt,arg1) {
        if ( (KNO_SLOTMAPP(elt)) || (KNO_SCHEMAPP(elt)) ) {
	  kno_incref(elt);
	  CHOICE_ADD(results,elt);}
	else if (all_maps)
	  all_maps = 0;
        else NO_ELSE;}}
    if (all_maps) {
      kno_decref(results);
      return kno_incref(arg1);}
    else NO_ELSE;
    return kno_simplify_choice(results);}
  else if ( (KNO_SLOTMAPP(arg1)) || (KNO_SCHEMAPP(arg1)) )
    return kno_incref(arg1);
  else return EMPTY;
}

static lispval pickone_opcode(lispval normal)
{
  int n = KNO_CHOICE_SIZE(normal);
  if (n) {
    lispval chosen;
    int i = u8_random(n);
    const lispval *data = KNO_CHOICE_DATA(normal);
    chosen = data[i]; kno_incref(chosen);
    return chosen;}
  else return EMPTY;
}

static lispval nd1_call(lispval opcode,lispval arg1)
{
  switch (opcode) {
  case KNO_AMBIGP_OPCODE:
    if (CHOICEP(arg1))
      return KNO_TRUE;
    else return KNO_FALSE;
  case KNO_SINGLETONP_OPCODE:
    if (EMPTYP(arg1))
      return KNO_FALSE;
    else if (CHOICEP(arg1))
      return KNO_FALSE;
    else return KNO_TRUE;
  case KNO_FAILP_OPCODE:
    if (arg1==EMPTY)
      return KNO_TRUE;
    else return KNO_FALSE;
  case KNO_EXISTSP_OPCODE:
    if (arg1==EMPTY)
      return KNO_FALSE;
    else return KNO_TRUE;
  case KNO_SINGLETON_OPCODE:
    if (CHOICEP(arg1))
      return EMPTY;
    else return kno_incref(arg1);
  case KNO_CAR_OPCODE:
    if (EMPTYP(arg1))
      return arg1;
    else if (PAIRP(arg1))
      return kno_incref(KNO_CAR(arg1));
    else if (CHOICEP(arg1)) {
      lispval results = EMPTY;
      DO_CHOICES(arg,arg1)
        if (PAIRP(arg)) {
          lispval car = KNO_CAR(arg);
          kno_incref(car);
          CHOICE_ADD(results,car);}
        else {
          kno_decref(results);
          return kno_type_error(_("pair"),"CAR opcode",arg);}
      return results;}
    else return kno_type_error(_("pair"),"CAR opcode",arg1);
  case KNO_CDR_OPCODE:
    if (EMPTYP(arg1)) return arg1;
    else if (PAIRP(arg1))
      return kno_incref(KNO_CDR(arg1));
    else if (CHOICEP(arg1)) {
      lispval results = EMPTY;
      DO_CHOICES(arg,arg1)
        if (PAIRP(arg)) {
          lispval cdr = KNO_CDR(arg); kno_incref(cdr);
          CHOICE_ADD(results,cdr);}
        else {
          kno_decref(results);
          return kno_type_error(_("pair"),"CDR opcode",arg);}
      return results;}
    else return kno_type_error(_("pair"),"CDR opcode",arg1);
  case KNO_LENGTH_OPCODE:
    if (arg1==EMPTY) return EMPTY;
    else if (CHOICEP(arg1)) {
      lispval results = EMPTY;
      DO_CHOICES(arg,arg1) {
        if (KNO_SEQUENCEP(arg)) {
          int len = kno_seq_length(arg);
          lispval dlen = KNO_INT(len);
          CHOICE_ADD(results,dlen);}
        else {
          kno_decref(results);
          return kno_type_error(_("sequence"),"LENGTH opcode",arg);}}
      return kno_simplify_choice(results);}
    else if (KNO_SEQUENCEP(arg1))
      return KNO_INT(kno_seq_length(arg1));
    else return kno_type_error(_("sequence"),"LENGTH opcode",arg1);
  case KNO_QCHOICE_OPCODE:
    if (CHOICEP(arg1)) {
      kno_incref(arg1);
      return kno_init_qchoice(NULL,arg1);}
    else if (EMPTYP(arg1))
      return kno_init_qchoice(NULL,EMPTY);
    else return kno_incref(arg1);
  case KNO_CHOICE_SIZE_OPCODE:
    if (CHOICEP(arg1)) {
      int sz = KNO_CHOICE_SIZE(arg1);
      return KNO_INT(sz);}
    else if (EMPTYP(arg1))
      return KNO_INT(0);
    else return KNO_INT(1);
  case KNO_PICKOIDS_OPCODE:
    return pickoids_opcode(arg1);
  case KNO_PICKSTRINGS_OPCODE:
    return pickstrings_opcode(arg1);
  case KNO_PICKNUMS_OPCODE:
    return picknums_opcode(arg1);
  case KNO_PICKMAPS_OPCODE:
    return pickmaps_opcode(arg1);
  case KNO_PICKONE_OPCODE:
    if (CHOICEP(arg1))
      return pickone_opcode(arg1);
    else return kno_incref(arg1);
  case KNO_IFEXISTS_OPCODE:
    if (EMPTYP(arg1))
      return VOID;
    else return kno_incref(arg1);
  case KNO_SOMETRUE_OPCODE:
    if ( (EMPTYP(arg1)) || (FALSEP(arg1)) )
      return KNO_FALSE;
    else return KNO_TRUE;
  case KNO_FIXCHOICE_OPCODE:
    if (PRECHOICEP(arg1))
      return kno_simplify_choice(arg1);
    else return kno_incref(arg1);
  default:
    return kno_err(_("Invalid opcode"),"opcode eval",NULL,VOID);
  }
}

static lispval d1_call(lispval opcode,lispval arg1)
{
  switch (opcode) {
  case KNO_MINUS1_OPCODE:
  case KNO_PLUS1_OPCODE: {
    int delta = (opcode == KNO_MINUS1_OPCODE) ? (-1) : (1);
    if (FIXNUMP(arg1)) {
      long long iarg = FIX2INT(arg1);
      return KNO_INT(iarg+delta);}
    else if (NUMBERP(arg1))
      return kno_plus(arg1,KNO_INT(delta));
    else return kno_type_error(_("number"),"opcode 1+/-",arg1);}
  case KNO_NUMBERP_OPCODE:
    if (NUMBERP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_ZEROP_OPCODE:
    if (arg1==KNO_INT(0))
      return KNO_TRUE;
    else if (KNO_FLONUMP(arg1))
      if ( (KNO_FLONUM(arg1)) == 0.0)
        return KNO_TRUE;
      else return KNO_FALSE;
    else if (!(KNO_NUMBERP(arg1)))
      return KNO_FALSE;
    else {
      int cmp = kno_numcompare(arg1,KNO_INT(0));
      if (cmp==0)
        return KNO_TRUE;
      else if (cmp>1)
        return KNO_ERROR;
      else return KNO_FALSE;}
  case KNO_VECTORP_OPCODE:
    if (VECTORP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_PAIRP_OPCODE:
    if (PAIRP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_EMPTY_LISTP_OPCODE:
    if (arg1==NIL) return KNO_TRUE; else return KNO_FALSE;
  case KNO_STRINGP_OPCODE:
    if (STRINGP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_OIDP_OPCODE:
    if (OIDP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_SYMBOLP_OPCODE:
    if (KNO_SYMBOLP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_FIXNUMP_OPCODE:
    if (FIXNUMP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_FLONUMP_OPCODE:
    if (KNO_FLONUMP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_SEQUENCEP_OPCODE:
    if (KNO_SEQUENCEP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_TABLEP_OPCODE:
    if (KNO_SEQUENCEP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_CADR_OPCODE: {
    lispval cdr = KNO_CDR(arg1);
    if (PAIRP(cdr)) return kno_incref(KNO_CAR(cdr));
    else return kno_err(kno_RangeError,"KNO_CADR",NULL,arg1);}
  case KNO_CDDR_OPCODE: {
    lispval cdr = KNO_CDR(arg1);
    if (PAIRP(cdr)) return kno_incref(KNO_CDR(cdr));
    else return kno_err(kno_RangeError,"KNO_CDDR",NULL,arg1);}
  case KNO_CADDR_OPCODE: {
    lispval cdr = KNO_CDR(arg1);
    if (PAIRP(cdr)) {
      lispval cddr = KNO_CDR(cdr);
      if (PAIRP(cddr)) return kno_incref(KNO_CAR(cddr));
      else return kno_err(kno_RangeError,"KNO_CADDR",NULL,arg1);}
    else return kno_err(kno_RangeError,"KNO_CADDR",NULL,arg1);}
  case KNO_CDDDR_OPCODE: {
    lispval cdr = KNO_CDR(arg1);
    if (PAIRP(cdr)) {
      lispval cddr = KNO_CDR(cdr);
      if (PAIRP(cddr))
        return kno_incref(KNO_CDR(cddr));
      else return kno_err(kno_RangeError,"KNO_CDDR",NULL,arg1);}
    else return kno_err(kno_RangeError,"KNO_CDDDR",NULL,arg1);}
  case KNO_FIRST_OPCODE:
    return eltn_opcode(arg1,0,"KNO_FIRST_OPCODE");
  case KNO_SECOND_OPCODE:
    return eltn_opcode(arg1,1,"KNO_SECOND_OPCODE");
  case KNO_THIRD_OPCODE:
    return eltn_opcode(arg1,2,"KNO_THIRD_OPCODE");
  case KNO_TONUMBER_OPCODE:
    if (FIXNUMP(arg1)) return arg1;
    else if (NUMBERP(arg1)) return kno_incref(arg1);
    else if (STRINGP(arg1))
      return kno_string2number(CSTRING(arg1),10);
    else if (KNO_CHARACTERP(arg1))
      return KNO_INT(KNO_CHARCODE(arg1));
    else return kno_type_error(_("number|string"),"opcode ->number",arg1);
  case KNO_GETKEYS_OPCODE:
    if (KNO_TABLEP(arg1))
      return kno_getkeys(arg1);
    else return kno_type_error(_("table"),"opcode GETKEYS",arg1);
  case KNO_GETVALUES_OPCODE:
    if (KNO_TABLEP(arg1))
      return kno_getvalues(arg1);
    else return kno_type_error(_("table"),"opcode GETVALUES",arg1);
  case KNO_GETASSOCS_OPCODE:
    if (KNO_TABLEP(arg1))
      return kno_getassocs(arg1);
    else return kno_type_error(_("table"),"opcode GETASSOCS",arg1);
  default:
    return kno_err(_("Invalid opcode"),"opcode eval",NULL,VOID);
  }
}

static lispval d1_loop(lispval opcode,lispval arg1)
{
  lispval results = KNO_EMPTY;
  KNO_DO_CHOICES(arg,arg1) {
    lispval r = d1_call(opcode,arg);
    if (KNO_ABORTED(r)) {
      kno_decref(results);
      KNO_STOP_DO_CHOICES;
      return r;}
    else {KNO_ADD_TO_CHOICE(results,r);}}
  return results;
}

static lispval d2_call(lispval opcode,lispval arg1,lispval arg2)
{
  switch (opcode) {
  case KNO_ELT_OPCODE:
    return elt_opcode(arg1,arg2);
  case KNO_CONS_OPCODE:
    return kno_init_pair(NULL,kno_incref(arg1),kno_incref(arg2));
  case KNO_EQ_OPCODE:
    if (arg1==arg2) return KNO_TRUE; else return KNO_FALSE;
  case KNO_EQV_OPCODE: {
    if (arg1==arg2) return KNO_TRUE;
    else if ((NUMBERP(arg1)) && (NUMBERP(arg2)))
      if (kno_numcompare(arg1,arg2)==0)
        return KNO_TRUE; else return KNO_FALSE;
    else return KNO_FALSE;}
  case KNO_EQUAL_OPCODE: {
    if ((ATOMICP(arg1)) && (ATOMICP(arg2)))
      if (arg1==arg2) return KNO_TRUE; else return KNO_FALSE;
    else if (KNO_EQUAL(arg1,arg2)) return KNO_TRUE;
    else return KNO_FALSE;}
  default:
    return kno_err(_("Invalid opcode"),"opcode eval",NULL,VOID);
  }
}

static lispval d2_loop(lispval opcode,lispval arg1,lispval arg2)
{
  lispval results = KNO_EMPTY;
  KNO_DO_CHOICES(arg,arg1) {
    KNO_DO_CHOICES(xarg,arg2) {
      lispval r = d2_call(opcode,arg,xarg);
      if (KNO_ABORTED(r)) {
	kno_decref(results);
	KNO_STOP_DO_CHOICES;
	results=r;
	break;}
      else {KNO_ADD_TO_CHOICE(results,r);}}
    if (KNO_ABORTED(results)) {
      KNO_STOP_DO_CHOICES;
      break;}}
  return results;
}


KNO_FASTOP int numeric_argp(lispval x)
{
  /* This checks if there is a type error.
     The empty choice isn't a type error since it will just
     generate an empty choice as a result. */
  if ((EMPTYP(x))||(FIXNUMP(x)))
    return 1;
  else if (!(CONSP(x)))
    return 0;
  else switch (KNO_CONSPTR_TYPE(x)) {
    case kno_flonum_type: case kno_bigint_type:
    case kno_rational_type: case kno_complex_type:
      return 1;
    case kno_choice_type: case kno_prechoice_type: {
      DO_CHOICES(a,x) {
        if (FIXNUMP(a)) {}
        else if (PRED_TRUE(NUMBERP(a))) {}
        else {
          KNO_STOP_DO_CHOICES;
          return 0;}}
      return 1;}
    default:
      return 0;}
}

static lispval numop_call(lispval opcode,lispval arg1,lispval arg2)
{
  if (KNO_EXPECT_FALSE(!(numeric_argp(arg1))))
    return kno_err("NotANumber","numop_call",NULL,arg1);
  else if (KNO_EXPECT_FALSE(!(numeric_argp(arg2))))
    return kno_err("NotANumber","numop_call",NULL,arg2);
  else switch (opcode) {
    case KNO_NUMEQ_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
        if ((FIX2INT(arg1)) == (FIX2INT(arg2)))
          return KNO_TRUE;
        else return KNO_FALSE;
      else if (kno_numcompare(arg1,arg2)==0) return KNO_TRUE;
      else return KNO_FALSE;}
    case KNO_GT_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
        if ((FIX2INT(arg1))>(FIX2INT(arg2)))
          return KNO_TRUE;
        else return KNO_FALSE;
      else if (kno_numcompare(arg1,arg2)>0) return KNO_TRUE;
      else return KNO_FALSE;}
    case KNO_GTE_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
        if ((FIX2INT(arg1))>=(FIX2INT(arg2)))
          return KNO_TRUE;
        else return KNO_FALSE;
      else if (kno_numcompare(arg1,arg2)>=0) return KNO_TRUE;
      else return KNO_FALSE;}
    case KNO_LT_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
        if ((FIX2INT(arg1))<(FIX2INT(arg2)))
          return KNO_TRUE;
        else return KNO_FALSE;
      else if (kno_numcompare(arg1,arg2)<0) return KNO_TRUE;
      else return KNO_FALSE;}
    case KNO_LTE_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
        if ((FIX2INT(arg1))<=(FIX2INT(arg2)))
          return KNO_TRUE;
        else return KNO_FALSE;
      else if (kno_numcompare(arg1,arg2)<=0) return KNO_TRUE;
      else return KNO_FALSE;}
    case KNO_PLUS_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
        long long m = FIX2INT(arg1), n = FIX2INT(arg2);
        return KNO_INT(m+n);}
      else if ((KNO_FLONUMP(arg1)) && (KNO_FLONUMP(arg2))) {
        double x = KNO_FLONUM(arg1), y = KNO_FLONUM(arg2);
        return kno_init_double(NULL,x+y);}
      else return kno_plus(arg1,arg2);}
    case KNO_MINUS_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
        long long m = FIX2INT(arg1), n = FIX2INT(arg2);
        return KNO_INT(m-n);}
      else if ((KNO_FLONUMP(arg1)) && (KNO_FLONUMP(arg2))) {
        double x = KNO_FLONUM(arg1), y = KNO_FLONUM(arg2);
        return kno_init_double(NULL,x-y);}
      else return kno_subtract(arg1,arg2);}
    case KNO_TIMES_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
        long long m = FIX2INT(arg1), n = FIX2INT(arg2);
        return KNO_INT(m*n);}
      else if ((KNO_FLONUMP(arg1)) && (KNO_FLONUMP(arg2))) {
        double x = KNO_FLONUM(arg1), y = KNO_FLONUM(arg2);
        return kno_init_double(NULL,x*y);}
      else return kno_multiply(arg1,arg2);}
    case KNO_FLODIV_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
        long long m = FIX2INT(arg1), n = FIX2INT(arg2);
        double x = (double)m, y = (double)n;
        return kno_init_double(NULL,x/y);}
      else if ((KNO_FLONUMP(arg1)) && (KNO_FLONUMP(arg2))) {
        double x = KNO_FLONUM(arg1), y = KNO_FLONUM(arg2);
        return kno_init_double(NULL,x/y);}
      else {
        double x = kno_todouble(arg1), y = kno_todouble(arg2);
        return kno_init_double(NULL,x/y);}}
    default:
      return kno_err(_("Invalid opcode"),"numop_call",NULL,VOID);
    }
}

static lispval nd2_call(lispval opcode,lispval arg1,lispval arg2)
{
  lispval result = KNO_ERROR_VALUE;
  if (KNO_ABORTED(arg2))
    result = arg2;
  else if (VOIDP(arg2)) {
    result = kno_err(kno_VoidArgument,"OPCODE setop",NULL,opcode);}
  else switch (opcode) {
    case KNO_IDENTICAL_OPCODE:
      if (arg1==arg2)
        result = KNO_TRUE;
      else if (KNO_EQUAL(arg1,arg2))
        result = KNO_TRUE;
      else result = KNO_FALSE;
      break;
    case KNO_OVERLAPS_OPCODE:
      if (arg1==arg2) {
        if (EMPTYP(arg1))
          result = KNO_FALSE;
        else result = KNO_TRUE;}
      else if (kno_overlapp(arg1,arg2))
        result = KNO_TRUE;
      else result = KNO_FALSE;
      break;
    case KNO_CONTAINSP_OPCODE:
      if (EMPTYP(arg2))
        result = KNO_FALSE;
      else if (arg1 == arg2)
        result = KNO_TRUE;
      else if (EMPTYP(arg1))
        result = KNO_FALSE;
      else if (kno_containsp(arg1,arg2))
        result = KNO_TRUE;
      else result = KNO_FALSE;
      break;
    case KNO_CHOICEREF_OPCODE:
      if (!(FIXNUMP(arg2))) {
        kno_decref(arg1);
        result = kno_err(kno_SyntaxError,"choiceref_opcode",NULL,arg2);}
      else {
        int i = FIX2INT(arg2);
        if (i<0)
          result = kno_err(kno_SyntaxError,"choiceref_opcode",NULL,arg2);
        else if ((i==0)&&(EMPTYP(arg1)))
          result = VOID;
        else if (CHOICEP(arg1)) {
          struct KNO_CHOICE *ch = (kno_choice)arg1;
          if (i<ch->choice_size) {
            const lispval *elts = KNO_XCHOICE_DATA(ch);
            lispval elt = elts[i];
            result = kno_incref(elt);}
          else result = VOID;}
        else if (i==0) result=arg1;
        else result = VOID;}
    default:
      result = kno_err(_("Invalid opcode"),"numop_call",NULL,VOID);
    }
  return result;
}

/* Loop */

static lispval until_opcode(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval params = KNO_CDR(expr);
  lispval test_expr = KNO_CAR(params), loop_body = KNO_CDR(params);
  if (VOIDP(test_expr))
    return kno_err(kno_SyntaxError,"KNO_LOOP_OPCODE",NULL,expr);
  lispval test_val = _kno_fast_eval(test_expr,env,stack,0);
  if (ABORTED(test_val))
    return test_val;
  else while ( (FALSEP(test_val)) || (EMPTYP(test_val)) ) {
      lispval body_result=op_eval_body(loop_body,env,stack,"UNTIL",NULL,0);
      if (KNO_BROKEP(body_result))
        return KNO_FALSE;
      else if (KNO_ABORTED(body_result))
        return body_result;
      else kno_decref(body_result);
      test_val = _kno_fast_eval(test_expr,env,stack,0);
      if (KNO_ABORTED(test_val))
        return test_val;}
  return test_val;
}

/* Assignment */

#define CURRENT_VALUEP(x)                                 \
  (! ((cur == KNO_DEFAULT_VALUE) || (cur == KNO_UNBOUND) || \
      (cur == VOID) || (cur == KNO_NULL)))

static lispval combine_values(lispval combiner,lispval cur,lispval value)
{
  int use_cur=((KNO_ABORTP(cur)) || (CURRENT_VALUEP(cur)));
  switch (combiner) {
  case VOID: case KNO_FALSE:
    return value;
  case KNO_TRUE: case KNO_DEFAULT_VALUE:
    if (use_cur)
      return cur;
    else return value;
  case KNO_PLUS_OPCODE:
    if (!(use_cur)) cur=KNO_FIXNUM_ZERO;
    if ( (FIXNUMP(value)) && (FIXNUMP(cur)) ) {
      long long ic=FIX2INT(cur), ip=FIX2INT(value);
      return KNO_MAKE_FIXNUM(ic+ip);}
    else return kno_plus(cur,value);
  case KNO_MINUS_OPCODE:
    if (!(use_cur)) cur=KNO_FIXNUM_ZERO;
    if ( (FIXNUMP(value)) && (FIXNUMP(cur)) ) {
      long long ic=FIX2INT(cur), im=FIX2INT(value);
      return KNO_MAKE_FIXNUM(ic-im);}
    else return kno_subtract(cur,value);
  default:
    return value;
  }
}
static lispval assignop(kno_stack stack,kno_lexenv env,
                        lispval var,lispval expr,lispval combiner)
{
  if (KNO_LEXREFP(var)) {
    int up = KNO_LEXREF_UP(var);
    int across = KNO_LEXREF_ACROSS(var);
    kno_lexenv scan = ( (env->env_copy) ? (env->env_copy) : (env) );
    while ((up)&&(scan)) {
      kno_lexenv parent = scan->env_parent;
      if ((parent) && (parent->env_copy))
        scan = parent->env_copy;
      else scan = parent;
      up--;}
    if (PRED_TRUE(scan!=NULL)) {
      lispval bindings = scan->env_bindings;
      if (PRED_TRUE(SCHEMAPP(bindings))) {
        struct KNO_SCHEMAP *map = (struct KNO_SCHEMAP *)bindings;
        int map_len = map->schema_length;
        if (PRED_TRUE( across < map_len )) {
          lispval *values = map->schema_values;
          lispval cur     = values[across];
          if ( map->schemap_stackvals ) {
            kno_incref_vec(values,map_len);
            map->schemap_stackvals = 0;}
          if ( ( (combiner == KNO_TRUE) || (combiner == KNO_DEFAULT) ) &&
               ( (CURRENT_VALUEP(cur)) || (KNO_ABORTED(cur)) ) ) {
            if (KNO_ABORTED(cur))
              return cur;
            else return VOID;}
          else {
            lispval value = _kno_fast_eval(expr,env,stack,0);
            /* This gnarly bit of code handles the case where
               evaluating 'expr' changed the environment structure,
               by, for instance, creating a lambda which made a
               dynamic environment copy. If so, we need to change the
               value of 'values' so that we store any resulting values
               in the right place. */
            if ( (scan->env_copy) && (scan->env_copy != scan) ) {
              lispval new_bindings = scan->env_copy->env_bindings;
              if ( (new_bindings != bindings) &&
                   (PRED_TRUE(SCHEMAPP(bindings))) ) {
                struct KNO_SCHEMAP *new_map =
                  (struct KNO_SCHEMAP *) new_bindings;
                values = new_map->schema_values;
                cur = values[across];}}
            if (KNO_ABORTED(value))
              return value;
            else if ( (combiner == KNO_FALSE) || (combiner == VOID) ) {
              /* Replace the currnet value */
              values[across]=value;
              kno_decref(cur);}
            else if (combiner == KNO_UNION_OPCODE) {
              if (KNO_ABORTED(value)) return value;
              if ((cur==VOID)||(cur==KNO_UNBOUND)||(cur==EMPTY))
                values[across]=value;
              else {CHOICE_ADD(values[across],value);}}
            else {
              lispval newv=combine_values(combiner,cur,value);
              if (cur != newv) {
                values[across]=newv;
                kno_decref(cur);
                if (newv != value) kno_decref(value);}
              else kno_decref(value);}
            return VOID;}}}}
    u8_string lexref=u8_mkstring("up%d/across%d",up,across);
    lispval env_copy=(lispval)kno_copy_env(env);
    return kno_err("BadLexref","ASSIGN_OPCODE",lexref,env_copy);}
  else if ((PAIRP(var)) &&
           (KNO_SYMBOLP(KNO_CAR(var))) &&
           (TABLEP(KNO_CDR(var)))) {
    int rv=-1;
    lispval table=KNO_CDR(var), sym=KNO_CAR(var);
    lispval value = _kno_fast_eval(expr,env,stack,0);
    if (KNO_ABORTED(value))
      return value;
    else if ( (combiner == KNO_FALSE) || (combiner == VOID) ) {
      if (KNO_LEXENVP(table))
        rv=kno_assign_value(sym,value,(kno_lexenv)table);
      else rv=kno_store(table,sym,value);}
    else if (combiner == KNO_UNION_OPCODE) {
      if (KNO_LEXENVP(table))
        rv=kno_add_value(sym,value,(kno_lexenv)table);
      else rv=kno_add(table,sym,value);}
    else {
      lispval cur=kno_get(table,sym,KNO_UNBOUND);
      lispval newv=combine_values(combiner,cur,value);
      if (KNO_ABORTED(newv))
        rv=-1;
      else rv=kno_store(table,sym,newv);
      kno_decref(cur);
      kno_decref(newv);}
    kno_decref(value);
    if (rv<0) {
      kno_seterr("AssignFailed","ASSIGN_OPCODE",NULL,expr);
      return KNO_ERROR_VALUE;}
    else return VOID;}
  return kno_err(kno_SyntaxError,"ASSIGN_OPCODE",NULL,expr);
}

/* Binding */

static lispval bindop(lispval op,
                      struct KNO_STACK *_stack,kno_lexenv env,
                      lispval vars,lispval inits,lispval body,
                      int tail)
{
  int i=0, n=VEC_LEN(vars);
  KNO_PUSH_STACK(bind_stack,"bindop","opframe",op);
  INIT_STACK_SCHEMA(bind_stack,bound,env,n,VEC_DATA(vars));
  lispval *values=bound_bindings.schema_values;
  lispval scan_inits = inits;
  kno_lexenv env_copy=NULL;
  while (i<n) {
    lispval val_expr = pop_arg(scan_inits);
    lispval val = _kno_fast_eval(val_expr,bound,bind_stack,0);
    if (KNO_ABORTED(val))
      _return val;
    if ( (env_copy == NULL) && (bound->env_copy) ) {
      env_copy=bound->env_copy; bound=env_copy;
      values=((kno_schemap)(bound->env_bindings))->schema_values;}
    values[i++]=val;}
  lispval result = op_eval_body(body,bound,bind_stack,"#BINDOP",NULL,tail);
  kno_pop_stack(bind_stack);
  return result;
}

static lispval vector_bindop(lispval op,
                     struct KNO_STACK *_stack,kno_lexenv env,
                             lispval vars,lispval inits,lispval body,
                             int tail)
{
  int i=0, n=VEC_LEN(vars);
  KNO_PUSH_STACK(bind_stack,"vector_bindop","opframe",op);
  INIT_STACK_SCHEMA(bind_stack,bound,env,n,VEC_DATA(vars));
  lispval *values=bound_bindings.schema_values;
  lispval *exprs=VEC_DATA(inits);
  kno_lexenv env_copy=NULL;
  while (i<n) {
    lispval val_expr=exprs[i];
    lispval val=_kno_fast_eval(val_expr,bound,bind_stack,0);
    if (KNO_ABORTED(val))
      _return val;
    if ( (env_copy == NULL) && (bound->env_copy) ) {
      env_copy=bound->env_copy; bound=env_copy;
      values=((kno_schemap)(bound->env_bindings))->schema_values;}
    values[i++]=val;}
  lispval result = op_eval_body(body,bound,bind_stack,"#VECTORBIND",NULL,tail);
  kno_pop_stack(bind_stack);
  return result;
}

static void reset_env_op(kno_lexenv env)
{
  if ( (env->env_copy) && (env != env->env_copy) ) {
    lispval tofree = (lispval) env;
    env->env_copy=NULL;
    kno_decref(tofree);}
}

static lispval handle_table_result(int rv)
{
  if (rv<0)
    return KNO_ERROR;
  else if (rv)
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval handle_table_opcode(lispval opcode,lispval expr,
                                   kno_lexenv env,
                                   kno_stack _stack)
{
  lispval args = KNO_CDR(expr);
  lispval subject_arg = pop_arg(args);
  if (KNO_EXPECT_FALSE(!(KNO_PAIRP(args))))
    return kno_err(kno_TooFewArgs,"handle_table_opcode",NULL,expr);
  lispval subject = arg_eval(subject_arg,env,_stack);
  if (KNO_ABORTP(subject))
    return subject;
  else if (KNO_EMPTYP(subject)) {
    switch (opcode) {
    case KNO_GET_OPCODE: case KNO_PRIMGET_OPCODE:
      return KNO_EMPTY_CHOICE;
    case KNO_TEST_OPCODE: case KNO_PRIMTEST_OPCODE:
      return KNO_FALSE;
    case KNO_ASSERT_OPCODE: case KNO_ADD_OPCODE:
    case KNO_RETRACT_OPCODE: case KNO_DROP_OPCODE:
      return KNO_VOID;
    default:
      return kno_err("BadOpcode","handle_table_opcode",NULL,expr);}}
  else NO_ELSE;
  if (KNO_EXPECT_FALSE(!(KNO_TABLEP(subject)))) {
    kno_seterr("NotATable","handle_table_opcode",NULL,subject);
    kno_decref(subject);
    return KNO_ERROR_VALUE;}
  else NO_ELSE;
  lispval slotid_arg = pop_arg(args);
  lispval slotid = arg_eval(slotid_arg,env,_stack);
  if (KNO_ABORTP(slotid)) {
    kno_decref(subject);
    return slotid;}
  else NO_ELSE;
  lispval value_arg = (KNO_PAIRP(args)) ? (pop_arg(args)) : (KNO_VOID);
  lispval value = (KNO_VOIDP(value_arg)) ? (KNO_VOID) :
    (arg_eval(value_arg,env,_stack));
  if (KNO_ABORTP(value)) {
    kno_decref(subject); kno_decref(slotid);
    return value;}
  else NO_ELSE;
  lispval result = KNO_VOID;
  switch (opcode) {
  case KNO_GET_OPCODE:
    result = kno_fget(subject,slotid); break;
  case KNO_PRIMGET_OPCODE:
    if (KNO_VOIDP(value)) value = KNO_EMPTY_CHOICE;
    result = kno_get(subject,slotid,value); break;
  case KNO_TEST_OPCODE:
    result = kno_ftest(subject,slotid,value); break;
  case KNO_PRIMTEST_OPCODE: {
    int rv = kno_test(subject,slotid,value);
    result = handle_table_result(rv);
    break;}
  case KNO_ADD_OPCODE:
    if (KNO_VOIDP(value))
      result = kno_err(kno_TooFewArgs,"handle_table_opcode/add",NULL,expr);
    else {
      int rv = kno_add(subject,slotid,value);
      result = handle_table_result(rv);}
    break;
  case KNO_DROP_OPCODE: {
    int rv = kno_drop(subject,slotid,value);
    result = handle_table_result(rv);
    break;}
  case KNO_STORE_OPCODE:
    if (KNO_VOIDP(value))
      result = kno_err(kno_TooFewArgs,"handle_table_opcode/store",NULL,expr);
    else {
      int rv = kno_store(subject,slotid,value);
      result = handle_table_result(rv);}
    break;
  case KNO_ASSERT_OPCODE:
    if (KNO_VOIDP(value))
      result = kno_err(kno_TooFewArgs,"handle_table_opcode",NULL,expr);
    else result = kno_assert(subject,slotid,value);
    break;
  case KNO_RETRACT_OPCODE:
    result = kno_retract(subject,slotid,value);
    break;
  default:
    result = kno_err("BadOpcode","handle_table_opcode",NULL,expr);}
  kno_decref(subject);
  kno_decref(slotid);
  kno_decref(value);
  return result;
}

/* Opcode dispatch */

static lispval handle_special_opcode(lispval opcode,lispval args,lispval expr,
                                     kno_lexenv env,
                                     kno_stack _stack,
                                     int tail)
{
  switch (opcode) {
  case KNO_QUOTE_OPCODE: {
    lispval arg = pop_arg(args);
    if (KNO_EXPECT_FALSE(KNO_VOIDP(arg)))
      return kno_err(kno_SyntaxError,"opcode_eval",NULL,expr);
    else if (KNO_CONSP(arg))
      return kno_incref(arg);
    else return arg;}
  case KNO_SYMREF_OPCODE: {
    lispval refenv=pop_arg(args);
    lispval sym=pop_arg(args);
    if (KNO_EXPECT_FALSE(!(KNO_SYMBOLP(sym))))
      return kno_err(kno_SyntaxError,"KNO_SYMREF_OPCODE/badsym",NULL,expr);
    if (HASHTABLEP(refenv))
      return kno_hashtable_get((kno_hashtable)refenv,sym,KNO_UNBOUND);
    else if (KNO_LEXENVP(refenv))
      return kno_symeval(sym,(kno_lexenv)refenv);
    else if (TABLEP(refenv))
      return kno_get(refenv,sym,KNO_UNBOUND);
    else return kno_err(kno_SyntaxError,"KNO_SYMREF_OPCODE/badenv",NULL,expr);}
  case KNO_VOID_OPCODE: {
    return VOID;}
  case KNO_RESET_ENV_OPCODE: {
    reset_env_op(env);
    return VOID;}
  case KNO_BEGIN_OPCODE:
    return op_eval_body(KNO_CDR(expr),env,_stack,"#BEGINOP",NULL,tail);
  case KNO_UNTIL_OPCODE:
    return until_opcode(expr,env,_stack);

  case KNO_BRANCH_OPCODE: {
    lispval test_expr = pop_arg(args);
    if (VOIDP(test_expr))
      return kno_err(kno_SyntaxError,"KNO_BRANCH_OPCODE",NULL,expr);
    lispval test_val = op_eval(_stack,test_expr,env,0);
    if (KNO_ABORTED(test_val))
      return test_val;
    else if (KNO_FALSEP(test_val)) { /* (  || (KNO_EMPTYP(test_val)) ) */
      pop_arg(args);
      lispval else_expr = pop_arg(args);
      return op_eval(_stack,else_expr,env,tail);}
    else {
      lispval then = pop_arg(args);
      U8_MAYBE_UNUSED lispval ignore = pop_arg(args);
      kno_decref(test_val);
      return op_eval(_stack,then,env,tail);}}

  case KNO_BIND_OPCODE: {
    lispval vars=pop_arg(args);
    lispval inits=pop_arg(args);
    lispval body=pop_arg(args);
    if (!(KNO_VECTORP(vars)))
      return kno_err(kno_SyntaxError,"BINDOP",NULL,expr);
    else if (KNO_VECTORP(inits))
      return vector_bindop(opcode,_stack,env,vars,inits,body,tail);
    else return bindop(opcode,_stack,env,vars,inits,body,tail);}

  case KNO_ASSIGN_OPCODE: {
    lispval var = pop_arg(args);
    lispval combiner = pop_arg(args);
    lispval val_expr = pop_arg(args);
    return assignop(_stack,env,var,val_expr,combiner);}

  case KNO_XREF_OPCODE: {
    lispval off_arg = pop_arg(args);
    lispval type_arg = pop_arg(args);
    lispval obj_expr = pop_arg(args);
    if ( (FIXNUMP(off_arg)) && (! (VOIDP(obj_expr)) ) ) {
      lispval obj_arg=fast_eval(obj_expr,env);
      return xref_opcode(kno_simplify_choice(obj_arg),
                         FIX2INT(off_arg),
                         type_arg);}
    kno_seterr(kno_SyntaxError,"KNO_XREF_OPCODE",NULL,expr);
    return KNO_ERROR_VALUE;}

  case KNO_XPRED_OPCODE: {
    lispval type_arg = pop_arg(args);
    lispval obj_expr = pop_arg(args);
    if (KNO_EXPECT_FALSE(VOIDP(obj_expr))) {
      kno_seterr(kno_SyntaxError,"KNO_XREF_OPCODE",NULL,expr);
      return KNO_ERROR_VALUE;}
    lispval obj_arg=fast_eval(obj_expr,env);
    if (KNO_ABORTED(obj_arg)) return obj_arg;
    else if (KNO_EMPTYP(obj_arg)) return KNO_EMPTY;
    else if ( (KNO_COMPOUNDP(obj_arg)) || (KNO_AMBIGP(obj_arg)) )
      return xpred_opcode(kno_simplify_choice(obj_arg),type_arg);
    else {
      kno_decref(obj_arg);
      return KNO_FALSE;}}

  case KNO_NOT_OPCODE: {
    lispval arg_val = _kno_fast_eval(pop_arg(args),env,_stack,0);
    if (FALSEP(arg_val))
      return KNO_TRUE;
    else {
      kno_decref(arg_val);
      return KNO_FALSE;}}

  case KNO_BREAK_OPCODE:
    return KNO_BREAK;

  case KNO_TRY_OPCODE: return try_op(args,env,_stack,tail);
  case KNO_AND_OPCODE: return and_op(args,env,_stack,tail);
  case KNO_OR_OPCODE:  return or_op(args,env,_stack,tail);

  case KNO_INTERSECT_OPCODE: return intersect_op(args,env,_stack);
  case KNO_UNION_OPCODE: return union_op(args,env,_stack);
  case KNO_DIFFERENCE_OPCODE: return difference_op(args,env,_stack);

  default:
    return kno_err("BadOpcode","handle_special_opcode",NULL,expr);
  }
}

/* Experimenting */

KNO_FASTOP lispval plus_reduce(lispval state,lispval step,int *done)
{
  if (KNO_EMPTYP(step)) {
    *done = 1;
    return step;}
  else if (PRED_FALSE(!(NUMBERP(step))))
    return kno_type_error(_("number"),"plus",step);
  else if (KNO_VOIDP(state))
    return step;
  else if (step == KNO_FIXNUM_ZERO)
    return state;
  else if ( (KNO_FIXNUMP(state)) && (KNO_FIXNUMP(step)) ) {
    long long istate = FIX2INT(state);
    long long istep = FIX2INT(step);
    if (istate==0) return step;
    else if (istep==0) return state;
    else {
      long long result = istate+istep;
      if ((result<KNO_MAX_FIXNUM) && (result>KNO_MIN_FIXNUM))
	return KNO_INT(result);
      else return (lispval) kno_long_long_to_bigint(result);}}
  else if ( (KNO_FLONUMP(state)) && (KNO_FLONUMP(step)) ) {
    double result = KNO_FLONUM(state)+KNO_FLONUM(step);
    return kno_init_flonum(NULL,result);}
  else if (KNO_FLONUMP(state)) {
    double as_double = kno_todouble(step);
    double result = KNO_FLONUM(state)+as_double;
    return kno_init_flonum(NULL,result);}
  else if (PRED_FALSE(!(NUMBERP(state))))
    return kno_type_error(_("number"),"plus",state);
  else return kno_plus(state,step);
}

static lispval plus_op(lispval exprs,kno_lexenv env,kno_stack stack)
{
  return nd_reduce_op(stack,exprs,env,
		      KNO_FIXNUM_ZERO,
		      plus_reduce);
}

KNO_FASTOP lispval minus_reduce(lispval state,lispval step,int *done)
{
  if (KNO_EMPTYP(step)) {
    *done = 1;
    return step;}
  else if (PRED_FALSE(!(NUMBERP(step))))
    return kno_type_error(_("number"),"minus",step);
  else if (KNO_VOIDP(state))
    return step;
  else if (step == KNO_FIXNUM_ZERO)
    return state;
  else if ( (KNO_FIXNUMP(state)) && (KNO_FIXNUMP(step)) ) {
    long long istate = FIX2INT(state);
    long long istep = FIX2INT(step);
    long long result = istate-istep;
    if ((result<KNO_MAX_FIXNUM) && (result>KNO_MIN_FIXNUM))
      return KNO_INT(result);
    else return (lispval) kno_long_long_to_bigint(result);}
  else if ( (KNO_FLONUMP(state)) && (KNO_FLONUMP(step)) ) {
    double result = KNO_FLONUM(state)-KNO_FLONUM(step);
    return kno_init_flonum(NULL,result);}
  else if (KNO_FLONUMP(state)) {
    double as_double = kno_todouble(step);
    double result = KNO_FLONUM(state)-as_double;
    return kno_init_flonum(NULL,result);}
  else if (PRED_FALSE(!(NUMBERP(state))))
    return kno_type_error(_("number"),"minus",state);
  else return kno_subtract(state,step);
}

static lispval minus_op(lispval exprs,kno_lexenv env,kno_stack stack)
{
  lispval arg0_expr = pop_arg(exprs);
  lispval arg0 = op_eval(stack,arg0_expr,env,0);
  if (EMPTYP(arg0))
    return KNO_EMPTY;
  else if (exprs == KNO_EMPTY_LIST) {
    if (KNO_FIXNUMP(arg0)) {
      int ix = kno_getint(arg0);
      int nix = -ix;
      return KNO_INT(nix);}
    else if (KNO_FLONUMP(arg0)) {
      double d = KNO_FLONUM(arg0);
      return kno_make_flonum(-d);}
    else if (KNO_CHOICEP(arg0)) {
      lispval results = KNO_EMPTY;
      KNO_DO_CHOICES(arg,arg0) {
	if (KNO_FIXNUMP(arg)) {
	  long long ix = kno_getint(arg);
	  long long nix = -ix;
	  lispval r = KNO_INT(nix);
	  KNO_ADD_TO_CHOICE(results,r);}
	else if (KNO_FLONUMP(arg)) {
	  double d = KNO_FLONUM(arg);
	  lispval r = kno_make_flonum(-d);
	  KNO_ADD_TO_CHOICE(results,r);}
	else {
	  lispval r = kno_subtract(KNO_FIXNUM_ZERO,arg0);
	  if (KNO_ABORTED(r)) {
	    kno_decref(results);
	    return r;}
	  else {
	    KNO_ADD_TO_CHOICE(results,r);}}}
      return kno_simplify_choice(results);}
    else {
      lispval result = kno_subtract(KNO_FIXNUM_ZERO,arg0);
      kno_decref(arg0);
      return result;}}
  else {
    lispval state = kno_simplify_choice(arg0);
    return nd_reduce_op(stack,exprs,env,state,minus_reduce);}
}

KNO_FASTOP lispval mult_reduce(lispval state,lispval step,int *done)
{
  if (KNO_EMPTYP(step)) {
    *done = 1;
    return step;}
  else if (PRED_FALSE(!(NUMBERP(step))))
    return kno_type_error(_("number"),"minus",step);
  else if (KNO_VOIDP(state))
    return step;
  else if (step == KNO_FIXNUM_ZERO) {
    *done = 1;
    return step;}
  else if (step == KNO_FIXNUM_ONE)
    return state;
  else if (state == KNO_FIXNUM_ONE)
    return step;
  else if ( (KNO_FIXNUMP(state)) && (KNO_FIXNUMP(step)) ) {
    long long ix = FIX2INT(state);
    long long iy = FIX2INT(step);
    long long q, result;
    /* Figure out if the result will fit in a long long */
    q = ( (iy>0) ? (KNO_MAX_FIXNUM/iy) : (KNO_MIN_FIXNUM/iy) );
    if ( (ix>0) ? (ix>q) : ((-ix)>q) )
      return kno_multiply(state,step);
    else result = ix*iy;
    if ( (result<KNO_MAX_FIXNUM) && (result>KNO_MIN_FIXNUM) )
      return KNO_INT(result);
    else return (lispval) kno_long_long_to_bigint(result);}
  else if ( (KNO_FLONUMP(state)) && (KNO_FLONUMP(step)) ) {
    double result = KNO_FLONUM(state)*KNO_FLONUM(step);
    return kno_init_flonum(NULL,result);}
  else if (KNO_FLONUMP(state)) {
    double as_double = kno_todouble(step);
    double result = KNO_FLONUM(state)*as_double;
    return kno_init_flonum(NULL,result);}
  else if (KNO_FLONUMP(step)) {
    double as_double = kno_todouble(state);
    double result = KNO_FLONUM(step)*as_double;
    return kno_init_flonum(NULL,result);}
  else if (PRED_FALSE(!(NUMBERP(state))))
    return kno_type_error(_("number"),"minus",state);
  else return kno_multiply(state,step);
}

static lispval mult_op(lispval exprs,kno_lexenv env,kno_stack stack)
{
  return nd_reduce_op(stack,exprs,env,KNO_FIXNUM_ONE,mult_reduce);
}

static lispval flodiv_reduce(lispval state,lispval step,int *done)
{
  if (KNO_EMPTYP(step)) {
    *done = 1;
    return step;}
  else if (PRED_FALSE(!(NUMBERP(step))))
    return kno_type_error(_("number"),"minus",step);
  else if (KNO_VOIDP(state))
    return step;
  else if (step == KNO_FIXNUM_ONE)
    return state;
  else if ( (KNO_FLONUMP(state)) && (KNO_FLONUMP(step)) ) {
    double result = KNO_FLONUM(state)/KNO_FLONUM(step);
    return kno_init_flonum(NULL,result);}
  else if (KNO_FLONUMP(state)) {
    double as_double = kno_todouble(step);
    double result = KNO_FLONUM(state)/as_double;
    return kno_init_flonum(NULL,result);}
  else if (PRED_FALSE(!(NUMBERP(state))))
    return kno_type_error(_("number"),"minus",state);
  else return kno_inexact_divide(state,step);
}

static lispval flodiv_op(lispval exprs,kno_lexenv env,kno_stack stack)
{
  lispval arg0_expr = pop_arg(exprs);
  lispval arg0 = op_eval(stack,arg0_expr,env,0);
  if (exprs == KNO_EMPTY_LIST) {
    if (KNO_FIXNUMP(arg0)) {
      int ix = kno_getint(arg0);
      double d = ix, r = 1/d;
      return kno_make_flonum(r);}
    else if (KNO_FLONUMP(arg0)) {
      double d = KNO_FLONUM(arg0), r = 1/d;
      return kno_make_flonum(r);}
    else {
      lispval dval = kno_todouble(arg0);
      if (KNO_ABORTED(dval)) return dval;
      double d = KNO_FLONUM(dval);
      double r = 1/d;
      kno_decref(dval);
      return kno_make_flonum(r);}}
  else return nd_reduce_op(stack,exprs,env,arg0,flodiv_reduce);
}

/* Comparisions */

KNO_FASTOP lispval bool_result(lispval result)
{
  if ( (KNO_ABORTP(result)) || (KNO_FALSEP(result)) || (KNO_EMPTYP(result)) )
    return result;
  kno_decref(result);
  return KNO_TRUE;
}

KNO_FASTOP lispval numeq_reduce(lispval state,lispval step,int *done)
{
  if (KNO_EMPTYP(step)) {
    *done = 1;
    return step;}
  else if (PRED_FALSE(!(NUMBERP(step))))
    return kno_type_error(_("number"),"=?",step);
  else if (KNO_VOIDP(state))
    return step;
  else if (state==step)
    return state;
  else if ( (KNO_FIXNUMP(state)) &&
	    (KNO_FIXNUMP(step)) )
    if (state == step)
      return step;
    else {
      *done = 1;
      return KNO_FALSE;}
  else if ( (KNO_FLONUMP(state)) &&
	    (KNO_FLONUMP(step)) )
    if ( (KNO_FLONUM(state)) == (KNO_FLONUM(step)) )
      return step;
    else {
      *done = 1;
      return KNO_FALSE;}
  else {
    int cmp = kno_numcompare(state,step);
    if (cmp == 0)
      return step;
    *done = 1;
    return KNO_FALSE;}
}

static lispval numeq_op(lispval exprs,kno_lexenv env,kno_stack stack)
{
  return bool_result(nd_reduce_op(stack,exprs,env,KNO_VOID,numeq_reduce));
}


KNO_FASTOP lispval numgt_reduce(lispval state,lispval step,int *done)
{
  if (KNO_EMPTYP(step)) {
    *done = 1;
    return step;}
  else if (PRED_FALSE(!(NUMBERP(step))))
    return kno_type_error(_("number"),"=?",step);
  else if (KNO_VOIDP(state))
    return step;
  else if ( (KNO_FIXNUMP(state)) &&
	    (KNO_FIXNUMP(step)) ) {
    long long istate = KNO_FIX2INT(state);
    long long istep = KNO_FIX2INT(step);
    if (istate > istep)
      return step;
    else {
      *done = 1;
      return KNO_FALSE;}}
  else if ( (KNO_FLONUMP(state)) &&
	    (KNO_FLONUMP(step)) )
    if ( (KNO_FLONUM(state)) > (KNO_FLONUM(step)) )
      return step;
    else {
      *done = 1;
      return KNO_FALSE;}
  else {
    int cmp = kno_numcompare(state,step);
    if (cmp > 0)
      return step;
    *done = 1;
    return KNO_FALSE;}
}

static lispval numgt_op(lispval exprs,kno_lexenv env,kno_stack stack)
{
  return bool_result(nd_reduce_op(stack,exprs,env,KNO_VOID,numgt_reduce));
}

KNO_FASTOP lispval numgte_reduce(lispval state,lispval step,int *done)
{
  if (KNO_EMPTYP(step)) {
    *done = 1;
    return step;}
  else if (PRED_FALSE(!(NUMBERP(step))))
    return kno_type_error(_("number"),"=?",step);
  else if (KNO_VOIDP(state))
    return step;
  else if ( (KNO_FIXNUMP(state)) &&
	    (KNO_FIXNUMP(step)) ) {
    long long istate = KNO_FIX2INT(state);
    long long istep = KNO_FIX2INT(step);
    if (istate >= istep)
      return step;
    else {
      *done = 1;
      return KNO_FALSE;}}
  else if ( (KNO_FLONUMP(state)) &&
	    (KNO_FLONUMP(step)) )
    if ( (KNO_FLONUM(state)) >= (KNO_FLONUM(step)) )
      return step;
    else {
      *done = 1;
      return KNO_FALSE;}
  else {
    int cmp = kno_numcompare(state,step);
    if (cmp >= 0)
      return step;
    *done = 1;
    return KNO_FALSE;}
}

static lispval numgte_op(lispval exprs,kno_lexenv env,kno_stack stack)
{
  return bool_result(nd_reduce_op(stack,exprs,env,KNO_VOID,numgte_reduce));
}

KNO_FASTOP lispval numlte_reduce(lispval state,lispval step,int *done)
{
  if (KNO_EMPTYP(step)) {
    *done = 1;
    return step;}
  else if (PRED_FALSE(!(NUMBERP(step))))
    return kno_type_error(_("number"),"=?",step);
  else if (KNO_VOIDP(state))
    return step;
  else if ( (KNO_FIXNUMP(state)) &&
	    (KNO_FIXNUMP(step)) ) {
    long long istate = KNO_FIX2INT(state);
    long long istep = KNO_FIX2INT(step);
    if (istate <= istep)
      return step;
    else {
      *done = 1;
      return KNO_FALSE;}}
  else if ( (KNO_FLONUMP(state)) &&
	    (KNO_FLONUMP(step)) )
    if ( (KNO_FLONUM(state)) <= (KNO_FLONUM(step)) )
      return step;
    else {
      *done = 1;
      return KNO_FALSE;}
  else {
    int cmp = kno_numcompare(state,step);
    if (cmp <= 0)
      return step;
    *done = 1;
    return KNO_FALSE;}
}

static lispval numlte_op(lispval exprs,kno_lexenv env,kno_stack stack)
{
  return bool_result(nd_reduce_op(stack,exprs,env,KNO_VOID,numlte_reduce));
}

KNO_FASTOP lispval numlt_reduce(lispval state,lispval step,int *done)
{
  if (KNO_EMPTYP(step)) {
    *done = 1;
    return step;}
  else if (PRED_FALSE(!(NUMBERP(step))))
    return kno_type_error(_("number"),"=?",step);
  else if (KNO_VOIDP(state))
    return step;
  else if ( (KNO_FIXNUMP(state)) &&
	    (KNO_FIXNUMP(step)) ) {
    long long istate = KNO_FIX2INT(state);
    long long istep = KNO_FIX2INT(step);
    if (istate < istep)
      return step;
    else {
      *done = 1;
      return KNO_FALSE;}}
  else if ( (KNO_FLONUMP(state)) &&
	    (KNO_FLONUMP(step)) )
    if ( (KNO_FLONUM(state)) < (KNO_FLONUM(step)) )
      return step;
    else {
      *done = 1;
      return KNO_FALSE;}
  else {
    int cmp = kno_numcompare(state,step);
    if (cmp < 0)
      return step;
    *done = 1;
    return KNO_FALSE;}
}

static lispval numlt_op(lispval exprs,kno_lexenv env,kno_stack stack)
{
  return bool_result(nd_reduce_op(stack,exprs,env,KNO_VOID,numlt_reduce));
}


static lispval handle_numeric_opcode(lispval opcode,lispval expr,
				     kno_lexenv env,
				     kno_stack _stack)
{
  switch (opcode) {
  case KNO_PLUS_OPCODE:
    return plus_op(KNO_CDR(expr),env,_stack);
  case KNO_MINUS_OPCODE:
    return minus_op(KNO_CDR(expr),env,_stack);
  case KNO_TIMES_OPCODE:
    return mult_op(KNO_CDR(expr),env,_stack);
  case KNO_FLODIV_OPCODE:
    return flodiv_op(KNO_CDR(expr),env,_stack);
  case KNO_NUMEQ_OPCODE:
    return numeq_op(KNO_CDR(expr),env,_stack);
  case KNO_GT_OPCODE:
    return numgt_op(KNO_CDR(expr),env,_stack);
  case KNO_GTE_OPCODE:
    return numgte_op(KNO_CDR(expr),env,_stack);
  case KNO_LTE_OPCODE:
    return numlte_op(KNO_CDR(expr),env,_stack);
  case KNO_LT_OPCODE:
    return numlt_op(KNO_CDR(expr),env,_stack);
  default:
    return kno_err("BadOPCODE","handle_numeric_opcode",NULL,expr);
  }
}
