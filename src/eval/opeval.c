static lispval op_get_headval(lispval head,kno_lexenv env,kno_stack eval_stack,
			      int *gc_headval);

lispval op_eval_expr(kno_stack eval_stack,
		     lispval head,lispval expr,kno_lexenv env,
		     int tail)
{
  int gc_head = 0, n_args = -1;
  u8_string label = NULL;
  if (head == KNO_SOURCEREF_OPCODE) {
    expr = handle_sourcerefs(expr,eval_stack);
    if (KNO_PAIRP(expr)) {
      head = KNO_CAR(sub_expr);
      label = head_label(head);}
    else return op_eval(eval_stack,sub_expr,env,tail);}
  lispval arg_exprs = KNO_EMPTY_LIST, headval =
    (KNO_OPCODEP(head)) ? (head) :
    (KNO_FCNIDP(head)) ? kno_fcnid_ref(head) :
    (KNO_APPLICABLEP(head)) ? (head) :
    (op_get_headval(head,env,eval_stack,&gc_head));
  if (ABORTED(headval)) return headval;
  else if (CHOICEP(headval))
    return eval_apply();
  else if (gc_head) {
    KNO_STACK_ADDREF(eval_stack,headval);}
  else NO_ELSE;
  kno_lisp_type headtype = KNO_TYPEOF(headval);
  kno_function f = (KNO_FUNCTION_TYPEP(headtype)) ?
    ((kno_function)headval) :
    (NULL);
  int isndop = (f) && (f->fcn_call_flags & KNO_FCN_CALL_NDCALL);
  int flags = eval_stack->stack_flags;
  lispval restore_op = eval_stack->stack_point;
  lispval restore_env = eval_stack->eval_env;
  u8_string restore_label = eval_stack->stack_label;
  if (env == restore_env) restore_env = NULL;
  eval_stack->stack_point = expr;
  if (restore_env)
    eval_stack->stack_point = env;
  eval_stack->stack_label = label;
  switch (headtype) {
  case kno_evalfn_type: {
    struct KNO_EVALFN *handler = (kno_evalfn)headval;
    /* These are evalfns which do all the evaluating themselves */
    if (handler->evalfn_name) eval_stack->stack_label=handler->evalfn_name;
    lispval eval_result = handler->evalfn_handler(expr,env,eval_stack);
    eval_stack->stack_point = restore_op;
    eval_stack->eval_env = restore_env;
    eval_stack->stack_label = restore_label;
    return eval_result;}
  case kno_macro_type: {
    /* These expand into expressions which are
       then evaluated. */
    struct KNO_MACRO *macrofn=
      kno_consptr(struct KNO_MACRO *,headval,kno_macro_type);
    eval_stack->stack_type="macro";
    lispval xformer = macrofn->macro_transformer;
    lispval new_expr = kno_dcall((kno_stack)eval_stack,xformer,1,&expr);
    lispval eval_result = VOID;
    if (KNO_ABORTED(new_expr)) {
      u8_string show_name = macrofn->macro_name;
      if (show_name == NULL) show_name = label;
      eval_result = kno_err(kno_SyntaxError,_("macro expansion"),show_name,new_expr);}
    else eval_result = kno_eval(new_expr,env,eval_stack,tail,-1);
    kno_decref(new_expr);
    return eval_result;}
  case kno_opcode_type:
    if (KNO_SPECIAL_OPCODEP(head))
      return handle_special_opcode();
    else if (KNO_CALL_OPCODEP(head)) {
      if (KNO_CALLN_OPCODEP(head))
	n_args = pop_arg(expr);
      else n_args = ((KNO_IMMEDIATE_DATA(head))&0xF);}
    else NO_ELSE;
    break;
  case kno_cprim_type: case kno_lambda_type: {
    struct KNO_FUNCTION *f = (struct KNO_FUNCTION *) headval;
    if (f->fcn_name) eval_stack->stack_label=f->fcn_name;
    result = eval_apply(f->fcn_name,headval,-1,KNO_CDR(expr),
			env,eval_stack,tail);
    break;}
  default: {
    if ( (f == NULL) && (!(KNO_APPLICABLEP(head))) ) 
      return kno_err(kno_NotAFunction,headval,expr);}}
  if (n_args < 0) n_args = list_length(KNO_CDR(expr));
  int call_width = n_args;
  if ( (f) && (f->fcn_call_width > call_width) )
    call_width = f->fcn_call_width;
  lispval args[call_width];
  int nd_args = 0, qc_args = 0, gc_args = 0;
  int i=0; while (i < n_args) {
    lispval arg_expr = pop_arg(arg_exprs);
    lispval arg_value = op_eval(stack,arg_expr,env,0);
    if ( (KNO_ABORTED(arg_value)) ||
	 ( (nd_op) && (KNO_EMPTYP(arg_value)) ) ) {
      kno_decref_vec(args,i);
      return arg_value;}
    if (KNO_MALLOCD_CONSP(arg_value)) gc_args++;
    if (KNO_PRECHOICEP(arg_value))
      arg_value = kno_simplify_choice(arg_value);
    if (KNO_CHOICEP(arg_value)) nd_args++;
    if (KNO_QCHOICEP(arg_value)) qc_args++;
    args[i++] = arg_value;}
  if ( ((nd_args) && (isndop == 0) ) || (qc_args) )
    return kno_call((kno_stack)stack,headval,n_args,argvec);
  else if ( (nd_args == 0) || (nd_op) )
    return kno_dcall((kno_stack)stack,headval,n_args,argvec);
  else return kno_call(stack,headval,n_args,argvec);
}
      
static lispval op_get_headval(lispval head,kno_lexenv env,kno_stack eval_stack,
			      int *gc_headval)
{
  lispval headval = VOID;
  if (KNO_CONSP(head)) {
    kno_lisp_type ctype = KNO_CONSTPR_TYPE(head);
    switch (ctype) {
    case kno_pair_type: case kno_choice_type: {
      headval=op_eval(head,env,eval_stack);
      headval=simplify_value(headval);
      break;}
    case kno_cprim_type: case kno_lambda_type:
    case kno_ffi_type: case kno_rpcproc_type:
    case kno_evalfn_type: case kno_macro_type:
      return head;
    default:
      if (KNO_APPLICABLEP(head))
	return head;
      else return kno_err("InvalidOP","op_get_headval",NULL,head);}}
  else if (KNO_LEXREFP(head))
    headval = kno_lexref(head,env);
  else if (KNO_SYMBOLP(head)) {
    if (head == quote_symbol) return KNO_QUOTE_OPCODE;
    else headval=kno_symeval(head,env);}
  else if (KNO_OPCODEP(head))
    headval=head;
  else return kno_err("InvalidOP","op_get_headval",NULL,head);
  if (KNO_FCNIDP(headval))
    return kno_fcnid_ref(headval);
  else if (KNO_CONSP(headval)) {
    kno_lisp_type ctype = KNO_CONSTPR_TYPE(head);
    if ( (KNO_APPLICABLE_TYPEP(ctype)) ||
	 (ctype == kno_evalfn_type) ||
	 (ctype == kno_macro_type) )
      return headval;
    else return kno_err("InvalidOPVal","op_get_headval",NULL,headval);}
  else return kno_err("InvalidOPVal","op_get_headval",NULL,headval);
}

