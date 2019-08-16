static lispval op_get_headval(lispval head,kno_lexenv env,kno_stack eval_stack,
			   int *gc_headval);

lispval op_eval_expr(struct KNO_STACK *eval_stack,lispval expr,kno_lexenv env,
		     int tail)
{
  int gc_head = 0, n_args = -1;
  lispval headval = (KNO_OPCODEP(head)) ? (head) :
    (get_headval(head,env,eval_stack,&gc_head));
  if (gc_head) {
    KNO_ADD_TO_CHOICE(eval_stack->stack_vals,headval);}
  u8_string label = NULL;
  if (head == KNO_SOURCEREF_OPCODE) {
    expr = handle_sourcerefs(expr,eval_stack);
    head = (KNO_PAIRP(expr)) ? (KNO_CAR(expr)) : (KNO_VOID);
    label = head_label(head);}
  int flags = eval_stack->stack_flags;
  lispval restore_op = eval_stack->stack_op;
  lispval restore_env = eval_stack->stack_env;
  u8_string restore_label = eval_stack->stack_label;
  if (env == restore_env) restore_env = NULL;
  eval_stack->stack_op = expr;
  if (restore_env)
    eval_stack->stack_op = env;
  eval_stack->stack_label = label;
  kno_lisp_type headtype = KNO_TYPEOF(headval);
  switch (headtype) {
  case kno_evalfn_type: {
    struct KNO_EVALFN *handler = (kno_evalfn)headval;
    /* These are evalfns which do all the evaluating themselves */
    if (handler->evalfn_name) eval_stack->stack_label=handler->evalfn_name;
    lispval r = handler->evalfn_handler(expr,env,eval_stack);
    eval_stack->stack_op = restore_op;
    eval_stack->stack_env = restore_env;
    eval_stack->stack_label = restore_label;
    return result;
    break;}
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
      result = kno_err(kno_SyntaxError,_("macro expansion"),show_name,new_expr);}
    else result = kno_stack_eval(new_expr,env,eval_stack,tail);
    kno_decref(new_expr);
    break;}
  case kno_opcode_type:
    if (KNO_SPECIAL_OPCODEP(head)) {
      result = handle_special_opcode();
      break;}
    else if (KNO_CALL_OPCODEP(head)) {
      if (KNO_CALLN_OPCODEP(head))
	n_args = pop_arg(expr);
      else n_args = ((KNO_IMMEDIATE_DATA(head))&0xF);}
    else NO_ELSE;
  case kno_cprim_type: case kno_lambda_type: {
    struct KNO_FUNCTION *f = (struct KNO_FUNCTION *) headval;
    if (f->fcn_name) eval_stack->stack_label=f->fcn_name;
    result = eval_apply(f->fcn_name,headval,-1,KNO_CDR(expr),
			env,eval_stack,tail);
    break;}
  default:
    if (KNO_FUNCTIONP(head)) {
      kno_function f = (kno_function) head;
      if (f->fcn_name) eval_stack->stack_label=f->fcn_name;}
    if (n_args<0) n_args = list_length(expr);
    lispval argvec[n_args];
    int i = 0; while ( (i < n_args) && (PAIRP(expr)) ) {
      lispval arg_expr = pop_arg(expr);
      lispval arg = op_eval(eval_stack,arg_expr,env,0);
      if (KNO_ABORTED(arg)) {
	result = arg;
	break;}
      argvec[i++] = n_args;}
    
      
    
    if (kno_function_types[headtype]) {
      struct KNO_FUNCTION *f = (struct KNO_FUNCTION *) headval;
      result = eval_apply(f->fcn_name,headval,-1,KNO_CDR(expr),env,
			  eval_stack,tail);}
    else if (kno_applyfns[headtype]) {
      u8_context cxt = kno_type_names[headtype];
      if (cxt == NULL) cxt = "extfcn";
      result = eval_apply(cxt,headval,-1,KNO_CDR(expr),env,eval_stack,tail);}
    else if (KNO_ABORTED(headval)) {
      result=headval;}
    else if (PRED_FALSE(VOIDP(headval))) {
      result=kno_err(kno_UnboundIdentifier,"for function",
		     ((SYMBOLP(head))?(SYM_NAME(head)):
		      (NULL)),
		     head);}
    else if (EMPTYP(headval) )
      result=EMPTY;
    else result=kno_err(kno_NotAFunction,NULL,NULL,headval);}
  if (!(KNO_CHECK_PTR(result)))
    return kno_err(kno_NullPtr,"pair_eval",NULL,expr);
  else if (!tail) {
    if (KNO_TAILCALLP(result))
      result=kno_finish_call(result);
    else {}}
  return simplify_value(result);
}

static lispval op_get_headval(lispval head,kno_lexenv env,kno_stack eval_stack,
			      int *gc_headval)
{
  lispval headval = VOID;
  if (KNO_IMMEDIATEP(head)) {
    kno_lisp_type head_type = KNO_IMMEDIATE_TYPE(head);
    switch (head_type)
    case kno_lexref_type: {
      headval=kno_lexref(head,env);
      if ( (KNO_CONSP(headval)) && (KNO_MALLOCD_CONSP(headval)) )
	*gc_headval=1;
      return headval;}
  case kno_symbol_type:
    if (head == quote_symbol)
      return KNO_QUOTE_OPCODE;
    else {
      headval=kno_symeval(head,env);
      if (KNO_CONSP(headval)) *gc_headval=1;
      return headval;}
  default:
    return kno_err("NotEvalable","op_pair_eval",NULL,head);
  }
  else if ( (KNO_PAIRP(head)) || (CHOICEP(head)) ) {
    headval=op_eval(head,env,eval_stack);
    headval=simplify_value(headval);
    if (MALLOCD_CONSP(headval)) *gc_headval=1;
    return headval;}
  else return kno_err("NotEvalable","op_pair_eval",NULL,head);
}
