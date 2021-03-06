#define INLINE_DEF static U8_MAYBE_UNUSED

#include "kno/eval.h"
#include "kno/opcodes.h"
#include "kno/cprims.h"

#ifndef INIT_ARGBUF_LEN
#define INIT_ARGBUF_LEN 7
#endif

extern u8_condition BadExpressionHead;

#define MU U8_MAYBE_UNUSED

#define BAD_ARGP(v) \
  (RARELY ( (KNO_IMMEDIATEP(v)) && ( (KNO_VOIDP(v)) || (KNO_ABORTP(v)) ) ) )

static u8_string MU opcode_name(lispval opcode)
{
  long opcode_offset = (KNO_GET_IMMEDIATE(opcode,kno_opcode_type));
  if ((opcode_offset<kno_opcodes_length) &&
      (kno_opcode_names[opcode_offset]))
    return kno_opcode_names[opcode_offset];
  else return NULL;
}

#define VEC_LENGTH KNO_VECTOR_LENGTH
#define VEC_ELTS   KNO_VECTOR_ELTS
#define opname     opcode_name

lispval eval_schemap(lispval expr,kno_lexenv env,kno_stack stack);
lispval eval_choice(lispval expr,kno_lexenv env,kno_stack stack);
int eval_args(int argc,lispval *into,lispval exprs,
	      kno_lexenv env,kno_stack stack,
	      int prune);
lispval eval_lambda(kno_lambda into,kno_lambda lambda,kno_lexenv env);

lispval lisp_eval(lispval head,lispval expr,
			kno_lexenv env,kno_stack stack,
			int tail);
lispval vm_eval(lispval head,lispval expr,
		kno_lexenv env,kno_stack stack,
		int tail);

lispval lambda_call(kno_stack stack,
		    struct KNO_LAMBDA *proc,
		    int n,kno_argvec args,
		    int free_given,
		    int tail);

lispval call_evalfn(lispval evalop,lispval expr,kno_lexenv env,
		    kno_stack stack,int tail);

lispval get_evalop(lispval head,kno_lexenv env,kno_stack stack);
lispval _lexref_error(lispval ref,int up,kno_lexenv env,kno_lexenv root);


INLINE_DEF lispval fastget(lispval table,lispval key)
{
  kno_lisp_type argtype = KNO_TYPEOF(table);
  switch (argtype) {
  case kno_schemap_type:
    return kno_schemap_get((kno_schemap)table,key,KNO_UNBOUND);
  case kno_hashtable_type:
    return kno_hashtable_get((kno_hashtable)table,key,KNO_UNBOUND);
  default: return kno_get(table,key,KNO_UNBOUND);}
}
INLINE_DEF lispval eval_lexref(lispval lexref,kno_lexenv env_arg)
{
  lispval v;
  int code = KNO_GET_IMMEDIATE(lexref,kno_lexref_type);
  int up = code/32, across = code%32;
  kno_lexenv env = env_arg;
  while ((env) && (up)) {
    if (RARELY((env->env_copy!=NULL))) env = env->env_copy;
    env = env->env_parent;
    up--;}
  if (KNO_USUALLY(env != NULL)) {
    if (RARELY((env->env_copy != NULL))) env = env->env_copy;
    if (env->env_vals) {
      int vals_len = env->env_bits & (0xFF);
      if (across < vals_len)
	v = env->env_vals[across];
      else v = KNO_ERROR;}
    else {
      lispval bindings = env->env_bindings;
      if (KNO_USUALLY(KNO_SCHEMAPP(bindings))) {
	struct KNO_SCHEMAP *s = (struct KNO_SCHEMAP *)bindings;
	if ( across < s->schema_length) {
	  v = s->table_values[across];}
	else v = KNO_ERROR;}
      else v = KNO_ERROR;}}
  else v = KNO_ERROR;
  if (KNO_ABORTED(v))
    return _lexref_error(lexref,up,env,env_arg);
  else if (KNO_CONSP(v)) {
    if (RARELY((KNO_CONS_TYPEOF(((kno_cons)v))) == kno_prechoice_type))
      return _kno_make_simple_choice(v);
    else return kno_incref(v);}
  else return v;
}
INLINE_DEF lispval symeval(lispval symbol,kno_lexenv env)
{
  if (env == NULL) return KNO_VOID;
  if (env->env_copy) env = env->env_copy;
  while (env) {
    lispval val = fastget(env->env_bindings,symbol);
    if (val == KNO_UNBOUND)
      env = env->env_parent;
    else return val;
    if ((env) && (env->env_copy))
      env = env->env_copy;}
  return KNO_VOID;
}
INLINE_DEF lispval eval_symbol(lispval symbol,kno_lexenv env)
{
  if (env == NULL)
    return kno_err("NoEnvironment","eval_symbol",NULL,symbol);
  if (env->env_copy) env = env->env_copy;
  while (env) {
    lispval val = fastget(env->env_bindings,symbol);
    if (val == KNO_UNBOUND)
      env = env->env_parent;
    else if (val == KNO_VOID)
      break;
    else return val;
    if ((env) && (env->env_copy))
      env = env->env_copy;}
  return kno_err(kno_UnboundIdentifier,"kno_eval",
		 KNO_SYMBOL_NAME(symbol),symbol);
}
INLINE_DEF lispval eval_fcnid(lispval fcnid)
{
  lispval v = kno_fcnid_ref(fcnid);
  kno_incref(v);
  return v;
}
INLINE_DEF U8_MAYBE_UNUSED lispval get_arg(lispval expr,int i)
{
  while (KNO_PAIRP(expr))
    if ((KNO_PAIRP(KNO_CAR(expr))) &&
        (KNO_EQ(KNO_CAR(KNO_CAR(expr)),_kno_comment_symbol)))
      expr = KNO_CDR(expr);
    else if (i == 0) return KNO_CAR(expr);
    else {expr = KNO_CDR(expr); i--;}
  return KNO_VOID;
}
INLINE_DEF U8_MAYBE_UNUSED lispval get_body(lispval expr,int i)
{
  while (KNO_PAIRP(expr))
    if (i == 0) break;
    else if ((KNO_PAIRP(KNO_CAR(expr))) &&
             (KNO_EQ(KNO_CAR(KNO_CAR(expr)),_kno_comment_symbol)))
      expr = KNO_CDR(expr);
    else {expr = KNO_CDR(expr); i--;}
  return expr;
}

static U8_MAYBE_UNUSED lispval unwrap_qchoice(lispval val)
{
  if (QCHOICEP(val)) {
    lispval choice_val = KNO_QCHOICEVAL(val);
    kno_incref(choice_val); kno_decref(val);
    return choice_val;}
  else return val;
}

INLINE_DEF lispval doeval(lispval x,kno_lexenv env,
			     kno_stack stack,
			     int tail)
{
  switch (KNO_PTR_MANIFEST_TYPE(x)) {
  case kno_cons_type: {
    kno_lisp_type type = KNO_CONS_TYPEOF(x);
    switch (type) {
    case kno_pair_type: {
      lispval op = KNO_CAR(x);
      if (KNO_OPCODEP(op))
	return vm_eval(op,x,env,stack,tail);
      return lisp_eval(op,x,env,stack,tail);}
    case kno_choice_type: case kno_prechoice_type:
      return eval_choice(x,env,stack);
    case kno_schemap_type:
      return eval_schemap(x,env,stack);
    case kno_slotmap_type:
      return kno_deep_copy(x);
    default:
      return kno_incref(x);}}
  case kno_immediate_ptr_type: {
    switch (KNO_IMMEDIATE_TYPE(x)) {
    case kno_lexref_type:
      return eval_lexref(x,env);
    case kno_symbol_type:
      return eval_symbol(x,env);
    case kno_fcnid_type:
      return eval_fcnid(x);
    default:
      return x;}}
  default:
    return x;}
}

static MU void unwrap_qchoices(int n,lispval *args)
{
  lispval *scan = args, *limit = args+n;
  while (scan<limit) {
    lispval arg = *scan;
    if (KNO_QCHOICEP(arg)) {
      lispval qval = KNO_QCHOICEVAL(arg);
      kno_incref(qval);
      kno_decref(arg);
      *scan=qval;}
    scan++;}
}

KNO_FASTOP MU lispval eval_arg(lispval x,kno_lexenv env,kno_stack stack)
{
  lispval v = doeval(x,env,stack,0);
  if (KNO_VOIDP(v))
    return kno_err(kno_VoidArgument,"eval_arg",NULL,x);
  else if (KNO_PRECHOICEP(v))
    return kno_simplify_choice(v);
  else return v;
}

static int testeval(lispval expr,kno_lexenv env,int fail_val,
		    lispval *whoops,struct KNO_STACK *s) U8_MAYBE_UNUSED;

INLINE_DEF int testeval(lispval expr,kno_lexenv env,int fail_val,
			lispval *whoops,
			kno_stack _stack)
{
  lispval val = doeval(expr,env,_stack,0);
  if (KNO_CONSP(val)) {
    kno_decref(val);
    return 1;}
  else if (KNO_ABORTED(val)) {
    *whoops = val;
    return -1;}
  else if (KNO_FALSEP(val))
    return 0;
  else if (KNO_EMPTYP(val))
    return fail_val;
  else if (KNO_VOIDP(val)) {
    *whoops = KNO_ERROR;
    return KNO_ERR(-1,kno_VoidBoolean,"testeval",NULL,expr);}
  else return 1;
}
#define TESTEVAL_FAIL_FALSE 0
#define TESTEVAL_FAIL_TRUE  1

INLINE_DEF lispval _pop_arg(lispval *scan)
{
  lispval expr = *scan;
  if (PAIRP(expr)) {
    lispval arg = KNO_CAR(expr), next = KNO_CDR(expr);
    if (KNO_CONSP(arg)) KNO_PREFETCH((struct KNO_CONS *)arg);
    if (KNO_CONSP(next)) KNO_PREFETCH((struct KNO_CONS *)next);
    *scan = next;
    return arg;}
  else return VOID;
}

#define pop_arg(args) (_pop_arg(&args))

#define simplify_value(v) \
  ( (KNO_PRECHOICEP(v)) ? (kno_simplify_choice(v)) : (v) )

lispval eval_body_error(u8_context cxt,u8_string label,lispval body);

INLINE_DEF
lispval eval_body(lispval body,kno_lexenv env,kno_stack stack,
		  u8_context cxt,u8_string label,
		  int tail)
{
  if (KNO_PAIRP(body)) {
    lispval head = KNO_CAR(body);
    if (KNO_OPCODEP(head))
      return vm_eval(head,body,env,stack,tail);
    KNO_STACK_SET_TAIL(stack,0);
    lispval scan = body; while (PAIRP(scan)) {
      lispval subex = pop_arg(scan);
      if (KNO_EMPTY_LISTP(scan)) {
	KNO_STACK_SET_TAIL(stack,tail);
	return doeval(subex,env,stack,tail);}
      else if (PAIRP(scan)) {
	lispval v = doeval(subex,env,stack,0);
	if (KNO_IMMEDIATEP(v)) {
	  if (KNO_ABORTED(v))
	    return v;
	  else if (v==KNO_TAIL)
	    return kno_err(kno_TailArgument,cxt,label,subex);
	  else NO_ELSE;}
	else kno_decref(v);}
      else return eval_body_error(cxt,label,body);}
    return KNO_VOID;}
  else if (KNO_EMPTY_LISTP(body)) return KNO_VOID;
  else return eval_body_error(cxt,label,body);
}

KNO_FASTOP kno_lexenv init_static_env
  (int n,kno_lexenv parent,
   struct KNO_SCHEMAP *bindings,struct KNO_LEXENV *envstruct,
   lispval *vars,lispval *vals,
   int zeromem)
{
  if (zeromem) {
    KNO_INIT_STATIC_CONS(envstruct,kno_lexenv_type);
    KNO_INIT_STATIC_CONS(bindings,kno_schemap_type);}
  else {
    KNO_INIT_STACK_CONS(envstruct,kno_lexenv_type);
    KNO_INIT_STACK_CONS(bindings,kno_schemap_type);}
  bindings->table_bits = KNO_SCHEMAP_STATIC_SCHEMA | KNO_SCHEMAP_STATIC_VALUES;
  bindings->table_schema = vars;
  bindings->table_values = vals;
  bindings->schema_length = n;
  // u8_init_rwlock(&(bindings->table_rwlock));
  envstruct->env_bindings = LISP_CONS((bindings));
  envstruct->env_exports = KNO_VOID;
  envstruct->env_parent  = parent;
  envstruct->env_copy    = NULL;
  envstruct->env_vals    = (n<256) ? (vals) : (NULL);
  envstruct->env_bits    = (n<256) ? (n) : (0);
  return envstruct;
}

#define INIT_STACK_ENV(stack,name,parent,n)		    \
  struct KNO_SCHEMAP name ## _bindings = { 0 };		    \
  struct KNO_LEXENV _ ## name = { 0 }, *name=&_ ## name;    \
  lispval name ## _vars[n];				    \
  lispval name ## _vals[n];				    \
  kno_init_elts(name ## _vars,n,KNO_VOID);		    \
  kno_init_elts(name ## _vals,n,KNO_VOID);		    \
  stack->stack_bits |=					    \
    KNO_STACK_FREE_ENV | KNO_STACK_OWNS_ENV;		    \
  stack->stack_args = name ## _vals;			    \
  stack->stack_argc = n;				    \
  stack->stack_width = n;				    \
  stack->eval_env =					    \
    init_static_env(n,parent,				    \
		    &name ## _bindings,			    \
		    &_ ## name,				    \
		    name ## _vars,			    \
		    name ## _vals,			    \
		    0);					    \
  _ ## name.env_vals = name ## _vals;			    \


#define INIT_STACK_SCHEMA(stack,name,parent,n,schema)	    \
  struct KNO_SCHEMAP name ## _bindings = { 0 };		    \
  struct KNO_LEXENV _ ## name = { 0 }, *name=&_ ## name;    \
  lispval name ## _vals[n];				    \
  kno_init_elts(name ## _vals,n,KNO_VOID);		    \
  stack->stack_args = name ## _vals;			    \
  stack->stack_argc = n;				    \
  stack->stack_width = n;				    \
  stack->stack_bits |=					    \
    KNO_STACK_FREE_ENV|KNO_STACK_OWNS_ENV;		    \
  stack->eval_env =					    \
    init_static_env(n,parent,				    \
		    &name ## _bindings,			    \
		    &_ ## name,				    \
		    schema,				    \
		    name ## _vals,			    \
		    0);					    \
  _ ## name.env_vals = name ## _vals

#define ENV_STACK_FLAGS (KNO_STACK_OWNS_ENV|KNO_STACK_FREE_ENV)

#define PUSH_STACK_ENV(name,parent,n,caller)				\
  struct KNO_STACK _ ## name ## _stack = { 0 };				\
  struct KNO_STACK *name ## _stack = &(_ ## name ## _stack);		\
  struct KNO_SCHEMAP name ## _bindings = { 0 };				\
  struct KNO_LEXENV _ ## name = { 0 }, *name=&_ ## name;		\
  lispval name ## _vars[n];						\
  lispval name ## _vals[n];						\
  KNO_SETUP_STACK((name ## _stack),# name);				\
  KNO_STACK_SET_BITS(name ## _stack,ENV_STACK_FLAGS);			\
  KNO_STACK_SET_CALLER((name ## _stack),(caller));			\
  kno_init_elts(name ## _vars,n,KNO_VOID);				\
  kno_init_elts(name ## _vals,n,KNO_VOID);				\
  (name ## _stack)->stack_bits |=					\
    KNO_STACK_FREE_ENV|KNO_STACK_OWNS_ENV;				\
  (name ## _stack)->stack_args = name ## _vals;				\
  (name ## _stack)->stack_argc = n;					\
  (name ## _stack)->stack_width = n;					\
  (name ## _stack)->eval_env =						\
    init_static_env(n,parent,						\
		    &name ## _bindings,					\
		    &_ ## name,						\
		    name ## _vars,					\
		    name ## _vals,					\
		    0);							\
  _ ## name.env_vals = name ## _vals;\
  KNO_PUSH_STACK(name ## _stack)


#define KNO_USE_ARGP(arg)				\
  ( ! ( (KNO_IMMEDIATEP(arg)) &&			\
	   ( (KNO_EMPTYP(arg)) || (KNO_VOIDP(arg)) ||	\
	     (KNO_ABORTED(arg)) ) ) )

KNO_FASTOP U8_MAYBE_UNUSED
void reset_env(kno_lexenv env)
{
  /* This decrefs any dynamic copy of this env, resetting it to its initial bindings. */
  if (env->env_copy) {
    kno_free_lexenv(env->env_copy);
    env->env_copy=NULL;}
}

KNO_FASTOP U8_MAYBE_UNUSED
void reset_stack_env(kno_stack stack)
{
  kno_lexenv env = stack->eval_env;
  if (USUALLY(env != NULL)) {
    stack->eval_env=NULL;
    stack->stack_bits &= (~(KNO_STACK_FREE_ENV));
    if (env->env_copy) {
      kno_free_lexenv(env->env_copy);
      env->env_copy=NULL;}}
}

#define set_stack_label(stack,label) (stack)->stack_label=label

#define KNO_INIT_ITER_LOOP(name,var,value,n_vars,caller,parent_env)	\
  u8_byte iter_label_buf[32];						\
  u8_string label = (KNO_SYMBOLP(var)) ?				\
    u8_bprintf(iter_label_buf,"%s.%s", # name,KNO_SYMBOL_NAME(var)) :	\
    ((u8_string)(# name));						\
  KNO_START_EVALX(name ## _stack,# name,expr,NULL,caller,5);		\
  set_stack_label((kno_stack)caller,label);				\
  kno_add_stack_ref(caller,value);					\
  INIT_STACK_ENV(name ## _stack,name,parent_env,n_vars);		\
  kno_lexenv name ## _env = &(_ ## name)

/* Environment utilities */

KNO_FASTOP int check_bindexprs(lispval bindexprs,lispval *why_not)
{
  if (PAIRP(bindexprs)) {
    int n = 0; KNO_DOLIST(bindexpr,bindexprs) {
      lispval var = kno_get_arg(bindexpr,0);
      if (VOIDP(var)) {
	*why_not = kno_err(kno_BindSyntaxError,NULL,NULL,bindexpr);
	return -1;}
      else n++;}
    return n;}
  else return -1;
}

static MU lispval parse_control_spec
(lispval expr,lispval *varp,lispval *count_var,lispval *whole_var,
 kno_lexenv env,kno_stack _stack)
{
  lispval control_expr = kno_get_arg(expr,1);
  if (PAIRP(control_expr)) {
    lispval var = kno_get_arg(control_expr,0);
    lispval val_expr = kno_get_arg(control_expr,1), val;
    lispval ivar = kno_get_arg(control_expr,2);
    lispval wvar = kno_get_arg(control_expr,3);
    if (KNO_VOIDP(ivar)) ivar = KNOSYM_HASHMARK;
    else if (KNO_FALSEP(ivar)) ivar = KNO_VOID;
    else NO_ELSE;
    if (KNO_VOIDP(wvar)) wvar = KNOSYM_ATMARK;
    else if (KNO_FALSEP(wvar)) wvar = KNO_VOID;
    else NO_ELSE;
    if (VOIDP(val_expr))
      return kno_err(kno_TooFewExpressions,"DO...",NULL,control_expr);
    else if (!(SYMBOLP(var)))
      return kno_err(kno_SyntaxError,
		     _("identifier is not a symbol"),"eltvar",control_expr);
    else if (!((VOIDP(ivar)) || (SYMBOLP(ivar))))
      return kno_err(kno_SyntaxError,
		     _("identifier is not a symbol"),"countvar",control_expr);
    else if (!((VOIDP(wvar)) || (SYMBOLP(wvar))))
      return kno_err(kno_SyntaxError,
		     _("identifier is not a symbol"),"wholevar",control_expr);
    val = kno_eval(val_expr,env,_stack);
    if (KNO_ABORTED(val))
      return val;
    *varp = var;
    if (count_var) *count_var = ivar;
    if (whole_var) *whole_var = wvar;
    return val;}
  else if (KNO_SYMBOLP(control_expr)) {
    lispval var = control_expr;
    lispval val = eval_symbol(var,env);
    if (KNO_ABORTED(val))
      return val;
    *varp = var;
    if (count_var) *count_var = KNOSYM_HASHMARK;
    if (whole_var) *whole_var = KNOSYM_ATMARK;
    return val;}
  else if (VOIDP(control_expr))
    return kno_err(kno_TooFewExpressions,"do/for...",NULL,expr);
  else return kno_err(kno_SyntaxError,"do/for...",NULL,expr);
}

KNO_FASTOP void init_iter_env(kno_schemap bindings,lispval value,
			      lispval item_var,lispval count_var,
			      lispval val_var)
{
  lispval *vars = bindings->table_schema;
  lispval *vals = bindings->table_values;
  vars[0]=item_var;
  if ( (SYMBOLP(count_var)) && (SYMBOLP(val_var)) ) {
    vars[1]=count_var;
    vars[2]=val_var;
    vals[2]=kno_incref(value);
    bindings->schema_length=3;}
  else if (SYMBOLP(count_var)) {
    vars[1]=count_var;
    bindings->schema_length=2;}
  else if (SYMBOLP(val_var)) {
    vars[1]=KNOSYM_HASHMARK;
    vars[2]=val_var;
    vals[2]=kno_incref(value);
    bindings->schema_length=3;}
  else bindings->schema_length=1;
}

