/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2015-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_APPLY_STACKS_C 1

#define KNO_INLINE_FCNIDS 1
#define KNO_INLINE_STACKS 1
#define KNO_INLINE_LEXENV 1
#define KNO_INLINE_APPLY  1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/lexenv.h"
#include "kno/apply.h"

#include <libu8/u8printf.h>
#include <libu8/u8contour.h>
#include <libu8/u8strings.h>
#include <libu8/u8logging.h>

#ifndef U8_MSG_INFO
#define U8_MSG_INFO (-(LOG_INFO))
#endif

#include <errno.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <stdarg.h>

u8_condition kno_NoStackChecking=
  _("Stack checking is not available in this build of Kno");
u8_condition kno_StackTooSmall=
  _("This value is too small for an application stack limit");
u8_condition kno_StackTooLarge=
  _("This value is larger than the available stack space");
u8_condition kno_BadStackSize=
  _("This is an invalid stack size");
u8_condition kno_BadStackFactor=
  _("This is an invalid stack resize factor");
u8_condition kno_InternalStackSizeError=
  _("Internal stack size didn't make any sense, punting");

#if ((KNO_THREADS_ENABLED)&&(KNO_USE_TLS))
u8_tld_key kno_stack_limit_key;
#elif ((KNO_THREADS_ENABLED)&&(HAVE_THREAD_STORAGE_CLASS))
__thread ssize_t kno_stack_limit = -1;
#else
ssize_t kno_stack_limit = -1;
#endif

#if (KNO_USE_TLS)
u8_tld_key kno_stackptr_key;
#elif (U8_USE__THREAD)
__thread struct KNO_STACK *kno_stackptr=NULL;
#else
struct KNO_STACK *kno_stackptr=NULL;
#endif

#if KNO_DEBUG_STACKS
int kno_debug_stacks = 0;
#endif

static lispval stack_entry_symbol, eval_target_symbol, opaque_symbol,
  pbound_symbol, pargs_symbol;
static lispval null_sym, unbound_sym, void_sym, default_sym, nofile_symbol;

/* This is for the C stack */
lispval kno_default_stackspec = VOID;

static lispval copy_args(int width,kno_argvec args);

/* Call stack (not used yet) */

KNO_EXPORT void _kno_stack_push_error(kno_stack stack,u8_context loc)
{
  kno_stack cur = kno_stackptr;
  kno_stack caller = stack->stack_caller;
  u8_condition cond =
    (cur == NULL) ? ("NullStackPtr(push)") :
    (caller == NULL) ? ("NullCaller(push)") :
    (!(STACK_LIVEP(caller))) ? ("DeadCaller(push)") :
    (caller < kno_stackptr) ? ("CallerAlreadyExited(push)") :
    (caller > kno_stackptr) ? ("CallerNotCurrent(push)") :
    ("OddStackError(push)");
  u8_log(LOGCRIT,cond,
	 "Push stack %p; caller %p; stackptr %p @ %s",stack,caller,cur,loc);
  kno_raisex(cond,loc,NULL);
}

KNO_EXPORT void _kno_stack_pop_error(kno_stack stack,u8_context loc)
{
  kno_stack cur = kno_stackptr;
  kno_stack caller = stack->stack_caller;
  u8_condition cond =
    (cur == NULL) ? ("NullStackPtr(pop)") :
    (!(STACK_LIVEP(cur))) ? ("DeadStack(pop)") :
    (caller == NULL) ? ("NullCaller(pop)") :
    (stack < kno_stackptr) ? ("FrameAlreadyExited(pop)") :
    (stack > kno_stackptr) ? ("FrameNotCurrent(pop)") :
    ("OddStackError(pop)");
  u8_log(LOGCRIT,cond,
	 "Pop stack %p; caller %p; stackptr %p @ %s",stack,caller,cur,loc);
  kno_raisex(cond,loc,NULL);
}

KNO_EXPORT void kno_stackvec_grow(struct KNO_STACKVEC *sv,int len)
{
  if ( (KNO_STACKVEC_LEN(sv)) > len ) return;
  lispval *elts = KNO_STACKVEC_ELTS(sv);
  if (elts == NULL) {
    elts = u8_alloc_n(len,lispval);
    sv->elts  = elts;
    sv->count = 0;
    sv->len   = KNO_STACKVEC_HEAPBIT|len;}
  else {
    int count     = KNO_STACKVEC_COUNT (sv);
    int onheap    = KNO_STACKVEC_ONHEAP(sv);
    if (onheap) {
      lispval *new_elts = u8_realloc(elts,len*sizeof(lispval));
      sv->elts = new_elts;
      sv->len  = KNO_STACKVEC_HEAPBIT|len;
      return;}
    else {
      lispval *new_elts = u8_alloc_n(len,lispval);
      memcpy(new_elts,elts,count*sizeof(lispval));
      sv->elts = new_elts;
      sv->len  = KNO_STACKVEC_HEAPBIT|len;
      return;}}
}

KNO_EXPORT void _kno_free_stackvec(struct KNO_STACKVEC *sv)
{
  __kno_free_stackvec(sv);
}

KNO_EXPORT void
_kno_stackvec_push(kno_stackvec sv,lispval v)
{
  __kno_stackvec_push(sv,v);
}

KNO_EXPORT void
_kno_decref_stackvec(kno_stackvec sv)
{
  __kno_decref_stackvec(sv);
}

KNO_EXPORT void
_kno_add_stack_ref(kno_stack stack,lispval v)
{
  __kno_add_stack_ref(stack,v);
}

KNO_EXPORT void
_KNO_STACK_SET_ARGS(kno_stack stack,lispval *args,
		    int width,int n,
		    int flags)
{
  __KNO_STACK_SET_ARGS(stack,args,width,n,flags);
}

KNO_EXPORT void
_KNO_STACK_FREE_ARGS(kno_stack stack)
{
  __KNO_STACK_FREE_ARGS(stack);
}


KNO_EXPORT void
_KNO_STACK_RESET_ARGS(kno_stack stack)
{
  __KNO_STACK_RESET_ARGS(stack);
}

KNO_EXPORT int _kno_free_stack(struct KNO_STACK *stack)
{
  return __kno_free_stack(stack);
}

KNO_EXPORT int _kno_reset_stack(struct KNO_STACK *stack)
{
  return __kno_reset_stack(stack);
}

KNO_EXPORT  void _KNO_STACK_SET_OP(kno_stack s,lispval new,int free)
{
  __KNO_STACK_SET_OP(s,new,free);
}

static void fix_void_args(lispval args);
static void fix_void_bindings(lispval bindings);

/* Stacks are rendered into LISP as vectors as follows:
   1. depth  (integer, increasing with calls)
   2. type   (apply, eval, load, other)
   3. label  (often the name of a function or expression type)
   4. status (a string (or #f) describing the state of the operation)
   5. op     (the thing being executed (expression, procedure applied, etc)
   6. args   (a vector of arguments to an applied procedure, or () otherwise)
*/

#define STACK_CREATE_OPTS KNO_COMPOUND_USEREF

static lispval annotate_source(lispval context,lispval point);

KNO_EXPORT lispval kno_stack2lisp(struct KNO_STACK *stack)
{
  lispval depth	  = KNO_INT(stack->stack_depth);
  lispval origin = (stack->stack_origin) ?
    (knostring(stack->stack_origin)) :
    (KNO_FALSE);
  lispval label	  = (stack->stack_label) ?
    (knostring(stack->stack_label)) :
    (KNO_FALSE);
  lispval file	  = (stack->stack_file) ?
    (knostring(stack->stack_file)) :
    (KNO_FALSE);
  lispval op	  = (KNO_VOIDP(stack->eval_source)) ? (KNO_FALSE) :
    (kno_incref(stack->stack_op));
  lispval source  = (KNO_VOIDP(stack->eval_source)) ? (kno_incref(op)) :
    (kno_incref(stack->eval_source));
  lispval context = ( (KNO_VOIDP(stack->eval_context)) ||
		      (stack->eval_context == stack->eval_source) ) ?
    (KNO_FALSE) : (annotate_source(stack->eval_context,source));
  lispval fn = (KNO_APPLICABLEP(op)) ? (kno_incref(op)) : (KNO_FALSE);

  lispval args = (((STACK_ARGS(stack)) && (STACK_LENGTH(stack))) ?
		  (copy_args(STACK_LENGTH(stack),STACK_ARGS(stack))) :
		  (KNO_EMPTY_LIST));
  lispval env =  (( (stack->eval_env) &&
		    (KNO_STACK_BITP(stack,KNO_STACK_OWNS_ENV) ) ) ?
		  (kno_deep_copy(stack->eval_env->env_bindings)) :
		  (KNO_FALSE));

  if (KNO_VECTORP(args)) fix_void_args(args);
  if (KNO_TABLEP(env)) fix_void_bindings(env);

  unsigned long icrumb = stack->stack_crumb;
  if (icrumb == 0) {
    icrumb = u8_random(UINT_MAX);
    if (icrumb > KNO_MAX_FIXNUM) icrumb = icrumb%KNO_MAX_FIXNUM;
    stack->stack_crumb=icrumb;}

  if (op == KNO_VOID) op = KNO_FALSE;
  if (source == KNO_VOID) source = KNO_FALSE;

  return kno_init_compound
    (NULL,stack_entry_symbol,STACK_CREATE_OPTS,
     11,depth,label,origin,file,KNO_INT(icrumb),
     fn,args,env,source,context,op);
}

static lispval copy_args(int width,kno_argvec args)
{
  lispval result = kno_make_vector(width,(lispval *)args);
  lispval *elts = KNO_VECTOR_ELTS(result);
  int i = 0; while (i<width) {
    lispval elt = elts[i++];
    kno_incref(elt);}
  return result;
}

KNO_EXPORT
lispval kno_get_backtrace(struct KNO_STACK *stack)
{
  if (stack == NULL) stack=kno_stackptr;
  if (stack == NULL) return KNO_EMPTY_LIST;
  int n = stack->stack_depth+1, i = 0;
  lispval result = kno_make_vector(n,NULL);
  while (stack) {
    lispval entry = kno_stack2lisp(stack);
    if (i < n) {
      KNO_VECTOR_SET(result,i,entry);}
    else u8_log(LOG_CRIT,"BacktraceOverflow",
		"Inconsistent depth %d",
		n-1);
    stack=stack->stack_caller;
    i++;}
  return result;
}

KNO_EXPORT
void kno_sum_backtrace(u8_output out,lispval stacktrace)
{
  if (KNO_VECTORP(stacktrace)) {
    int i = 0, len = KNO_VECTOR_LENGTH(stacktrace), n = 0;
    while (i<len) {
      lispval entry = KNO_VECTOR_REF(stacktrace,i);
      if (KNO_COMPOUND_TYPEP(entry,stack_entry_symbol)) {
	lispval type=KNO_COMPOUND_REF(entry,1);
	lispval label=KNO_COMPOUND_REF(entry,2);
	lispval status=KNO_COMPOUND_REF(entry,3);
	if (n) u8_puts(out," ⇒ ");
	if (STRINGP(label)) u8_puts(out,CSTRING(label));
	else u8_putc(out,'?');
	if (STRINGP(type)) {
	  u8_putc(out,'.');
	  u8_puts(out,CSTRING(type));}
	else u8_puts(out,".?");
	if (STRINGP(status)) {
	  u8_putc(out,'(');
	  u8_puts(out,CSTRING(status));
	  u8_putc(out,')');}
	n++;}
      i++;}}
}

/* Annotating expressions */

static lispval annotate_source(lispval context,lispval point)
{
  if (context == point)
    return eval_target_symbol;
  else if (KNO_CONSP(context)) {
    kno_lisp_type ctype = KNO_CONS_TYPEOF(context);
    switch (ctype) {
    case kno_pair_type: {
      lispval head = KNO_VOID, tail = KNO_VOID;
      lispval scan = context;
      while (KNO_PAIRP(scan)) {
	lispval car = KNO_CAR(scan);
	lispval annotated_car = annotate_source(car,point);
	lispval consed = kno_init_pair(NULL,annotated_car,KNO_EMPTY_LIST);
	if (KNO_VOIDP(tail)) {
	  head = tail = consed;}
	else {
	  struct KNO_PAIR *pair = (kno_pair) tail;
	  pair->cdr = consed;
	  tail = consed;}
	scan = KNO_CDR(scan);}
      if (KNO_CONSP(scan)) {
	lispval annotated_cdr = annotate_source(scan,point);
	struct KNO_PAIR *pair = (kno_pair) tail;
	pair->cdr = annotated_cdr;}
      return head;}
    default:
      return kno_incref(context);}}
  else return context;
}

/* Fixing VOID values in backtrace structures */

static void fix_void_args(lispval args)
{
  if (KNO_VECTORP(args)) {
    int i = 0, n = KNO_VECTOR_LENGTH(args);
    lispval *data = KNO_VECTOR_ELTS(args);
    while (i<n) {
      if (KNO_VOIDP(data[i])) data[i++]=KNO_QVOID;
      else i++;}}
}
static void fix_void_bindings(lispval bindings)
{
  if (KNO_SCHEMAPP(bindings)) {
    struct KNO_SCHEMAP *smap = (kno_schemap) bindings;
    int i = 0, n = smap->schema_length, changed = 0;
    lispval *schema = smap->table_schema;
    lispval *values = smap->table_values;
    while (i<n) {
      lispval key = schema[i], val = values[i];
      if (KNO_VOIDP(key)) {schema[i]=KNO_QVOID; changed=1;}
      if (KNO_VOIDP(val)) {values[i]=KNO_QVOID; changed=1;}
      i++;}
    if (changed) smap->table_bits &= (~KNO_SCHEMAP_SORTED);}
  else if (KNO_SLOTMAPP(bindings)) {
    struct KNO_SLOTMAP *smap = (kno_slotmap) bindings;
    int i = 0, n = smap->n_slots;
    struct KNO_KEYVAL *keyvals = KNO_XSLOTMAP_KEYVALS(smap);
    while (i<n) {
      lispval key = keyvals[i].kv_key, val = keyvals[i].kv_val;
      if (KNO_VOIDP(key)) {keyvals[i].kv_key=KNO_QVOID;}
      if (KNO_VOIDP(val)) {keyvals[i].kv_val=KNO_QVOID;}
      i++;}}
  else NO_ELSE;
}

/* Checking the C stack depth */

int kno_wrap_apply = KNO_WRAP_APPLY_DEFAULT;

#if KNO_STACKCHECK
static int stackcheck()
{
  if (kno_stack_limit>=KNO_MIN_STACKSIZE) {
    ssize_t depth = u8_stack_depth();
    if (depth>kno_stack_limit)
      return 0;
    else return 1;}
  else return 1;
}
KNO_EXPORT ssize_t kno_stack_getsize()
{
  if (kno_stack_limit>=KNO_MIN_STACKSIZE)
    return kno_stack_limit;
  else return -1;
}
KNO_EXPORT ssize_t kno_stack_setsize(ssize_t limit)
{
  if (limit<KNO_MIN_STACKSIZE) {
    char *detailsbuf = u8_malloc(64);
    u8_seterr("StackLimitTooSmall","kno_stack_setsize",
	      u8_write_long_long(limit,detailsbuf,64));
    return -1;}
  else {
    ssize_t maxstack = u8_stack_size;
    if (maxstack < KNO_MIN_STACKSIZE) {
      u8_log(LOGWARN,kno_InternalStackSizeError,
	     "Initialized u8_stacksize too small: %lld",maxstack);
      return -1;}
    else if (limit <=  maxstack) {
      ssize_t old = kno_stack_limit;
      kno_set_stack_limit(limit);
      return old;}
    else {
      char *detailsbuf = u8_malloc(64);
      u8_seterr("StackLimitTooLarge","kno_stack_setsize",
		u8_write_long_long(limit,detailsbuf,64));
      return -1;}}
}
KNO_EXPORT ssize_t kno_stack_resize(double factor)
{
  if ( (factor<0) || (factor>1000) ) {
    u8_log(LOG_WARN,kno_BadStackFactor,
	   "The value %f is not a valid stack resize factor",factor);
    return kno_stack_limit;}
  else {
    ssize_t current = kno_stack_limit;
    ssize_t limit = u8_stack_size;
    if (limit<KNO_MIN_STACKSIZE) limit = current;
    return kno_stack_setsize((limit*factor));}
}
#else
static int youve_been_warned = 0;
#define stackcheck() (1)
KNO_EXPORT ssize_t kno_stack_getsize()
{
  if (youve_been_warned) return -1;
  u8_log(LOG_WARN,"NoStackChecking",
	 "Stack checking is not enabled in this build");
  youve_been_warned = 1;
  return -1;
}
KNO_EXPORT ssize_t kno_stack_setsize(ssize_t limit)
{
  u8_log(LOG_WARN,"NoStackChecking",
	 "Stack checking is not enabled in this build");
  return -1;
}
KNO_EXPORT ssize_t kno_stack_resize(double factor)
{
  if (factor<0) {
    u8_log(LOG_WARN,"NoStackChecking",
	   "Stack checking is not enabled in this build and the value you provided (%f) is invalid anyway",
	   factor);}
  else if (factor<=1000) {
    u8_log(LOG_WARN,"StackResizeTooLarge",
	   "Stack checking is not enabled in this build and the value you provided (%f) is too big anyway",
	   factor);}
  else {
    u8_log(LOG_WARN,"NoStackChecking",
	   "Stack checking is not enabled in this build so setting the limit doesn't do anything",
	   factor);}
  return -1;
}
#endif

KNO_EXPORT int kno_stackcheck()
{
  return stackcheck();
}

/* Add showstack signals */

static void showstatus_handler(int signum,siginfo_t *info,void *stuff)
{
  /* What should this do? */
  lispval cur_thread = kno_current_thread;
  u8_string log_context = u8_log_context;
  kno_stack stackptr = kno_stackptr;
  int emissions = 0;
  if (TYPEP(cur_thread,kno_thread_type)) {
    u8_log(-LOGNOTICE,"Thread","%q",cur_thread);
    emissions++;}
  if (stackptr) {
    u8_log(-LOGNOTICE,"ThreadStack",
	   "(%d) %s.%s %q",
	   stackptr->stack_depth,
	   stackptr->stack_label,
	   stackptr->stack_file,
	   stackptr->stack_op);
    emissions++;}
  if (log_context) {
    u8_log(-LOGNOTICE,"LogContext","%s",log_context);
    emissions++;}
  if (! (emissions) )
    u8_log(-LOGNOTICE,"ThreadInfo","None available");
}

static void showstack_handler(int signum,siginfo_t *info,void *stuff)
{
  /* This should do more */
  lispval cur_thread = kno_current_thread;
  u8_string log_context = u8_log_context;
  kno_stack stackptr = kno_stackptr;
  int emissions = 0;
  if (TYPEP(cur_thread,kno_thread_type)) {
    u8_log(-LOGNOTICE,"Thread","%q",cur_thread);
    emissions++;}
  if (stackptr) {
    u8_log(-LOGNOTICE,"ThreadStack",
	   "(%d) %s.%s %q",
	   stackptr->stack_depth,
	   stackptr->stack_label,
	   stackptr->stack_file,
	   stackptr->stack_op);
    emissions++;}
  if (log_context) {
    u8_log(-LOGNOTICE,"LogContext","%s",log_context);
    emissions++;}
  if (! (emissions) )
    u8_log(-LOGNOTICE,"ThreadInfo","None available");
}

/* Initialize C stack limits */

KNO_EXPORT ssize_t kno_init_cstack()
{
  int rv = 0;
  u8_init_stack();
  if (kno_stack_limit>0) {
    /* Already initialized */}
  else if (VOIDP(kno_default_stackspec)) {
    ssize_t stacksize = u8_stack_size;
    if (stacksize < KNO_MIN_STACKSIZE) {
      u8_log(LOGWARN,"DefaultStackTooSmall",
	     "\nThe C stack size (%lld) is smaller than recommended minimum (%lld). "
	     "\nSome programs (including unit tests) may fail with this setting. \nTo correct, "
	     "configure using --with-cstacksize=16777216 at build time.",
	     stacksize,KNO_MIN_STACKSIZE);
      kno_set_stack_limit(stacksize-stacksize/8);}
    else rv = kno_stack_setsize(stacksize-stacksize/8);}
  else if (FIXNUMP(kno_default_stackspec))
    rv = kno_stack_setsize((ssize_t)(FIX2INT(kno_default_stackspec)));
  else if (KNO_FLONUMP(kno_default_stackspec))
    rv = kno_stack_resize(KNO_FLONUM(kno_default_stackspec));
  else if (KNO_BIGINTP(kno_default_stackspec)) {
    unsigned long long val = kno_getint(kno_default_stackspec);
    if (val) rv = kno_stack_setsize((ssize_t) val);
    else {
      u8_log(LOG_CRIT,kno_BadStackSize,
	     "The default stack value %q wasn't a valid stack size",
	     kno_default_stackspec);
      return -1;}}
  else {
    u8_log(LOG_CRIT,kno_BadStackSize,
	   "The default stack value %q wasn't a valid stack size",
	   kno_default_stackspec);
    return -1;}
  if (rv<0) {
    kno_clear_errors(1);
    return 0;}
  else return rv;
}

static int init_thread_stack_limit()
{
  kno_init_cstack();
  return 1;
}

/* Debugging support */

static void concise_stack_frame(u8_output out,struct KNO_STACK *stack)
{
  lispval op = stack->stack_op;
  kno_stackvec refs = &(stack->stack_refs);
  u8_string file = stack->stack_file;
  if ( (stack->stack_label) && (stack->stack_origin) &&
       ( (stack->stack_origin) != (stack->stack_label) ) )
    u8_printf(out,"(%d) 0x%llx %s:%s%s%s%s %d/%d args, %d/%d%s refs",
	      stack->stack_depth,KNO_LONGVAL(stack),
	      stack->stack_label,stack->stack_origin,
	      U8OPTSTR("(",file,")"),
	      stack->stack_argc,stack->stack_width,
	      refs->count,KNO_STACKVEC_LEN(refs),
	      (KNO_STACKVEC_ONHEAP(refs)) ? ("(heap)") : (""));
  else if ( (stack->stack_label) || (stack->stack_origin) )
    u8_printf(out,"(%d) 0x%llx %s%s%s%s %d/%d args, %d/%d%s refs",
	      stack->stack_depth,KNO_LONGVAL(stack),
	      ( (stack->stack_label) ?
		(stack->stack_label) :
		(stack->stack_origin) ),
	      U8OPTSTR("(",file,")"),
	      stack->stack_argc,stack->stack_width,
	      refs->count,KNO_STACKVEC_LEN(refs),
	      (KNO_STACKVEC_ONHEAP(refs)) ? ("(heap)") : (""));
  else u8_printf(out,"(%d) 0x%llx %s%s%s %d/%d args, %d/%d refs",
		 stack->stack_depth,KNO_LONGVAL(stack),
		 U8OPTSTR("(",file,")"),
		 stack->stack_argc,stack->stack_width,
		 refs->count,refs->len);
  unsigned int bits = stack->stack_bits;
  if (!(U8_BITP(bits,KNO_STACK_LIVE))) u8_puts(out," dead");
  if (U8_BITP(bits,KNO_STACK_TAIL_POS)) u8_puts(out," tailpos");
  if (U8_BITP(bits,KNO_STACK_TAIL_LOOP)) u8_puts(out," loop");
  if (U8_BITP(bits,KNO_STACK_LAMBDA_CALL)) u8_puts(out," lambda");
  if (U8_BITP(bits,KNO_STACK_VOID_VAL)) u8_puts(out," void");
  if (U8_BITP(bits,KNO_STACK_DECREF_ARGS)) u8_puts(out," decref");
  if (U8_BITP(bits,KNO_STACK_FREE_ARGVEC)) u8_puts(out," freevec");
  if ( (U8_BITP(bits,KNO_STACK_OWNS_ENV)) && (stack->eval_env) )
    u8_puts(out," env");
  if (U8_BITP(bits,KNO_STACK_FREE_ENV)) u8_puts(out,"/free");
  if ( (U8_BITP(bits,KNO_STACK_OWNS_ENV)) && (stack->eval_env) ) {
    kno_lexenv env = stack->eval_env;
    lispval bindings = env->env_bindings;
    if (KNO_SCHEMAPP(bindings)) {
      struct KNO_SCHEMAP *map = (kno_schemap) bindings;
      lispval *schema = map->table_schema;
      int i = 0, n = map->schema_length;
      if (n>0) u8_puts(out," binds:");
      while (i<n) {
	lispval argname = schema[i];
	if (i>0) u8_putc(out,',');
	i++;
	u8_putc(out,' ');
	if (i == n) u8_puts(out,"and ");
	if (KNO_SYMBOLP(argname))
	  u8_puts(out,KNO_SYMBOL_NAME(argname));
      else u8_puts(out,"+weird+");}
      u8_puts(out,"; ");}}
  if (stack->stack_file)
    u8_printf(out," %s;",stack->stack_file);
  if (KNO_SYMBOLP(op))
    u8_printf(out," point=:%s",SYM_NAME(op));
  else if ( (KNO_FUNCTIONP(op)) &&
	    ((KNO_FUNCTION_INFO(op))->fcn_name) ) {
    struct KNO_FUNCTION *fn=KNO_FUNCTION_INFO(op);
    u8_string prefix = (KNO_CPRIMP(op)) ? ("c#") :
      (KNO_LAMBDAP(op)) ? ("l#") : ("#");
    u8_printf(out," point=%s%s",prefix,fn->fcn_name);}
  else if (!(KNO_CONSP(op))) {
    u8_byte buf[64];
    u8_printf(out," point=%s",u8_bprintf(buf,"%q",op));}
  else if (KNO_PAIRP(op))
    u8_printf(out," point=expr");
  else u8_printf(out," point=%s",kno_type_name(op));
}

KNO_EXPORT void knodbg_show_stack_frame
(u8_output out,struct KNO_STACK *stack,int concise)
{
  kno_lexenv env = NULL;
  int depth = stack->stack_depth;
  concise_stack_frame(out,stack); u8_putc(out,'\n');
  if (concise) return;
  if (KNO_CONSP(stack->eval_source))
    u8_printf(out,"(%d) source: %Q\n",depth,stack->eval_source);
  if (KNO_APPLICABLEP(stack->stack_op)) {
    u8_printf(out,"(%d) Applying %q to %d args\n",
	      depth,stack->stack_op,stack->stack_argc);
    if (stack->stack_argc) {
      u8_byte buf[200];
      kno_argvec args = stack->stack_args;
      int i=0, n = stack->stack_argc;
      while (i<n) {
	lispval arg = args[i];
	u8_string line=u8_bprintf(buf,"(%d) #%d %p\t%q",depth,i,arg,arg);
	u8_puts(out,line); u8_putc(out,'\n');
	i++;}}}
  else if (CONSP(stack->stack_op)) {
    u8_printf(out,"(%d) Evaluating in %p %Q\n",
	      depth,stack->eval_env,stack->stack_op);}
  if ( (KNO_STACK_BITP(stack,KNO_STACK_OWNS_ENV)) ) {
    if (stack->eval_env) {
      env = stack->eval_env;
      lispval bindings = env->env_bindings;
      if (KNO_SCHEMAPP(bindings)) {
	kno_schemap map = (kno_schemap)bindings;
	lispval *schema = map->table_schema;
	lispval *values = map->table_values;
	int i = 0, n = map->schema_length;
	while (i<n) {
	  lispval key = schema[i];
	  lispval val = values[i];
	  u8_printf(out,"(%d) %q\t%p\t%q\n",depth,key,val,val);
	  i++;}}}
    else u8_putc(out,'\n');}
  else NO_ELSE;
}

KNO_EXPORT void knodbg_log_stack(int level,u8_condition c,int concise)
{
  kno_stack stack = kno_stackptr, scan = stack;
  if (stack == NULL) return;
  if (c == NULL) c = "Stack";
  U8_STATIC_OUTPUT(temp,1000);
  while (scan) {
    knodbg_show_stack_frame(tempout,scan,concise);
    u8_log(level,c,"%s",temp.u8_outbuf);
    u8_reset_output(tempout);
    scan=scan->stack_caller;}
  u8_close_output(tempout);
}

/* Throwing exceptions (and popping the stack) */

KNO_EXPORT void kno_throw_contour(u8_contour c,u8_context cxt)
{
  if (c == NULL) c = u8_dynamic_contour;
  if  (c) {
    struct KNO_STACK *scan = kno_stackptr;
    if (scan) {
      void *stack = (void *) scan;
      void *contour = (void *) c;
      while ( (stack) &&
	      ( (u8_stack_direction>0) ?
		(stack > contour) :
		(stack < contour) ) ) {
	kno_pop_stack(scan);
	scan = kno_stackptr;
	stack = (void *) scan;}}
    u8_throw_contour(c);}
  else u8_raise("NoContour",cxt,NULL);
}

KNO_EXPORT void kno_raise_exception
(u8_condition cond,u8_context cxt,u8_string details,
 lispval irritant)
{
  u8_contour contour = u8_dynamic_contour;
  if (cond == NULL) cond = "Unknown (NULL) condition";
  if (u8_raise_debug) {
    struct U8_EXCEPTION ex = { 0 };
    ex.u8x_cond = cond;
    ex.u8x_context = cxt;
    ex.u8x_details = details;
    u8_debug_wait(&ex,1);}
  if (contour) {
    struct KNO_STACK *scan = kno_stackptr;
    _kno_mkerr(cond,cxt,details,irritant,NULL);
    if (scan) {
      /* Pop all of the stack frames below contour */
      void *stack_loc = (void *) scan;
      void *contour_loc = (void *) contour;
      while ( (scan) &&
	      ( (u8_stack_direction>0) ?
		(stack_loc > contour_loc) :
		(stack_loc < contour_loc) ) ) {
	kno_stack next =scan->stack_caller;
	kno_pop_stack(scan);
	scan = kno_stackptr;
	if (next != scan)
	  u8_log(LOGPANIC,"CorruptedStack",
		 "Stack pointer is %p != %p after pop",
		 next,scan);
	stack_loc = (void *) scan;}}
    kno_throw_contour(contour,contour->u8c_label);}
  else {
    u8_exception ex = NULL;
    _kno_mkerr(cond,cxt,details,irritant,&ex);
    u8_raise_exception(ex);}
}

KNO_EXPORT void kno_signal_exception(u8_condition cond,u8_context cxt)
{
  u8_contour contour = u8_dynamic_contour;
  if (cond == NULL) cond = "Unknown (NULL) condition";
  if (u8_raise_debug) {
    struct U8_EXCEPTION ex = { 0 };
    ex.u8x_cond = cond;
    ex.u8x_context = cxt;
    ex.u8x_details = NULL;
    u8_debug_wait(&ex,1);}
  if (contour) {
    struct KNO_STACK *scan = kno_stackptr;
    if (scan) {
      /* Pop all of the stack frames below contour */
      void *stack_loc = (void *) scan;
      void *contour_loc = (void *) contour;
      while ( (scan) &&
	      ( (u8_stack_direction>0) ?
		(stack_loc > contour_loc) :
		(stack_loc < contour_loc) ) ) {
	kno_stack next =scan->stack_caller;
	kno_pop_stack(scan);
	scan = kno_stackptr;
	if (next != scan)
	  u8_log(LOGPANIC,"CorruptedStack",
		 "Stack pointer is %p != %p after pop",
		 next,scan);
	stack_loc = (void *) scan;}}
    u8_raise(cond,cxt,NULL);}
  else u8_raise(cond,cxt,NULL);
}

void kno_init_stacks_c()
{
  u8_register_threadinit(init_thread_stack_limit);

#if (KNO_USE_TLS)
  u8_new_threadkey(&kno_stackptr_key,NULL);
  u8_tld_set(kno_stackptr_key,(void *)NULL);
#else
  kno_stackptr=NULL;
#endif

  _kno_raise=kno_raise_exception;

  stack_entry_symbol = kno_intern("%%stack");


  opaque_symbol = kno_intern("%opaque");
  eval_target_symbol = kno_intern("$<<*eval*>>$");
  pbound_symbol = kno_intern("%bound");
  pargs_symbol = kno_intern("%args");

  null_sym = kno_intern("#null");
  unbound_sym = kno_intern("#unbound");
  void_sym = kno_intern("#void");
  default_sym = kno_intern("#default");
  nofile_symbol = kno_intern("nofile");

  /* Setup sigaction for status action */
  kno_sigaction_status->sa_sigaction = showstatus_handler;
  kno_sigaction_status->sa_flags = SA_SIGINFO;

  /* Setup sigaction for status action */
  kno_sigaction_stack->sa_sigaction = showstack_handler;
  kno_sigaction_stack->sa_flags = SA_SIGINFO;

  kno_register_config
    ("STACKLIMIT",
     _("Size of the stack (in bytes or as a factor of the current size)"),
     kno_sizeconfig_get,kno_sizeconfig_set,&kno_default_stackspec);
  kno_register_config
    ("STACK:DEBUG",_("Whether to trace all stack push/pop operations"),
     kno_boolconfig_get,kno_boolconfig_set,&kno_debug_stacks);

}

