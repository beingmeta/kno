#include <libu8/u8printf.h>

KNO_EXPORT u8_condition kno_VoidArgument;

static U8_MAYBE_UNUSED
int check_args(int n,kno_argvec args)
{
  int needs_work = 0;
  int i = 0; while (i<n) {
    lispval arg = args[i];
    if (! (PRED_TRUE(KNO_CHECK_PTR(arg))) )
      return -(i+1);
    else if (CONSP(arg)) {
      if (KNO_PRECHOICEP(arg)) {needs_work = 1;}
      else if (KNO_QCHOICEP(arg)) {needs_work = 1;}}
    else if ( (KNO_ABORTED(arg)) || (KNO_VOIDP(arg)) )
      return -(i+1);
    else NO_ELSE;
    i++;}
  return needs_work;
}

static void arg_error(lispval fcn,lispval arg,int i)
{
  u8_byte buf[64];
  u8_string fcn_name = (KNO_FUNCTIONP(fcn)) ?
    (((kno_function)fcn)->fcn_name) : (NULL);
  u8_string details =
    ( (fcn_name) ?
      (u8_bprintf(buf,"%s[%d]",fcn_name,i)) :
      (u8_bprintf(buf,"%q[%d]",fcn,i)) );
  if (arg == KNO_NULL)
    kno_seterr(kno_NullPtr,"docall",details,fcn);
  else if (KNO_ABORTP(arg))
    kno_seterr("ArgumentError","docall",details,fcn);
  else if (KNO_VOIDP(arg))
    kno_seterr(kno_VoidArgument,"docall",details,fcn);
  else kno_badptr_err(arg,"docall",details);
}

static U8_MAYBE_UNUSED
void errno_warning(u8_string label)
{
  if (errno) {
    u8_string cond=u8_strerror(errno);
    u8_log(LOG_WARN,cond,"Unexpected errno=%d (%s) after %s",
	   errno,cond,(U8ALT(label,"core_apply")));
    errno=0;}
}

static U8_MAYBE_UNUSED
int too_few_args(lispval fn,u8_string fname,int n,int min,int max)
{
  kno_lisp_type ftype = KNO_TYPEOF(fn);
  u8_byte buf[64], namebuf[64];
  if (fname == NULL) {
    u8_string type_name = kno_type_names[ftype];
    if (!(type_name)) type_name="applicable";
    fname = u8_bprintf(namebuf,"<%s>0x%llx",type_name,KNO_LONGVAL(fn));}
  kno_seterr(kno_TooFewArgs,"kno_dcall",
	     ((max>=0) ?
	      (u8_bprintf(buf,"%s %d args < [%d-%d] expected",
			  fname,n,min,max)) :
	      (u8_bprintf(buf,"%s %d args < [%d+] expected",
			  fname,n,min))),
	     fn);
  return -1;
}

static U8_MAYBE_UNUSED
int too_many_args(lispval fn,u8_string fname,int n,int min,int max)
{
  kno_lisp_type ftype = KNO_TYPEOF(fn);
  u8_byte buf[64], namebuf[64];
  if (fname == NULL) {
    u8_string type_name = kno_type_names[ftype];
    if (!(type_name)) type_name="applicable";
    fname = u8_bprintf(namebuf,"<%s>0x%llx",type_name,KNO_LONGVAL(fn));}
  kno_seterr(kno_TooManyArgs,"kno_dcall",
	     u8_bprintf(buf,"%s %d args > [%d,%d] expected",
			fname,n,min,max),
	     fn);
  return -1;
}

static U8_MAYBE_UNUSED
int setup_call(kno_stack stack,lispval fcn,
	       int width,lispval *callbuf,
	       int n,kno_argvec args)
{
  int i = 0; while (i<n) {
    lispval arg = args[i];
    if (PRED_FALSE ( (arg == KNO_NULL) ||
		     (KNO_VOIDP(arg)) ||
		     (KNO_ABORTED(arg)) ||
		     (!(KNO_CHECK_PTR(arg))) ) ) {
      arg_error(fcn,arg,i);
      return -1;}
    else if (CONSP(arg)) {
      if (KNO_PRECHOICEP(arg)) {
	lispval simple = kno_make_simple_choice(arg);
	KNO_STACK_ADDREF(stack,simple);
	callbuf[i]=simple;}
      else if (KNO_QCHOICEP(arg)) {
	lispval v = KNO_QCHOICEVAL(arg);
	kno_incref(v);
	KNO_STACK_ADDREF(stack,v);
	callbuf[i]=v;}
      else callbuf[i]=arg;}
    else callbuf[i]=arg;
    i++;}
  while (i<width) callbuf[i++]=KNO_VOID;
  stack->stack_args  = callbuf;
  stack->stack_width = width;
  stack->stack_argc  = n;
  return n;
}

static int cprim_prep(kno_stack stack,
		      u8_string fname,
		      int n,int width,
		      lispval *args,
		      const kno_lisp_type *typeinfo,
		      const lispval *defaults)
{
  int i = 0;
  if (typeinfo) {
    int i = 0; while (i<n) {
      lispval arg = args[i];
      kno_lisp_type type = typeinfo[i];
      kno_lisp_type arg_type = KNO_TYPEOF(arg);
      if (type<0) i++;
      else if (arg_type == type) i++;
      else {
	u8_byte buf[128];
	kno_seterr(kno_TypeError,kno_type_names[type],
		   u8_bprintf(buf,"%s[%d](%s)",fname,i,
			      kno_type_names[arg_type]),
		   arg);
	return -1;}}}
  else NO_ELSE;
  i = n;
  if (defaults) {
    while (i<width) {
      args[i] = defaults[i];
      i++;}}
  else while (i<width) args[i++]=KNO_VOID;
  return width;
}

KNO_FASTOP lispval cprim_call(u8_string fname,kno_cprim cp,
			      int n,kno_argvec given,
			      kno_stack stack)
{
  kno_lisp_type *typeinfo = cp->fcn_typeinfo;
  const lispval *defaults = cp->fcn_defaults;
  int call_width = cp->fcn_call_width, rv = -1;
  if (call_width<0) call_width=n;
  lispval *args, _args[call_width];
  if (call_width <= stack->stack_width) {
    rv = cprim_prep(stack,fname,n,call_width,
		    (lispval *)given,
		    typeinfo,defaults);
    args = (lispval *)given;}
  else {
    rv = cprim_prep(stack,fname,n,call_width,
		    _args,
		    typeinfo,defaults);
    args = _args;}
  int arity = cp->fcn_arity;
  if (rv<0)
    return KNO_ERROR;
  else if (FCN_XCALLP(cp))
    return cp->fcn_handler.xcalln(stack,(kno_function)cp,n,args);
  else if ( (FCN_LEXPRP(cp)) || (arity < 0) )
    return cp->fcn_handler.calln(n,args);
  else switch (arity) {
    case 0: return cp->fcn_handler.call0();
    case 1: return cp->fcn_handler.call1(args[0]);
    case 2: return cp->fcn_handler.call2(args[0],args[1]);
    case 3: return cp->fcn_handler.call3(args[0],args[1],args[2]);
    case 4: return cp->fcn_handler.call4
	(args[0],args[1],args[2],args[3]);
    case 5: return cp->fcn_handler.call5
	(args[0],args[1],args[2],args[3],args[4]);
    case 6:
      return cp->fcn_handler.call6
	(args[0],args[1],args[2],args[3],args[4],args[5]);
    case 7:
      return cp->fcn_handler.call7
	(args[0],args[1],args[2],args[3],
	 args[4],args[5],args[6]);
    case 8:
      return cp->fcn_handler.call8
	(args[0],args[1],args[2],args[3],
	 args[4],args[5],args[6],args[7]);
    case 9:
      return cp->fcn_handler.call9
	(args[0],args[1],args[2],args[3],
	 args[4],args[5],args[6],args[7],args[8]);
    case 10:
      return cp->fcn_handler.call10
	(args[0],args[1],args[2],args[3],
	 args[4],args[5],args[6],args[7],args[8],
	 args[9]);
    case 11:
      return cp->fcn_handler.call11
	(args[0],args[1],args[2],
	 args[3],args[4],args[5],
	 args[6],args[7],args[8],
	 args[9],args[10]);
    case 12:
      return cp->fcn_handler.call12
	(args[0],args[1],args[2],
	 args[3],args[4],args[5],
	 args[6],args[7],args[8],
	 args[9],args[10],args[11]);
    case 13:
      return cp->fcn_handler.call13
	(args[0],args[1],args[2],
	 args[3],args[4],args[5],
	 args[6],args[7],args[8],
	 args[9],args[10],args[11],
	 args[12]);
    case 14:
      return cp->fcn_handler.call14
	(args[0],args[1],args[2],
	 args[3],args[4],args[5],
	 args[6],args[7],args[8],
	 args[9],args[10],args[11],
	 args[12],args[13]);
    case 15:
      return cp->fcn_handler.call15
	(args[0],args[1],args[2],
	 args[3],args[4],args[5],
	 args[6],args[7],args[8],
	 args[9],args[10],args[11],
	 args[12],args[13],args[14]);
    default:
      if (FCN_XCALLP(cp))
	return cp->fcn_handler.xcalln(stack,(kno_function)cp,n,args);
      else return cp->fcn_handler.calln(n,args);}
}

