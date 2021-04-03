#ifndef APPLY_INTERNALS_H
#define APPLY_INTERNALS_H 1

#include <libu8/u8printf.h>

#include "kno/lisp.h"
#include "kno/apply.h"

KNO_EXPORT u8_condition kno_VoidArgument;

#define KNO_ARGS_FAIL    0x01
#define KNO_ARGS_CONSED  0x02
#define KNO_ARGS_CHOICES 0x04

static int arg_type_error(lispval f,lispval arg,lispval type,int i);
static int arg_error(lispval f,lispval arg,int i);

static U8_MAYBE_UNUSED
int check_args(lispval f,int n,kno_argvec args)
{
  const lispval *scan = args, *limit =scan+n;
  while (scan < limit) {
    lispval arg  = *scan;
    if (RARELY ( (arg == KNO_NULL) ||
		 (KNO_VOIDP(arg)) ||
		 (KNO_ABORTED(arg)) ||
		 (!(KNO_CHECK_PTR(arg))) ) )
      return arg_error(f,arg,scan-args);
    else NO_ELSE;
    scan++;}
  return 0;
}

static U8_MAYBE_UNUSED
int check_argtypes(struct KNO_FUNCTION *f,int n,kno_argvec args)
{
  const lispval *scan = args, *limit =scan+n;
  const lispval *types = f->fcn_typeinfo;
  const lispval *maxtypes = types+f->fcn_arginfo_len;
  while (scan < limit) {
    lispval arg  = *scan;
    lispval type  = (types>=maxtypes) ? (KNO_VOID) : (*types++);
    if (KNO_VOIDP(arg)) return arg_error((lispval)f,arg,scan-args);
    else if (KNO_ABORTED(arg)) return arg_error((lispval)f,arg,scan-args);
    else if (KNO_EMPTYP(arg)) {}
    else if (KNO_VOIDP(type)) {}
    else if (KNO_CHECKTYPE(arg,type)) {}
#if 0
    else if (KNO_CHOICEP(arg)) {
      KNO_ITER_CHOICES(elts,maxelts,arg);
      while (elts<maxelts) {
	lispval elt = *maxelts++;
	if (RARELY(!(KNO_CHECKTYPE(arg,type))))
	  return arg_type_error((lispval)f,elt,type,scan-args);}}
#endif
    else return arg_type_error((lispval)f,arg,type,scan-args);
    scan++;}
  return 0;
}

static int arg_type_error(lispval fn,lispval arg,lispval type,int i)
{
  u8_byte buf[100];
  u8_string fcn_name = (KNO_FUNCTIONP(fn)) ? (((kno_function)fn)->fcn_name) :
    (NULL);
  u8_string details =
    ( (fcn_name) ?
      (u8_bprintf(buf,"%s[%d]!=%q",fcn_name,i,type)) :
      (u8_bprintf(buf,"arg[%d]!=%q for %q",i,type,fn)) );
  kno_seterr(kno_TypeError,"apply",details,arg);
  return -1;
}

static int arg_error(lispval f,lispval arg,int i)
{
  u8_byte buf[100];
  u8_string fcn_name = (KNO_FUNCTIONP(f)) ? (((kno_function)f)->fcn_name) :
    (NULL);
  u8_string details =
    ( (fcn_name) ?
      (u8_bprintf(buf,"%s[%d]",fcn_name,i)) :
      (u8_bprintf(buf,"arg[%d] for %q",i,f)) );
  if (arg == KNO_NULL)
    kno_seterr(kno_NullPtr,"docall",details,f);
  else if (KNO_ABORTP(arg))
    kno_seterr("ArgumentError","docall",details,f);
  else if (KNO_VOIDP(arg))
    kno_seterr(kno_VoidArgument,"docall",details,f);
  else kno_badptr_err(arg,"docall",details);
  return -1;
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
    fname = u8_bprintf(namebuf,"<%s>%p",type_name,fn);}
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
    fname = u8_bprintf(namebuf,"<%s>%p",type_name,fn);}
  kno_seterr(kno_TooManyArgs,"kno_dcall",
	     u8_bprintf(buf,"%s %d args > [%d,%d] expected",
			fname,n,min,max),
	     fn);
  return -1;
}

static U8_MAYBE_UNUSED
int setup_call(kno_stack stack,kno_function fcn,
	       int width,lispval *callbuf,
	       int n,kno_argvec args)
{
  int i = 0; while (i<n) {
    lispval arg = args[i];
    if (RARELY ( (arg == KNO_NULL) ||
		 (KNO_VOIDP(arg)) ||
		 (KNO_ABORTED(arg)) ||
		 (!(KNO_CHECK_PTR(arg))) ) ) {
      arg_error((lispval)fcn,arg,i);
      return -1;}
    else if (CONSP(arg)) {
      if (KNO_PRECHOICEP(arg)) {
	lispval simple = kno_make_simple_choice(arg);
	kno_add_stack_ref(stack,simple);
	callbuf[i]=simple;}
      else if (KNO_QCHOICEP(arg)) {
	lispval v = KNO_QCHOICEVAL(arg);
	kno_incref(v);
	kno_add_stack_ref(stack,v);
	callbuf[i]=v;}
      else callbuf[i]=arg;}
    else callbuf[i]=arg;
    i++;}
  while (i<width) callbuf[i++]=KNO_VOID;
  STACK_ARGS(stack)      = callbuf;
  STACK_WIDTH(stack)     = width;
  STACK_ARGCOUNT(stack)  = n;
  return n;
}

#if 0
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
      if (type<0) i++;
      else if (KNO_TYPEP(arg,type)) i++;
      else {
	u8_byte buf[128];
	kno_lisp_type arg_type = KNO_TYPEOF(arg);
	kno_seterr(kno_TypeError,kno_type_names[type],
		   u8_bprintf(buf,"%s[%d](%s)",fname,i,
			      kno_type_names[arg_type]),
		   arg);
	return -1;}}}
  i = n;
  if (defaults) {
    while (i<width) {
      args[i] = defaults[i];
      i++;}}
  else while (i<width) args[i++]=KNO_VOID;
  return width;
}
#endif

KNO_FASTOP lispval cprim_call(u8_string fname,kno_cprim cp,
			      int n,kno_argvec given,
			      kno_stack stack)
{
  const lispval *defaults = cp->fcn_defaults;
  int info_len = cp->fcn_arginfo_len;
  int call_width = cp->fcn_call_width;
  if (call_width<0) call_width=n;
  const lispval *args, _args[call_width], *arglimit;
  if (call_width > n) {
    args = _args; arglimit=args+call_width;
    lispval *write = (lispval *) ( args = _args);
    lspcpy(write,given,n); write += n;
    if ( (defaults) && (info_len>n) ) {
      int fill_in = info_len-n;
      lspcpy(write,defaults+n,info_len-n);
      write += fill_in;}
    if (write < arglimit)
      lspset(write,arglimit-write,KNO_VOID);}
  else args = given;
  int rv = (cp->fcn_typeinfo) ?
    (check_argtypes((kno_function)cp,n,args)) :
    (check_args((lispval)cp,n,args));
  if (rv<0) return KNO_ERROR; else NO_ELSE;
  int arity = cp->fcn_arity;
  if (rv<0)
    return KNO_ERROR;
  else if (FCN_XCALLP(cp))
    return cp->fcn_handler.xcalln(stack,(kno_function)cp,n,args);
  else if ( (FCN_VARARGP(cp)) || (arity < 0) )
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

#endif
