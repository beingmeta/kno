static int null_ptr_error(u8_string fname,int i)
{
  return KNO_ERR(-1,kno_NullPtr,"cprim_prep",fname,KNO_INT(i));
}

static int cprim_prep(u8_string fname,
		      int n_given,kno_argvec given,
		      int n_needed,kno_lispval *needed,
		      kno_lisp_type *typeinfo,
		      const lispval *defaults)
{
  if ( (typeinfo) && (defaults) ) {
    int i = 0; while (i < n_given) {
      lispval arg = given[i];
      if (KNO_NULLP(arg)) return null_ptr_error(fname,i);
      else if ( (KNO_VOIDP(arg)) || (KNO_DEFAULTP(arg)) ) {
	/* Note that the defaults for cprims can't be conses, so we don't bother with
	   incref/decref */
	needed[i] = defaults[i]; i++;
	continue;}
      else if (KNO_QCHOICEP(arg))
	arg = (KNO_XQCHOICE(arg))->qchoiceval;
      else NO_ELSE;
      if (typeinfo[i]>0) {
	/* We don't check the type of defaults, since it lets
	   the handler use non-standard arguments as signals. */
	if (! (PRED_TRUE(KNO_TYPEP(arg,typeinfo[i]))) ) {
	  u8_byte buf[128];
	  kno_seterr(kno_TypeError,kno_type_names[typeinfo[i]],
		     u8_bprintf(buf,"%s[%d]",fname,i),
		     arg);
	  return -1;}}
      needed[i] = arg;
      i++;}
    while (i < n_needed) {
      needed[i] = defaults[i];
      i++;}
    return i;}
  else if (defaults) {
    int i = 0; while (i < n_given) {
      lispval arg = given[i];
      if (KNO_NULLP(arg)) return null_ptr_error(fname,i);
      if ( (KNO_VOIDP(arg)) || (KNO_DEFAULTP(arg)) ) {
	/* Note that the defaults can't be conses */
	needed[i] = defaults[i]; i++;
	continue;}
      else if (KNO_QCHOICEP(arg))
	arg = (KNO_XQCHOICE(arg))->qchoiceval;
      else needed[i] = arg;
      i++;}
    while (i < n_needed) {
      needed[i] = defaults[i];
      i++;}
    return i;}
  else if (typeinfo) {
    int i = 0; while (i < n_given) {
      lispval arg = given[i];
      if (KNO_NULLP(arg)) return null_ptr_error(fname,i);
      else if (KNO_QCHOICEP(arg))
	arg = (KNO_XQCHOICE(arg))->qchoiceval;
      else NO_ELSE;
      if (typeinfo[i]>0) {
	if (! (PRED_TRUE(KNO_TYPEP(arg,typeinfo[i]))) ) {
	  u8_byte buf[128];
	  kno_seterr(kno_TypeError,kno_type_names[typeinfo[i]],
		     u8_bprintf(buf,"%s[%d]",fname,i),
		     arg);
	  return -1;}}
      needed[i] = arg;
      i++;}
    while (i < n_needed) {needed[i++] = KNO_VOID;}
    return i;}
  else  {
    int i = 0; while (i < n_given) {
      lispval arg = given[i];
      if (KNO_NULLP(arg)) return null_ptr_error(fname,i);
      else if (KNO_QCHOICEP(arg))
	arg = (KNO_XQCHOICE(arg))->qchoiceval;
      else NO_ELSE;
      needed[i] = arg;
      i++;}
    while (i < n_needed) {needed[i++] = KNO_VOID;}
    return i;}
}

KNO_FASTOP lispval cprim_call(u8_string fname,kno_cprim cp,
			      int n,kno_argvec args,
			      kno_stack stack)
{
  kno_lisp_type *typeinfo = cp->fcn_typeinfo;
  const lispval *defaults = cp->fcn_defaults;
  int arity = cp->fcn_arity, buflen = stack->stack_buflen;
  lispval *argbuf = stack->stack_buf;
  kno_function f = (kno_function) cp;
  if (PRED_FALSE(cprim_prep(fname,n,args,buflen,argbuf,typeinfo,defaults) < 0))
    return KNO_ERROR;
  else {
    stack->stack_args = (kno_argvec) argbuf;
    stack->stack_arglen = buflen;}
  if (FCN_XCALLP(f))
    return f->fcn_handler.xcalln(stack,(kno_function)f,n,argbuf);
  else if ( (FCN_LEXPRP(f)) || (arity < 0) )
    return f->fcn_handler.calln(n,argbuf);
  else switch (arity) {
    case 0: return f->fcn_handler.call0();
    case 1: return f->fcn_handler.call1(argbuf[0]);
    case 2: return f->fcn_handler.call2(argbuf[0],argbuf[1]);
    case 3: return f->fcn_handler.call3(argbuf[0],argbuf[1],argbuf[2]);
    case 4: return f->fcn_handler.call4
	(argbuf[0],argbuf[1],argbuf[2],argbuf[3]);
    case 5: return f->fcn_handler.call5
	(argbuf[0],argbuf[1],argbuf[2],argbuf[3],argbuf[4]);
    case 6:
      return f->fcn_handler.call6
	(argbuf[0],argbuf[1],argbuf[2],argbuf[3],argbuf[4],argbuf[5]);
    case 7:
      return f->fcn_handler.call7
	(argbuf[0],argbuf[1],argbuf[2],argbuf[3],
	 argbuf[4],argbuf[5],argbuf[6]);
    case 8:
      return f->fcn_handler.call8
	(argbuf[0],argbuf[1],argbuf[2],argbuf[3],
	 argbuf[4],argbuf[5],argbuf[6],argbuf[7]);
    case 9:
      return f->fcn_handler.call9
	(argbuf[0],argbuf[1],argbuf[2],argbuf[3],
	 argbuf[4],argbuf[5],argbuf[6],argbuf[7],argbuf[8]);
    case 10:
      return f->fcn_handler.call10
	(argbuf[0],argbuf[1],argbuf[2],argbuf[3],
	 argbuf[4],argbuf[5],argbuf[6],argbuf[7],argbuf[8],
	 argbuf[9]);
    case 11:
      return f->fcn_handler.call11
	(argbuf[0],argbuf[1],argbuf[2],
	 argbuf[3],argbuf[4],argbuf[5],
	 argbuf[6],argbuf[7],argbuf[8],
	 argbuf[9],argbuf[10]);
    case 12:
      return f->fcn_handler.call12
	(argbuf[0],argbuf[1],argbuf[2],
	 argbuf[3],argbuf[4],argbuf[5],
	 argbuf[6],argbuf[7],argbuf[8],
	 argbuf[9],argbuf[10],argbuf[11]);
    case 13:
      return f->fcn_handler.call13
	(argbuf[0],argbuf[1],argbuf[2],
	 argbuf[3],argbuf[4],argbuf[5],
	 argbuf[6],argbuf[7],argbuf[8],
	 argbuf[9],argbuf[10],argbuf[11],
	 argbuf[12]);
    case 14:
      return f->fcn_handler.call14
	(argbuf[0],argbuf[1],argbuf[2],
	 argbuf[3],argbuf[4],argbuf[5],
	 argbuf[6],argbuf[7],argbuf[8],
	 argbuf[9],argbuf[10],argbuf[11],
	 argbuf[12],argbuf[13]);
    case 15:
      return f->fcn_handler.call15
	(argbuf[0],argbuf[1],argbuf[2],
	 argbuf[3],argbuf[4],argbuf[5],
	 argbuf[6],argbuf[7],argbuf[8],
	 argbuf[9],argbuf[10],argbuf[11],
                     argbuf[12],argbuf[13],argbuf[14]);
    default:
      if (FCN_XCALLP(f))
	return f->fcn_handler.xcalln(stack,f,n,args);
      else return f->fcn_handler.calln(n,args);}
}
