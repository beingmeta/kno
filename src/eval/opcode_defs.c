static lispval xref_type_error(lispval x,lispval tag)
{
  if (VOIDP(tag))
    fd_seterr(fd_TypeError,"XREF_OPCODE","compound",x);
  else {
    u8_string buf=fd_lisp2string(tag);
    fd_seterr(fd_TypeError,"XREF_OPCODE",buf,x);
    u8_free(buf);}
  return FD_ERROR_VALUE;
}

static lispval xref_op(struct FD_COMPOUND *c,long long i,lispval tag,int free)
{
  if ((VOIDP(tag)) || ((c->compound_typetag) == tag)) {
    if ((i>=0) && (i<c->compound_length)) {
      lispval *values = &(c->compound_0), value;
      if (c->compound_ismutable)
        u8_lock_mutex(&(c->compound_lock));
      value = values[i];
      fd_incref(value);
      if (c->compound_ismutable)
        u8_unlock_mutex(&(c->compound_lock));
      if (free) fd_decref((lispval)c);
      return value;}
    else {
      fd_seterr(fd_RangeError,"xref",NULL,(lispval)c);
      if (free) fd_decref((lispval)c);
      return FD_ERROR_VALUE;}}
  else {
    lispval err=xref_type_error((lispval)c,tag);
    if (free) fd_decref((lispval)c);
    return err;}
}

static lispval xref_opcode(lispval x,long long i,lispval tag)
{
  if (!(CONSP(x))) {
    if (EMPTYP(x))
      return EMPTY;
    else return xref_type_error(x,tag);}
  else if (FD_COMPOUNDP(x))
    return xref_op((struct FD_COMPOUND *)x,i,tag,1);
  else if (CHOICEP(x)) {
    lispval results = EMPTY;
    DO_CHOICES(c,x) {
      lispval r = xref_op((struct FD_COMPOUND *)c,i,tag,0);
      if (FD_ABORTED(r)) {
        fd_decref(results); results = r;
        FD_STOP_DO_CHOICES;
        break;}
      else {CHOICE_ADD(results,r);}}
    /* Need to free this */
    fd_decref(x);
    return results;}
  else return fd_err(fd_TypeError,"xref",fd_lisp2string(tag),x);
}

static lispval xpred_opcode(lispval x,lispval tag)
{
  if (FD_COMPOUNDP(x)) {
    int match = FD_COMPOUND_TYPEP(x,tag);
    fd_decref(x);
    if (match)
      return FD_TRUE;
    else return FD_FALSE;}
  else if (FD_CHOICEP(x)) {
    int match = 0, nomatch = 0;
    FD_DO_CHOICES(e,x) {
      if (FD_COMPOUND_TYPEP(e,tag)) {
	match=1; if (nomatch) {
	  FD_STOP_DO_CHOICES;
	  break;}}
      else {
	nomatch = 1;
	if (match) {
	  FD_STOP_DO_CHOICES;
	  break;}}}
    fd_decref(x);
    if (match) {
      if (nomatch) {
	lispval tmpvec[2] = {FD_FALSE,FD_TRUE};
	return fd_init_choice(NULL,2,tmpvec,FD_CHOICE_ISATOMIC);}
      else return FD_TRUE;}
    else return FD_FALSE;}
  else {
    fd_decref(x);
    return FD_FALSE;}
}

static lispval elt_opcode(lispval arg1,lispval arg2)
{
  if ((FD_SEQUENCEP(arg1)) && (FD_INTP(arg2))) {
    lispval result;
    long long off = FIX2INT(arg2), len = fd_seq_length(arg1);
    if (off<0) off = len+off;
    result = fd_seq_elt(arg1,off);
    if (result == FD_TYPE_ERROR)
      return fd_type_error(_("sequence"),"FD_OPCODE_ELT",arg1);
    else if (result == FD_RANGE_ERROR) {
      char buf[32];
      sprintf(buf,"%lld",off);
      return fd_err(fd_RangeError,"FD_OPCODE_ELT",u8_strdup(buf),arg1);}
    else return result;}
  else if (!(FD_SEQUENCEP(arg1)))
    return fd_type_error(_("sequence"),"FD_OPCODE_ELT",arg1);
  else return fd_type_error(_("fixnum"),"FD_OPCODE_ELT",arg2);
}

static lispval eltn_opcode(lispval arg1,int n,u8_context opname)
{
  if ( ( n == 0) && (FD_PAIRP(arg1)) )
    return fd_incref(FD_CAR(arg1));
  else if (VECTORP(arg1))
    if (VEC_LEN(arg1) > n)
      return fd_incref(VEC_REF(arg1,n));
    else return fd_err(fd_RangeError,opname,NULL,arg1);
  else if (COMPOUND_VECTORP(arg1))
    if (COMPOUND_VECLEN(arg1) > n)
      return fd_incref(XCOMPOUND_VEC_REF(arg1,n));
    else return fd_err(fd_RangeError,opname,NULL,arg1);
  else return fd_seq_elt(arg1,n);
}

/* PICK opcodes */

static lispval pickoids_opcode(lispval arg1)
{
  if (OIDP(arg1)) return arg1;
  else if (EMPTYP(arg1)) return arg1;
  else if ((CHOICEP(arg1)) || (PRECHOICEP(arg1))) {
    lispval choice, results = EMPTY;
    int free_choice = 0, all_oids = 1;
    if (CHOICEP(arg1)) choice = arg1;
    else {choice = fd_make_simple_choice(arg1); free_choice = 1;}
    {DO_CHOICES(elt,choice) {
        if (OIDP(elt)) {CHOICE_ADD(results,elt);}
        else if (all_oids) all_oids = 0;}}
    if (all_oids) {
      fd_decref(results);
      if (free_choice) return choice;
      else return fd_incref(choice);}
    else if (free_choice) fd_decref(choice);
    return fd_simplify_choice(results);}
  else return EMPTY;
}

static lispval pickstrings_opcode(lispval arg1)
{
  if ((CHOICEP(arg1)) || (PRECHOICEP(arg1))) {
    lispval choice, results = EMPTY;
    int free_choice = 0, all_strings = 1;
    if (CHOICEP(arg1)) choice = arg1;
    else {choice = fd_make_simple_choice(arg1); free_choice = 1;}
    {DO_CHOICES(elt,choice) {
        if (STRINGP(elt)) {
          fd_incref(elt); CHOICE_ADD(results,elt);}
        else if (all_strings) all_strings = 0;}}
    if (all_strings) {
      fd_decref(results);
      if (free_choice) return choice;
      else return fd_incref(choice);}
    else if (free_choice) fd_decref(choice);
    return fd_simplify_choice(results);}
  else if (STRINGP(arg1)) return fd_incref(arg1);
  else return EMPTY;
}

static lispval pickone_opcode(lispval normal)
{
  int n = FD_CHOICE_SIZE(normal);
  if (n) {
    lispval chosen;
    int i = u8_random(n);
    const lispval *data = FD_CHOICE_DATA(normal);
    chosen = data[i]; fd_incref(chosen);
    return chosen;}
  else return EMPTY;
}

static lispval nd1_call(lispval opcode,lispval arg1)
{
  switch (opcode) {
  case FD_AMBIGP_OPCODE:
    if (CHOICEP(arg1))
      return FD_TRUE;
    else return FD_FALSE;
  case FD_SINGLETONP_OPCODE:
    if (EMPTYP(arg1))
      return FD_FALSE;
    else if (CHOICEP(arg1))
      return FD_FALSE;
    else return FD_TRUE;
  case FD_FAILP_OPCODE:
    if (arg1==EMPTY)
      return FD_TRUE;
    else return FD_FALSE;
  case FD_EXISTSP_OPCODE:
    if (arg1==EMPTY)
      return FD_FALSE;
    else return FD_TRUE;
  case FD_SINGLETON_OPCODE:
    if (CHOICEP(arg1))
      return EMPTY;
    else return fd_incref(arg1);
  case FD_CAR_OPCODE:
    if (EMPTYP(arg1))
      return arg1;
    else if (PAIRP(arg1))
      return fd_incref(FD_CAR(arg1));
    else if (CHOICEP(arg1)) {
      lispval results = EMPTY;
      DO_CHOICES(arg,arg1)
        if (PAIRP(arg)) {
          lispval car = FD_CAR(arg);
          fd_incref(car);
          CHOICE_ADD(results,car);}
        else {
          fd_decref(results);
          return fd_type_error(_("pair"),"CAR opcode",arg);}
      return results;}
    else return fd_type_error(_("pair"),"CAR opcode",arg1);
  case FD_CDR_OPCODE:
    if (EMPTYP(arg1)) return arg1;
    else if (PAIRP(arg1))
      return fd_incref(FD_CDR(arg1));
    else if (CHOICEP(arg1)) {
      lispval results = EMPTY;
      DO_CHOICES(arg,arg1)
        if (PAIRP(arg)) {
          lispval cdr = FD_CDR(arg); fd_incref(cdr);
          CHOICE_ADD(results,cdr);}
        else {
          fd_decref(results);
          return fd_type_error(_("pair"),"CDR opcode",arg);}
      return results;}
    else return fd_type_error(_("pair"),"CDR opcode",arg1);
  case FD_LENGTH_OPCODE:
    if (arg1==EMPTY) return EMPTY;
    else if (CHOICEP(arg1)) {
      lispval results = EMPTY;
      DO_CHOICES(arg,arg1) {
        if (FD_SEQUENCEP(arg)) {
          int len = fd_seq_length(arg);
          lispval dlen = FD_INT(len);
          CHOICE_ADD(results,dlen);}
        else {
          fd_decref(results);
          return fd_type_error(_("sequence"),"LENGTH opcode",arg);}}
      return fd_simplify_choice(results);}
    else if (FD_SEQUENCEP(arg1))
      return FD_INT(fd_seq_length(arg1));
    else return fd_type_error(_("sequence"),"LENGTH opcode",arg1);
  case FD_QCHOICE_OPCODE:
    if (CHOICEP(arg1)) {
      fd_incref(arg1);
      return fd_init_qchoice(NULL,arg1);}
    else if (PRECHOICEP(arg1))
      return fd_init_qchoice(NULL,fd_make_simple_choice(arg1));
    else if (EMPTYP(arg1))
      return fd_init_qchoice(NULL,EMPTY);
    else return fd_incref(arg1);
  case FD_CHOICE_SIZE_OPCODE:
    if (CHOICEP(arg1)) {
      int sz = FD_CHOICE_SIZE(arg1);
      return FD_INT(sz);}
    else if (PRECHOICEP(arg1)) {
      lispval simple = fd_make_simple_choice(arg1);
      int size = FD_CHOICE_SIZE(simple);
      fd_decref(simple);
      return FD_INT(size);}
    else if (EMPTYP(arg1))
      return FD_INT(0);
    else return FD_INT(1);
  case FD_PICKOIDS_OPCODE:
    return pickoids_opcode(arg1);
  case FD_PICKSTRINGS_OPCODE:
    return pickstrings_opcode(arg1);
  case FD_PICKONE_OPCODE:
    if (CHOICEP(arg1))
      return pickone_opcode(arg1);
    else return fd_incref(arg1);
  case FD_IFEXISTS_OPCODE:
    if (EMPTYP(arg1))
      return VOID;
    else return fd_incref(arg1);
  case FD_FIXCHOICE_OPCODE:
    if (PRECHOICEP(arg1))
      return fd_simplify_choice(arg1);
    else return fd_incref(arg1);
  default:
    return fd_err(_("Invalid opcode"),"opcode eval",NULL,VOID);
  }
}

static lispval d1_call(lispval opcode,lispval arg1)
{
  switch (opcode) {
  case FD_MINUS1_OPCODE:
  case FD_PLUS1_OPCODE: {
    int delta = (opcode == FD_MINUS1_OPCODE) ? (-1) : (1);
    if (FIXNUMP(arg1)) {
      long long iarg = FIX2INT(arg1);
      return FD_INT(iarg+delta);}
    else if (NUMBERP(arg1))
      return fd_plus(arg1,FD_INT(delta));
    else return fd_type_error(_("number"),"opcode 1+/-",arg1);}
  case FD_NUMBERP_OPCODE:
    if (NUMBERP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_ZEROP_OPCODE:
    if (arg1==FD_INT(0)) return FD_TRUE; else return FD_FALSE;
  case FD_VECTORP_OPCODE:
    if (VECTORP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_PAIRP_OPCODE:
    if (PAIRP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_EMPTY_LISTP_OPCODE:
    if (arg1==NIL) return FD_TRUE; else return FD_FALSE;
  case FD_STRINGP_OPCODE:
    if (STRINGP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_OIDP_OPCODE:
    if (OIDP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_SYMBOLP_OPCODE:
    if (FD_SYMBOLP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_FIXNUMP_OPCODE:
    if (FIXNUMP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_FLONUMP_OPCODE:
    if (FD_FLONUMP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_SEQUENCEP_OPCODE:
    if (FD_SEQUENCEP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_TABLEP_OPCODE:
    if (FD_SEQUENCEP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_CADR_OPCODE: {
    lispval cdr = FD_CDR(arg1);
    if (PAIRP(cdr)) return fd_incref(FD_CAR(cdr));
    else return fd_err(fd_RangeError,"FD_CADR",NULL,arg1);}
  case FD_CDDR_OPCODE: {
    lispval cdr = FD_CDR(arg1);
    if (PAIRP(cdr)) return fd_incref(FD_CDR(cdr));
    else return fd_err(fd_RangeError,"FD_CDDR",NULL,arg1);}
  case FD_CADDR_OPCODE: {
    lispval cdr = FD_CDR(arg1);
    if (PAIRP(cdr)) {
      lispval cddr = FD_CDR(cdr);
      if (PAIRP(cddr)) return fd_incref(FD_CAR(cddr));
      else return fd_err(fd_RangeError,"FD_CADDR",NULL,arg1);}
    else return fd_err(fd_RangeError,"FD_CADDR",NULL,arg1);}
  case FD_CDDDR_OPCODE: {
    lispval cdr = FD_CDR(arg1);
    if (PAIRP(cdr)) {
      lispval cddr = FD_CDR(cdr);
      if (PAIRP(cddr))
        return fd_incref(FD_CDR(cddr));
      else return fd_err(fd_RangeError,"FD_CDDR",NULL,arg1);}
    else return fd_err(fd_RangeError,"FD_CDDDR",NULL,arg1);}
  case FD_FIRST_OPCODE:
    return eltn_opcode(arg1,0,"FD_FIRST_OPCODE");
  case FD_SECOND_OPCODE:
    return eltn_opcode(arg1,1,"FD_SECOND_OPCODE");
  case FD_THIRD_OPCODE:
    return eltn_opcode(arg1,2,"FD_THIRD_OPCODE");
  case FD_TONUMBER_OPCODE:
    if (FIXNUMP(arg1)) return arg1;
    else if (NUMBERP(arg1)) return fd_incref(arg1);
    else if (STRINGP(arg1))
      return fd_string2number(CSTRING(arg1),10);
    else if (FD_CHARACTERP(arg1))
      return FD_INT(FD_CHARCODE(arg1));
    else return fd_type_error(_("number|string"),"opcode ->number",arg1);
  default:
    return fd_err(_("Invalid opcode"),"opcode eval",NULL,VOID);
  }
}

static lispval d2_call(lispval opcode,lispval arg1,lispval arg2)
{
  switch (opcode) {
  case FD_ELT_OPCODE:
    return elt_opcode(arg1,arg2);
  case FD_CONS_OPCODE:
    return fd_init_pair(NULL,fd_incref(arg1),fd_incref(arg2));
  case FD_EQ_OPCODE:
    if (arg1==arg2) return FD_TRUE; else return FD_FALSE;
  case FD_EQV_OPCODE: {
    if (arg1==arg2) return FD_TRUE;
    else if ((NUMBERP(arg1)) && (NUMBERP(arg2)))
      if (fd_numcompare(arg1,arg2)==0)
        return FD_TRUE; else return FD_FALSE;
    else return FD_FALSE;}
  case FD_EQUAL_OPCODE: {
    if ((ATOMICP(arg1)) && (ATOMICP(arg2)))
      if (arg1==arg2) return FD_TRUE; else return FD_FALSE;
    else if (FD_EQUAL(arg1,arg2)) return FD_TRUE;
    else return FD_FALSE;}
  default:
    return fd_err(_("Invalid opcode"),"opcode eval",NULL,VOID);
  }
}


FD_FASTOP int numeric_argp(lispval x)
{
  /* This checks if there is a type error.
     The empty choice isn't a type error since it will just
     generate an empty choice as a result. */
  if ((EMPTYP(x))||(FIXNUMP(x)))
    return 1;
  else if (!(CONSP(x)))
    return 0;
  else switch (FD_CONSPTR_TYPE(x)) {
    case fd_flonum_type: case fd_bigint_type:
    case fd_rational_type: case fd_complex_type:
      return 1;
    case fd_choice_type: case fd_prechoice_type: {
      DO_CHOICES(a,x) {
        if (FIXNUMP(a)) {}
        else if (PRED_TRUE(NUMBERP(a))) {}
        else {
          FD_STOP_DO_CHOICES;
          return 0;}}
      return 1;}
    default:
      return 0;}
}

static lispval numop_call(lispval opcode,lispval arg1,lispval arg2)
{
  if (FD_EXPECT_FALSE(!(numeric_argp(arg1))))
    return fd_err("NotANumber","numop_call",NULL,arg1);
  else if (FD_EXPECT_FALSE(!(numeric_argp(arg2))))
    return fd_err("NotANumber","numop_call",NULL,arg2);
  else switch (opcode) {
    case FD_NUMEQ_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
        if ((FIX2INT(arg1)) == (FIX2INT(arg2)))
          return FD_TRUE;
        else return FD_FALSE;
      else if (fd_numcompare(arg1,arg2)==0) return FD_TRUE;
      else return FD_FALSE;}
    case FD_GT_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
        if ((FIX2INT(arg1))>(FIX2INT(arg2)))
          return FD_TRUE;
        else return FD_FALSE;
      else if (fd_numcompare(arg1,arg2)>0) return FD_TRUE;
      else return FD_FALSE;}
    case FD_GTE_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
        if ((FIX2INT(arg1))>=(FIX2INT(arg2)))
          return FD_TRUE;
        else return FD_FALSE;
      else if (fd_numcompare(arg1,arg2)>=0) return FD_TRUE;
      else return FD_FALSE;}
    case FD_LT_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
        if ((FIX2INT(arg1))<(FIX2INT(arg2)))
          return FD_TRUE;
        else return FD_FALSE;
      else if (fd_numcompare(arg1,arg2)<0) return FD_TRUE;
      else return FD_FALSE;}
    case FD_LTE_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
        if ((FIX2INT(arg1))<=(FIX2INT(arg2)))
          return FD_TRUE;
        else return FD_FALSE;
      else if (fd_numcompare(arg1,arg2)<=0) return FD_TRUE;
      else return FD_FALSE;}
    case FD_PLUS_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
        long long m = FIX2INT(arg1), n = FIX2INT(arg2);
        return FD_INT(m+n);}
      else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
        double x = FD_FLONUM(arg1), y = FD_FLONUM(arg2);
        return fd_init_double(NULL,x+y);}
      else return fd_plus(arg1,arg2);}
    case FD_MINUS_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
        long long m = FIX2INT(arg1), n = FIX2INT(arg2);
        return FD_INT(m-n);}
      else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
        double x = FD_FLONUM(arg1), y = FD_FLONUM(arg2);
        return fd_init_double(NULL,x-y);}
      else return fd_subtract(arg1,arg2);}
    case FD_TIMES_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
        long long m = FIX2INT(arg1), n = FIX2INT(arg2);
        return FD_INT(m*n);}
      else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
        double x = FD_FLONUM(arg1), y = FD_FLONUM(arg2);
        return fd_init_double(NULL,x*y);}
      else return fd_multiply(arg1,arg2);}
    case FD_FLODIV_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
        long long m = FIX2INT(arg1), n = FIX2INT(arg2);
        double x = (double)m, y = (double)n;
        return fd_init_double(NULL,x/y);}
      else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
        double x = FD_FLONUM(arg1), y = FD_FLONUM(arg2);
        return fd_init_double(NULL,x/y);}
      else {
        double x = fd_todouble(arg1), y = fd_todouble(arg2);
        return fd_init_double(NULL,x/y);}}
    default:
      return fd_err(_("Invalid opcode"),"numop_call",NULL,VOID);
    }
}

static lispval nd2_call(lispval opcode,lispval arg1,lispval arg2)
{
  lispval result = FD_ERROR_VALUE;
  lispval argv[2]={arg1,arg2};
  if (FD_ABORTED(arg2)) result = arg2;
  else if (VOIDP(arg2)) {
    result = fd_err(fd_VoidArgument,"OPCODE setop",NULL,opcode);}
  else switch (opcode) {
    case FD_IDENTICAL_OPCODE:
      if (arg1==arg2) result = FD_TRUE;
      else if (FD_EQUAL(arg1,arg2)) result = FD_TRUE;
      else result = FD_FALSE;
      break;
    case FD_OVERLAPS_OPCODE:
      if (arg1==arg2) result = FD_TRUE;
      else if (fd_overlapp(arg1,arg2)) result = FD_TRUE;
      else result = FD_FALSE;
      break;
    case FD_CONTAINSP_OPCODE:
      if (fd_containsp(arg1,arg2)) result = FD_TRUE;
      else result = FD_FALSE;
      break;
      /*
    case FD_INTERSECT_OPCODE:
      if ((EMPTYP(arg1)) || (EMPTYP(arg2)))
        result = EMPTY;
      else result = fd_intersection(argv,2);
      break;
    case FD_UNION_OPCODE:
      if (EMPTYP(arg1)) result = fd_incref(arg2);
      else if (EMPTYP(arg2)) result = fd_incref(arg1);
      else result = fd_union(argv,2);
      break;
    case FD_DIFFERENCE_OPCODE:
      if ((EMPTYP(arg1)) || (EMPTYP(arg2)))
        result = fd_incref(arg1);
      else result = fd_difference(arg1,arg2);
      break;
      */
    case FD_CHOICEREF_OPCODE:
      if (!(FIXNUMP(arg2))) {
        fd_decref(arg1);
        result = fd_err(fd_SyntaxError,"choiceref_opcode",NULL,arg2);}
      else {
        int i = FIX2INT(arg2);
        if (i<0)
          result = fd_err(fd_SyntaxError,"choiceref_opcode",NULL,arg2);
        else if ((i==0)&&(EMPTYP(arg1)))
          result = VOID;
        else if (CHOICEP(arg1)) {
          struct FD_CHOICE *ch = (fd_choice)arg1;
          if (i<ch->choice_size) {
            const lispval *elts = FD_XCHOICE_DATA(ch);
            lispval elt = elts[i];
            result = fd_incref(elt);}
          else result = VOID;}
        else if (i==0) result=arg1;
        else result = VOID;}
    default:
      result = fd_err(_("Invalid opcode"),"numop_call",NULL,VOID);
    }
  return result;
}

static lispval try_op(lispval exprs,fd_lexenv env,
                      fd_stack stack,int tail)
{
  while (PAIRP(exprs)) {
    lispval expr = pop_arg(exprs);
    if (NILP(exprs))
      return _fd_fast_eval(expr,env,stack,tail);
    else {
      lispval val  = _fd_fast_eval(expr,env,stack,0);
      if (FD_ABORTED(val)) return val;
      else if (!(EMPTYP(val)))
        return val;
      else fd_decref(val);}}
  return EMPTY;
}

static lispval union_op(lispval exprs,fd_lexenv env,fd_stack stack)
{
  lispval result = FD_EMPTY_CHOICE;
  while (PAIRP(exprs)) {
    lispval expr = pop_arg(exprs);
    lispval val  = _fd_fast_eval(expr,env,stack,0);
    if (FD_ABORTED(val)) {
      fd_decref(result);
      return val;}
    else {FD_ADD_TO_CHOICE(result,val);}}
  return fd_simplify_choice(result);
}

static lispval intersect_op(lispval exprs,fd_lexenv env,fd_stack stack)
{
  lispval result = VOID;
  while (PAIRP(exprs)) {
    lispval expr = pop_arg(exprs);
    lispval val  = _fd_fast_eval(expr,env,stack,0);
    if (FD_ABORTED(val)) {
      fd_decref(result);
      return val;}
    else if (FD_VOIDP(result))
      result = val;
    else if (FD_EMPTYP(val)) {
      fd_decref(result);
      return val;}
    else {
      lispval combine[2] = {result,val};
      lispval combined = fd_intersection(combine,2);
      fd_decref(result);
      if (FD_EMPTYP(combined))
	return combined;
      else result=combined;}}
  return fd_simplify_choice(result);
}

static lispval difference_op(lispval exprs,fd_lexenv env,fd_stack stack)
{
  lispval result = VOID;
  while (PAIRP(exprs)) {
    lispval expr = pop_arg(exprs);
    lispval val  = _fd_fast_eval(expr,env,stack,0);
    if (FD_ABORTED(val)) {
      fd_decref(result);
      return val;}
    else if (FD_VOIDP(result))
      result = val;
    else if (FD_EMPTYP(val)) {}
    else {
      lispval combined = fd_difference(result,val);
      fd_decref(result);
      if (FD_EMPTYP(combined))
	return combined;
      else result=combined;}}
  return fd_simplify_choice(result);
}

static lispval and_op(lispval exprs,fd_lexenv env,fd_stack stack,int tail)
{
  while (PAIRP(exprs)) {
    lispval expr = pop_arg(exprs);
    if (NILP(exprs))
      return _fd_fast_eval(expr,env,stack,tail);
    else {
      lispval val  = _fd_fast_eval(expr,env,stack,0);
      if (FD_ABORTED(val)) return val;
      else if (FALSEP(val))
        return FD_FALSE;
      else fd_decref(val);}}
  return FD_TRUE;
}

static lispval or_op(lispval exprs,fd_lexenv env,fd_stack stack,int tail)
{
  while (PAIRP(exprs)) {
    lispval expr = pop_arg(exprs);
    if (NILP(exprs))
      return _fd_fast_eval(expr,env,stack,tail);
    else {
      lispval val  = _fd_fast_eval(expr,env,stack,0);
      if (FD_ABORTED(val)) return val;
      else if (!(FALSEP(val)))
        return val;
      else {}}}
  return FD_FALSE;
}

/* Loop */

static lispval until_opcode(lispval expr,fd_lexenv env,fd_stack stack)
{
  lispval params = FD_CDR(expr);
  lispval test_expr = FD_CAR(params), loop_body = FD_CDR(params);
  if (VOIDP(test_expr))
    return fd_err(fd_SyntaxError,"FD_LOOP_OPCODE",NULL,expr);
  lispval test_val = _fd_fast_eval(test_expr,env,stack,0);
  if (FD_ABORTED(test_val))
    return test_val;
  else while (FALSEP(test_val)) {
      lispval body_result=eval_body(loop_body,env,stack,0);
      if (FD_BROKEP(body_result))
        return FD_FALSE;
      else if (FD_ABORTED(body_result))
        return body_result;
      else fd_decref(body_result);
      test_val = _fd_fast_eval(test_expr,env,stack,0);
      if (FD_ABORTED(test_val))
        return test_val;}
  return test_val;
}

/* Assignment */

#define CURRENT_VALUEP(x)                                 \
  (! ((cur == FD_DEFAULT_VALUE) || (cur == FD_UNBOUND) || \
      (cur == VOID) || (cur == FD_NULL)))

static lispval combine_values(lispval combiner,lispval cur,lispval value)
{
  int use_cur=((FD_ABORTP(cur)) || (CURRENT_VALUEP(cur)));
  switch (combiner) {
  case VOID: case FD_FALSE:
    return value;
  case FD_TRUE: case FD_DEFAULT_VALUE:
    if (use_cur)
      return cur;
    else return value;
  case FD_PLUS_OPCODE:
    if (!(use_cur)) cur=FD_FIXNUM_ZERO;
    if ( (FIXNUMP(value)) && (FIXNUMP(cur)) ) {
      long long ic=FIX2INT(cur), ip=FIX2INT(value);
      return FD_MAKE_FIXNUM(ic+ip);}
    else return fd_plus(cur,value);
  case FD_MINUS_OPCODE:
    if (!(use_cur)) cur=FD_FIXNUM_ZERO;
    if ( (FIXNUMP(value)) && (FIXNUMP(cur)) ) {
      long long ic=FIX2INT(cur), im=FIX2INT(value);
      return FD_MAKE_FIXNUM(ic-im);}
    else return fd_subtract(cur,value);
  default:
    return value;
  }
}
static lispval assignop(fd_stack stack,fd_lexenv env,
                        lispval var,lispval expr,lispval combiner)
{
  if (FD_LEXREFP(var)) {
    int up = FD_LEXREF_UP(var);
    int across = FD_LEXREF_ACROSS(var);
    fd_lexenv scan = ( (env->env_copy) ? (env->env_copy) : (env) );
    while ((up)&&(scan)) {
      fd_lexenv parent = scan->env_parent;
      if ((parent) && (parent->env_copy))
        scan = parent->env_copy;
      else scan = parent;
      up--;}
    if (PRED_TRUE(scan!=NULL)) {
      lispval bindings = scan->env_bindings;
      if (PRED_TRUE(SCHEMAPP(bindings))) {
        struct FD_SCHEMAP *map = (struct FD_SCHEMAP *)bindings;
        int map_len = map->schema_length;
        if (PRED_TRUE( across < map_len )) {
          lispval *values = map->schema_values;
          lispval cur     = values[across];
          if ( map->schemap_stackvals ) {
            fd_incref_vec(values,map_len);
            map->schemap_stackvals = 0;}
          if ( ( (combiner == FD_TRUE) || (combiner == FD_DEFAULT) ) &&
               ( (CURRENT_VALUEP(cur)) || (FD_ABORTED(cur)) ) ) {
            if (FD_ABORTED(cur))
              return cur;
            else return VOID;}
          else {
	    lispval value = _fd_fast_eval(expr,env,stack,0);
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
                struct FD_SCHEMAP *new_map =
                  (struct FD_SCHEMAP *) new_bindings;
                values = new_map->schema_values;
                cur = values[across];}}
            if (FD_ABORTED(value))
              return value;
            else if ( (combiner == FD_FALSE) || (combiner == VOID) ) {
              /* Replace the currnet value */
              values[across]=value;
              fd_decref(cur);}
            else if (combiner == FD_UNION_OPCODE) {
              if (FD_ABORTED(value)) return value;
              if ((cur==VOID)||(cur==FD_UNBOUND)||(cur==EMPTY))
                values[across]=value;
              else {CHOICE_ADD(values[across],value);}}
            else {
              lispval newv=combine_values(combiner,cur,value);
              if (cur != newv) {
                values[across]=newv;
                fd_decref(cur);
                if (newv != value) fd_decref(value);}
              else fd_decref(value);}
            return VOID;}}}}
    u8_string lexref=u8_mkstring("up%d/across%d",up,across);
    lispval env_copy=(lispval)fd_copy_env(env);
    return fd_err("BadLexref","ASSIGN_OPCODE",lexref,env_copy);}
  else if ((PAIRP(var)) &&
           (FD_SYMBOLP(FD_CAR(var))) &&
           (TABLEP(FD_CDR(var)))) {
    int rv=-1;
    lispval table=FD_CDR(var), sym=FD_CAR(var);
    lispval value = _fd_fast_eval(expr,env,stack,0);
    if ( (combiner == FD_FALSE) || (combiner == VOID) ) {
      if (FD_LEXENVP(table))
        rv=fd_assign_value(sym,value,(fd_lexenv)table);
      else rv=fd_store(table,sym,value);}
    else if (combiner == FD_UNION_OPCODE) {
      if (FD_LEXENVP(table))
        rv=fd_add_value(sym,value,(fd_lexenv)table);
      else rv=fd_add(table,sym,value);}
    else {
      lispval cur=fd_get(table,sym,FD_UNBOUND);
      lispval newv=combine_values(combiner,cur,value);
      if (FD_ABORTED(newv))
        rv=-1;
      else rv=fd_store(table,sym,newv);
      fd_decref(cur);
      fd_decref(newv);}
    fd_decref(value);
    if (rv<0) {
      fd_seterr("AssignFailed","ASSIGN_OPCODE",NULL,expr);
      return FD_ERROR_VALUE;}
    else return VOID;}
  return fd_err(fd_SyntaxError,"ASSIGN_OPCODE",NULL,expr);
}

/* Binding */

static lispval bindop(lispval op,
                      struct FD_STACK *_stack,fd_lexenv env,
                      lispval vars,lispval inits,lispval body,
                      int tail)
{
  int i=0, n=VEC_LEN(vars);
  FD_PUSH_STACK(bind_stack,"bindop","opframe",op);
  INIT_STACK_SCHEMA(bind_stack,bound,env,n,VEC_DATA(vars));
  lispval *values=bound_bindings.schema_values;
  lispval *exprs=VEC_DATA(inits);
  fd_lexenv env_copy=NULL;
  while (i<n) {
    lispval val_expr=exprs[i];
    lispval val=_fd_fast_eval(val_expr,bound,bind_stack,0);
    if (FD_ABORTED(val)) _return val;
    if ( (env_copy == NULL) && (bound->env_copy) ) {
      env_copy=bound->env_copy; bound=env_copy;
      values=((fd_schemap)(bound->env_bindings))->schema_values;}
    values[i++]=val;}
  lispval result = eval_body(body,bound,bind_stack,tail);
  fd_pop_stack(bind_stack);
  return result;
}

static void reset_env_op(fd_lexenv env)
{
  if ( (env->env_copy) && (env != env->env_copy) ) {
    lispval tofree = (lispval) env;
    env->env_copy=NULL;
    fd_decref(tofree);}
}

static lispval handle_table_opcode(lispval opcode,lispval expr,
                                   fd_lexenv env,
                                   fd_stack _stack,
                                   int tail)
{
  lispval args = FD_CDR(expr);
  lispval subject_arg = pop_arg(args);
  if (FD_EXPECT_FALSE(!(FD_PAIRP(args))))
    return fd_err(fd_TooFewArgs,"handle_table_opcode",NULL,expr);
  lispval subject = arg_eval(subject_arg,env,_stack);
  if (FD_ABORTP(subject))
    return subject;
  else if (FD_EMPTYP(subject)) {
    switch (opcode) {
    case FD_GET_OPCODE: case FD_PRIMGET_OPCODE:
      return FD_EMPTY_CHOICE;
    case FD_TEST_OPCODE: case FD_PRIMTEST_OPCODE:
      return FD_FALSE;
    case FD_ASSERT_OPCODE: case FD_ADD_OPCODE:
    case FD_RETRACT_OPCODE: case FD_DROP_OPCODE:
      return FD_VOID;
    default:
      return fd_err("BadOpcode","handle_table_opcode",NULL,expr);}}
  else if (FD_PRECHOICEP(subject))
    subject = fd_simplify_choice(subject);
  else NO_ELSE;
  if (FD_EXPECT_FALSE(!(FD_TABLEP(subject)))) {
    fd_seterr("NotATable","handle_table_opcode",NULL,subject);
    fd_decref(subject);
    return FD_ERROR_VALUE;}
  else NO_ELSE;
  lispval slotid_arg = pop_arg(args);
  lispval slotid = arg_eval(slotid_arg,env,_stack);
  if (FD_ABORTP(slotid)) {
    fd_decref(subject);
    return slotid;}
  else if (FD_PRECHOICEP(slotid))
    slotid = fd_simplify_choice(slotid);
  else NO_ELSE;
  lispval value_arg = (FD_PAIRP(args)) ? (pop_arg(args)) : (FD_VOID);
  lispval value = (FD_VOIDP(value_arg)) ? (FD_VOID) :
    (arg_eval(value_arg,env,_stack));
  if (FD_ABORTP(value)) {
    fd_decref(subject); fd_decref(slotid);
    return value;}
  else if (FD_PRECHOICEP(value))
    value = fd_simplify_choice(value);
  else NO_ELSE;
  lispval result = FD_VOID;
  switch (opcode) {
  case FD_GET_OPCODE:
    result = fd_fget(subject,slotid); break;
  case FD_PRIMGET_OPCODE:
    if (FD_VOIDP(value)) value = FD_EMPTY_CHOICE;
    result = fd_get(subject,slotid,value); break;
  case FD_TEST_OPCODE:
    result = fd_ftest(subject,slotid,value); break;
  case FD_PRIMTEST_OPCODE: {
    int rv = fd_test(subject,slotid,value);
    if (rv < 0)
      result = FD_ERROR_VALUE;
    else if (rv)
      result = FD_TRUE;
    else result = FD_FALSE;
    break;}
  case FD_ADD_OPCODE:
    if (FD_VOIDP(value))
      result = fd_err(fd_TooFewArgs,"handle_table_opcode/add",NULL,expr);
    else {
      int rv = fd_add(subject,slotid,value);
      if (rv < 0)
        result = FD_ERROR_VALUE;
      else if (rv)
        result = FD_TRUE;
      else result = FD_FALSE;}
    break;
  case FD_DROP_OPCODE: {
    int rv = fd_drop(subject,slotid,value);
    if (rv < 0)
      result = FD_ERROR_VALUE;
    else if (rv)
      result = FD_TRUE;
    else result = FD_FALSE;
    break;}
  case FD_STORE_OPCODE:
    if (FD_VOIDP(value))
      result = fd_err(fd_TooFewArgs,"handle_table_opcode/store",NULL,expr);
    else {
      int rv = fd_store(subject,slotid,value);
      if (rv < 0)
        result = FD_ERROR_VALUE;
      else if (rv)
        result = FD_TRUE;
      else result = FD_FALSE;}
    break;
  case FD_ASSERT_OPCODE:
    if (FD_VOIDP(value))
      result = fd_err(fd_TooFewArgs,"handle_table_opcode",NULL,expr);
    else result = fd_assert(subject,slotid,value);
    break;
  case FD_RETRACT_OPCODE:
    result = fd_retract(subject,slotid,value);
    break;
  default:
    result = fd_err("BadOpcode","handle_table_opcode",NULL,expr);}
  fd_decref(subject); fd_decref(slotid); fd_decref(value);
  return result;
}

/* Opcode dispatch */

static lispval handle_special_opcode(lispval opcode,lispval args,lispval expr,
                                     fd_lexenv env,
                                     fd_stack _stack,
                                     int tail)
{
  switch (opcode) {
  case FD_QUOTE_OPCODE: {
    lispval arg = pop_arg(args);
    if (FD_EXPECT_FALSE(FD_VOIDP(arg)))
      return fd_err(fd_SyntaxError,"opcode_eval",NULL,expr);
    else if (FD_CONSP(arg))
      return fd_incref(arg);
    else return arg;}
  case FD_SYMREF_OPCODE: {
    lispval refenv=pop_arg(args);
    lispval sym=pop_arg(args);
    if (FD_EXPECT_FALSE(!(FD_SYMBOLP(sym))))
      return fd_err(fd_SyntaxError,"FD_SYMREF_OPCODE/badsym",NULL,expr);
    if (HASHTABLEP(refenv))
      return fd_hashtable_get((fd_hashtable)refenv,sym,FD_UNBOUND);
    else if (FD_LEXENVP(refenv))
      return fd_symeval(sym,(fd_lexenv)refenv);
    else if (TABLEP(refenv))
      return fd_get(refenv,sym,FD_UNBOUND);
    else return fd_err(fd_SyntaxError,"FD_SYMREF_OPCODE/badenv",NULL,expr);}
  case FD_VOID_OPCODE: {
    return VOID;}
  case FD_RESET_ENV_OPCODE: {
    reset_env_op(env);
    return VOID;}
  case FD_BEGIN_OPCODE:
    return eval_body(FD_CDR(expr),env,_stack,tail);
  case FD_UNTIL_OPCODE:
    return until_opcode(expr,env,_stack);

  case FD_BRANCH_OPCODE: {
    lispval test_expr = pop_arg(args);
    if (VOIDP(test_expr))
      return fd_err(fd_SyntaxError,"FD_BRANCH_OPCODE",NULL,expr);
    lispval test_val = _fd_fast_eval(test_expr,env,_stack,0);
    if (FD_ABORTED(test_val))
      return test_val;
    else if (FD_FALSEP(test_val)) { /* (  || (FD_EMPTYP(test_val)) ) */
      pop_arg(args);
      lispval else_expr = pop_arg(args);
      return _fd_fast_eval(else_expr,env,_stack,tail);}
    else {
      lispval then = pop_arg(args);
      U8_MAYBE_UNUSED lispval ignore = pop_arg(args);
      fd_decref(test_val);
      return _fd_fast_eval(then,env,_stack,tail);}}

  case FD_BIND_OPCODE: {
    lispval vars=pop_arg(args);
    lispval inits=pop_arg(args);
    lispval body=pop_arg(args);
    return bindop(opcode,_stack,env,vars,inits,body,tail);}

  case FD_ASSIGN_OPCODE: {
    lispval var = pop_arg(args);
    lispval combiner = pop_arg(args);
    lispval val_expr = pop_arg(args);
    return assignop(_stack,env,var,val_expr,combiner);}

  case FD_XREF_OPCODE: {
    lispval off_arg = pop_arg(args);
    lispval type_arg = pop_arg(args);
    lispval obj_expr = pop_arg(args);
    if ( (FIXNUMP(off_arg)) && (! (VOIDP(obj_expr)) ) ) {
      lispval obj_arg=fast_eval(obj_expr,env);
      return xref_opcode(fd_simplify_choice(obj_arg),
                         FIX2INT(off_arg),
                         type_arg);}
    fd_seterr(fd_SyntaxError,"FD_XREF_OPCODE",NULL,expr);
    return FD_ERROR_VALUE;}

  case FD_XPRED_OPCODE: {
    lispval type_arg = pop_arg(args);
    lispval obj_expr = pop_arg(args);
    if (FD_EXPECT_FALSE(VOIDP(obj_expr))) {
      fd_seterr(fd_SyntaxError,"FD_XREF_OPCODE",NULL,expr);
      return FD_ERROR_VALUE;}
    lispval obj_arg=fast_eval(obj_expr,env);
    if (FD_ABORTED(obj_arg)) return obj_arg;
    else if (FD_EMPTYP(obj_arg)) return FD_EMPTY;
    else if ( (FD_COMPOUNDP(obj_arg)) || (FD_AMBIGP(obj_arg)) )
      return xpred_opcode(fd_simplify_choice(obj_arg),type_arg);
    else {
      fd_decref(obj_arg);
      return FD_FALSE;}}

  case FD_NOT_OPCODE: {
    lispval arg_val = _fd_fast_eval(pop_arg(args),env,_stack,0);
    if (FALSEP(arg_val))
      return FD_TRUE;
    else {
      fd_decref(arg_val);
      return FD_FALSE;}}

  case FD_BREAK_OPCODE:
    return FD_BREAK;

  case FD_TRY_OPCODE: return try_op(args,env,_stack,tail);
  case FD_AND_OPCODE: return and_op(args,env,_stack,tail);
  case FD_OR_OPCODE:  return or_op(args,env,_stack,tail);
  
  case FD_INTERSECT_OPCODE: return intersect_op(args,env,_stack);
  case FD_UNION_OPCODE: return union_op(args,env,_stack);
  case FD_DIFFERENCE_OPCODE: return difference_op(args,env,_stack);

  default:
    return fd_err("BadOpcode","handle_special_opcode",NULL,expr);
  }
}


