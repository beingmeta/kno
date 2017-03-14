#include "framerd/dbprims.h"

static fdtype op_eval(fdtype x,fd_lispenv env,int tail);

/* Opcode names */

u8_string fd_opcode_names[1024];

int fd_opcode_table_len=256;

static int unparse_opcode(u8_output out,fdtype opcode)
{
  int opcode_offset=(FD_GET_IMMEDIATE(opcode,fd_opcode_type));
  if (opcode_offset>fd_opcode_table_len) {
    u8_printf(out,"##invalidop");
    return 1;}
  else if (fd_opcode_names[opcode_offset]==NULL) {
    u8_printf(out,"##op_%x",opcode_offset);
    return 1;}
  else {
    u8_printf(out,"##op_%s",fd_opcode_names[opcode_offset]);
    return 1;}
}

static int validate_opcode(fdtype opcode)
{
  int opcode_offset=(FD_GET_IMMEDIATE(opcode,fd_opcode_type));
  if ((opcode_offset>=0) && (opcode_offset<fd_opcode_table_len))
    return 1;
  else return 0;
}

static u8_string opcode_name(fdtype opcode)
{
  int opcode_offset=(FD_GET_IMMEDIATE(opcode,fd_opcode_type));
  if ((opcode_offset<fd_opcode_table_len) &&
      (fd_opcode_names[opcode_offset]))
    return fd_opcode_names[opcode_offset];
  else return "anonymous_opcode";
}

/* OPCODE dispatching */

enum TAILOP { any_op=0, or_op, and_op, try_op };
static fdtype eval_tail(fdtype expr,int start,enum TAILOP op,
                        u8_context cxt,fd_lispenv env);

static fdtype eval_tail(fdtype expr,int start,enum TAILOP op,
                        u8_context cxt,fd_lispenv env)
{
  if (FD_RAILP(expr)) {
    int n_elts=FD_RAIL_LENGTH(expr), n_butlast=n_elts-1;
    fdtype *rail_data=FD_RAIL_DATA(expr), final_expr=rail_data[n_butlast];
    int i=start; while (i<n_butlast) {
      fdtype each=rail_data[i++];
      fdtype each_value=op_eval(each,env,0);
      if (FD_ABORTED(each_value)) return each_value;
      else if (op==any_op) fd_decref(each_value);
      else if ((op==and_op)&&(FD_FALSEP(each_value)))
        return each_value;
      else if ((op==or_op)&&(!(FD_FALSEP(each_value))))
        return each_value;
      else if ((op==try_op)&&(!(FD_EMPTY_CHOICEP(each_value))))
        return each_value;
      else fd_decref(each_value);}
    if ((FD_PAIRP(final_expr))||(FD_RAILP(final_expr)))
      return op_eval(final_expr,env,1);
    else return op_eval(final_expr,env,1);}
  else {
    fdtype exprs=expr, next=FD_CDR(expr); int i=0;
    while ((i<start)&&(FD_PAIRP(next))) {
      exprs=next; next=FD_CDR(exprs); i++;}
    while (FD_PAIRP(next)) {
      fdtype each=FD_CAR(exprs);
      fdtype each_value=op_eval(each,env,0);
      if (FD_ABORTED(each_value)) return each_value;
      else if (op==any_op) fd_decref(each_value);
      else if ((op==and_op)&&(FD_FALSEP(each_value)))
        return each_value;
      else if ((op==or_op)&&(!(FD_FALSEP(each_value))))
        return each_value;
      else if ((op==try_op)&&(!(FD_EMPTY_CHOICEP(each_value))))
        return each_value;
      else fd_decref(each_value);
      exprs=next; next=FD_CDR(exprs);}
    if (FD_PAIRP(exprs)) {
      fdtype final=FD_CAR(exprs);
      if ((FD_PAIRP(final))||(FD_RAILP(final)))
        return op_eval(final,env,1);
      else return op_eval(final,env,1);}
    else return fd_err(fd_SyntaxError,cxt,NULL,exprs);}
}

static fdtype pickoids_opcode(fdtype arg1)
{
  if (FD_OIDP(arg1)) return arg1;
  else if (FD_EMPTY_CHOICEP(arg1)) return arg1;
  else if ((FD_CHOICEP(arg1)) || (FD_ACHOICEP(arg1))) {
    fdtype choice, results=FD_EMPTY_CHOICE;
    int free_choice=0, all_oids=1;
    if (FD_CHOICEP(arg1)) choice=arg1;
    else {choice=fd_make_simple_choice(arg1); free_choice=1;}
    {FD_DO_CHOICES(elt,choice) {
	if (FD_OIDP(elt)) {FD_ADD_TO_CHOICE(results,elt);}
	else if (all_oids) all_oids=0;}}
    if (all_oids) {
      fd_decref(results);
      if (free_choice) return choice;
      else return fd_incref(choice);}
    else if (free_choice) fd_decref(choice);
    return fd_simplify_choice(results);}
  else return FD_EMPTY_CHOICE;
}

static fdtype pickstrings_opcode(fdtype arg1)
{
  if ((FD_CHOICEP(arg1)) || (FD_ACHOICEP(arg1))) {
    fdtype choice, results=FD_EMPTY_CHOICE;
    int free_choice=0, all_strings=1;
    if (FD_CHOICEP(arg1)) choice=arg1;
    else {choice=fd_make_simple_choice(arg1); free_choice=1;}
    {FD_DO_CHOICES(elt,choice) {
	if (FD_STRINGP(elt)) {
	  fd_incref(elt); FD_ADD_TO_CHOICE(results,elt);}
	else if (all_strings) all_strings=0;}}
    if (all_strings) {
      fd_decref(results);
      if (free_choice) return choice;
      else return fd_incref(choice);}
    else if (free_choice) fd_decref(choice);
    return fd_simplify_choice(results);}
  else if (FD_STRINGP(arg1)) return fd_incref(arg1);
  else return FD_EMPTY_CHOICE;
}

static fdtype pickone_opcode(fdtype normal)
{
  int n=FD_CHOICE_SIZE(normal);
  if (n) {
    fdtype chosen;
    int i=u8_random(n);
    const fdtype *data=FD_CHOICE_DATA(normal);
    chosen=data[i]; fd_incref(chosen);
    return chosen;}
  else return FD_EMPTY_CHOICE;
}

static fdtype nd1_dispatch(fdtype opcode,fdtype arg1)
{
  switch (opcode) {
  case FD_AMBIGP_OPCODE: 
    if (FD_CHOICEP(arg1)) return FD_TRUE;
    else return FD_FALSE;
  case FD_SINGLETONP_OPCODE: 
    if (FD_EMPTY_CHOICEP(arg1)) return FD_FALSE;
    else if (FD_CHOICEP(arg1)) return FD_FALSE;
    else return FD_TRUE;
  case FD_FAILP_OPCODE: 
    if (arg1==FD_EMPTY_CHOICE) return FD_TRUE;
    else return FD_FALSE;
  case FD_EXISTSP_OPCODE: 
    if (arg1==FD_EMPTY_CHOICE) return FD_FALSE;
    else return FD_TRUE;
  case FD_SINGLETON_OPCODE:
    if (FD_CHOICEP(arg1)) return FD_EMPTY_CHOICE;
    else return fd_incref(arg1);
  case FD_CAR_OPCODE: 
    if (FD_EMPTY_CHOICEP(arg1)) return arg1;
    else if (FD_PAIRP(arg1))
      return fd_incref(FD_CAR(arg1));
    else if (FD_CHOICEP(arg1)) {
      fdtype results=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(arg,arg1)
	if (FD_PAIRP(arg)) {
	  fdtype car=FD_CAR(arg); fd_incref(car);
	  FD_ADD_TO_CHOICE(results,car);}
	else {
	  fd_decref(results);
	  return fd_type_error(_("pair"),"CAR opcode",arg);}
      return results;}
    else return fd_type_error(_("pair"),"CAR opcode",arg1);
  case FD_CDR_OPCODE: 
    if (FD_EMPTY_CHOICEP(arg1)) return arg1;
    else if (FD_PAIRP(arg1))
      return fd_incref(FD_CDR(arg1));
    else if (FD_CHOICEP(arg1)) {
      fdtype results=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(arg,arg1)
	if (FD_PAIRP(arg)) {
	  fdtype cdr=FD_CDR(arg); fd_incref(cdr);
	  FD_ADD_TO_CHOICE(results,cdr);}
	else {
	  fd_decref(results);
	  return fd_type_error(_("pair"),"CDR opcode",arg);}
      return results;}
    else return fd_type_error(_("pair"),"CDR opcode",arg1);
  case FD_LENGTH_OPCODE:
    if (arg1==FD_EMPTY_CHOICE) return FD_EMPTY_CHOICE;
    else if (FD_CHOICEP(arg1)) {
      fdtype results=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(arg,arg1) {
	if (FD_SEQUENCEP(arg)) {
	  int len=fd_seq_length(arg);
	  fdtype dlen=FD_INT(len);
	  FD_ADD_TO_CHOICE(results,dlen);}
	else {
	  fd_decref(results);
	  return fd_type_error(_("sequence"),"LENGTH opcode",arg);}}
      return fd_simplify_choice(results);}
    else if (FD_SEQUENCEP(arg1))
      return FD_INT(fd_seq_length(arg1));
    else return fd_type_error(_("sequence"),"LENGTH opcode",arg1);
  case FD_QCHOICE_OPCODE:
    if (FD_CHOICEP(arg1)) {
      fd_incref(arg1);
      return fd_init_qchoice(NULL,arg1);}
    else if (FD_ACHOICEP(arg1)) 
      return fd_init_qchoice(NULL,fd_make_simple_choice(arg1));
    else if (FD_EMPTY_CHOICEP(arg1))
      return fd_init_qchoice(NULL,FD_EMPTY_CHOICE);
    else return fd_incref(arg1);
  case FD_CHOICE_SIZE_OPCODE:
    if (FD_CHOICEP(arg1)) {
      int sz=FD_CHOICE_SIZE(arg1);
      return FD_INT(sz);}
    else if (FD_ACHOICEP(arg1)) {
      fdtype simple=fd_make_simple_choice(arg1);
      int size=FD_CHOICE_SIZE(simple);
      fd_decref(simple);
      return FD_INT(size);}
    else if (FD_EMPTY_CHOICEP(arg1))
      return FD_INT(0);
    else return FD_INT(1);
  case FD_PICKOIDS_OPCODE:
    return pickoids_opcode(arg1);
  case FD_PICKSTRINGS_OPCODE:
    return pickstrings_opcode(arg1);
  case FD_PICKONE_OPCODE:
    if (FD_CHOICEP(arg1))
      return pickone_opcode(arg1);
    else return fd_incref(arg1);
  case FD_IFEXISTS_OPCODE:
    if (FD_EMPTY_CHOICEP(arg1))
      return FD_VOID;
    else return fd_incref(arg1);
  default:
    return fd_err(_("Invalid opcode"),"opcode eval",NULL,FD_VOID);
  }
}

static fdtype first_opcode(fdtype arg1)
{
  if (FD_PAIRP(arg1)) return fd_incref(FD_CAR(arg1));
  else if (FD_VECTORP(arg1))
    if (FD_VECTOR_LENGTH(arg1)>0)
      return fd_incref(FD_VECTOR_REF(arg1,0));
    else return fd_err(fd_RangeError,"FD_FIRST_OPCODE",NULL,arg1);
  else if (FD_STRINGP(arg1)) {
    u8_string data=FD_STRDATA(arg1); int c=u8_sgetc(&data);
    if (c<0) return fd_err(fd_RangeError,"FD_FIRST_OPCODE",NULL,arg1);
    else return FD_CODE2CHAR(c);}
  else return fd_seq_elt(arg1,0);
}

static fdtype eltn_opcode(fdtype arg1,int n,u8_context opname)
{
  if (FD_VECTORP(arg1))
    if (FD_VECTOR_LENGTH(arg1)>n)
      return fd_incref(FD_VECTOR_REF(arg1,n));
    else return fd_err(fd_RangeError,opname,NULL,arg1);
  else return fd_seq_elt(arg1,n);
}

static fdtype d1_dispatch(fdtype opcode,fdtype arg1)
{
  int delta=1;
  switch (opcode) {
  case FD_MINUS1_OPCODE: delta=-1;
  case FD_PLUS1_OPCODE: 
    if (FD_FIXNUMP(arg1)) {
      int iarg=FD_FIX2INT(arg1);
      return FD_INT(iarg+delta);}
    else if (FD_NUMBERP(arg1))
      return fd_plus(arg1,FD_FIX2INT(-1));
    else return fd_type_error(_("number"),"opcode 1+/-",arg1);
  case FD_NUMBERP_OPCODE: 
    if (FD_NUMBERP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_ZEROP_OPCODE: 
    if (arg1==FD_INT(0)) return FD_TRUE; else return FD_FALSE;
  case FD_VECTORP_OPCODE: 
    if (FD_VECTORP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_PAIRP_OPCODE: 
    if (FD_PAIRP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_NULLP_OPCODE: 
    if (arg1==FD_EMPTY_LIST) return FD_TRUE; else return FD_FALSE;
  case FD_STRINGP_OPCODE: 
    if (FD_STRINGP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_OIDP_OPCODE: 
    if (FD_OIDP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_SYMBOLP_OPCODE: 
    if (FD_SYMBOLP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_FIRST_OPCODE:
    return first_opcode(arg1);
  case FD_SECOND_OPCODE:
    return eltn_opcode(arg1,1,"FD_SECOND_OPCODE");
  case FD_THIRD_OPCODE:
    return eltn_opcode(arg1,2,"FD_THIRD_OPCODE");
  case FD_TONUMBER_OPCODE:
    if (FD_FIXNUMP(arg1)) return arg1;
    else if (FD_NUMBERP(arg1)) return fd_incref(arg1);
    else if (FD_STRINGP(arg1))
      return fd_string2number(FD_STRDATA(arg1),10);
    else if (FD_CHARACTERP(arg1))
      return FD_INT(FD_CHARCODE(arg1));
    else return fd_type_error(_("number|string"),"opcode ->number",arg1);
  default:
    return fd_err(_("Invalid opcode"),"opcode eval",NULL,FD_VOID);
  }
}

static fdtype elt_opcode(fdtype arg1,fdtype arg2)
{
  if (FD_EMPTY_CHOICEP(arg1)) {
    fd_decref(arg2); return arg1;}
  else if ((FD_SEQUENCEP(arg1)) && (FD_FIXNUMP(arg2))) {
    fdtype result;
    int off=FD_FIX2INT(arg2), len=fd_seq_length(arg1);
    if (off<0) off=len+off;
    result=fd_seq_elt(arg1,off);
    if (result == FD_TYPE_ERROR)
      return fd_type_error(_("sequence"),"FD_OPCODE_ELT",arg1);
    else if (result == FD_RANGE_ERROR) {
      char buf[32];
      sprintf(buf,"%d",off);
      return fd_err(fd_RangeError,"FD_OPCODE_ELT",u8_strdup(buf),arg1);}
    else return result;}
  else if (!(FD_SEQUENCEP(arg1)))
    return fd_type_error(_("sequence"),"FD_OPCODE_ELT",arg1);
  else return fd_type_error(_("fixnum"),"FD_OPCODE_ELT",arg2);
}

static fdtype d2_dispatch(fdtype opcode,fdtype arg1,fdtype arg2)
{
  switch (opcode) {
  case FD_NUMEQ_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))==(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)==0) return FD_TRUE;
    else return FD_FALSE;
  case FD_GT_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))>(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)>0) return FD_TRUE;
    else return FD_FALSE;
  case FD_GTE_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))>=(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)>=0) return FD_TRUE;
    else return FD_FALSE;
  case FD_LT_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))<(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)<0) return FD_TRUE;
    else return FD_FALSE;
  case FD_LTE_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))<=(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)<=0) return FD_TRUE;
    else return FD_FALSE;
  case FD_EQ_OPCODE: {
    if (arg1==arg2) return FD_TRUE; else return FD_FALSE;
    break;}
  case FD_EQV_OPCODE: {
    if (arg1==arg2) return FD_TRUE;
    else if ((FD_NUMBERP(arg1)) && (FD_NUMBERP(arg2)))
      if (fd_numcompare(arg1,arg2)==0)
	return FD_TRUE; else return FD_FALSE;
    else return FD_FALSE;
    break;}
  case FD_EQUAL_OPCODE: {
    if ((FD_ATOMICP(arg1)) && (FD_ATOMICP(arg2)))
      if (arg1==arg2) return FD_TRUE; else return FD_FALSE;
    else if (FD_EQUAL(arg1,arg2)) return FD_TRUE;
    else return FD_FALSE;
    break;}
  case FD_PLUS_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2)))  {
      int m=FD_FIX2INT(arg1), n=FD_FIX2INT(arg2);
      return FD_INT(m+n);}
    else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
      double x=FD_FLONUM(arg1), y=FD_FLONUM(arg2);
      return fd_init_double(NULL,x+y);}
    else return fd_plus(arg1,arg2);
  case FD_MINUS_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2)))  {
      int m=FD_FIX2INT(arg1), n=FD_FIX2INT(arg2);
      return FD_INT(m-n);}
    else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
      double x=FD_FLONUM(arg1), y=FD_FLONUM(arg2);
      return fd_init_double(NULL,x-y);}
    else return fd_subtract(arg1,arg2);
  case FD_TIMES_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2)))  {
      int m=FD_FIX2INT(arg1), n=FD_FIX2INT(arg2);
      return FD_INT(m*n);}
    else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
      double x=FD_FLONUM(arg1), y=FD_FLONUM(arg2);
      return fd_init_double(NULL,x*y);}
    else return fd_multiply(arg1,arg2);
  case FD_FLODIV_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2)))  {
      int m=FD_FIX2INT(arg1), n=FD_FIX2INT(arg2);
      double x=(double)m, y=(double)n;
      return fd_init_double(NULL,x/y);}
    else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
      double x=FD_FLONUM(arg1), y=FD_FLONUM(arg2);
      return fd_init_double(NULL,x/y);}
    else {
      double x=fd_todouble(arg1), y=fd_todouble(arg2);
      return fd_init_double(NULL,x/y);}
  case FD_ELT_OPCODE:
    return elt_opcode(arg1,arg2);
  default:
    return fd_err(_("Invalid opcode"),"opcode eval",NULL,FD_VOID);
  }
}

static int numeric_argp(fdtype x)
{
  /* This checks if there is a type error.
     The empty choice isn't a type error since it will just
     generate an empty choice as a result. */
  if (FD_EMPTY_CHOICEP(x)) return 1;
  else if (FD_EXPECT_TRUE(FD_NUMBERP(x))) return 1;
  else if ((FD_ACHOICEP(x))||(FD_CHOICEP(x))) {
    FD_DO_CHOICES(a,x)
      if (FD_EXPECT_TRUE(FD_NUMBERP(a))) {}
      else {
	FD_STOP_DO_CHOICES;
	return 0;}
    return 1;}
  else return 0;
}

static fdtype nd2_dispatch(fdtype opcode,fdtype arg1,fdtype arg2)
{
  if (FD_EXPECT_FALSE((opcode<FD_EQ_OPCODE) && (!(numeric_argp(arg2))))) {
    fd_decref(arg1);
    return fd_type_error(_("number"),"numeric opcode",arg2);}
  else {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(a1,arg1) {
      {FD_DO_CHOICES(a2,arg2) {
          fdtype result=d2_dispatch(opcode,a1,a2);
          /* If we need to abort due to an error, we need to pop out of
             two choice loops.  So on the inside, we decref results and
             replace it with the error object.  We then break and
             do FD_STOP_DO_CHOICES (for potential cleanup). */
          if (FD_ABORTED(result)) {
            fd_decref(results); results=result;
            FD_STOP_DO_CHOICES; break;}
          else {FD_ADD_TO_CHOICE(results,result);}}}
      /* If the inner loop aborted due to an error, results is now bound
         to the error, so we just FD_STOP_DO_CHOICES (this time for the
         outer loop) and break; */
      if (FD_ABORTED(results)) {
        FD_STOP_DO_CHOICES; break;}}
    fd_decref(arg1); fd_decref(arg2);
    return results;}
}

static fdtype xref_opcode(fdtype x,int i,fdtype tag)
{
  struct FD_COMPOUND *c=(fd_compound)x;
  if ((FD_VOIDP(tag)) || ((c->compound_typetag)==tag))
    if (i<c->fd_n_elts) {
      fdtype *values=&(c->compound_0), value;
      if (c->compound_ismutable) 
        u8_lock_mutex(&(c->compound_lock));
      value=values[i];
      fd_incref(value);
      if (c->compound_ismutable)
        u8_unlock_mutex(&(c->compound_lock));
      return value;}
    else {
      fd_seterr(fd_RangeError,"xref",NULL,x);
      return FD_ERROR_VALUE;}
  else return fd_err(fd_TypeError,"xref",fd_dtype2string(tag),x);
}

static fdtype opcode_dispatch(fdtype opcode,fdtype expr,fd_lispenv env)
{
  if (!(FD_EXPECT_FALSE(FD_PAIRP(FD_CDR(expr))))) {
    /* Otherwise, we should have at least one argument,
       return an error otherwise. */
    return fd_err(fd_SyntaxError,"opcode eval",NULL,expr);}
  else {
    /* We have at least one argument to evaluate and we also get the body. */
    fdtype remaining=fd_get_body(expr,2);
    fdtype arg1_expr=fd_get_arg(expr,1), arg1;
    if (FD_EXPECT_FALSE((opcode<FD_MAX_UNARY_OPCODE)&&
                        (!(FD_EMPTY_LISTP(remaining)))))
      return fd_err(fd_TooManyArgs,opcode_name(opcode),NULL,expr);
    else arg1=op_eval(arg1_expr,env,0);
    /* Now, check the result of the first argument expression */
    if (FD_ABORTED(arg1)) return arg1;
    else if (FD_VOIDP(arg1))
      return fd_err(fd_VoidArgument,"opcode eval",NULL,arg1_expr);
    else if (FD_ACHOICEP(arg1))
      arg1=fd_simplify_choice(arg1);
    else {}
    if (opcode<FD_MAX_ND1_OPCODE)
      return nd1_dispatch(opcode,arg1);
    else if (opcode<FD_MAX_UNARY_OPCODE) {
      if (FD_EMPTY_CHOICEP(arg1))
        return FD_EMPTY_CHOICE;
      else if (!(FD_CHOICEP(arg1))) {
        fdtype result=d1_dispatch(opcode,arg1);
        fd_decref(arg1);
        return result;}
      else {
        fdtype results=FD_EMPTY_CHOICE;
        FD_DO_CHOICES(arg,arg1) {
          fdtype result=d1_dispatch(opcode,arg);
          if (FD_ABORTED(result)) {
            fd_decref(results); fd_decref(arg1);
            FD_STOP_DO_CHOICES;
            return result;}
          else {FD_ADD_TO_CHOICE(results,result);}}
        fd_decref(arg1);
        return results;}}
    /* Check the type for numeric arguments here. */
    else if ((opcode>=FD_NUM2_OPCODES) && (opcode<FD_MAX_NUM2_OPCODES)) {
      if (FD_EXPECT_FALSE(FD_EMPTY_CHOICEP(arg1)))
        return FD_EMPTY_CHOICE;
      else if (FD_EXPECT_FALSE(!(numeric_argp(arg1)))) {
        fdtype result=fd_type_error(_("number"),"numeric opcode",arg1);
        fd_decref(arg1);
        return result;}
      else {
        fdtype arg2_expr=fd_get_arg(expr,2), arg2;
        if (FD_VOIDP(arg2_expr)) {
          fd_decref(arg1);
          return fd_err(fd_TooFewArgs,opcode_name(opcode),NULL,expr);}
        else arg2=op_eval(arg2_expr,env,0);
        if (FD_ACHOICEP(arg2)) arg2=fd_simplify_choice(arg2);
        if (FD_ABORTP(arg2)) {
          fd_decref(arg1);
          return arg2;}
        else if (FD_EMPTY_CHOICEP(arg2)) {
          fd_decref(arg1);
          return FD_EMPTY_CHOICE;}
        else if ((FD_CHOICEP(arg1))||(FD_CHOICEP(arg2)))
          return nd2_dispatch(opcode,arg1,arg2);
        else return d2_dispatch(opcode,arg1,arg2);}}
    else if (opcode<FD_MAX_BINARY_OPCODE) {
      if (FD_EMPTY_CHOICEP(arg1)) return FD_EMPTY_CHOICE;
      else {
        fdtype arg2_expr=fd_get_arg(expr,2), arg2;
        if (FD_VOIDP(arg2_expr)) {
          fd_decref(arg1);
          return fd_err(fd_TooFewArgs,opcode_name(opcode),NULL,expr);}
        else arg2=op_eval(arg2_expr,env,0);
        if (FD_ACHOICEP(arg2)) arg2=fd_simplify_choice(arg2);
        if (FD_ABORTED(arg2)) {
          fd_decref(arg1); return arg2;}
        else if (FD_VOIDP(arg2)) {
          fd_decref(arg1);
          return fd_err(fd_VoidArgument,"opcode eval",NULL,arg2_expr);}
        else if (FD_EMPTY_CHOICEP(arg2)) {
          /* Prune the call */
          fd_decref(arg1); return arg2;}
        else if ((FD_CHOICEP(arg1)) || (FD_CHOICEP(arg2)))
          /* nd2_dispatch handles decref of arg1 and arg2 */
          return nd2_dispatch(opcode,arg1,arg2);
        else {
          /* This is the dispatch case where we just go to dispatch. */
          fdtype result=d2_dispatch(opcode,arg1,arg2);
          fd_decref(arg1); fd_decref(arg2);
          return result;}}}
    else if ((opcode==FD_GET_OPCODE) || (opcode==FD_PGET_OPCODE)) {
      if (FD_EMPTY_CHOICEP(arg1)) return FD_EMPTY_CHOICE;
      else {
        fdtype slotid_arg=fd_get_arg(expr,2);
        fdtype slotids=op_eval(slotid_arg,env,0), result;
        if (FD_ABORTED(slotids)) return slotids;
        else if (FD_VOIDP(slotids))
          return fd_err(fd_SyntaxError,"OPCODE fget",NULL,expr);
        else slotids=fd_simplify_choice(slotids);
        if (opcode==FD_GET_OPCODE)
          result=fd_fget(arg1,slotids);
        else {
          fdtype dflt_arg=fd_get_arg(expr,3);
          if (FD_VOIDP(dflt_arg))
            result=fd_get(arg1,slotids,FD_EMPTY_CHOICE);
          else {
            fdtype dflt=op_eval(dflt_arg,env,0);
            if (FD_ABORTED(dflt)) result=dflt;
            else if (FD_VOIDP(dflt)) {
              result=fd_err(fd_VoidArgument,"OPCODE pget",NULL,
                            fd_get_arg(expr,3));}
            else result=fd_get(arg1,slotids,dflt);
            fd_decref(dflt);}}
        fd_decref(arg1); fd_decref(slotids);
        return result;}}
    else if ((opcode==FD_TEST_OPCODE) || (opcode==FD_PTEST_OPCODE)) {
      if (FD_EMPTY_CHOICEP(arg1)) return FD_FALSE;
      else {
        fdtype slotid_arg=fd_get_arg(expr,2);
        fdtype values_arg=fd_get_arg(expr,3);
        fdtype slotids=op_eval(slotid_arg,env,0), values, result;
        if (FD_ABORTED(slotids)) return slotids;
        else if (FD_VOIDP(slotids))
          return fd_err(fd_SyntaxError,"OPCODE ftest",NULL,expr);
        else if (FD_EMPTY_CHOICEP(slotids)) {
          fd_decref(arg1); return FD_FALSE;}
        else if (FD_ACHOICEP(slotids))
          slotids=fd_simplify_choice(slotids);
        else {}
        if (FD_VOIDP(values_arg))
          values=FD_VOID;
        else values=op_eval(values_arg,env,0);
        if (FD_ABORTED(values)) {
          fd_decref(arg1); fd_decref(slotids);
          return values;}
        if (opcode==FD_TEST_OPCODE)
          result=fd_ftest(arg1,slotids,values);
        else if (fd_test(arg1,slotids,values))
          result=FD_TRUE;
        else result=FD_FALSE;
        fd_decref(arg1); fd_decref(slotids); fd_decref(values);
        return result;}}
    else if ((opcode>=FD_SETOPS_OPCODES) &&
             (opcode<=FD_DIFFERENCE_OPCODE)) {
      fdtype arg2expr=fd_get_arg(expr,2), argv[2];
      fdtype arg2=op_eval(arg2expr,env,0);
      fdtype result=FD_ERROR_VALUE;
      if (FD_ACHOICEP(arg2)) arg2=fd_simplify_choice(arg2);
      argv[0]=arg1; argv[1]=arg2;
      if (FD_ABORTED(arg2)) result=arg2;
      else if (FD_VOIDP(arg2)) {
        result=fd_err(fd_VoidArgument,"OPCODE setop",NULL,arg2expr);}
      else switch (opcode) {
        case FD_IDENTICAL_OPCODE:
          if (arg1==arg2) result=FD_TRUE;
          else if (FD_EQUAL(arg1,arg2)) result=FD_TRUE;
          else result=FD_FALSE;
          break;
        case FD_OVERLAPS_OPCODE:
          if (arg1==arg2) result=FD_TRUE;
          else if (fd_overlapp(arg1,arg2)) result=FD_TRUE;
          else result=FD_FALSE;
          break;
        case FD_CONTAINSP_OPCODE:
          if (fd_containsp(arg1,arg2)) result=FD_TRUE;
          else result=FD_FALSE;
          break;
        case FD_INTERSECT_OPCODE:
          if ((FD_EMPTY_CHOICEP(arg1)) || (FD_EMPTY_CHOICEP(arg2)))
            result=FD_EMPTY_CHOICE;
          else result=fd_intersection(argv,2);
          break;
        case FD_UNION_OPCODE:
          if (FD_EMPTY_CHOICEP(arg1)) result=fd_incref(arg2);
          else if (FD_EMPTY_CHOICEP(arg2)) result=fd_incref(arg1);
          else result=fd_union(argv,2);
          break;
        case FD_DIFFERENCE_OPCODE:
          if ((FD_EMPTY_CHOICEP(arg1)) || (FD_EMPTY_CHOICEP(arg2)))
            result=fd_incref(arg1);
          else result=fd_difference(arg1,arg2);
          break;}
      fd_decref(arg1); fd_decref(arg2);
      return result;}
    else if (opcode==FD_XREF_OPCODE) {
      if (FD_EMPTY_CHOICEP(arg1))
        return FD_EMPTY_CHOICE;
      else {
        fdtype offset_arg=fd_get_arg(expr,2);
        fdtype type_arg=fd_get_arg(expr,3);
        if ((FD_PAIRP(type_arg)) &&
            (FD_EQ(FD_CAR(type_arg),quote_symbol)) &&
            (FD_PAIRP(FD_CDR(type_arg))))
          type_arg=FD_CAR(FD_CDR(type_arg));
        else if ((FD_RAILP(type_arg))&&
                 (FD_RAIL_LENGTH(type_arg)>1)&&
                 (FD_EQ(FD_RAIL_REF(type_arg,0),quote_symbol)))
          type_arg=fd_get_arg(type_arg,1);
        else {}
        if (FD_CHOICEP(arg1)) {
          fdtype results=FD_EMPTY_CHOICE;
          FD_DO_CHOICES(a1,arg1)
            if (FD_COMPOUNDP(a1)) {
              fdtype result=xref_opcode(a1,FD_FIX2INT(offset_arg),type_arg);
              if (FD_ABORTED(result)) {
                fd_decref(arg1);
                fd_decref(results);
                return result;}
              else {FD_ADD_TO_CHOICE(results,result);}}
            else {
              fdtype result=
                fd_err(fd_TypeError,"xref",
                       ((FD_VOIDP(type_arg)) ?
                        ((u8_string)u8_strdup("compound")) :
                        (fd_dtype2string(type_arg))),
                       a1);
              fd_decref(results);
              fd_decref(arg1);
              return result;}
          fd_decref(arg1);
          return results;}
        else if (FD_COMPOUNDP(arg1)) {
          fdtype result=xref_opcode(arg1,FD_FIX2INT(offset_arg),type_arg);
          fd_decref(arg1);
          return result;}
        else {
          fdtype result=fd_err(fd_TypeError,"xref",
                               ((FD_VOIDP(type_arg)) ?
                                ((u8_string)u8_strdup("compound")) :
                                (fd_dtype2string(type_arg))),
                               arg1);
          fd_decref(arg1);
          return result;}}}
    else {
      fd_decref(arg1);
      return fd_err(fd_SyntaxError,"opcode eval",NULL,expr);}
  }
}

static void unwrap_qchoices(fdtype *args,int n)
{
  int j=0; while (j<n) {
    fdtype v=args[j];
    if (FD_EMPTY_QCHOICEP(v)) {
      args[j++]=FD_EMPTY_CHOICE;}
    else if (FD_QCHOICEP(v)) {
      struct FD_QCHOICE *qc=(fd_qchoice)v;
      fd_incref(qc->fd_choiceval);
      args[j++]=qc->fd_choiceval;
      fd_decref(v);}
    else j++;}
}

FD_FASTOP fdtype op_eval(fdtype x,fd_lispenv env,int tail)
{
  switch (FD_PTR_MANIFEST_TYPE(x)) {
  case fd_oid_ptr_type: case fd_fixnum_ptr_type:
    return x;
  case fd_immediate_ptr_type:
    if (FD_TYPEP(x,fd_lexref_type))
      return fd_lexref(x,env);
    else if (FD_SYMBOLP(x)) {
      fdtype val=fd_symeval(x,env);
      if (FD_EXPECT_FALSE(FD_VOIDP(val)))
        return fd_err(fd_UnboundIdentifier,"op_eval",FD_SYMBOL_NAME(x),x);
      else return val;}
    else return x;
  case fd_cons_ptr_type: {
    fd_ptr_type cons_type=FD_PTR_TYPE(x);
    switch (cons_type) {
    case fd_rail_type: {
      fdtype head=FD_RAIL_REF(x,0), n=FD_RAIL_LENGTH(x);
      fd_ptr_type head_type=FD_PTR_TYPE(head);
      if (head_type==fd_opcode_type) {
        if (tail)
          return opcode_dispatch(head,x,env);
        else {
          fdtype v=opcode_dispatch(head,x,env);
          return fd_finish_call(v);}}
      else if (n>7)
        if (tail)
          return fd_tail_eval(x,env);
        else return fd_eval(x,env);
      else if ((head_type==fd_primfcn_type)||
               (head_type==fd_sproc_type)) {
        fdtype args[7], result=FD_VOID;
        struct FD_FUNCTION *fn=(struct FD_FUNCTION *)head;
        int rail_i=1, arg_i=0, nd=0, qchoices=0, ndcall=fn->fcn_ndcall;
        while (rail_i<n) {
          fdtype e=FD_RAIL_REF(x,rail_i), v=op_eval(e,env,0); 
          if (FD_ABORTP(v)) {
            arg_i--; while (arg_i>=0) {
              fd_decref(args[arg_i]); arg_i--;}
            return v;}
          else if ((!(ndcall))&&(FD_EMPTY_CHOICEP(v))) {
            arg_i--; while (arg_i>=0) {
              fd_decref(args[arg_i]); arg_i--;}
            return v;}
          if ((FD_CHOICEP(v))||(FD_ACHOICEP(v))) nd++;
          if ((FD_EMPTY_QCHOICEP(v))||(FD_QCHOICEP(v)))
            qchoices++;
          args[arg_i++]=v;
          rail_i++;}
        /* if (tail) return fd_tail_call(head,n,args); */
        if ((nd)&&(!(fn->fcn_ndcall))&&(fn->fcn_arity>=0))
          result=FD_EMPTY_CHOICE;
        else if (fn->fcn_ndcall) {
          if (qchoices) unwrap_qchoices(args,arg_i);
          if (head_type==fd_sproc_type)
            result=fd_apply_sproc((struct FD_SPROC *)fn,arg_i,args);
          else result=fd_dapply(head,n-1,args);}
        else {
          if (qchoices) unwrap_qchoices(args,arg_i);
          if (head_type==fd_sproc_type)
            result=fd_apply_sproc((struct FD_SPROC *)fn,arg_i,args);
          else result=fd_dapply(head,n-1,args);}
        arg_i--; while (arg_i>=0) {fd_decref(args[arg_i]); arg_i--;}
        if (FD_TYPEP(result,fd_tailcall_type))
          result=fd_finish_call(result);
        return result;}
      else return fd_eval(x,env);}
    case fd_pair_type:
      if (FD_TYPEP(FD_CAR(x),fd_opcode_type)) {
        if (tail)
          return opcode_dispatch(FD_CAR(x),x,env);
        else {
          fdtype v=opcode_dispatch(FD_CAR(x),x,env);
          return fd_finish_call(v);}}
      else if (tail)
        return fd_tail_eval(x,env);
      else return fd_eval(x,env);
    case fd_choice_type: case fd_achoice_type:
      return fd_eval(x,env);
    case fd_slotmap_type:
      return fd_deep_copy(x);
    default:
      return fd_incref(x);}}
  default: /* Never reached */
    return x;
  }
}

static void init_opcode_names()
{
  fd_opcode_names[0x20]="AMBIGUOUS?";
  fd_opcode_names[0x21]="SINGLETON?";
  fd_opcode_names[0x22]="FAIL?";
  fd_opcode_names[0x22]="EMPTY?";
  fd_opcode_names[0x23]="EXISTS?";
  fd_opcode_names[0x24]="SINGLETON";
  fd_opcode_names[0x25]="CAR";
  fd_opcode_names[0x26]="CDR";
  fd_opcode_names[0x27]="LENGTH";
  fd_opcode_names[0x28]="QCHOICE";
  fd_opcode_names[0x29]="CHOICE-SIZE";
  fd_opcode_names[0x2A]="PICKOIDS";
  fd_opcode_names[0x2B]="PICKSTRINGS";
  fd_opcode_names[0x2C]="PICK-ONE";
  fd_opcode_names[0x2D]="IFEXISTS";
  fd_opcode_names[0x40]="1-";
  fd_opcode_names[0x40]="-1+";
  fd_opcode_names[0x41]="1+";
  fd_opcode_names[0x42]="NUMBER?";
  fd_opcode_names[0x43]="ZERO?";
  fd_opcode_names[0x44]="VECTOR?";
  fd_opcode_names[0x45]="PAIR?";
  fd_opcode_names[0x46]="NULL?";
  fd_opcode_names[0x47]="STRING?";
  fd_opcode_names[0x48]="OID?";
  fd_opcode_names[0x49]="SYMBOL?";
  fd_opcode_names[0x4A]="FIRST";
  fd_opcode_names[0x4B]="SECOND";
  fd_opcode_names[0x4C]="THIRD";
  fd_opcode_names[0x4D]="->NUMBER";
  fd_opcode_names[0x60]="=";
  fd_opcode_names[0x61]=">";
  fd_opcode_names[0x62]=">=";
  fd_opcode_names[0x63]="<";
  fd_opcode_names[0x64]="<=";
  fd_opcode_names[0x65]="+";
  fd_opcode_names[0x66]="-";
  fd_opcode_names[0x67]="*";
  fd_opcode_names[0x68]="/~";
  fd_opcode_names[0x80]="EQ?";
  fd_opcode_names[0x81]="EQV?";
  fd_opcode_names[0x82]="EQUAL?";
  fd_opcode_names[0x83]="ELT";
  fd_opcode_names[0xA0]="GET";
  fd_opcode_names[0xA1]="TEST";
  fd_opcode_names[0xA2]="XREF";
  fd_opcode_names[0xA3]="%GET";
  fd_opcode_names[0xA4]="%TEST";
  fd_opcode_names[0xC0]="IDENTICAL?";
  fd_opcode_names[0xC1]="OVERLAPS?";
  fd_opcode_names[0xC2]="CONTAINS?";
  fd_opcode_names[0xC3]="UNION";
  fd_opcode_names[0xC4]="INTERSECTION";
  fd_opcode_names[0xC5]="DIFFERENCE";
}

static double opcodes_initialized=0;

void fd_init_opcodes_c()
{
  if (opcodes_initialized) return;
  else opcodes_initialized=1;
  memset(fd_opcode_names,0,sizeof(fd_opcode_names));
  init_opcode_names();
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
