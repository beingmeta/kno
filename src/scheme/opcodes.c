#include "framerd/dbprims.h"

/* Opcode names */

const u8_string fd_opcode_names[256]={
  /* 0x00 */
  "quote","begin","and","or","not","fail",
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x10 */
  "if","when","unless","ifelse",
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x20 */
  "ambigp","singeltonp","failp","existsp",
  "singleton","car","cdr","length","qchoice","choicesize",
  "pickoids","pickstrings","pickone",NULL,NULL,NULL,
  /* 0x30 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x40 */
  "minus1","plus1","numberp","zerop",
  "vectorp","pairp","emptylistp","stringp",
  "oidp","symbolp","first","second","third","tonumber",NULL,NULL,
  /* 0x50 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x60 */
  "numeq","numgt","numgte","numle","numlte",
  "plus","subtract","multiple","flodiv",
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x70 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x80 */
  "eq","eqv","equal","elt",NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x90 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0xA0 */
  "get","test","xref",NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0xB0 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0xC0 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0xD0 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0xE0 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0xF0 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
};

int fd_opcode_table_len=256;

static int unparse_opcode(u8_output out,fdtype x)
{
  int opcode_offset=(FD_GET_IMMEDIATE(x,fd_opcode_type));
  if (opcode_offset>fd_opcode_table_len) {
    u8_printf(out,"##invalidop");
    return 1;}
  else if (fd_opcode_names[opcode_offset]==NULL) {
    u8_printf(out,"##unknownop");
    return 1;}
  else {
    u8_printf(out,"##op_%s",fd_opcode_names[opcode_offset]);
    return 1;}
}

static int validate_opcode(fdtype x)
{
  int opcode_offset=(FD_GET_IMMEDIATE(x,fd_opcode_type));
  if ((opcode_offset>=0) && (opcode_offset<fd_opcode_table_len) &&
      (fd_opcode_names[opcode_offset] != NULL))
    return 1;
  else return 0;
}

/* OPCODE dispatching */

static fdtype opcode_special_dispatch(fdtype opcode,fdtype expr,fd_lispenv env)
{
  fdtype body=FD_CDR(expr), remainder;
  if (opcode>=FD_IF_OPCODE) {
    /* It's a conditional opcode */
    fdtype test=FD_VOID;
    if (FD_EXPECT_FALSE(!(FD_PAIRP(body))))
      return fd_err(fd_SyntaxError,"OPCODE conditional",NULL,expr);
    test=fd_eval(FD_CAR(body),env); remainder=FD_CDR(body);
    if (FD_ABORTP(test)) return test;
    else if (FD_VOIDP(test))
      return fd_err(fd_VoidArgument,"conditional OPCODE",NULL,FD_CAR(body));
    else switch (opcode) {
    case FD_IF_OPCODE: {
      fdtype consequent_expr=FD_VOID;
      if (FD_EMPTY_CHOICEP(test))
	return FD_EMPTY_CHOICE;
      else if (FD_FALSEP(test)) {
	fdtype if_tail=FD_CDR(remainder);
	if (FD_PAIRP(if_tail))
	  consequent_expr=FD_CAR(if_tail);
	else consequent_expr=FD_VOID;}
      else consequent_expr=FD_CAR(remainder);
      fd_decref(test);
      if (FD_PAIRP(consequent_expr))
	return fd_tail_eval(consequent_expr,env);
      else return fasteval(consequent_expr,env);}
    case FD_WHEN_OPCODE: {
      if (!(FD_FALSEP(test))) {
	fdtype test_body=remainder;
	while (FD_PAIRP(test_body)) {
	  fdtype tmp=fd_eval(FD_CAR(test_body),env);
	  if (FD_ABORTP(tmp)) return tmp;
	  fd_decref(tmp);
	  test_body=FD_CDR(test_body);}
	return FD_VOID;}
      fd_decref(test);
      return FD_VOID;}
    case FD_UNLESS_OPCODE: {
      if (FD_FALSEP(test)) {
	fdtype test_body=remainder;
	while (FD_PAIRP(test_body)) {
	  fdtype tmp=fd_eval(FD_CAR(test_body),env);
	  if (FD_ABORTP(tmp)) return tmp;
	  fd_decref(tmp);
	  test_body=FD_CDR(test_body);}
	return FD_VOID;}
      fd_decref(test);
      return FD_VOID;}
    case FD_IFELSE_OPCODE: {
      if (FD_EMPTY_CHOICEP(test))
	return FD_EMPTY_CHOICE;
      else if (FD_FALSEP(test)) {
	fdtype if_tail=FD_CDR(remainder);
	/* Done with this: */
	fd_decref(test);
	/* Execute the else expressions */
	while (FD_PAIRP(if_tail)) {
	  fdtype else_expr=FD_CAR(if_tail);
	  if (FD_PAIRP(FD_CDR(if_tail))) {
	    fdtype result=fd_eval(else_expr,env);
	    fd_decref(result);
	    if_tail=FD_CDR(if_tail);}
	  else if (FD_PAIRP(else_expr))
	    return fd_tail_eval(else_expr,env);
	  else return fasteval(else_expr,env);}
	return FD_VOID;}
      else {
	fdtype consequent_expr=FD_CAR(remainder);
	fd_decref(test);
	if (FD_PAIRP(consequent_expr))
	  return fd_tail_eval(consequent_expr,env);
	else return fasteval(consequent_expr,env);}}
    default:
      return fd_err(_("Invalid opcode"),"opcode eval",NULL,body);
    }
  }
  else switch (opcode) {
    case FD_QUOTE_OPCODE: {
      if ((FD_PAIRP(body)) && (FD_EMPTY_LISTP(FD_CDR(body))))
        return fd_incref(FD_CAR(body));
      else return fd_err(fd_SyntaxError,"opcode QUOTE",NULL,expr);}
    case FD_BEGIN_OPCODE: {
      fdtype retval=FD_VOID;
      while (FD_PAIRP(body)) {
        fdtype each=FD_CAR(body); body=FD_CDR(body);
        fd_decref(retval);
        if (FD_PAIRP(body)) {
          retval=fd_eval(each,env);
          if (FD_ABORTP(retval)) return retval;}
        else if (FD_PAIRP(each))
          return fd_tail_eval(each,env);
        else return fasteval(each,env);}
      return retval;}
    case FD_AND_OPCODE: {
      fdtype retval=FD_TRUE;
      if (FD_EMPTY_LISTP(body)) return FD_TRUE;
      else if (!(FD_PAIRP(body)))
        return fd_err(fd_SyntaxError,"AND opcode",NULL,expr);
      else while (FD_PAIRP(body)) {
          fdtype each=FD_CAR(body); body=FD_CDR(body);
          fd_decref(retval);
          if (FD_PAIRP(body)) {
            retval=fd_eval(each,env);
            if (FD_ABORTP(retval)) return retval;
            else if (FD_VOIDP(retval))
              return fd_err(fd_VoidArgument,"AND opcode",NULL,each);
            else if (FD_FALSEP(retval))
              return FD_FALSE;}
          else if (FD_PAIRP(each))
            return fd_tail_eval(each,env);
          else return fasteval(each,env);}
      return retval;}
    case FD_OR_OPCODE: {
      fdtype retval=FD_FALSE;
      if (FD_EMPTY_LISTP(body)) return FD_FALSE;
      else if (!(FD_PAIRP(body)))
        return fd_err(fd_SyntaxError,"OR opcode",NULL,expr);
      else while (FD_PAIRP(body)) {
          fdtype each=FD_CAR(body); body=FD_CDR(body);
          fd_decref(retval);
          if (FD_PAIRP(body)) {
            retval=fd_eval(each,env);
            if (FD_ABORTP(retval)) return retval;
            else if (FD_VOIDP(retval))
              return fd_err(fd_VoidArgument,"OR opcode",NULL,each);
            else if (!(FD_FALSEP(retval))) return retval;}
          else if (FD_PAIRP(each))
            return fd_tail_eval(each,env);
          else return fasteval(each,env);}
      return FD_FALSE;}
    case FD_NOT_OPCODE: {
      if (!(FD_PAIRP(body)))
        return fd_err(fd_SyntaxError,"AND opcode",NULL,expr);
      else {
        fdtype arg1_expr=FD_CAR(body);
        fdtype arg1=fd_eval(arg1_expr,env);
        if (FD_VOIDP(arg1))
          return fd_err(fd_VoidArgument,"AND opcode",NULL,arg1_expr);
        else if (!(FD_FALSEP(arg1))) {
          fd_decref(arg1); return FD_FALSE;}
        else return FD_TRUE;}}
    case FD_FAIL_OPCODE: 
      if (FD_PAIRP(body))
        return fd_err(fd_SyntaxError,"AND opcode",NULL,expr);
      else return FD_EMPTY_CHOICE;
    case FD_MODREF_OPCODE: {
      fdtype module=fd_get_arg(expr,1);
      fdtype symbol=fd_get_arg(expr,2);
      if (FD_EXPECT_FALSE((FD_VOIDP(module))||(FD_VOIDP(symbol))))
        return fd_err(fd_SyntaxError,"MODREF opcode",NULL,expr);
      else if (FD_EXPECT_FALSE(!(FD_HASHTABLEP(module))))
        return fd_type_error("module exports(hashtable)",
                             "MODREF opcode",module);
      else if (FD_EXPECT_FALSE((!(FD_SYMBOLP(symbol)))))
        return fd_type_error("module exports(hashtable)",
                             "MODREF opcode",module);
      else return fd_hashtable_get
             (FD_STRIP_CONS(module,fd_hashtable_type,fd_hashtable),
              symbol,FD_UNBOUND);}
    default:
      return fd_err(_("Invalid opcode"),"opcode eval",NULL,body);
    }
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

static fdtype opcode_unary_nd_dispatch(fdtype opcode,fdtype arg1)
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
	  fdtype dlen=FD_INT2DTYPE(len);
	  FD_ADD_TO_CHOICE(results,dlen);}
	else {
	  fd_decref(results);
	  return fd_type_error(_("sequence"),"LENGTH opcode",arg);}}
      return fd_simplify_choice(results);}
    else if (FD_SEQUENCEP(arg1))
      return FD_INT2DTYPE(fd_seq_length(arg1));
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
      return FD_INT2DTYPE(sz);}
    else if (FD_ACHOICEP(arg1)) {
      fdtype simple=fd_make_simple_choice(arg1);
      int size=FD_CHOICE_SIZE(simple);
      fd_decref(simple);
      return FD_INT2DTYPE(size);}
    else if (FD_EMPTY_CHOICEP(arg1))
      return FD_INT2DTYPE(0);
    else return FD_INT2DTYPE(1);
  case FD_PICKOIDS_OPCODE:
    return pickoids_opcode(arg1);
  case FD_PICKSTRINGS_OPCODE:
    return pickstrings_opcode(arg1);
  case FD_PICKONE_OPCODE:
    if (FD_CHOICEP(arg1))
      return pickone_opcode(arg1);
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

static fdtype opcode_unary_dispatch(fdtype opcode,fdtype arg1)
{
  int delta=1;
  switch (opcode) {
  case FD_MINUS1_OPCODE: delta=-1;
  case FD_PLUS1_OPCODE: 
    if (FD_FIXNUMP(arg1)) {
      int iarg=FD_FIX2INT(arg1);
      return FD_INT2DTYPE(iarg+delta);}
    else if (FD_NUMBERP(arg1))
      return fd_plus(arg1,FD_FIX2INT(-1));
    else return fd_type_error(_("number"),"opcode 1+/-",arg1);
  case FD_NUMBERP_OPCODE: 
    if (FD_NUMBERP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_ZEROP_OPCODE: 
    if (arg1==FD_INT2DTYPE(0)) return FD_TRUE; else return FD_FALSE;
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
      return FD_INT2DTYPE(FD_CHARCODE(arg1));
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

static fdtype opcode_binary_dispatch(fdtype opcode,fdtype arg1,fdtype arg2)
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
      return FD_INT2DTYPE(m+n);}
    else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
      double x=FD_FLONUM(arg1), y=FD_FLONUM(arg2);
      return fd_init_double(NULL,x+y);}
    else return fd_plus(arg1,arg2);
  case FD_MINUS_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2)))  {
      int m=FD_FIX2INT(arg1), n=FD_FIX2INT(arg2);
      return FD_INT2DTYPE(m-n);}
    else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
      double x=FD_FLONUM(arg1), y=FD_FLONUM(arg2);
      return fd_init_double(NULL,x-y);}
    else return fd_subtract(arg1,arg2);
  case FD_TIMES_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2)))  {
      int m=FD_FIX2INT(arg1), n=FD_FIX2INT(arg2);
      return FD_INT2DTYPE(m*n);}
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

#if 0
static fdtype opcode_other_dispatch
  (fdtype opcode,fdtype arg1,fdtype body,fd_lispenv env)
{
  /* Wait with implementing these */
  return FD_VOID;
}
#endif

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

static fdtype opcode_binary_nd_dispatch(fdtype opcode,fdtype arg1,fdtype arg2)
{
  if (FD_EXPECT_FALSE((opcode<FD_EQ_OPCODE) && (!(numeric_argp(arg2))))) {
    fd_decref(arg1);
    return fd_type_error(_("number"),"numeric opcode",arg2);}
  else {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(a1,arg1) {
      {FD_DO_CHOICES(a2,arg2) {
	  fdtype result=opcode_binary_dispatch(opcode,a1,a2);
	  /* If we need to abort due to an error, we need to pop out of
	     two choice loops.  So on the inside, we decref results and
	     replace it with the error object.  We then break and
	     do FD_STOP_DO_CHOICES (for potential cleanup). */
	  if (FD_ABORTP(result)) {
	    fd_decref(results); results=result;
	    FD_STOP_DO_CHOICES; break;}}}
      /* If the inner loop aborted due to an error, results is now bound
	 to the error, so we just FD_STOP_DO_CHOICES (this time for the
	 outer loop) and break; */
      if (FD_ABORTP(results)) {
	FD_STOP_DO_CHOICES; break;}}
    fd_decref(arg1); fd_decref(arg2);
    return results;}
}

static fdtype xref_opcode(fdtype x,int i,fdtype tag)
{
  struct FD_COMPOUND *c=(fd_compound)x;
  if ((FD_VOIDP(tag)) || ((c->tag)==tag))
    if (i<c->n_elts) {
      fdtype *values=&(c->elt0), value;
      if (c->mutable) fd_lock_struct(c);
      value=values[i];
      fd_incref(value);
      if (c->mutable) fd_unlock_struct(c);
      return value;}
    else {
      fd_seterr(fd_RangeError,"xref",NULL,x);
      return FD_ERROR_VALUE;}
  else return fd_err(fd_TypeError,"xref",fd_dtype2string(tag),x);
}

static fdtype opcode_dispatch(fdtype opcode,fdtype expr,fd_lispenv env)
{
  fdtype body=FD_CDR(expr), arg1_expr, arg1;
  if (opcode<FD_UNARY_ND_OPCODES)
    /* These handle the raw expression without evaluation */
    return opcode_special_dispatch(opcode,expr,env);
  else if (FD_EXPECT_FALSE(!(FD_PAIRP(body))))
    /* Otherwise, we should have at least one argument,
       return an error otherwise. */
    return fd_err(fd_SyntaxError,"opcode eval",NULL,expr);
  /* We have at least one argument to evaluate and we also
     "advance" the body. */
  arg1_expr=FD_CAR(body);
  arg1=fd_eval(arg1_expr,env);
  body=FD_CDR(body);
  /* Now, check the result of the first argument expression */
  if (FD_ABORTP(arg1)) return arg1;
  else if (FD_VOIDP(arg1))
    return fd_err(fd_VoidArgument,"opcode eval",NULL,arg1_expr);
  /* Check the type for numeric arguments here. */
  else if (FD_EXPECT_FALSE
	   (((opcode>=FD_NUMERIC2_OPCODES) &&
	     (opcode<FD_BINARY_OPCODES)) &&
	    (!(numeric_argp(arg1))))) {
    fdtype result=fd_type_error(_("number"),"numeric opcode",arg1);
    fd_decref(arg1);
    return result;}
  else if (FD_EXPECT_FALSE
	   (((opcode>=FD_NUMERIC2_OPCODES) &&
	     (opcode<FD_BINARY_OPCODES)) &&
	    (FD_EMPTY_CHOICEP(arg1))))
    return arg1;
  else if (FD_EMPTY_LISTP(body)) /* Unary call */
    /* Now we know that there is only one argument, which means you're either
       dispatching without doing ND iteration or with doing it.  */
    if (opcode<FD_UNARY_OPCODES)
      /* This means you don't have to bother iterating over choices. */
      if (FD_EXPECT_FALSE(FD_ACHOICEP(arg1))) {
	/* We handle choice normalization here so that the
	   inner code can be faster. */
	fdtype simplified=fd_make_simple_choice(arg1);
	fdtype result=opcode_unary_nd_dispatch(opcode,simplified);
	fd_decref(arg1); fd_decref(simplified);
	return result;}
      else {
	fdtype result=opcode_unary_nd_dispatch(opcode,arg1);
	fd_decref(arg1);
	return result;}
    else if (opcode<FD_NUMERIC2_OPCODES)
      /* Otherwise, we iterate over the argument */
      if (FD_EMPTY_CHOICEP(arg1)) return arg1;
      else if (FD_EXPECT_FALSE((FD_CHOICEP(arg1)) || (FD_ACHOICEP(arg1)))) {
	fdtype results=FD_EMPTY_CHOICE;
	FD_DO_CHOICES(arg,arg1) {
	  fdtype result=opcode_unary_dispatch(opcode,arg);
	  if (FD_ABORTP(result)) {
	    fd_decref(results); fd_decref(arg1);
	    FD_STOP_DO_CHOICES;
	    return result;}
	  else {FD_ADD_TO_CHOICE(results,result);}}
	fd_decref(arg1);
	return results;}
      else {
	/* Or just dispatch directly if we have a singleton */
	fdtype result=opcode_unary_dispatch(opcode,arg1);
	fd_decref(arg1);
	return result;}
    else { /* At this point, we must not have enough arguments, since
	      the opcode is past the unary operation range. */
      fd_decref(arg1);
      return fd_err(fd_TooFewArgs,"opcode eval",NULL,expr);}
  /* If we get here, we have additional arguments. */
  else if (FD_EXPECT_FALSE(opcode<FD_NUMERIC2_OPCODES)) {
    /* All unary opcodes live beneath FD_NUMERIC2_OPCODES.
       We should probably catch this earlier. */
    fd_decref(arg1);
    return fd_err(fd_TooManyArgs,"opcode eval",NULL,expr);}
  else if ((opcode<FD_NARY_OPCODES) && (FD_EMPTY_CHOICEP(arg1)))
    /* Prune calls */
    return arg1;
  else if (opcode<FD_NARY_OPCODES) /* Binary opcodes all live beneath FD_NARY_OPCODES */ { 
    /* Binary calls start by evaluating the second argument */
    fdtype arg2=fd_eval(FD_CAR(body),env);
    if (FD_ABORTP(arg2)) {
      fd_decref(arg1); return arg2;}
    else if (FD_VOIDP(arg2)) {
      fd_decref(arg1);
      return fd_err(fd_VoidArgument,"opcode eval",NULL,FD_CAR(body));}
    else if (FD_EMPTY_CHOICEP(arg2)) {
      /* Prune the call */
      fd_decref(arg1); return arg2;}
    else if ((FD_CHOICEP(arg1)) || (FD_ACHOICEP(arg1)) ||
	     (FD_CHOICEP(arg2)) || (FD_ACHOICEP(arg2))) 
      /* opcode_binary_nd_dispatch handles decref of arg1 and arg2 */
      return opcode_binary_nd_dispatch(opcode,arg1,arg2);
    else {
      /* This is the dispatch case where we just go to dispatch. */
      fdtype result;
      if (opcode<FD_EQ_OPCODE) /* Numeric operation */
	if (FD_EXPECT_FALSE(!(FD_NUMBERP(arg2))))
	  result=fd_type_error(_("number"),"numeric opcode",arg2);
	else result=opcode_binary_dispatch(opcode,arg1,arg2);
      else result=opcode_binary_dispatch(opcode,arg1,arg2);
      fd_decref(arg1); fd_decref(arg2);
      return result;}}
  else if ((opcode==FD_GET_OPCODE) || (opcode==FD_PGET_OPCODE))
    if (FD_EMPTY_CHOICEP(arg1)) return FD_EMPTY_CHOICE;
    else {
      fdtype slotid_arg=fd_get_arg(expr,2);
      fdtype slotids=fd_eval(slotid_arg,env), result;
      if (FD_ABORTP(slotids)) return slotids;
      else if (FD_VOIDP(slotids))
	return fd_err(fd_SyntaxError,"OPCODE fget",NULL,expr);
      if (opcode==FD_GET_OPCODE)
	result=fd_fget(arg1,slotids);
      else {
	fdtype dflt=fd_eval(fd_get_arg(expr,3),env);
	if (FD_ABORTP(dflt)) result=dflt;
	else if (FD_VOIDP(dflt)) {
	  result=fd_err(fd_VoidArgument,"OPCODE fget",NULL,fd_get_arg(expr,3));}
	else result=fd_get(arg1,slotids,dflt);
	fd_decref(dflt);}
      fd_decref(arg1); fd_decref(slotids);
      return result;}
  else if ((opcode==FD_TEST_OPCODE) || (opcode==FD_PTEST_OPCODE))
    if (FD_EMPTY_CHOICEP(arg1)) return FD_FALSE;
    else {
      fdtype slotid_arg=fd_get_arg(expr,2);
      fdtype values_arg=fd_get_arg(expr,3);
      fdtype slotids=fd_eval(slotid_arg,env), values, result;
      if (FD_ABORTP(slotids)) return slotids;
      else if (FD_VOIDP(slotids))
	return fd_err(fd_SyntaxError,"OPCODE ftest",NULL,expr);
      else if (FD_VOIDP(values_arg))
	values=FD_EMPTY_CHOICE;
      else values=fd_eval(values_arg,env);
      if (FD_ABORTP(values)) return values;
      if (opcode==FD_TEST_OPCODE)
	result=fd_ftest(arg1,slotids,values);
      else if (fd_test(arg1,slotids,values))
	result=FD_TRUE;
      else result=FD_FALSE;
      fd_decref(arg1); fd_decref(slotids); fd_decref(values);
      return result;}
  else if ((opcode>FD_SETOPS_OPCODES) &&
	   (opcode<=FD_DIFFERENCE_OPCODE)) {
    fdtype arg2expr=fd_get_arg(expr,2), argv[2];
    fdtype arg2=fd_eval(arg2expr,env);
    fdtype result=FD_ERROR_VALUE;
    argv[0]=arg1; argv[1]=arg2;
    if (FD_ABORTP(arg2)) result=arg2;
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
  else if (opcode==FD_XREF_OPCODE)
    if (FD_EMPTY_CHOICEP(arg1))
      return FD_EMPTY_CHOICE;
    else {
      fdtype offset_arg=fd_get_arg(expr,2);
      fdtype type_arg=fd_get_arg(expr,3);
      if ((FD_PAIRP(type_arg)) &&
	  (FD_EQ(FD_CAR(type_arg),quote_symbol)) &&
	  (FD_PAIRP(FD_CDR(type_arg))))
	type_arg=FD_CAR(FD_CDR(type_arg));
      if (FD_CHOICEP(arg1)) {
	fdtype results=FD_EMPTY_CHOICE;
	FD_DO_CHOICES(a1,arg1)
	  if (FD_COMPOUNDP(a1)) {
	    fdtype result=xref_opcode(a1,FD_FIX2INT(offset_arg),type_arg);
	    if (FD_ABORTP(result)) {
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
      else if (FD_COMPOUNDP(arg1))
	return xref_opcode(arg1,FD_FIX2INT(offset_arg),type_arg);
      else {
	fdtype result=fd_err(fd_TypeError,"xref",
			     ((FD_VOIDP(type_arg)) ?
			      ((u8_string)u8_strdup("compound")) :
			      (fd_dtype2string(type_arg))),
			     arg1);
	fd_decref(arg1);
	return result;}}
  else {
    fd_decref(arg1);
    return fd_err(fd_SyntaxError,"opcode eval",NULL,expr);}
}




/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
