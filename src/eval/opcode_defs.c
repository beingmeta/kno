static lispval xref_type_error(lispval x,lispval tag)
{
  if (VOIDP(tag))
    kno_seterr(kno_TypeError,"XREF_OPCODE","compound",x);
  else {
    u8_string buf=kno_lisp2string(tag);
    kno_seterr(kno_TypeError,"XREF_OPCODE",buf,x);
    u8_free(buf);}
  return KNO_ERROR_VALUE;
}

static lispval xref_op(struct KNO_COMPOUND *c,long long i,lispval tag,int free)
{
  if ((VOIDP(tag)) || ((c->compound_typetag) == tag)) {
    if ((i>=0) && (i<c->compound_length)) {
      lispval *values = &(c->compound_0), value;
      if (c->compound_ismutable)
        u8_lock_mutex(&(c->compound_lock));
      value = values[i];
      kno_incref(value);
      if (c->compound_ismutable)
        u8_unlock_mutex(&(c->compound_lock));
      if (free) kno_decref((lispval)c);
      return value;}
    else {
      kno_seterr(kno_RangeError,"xref",NULL,(lispval)c);
      if (free) kno_decref((lispval)c);
      return KNO_ERROR_VALUE;}}
  else {
    lispval err=xref_type_error((lispval)c,tag);
    if (free) kno_decref((lispval)c);
    return err;}
}

static lispval xref_opcode(lispval x,long long i,lispval tag)
{
  if (!(CONSP(x))) {
    if (EMPTYP(x))
      return EMPTY;
    else return xref_type_error(x,tag);}
  else if (KNO_COMPOUNDP(x))
    return xref_op((struct KNO_COMPOUND *)x,i,tag,1);
  else if (CHOICEP(x)) {
    lispval results = EMPTY;
    DO_CHOICES(c,x) {
      lispval r = xref_op((struct KNO_COMPOUND *)c,i,tag,0);
      if (KNO_ABORTED(r)) {
        kno_decref(results); results = r;
        KNO_STOP_DO_CHOICES;
        break;}
      else {CHOICE_ADD(results,r);}}
    /* Need to free this */
    kno_decref(x);
    return results;}
  else return kno_err(kno_TypeError,"xref",kno_lisp2string(tag),x);
}

static lispval xpred_opcode(lispval x,lispval tag)
{
  if (KNO_COMPOUNDP(x)) {
    int match = KNO_COMPOUND_TYPEP(x,tag);
    kno_decref(x);
    if (match)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if (KNO_CHOICEP(x)) {
    int match = 0, nomatch = 0;
    KNO_DO_CHOICES(e,x) {
      if (KNO_COMPOUND_TYPEP(e,tag)) {
        match=1; if (nomatch) {
          KNO_STOP_DO_CHOICES;
          break;}}
      else {
        nomatch = 1;
        if (match) {
          KNO_STOP_DO_CHOICES;
          break;}}}
    kno_decref(x);
    if (match) {
      if (nomatch) {
        lispval tmpvec[2] = {KNO_FALSE,KNO_TRUE};
        return kno_init_choice(NULL,2,tmpvec,KNO_CHOICE_ISATOMIC);}
      else return KNO_TRUE;}
    else return KNO_FALSE;}
  else {
    kno_decref(x);
    return KNO_FALSE;}
}

static lispval elt_opcode(lispval arg1,lispval arg2)
{
  if ((KNO_SEQUENCEP(arg1)) && (KNO_INTP(arg2))) {
    lispval result;
    long long off = FIX2INT(arg2), len = kno_seq_length(arg1);
    if (off<0) off = len+off;
    result = kno_seq_elt(arg1,off);
    if (result == KNO_TYPE_ERROR)
      return kno_type_error(_("sequence"),"KNO_OPCODE_ELT",arg1);
    else if (result == KNO_RANGE_ERROR) {
      char buf[32];
      sprintf(buf,"%lld",off);
      return kno_err(kno_RangeError,"KNO_OPCODE_ELT",u8_strdup(buf),arg1);}
    else return result;}
  else if (!(KNO_SEQUENCEP(arg1)))
    return kno_type_error(_("sequence"),"KNO_OPCODE_ELT",arg1);
  else return kno_type_error(_("fixnum"),"KNO_OPCODE_ELT",arg2);
}

static lispval eltn_opcode(lispval arg1,int n,u8_context opname)
{
  if ( ( n == 0) && (KNO_PAIRP(arg1)) )
    return kno_incref(KNO_CAR(arg1));
  else if (VECTORP(arg1))
    if (VEC_LEN(arg1) > n)
      return kno_incref(VEC_REF(arg1,n));
    else return kno_err(kno_RangeError,opname,NULL,arg1);
  else if (COMPOUND_VECTORP(arg1))
    if (COMPOUND_VECLEN(arg1) > n)
      return kno_incref(XCOMPOUND_VEC_REF(arg1,n));
    else return kno_err(kno_RangeError,opname,NULL,arg1);
  else return kno_seq_elt(arg1,n);
}

/* PICK opcodes */

static lispval pickoids_opcode(lispval arg1)
{
  if (OIDP(arg1)) return arg1;
  else if (EMPTYP(arg1)) return arg1;
  else if (CHOICEP(arg1)) {
    lispval choice, results = EMPTY;
    int free_choice = 0, all_oids = 1;
    if (CHOICEP(arg1)) choice = arg1;
    else {
      choice = kno_make_simple_choice(arg1);
      free_choice = 1;}
    {DO_CHOICES(elt,choice) {
        if (OIDP(elt)) {CHOICE_ADD(results,elt);}
        else if (all_oids)
          all_oids = 0;
        else NO_ELSE;}}
    if (all_oids) {
      kno_decref(results);
      if (free_choice)
        return choice;
      else return kno_incref(choice);}
    else if (free_choice) kno_decref(choice);
    else NO_ELSE;
    return kno_simplify_choice(results);}
  else return EMPTY;
}

static lispval pickstrings_opcode(lispval arg1)
{
  if (CHOICEP(arg1)) {
    lispval choice, results = EMPTY;
    int free_choice = 0, all_strings = 1;
    if (CHOICEP(arg1))
      choice = arg1;
    else {
      choice = kno_make_simple_choice(arg1);
      free_choice = 1;}
    {DO_CHOICES(elt,choice) {
        if (STRINGP(elt)) {
          kno_incref(elt);
          CHOICE_ADD(results,elt);}
        else if (all_strings)
          all_strings = 0;
        else NO_ELSE;}}
    if (all_strings) {
      kno_decref(results);
      if (free_choice)
        return choice;
      else return kno_incref(choice);}
    else if (free_choice)
      kno_decref(choice);
    else NO_ELSE;
    return kno_simplify_choice(results);}
  else if (STRINGP(arg1))
    return kno_incref(arg1);
  else return EMPTY;
}

static lispval picknums_opcode(lispval arg1)
{
  if (EMPTYP(arg1)) return arg1;
  else if (CHOICEP(arg1)) {
    lispval choice, results = EMPTY;
    int free_choice = 0, all_nums = 1;
    if (CHOICEP(arg1))
      choice = arg1;
    else {
      choice = kno_make_simple_choice(arg1);
      free_choice = 1;}
    {DO_CHOICES(elt,choice) {
        if (KNO_FIXNUMP(elt)) {
          CHOICE_ADD(results,elt);}
        else if (KNO_NUMBERP(elt)) {
          kno_incref(elt);
          CHOICE_ADD(results,elt);}
        else if (all_nums)
          all_nums = 0;
        else NO_ELSE;}}
    if (all_nums) {
      kno_decref(results);
      if (free_choice)
        return choice;
      else return kno_incref(choice);}
    else if (free_choice)
      kno_decref(choice);
    else NO_ELSE;
    return kno_simplify_choice(results);}
  else if (KNO_NUMBERP(arg1))
    return kno_incref(arg1);
  else return EMPTY;
}

static lispval pickmaps_opcode(lispval arg1)
{
  if (EMPTYP(arg1))
    return arg1;
  else if (CHOICEP(arg1)) {
    lispval choice, results = EMPTY;
    int free_choice = 0, all_nums = 1;
    if (CHOICEP(arg1))
      choice = arg1;
    else {
      choice = kno_make_simple_choice(arg1);
      free_choice = 1;}
    {DO_CHOICES(elt,choice) {
        if ( (KNO_SLOTMAPP(elt)) || (KNO_SCHEMAPP(elt)) ) {
          kno_incref(elt);
          CHOICE_ADD(results,elt);}
        else if (all_nums) 
          all_nums = 0;
        else NO_ELSE;}}
    if (all_nums) {
      kno_decref(results);
      if (free_choice)
        return choice;
      else return kno_incref(choice);}
    else if (free_choice)
      kno_decref(choice);
    else NO_ELSE;
    return kno_simplify_choice(results);}
  else if ( (KNO_SLOTMAPP(arg1)) || (KNO_SCHEMAPP(arg1)) )
    return kno_incref(arg1);
  else return EMPTY;
}

static lispval pickone_opcode(lispval normal)
{
  int n = KNO_CHOICE_SIZE(normal);
  if (n) {
    lispval chosen;
    int i = u8_random(n);
    const lispval *data = KNO_CHOICE_DATA(normal);
    chosen = data[i]; kno_incref(chosen);
    return chosen;}
  else return EMPTY;
}

static lispval nd1_call(lispval opcode,lispval arg1)
{
  switch (opcode) {
  case KNO_AMBIGP_OPCODE:
    if (CHOICEP(arg1))
      return KNO_TRUE;
    else return KNO_FALSE;
  case KNO_SINGLETONP_OPCODE:
    if (EMPTYP(arg1))
      return KNO_FALSE;
    else if (CHOICEP(arg1))
      return KNO_FALSE;
    else return KNO_TRUE;
  case KNO_FAILP_OPCODE:
    if (arg1==EMPTY)
      return KNO_TRUE;
    else return KNO_FALSE;
  case KNO_EXISTSP_OPCODE:
    if (arg1==EMPTY)
      return KNO_FALSE;
    else return KNO_TRUE;
  case KNO_SINGLETON_OPCODE:
    if (CHOICEP(arg1))
      return EMPTY;
    else return kno_incref(arg1);
  case KNO_CAR_OPCODE:
    if (EMPTYP(arg1))
      return arg1;
    else if (PAIRP(arg1))
      return kno_incref(KNO_CAR(arg1));
    else if (CHOICEP(arg1)) {
      lispval results = EMPTY;
      DO_CHOICES(arg,arg1)
        if (PAIRP(arg)) {
          lispval car = KNO_CAR(arg);
          kno_incref(car);
          CHOICE_ADD(results,car);}
        else {
          kno_decref(results);
          return kno_type_error(_("pair"),"CAR opcode",arg);}
      return results;}
    else return kno_type_error(_("pair"),"CAR opcode",arg1);
  case KNO_CDR_OPCODE:
    if (EMPTYP(arg1)) return arg1;
    else if (PAIRP(arg1))
      return kno_incref(KNO_CDR(arg1));
    else if (CHOICEP(arg1)) {
      lispval results = EMPTY;
      DO_CHOICES(arg,arg1)
        if (PAIRP(arg)) {
          lispval cdr = KNO_CDR(arg); kno_incref(cdr);
          CHOICE_ADD(results,cdr);}
        else {
          kno_decref(results);
          return kno_type_error(_("pair"),"CDR opcode",arg);}
      return results;}
    else return kno_type_error(_("pair"),"CDR opcode",arg1);
  case KNO_LENGTH_OPCODE:
    if (arg1==EMPTY) return EMPTY;
    else if (CHOICEP(arg1)) {
      lispval results = EMPTY;
      DO_CHOICES(arg,arg1) {
        if (KNO_SEQUENCEP(arg)) {
          int len = kno_seq_length(arg);
          lispval dlen = KNO_INT(len);
          CHOICE_ADD(results,dlen);}
        else {
          kno_decref(results);
          return kno_type_error(_("sequence"),"LENGTH opcode",arg);}}
      return kno_simplify_choice(results);}
    else if (KNO_SEQUENCEP(arg1))
      return KNO_INT(kno_seq_length(arg1));
    else return kno_type_error(_("sequence"),"LENGTH opcode",arg1);
  case KNO_QCHOICE_OPCODE:
    if (CHOICEP(arg1)) {
      kno_incref(arg1);
      return kno_init_qchoice(NULL,arg1);}
    else if (EMPTYP(arg1))
      return kno_init_qchoice(NULL,EMPTY);
    else return kno_incref(arg1);
  case KNO_CHOICE_SIZE_OPCODE:
    if (CHOICEP(arg1)) {
      int sz = KNO_CHOICE_SIZE(arg1);
      return KNO_INT(sz);}
    else if (EMPTYP(arg1))
      return KNO_INT(0);
    else return KNO_INT(1);
  case KNO_PICKOIDS_OPCODE:
    return pickoids_opcode(arg1);
  case KNO_PICKSTRINGS_OPCODE:
    return pickstrings_opcode(arg1);
  case KNO_PICKNUMS_OPCODE:
    return picknums_opcode(arg1);
  case KNO_PICKMAPS_OPCODE:
    return pickmaps_opcode(arg1);
  case KNO_PICKONE_OPCODE:
    if (CHOICEP(arg1))
      return pickone_opcode(arg1);
    else return kno_incref(arg1);
  case KNO_IFEXISTS_OPCODE:
    if (EMPTYP(arg1))
      return VOID;
    else return kno_incref(arg1);
  case KNO_SOMETRUE_OPCODE:
    if ( (EMPTYP(arg1)) || (FALSEP(arg1)) )
      return KNO_FALSE;
    else return KNO_TRUE;
  case KNO_FIXCHOICE_OPCODE:
    if (PRECHOICEP(arg1))
      return kno_simplify_choice(arg1);
    else return kno_incref(arg1);
  default:
    return kno_err(_("Invalid opcode"),"opcode eval",NULL,VOID);
  }
}

static lispval d1_call(lispval opcode,lispval arg1)
{
  switch (opcode) {
  case KNO_MINUS1_OPCODE:
  case KNO_PLUS1_OPCODE: {
    int delta = (opcode == KNO_MINUS1_OPCODE) ? (-1) : (1);
    if (FIXNUMP(arg1)) {
      long long iarg = FIX2INT(arg1);
      return KNO_INT(iarg+delta);}
    else if (NUMBERP(arg1))
      return kno_plus(arg1,KNO_INT(delta));
    else return kno_type_error(_("number"),"opcode 1+/-",arg1);}
  case KNO_NUMBERP_OPCODE:
    if (NUMBERP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_ZEROP_OPCODE:
    if (arg1==KNO_INT(0))
      return KNO_TRUE;
    else if (KNO_FLONUMP(arg1))
      if ( (KNO_FLONUM(arg1)) == 0.0)
        return KNO_TRUE;
      else return KNO_FALSE;
    else if (!(KNO_NUMBERP(arg1)))
      return KNO_FALSE;
    else {
      int cmp = kno_numcompare(arg1,KNO_INT(0));
      if (cmp==0)
        return KNO_TRUE;
      else if (cmp>1)
        return KNO_ERROR;
      else return KNO_FALSE;}
  case KNO_VECTORP_OPCODE:
    if (VECTORP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_PAIRP_OPCODE:
    if (PAIRP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_EMPTY_LISTP_OPCODE:
    if (arg1==NIL) return KNO_TRUE; else return KNO_FALSE;
  case KNO_STRINGP_OPCODE:
    if (STRINGP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_OIDP_OPCODE:
    if (OIDP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_SYMBOLP_OPCODE:
    if (KNO_SYMBOLP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_FIXNUMP_OPCODE:
    if (FIXNUMP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_FLONUMP_OPCODE:
    if (KNO_FLONUMP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_SEQUENCEP_OPCODE:
    if (KNO_SEQUENCEP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_TABLEP_OPCODE:
    if (KNO_SEQUENCEP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_CADR_OPCODE: {
    lispval cdr = KNO_CDR(arg1);
    if (PAIRP(cdr)) return kno_incref(KNO_CAR(cdr));
    else return kno_err(kno_RangeError,"KNO_CADR",NULL,arg1);}
  case KNO_CDDR_OPCODE: {
    lispval cdr = KNO_CDR(arg1);
    if (PAIRP(cdr)) return kno_incref(KNO_CDR(cdr));
    else return kno_err(kno_RangeError,"KNO_CDDR",NULL,arg1);}
  case KNO_CADDR_OPCODE: {
    lispval cdr = KNO_CDR(arg1);
    if (PAIRP(cdr)) {
      lispval cddr = KNO_CDR(cdr);
      if (PAIRP(cddr)) return kno_incref(KNO_CAR(cddr));
      else return kno_err(kno_RangeError,"KNO_CADDR",NULL,arg1);}
    else return kno_err(kno_RangeError,"KNO_CADDR",NULL,arg1);}
  case KNO_CDDDR_OPCODE: {
    lispval cdr = KNO_CDR(arg1);
    if (PAIRP(cdr)) {
      lispval cddr = KNO_CDR(cdr);
      if (PAIRP(cddr))
        return kno_incref(KNO_CDR(cddr));
      else return kno_err(kno_RangeError,"KNO_CDDR",NULL,arg1);}
    else return kno_err(kno_RangeError,"KNO_CDDDR",NULL,arg1);}
  case KNO_FIRST_OPCODE:
    return eltn_opcode(arg1,0,"KNO_FIRST_OPCODE");
  case KNO_SECOND_OPCODE:
    return eltn_opcode(arg1,1,"KNO_SECOND_OPCODE");
  case KNO_THIRD_OPCODE:
    return eltn_opcode(arg1,2,"KNO_THIRD_OPCODE");
  case KNO_TONUMBER_OPCODE:
    if (FIXNUMP(arg1)) return arg1;
    else if (NUMBERP(arg1)) return kno_incref(arg1);
    else if (STRINGP(arg1))
      return kno_string2number(CSTRING(arg1),10);
    else if (KNO_CHARACTERP(arg1))
      return KNO_INT(KNO_CHARCODE(arg1));
    else return kno_type_error(_("number|string"),"opcode ->number",arg1);
  case KNO_GETKEYS_OPCODE:
    if (KNO_TABLEP(arg1))
      return kno_getkeys(arg1);
    else return kno_type_error(_("table"),"opcode GETKEYS",arg1);
  case KNO_GETVALUES_OPCODE:
    if (KNO_TABLEP(arg1))
      return kno_getvalues(arg1);
    else return kno_type_error(_("table"),"opcode GETVALUES",arg1);
  case KNO_GETASSOCS_OPCODE:
    if (KNO_TABLEP(arg1))
      return kno_getassocs(arg1);
    else return kno_type_error(_("table"),"opcode GETASSOCS",arg1);
  default:
    return kno_err(_("Invalid opcode"),"opcode eval",NULL,VOID);
  }
}

static lispval d2_call(lispval opcode,lispval arg1,lispval arg2)
{
  switch (opcode) {
  case KNO_ELT_OPCODE:
    return elt_opcode(arg1,arg2);
  case KNO_CONS_OPCODE:
    return kno_init_pair(NULL,kno_incref(arg1),kno_incref(arg2));
  case KNO_EQ_OPCODE:
    if (arg1==arg2) return KNO_TRUE; else return KNO_FALSE;
  case KNO_EQV_OPCODE: {
    if (arg1==arg2) return KNO_TRUE;
    else if ((NUMBERP(arg1)) && (NUMBERP(arg2)))
      if (kno_numcompare(arg1,arg2)==0)
        return KNO_TRUE; else return KNO_FALSE;
    else return KNO_FALSE;}
  case KNO_EQUAL_OPCODE: {
    if ((ATOMICP(arg1)) && (ATOMICP(arg2)))
      if (arg1==arg2) return KNO_TRUE; else return KNO_FALSE;
    else if (KNO_EQUAL(arg1,arg2)) return KNO_TRUE;
    else return KNO_FALSE;}
  default:
    return kno_err(_("Invalid opcode"),"opcode eval",NULL,VOID);
  }
}


KNO_FASTOP int numeric_argp(lispval x)
{
  /* This checks if there is a type error.
     The empty choice isn't a type error since it will just
     generate an empty choice as a result. */
  if ((EMPTYP(x))||(FIXNUMP(x)))
    return 1;
  else if (!(CONSP(x)))
    return 0;
  else switch (KNO_CONSPTR_TYPE(x)) {
    case kno_flonum_type: case kno_bigint_type:
    case kno_rational_type: case kno_complex_type:
      return 1;
    case kno_choice_type: case kno_prechoice_type: {
      DO_CHOICES(a,x) {
        if (FIXNUMP(a)) {}
        else if (PRED_TRUE(NUMBERP(a))) {}
        else {
          KNO_STOP_DO_CHOICES;
          return 0;}}
      return 1;}
    default:
      return 0;}
}

static lispval numop_call(lispval opcode,lispval arg1,lispval arg2)
{
  if (KNO_EXPECT_FALSE(!(numeric_argp(arg1))))
    return kno_err("NotANumber","numop_call",NULL,arg1);
  else if (KNO_EXPECT_FALSE(!(numeric_argp(arg2))))
    return kno_err("NotANumber","numop_call",NULL,arg2);
  else switch (opcode) {
    case KNO_NUMEQ_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
        if ((FIX2INT(arg1)) == (FIX2INT(arg2)))
          return KNO_TRUE;
        else return KNO_FALSE;
      else if (kno_numcompare(arg1,arg2)==0) return KNO_TRUE;
      else return KNO_FALSE;}
    case KNO_GT_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
        if ((FIX2INT(arg1))>(FIX2INT(arg2)))
          return KNO_TRUE;
        else return KNO_FALSE;
      else if (kno_numcompare(arg1,arg2)>0) return KNO_TRUE;
      else return KNO_FALSE;}
    case KNO_GTE_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
        if ((FIX2INT(arg1))>=(FIX2INT(arg2)))
          return KNO_TRUE;
        else return KNO_FALSE;
      else if (kno_numcompare(arg1,arg2)>=0) return KNO_TRUE;
      else return KNO_FALSE;}
    case KNO_LT_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
        if ((FIX2INT(arg1))<(FIX2INT(arg2)))
          return KNO_TRUE;
        else return KNO_FALSE;
      else if (kno_numcompare(arg1,arg2)<0) return KNO_TRUE;
      else return KNO_FALSE;}
    case KNO_LTE_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
        if ((FIX2INT(arg1))<=(FIX2INT(arg2)))
          return KNO_TRUE;
        else return KNO_FALSE;
      else if (kno_numcompare(arg1,arg2)<=0) return KNO_TRUE;
      else return KNO_FALSE;}
    case KNO_PLUS_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
        long long m = FIX2INT(arg1), n = FIX2INT(arg2);
        return KNO_INT(m+n);}
      else if ((KNO_FLONUMP(arg1)) && (KNO_FLONUMP(arg2))) {
        double x = KNO_FLONUM(arg1), y = KNO_FLONUM(arg2);
        return kno_init_double(NULL,x+y);}
      else return kno_plus(arg1,arg2);}
    case KNO_MINUS_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
        long long m = FIX2INT(arg1), n = FIX2INT(arg2);
        return KNO_INT(m-n);}
      else if ((KNO_FLONUMP(arg1)) && (KNO_FLONUMP(arg2))) {
        double x = KNO_FLONUM(arg1), y = KNO_FLONUM(arg2);
        return kno_init_double(NULL,x-y);}
      else return kno_subtract(arg1,arg2);}
    case KNO_TIMES_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
        long long m = FIX2INT(arg1), n = FIX2INT(arg2);
        return KNO_INT(m*n);}
      else if ((KNO_FLONUMP(arg1)) && (KNO_FLONUMP(arg2))) {
        double x = KNO_FLONUM(arg1), y = KNO_FLONUM(arg2);
        return kno_init_double(NULL,x*y);}
      else return kno_multiply(arg1,arg2);}
    case KNO_FLODIV_OPCODE: {
      if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
        long long m = FIX2INT(arg1), n = FIX2INT(arg2);
        double x = (double)m, y = (double)n;
        return kno_init_double(NULL,x/y);}
      else if ((KNO_FLONUMP(arg1)) && (KNO_FLONUMP(arg2))) {
        double x = KNO_FLONUM(arg1), y = KNO_FLONUM(arg2);
        return kno_init_double(NULL,x/y);}
      else {
        double x = kno_todouble(arg1), y = kno_todouble(arg2);
        return kno_init_double(NULL,x/y);}}
    default:
      return kno_err(_("Invalid opcode"),"numop_call",NULL,VOID);
    }
}

static lispval nd2_call(lispval opcode,lispval arg1,lispval arg2)
{
  lispval result = KNO_ERROR_VALUE;
  if (KNO_ABORTED(arg2))
    result = arg2;
  else if (VOIDP(arg2)) {
    result = kno_err(kno_VoidArgument,"OPCODE setop",NULL,opcode);}
  else switch (opcode) {
    case KNO_IDENTICAL_OPCODE:
      if (arg1==arg2)
        result = KNO_TRUE;
      else if (KNO_EQUAL(arg1,arg2))
        result = KNO_TRUE;
      else result = KNO_FALSE;
      break;
    case KNO_OVERLAPS_OPCODE:
      if (arg1==arg2) {
        if (EMPTYP(arg1))
          result = KNO_FALSE;
        else result = KNO_TRUE;}
      else if (kno_overlapp(arg1,arg2))
        result = KNO_TRUE;
      else result = KNO_FALSE;
      break;
    case KNO_CONTAINSP_OPCODE:
      if (EMPTYP(arg2))
        result = KNO_FALSE;
      else if (arg1 == arg2)
        result = KNO_TRUE;
      else if (EMPTYP(arg1))
        result = KNO_FALSE;
      else if (kno_containsp(arg1,arg2))
        result = KNO_TRUE;
      else result = KNO_FALSE;
      break;
    case KNO_CHOICEREF_OPCODE:
      if (!(FIXNUMP(arg2))) {
        kno_decref(arg1);
        result = kno_err(kno_SyntaxError,"choiceref_opcode",NULL,arg2);}
      else {
        int i = FIX2INT(arg2);
        if (i<0)
          result = kno_err(kno_SyntaxError,"choiceref_opcode",NULL,arg2);
        else if ((i==0)&&(EMPTYP(arg1)))
          result = VOID;
        else if (CHOICEP(arg1)) {
          struct KNO_CHOICE *ch = (kno_choice)arg1;
          if (i<ch->choice_size) {
            const lispval *elts = KNO_XCHOICE_DATA(ch);
            lispval elt = elts[i];
            result = kno_incref(elt);}
          else result = VOID;}
        else if (i==0) result=arg1;
        else result = VOID;}
    default:
      result = kno_err(_("Invalid opcode"),"numop_call",NULL,VOID);
    }
  return result;
}

static lispval try_op(lispval exprs,kno_lexenv env,
                      kno_stack stack,int tail)
{
  while (PAIRP(exprs)) {
    lispval expr = pop_arg(exprs);
    if (NILP(exprs))
      return _kno_fast_eval(expr,env,stack,tail);
    else {
      lispval val  = _kno_fast_eval(expr,env,stack,0);
      if (KNO_ABORTED(val)) return val;
      else if (!(EMPTYP(val)))
        return val;
      else kno_decref(val);}}
  return EMPTY;
}

static lispval union_op(lispval exprs,kno_lexenv env,kno_stack stack)
{
  lispval result = KNO_EMPTY_CHOICE;
  while (PAIRP(exprs)) {
    lispval expr = pop_arg(exprs);
    lispval val  = _kno_fast_eval(expr,env,stack,0);
    if (KNO_ABORTED(val)) {
      kno_decref(result);
      return val;}
    else {KNO_ADD_TO_CHOICE(result,val);}}
  return kno_simplify_choice(result);
}

static lispval intersect_op(lispval exprs,kno_lexenv env,kno_stack stack)
{
  lispval result = VOID;
  while (PAIRP(exprs)) {
    lispval expr = pop_arg(exprs);
    lispval val  = _kno_fast_eval(expr,env,stack,0);
    if (KNO_ABORTED(val)) {
      kno_decref(result);
      return val;}
    else if (KNO_VOIDP(result))
      result = val;
    else if (KNO_EMPTYP(val)) {
      kno_decref(result);
      return val;}
    else {
      lispval combine[2] = {result,val};
      lispval combined = kno_intersection(combine,2);
      kno_decref(result);
      kno_decref(val);
      if (KNO_EMPTYP(combined))
        return combined;
      else result=combined;}}
  return kno_simplify_choice(result);
}

static lispval difference_op(lispval exprs,kno_lexenv env,kno_stack stack)
{
  lispval result = VOID;
  while (PAIRP(exprs)) {
    lispval expr = pop_arg(exprs);
    lispval val  = _kno_fast_eval(expr,env,stack,0);
    if (KNO_ABORTED(val)) {
      kno_decref(result);
      return val;}
    else if (KNO_VOIDP(result))
      result = val;
    else if (KNO_EMPTYP(val)) {}
    else {
      lispval combined = kno_difference(result,val);
      kno_decref(result);
      kno_decref(val);
      if (KNO_EMPTYP(combined))
        return combined;
      else result=combined;}}
  return kno_simplify_choice(result);
}

static lispval and_op(lispval exprs,kno_lexenv env,kno_stack stack,int tail)
{
  while (PAIRP(exprs)) {
    lispval expr = pop_arg(exprs);
    if (NILP(exprs))
      return _kno_fast_eval(expr,env,stack,tail);
    else {
      lispval val  = _kno_fast_eval(expr,env,stack,0);
      if (KNO_ABORTED(val))
        return val;
      else if (FALSEP(val))
        return KNO_FALSE;
      else kno_decref(val);}}
  return KNO_TRUE;
}

static lispval or_op(lispval exprs,kno_lexenv env,kno_stack stack,int tail)
{
  while (PAIRP(exprs)) {
    lispval expr = pop_arg(exprs);
    if (NILP(exprs))
      return _kno_fast_eval(expr,env,stack,tail);
    else {
      lispval val  = _kno_fast_eval(expr,env,stack,0);
      if (KNO_ABORTED(val))
        return val;
      else if (!(FALSEP(val)))
        return val;
      else {}}}
  return KNO_FALSE;
}

/* Loop */

static lispval until_opcode(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval params = KNO_CDR(expr);
  lispval test_expr = KNO_CAR(params), loop_body = KNO_CDR(params);
  if (VOIDP(test_expr))
    return kno_err(kno_SyntaxError,"KNO_LOOP_OPCODE",NULL,expr);
  lispval test_val = _kno_fast_eval(test_expr,env,stack,0);
  if (KNO_ABORTED(test_val))
    return test_val;
  else while (FALSEP(test_val)) {
      lispval body_result=eval_body(loop_body,env,stack,"UNTIL",NULL,0);
      if (KNO_BROKEP(body_result))
        return KNO_FALSE;
      else if (KNO_ABORTED(body_result))
        return body_result;
      else kno_decref(body_result);
      test_val = _kno_fast_eval(test_expr,env,stack,0);
      if (KNO_ABORTED(test_val))
        return test_val;}
  return test_val;
}

/* Assignment */

#define CURRENT_VALUEP(x)                                 \
  (! ((cur == KNO_DEFAULT_VALUE) || (cur == KNO_UNBOUND) || \
      (cur == VOID) || (cur == KNO_NULL)))

static lispval combine_values(lispval combiner,lispval cur,lispval value)
{
  int use_cur=((KNO_ABORTP(cur)) || (CURRENT_VALUEP(cur)));
  switch (combiner) {
  case VOID: case KNO_FALSE:
    return value;
  case KNO_TRUE: case KNO_DEFAULT_VALUE:
    if (use_cur)
      return cur;
    else return value;
  case KNO_PLUS_OPCODE:
    if (!(use_cur)) cur=KNO_FIXNUM_ZERO;
    if ( (FIXNUMP(value)) && (FIXNUMP(cur)) ) {
      long long ic=FIX2INT(cur), ip=FIX2INT(value);
      return KNO_MAKE_FIXNUM(ic+ip);}
    else return kno_plus(cur,value);
  case KNO_MINUS_OPCODE:
    if (!(use_cur)) cur=KNO_FIXNUM_ZERO;
    if ( (FIXNUMP(value)) && (FIXNUMP(cur)) ) {
      long long ic=FIX2INT(cur), im=FIX2INT(value);
      return KNO_MAKE_FIXNUM(ic-im);}
    else return kno_subtract(cur,value);
  default:
    return value;
  }
}
static lispval assignop(kno_stack stack,kno_lexenv env,
                        lispval var,lispval expr,lispval combiner)
{
  if (KNO_LEXREFP(var)) {
    int up = KNO_LEXREF_UP(var);
    int across = KNO_LEXREF_ACROSS(var);
    kno_lexenv scan = ( (env->env_copy) ? (env->env_copy) : (env) );
    while ((up)&&(scan)) {
      kno_lexenv parent = scan->env_parent;
      if ((parent) && (parent->env_copy))
        scan = parent->env_copy;
      else scan = parent;
      up--;}
    if (PRED_TRUE(scan!=NULL)) {
      lispval bindings = scan->env_bindings;
      if (PRED_TRUE(SCHEMAPP(bindings))) {
        struct KNO_SCHEMAP *map = (struct KNO_SCHEMAP *)bindings;
        int map_len = map->schema_length;
        if (PRED_TRUE( across < map_len )) {
          lispval *values = map->schema_values;
          lispval cur     = values[across];
          if ( map->schemap_stackvals ) {
            kno_incref_vec(values,map_len);
            map->schemap_stackvals = 0;}
          if ( ( (combiner == KNO_TRUE) || (combiner == KNO_DEFAULT) ) &&
               ( (CURRENT_VALUEP(cur)) || (KNO_ABORTED(cur)) ) ) {
            if (KNO_ABORTED(cur))
              return cur;
            else return VOID;}
          else {
            lispval value = _kno_fast_eval(expr,env,stack,0);
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
                struct KNO_SCHEMAP *new_map =
                  (struct KNO_SCHEMAP *) new_bindings;
                values = new_map->schema_values;
                cur = values[across];}}
            if (KNO_ABORTED(value))
              return value;
            else if ( (combiner == KNO_FALSE) || (combiner == VOID) ) {
              /* Replace the currnet value */
              values[across]=value;
              kno_decref(cur);}
            else if (combiner == KNO_UNION_OPCODE) {
              if (KNO_ABORTED(value)) return value;
              if ((cur==VOID)||(cur==KNO_UNBOUND)||(cur==EMPTY))
                values[across]=value;
              else {CHOICE_ADD(values[across],value);}}
            else {
              lispval newv=combine_values(combiner,cur,value);
              if (cur != newv) {
                values[across]=newv;
                kno_decref(cur);
                if (newv != value) kno_decref(value);}
              else kno_decref(value);}
            return VOID;}}}}
    u8_string lexref=u8_mkstring("up%d/across%d",up,across);
    lispval env_copy=(lispval)kno_copy_env(env);
    return kno_err("BadLexref","ASSIGN_OPCODE",lexref,env_copy);}
  else if ((PAIRP(var)) &&
           (KNO_SYMBOLP(KNO_CAR(var))) &&
           (TABLEP(KNO_CDR(var)))) {
    int rv=-1;
    lispval table=KNO_CDR(var), sym=KNO_CAR(var);
    lispval value = _kno_fast_eval(expr,env,stack,0);
    if ( (combiner == KNO_FALSE) || (combiner == VOID) ) {
      if (KNO_LEXENVP(table))
        rv=kno_assign_value(sym,value,(kno_lexenv)table);
      else rv=kno_store(table,sym,value);}
    else if (combiner == KNO_UNION_OPCODE) {
      if (KNO_LEXENVP(table))
        rv=kno_add_value(sym,value,(kno_lexenv)table);
      else rv=kno_add(table,sym,value);}
    else {
      lispval cur=kno_get(table,sym,KNO_UNBOUND);
      lispval newv=combine_values(combiner,cur,value);
      if (KNO_ABORTED(newv))
        rv=-1;
      else rv=kno_store(table,sym,newv);
      kno_decref(cur);
      kno_decref(newv);}
    kno_decref(value);
    if (rv<0) {
      kno_seterr("AssignFailed","ASSIGN_OPCODE",NULL,expr);
      return KNO_ERROR_VALUE;}
    else return VOID;}
  return kno_err(kno_SyntaxError,"ASSIGN_OPCODE",NULL,expr);
}

/* Binding */

static lispval bindop(lispval op,
                      struct KNO_STACK *_stack,kno_lexenv env,
                      lispval vars,lispval inits,lispval body,
                      int tail)
{
  int i=0, n=VEC_LEN(vars);
  KNO_PUSH_STACK(bind_stack,"bindop","opframe",op);
  INIT_STACK_SCHEMA(bind_stack,bound,env,n,VEC_DATA(vars));
  lispval *values=bound_bindings.schema_values;
  lispval scan_inits = inits;
  kno_lexenv env_copy=NULL;
  while (i<n) {
    lispval val_expr = pop_arg(scan_inits);
    lispval val = _kno_fast_eval(val_expr,bound,bind_stack,0);
    if (KNO_ABORTED(val))
      _return val;
    if ( (env_copy == NULL) && (bound->env_copy) ) {
      env_copy=bound->env_copy; bound=env_copy;
      values=((kno_schemap)(bound->env_bindings))->schema_values;}
    values[i++]=val;}
  lispval result = eval_body(body,bound,bind_stack,"#BINDOP",NULL,tail);
  kno_pop_stack(bind_stack);
  return result;
}

static lispval vector_bindop(lispval op,
                     struct KNO_STACK *_stack,kno_lexenv env,
                             lispval vars,lispval inits,lispval body,
                             int tail)
{
  int i=0, n=VEC_LEN(vars);
  KNO_PUSH_STACK(bind_stack,"vector_bindop","opframe",op);
  INIT_STACK_SCHEMA(bind_stack,bound,env,n,VEC_DATA(vars));
  lispval *values=bound_bindings.schema_values;
  lispval *exprs=VEC_DATA(inits);
  kno_lexenv env_copy=NULL;
  while (i<n) {
    lispval val_expr=exprs[i];
    lispval val=_kno_fast_eval(val_expr,bound,bind_stack,0);
    if (KNO_ABORTED(val))
      _return val;
    if ( (env_copy == NULL) && (bound->env_copy) ) {
      env_copy=bound->env_copy; bound=env_copy;
      values=((kno_schemap)(bound->env_bindings))->schema_values;}
    values[i++]=val;}
  lispval result = eval_body(body,bound,bind_stack,"#VECTORBIND",NULL,tail);
  kno_pop_stack(bind_stack);
  return result;
}

static void reset_env_op(kno_lexenv env)
{
  if ( (env->env_copy) && (env != env->env_copy) ) {
    lispval tofree = (lispval) env;
    env->env_copy=NULL;
    kno_decref(tofree);}
}

static lispval handle_table_opcode(lispval opcode,lispval expr,
                                   kno_lexenv env,
                                   kno_stack _stack,
                                   int tail)
{
  lispval args = KNO_CDR(expr);
  lispval subject_arg = pop_arg(args);
  if (KNO_EXPECT_FALSE(!(KNO_PAIRP(args))))
    return kno_err(kno_TooFewArgs,"handle_table_opcode",NULL,expr);
  lispval subject = arg_eval(subject_arg,env,_stack);
  if (KNO_ABORTP(subject))
    return subject;
  else if (KNO_EMPTYP(subject)) {
    switch (opcode) {
    case KNO_GET_OPCODE: case KNO_PRIMGET_OPCODE:
      return KNO_EMPTY_CHOICE;
    case KNO_TEST_OPCODE: case KNO_PRIMTEST_OPCODE:
      return KNO_FALSE;
    case KNO_ASSERT_OPCODE: case KNO_ADD_OPCODE:
    case KNO_RETRACT_OPCODE: case KNO_DROP_OPCODE:
      return KNO_VOID;
    default:
      return kno_err("BadOpcode","handle_table_opcode",NULL,expr);}}
  else NO_ELSE;
  if (KNO_EXPECT_FALSE(!(KNO_TABLEP(subject)))) {
    kno_seterr("NotATable","handle_table_opcode",NULL,subject);
    kno_decref(subject);
    return KNO_ERROR_VALUE;}
  else NO_ELSE;
  lispval slotid_arg = pop_arg(args);
  lispval slotid = arg_eval(slotid_arg,env,_stack);
  if (KNO_ABORTP(slotid)) {
    kno_decref(subject);
    return slotid;}
  else NO_ELSE;
  lispval value_arg = (KNO_PAIRP(args)) ? (pop_arg(args)) : (KNO_VOID);
  lispval value = (KNO_VOIDP(value_arg)) ? (KNO_VOID) :
    (arg_eval(value_arg,env,_stack));
  if (KNO_ABORTP(value)) {
    kno_decref(subject); kno_decref(slotid);
    return value;}
  else NO_ELSE;
  lispval result = KNO_VOID;
  switch (opcode) {
  case KNO_GET_OPCODE:
    result = kno_fget(subject,slotid); break;
  case KNO_PRIMGET_OPCODE:
    if (KNO_VOIDP(value)) value = KNO_EMPTY_CHOICE;
    result = kno_get(subject,slotid,value); break;
  case KNO_TEST_OPCODE:
    result = kno_ftest(subject,slotid,value); break;
  case KNO_PRIMTEST_OPCODE: {
    int rv = kno_test(subject,slotid,value);
    if (rv < 0)
      result = KNO_ERROR_VALUE;
    else if (rv)
      result = KNO_TRUE;
    else result = KNO_FALSE;
    break;}
  case KNO_ADD_OPCODE:
    if (KNO_VOIDP(value))
      result = kno_err(kno_TooFewArgs,"handle_table_opcode/add",NULL,expr);
    else {
      int rv = kno_add(subject,slotid,value);
      if (rv < 0)
        result = KNO_ERROR_VALUE;
      else if (rv)
        result = KNO_TRUE;
      else result = KNO_FALSE;}
    break;
  case KNO_DROP_OPCODE: {
    int rv = kno_drop(subject,slotid,value);
    if (rv < 0)
      result = KNO_ERROR_VALUE;
    else if (rv)
      result = KNO_TRUE;
    else result = KNO_FALSE;
    break;}
  case KNO_STORE_OPCODE:
    if (KNO_VOIDP(value))
      result = kno_err(kno_TooFewArgs,"handle_table_opcode/store",NULL,expr);
    else {
      int rv = kno_store(subject,slotid,value);
      if (rv < 0)
        result = KNO_ERROR_VALUE;
      else if (rv)
        result = KNO_TRUE;
      else result = KNO_FALSE;}
    break;
  case KNO_ASSERT_OPCODE:
    if (KNO_VOIDP(value))
      result = kno_err(kno_TooFewArgs,"handle_table_opcode",NULL,expr);
    else result = kno_assert(subject,slotid,value);
    break;
  case KNO_RETRACT_OPCODE:
    result = kno_retract(subject,slotid,value);
    break;
  default:
    result = kno_err("BadOpcode","handle_table_opcode",NULL,expr);}
  kno_decref(subject); kno_decref(slotid); kno_decref(value);
  return result;
}

/* Opcode dispatch */

static lispval handle_special_opcode(lispval opcode,lispval args,lispval expr,
                                     kno_lexenv env,
                                     kno_stack _stack,
                                     int tail)
{
  switch (opcode) {
  case KNO_QUOTE_OPCODE: {
    lispval arg = pop_arg(args);
    if (KNO_EXPECT_FALSE(KNO_VOIDP(arg)))
      return kno_err(kno_SyntaxError,"opcode_eval",NULL,expr);
    else if (KNO_CONSP(arg))
      return kno_incref(arg);
    else return arg;}
  case KNO_SYMREF_OPCODE: {
    lispval refenv=pop_arg(args);
    lispval sym=pop_arg(args);
    if (KNO_EXPECT_FALSE(!(KNO_SYMBOLP(sym))))
      return kno_err(kno_SyntaxError,"KNO_SYMREF_OPCODE/badsym",NULL,expr);
    if (HASHTABLEP(refenv))
      return kno_hashtable_get((kno_hashtable)refenv,sym,KNO_UNBOUND);
    else if (KNO_LEXENVP(refenv))
      return kno_symeval(sym,(kno_lexenv)refenv);
    else if (TABLEP(refenv))
      return kno_get(refenv,sym,KNO_UNBOUND);
    else return kno_err(kno_SyntaxError,"KNO_SYMREF_OPCODE/badenv",NULL,expr);}
  case KNO_VOID_OPCODE: {
    return VOID;}
  case KNO_RESET_ENV_OPCODE: {
    reset_env_op(env);
    return VOID;}
  case KNO_BEGIN_OPCODE:
    return eval_body(KNO_CDR(expr),env,_stack,"#BEGINOP",NULL,tail);
  case KNO_UNTIL_OPCODE:
    return until_opcode(expr,env,_stack);

  case KNO_BRANCH_OPCODE: {
    lispval test_expr = pop_arg(args);
    if (VOIDP(test_expr))
      return kno_err(kno_SyntaxError,"KNO_BRANCH_OPCODE",NULL,expr);
    lispval test_val = _kno_fast_eval(test_expr,env,_stack,0);
    if (KNO_ABORTED(test_val))
      return test_val;
    else if (KNO_FALSEP(test_val)) { /* (  || (KNO_EMPTYP(test_val)) ) */
      pop_arg(args);
      lispval else_expr = pop_arg(args);
      return _kno_fast_eval(else_expr,env,_stack,tail);}
    else {
      lispval then = pop_arg(args);
      U8_MAYBE_UNUSED lispval ignore = pop_arg(args);
      kno_decref(test_val);
      return _kno_fast_eval(then,env,_stack,tail);}}

  case KNO_BIND_OPCODE: {
    lispval vars=pop_arg(args);
    lispval inits=pop_arg(args);
    lispval body=pop_arg(args);
    if (!(KNO_VECTORP(vars)))
      return kno_err(kno_SyntaxError,"BINDOP",NULL,expr);
    else if (KNO_VECTORP(inits))
      return vector_bindop(opcode,_stack,env,vars,inits,body,tail);
    else return bindop(opcode,_stack,env,vars,inits,body,tail);}

  case KNO_ASSIGN_OPCODE: {
    lispval var = pop_arg(args);
    lispval combiner = pop_arg(args);
    lispval val_expr = pop_arg(args);
    return assignop(_stack,env,var,val_expr,combiner);}

  case KNO_XREF_OPCODE: {
    lispval off_arg = pop_arg(args);
    lispval type_arg = pop_arg(args);
    lispval obj_expr = pop_arg(args);
    if ( (FIXNUMP(off_arg)) && (! (VOIDP(obj_expr)) ) ) {
      lispval obj_arg=fast_eval(obj_expr,env);
      return xref_opcode(kno_simplify_choice(obj_arg),
                         FIX2INT(off_arg),
                         type_arg);}
    kno_seterr(kno_SyntaxError,"KNO_XREF_OPCODE",NULL,expr);
    return KNO_ERROR_VALUE;}

  case KNO_XPRED_OPCODE: {
    lispval type_arg = pop_arg(args);
    lispval obj_expr = pop_arg(args);
    if (KNO_EXPECT_FALSE(VOIDP(obj_expr))) {
      kno_seterr(kno_SyntaxError,"KNO_XREF_OPCODE",NULL,expr);
      return KNO_ERROR_VALUE;}
    lispval obj_arg=fast_eval(obj_expr,env);
    if (KNO_ABORTED(obj_arg)) return obj_arg;
    else if (KNO_EMPTYP(obj_arg)) return KNO_EMPTY;
    else if ( (KNO_COMPOUNDP(obj_arg)) || (KNO_AMBIGP(obj_arg)) )
      return xpred_opcode(kno_simplify_choice(obj_arg),type_arg);
    else {
      kno_decref(obj_arg);
      return KNO_FALSE;}}

  case KNO_NOT_OPCODE: {
    lispval arg_val = _kno_fast_eval(pop_arg(args),env,_stack,0);
    if (FALSEP(arg_val))
      return KNO_TRUE;
    else {
      kno_decref(arg_val);
      return KNO_FALSE;}}

  case KNO_BREAK_OPCODE:
    return KNO_BREAK;

  case KNO_TRY_OPCODE: return try_op(args,env,_stack,tail);
  case KNO_AND_OPCODE: return and_op(args,env,_stack,tail);
  case KNO_OR_OPCODE:  return or_op(args,env,_stack,tail);

  case KNO_INTERSECT_OPCODE: return intersect_op(args,env,_stack);
  case KNO_UNION_OPCODE: return union_op(args,env,_stack);
  case KNO_DIFFERENCE_OPCODE: return difference_op(args,env,_stack);

  default:
    return kno_err("BadOpcode","handle_special_opcode",NULL,expr);
  }
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
