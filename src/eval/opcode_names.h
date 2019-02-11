static void init_opcode_names()
{
  set_opcode_name(FD_BRANCH_OPCODE,"OP_BRANCH");
  set_opcode_name(FD_NOT_OPCODE,"OP_NOT");
  set_opcode_name(FD_UNTIL_OPCODE,"OP_UNTIL");
  set_opcode_name(FD_BEGIN_OPCODE,"OP_BEGIN");
  set_opcode_name(FD_QUOTE_OPCODE,"OP_QUOTE");
  set_opcode_name(FD_ASSIGN_OPCODE,"OP_ASSIGN");
  set_opcode_name(FD_SYMREF_OPCODE,"OP_SYMREF");
  set_opcode_name(FD_BIND_OPCODE,"OP_BIND");
  set_opcode_name(FD_VOID_OPCODE,"OP_VOID");
  set_opcode_name(FD_AND_OPCODE,"OP_AND");
  set_opcode_name(FD_OR_OPCODE,"OP_OR");
  set_opcode_name(FD_TRY_OPCODE,"OP_TRY");
  set_opcode_name(FD_CHOICEREF_OPCODE,"OP_CHOICEREF");
  set_opcode_name(FD_FIXCHOICE_OPCODE,"OP_FIXCHOICE");

  set_opcode_name(FD_SOURCEREF_OPCODE,"OP_SOURCEREF");
  set_opcode_name(FD_RESET_ENV_OPCODE,"OP_RESET_ENV");

  set_opcode_name(FD_AMBIGP_OPCODE,"OP_AMBIGP");
  set_opcode_name(FD_SINGLETONP_OPCODE,"OP_SINGLETONP");
  set_opcode_name(FD_FAILP_OPCODE,"OP_FAILP");
  set_opcode_name(FD_EXISTSP_OPCODE,"OP_EXISTSP");
  set_opcode_name(FD_SINGLETON_OPCODE,"OP_SINGLETON");
  set_opcode_name(FD_CAR_OPCODE,"OP_CAR");
  set_opcode_name(FD_CDR_OPCODE,"OP_CDR");
  set_opcode_name(FD_LENGTH_OPCODE,"OP_LENGTH");
  set_opcode_name(FD_QCHOICE_OPCODE,"OP_QCHOICE");
  set_opcode_name(FD_CHOICE_SIZE_OPCODE,"OP_CHOICESIZE");
  set_opcode_name(FD_PICKOIDS_OPCODE,"OP_PICKOIDS");
  set_opcode_name(FD_PICKSTRINGS_OPCODE,"OP_PICKSTRINGS");
  set_opcode_name(FD_PICKONE_OPCODE,"OP_PICKONE");
  set_opcode_name(FD_IFEXISTS_OPCODE,"OP_IFEXISTS");
  set_opcode_name(FD_MINUS1_OPCODE,"OP_MINUS1");
  set_opcode_name(FD_PLUS1_OPCODE,"OP_PLUS1");
  set_opcode_name(FD_NUMBERP_OPCODE,"OP_NUMBERP");
  set_opcode_name(FD_ZEROP_OPCODE,"OP_ZEROP");
  set_opcode_name(FD_VECTORP_OPCODE,"OP_VECTORP");
  set_opcode_name(FD_PAIRP_OPCODE,"OP_PAIRP");
  set_opcode_name(FD_EMPTY_LISTP_OPCODE,"OP_NILP");
  set_opcode_name(FD_STRINGP_OPCODE,"OP_STRINGP");
  set_opcode_name(FD_OIDP_OPCODE,"OP_OIDP");
  set_opcode_name(FD_SYMBOLP_OPCODE,"OP_SYMBOLP");
  set_opcode_name(FD_FIRST_OPCODE,"OP_FIRST");
  set_opcode_name(FD_SECOND_OPCODE,"OP_SECOND");
  set_opcode_name(FD_THIRD_OPCODE,"OP_THIRD");
  set_opcode_name(FD_CADR_OPCODE,"OP_CADR");
  set_opcode_name(FD_CDDR_OPCODE,"OP_CDDR");
  set_opcode_name(FD_CADDR_OPCODE,"OP_CADDR");
  set_opcode_name(FD_CDDDR_OPCODE,"OP_CDDDR");
  set_opcode_name(FD_TONUMBER_OPCODE,"OP_2NUMBER");
  set_opcode_name(FD_NUMEQ_OPCODE,"OP_NUMEQ");
  set_opcode_name(FD_GT_OPCODE,"OP_GT");
  set_opcode_name(FD_GTE_OPCODE,"OP_GTE");
  set_opcode_name(FD_LT_OPCODE,"OP_LT");
  set_opcode_name(FD_LTE_OPCODE,"OP_LTE");
  set_opcode_name(FD_PLUS_OPCODE,"OP_PLUS");
  set_opcode_name(FD_MINUS_OPCODE,"OP_MINUS");
  set_opcode_name(FD_TIMES_OPCODE,"OP_MULT");
  set_opcode_name(FD_FLODIV_OPCODE,"OP_FLODIV");
  set_opcode_name(FD_IDENTICAL_OPCODE,"OP_IDENTICALP");
  set_opcode_name(FD_OVERLAPS_OPCODE,"OP_OVERLAPSP");
  set_opcode_name(FD_CONTAINSP_OPCODE,"OP_CONTAINSP");
  set_opcode_name(FD_UNION_OPCODE,"OP_UNION");
  set_opcode_name(FD_INTERSECT_OPCODE,"OP_INTERSECTION");
  set_opcode_name(FD_DIFFERENCE_OPCODE,"OP_DIFFERENCE");
  set_opcode_name(FD_EQ_OPCODE,"OP_EQP");
  set_opcode_name(FD_EQV_OPCODE,"OP_EQVP");
  set_opcode_name(FD_EQUAL_OPCODE,"OP_EQUALP");
  set_opcode_name(FD_ELT_OPCODE,"OP_SEQELT");
  set_opcode_name(FD_ASSERT_OPCODE,"OP_ASSERT");
  set_opcode_name(FD_RETRACT_OPCODE,"OP_RETRACT");
  set_opcode_name(FD_GET_OPCODE,"OP_FGET");
  set_opcode_name(FD_TEST_OPCODE,"OP_FTEST");
  set_opcode_name(FD_ADD_OPCODE,"OP_ADD");
  set_opcode_name(FD_DROP_OPCODE,"OP_DROP");
  set_opcode_name(FD_XREF_OPCODE,"OP_XREF");
  set_opcode_name(FD_XPRED_OPCODE,"OP_XPRED");
  set_opcode_name(FD_BREAK_OPCODE,"OP_BREAK");
  set_opcode_name(FD_PRIMGET_OPCODE,"OP_PGET");
  set_opcode_name(FD_PRIMTEST_OPCODE,"OP_PTEST");
  set_opcode_name(FD_STORE_OPCODE,"OP_PSTORE");

  set_opcode_name(FD_PICKNUMS_OPCODE,"OP_PICKNUMS");
  set_opcode_name(FD_PICKMAPS_OPCODE,"OP_PICKMAPS");
  set_opcode_name(FD_SOMETRUE_OPCODE,"OP_SOMETRUE");
  set_opcode_name(FD_GETKEYS_OPCODE,"OP_GETKEYS");
  set_opcode_name(FD_GETVALUES_OPCODE,"OP_GETVALUES");
  set_opcode_name(FD_GETASSOCS_OPCODE,"OP_GETASSOCS");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
