static void init_opcode_names()
{
  set_opcode_name(KNO_BRANCH_OPCODE,"OP_BRANCH");
  set_opcode_name(KNO_NOT_OPCODE,"OP_NOT");
  set_opcode_name(KNO_UNTIL_OPCODE,"OP_UNTIL");
  set_opcode_name(KNO_BEGIN_OPCODE,"OP_BEGIN");
  set_opcode_name(KNO_QUOTE_OPCODE,"OP_QUOTE");
  set_opcode_name(KNO_ASSIGN_OPCODE,"OP_ASSIGN");
  set_opcode_name(KNO_SYMREF_OPCODE,"OP_SYMREF");
  set_opcode_name(KNO_BIND_OPCODE,"OP_BIND");
  set_opcode_name(KNO_VOID_OPCODE,"OP_VOID");
  set_opcode_name(KNO_AND_OPCODE,"OP_AND");
  set_opcode_name(KNO_OR_OPCODE,"OP_OR");
  set_opcode_name(KNO_TRY_OPCODE,"OP_TRY");
  set_opcode_name(KNO_CHOICEREF_OPCODE,"OP_CHOICEREF");

  set_opcode_name(KNO_SOURCEREF_OPCODE,"OP_SOURCEREF");
  set_opcode_name(KNO_RESET_ENV_OPCODE,"OP_RESET_ENV");

  /* ND ops */
  set_opcode_info(KNO_AMBIGP_OPCODE,"OP_AMBIGP",KNO_CALL_NDCALL);
  set_opcode_info(KNO_SINGLETONP_OPCODE,"OP_SINGLETONP",KNO_CALL_NDCALL);
  set_opcode_info(KNO_FAILP_OPCODE,"OP_FAILP",KNO_CALL_NDCALL);
  set_opcode_info(KNO_EXISTSP_OPCODE,"OP_EXISTSP",KNO_CALL_NDCALL);
  set_opcode_info(KNO_SINGLETON_OPCODE,"OP_SINGLETON",KNO_CALL_NDCALL);
  set_opcode_info(KNO_CAR_OPCODE,"OP_CAR",KNO_CALL_XITER);
  set_opcode_info(KNO_CDR_OPCODE,"OP_CDR",KNO_CALL_XITER);
  set_opcode_info(KNO_LENGTH_OPCODE,"OP_LENGTH",KNO_CALL_XITER);
  set_opcode_info(KNO_QCHOICE_OPCODE,"OP_QCHOICE",KNO_CALL_NDCALL);
  set_opcode_info(KNO_CHOICE_SIZE_OPCODE,"OP_CHOICESIZE",KNO_CALL_NDCALL);
  set_opcode_info(KNO_PICKOIDS_OPCODE,"OP_PICKOIDS",KNO_CALL_XITER);
  set_opcode_info(KNO_PICKSTRINGS_OPCODE,"OP_PICKSTRINGS",KNO_CALL_XITER);
  set_opcode_info(KNO_PICKONE_OPCODE,"OP_PICKONE",KNO_CALL_XITER);
  set_opcode_info(KNO_FIXCHOICE_OPCODE,"OP_FIXCHOICE",KNO_CALL_XITER);
  set_opcode_info(KNO_IFEXISTS_OPCODE,"OP_IFEXISTS",KNO_CALL_NDCALL);
  set_opcode_info(KNO_PICKNUMS_OPCODE,"OP_PICKNUMS",KNO_CALL_XITER);
  set_opcode_info(KNO_PICKMAPS_OPCODE,"OP_PICKMAPS",KNO_CALL_XITER);
  set_opcode_info(KNO_SOMETRUE_OPCODE,"OP_SOMETRUE",KNO_CALL_NDCALL);
  set_opcode_info(KNO_UNION_OPCODE,"OP_UNION",KNO_CALL_NDCALL);
  set_opcode_info(KNO_INTERSECT_OPCODE,"OP_INTERSECTION",KNO_CALL_XITER);
  set_opcode_info(KNO_DIFFERENCE_OPCODE,"OP_DIFFERENCE",KNO_CALL_NDCALL);


  set_opcode_name(KNO_MINUS1_OPCODE,"OP_MINUS1");
  set_opcode_name(KNO_PLUS1_OPCODE,"OP_PLUS1");
  set_opcode_name(KNO_NUMBERP_OPCODE,"OP_NUMBERP");
  set_opcode_name(KNO_ZEROP_OPCODE,"OP_ZEROP");
  set_opcode_name(KNO_VECTORP_OPCODE,"OP_VECTORP");
  set_opcode_name(KNO_PAIRP_OPCODE,"OP_PAIRP");
  set_opcode_name(KNO_EMPTY_LISTP_OPCODE,"OP_NILP");
  set_opcode_name(KNO_STRINGP_OPCODE,"OP_STRINGP");
  set_opcode_name(KNO_OIDP_OPCODE,"OP_OIDP");
  set_opcode_name(KNO_SYMBOLP_OPCODE,"OP_SYMBOLP");
  set_opcode_name(KNO_FIXNUMP_OPCODE,"OP_FIXNUMP");
  set_opcode_name(KNO_FLONUMP_OPCODE,"OP_FLONUMP");
  set_opcode_name(KNO_SEQUENCEP_OPCODE,"OP_SEQUENCEP");
  set_opcode_name(KNO_TABLEP_OPCODE,"OP_TABLEP");

  set_opcode_name(KNO_FIRST_OPCODE,"OP_FIRST");
  set_opcode_name(KNO_SECOND_OPCODE,"OP_SECOND");
  set_opcode_name(KNO_THIRD_OPCODE,"OP_THIRD");
  set_opcode_name(KNO_CADR_OPCODE,"OP_CADR");
  set_opcode_name(KNO_CDDR_OPCODE,"OP_CDDR");
  set_opcode_name(KNO_CADDR_OPCODE,"OP_CADDR");
  set_opcode_name(KNO_CDDDR_OPCODE,"OP_CDDDR");
  set_opcode_name(KNO_TONUMBER_OPCODE,"OP_2NUMBER");

  set_opcode_name(KNO_ELTS_OPCODE,"OP_ELTS");
  set_opcode_name(KNO_GETKEYS_OPCODE,"OP_GETKEYS");
  set_opcode_name(KNO_GETVALUES_OPCODE,"OP_GETVALUES");
  set_opcode_name(KNO_GETASSOCS_OPCODE,"OP_GETASSOCS");

  set_opcode_name(KNO_EQ_OPCODE,"OP_EQP");
  set_opcode_name(KNO_EQV_OPCODE,"OP_EQVP");
  set_opcode_name(KNO_EQUAL_OPCODE,"OP_EQUALP");
  set_opcode_name(KNO_ELT_OPCODE,"OP_SEQELT");
  set_opcode_name(KNO_CONS_OPCODE,"OP_CONSPAIR");

  set_opcode_info(KNO_IDENTICAL_OPCODE,"OP_IDENTICALP",KNO_CALL_NDCALL);
  set_opcode_info(KNO_OVERLAPS_OPCODE,"OP_OVERLAPSP",KNO_CALL_NDCALL);
  set_opcode_info(KNO_CONTAINSP_OPCODE,"OP_CONTAINSP",KNO_CALL_NDCALL);

  set_opcode_name(KNO_NUMEQ_OPCODE,"OP_NUMEQ");
  set_opcode_name(KNO_GT_OPCODE,"OP_GT");
  set_opcode_name(KNO_GTE_OPCODE,"OP_GTE");
  set_opcode_name(KNO_LT_OPCODE,"OP_LT");
  set_opcode_name(KNO_LTE_OPCODE,"OP_LTE");
  set_opcode_name(KNO_PLUS_OPCODE,"OP_PLUS");
  set_opcode_name(KNO_MINUS_OPCODE,"OP_MINUS");
  set_opcode_name(KNO_TIMES_OPCODE,"OP_MULT");
  set_opcode_name(KNO_FLODIV_OPCODE,"OP_FLODIV");
  set_opcode_name(KNO_DIV_OPCODE,"OP_DIVIDE");

  set_opcode_name(KNO_ASSERT_OPCODE,"OP_ASSERT");
  set_opcode_name(KNO_RETRACT_OPCODE,"OP_RETRACT");
  set_opcode_name(KNO_GET_OPCODE,"OP_FGET");
  set_opcode_name(KNO_TEST_OPCODE,"OP_FTEST");
  set_opcode_name(KNO_ADD_OPCODE,"OP_ADD");
  set_opcode_name(KNO_DROP_OPCODE,"OP_DROP");
  set_opcode_name(KNO_XREF_OPCODE,"OP_XREF");
  set_opcode_name(KNO_XPRED_OPCODE,"OP_XPRED");
  set_opcode_name(KNO_BREAK_OPCODE,"OP_BREAK");
  set_opcode_name(KNO_PRIMGET_OPCODE,"OP_PGET");
  set_opcode_name(KNO_PRIMTEST_OPCODE,"OP_PTEST");
  set_opcode_name(KNO_STORE_OPCODE,"OP_PSTORE");

  set_opcode_name(KNO_APPLY0_OPCODE,"OP_APPLY0");
  set_opcode_name(KNO_APPLY1_OPCODE,"OP_APPLY1");
  set_opcode_name(KNO_APPLY2_OPCODE,"OP_APPLY2");
  set_opcode_name(KNO_APPLY3_OPCODE,"OP_APPLY3");
  set_opcode_name(KNO_APPLY4_OPCODE,"OP_APPLY4");
  set_opcode_name(KNO_APPLY5_OPCODE,"OP_APPLY5");
  set_opcode_name(KNO_APPLY6_OPCODE,"OP_APPLY6");
  set_opcode_name(KNO_APPLY7_OPCODE,"OP_APPLY7");
  set_opcode_name(KNO_APPLY8_OPCODE,"OP_APPLY8");
  set_opcode_name(KNO_APPLY9_OPCODE,"OP_APPLY9");
  set_opcode_name(KNO_APPLY10_OPCODE,"OP_APPLY10");
  set_opcode_name(KNO_APPLY11_OPCODE,"OP_APPLY11");
  set_opcode_name(KNO_APPLY12_OPCODE,"OP_APPLY12");
  set_opcode_name(KNO_APPLY13_OPCODE,"OP_APPLY13");
  set_opcode_name(KNO_APPLY14_OPCODE,"OP_APPLY14");
  set_opcode_name(KNO_APPLY15_OPCODE,"OP_APPLY15");
  set_opcode_name(KNO_APPLY_N_OPCODE,"OP_APPLY_N");

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
