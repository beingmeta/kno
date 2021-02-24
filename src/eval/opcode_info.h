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
  set_opcode_name(KNO_TRY_OPCODE,"OP_TRY");
  set_opcode_name(KNO_AND_OPCODE,"OP_AND");
  set_opcode_name(KNO_OR_OPCODE,"OP_OR");
  set_opcode_name(KNO_SOURCEREF_OPCODE,"OP_SOURCEREF");
  set_opcode_name(KNO_RESET_ENV_OPCODE,"OP_RESET_ENV");
  set_opcode_info(KNO_XREF_OPCODE,"OP_XREF",KNO_CALL_NDCALL);
  set_opcode_name(KNO_XPRED_OPCODE,"OP_XPRED");
  set_opcode_name(KNO_BREAK_OPCODE,"OP_BREAK");
  set_opcode_info(KNO_UNION_OPCODE,"OP_UNION",KNO_CALL_NDCALL);
  set_opcode_info(KNO_INTERSECT_OPCODE,"OP_INTERSECTION",KNO_CALL_XITER);
  set_opcode_info(KNO_DIFFERENCE_OPCODE,"OP_DIFFERENCE",KNO_CALL_NDCALL);
  set_opcode_name(KNO_EVALFN_OPCODE,"OP_EVALFN");
  set_opcode_name(KNO_APPLY_OPCODE,"OP_APPLY");
  set_opcode_name(KNO_ISA_OPCODE,"OP_ISA");
  set_opcode_name(KNO_ISA_ALL_OPCODE,"OP_ISA_ALL");
  set_opcode_name(KNO_ISA_ANY_OPCODE,"OP_ISA_ANY");
  set_opcode_name(KNO_PICK_TYPE_OPCODE,"OP_PICK_TYPE");
  set_opcode_name(KNO_SKIP_TYPE_OPCODE,"OP_SKIP_TYPE");
  set_opcode_name(KNO_CHECK_TYPE_OPCODE,"OP_CHECK_TYPE");

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
  set_opcode_info(KNO_PICKONE_OPCODE,"OP_PICKONE",KNO_CALL_XITER);
  set_opcode_info(KNO_IFEXISTS_OPCODE,"OP_IFEXISTS",KNO_CALL_NDCALL);
  set_opcode_info(KNO_SOMETRUE_OPCODE,"OP_SOMETRUE",KNO_CALL_NDCALL);


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
  set_opcode_info(KNO_CHOICEREF_OPCODE,"OP_CHOICEREF",KNO_CALL_NDCALL);

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

  set_opcode_info(KNO_ASSERT_OPCODE,"OP_ASSERT",KNO_CALL_NDCALL);
  set_opcode_info(KNO_RETRACT_OPCODE,"OP_RETRACT",KNO_CALL_NDCALL);
  set_opcode_info(KNO_GET_OPCODE,"OP_GET",KNO_CALL_NDCALL);
  set_opcode_info(KNO_TEST_OPCODE,"OP_TEST",KNO_CALL_NDCALL);
  set_opcode_info(KNO_ADD_OPCODE,"OP_ADD",KNO_CALL_NDCALL);
  set_opcode_info(KNO_DROP_OPCODE,"OP_DROP",KNO_CALL_NDCALL);
  set_opcode_info(KNO_PRIMGET_OPCODE,"OP_PGET",KNO_CALL_NDCALL);
  set_opcode_info(KNO_PRIMTEST_OPCODE,"OP_PTEST",KNO_CALL_NDCALL);
  set_opcode_info(KNO_STORE_OPCODE,"OP_PSTORE",KNO_CALL_NDCALL);

  set_opcode_name(KNO_CALL0_OPCODE,"OP_CALL0");
  set_opcode_name(KNO_CALL1_OPCODE,"OP_CALL1");
  set_opcode_name(KNO_CALL2_OPCODE,"OP_CALL2");
  set_opcode_name(KNO_CALL3_OPCODE,"OP_CALL3");
  set_opcode_name(KNO_CALL4_OPCODE,"OP_CALL4");
  set_opcode_name(KNO_CALL5_OPCODE,"OP_CALL5");
  set_opcode_name(KNO_CALL6_OPCODE,"OP_CALL6");
  set_opcode_name(KNO_CALL7_OPCODE,"OP_CALL7");
  set_opcode_name(KNO_CALL8_OPCODE,"OP_CALL8");
  set_opcode_name(KNO_CALL9_OPCODE,"OP_CALL9");
  set_opcode_name(KNO_CALL10_OPCODE,"OP_CALL10");
  set_opcode_name(KNO_CALL11_OPCODE,"OP_CALL11");
  set_opcode_name(KNO_CALL12_OPCODE,"OP_CALL12");
  set_opcode_name(KNO_CALL13_OPCODE,"OP_CALL13");
  set_opcode_name(KNO_CALL14_OPCODE,"OP_CALL14");
  set_opcode_name(KNO_CALL15_OPCODE,"OP_CALL15");
  set_opcode_name(KNO_CALLN_OPCODE,"OP_CALLN");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
