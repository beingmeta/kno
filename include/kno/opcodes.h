/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_OPCODES_H
#define KNO_OPCODES_H 1
#ifndef KNO_OPCODES_H_INFO
#define KNO_OPCODES_H_INFO "include/kno/opcodes.h"
#endif

KNO_EXPORT lispval kno_opcode_dispatch
(lispval opcode,lispval expr,kno_lexenv env,
 kno_stack caller,
 int tail);

/* Opcodes */

KNO_EXPORT u8_string kno_opcode_names[];
KNO_EXPORT int kno_opcodes_length;

#define KNO_SPECIAL_OPCODE(x)  KNO_OPCODE(x+0x00)
#define KNO_SPECIAL_OPCODEP(x) ((x>=KNO_OPCODE(0x00))&&(x<KNO_OPCODE(0x100)))
#define KNO_BRANCH_OPCODE      KNO_SPECIAL_OPCODE(0x01)
#define KNO_NOT_OPCODE         KNO_SPECIAL_OPCODE(0x02)
#define KNO_UNTIL_OPCODE       KNO_SPECIAL_OPCODE(0x03)
#define KNO_BEGIN_OPCODE       KNO_SPECIAL_OPCODE(0x04)
#define KNO_QUOTE_OPCODE       KNO_SPECIAL_OPCODE(0x05)
#define KNO_ASSIGN_OPCODE      KNO_SPECIAL_OPCODE(0x06)
#define KNO_SYMREF_OPCODE      KNO_SPECIAL_OPCODE(0x07)
#define KNO_BIND_OPCODE        KNO_SPECIAL_OPCODE(0x08)
#define KNO_VOID_OPCODE        KNO_SPECIAL_OPCODE(0x09)
#define KNO_TRY_OPCODE         KNO_SPECIAL_OPCODE(0x0A)
#define KNO_AND_OPCODE         KNO_SPECIAL_OPCODE(0x0B)
#define KNO_OR_OPCODE          KNO_SPECIAL_OPCODE(0x0C)
#define KNO_SOURCEREF_OPCODE   KNO_SPECIAL_OPCODE(0x0D)
#define KNO_RESET_ENV_OPCODE   KNO_SPECIAL_OPCODE(0x0E)
#define KNO_XREF_OPCODE        KNO_SPECIAL_OPCODE(0x0F)
#define KNO_XPRED_OPCODE       KNO_SPECIAL_OPCODE(0x10)
#define KNO_BREAK_OPCODE       KNO_SPECIAL_OPCODE(0x11)
#define KNO_UNION_OPCODE       KNO_SPECIAL_OPCODE(0x12)
#define KNO_INTERSECT_OPCODE   KNO_SPECIAL_OPCODE(0x13)
#define KNO_DIFFERENCE_OPCODE  KNO_SPECIAL_OPCODE(0x14)

/* Unary primitives which handle their own non-determinism. */
#define KNO_ND1_OPCODE(x)      KNO_OPCODE(0x100+x)
#define KNO_ND1_OPCODEP(x)     ((x>=KNO_OPCODE(0x100))&&(x<KNO_OPCODE(0x200)))
#define KNO_AMBIGP_OPCODE      KNO_ND1_OPCODE(0x01)
#define KNO_SINGLETONP_OPCODE  KNO_ND1_OPCODE(0x02)
#define KNO_FAILP_OPCODE       KNO_ND1_OPCODE(0x03)
#define KNO_EXISTSP_OPCODE     KNO_ND1_OPCODE(0x04)
#define KNO_SINGLETON_OPCODE   KNO_ND1_OPCODE(0x05)
#define KNO_CAR_OPCODE         KNO_ND1_OPCODE(0x06)
#define KNO_CDR_OPCODE         KNO_ND1_OPCODE(0x07)
#define KNO_LENGTH_OPCODE      KNO_ND1_OPCODE(0x08)
#define KNO_QCHOICE_OPCODE     KNO_ND1_OPCODE(0x09)
#define KNO_CHOICE_SIZE_OPCODE KNO_ND1_OPCODE(0x0a)
#define KNO_PICKOIDS_OPCODE    KNO_ND1_OPCODE(0x0b)
#define KNO_PICKSTRINGS_OPCODE KNO_ND1_OPCODE(0x0c)
#define KNO_PICKONE_OPCODE     KNO_ND1_OPCODE(0x0d)
#define KNO_IFEXISTS_OPCODE    KNO_ND1_OPCODE(0x0e)
#define KNO_FIXCHOICE_OPCODE   KNO_ND1_OPCODE(0x0f)
#define KNO_PICKNUMS_OPCODE    KNO_ND1_OPCODE(0x10)
#define KNO_PICKMAPS_OPCODE    KNO_ND1_OPCODE(0x11)
#define KNO_SOMETRUE_OPCODE    KNO_ND1_OPCODE(0x12)

/* Unary primitives which don't handle their own non-determinism. */
#define KNO_D1_OPCODE(x)       KNO_OPCODE(0x200+x)
#define KNO_D1_OPCODEP(x)      (x>KNO_OPCODE(0x200))&&(x<KNO_OPCODE(0x300))
#define KNO_MINUS1_OPCODE      KNO_D1_OPCODE(0x01)
#define KNO_PLUS1_OPCODE       KNO_D1_OPCODE(0x02)
#define KNO_NUMBERP_OPCODE     KNO_D1_OPCODE(0x03)
#define KNO_ZEROP_OPCODE       KNO_D1_OPCODE(0x04)
#define KNO_VECTORP_OPCODE     KNO_D1_OPCODE(0x05)
#define KNO_PAIRP_OPCODE       KNO_D1_OPCODE(0x06)
#define KNO_EMPTY_LISTP_OPCODE KNO_D1_OPCODE(0x07)
#define KNO_STRINGP_OPCODE     KNO_D1_OPCODE(0x08)
#define KNO_OIDP_OPCODE        KNO_D1_OPCODE(0x09)
#define KNO_SYMBOLP_OPCODE     KNO_D1_OPCODE(0x0a)
#define KNO_FIXNUMP_OPCODE     KNO_D1_OPCODE(0x0b)
#define KNO_FLONUMP_OPCODE     KNO_D1_OPCODE(0x0c)
#define KNO_SEQUENCEP_OPCODE   KNO_D1_OPCODE(0x0d)
#define KNO_TABLEP_OPCODE      KNO_D1_OPCODE(0x0e)
#define KNO_FIRST_OPCODE       KNO_D1_OPCODE(0x0f)
#define KNO_SECOND_OPCODE      KNO_D1_OPCODE(0x10)
#define KNO_THIRD_OPCODE       KNO_D1_OPCODE(0x11)
#define KNO_CADR_OPCODE        KNO_D1_OPCODE(0x12)
#define KNO_CDDR_OPCODE        KNO_D1_OPCODE(0x13)
#define KNO_CADDR_OPCODE       KNO_D1_OPCODE(0x14)
#define KNO_CDDDR_OPCODE       KNO_D1_OPCODE(0x15)
#define KNO_TONUMBER_OPCODE    KNO_D1_OPCODE(0x16)
#define KNO_ELTS_OPCODE        KNO_D1_OPCODE(0x17)
#define KNO_GETKEYS_OPCODE     KNO_D1_OPCODE(0x18)
#define KNO_GETVALUES_OPCODE   KNO_D1_OPCODE(0x19)
#define KNO_GETASSOCS_OPCODE   KNO_D1_OPCODE(0x1a)

#define KNO_D2_OPCODE(x)      KNO_OPCODE(0x400+x)
#define KNO_D2_OPCODEP(x)     ((x>=KNO_OPCODE(0x400))&&(x<KNO_OPCODE(0x500)))
#define KNO_EQ_OPCODE         KNO_D2_OPCODE(0x01)
#define KNO_EQV_OPCODE        KNO_D2_OPCODE(0x02)
#define KNO_EQUAL_OPCODE      KNO_D2_OPCODE(0x03)
#define KNO_ELT_OPCODE        KNO_D2_OPCODE(0x04)
#define KNO_CONS_OPCODE       KNO_D2_OPCODE(0x05)

#define KNO_ND2_OPCODE(x)     KNO_OPCODE(0x500+x)
#define KNO_ND2_OPCODEP(x)    ((x>=KNO_OPCODE(0x500))&&(x<KNO_OPCODE(0x600)))
#define KNO_IDENTICAL_OPCODE  KNO_ND2_OPCODE(0x01)
#define KNO_OVERLAPS_OPCODE   KNO_ND2_OPCODE(0x02)
#define KNO_CONTAINSP_OPCODE  KNO_ND2_OPCODE(0x03)
/*
#define KNO_UNION_OPCODE      KNO_ND2_OPCODE(0x04)
#define KNO_INTERSECT_OPCODE  KNO_ND2_OPCODE(0x05)
#define KNO_DIFFERENCE_OPCODE KNO_ND2_OPCODE(0x06)
*/
#define KNO_CHOICEREF_OPCODE  KNO_ND2_OPCODE(0x07)

/* Arithmetic primitives with two arguments */
#define KNO_NUMERIC_OPCODE(x) KNO_OPCODE(0x300+x)
#define KNO_NUMERIC_OPCODEP(x) (x>=KNO_OPCODE(0x300))&&(x<KNO_OPCODE(0x400))
#define KNO_NUMEQ_OPCODE      KNO_NUMERIC_OPCODE(0x01)
#define KNO_GT_OPCODE         KNO_NUMERIC_OPCODE(0x02)
#define KNO_GTE_OPCODE        KNO_NUMERIC_OPCODE(0x03)
#define KNO_LT_OPCODE         KNO_NUMERIC_OPCODE(0x04)
#define KNO_LTE_OPCODE        KNO_NUMERIC_OPCODE(0x05)
#define KNO_PLUS_OPCODE       KNO_NUMERIC_OPCODE(0x06)
#define KNO_MINUS_OPCODE      KNO_NUMERIC_OPCODE(0x07)
#define KNO_TIMES_OPCODE      KNO_NUMERIC_OPCODE(0x08)
#define KNO_FLODIV_OPCODE     KNO_NUMERIC_OPCODE(0x09)
#define KNO_DIV_OPCODE        KNO_NUMERIC_OPCODE(0x0a)

#define KNO_TABLE_OPCODE(x)     KNO_OPCODE(0x700+x)
#define KNO_TABLE_OPCODEP(x)    (((x)>=KNO_OPCODE(0x600))&&((x)<KNO_OPCODE(0x800)))
/* Other primitives with more than two arguments */
#define KNO_GET_OPCODE        KNO_TABLE_OPCODE(0x01)
#define KNO_ASSERT_OPCODE     KNO_TABLE_OPCODE(0x02)
#define KNO_RETRACT_OPCODE    KNO_TABLE_OPCODE(0x03)
#define KNO_TEST_OPCODE       KNO_TABLE_OPCODE(0x04)
#define KNO_STORE_OPCODE      KNO_TABLE_OPCODE(0x05)
#define KNO_PRIMGET_OPCODE    KNO_TABLE_OPCODE(0x06)
#define KNO_ADD_OPCODE        KNO_TABLE_OPCODE(0x07)
#define KNO_DROP_OPCODE       KNO_TABLE_OPCODE(0x08)
#define KNO_PRIMTEST_OPCODE   KNO_TABLE_OPCODE(0x09)

#define KNO_APPLY_OPCODE(x)      KNO_OPCODE(0x800+x)
#define KNO_APPLY_OPCODEP(x)     ( (x>=KNO_OPCODE(0x800)) && (x<KNO_OPCODE(0xA00)) )
/* Other primitives with more than two arguments */
#define KNO_APPLY0_OPCODE        KNO_APPLY_OPCODE(0x00)
#define KNO_APPLY1_OPCODE        KNO_APPLY_OPCODE(0x01)
#define KNO_APPLY2_OPCODE        KNO_APPLY_OPCODE(0x02)
#define KNO_APPLY3_OPCODE        KNO_APPLY_OPCODE(0x03)
#define KNO_APPLY4_OPCODE        KNO_APPLY_OPCODE(0x04)
#define KNO_APPLY5_OPCODE        KNO_APPLY_OPCODE(0x05)
#define KNO_APPLY6_OPCODE        KNO_APPLY_OPCODE(0x06)
#define KNO_APPLY7_OPCODE        KNO_APPLY_OPCODE(0x07)
#define KNO_APPLY8_OPCODE        KNO_APPLY_OPCODE(0x08)
#define KNO_APPLY9_OPCODE        KNO_APPLY_OPCODE(0x09)
#define KNO_APPLY10_OPCODE       KNO_APPLY_OPCODE(0x0a)
#define KNO_APPLY11_OPCODE       KNO_APPLY_OPCODE(0x0b)
#define KNO_APPLY12_OPCODE       KNO_APPLY_OPCODE(0x0c)
#define KNO_APPLY13_OPCODE       KNO_APPLY_OPCODE(0x0d)
#define KNO_APPLY14_OPCODE       KNO_APPLY_OPCODE(0x0e)
#define KNO_APPLY15_OPCODE       KNO_APPLY_OPCODE(0x0f)
#define KNO_APPLY_N_OPCODE       KNO_APPLY_OPCODE(0x10)

#define KNO_ND_OPCODEP(op)    \
  ( (KNO_TABLE_OPCODEP(op)) || \
    (KNO_ND1_OPCODEP(op)) ||   \
    (KNO_ND2_OPCODEP(op)) )

#endif /* KNO_OPCODES_H */

