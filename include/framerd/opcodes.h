/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_OPCODES_H
#define FRAMERD_OPCODES_H 1
#ifndef FRAMERD_OPCODES_H_INFO
#define FRAMERD_OPCODES_H_INFO "include/framerd/opcodes.h"
#endif

FD_EXPORT lispval fd_opcode_dispatch
(lispval opcode,lispval expr,fd_lexenv env,
 fd_stack caller,int tail);

/* Opcodes */

FD_EXPORT u8_string fd_opcode_names[];
FD_EXPORT int fd_opcodes_length;

#define FD_SPECIAL_OPCODE(x)  FD_OPCODE(x+0x00)
#define FD_SPECIAL_OPCODEP(x) ((x>FD_OPCODE(0x00))&&(x<FD_OPCODE(0x100)))
#define FD_BRANCH_OPCODE      FD_SPECIAL_OPCODE(0x01)
#define FD_NOT_OPCODE         FD_SPECIAL_OPCODE(0x02)
#define FD_UNTIL_OPCODE       FD_SPECIAL_OPCODE(0x03)
#define FD_BEGIN_OPCODE       FD_SPECIAL_OPCODE(0x04)
#define FD_QUOTE_OPCODE       FD_SPECIAL_OPCODE(0x05)
#define FD_ASSIGN_OPCODE      FD_SPECIAL_OPCODE(0x06)
#define FD_SYMREF_OPCODE      FD_SPECIAL_OPCODE(0x07)
#define FD_BIND_OPCODE        FD_SPECIAL_OPCODE(0x08)
#define FD_VOID_OPCODE        FD_SPECIAL_OPCODE(0x09)
#define FD_TRY_OPCODE         FD_SPECIAL_OPCODE(0x0A)
#define FD_AND_OPCODE         FD_SPECIAL_OPCODE(0x0B)
#define FD_OR_OPCODE          FD_SPECIAL_OPCODE(0x0C)
#define FD_INLINE_OPCODE      FD_SPECIAL_OPCODE(0x0D)
#define FD_SOURCEREF_OPCODE   FD_SPECIAL_OPCODE(0x0D)

/* Unary primitives which handle their own non-determinism. */
#define FD_ND1_OPCODE(x)      FD_OPCODE(0x100+x)
#define FD_ND1_OPCODEP(x)     ((x>FD_OPCODE(0x100))&&(x<FD_OPCODE(0x200)))
#define FD_AMBIGP_OPCODE      FD_ND1_OPCODE(0x01)
#define FD_SINGLETONP_OPCODE  FD_ND1_OPCODE(0x02)
#define FD_FAILP_OPCODE       FD_ND1_OPCODE(0x03)
#define FD_EXISTSP_OPCODE     FD_ND1_OPCODE(0x04)
#define FD_SINGLETON_OPCODE   FD_ND1_OPCODE(0x05)
#define FD_CAR_OPCODE         FD_ND1_OPCODE(0x06)
#define FD_CDR_OPCODE         FD_ND1_OPCODE(0x07)
#define FD_LENGTH_OPCODE      FD_ND1_OPCODE(0x08)
#define FD_QCHOICE_OPCODE     FD_ND1_OPCODE(0x09)
#define FD_CHOICE_SIZE_OPCODE FD_ND1_OPCODE(0x0a)
#define FD_PICKOIDS_OPCODE    FD_ND1_OPCODE(0x0b)
#define FD_PICKSTRINGS_OPCODE FD_ND1_OPCODE(0x0c)
#define FD_PICKONE_OPCODE     FD_ND1_OPCODE(0x0d)
#define FD_IFEXISTS_OPCODE    FD_ND1_OPCODE(0x0e)
#define FD_FIXCHOICE_OPCODE   FD_ND1_OPCODE(0x0f)

/* Unary primitives which don't handle their own non-determinism. */
#define FD_D1_OPCODE(x)       FD_OPCODE(0x200+x)
#define FD_D1_OPCODEP(x)      (x>FD_OPCODE(0x200))&&(x<FD_OPCODE(0x300))
#define FD_MINUS1_OPCODE      FD_D1_OPCODE(0x01)
#define FD_PLUS1_OPCODE       FD_D1_OPCODE(0x02)
#define FD_NUMBERP_OPCODE     FD_D1_OPCODE(0x03)
#define FD_ZEROP_OPCODE       FD_D1_OPCODE(0x04)
#define FD_VECTORP_OPCODE     FD_D1_OPCODE(0x05)
#define FD_PAIRP_OPCODE       FD_D1_OPCODE(0x06)
#define FD_EMPTY_LISTP_OPCODE FD_D1_OPCODE(0x07)
#define FD_STRINGP_OPCODE     FD_D1_OPCODE(0x08)
#define FD_OIDP_OPCODE        FD_D1_OPCODE(0x09)
#define FD_SYMBOLP_OPCODE     FD_D1_OPCODE(0x0a)
#define FD_FIXNUMP_OPCODE     FD_D1_OPCODE(0x0b)
#define FD_FLONUMP_OPCODE     FD_D1_OPCODE(0x0c)
#define FD_SEQUENCEP_OPCODE   FD_D1_OPCODE(0x0d)
#define FD_TABLEP_OPCODE      FD_D1_OPCODE(0x0e)
#define FD_FIRST_OPCODE       FD_D1_OPCODE(0x0f)
#define FD_SECOND_OPCODE      FD_D1_OPCODE(0x10)
#define FD_THIRD_OPCODE       FD_D1_OPCODE(0x11)
#define FD_CADR_OPCODE        FD_D1_OPCODE(0x12)
#define FD_CDDR_OPCODE        FD_D1_OPCODE(0x13)
#define FD_CADDR_OPCODE       FD_D1_OPCODE(0x14)
#define FD_CDDDR_OPCODE       FD_D1_OPCODE(0x15)
#define FD_TONUMBER_OPCODE    FD_D1_OPCODE(0x16)
#define FD_ELTS_OPCODE        FD_D1_OPCODE(0x17)

/* Arithmetic primitives with two arguments */
#define FD_NUMERIC_OPCODE(x) FD_OPCODE(0x300+x)
#define FD_NUMERIC_OPCODEP(x) (x>FD_OPCODE(0x300))&&(x<FD_OPCODE(0x400))
#define FD_NUMEQ_OPCODE      FD_NUMERIC_OPCODE(0x01)
#define FD_GT_OPCODE         FD_NUMERIC_OPCODE(0x02)
#define FD_GTE_OPCODE        FD_NUMERIC_OPCODE(0x03)
#define FD_LT_OPCODE         FD_NUMERIC_OPCODE(0x04)
#define FD_LTE_OPCODE        FD_NUMERIC_OPCODE(0x05)
#define FD_PLUS_OPCODE       FD_NUMERIC_OPCODE(0x06)
#define FD_MINUS_OPCODE      FD_NUMERIC_OPCODE(0x07)
#define FD_TIMES_OPCODE      FD_NUMERIC_OPCODE(0x08)
#define FD_FLODIV_OPCODE     FD_NUMERIC_OPCODE(0x09)

#define FD_D2_OPCODE(x)      FD_OPCODE(0x400+x)
#define FD_D2_OPCODEP(x)     ((x>FD_OPCODE(0x400))&&(x<FD_OPCODE(0x500)))
#define FD_EQ_OPCODE         FD_D2_OPCODE(0x01)
#define FD_EQV_OPCODE        FD_D2_OPCODE(0x02)
#define FD_EQUAL_OPCODE      FD_D2_OPCODE(0x03)
#define FD_ELT_OPCODE        FD_D2_OPCODE(0x04)
#define FD_CONS_OPCODE       FD_D2_OPCODE(0x05)

#define FD_ND2_OPCODE(x)     FD_OPCODE(0x500+x)
#define FD_ND2_OPCODEP(x)    ((x>FD_OPCODE(0x500))&&(x<FD_OPCODE(0x600)))
#define FD_IDENTICAL_OPCODE  FD_ND2_OPCODE(0x01)
#define FD_OVERLAPS_OPCODE   FD_ND2_OPCODE(0x02)
#define FD_CONTAINSP_OPCODE  FD_ND2_OPCODE(0x03)
#define FD_UNION_OPCODE      FD_ND2_OPCODE(0x04)
#define FD_INTERSECT_OPCODE  FD_ND2_OPCODE(0x05)
#define FD_DIFFERENCE_OPCODE FD_ND2_OPCODE(0x06)
#define FD_CHOICEREF_OPCODE  FD_ND2_OPCODE(0x07)

#define FD_ND3_OPCODE(x)     FD_OPCODE(0x700+x)
#define FD_ND3_OPCODEP(x)    ((x>FD_OPCODE(0x600))&&(x<FD_OPCODE(0x800))
/* Other primitives with more than two arguments */
#define FD_GET_OPCODE        FD_ND3_OPCODE(0x01)
#define FD_ASSERT_OPCODE     FD_ND3_OPCODE(0x02)
#define FD_RETRACT_OPCODE    FD_ND3_OPCODE(0x03)
#define FD_TEST_OPCODE       FD_ND3_OPCODE(0x04)
#define FD_STORE_OPCODE      FD_ND3_OPCODE(0x05)
#define FD_PRIMGET_OPCODE    FD_ND3_OPCODE(0x06)
#define FD_ADD_OPCODE        FD_ND3_OPCODE(0x07)
#define FD_DROP_OPCODE       FD_ND3_OPCODE(0x08)
#define FD_PRIMTEST_OPCODE   FD_ND3_OPCODE(0x09)
#define FD_XREF_OPCODE       FD_ND3_OPCODE(0x20)

#define FD_APPLY_OPCODE(x)   FD_OPCODE(0x800+x)
#define FD_APPLY_OPCODEP(x)  ((x>FD_OPCODE(0x800))&&(x<FD_OPCODE(0xA00))
/* Other primitives with more than two arguments */
#define FD_APPLY0_OPCODE        FD_APPLY_OPCODE(0x00)
#define FD_APPLY1_OPCODE        FD_APPLY_OPCODE(0x01)
#define FD_APPLY2_OPCODE        FD_APPLY_OPCODE(0x02)
#define FD_APPLY3_OPCODE        FD_APPLY_OPCODE(0x03)
#define FD_APPLY4_OPCODE        FD_APPLY_OPCODE(0x04)
#define FD_APPLY5_OPCODE        FD_APPLY_OPCODE(0x05)
#define FD_APPLY6_OPCODE        FD_APPLY_OPCODE(0x06)
#define FD_APPLY7_OPCODE        FD_APPLY_OPCODE(0x07)
#define FD_APPLY8_OPCODE        FD_APPLY_OPCODE(0x08)
#define FD_APPLY9_OPCODE        FD_APPLY_OPCODE(0x09)
#define FD_APPLY10_OPCODE       FD_APPLY_OPCODE(0x0a)
#define FD_APPLY11_OPCODE       FD_APPLY_OPCODE(0x0b)
#define FD_APPLY12_OPCODE       FD_APPLY_OPCODE(0x0c)
#define FD_APPLY13_OPCODE       FD_APPLY_OPCODE(0x0d)
#define FD_APPLY14_OPCODE       FD_APPLY_OPCODE(0x0e)
#define FD_APPLY15_OPCODE       FD_APPLY_OPCODE(0x0f)
#define FD_APPLY_N_OPCODE       FD_APPLY_OPCODE(0x10)

#endif /* FRAMERD_OPCODES_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
