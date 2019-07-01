/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2006-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Persistent pointers are immediate values which refer to conses
   maintained in a table.  The idea is that they can be passed around
   without GC operations or the corresponding lock contentions.  They
   especially help speed up the evaluator when used to wrap primitives
   and (to a lesser degree) primitives.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_FCNIDS 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/apply.h"

#include <libu8/u8printf.h>

#define KNO_FCNID_MAX (KNO_FCNID_NBLOCKS*KNO_FCNID_BLOCKSIZE)

u8_condition kno_InvalidFCNID=_("Invalid persistent pointer reference");
u8_condition kno_FCNIDOverflow=_("No more valid persistent pointers");

struct KNO_CONS **_kno_fcnids[KNO_FCNID_NBLOCKS];
int _kno_fcnid_count = 0;
u8_mutex _kno_fcnid_lock;

int _kno_leak_fcnids = 1;

KNO_EXPORT lispval kno_resolve_fcnid(lispval x)
{
  return kno_fcnid_ref(x);
}

KNO_EXPORT lispval kno_register_fcnid(lispval x)
{
  int serialno;
  if (!(CONSP(x)))
    return kno_type_error("cons","kno_register_fcnid",x);
  u8_lock_mutex(&_kno_fcnid_lock);
  if (_kno_fcnid_count>=KNO_FCNID_MAX) {
    u8_unlock_mutex(&_kno_fcnid_lock);
    return kno_err(kno_FCNIDOverflow,"kno_register_fcnid",NULL,x);}
  serialno=_kno_fcnid_count++;
  if ((serialno%KNO_FCNID_BLOCKSIZE)==0) {
    struct KNO_CONS **block = u8_alloc_n(KNO_FCNID_BLOCKSIZE,struct KNO_CONS *);
    int i = 0, n = KNO_FCNID_BLOCKSIZE;
    while (i<n) block[i++]=NULL;
    _kno_fcnids[serialno/KNO_FCNID_BLOCKSIZE]=block;}
  _kno_fcnids[serialno/KNO_FCNID_BLOCKSIZE][serialno%KNO_FCNID_BLOCKSIZE]=
    (struct KNO_CONS *) (kno_make_simple_choice(x));
  u8_unlock_mutex(&_kno_fcnid_lock);
  if (KNO_FUNCTIONP(x)) {
    struct KNO_FUNCTION *f = (kno_function)x;
    f->fcnid = LISPVAL_IMMEDIATE(kno_fcnid_type,serialno);}
  return LISPVAL_IMMEDIATE(kno_fcnid_type,serialno);
}

KNO_EXPORT lispval kno_set_fcnid(lispval id,lispval value)
{
  if (!(KNO_FCNIDP(id)))
    return kno_type_error("fcnid","kno_set_fcnid",id);
  else if (!(CONSP(value)))
    return kno_type_error("cons","kno_set_fcnid",value);
  else if (!((KNO_FUNCTIONP(value))||
             (TYPEP(value,kno_evalfn_type))))
    return kno_type_error("function/fexpr","kno_set_fcnid",value);
  else {
    u8_lock_mutex(&_kno_fcnid_lock);
    int serialno = KNO_GET_IMMEDIATE(id,kno_fcnid_type);
    int block_num = serialno/KNO_FCNID_BLOCKSIZE;
    int block_off = serialno%KNO_FCNID_BLOCKSIZE;
    if (serialno>=_kno_fcnid_count) {
      u8_unlock_mutex(&_kno_fcnid_lock);
      return kno_err(kno_InvalidFCNID,"kno_set_fcnid",NULL,id);}
    else {
      struct KNO_CONS **block=_kno_fcnids[block_num];
      struct KNO_FUNCTION *fcn = (kno_function)value;
      if (!(block)) {
        /* We should never get here, but let's check anyway */
        u8_unlock_mutex(&_kno_fcnid_lock);
        return kno_err(kno_InvalidFCNID,"kno_set_fcnid",NULL,id);}
      else {
        struct KNO_CONS *current = block[block_off];
        if (current == ((kno_cons)value)) {
          u8_unlock_mutex(&_kno_fcnid_lock);
          return id;}
        block[block_off]=(kno_cons) kno_make_simple_choice(value);
        fcn->fcnid = id;
        if (!(_kno_leak_fcnids)) {
          /* This is dangerous if, for example, a module is being reloaded
             (and fcnid's redefined) while another thread is using the old
             value. If this bothers you, set kno_leak_fcnids to 1. */
          if (current) {kno_decref((lispval)current);}}
        u8_unlock_mutex(&_kno_fcnid_lock);
        return id;}}}
}

KNO_EXPORT int kno_deregister_fcnid(lispval id,lispval value)
{
  if (!(KNO_FCNIDP(id)))
    return KNO_ERR(-1,kno_TypeError,"kno_degister_fcnid","fcnid",id);
  else if (!(CONSP(value)))
    return 0;
  else if (!((KNO_FUNCTIONP(value))||
             (TYPEP(value,kno_evalfn_type))))
    return 0;
  else {
    u8_lock_mutex(&_kno_fcnid_lock);
    int serialno = KNO_GET_IMMEDIATE(id,kno_fcnid_type);
    int block_num = serialno/KNO_FCNID_BLOCKSIZE;
    int block_off = serialno%KNO_FCNID_BLOCKSIZE;
    if (serialno>=_kno_fcnid_count) {
      u8_unlock_mutex(&_kno_fcnid_lock);
      return KNO_ERR(-1,kno_InvalidFCNID,"kno_set_fcnid",NULL,id);}
    else {
      struct KNO_CONS **block=_kno_fcnids[block_num];
      if (!(block)) {
        /* We should never get here, but let's check anyway */
        u8_unlock_mutex(&_kno_fcnid_lock);
        return KNO_ERR(-1,kno_InvalidFCNID,"kno_set_fcnid",NULL,id);}
      else {
        struct KNO_CONS *current = block[block_off];
        /* No longer registered */
        if (current!=((kno_cons)value)) {
          u8_unlock_mutex(&_kno_fcnid_lock);
          return 0;}
        else block[block_off]=(kno_cons)NULL;
        u8_unlock_mutex(&_kno_fcnid_lock);
        return 1;}}}
}

static int unparse_fcnid(u8_output out,lispval x)
{
  lispval lp = kno_fcnid_ref(x);
  if (TYPEP(lp,kno_cprim_type)) {
    struct KNO_FUNCTION *fcn = (kno_function)lp;
    u8_string filename = fcn->fcn_filename;
    u8_byte arity[64]="", codes[64]="", numbuf[32], namebuf[64];
    u8_string name = kno_fcn_sig(fcn,namebuf);
    if ((filename)&&(filename[0]=='\0')) filename = NULL;
    if (fcn->fcn_ndcall) strcat(codes,"∀");
    if ((fcn->fcn_arity<0)&&(fcn->fcn_min_arity<0))
      strcat(arity,"…");
    else if (fcn->fcn_arity == fcn->fcn_min_arity) {
      strcat(arity,"[");
      strcat(arity,u8_itoa10(fcn->fcn_arity,numbuf));
      strcat(arity,"]");}
    else if (fcn->fcn_arity<0) {
      strcat(arity,"[");
      strcat(arity,u8_itoa10(fcn->fcn_min_arity,numbuf));
      strcat(arity,"…]");}
    else {
      strcat(arity,"[");
      strcat(arity,u8_itoa10(fcn->fcn_min_arity,numbuf));
      strcat(arity,"-");
      strcat(arity,u8_itoa10(fcn->fcn_arity,numbuf));
      strcat(arity,"]");}
    if (name)
      u8_printf(out,"#<~%d<Φ%s%s%s%s%s%s>>",
                KNO_GET_IMMEDIATE(x,kno_fcnid_type),
                codes,name,arity,U8OPTSTR("'",filename,"'"));
    else u8_printf(out,"#<~%d<Φ%s%s #!0x%llx%s%s%s>>",
                   KNO_GET_IMMEDIATE(x,kno_fcnid_type),codes,arity,
                   u8_uitoa16((KNO_PTRVAL(fcn)),numbuf),
                   U8OPTSTR(" '",filename,"'"));
    return 1;}
  else {
    u8_printf(out,"#<~%ld %q>",KNO_GET_IMMEDIATE(x,kno_fcnid_type),lp);
    return 1;}
}

KNO_EXPORT void kno_init_fcnids_c()
{
  kno_type_names[kno_fcnid_type]=_("function identifier");
  kno_unparsers[kno_fcnid_type]=unparse_fcnid;
  u8_init_mutex(&_kno_fcnid_lock);
  u8_register_source_file(_FILEINFO);
  kno_register_config
    ("FCNID:LEAK",
     "Leak values stored behind function IDs, to avoid use after free due "
     "to dangling references",
     kno_boolconfig_get,kno_boolconfig_set,&_kno_leak_fcnids);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
