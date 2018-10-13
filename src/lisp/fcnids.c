/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2006-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
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

#define FD_INLINE_FCNIDS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/apply.h"

#include <libu8/u8printf.h>

#define FD_FCNID_MAX (FD_FCNID_NBLOCKS*FD_FCNID_BLOCKSIZE)

u8_condition fd_InvalidFCNID=_("Invalid persistent pointer reference");
u8_condition fd_FCNIDOverflow=_("No more valid persistent pointers");

struct FD_CONS **_fd_fcnids[FD_FCNID_NBLOCKS];
int _fd_fcnid_count = 0;
u8_mutex _fd_fcnid_lock;

int _fd_leak_fcnids = 1;

FD_EXPORT lispval fd_resolve_fcnid(lispval x)
{
  return fd_fcnid_ref(x);
}

FD_EXPORT lispval fd_register_fcnid(lispval x)
{
  int serialno;
  if (!(CONSP(x)))
    return fd_type_error("cons","fd_register_fcnid",x);
  u8_lock_mutex(&_fd_fcnid_lock);
  if (_fd_fcnid_count>=FD_FCNID_MAX) {
    u8_unlock_mutex(&_fd_fcnid_lock);
    return fd_err(fd_FCNIDOverflow,"fd_register_fcnid",NULL,x);}
  serialno=_fd_fcnid_count++;
  if ((serialno%FD_FCNID_BLOCKSIZE)==0) {
    struct FD_CONS **block = u8_alloc_n(FD_FCNID_BLOCKSIZE,struct FD_CONS *);
    int i = 0, n = FD_FCNID_BLOCKSIZE;
    while (i<n) block[i++]=NULL;
    _fd_fcnids[serialno/FD_FCNID_BLOCKSIZE]=block;}
  fd_incref(x);
  _fd_fcnids[serialno/FD_FCNID_BLOCKSIZE][serialno%FD_FCNID_BLOCKSIZE]=
    (struct FD_CONS *)x;
  u8_unlock_mutex(&_fd_fcnid_lock);
  if (FD_FUNCTIONP(x)) {
    struct FD_FUNCTION *f = (fd_function)x;
    f->fcnid = LISPVAL_IMMEDIATE(fd_fcnid_type,serialno);}
  return LISPVAL_IMMEDIATE(fd_fcnid_type,serialno);
}

FD_EXPORT lispval fd_set_fcnid(lispval id,lispval value)
{
  if (!(FD_FCNIDP(id)))
    return fd_type_error("fcnid","fd_set_fcnid",id);
  else if (!(CONSP(value)))
    return fd_type_error("cons","fd_set_fcnid",value);
  else if (!((FD_FUNCTIONP(value))||
             (TYPEP(value,fd_evalfn_type))))
    return fd_type_error("function/fexpr","fd_set_fcnid",value);
  else {
    u8_lock_mutex(&_fd_fcnid_lock);
    int serialno = FD_GET_IMMEDIATE(id,fd_fcnid_type);
    int block_num = serialno/FD_FCNID_BLOCKSIZE;
    int block_off = serialno%FD_FCNID_BLOCKSIZE;
    if (serialno>=_fd_fcnid_count) {
      u8_unlock_mutex(&_fd_fcnid_lock);
      return fd_err(fd_InvalidFCNID,"fd_set_fcnid",NULL,id);}
    else {
      struct FD_CONS **block=_fd_fcnids[block_num];
      struct FD_FUNCTION *fcn = (fd_function)value;
      if (!(block)) {
        /* We should never get here, but let's check anyway */
        u8_unlock_mutex(&_fd_fcnid_lock);
        return fd_err(fd_InvalidFCNID,"fd_set_fcnid",NULL,id);}
      else {
        struct FD_CONS *current = block[block_off];
        if (current == ((fd_cons)value)) {
          u8_unlock_mutex(&_fd_fcnid_lock);
          return id;}
        block[block_off]=(fd_cons)value;
        fd_incref(value);
        fcn->fcnid = id;
        if (!(_fd_leak_fcnids)) {
          /* This is dangerous if, for example, a module is being reloaded
             (and fcnid's redefined) while another thread is using the old
             value. If this bothers you, set fd_leak_fcnids to 1. */
          if (current) {fd_decref((lispval)current);}}
        u8_unlock_mutex(&_fd_fcnid_lock);
        return id;}}}
}

FD_EXPORT int fd_deregister_fcnid(lispval id,lispval value)
{
  if (!(FD_FCNIDP(id))) {
    fd_seterr(fd_TypeError,"fd_degister_fcnid","fcnid",id);
    return -1;}
  else if (!(CONSP(value)))
    return 0;
  else if (!((FD_FUNCTIONP(value))||
             (TYPEP(value,fd_evalfn_type))))
    return 0;
  else {
    u8_lock_mutex(&_fd_fcnid_lock);
    int serialno = FD_GET_IMMEDIATE(id,fd_fcnid_type);
    int block_num = serialno/FD_FCNID_BLOCKSIZE;
    int block_off = serialno%FD_FCNID_BLOCKSIZE;
    if (serialno>=_fd_fcnid_count) {
      u8_unlock_mutex(&_fd_fcnid_lock);
      fd_seterr(fd_InvalidFCNID,"fd_set_fcnid",NULL,id);
      return -1;}
    else {
      struct FD_CONS **block=_fd_fcnids[block_num];
      if (!(block)) {
        /* We should never get here, but let's check anyway */
        u8_unlock_mutex(&_fd_fcnid_lock);
        fd_seterr(fd_InvalidFCNID,"fd_set_fcnid",NULL,id);
        return -1;}
      else {
        struct FD_CONS *current = block[block_off];
        /* No longer registered */
        if (current!=((fd_cons)value)) {
          u8_unlock_mutex(&_fd_fcnid_lock);
          return 0;}
        else block[block_off]=(fd_cons)NULL;
        u8_unlock_mutex(&_fd_fcnid_lock);
        return 1;}}}
}

static int unparse_fcnid(u8_output out,lispval x)
{
  lispval lp = fd_fcnid_ref(x);
  if (TYPEP(lp,fd_cprim_type)) {
    struct FD_FUNCTION *fcn = (fd_function)lp;
    u8_string filename = fcn->fcn_filename;
    u8_byte arity[64]="", codes[64]="", numbuf[32], namebuf[64];
    u8_string name = fd_fcn_sig(fcn,namebuf);
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
                FD_GET_IMMEDIATE(x,fd_fcnid_type),
                codes,name,arity,U8OPTSTR("'",filename,"'"));
    else u8_printf(out,"#<~%d<Φ%s%s #!0x%llx%s%s%s>>",
                   FD_GET_IMMEDIATE(x,fd_fcnid_type),codes,arity,
                   u8_uitoa16((unsigned long long) fcn,numbuf),
                   U8OPTSTR(" '",filename,"'"));
    return 1;}
  else {
    u8_printf(out,"#<~%ld %q>",FD_GET_IMMEDIATE(x,fd_fcnid_type),lp);
    return 1;}
}

FD_EXPORT void fd_init_fcnids_c()
{
  fd_type_names[fd_fcnid_type]=_("function identifier");
  fd_unparsers[fd_fcnid_type]=unparse_fcnid;
  u8_init_mutex(&_fd_fcnid_lock);
  u8_register_source_file(_FILEINFO);
  fd_register_config
    ("FCNID:LEAK",
     "Leak values stored behind function IDs, to avoid use after free due "
     "to dangling references",
     fd_boolconfig_get,fd_boolconfig_set,&_fd_leak_fcnids);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
