/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/streams.h"
#include "framerd/dtypeio.h"
#include "framerd/ports.h"

#include <ctype.h>

static lispval reqgetvar(lispval cgidata,lispval var)
{
  int noparse=
    ((SYMBOLP(var))&&((SYM_NAME(var))[0]=='%'));
  lispval name = ((noparse)?(fd_intern(SYM_NAME(var)+1)):(var));
  lispval val = ((TABLEP(cgidata))?(fd_get(cgidata,name,VOID)):
              (fd_req_get(name,VOID)));
  if (VOIDP(val)) return val;
  else if ((noparse)&&(STRINGP(val)))
    return val;
  else if (STRINGP(val)) {
    u8_string data = CSTRING(val);
    if (*data=='\0') return val;
    else if (strchr("@{#(",data[0])) {
      lispval parsed = fd_parse_arg(data);
      fd_decref(val); return parsed;}
    else if (isdigit(data[0])) {
      lispval parsed = fd_parse_arg(data);
      if (NUMBERP(parsed)) {
        fd_decref(val); return parsed;}
      else {
        fd_decref(parsed); return val;}}
    else if (*data == ':')
      if (data[1]=='\0')
        return lispval_string(data);
      else {
        lispval arg = fd_parse(data+1);
        if (FD_ABORTP(arg)) {
          u8_log(LOG_WARN,fd_ParseArgError,"Bad colon spec arg '%s'",arg);
          fd_clear_errors(1);
          return lispval_string(data);}
        else return arg;}
    else if (*data == '\\') {
      lispval shorter = lispval_string(data+1);
      fd_decref(val);
      return shorter;}
    else return val;}
  else if ((CHOICEP(val))||(PRECHOICEP(val))) {
    lispval result = EMPTY;
    DO_CHOICES(v,val) {
      if (!(STRINGP(v))) {
        fd_incref(v); CHOICE_ADD(result,v);}
      else {
        u8_string data = CSTRING(v); lispval parsed = v;
        if (*data=='\\') parsed = lispval_string(data+1);
        else if ((*data==':')&&(data[1]=='\0')) {fd_incref(parsed);}
        else if (*data==':')
          parsed = fd_parse(data+1);
        else if ((isdigit(*data))||(*data=='+')||(*data=='-')||(*data=='.')) {
          parsed = fd_parse_arg(data);
          if (!(NUMBERP(parsed))) {
            fd_decref(parsed); parsed = v; fd_incref(parsed);}}
        else if ( (noparse==0) && (strchr("@{#(",data[0])) )
          parsed = fd_parse_arg(data);
        else fd_incref(parsed);
        if (FD_ABORTP(parsed)) {
          u8_log(LOG_WARN,fd_ParseArgError,"Bad LISP arg '%s'",data);
          fd_clear_errors(1);
          parsed = v; fd_incref(v);}
        CHOICE_ADD(result,parsed);}}
    fd_decref(val);
    return result;}
  else return val;
}

/* The init function */

static lispval reqcall_prim(lispval proc)
{
  lispval value = VOID;
  if (!(FD_APPLICABLEP(proc)))
    value = fd_type_error("applicable","cgicall",proc);
  else {
    lispval fn = fd_fcnid_ref(proc);
    if (FD_LAMBDAP(fn))
      value=fd_xapply_lambda((fd_lambda)fn,
                             (void *)VOID,
                             (lispval (*)(void *,lispval))reqgetvar);
    else value = fd_apply(fn,0,NULL);}
  return value;
}

static lispval reqget_prim(lispval vars,lispval dflt)
{
  lispval results = EMPTY; int found = 0;
  DO_CHOICES(var,vars) {
    lispval name = ((STRINGP(var))?(fd_intern(CSTRING(var))):(var));
    lispval val = fd_req_get(name,VOID);
    if (!(VOIDP(val))) {
      found = 1; CHOICE_ADD(results,val);}}
  if (found) return fd_simplify_choice(results);
  else if (VOIDP(dflt)) return EMPTY;
  else if (QCHOICEP(dflt)) {
    struct FD_QCHOICE *qc = FD_XQCHOICE(dflt);
    return fd_make_simple_choice(qc->qchoiceval);}
  else return fd_incref(dflt);
}

static lispval reqval_prim(lispval vars,lispval dflt)
{
  lispval results = EMPTY; int found = 0;
  DO_CHOICES(var,vars) {
    lispval val;
    if (STRINGP(var)) var = fd_intern(CSTRING(var));
    val = fd_req_get(var,VOID);
    if (VOIDP(val)) {}
    else if (STRINGP(val)) {
      lispval parsed = fd_parse_arg(CSTRING(val));
      fd_decref(val);
      CHOICE_ADD(results,parsed);
      found = 1;}
    else if (CHOICEP(val)) {
      DO_CHOICES(v,val) {
        if (STRINGP(v)) {
          lispval parsed = fd_parse_arg(CSTRING(v));
          CHOICE_ADD(results,parsed);}
        else {
          fd_incref(v); CHOICE_ADD(results,v);}}
      fd_decref(val);
      found = 1;}
    else {
      CHOICE_ADD(results,val);
      found = 1;}}
  if (found) return results;
  else if (VOIDP(dflt)) return EMPTY;
  else if (QCHOICEP(dflt)) {
    struct FD_QCHOICE *qc = FD_XQCHOICE(dflt);
    return fd_make_simple_choice(qc->qchoiceval);}
  else return fd_incref(dflt);
}

static lispval hashcolon_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval var = fd_get_arg(expr,1);
  if (VOIDP(var))
    return fd_err(fd_SyntaxError,"hashcolon_evalfn",NULL,expr);
  else return reqget_prim(var,EMPTY);
}

static lispval hashcoloncolon_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval var = fd_get_arg(expr,1);
  if (VOIDP(var))
    return fd_err(fd_SyntaxError,"hashcoloncolon_evalfn",NULL,expr);
  else {
    lispval val = reqget_prim(var,EMPTY);
    if (STRINGP(val)) {
      lispval result = fd_parse_arg(CSTRING(val));
      fd_decref(val);
      return result;}
    else return val;}
}

static lispval hashcolondollar_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval var = fd_get_arg(expr,1);
  if (VOIDP(var))
    return fd_err(fd_SyntaxError,"hashcoloncolon_evalfn",NULL,expr);
  else {
    lispval val = reqget_prim(var,VOID);
    if (STRINGP(val)) {
      lispval result = fd_parse_arg(CSTRING(val));
      fd_decref(val);
      return result;}
    else if (VOIDP(val))
      return fd_make_string(NULL,0,"");
    else {
      lispval result; struct U8_OUTPUT out;
      U8_INIT_OUTPUT(&out,64); fd_unparse(&out,val);
      result = fd_stream_string(&out);
      u8_free(out.u8_outbuf);
      return result;}}
}

static lispval hashcolonquestion_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval var = fd_get_arg(expr,1);
  if (VOIDP(var))
    return fd_err(fd_SyntaxError,"hashcoloncolon_evalfn",NULL,expr);
  else if (fd_req_test(var,VOID))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval reqtest_prim(lispval vars,lispval val)
{
  DO_CHOICES(var,vars) {
    lispval name = ((STRINGP(var))?(fd_intern(CSTRING(var))):(var));
    int retval = fd_req_test(name,val);
    if (retval<0) {
      FD_STOP_DO_CHOICES;
      return FD_ERROR;}
    else if (retval) {
      FD_STOP_DO_CHOICES;
      return FD_TRUE;}
    else {}}
  return FD_FALSE;
}

static lispval reqset_prim(lispval vars,lispval value)
{
  {DO_CHOICES(var,vars) {
      lispval name = ((STRINGP(var))?(fd_intern(CSTRING(var))):(var));
      fd_req_store(name,value);}}
  return VOID;
}

static lispval reqadd_prim(lispval vars,lispval value)
{
  {DO_CHOICES(var,vars) {
      lispval name = ((STRINGP(var))?(fd_intern(CSTRING(var))):(var));
      fd_req_add(name,value);}}
  return VOID;
}

static lispval reqdrop_prim(lispval vars,lispval value)
{
  {DO_CHOICES(var,vars) {
      lispval name = ((STRINGP(var))?(fd_intern(CSTRING(var))):(var));
      fd_req_drop(name,value);}}
  return VOID;
}

static lispval reqpush_prim(lispval vars,lispval values)
{
  {DO_CHOICES(var,vars) {
      lispval name = ((STRINGP(var))?(fd_intern(CSTRING(var))):(var));
      DO_CHOICES(value,values) {
        fd_req_push(name,value);}}}
  return VOID;
}

lispval reqdata_prim()
{
  return fd_req_call(fd_deep_copy);
}

static lispval withreq_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval body = fd_get_body(expr,1), result = VOID;
  fd_use_reqinfo(FD_TRUE); fd_reqlog(1);
  {FD_DOLIST(ex,body) {
      if (FD_ABORTP(result)) {
        fd_use_reqinfo(EMPTY);
        fd_reqlog(-1);
        return result;}
      fd_decref(result);
      result = fd_eval(ex,env);}}
  fd_use_reqinfo(EMPTY);
  fd_reqlog(-1);
  return result;
}

static lispval req_livep_prim()
{
  if (fd_isreqlive()) return FD_TRUE;
  else return FD_FALSE;
}

FD_EXPORT lispval reqgetlog_prim()
{
  struct U8_OUTPUT *log = fd_reqlog(0);
  if (!(log)) return FD_FALSE;
  else {
    int len = log->u8_write-log->u8_outbuf;
    if (len==0) return FD_FALSE;
    else return fd_make_string(NULL,len,log->u8_outbuf);}
}

FD_EXPORT lispval reqloglen_prim()
{
  struct U8_OUTPUT *log = fd_reqlog(0);
  if (!(log)) return FD_FALSE;
  else {
    int len = log->u8_write-log->u8_outbuf;
    return FD_INT(len);}
}

FD_EXPORT lispval reqlog_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  struct U8_XTIME xt;
  struct U8_OUTPUT *reqout = fd_reqlog(1);
  u8_string cond = NULL, cxt = NULL;
  long long body_off = 1, level = -1;
  lispval arg1 = fd_get_arg(expr,1), arg2 = fd_get_arg(expr,2);
  lispval arg3 = fd_get_arg(expr,3), body, outval;
  if (FIXNUMP(arg1)) {
    level = FIX2INT(arg1); body_off++;
    arg1 = arg2; arg2 = arg3; arg3 = VOID;}
  if (SYMBOLP(arg1)) {
    cond = SYM_NAME(arg1); body_off++;
    arg1 = arg2; arg2 = arg3; arg3 = VOID;}
  if (SYMBOLP(arg1)) {
    cxt = SYM_NAME(arg2); body_off++;}
  u8_local_xtime(&xt,-1,u8_nanosecond,0);
  if (level>=0)
    u8_printf(reqout,"<logentry level='%d' scope='request'>",level);
  else u8_printf(reqout,"<logentry scope='request'>");
  if (level>=0) u8_printf(reqout,"\n\t<level>%d</level>",level);
  u8_printf(reqout,"\n\t<datetime tick='%ld' nsecs='%d'>%lXt</datetime>",
            xt.u8_tick,xt.u8_nsecs,&xt);
  if (cond) u8_printf(reqout,"\n\t<condition>%s</condition>",cond);
  if (cxt) u8_printf(reqout,"\n\t<context>%s</context>",cxt);
  u8_printf(reqout,"\n\t<message>\n");
  body = fd_get_body(expr,body_off);
  outval = fd_printout_to(reqout,body,env);
  u8_printf(reqout,"\n\t</message>");
  u8_printf(reqout,"\n</logentry>\n");
  fd_decref(body);
  if (FD_ABORTP(outval)) {
    return outval;}
  else return VOID;
}

FD_EXPORT void fd_init_reqstate_c()
{
  lispval module = fd_scheme_module;

  u8_register_source_file(_FILEINFO);

  fd_idefn(module,fd_make_cprim1("REQ/CALL",reqcall_prim,1));
  fd_idefn(module,fd_make_ndprim(fd_make_cprim2("REQ/GET",reqget_prim,1)));
  fd_idefn(module,fd_make_ndprim(fd_make_cprim2("REQ/VAL",reqval_prim,1)));
  fd_idefn(module,fd_make_ndprim(fd_make_cprim2("REQ/TEST",reqtest_prim,1)));
  fd_idefn(module,fd_make_ndprim(fd_make_cprim2("REQ/SET!",reqset_prim,2)));
  fd_idefn(module,fd_make_cprim2("REQ/ADD!",reqadd_prim,2));
  fd_idefn(module,fd_make_cprim2("REQ/DROP!",reqdrop_prim,1));
  fd_idefn(module,fd_make_cprim2("REQ/PUSH!",reqpush_prim,2));
  fd_idefn(module,fd_make_cprim0("REQ/LIVE?",req_livep_prim));
  fd_def_evalfn(module,"#:","",hashcolon_evalfn);
  fd_def_evalfn(module,"#::","",hashcoloncolon_evalfn);
  fd_def_evalfn(module,"#:$","",hashcolondollar_evalfn);
  fd_def_evalfn(module,"#:?","",hashcolonquestion_evalfn);

  fd_def_evalfn(module,"REQLOG","",reqlog_evalfn);
  fd_def_evalfn(module,"REQ/LOG!","",reqlog_evalfn);
  fd_idefn(module,fd_make_cprim0("REQ/GETLOG",reqgetlog_prim));
  fd_idefn(module,fd_make_cprim0("REQ/LOGLEN",reqloglen_prim));

  fd_idefn(module,fd_make_cprim0("REQ/DATA",reqdata_prim));

  fd_idefn(module,fd_make_cprim0("REQ/DATA",reqdata_prim));
  fd_def_evalfn(module,"WITH/REQUEST","",withreq_evalfn);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
