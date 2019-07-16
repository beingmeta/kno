/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/streams.h"
#include "kno/dtypeio.h"
#include "kno/ports.h"

#include <ctype.h>
#include <kno/cprims.h>


static lispval reqgetvar(lispval cgidata,lispval var)
{
  int noparse=
    ((SYMBOLP(var))&&((SYM_NAME(var))[0]=='%'));
  lispval name = ((noparse)?(kno_intern(SYM_NAME(var)+1)):(var));
  lispval val = ((TABLEP(cgidata))?(kno_get(cgidata,name,VOID)):
              (kno_req_get(name,VOID)));
  if (VOIDP(val)) return val;
  else if ((noparse)&&(STRINGP(val)))
    return val;
  else if (STRINGP(val)) {
    u8_string data = CSTRING(val);
    if (*data=='\0') return val;
    else if (strchr("@{#(",data[0])) {
      lispval parsed = kno_parse_arg(data);
      kno_decref(val); return parsed;}
    else if (isdigit(data[0])) {
      lispval parsed = kno_parse_arg(data);
      if (NUMBERP(parsed)) {
        kno_decref(val); return parsed;}
      else {
        kno_decref(parsed); return val;}}
    else if (*data == ':')
      if (data[1]=='\0')
        return kno_mkstring(data);
      else {
        lispval arg = kno_parse(data+1);
        if (KNO_ABORTP(arg)) {
          u8_log(LOG_WARN,kno_ParseArgError,"Bad colon spec arg '%s'",arg);
          kno_clear_errors(1);
          return kno_mkstring(data);}
        else return arg;}
    else if (*data == '\\') {
      lispval shorter = kno_mkstring(data+1);
      kno_decref(val);
      return shorter;}
    else return val;}
  else if ((CHOICEP(val))||(PRECHOICEP(val))) {
    lispval result = EMPTY;
    DO_CHOICES(v,val) {
      if (!(STRINGP(v))) {
        kno_incref(v); CHOICE_ADD(result,v);}
      else {
        u8_string data = CSTRING(v); lispval parsed = v;
        if (*data=='\\') parsed = kno_mkstring(data+1);
        else if ((*data==':')&&(data[1]=='\0')) {kno_incref(parsed);}
        else if (*data==':')
          parsed = kno_parse(data+1);
        else if ((isdigit(*data))||(*data=='+')||(*data=='-')||(*data=='.')) {
          parsed = kno_parse_arg(data);
          if (!(NUMBERP(parsed))) {
            kno_decref(parsed); parsed = v; kno_incref(parsed);}}
        else if ( (noparse==0) && (strchr("@{#(",data[0])) )
          parsed = kno_parse_arg(data);
        else kno_incref(parsed);
        if (KNO_ABORTP(parsed)) {
          u8_log(LOG_WARN,kno_ParseArgError,"Bad LISP arg '%s'",data);
          kno_clear_errors(1);
          parsed = v; kno_incref(v);}
        CHOICE_ADD(result,parsed);}}
    kno_decref(val);
    return result;}
  else return val;
}

/* The init function */

KNO_DCLPRIM1("req/call",reqcall_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(REQ/CALL *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval reqcall_prim(lispval proc)
{
  lispval value = VOID;
  if (!(KNO_APPLICABLEP(proc)))
    value = kno_type_error("applicable","cgicall",proc);
  else {
    lispval fn = kno_fcnid_ref(proc);
    if (KNO_LAMBDAP(fn))
      value=kno_xapply_lambda((kno_lambda)fn,
                             (void *)VOID,
                             (lispval (*)(void *,lispval))reqgetvar);
    else value = kno_apply(fn,0,NULL);}
  return value;
}

KNO_DCLPRIM2("req/get",reqget_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(REQ/GET *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval reqget_prim(lispval vars,lispval dflt)
{
  lispval results = EMPTY; int found = 0;
  DO_CHOICES(var,vars) {
    lispval name = ((STRINGP(var))?(kno_intern(CSTRING(var))):(var));
    lispval val = kno_req_get(name,VOID);
    if (!(VOIDP(val))) {
      found = 1; CHOICE_ADD(results,val);}}
  if (found) return kno_simplify_choice(results);
  else if (VOIDP(dflt)) return EMPTY;
  else if (QCHOICEP(dflt)) {
    struct KNO_QCHOICE *qc = KNO_XQCHOICE(dflt);
    return kno_make_simple_choice(qc->qchoiceval);}
  else return kno_incref(dflt);
}

KNO_DCLPRIM2("req/val",reqval_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(REQ/VAL *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval reqval_prim(lispval vars,lispval dflt)
{
  lispval results = EMPTY; int found = 0;
  DO_CHOICES(var,vars) {
    lispval val;
    if (STRINGP(var)) var = kno_intern(CSTRING(var));
    val = kno_req_get(var,VOID);
    if (VOIDP(val)) {}
    else if (STRINGP(val)) {
      lispval parsed = kno_parse_arg(CSTRING(val));
      kno_decref(val);
      CHOICE_ADD(results,parsed);
      found = 1;}
    else if (CHOICEP(val)) {
      DO_CHOICES(v,val) {
        if (STRINGP(v)) {
          lispval parsed = kno_parse_arg(CSTRING(v));
          CHOICE_ADD(results,parsed);}
        else {
          kno_incref(v); CHOICE_ADD(results,v);}}
      kno_decref(val);
      found = 1;}
    else {
      CHOICE_ADD(results,val);
      found = 1;}}
  if (found) return results;
  else if (VOIDP(dflt)) return EMPTY;
  else if (QCHOICEP(dflt)) {
    struct KNO_QCHOICE *qc = KNO_XQCHOICE(dflt);
    return kno_make_simple_choice(qc->qchoiceval);}
  else return kno_incref(dflt);
}

#if 0
static lispval hashcolon_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1);
  if (VOIDP(var))
    return kno_err(kno_SyntaxError,"hashcolon_evalfn",NULL,expr);
  else return reqget_prim(var,EMPTY);
}

static lispval hashcoloncolon_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1);
  if (VOIDP(var))
    return kno_err(kno_SyntaxError,"hashcoloncolon_evalfn",NULL,expr);
  else {
    lispval val = reqget_prim(var,EMPTY);
    if (STRINGP(val)) {
      lispval result = kno_parse_arg(CSTRING(val));
      kno_decref(val);
      return result;}
    else return val;}
}

static lispval hashcolondollar_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1);
  if (VOIDP(var))
    return kno_err(kno_SyntaxError,"hashcoloncolon_evalfn",NULL,expr);
  else {
    lispval val = reqget_prim(var,VOID);
    if (STRINGP(val)) {
      lispval result = kno_parse_arg(CSTRING(val));
      kno_decref(val);
      return result;}
    else if (VOIDP(val))
      return kno_make_string(NULL,0,"");
    else {
      lispval result; struct U8_OUTPUT out;
      U8_INIT_OUTPUT(&out,64); kno_unparse(&out,val);
      result = kno_stream_string(&out);
      u8_free(out.u8_outbuf);
      return result;}}
}

static lispval hashcolonquestion_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1);
  if (VOIDP(var))
    return kno_err(kno_SyntaxError,"hashcoloncolon_evalfn",NULL,expr);
  else if (kno_req_test(var,VOID))
    return KNO_TRUE;
  else return KNO_FALSE;
}
#endif


KNO_DCLPRIM2("req/test",reqtest_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
 "`(REQ/TEST *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval reqtest_prim(lispval vars,lispval val)
{
  DO_CHOICES(var,vars) {
    lispval name = ((STRINGP(var))?(kno_intern(CSTRING(var))):(var));
    int retval = kno_req_test(name,val);
    if (retval<0) {
      KNO_STOP_DO_CHOICES;
      return KNO_ERROR;}
    else if (retval) {
      KNO_STOP_DO_CHOICES;
      return KNO_TRUE;}
    else {}}
  return KNO_FALSE;
}

KNO_DCLPRIM2("req/store!",reqstore_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
 "`(REQ/STORE! *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval reqstore_prim(lispval vars,lispval value)
{
  {DO_CHOICES(var,vars) {
      lispval name = ((STRINGP(var))?(kno_intern(CSTRING(var))):(var));
      kno_req_store(name,value);}}
  return VOID;
}

KNO_DCLPRIM2("req/add!",reqadd_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(REQ/ADD! *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval reqadd_prim(lispval vars,lispval value)
{
  {DO_CHOICES(var,vars) {
      lispval name = ((STRINGP(var))?(kno_intern(CSTRING(var))):(var));
      kno_req_add(name,value);}}
  return VOID;
}

KNO_DCLPRIM2("req/drop!",reqdrop_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
 "`(REQ/DROP! *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval reqdrop_prim(lispval vars,lispval value)
{
  {DO_CHOICES(var,vars) {
      lispval name = ((STRINGP(var))?(kno_intern(CSTRING(var))):(var));
      kno_req_drop(name,value);}}
  return VOID;
}

KNO_DCLPRIM2("req/push!",reqpush_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(REQ/PUSH! *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval reqpush_prim(lispval vars,lispval values)
{
  {DO_CHOICES(var,vars) {
      lispval name = ((STRINGP(var))?(kno_intern(CSTRING(var))):(var));
      DO_CHOICES(value,values) {
        kno_req_push(name,value);}}}
  return VOID;
}
KNO_DCLPRIM("req/data",reqdata_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
 "`(REQ/DATA)` **undocumented**");

lispval reqdata_prim()
{
  return kno_req_call(kno_deep_copy);
}

static lispval withreq_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval reqdata_expr = kno_get_arg(expr,1);
  lispval reqdata = kno_stack_eval(reqdata_expr,env,_stack,0);
  lispval body = kno_get_body(expr,2), result = VOID;
  kno_use_reqinfo(reqdata); kno_reqlog(1);
  {KNO_DOLIST(ex,body) {
      if (KNO_ABORTP(result)) {
        kno_use_reqinfo(EMPTY);
        kno_reqlog(-1);
        return result;}
      kno_decref(result);
      result = kno_eval(ex,env);}}
  kno_use_reqinfo(EMPTY);
  kno_decref(reqdata);
  kno_reqlog(-1);
  return result;
}

KNO_DCLPRIM("req/live?",req_livep_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
 "`(REQ/LIVE?)` **undocumented**");
static lispval req_livep_prim()
{
  if (kno_isreqlive()) return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM("req/getlog",reqgetlog_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
            "`(REQ/GETLOG)` **undocumented**");
KNO_EXPORT lispval reqgetlog_prim()
{
  struct U8_OUTPUT *log = kno_reqlog(0);
  if (!(log)) return KNO_FALSE;
  else {
    int len = log->u8_write-log->u8_outbuf;
    if (len==0) return KNO_FALSE;
    else return kno_make_string(NULL,len,log->u8_outbuf);}
}

KNO_DCLPRIM("req/loglen",reqloglen_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
 "`(REQ/LOGLEN)` **undocumented**");
KNO_EXPORT lispval reqloglen_prim()
{
  struct U8_OUTPUT *log = kno_reqlog(0);
  if (!(log)) return KNO_FALSE;
  else {
    int len = log->u8_write-log->u8_outbuf;
    return KNO_INT(len);}
}

KNO_EXPORT lispval reqlog_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  struct U8_XTIME xt;
  struct U8_OUTPUT *reqout = kno_reqlog(1);
  u8_string cond = NULL, cxt = NULL;
  long long body_off = 1, level = -1;
  lispval arg1 = kno_get_arg(expr,1), arg2 = kno_get_arg(expr,2);
  lispval arg3 = kno_get_arg(expr,3), body, outval;
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
  body = kno_get_body(expr,body_off);
  outval = kno_printout_to(reqout,body,env);
  u8_printf(reqout,"\n\t</message>");
  u8_printf(reqout,"\n</logentry>\n");
  kno_decref(body);
  if (KNO_ABORTP(outval)) {
    return outval;}
  else return VOID;
}

KNO_EXPORT void kno_init_reqstate_c()
{
  lispval module = kno_scheme_module;

  u8_register_source_file(_FILEINFO);

  init_local_cprims();

#if 0
  kno_idefn(module,kno_make_cprim1("REQ/CALL",reqcall_prim,1));
  kno_idefn(module,kno_make_ndprim(kno_make_cprim2("REQ/GET",reqget_prim,1)));
  kno_idefn(module,kno_make_ndprim(kno_make_cprim2("REQ/VAL",reqval_prim,1)));
  kno_idefn(module,kno_make_ndprim(kno_make_cprim2("REQ/TEST",reqtest_prim,1)));
  kno_idefn(module,kno_make_ndprim(kno_make_cprim2("REQ/STORE!",reqstore_prim,2)));
  kno_defalias(module,"REQ/SET!","REQ/STORE!");
  kno_idefn(module,kno_make_cprim2("REQ/ADD!",reqadd_prim,2));
  kno_idefn(module,kno_make_cprim2("REQ/DROP!",reqdrop_prim,1));
  kno_idefn(module,kno_make_cprim2("REQ/PUSH!",reqpush_prim,2));
  kno_idefn(module,kno_make_cprim0("REQ/LIVE?",req_livep_prim));
#endif
#if 0
  kno_def_evalfn(module,"#:","",hashcolon_evalfn);
  kno_def_evalfn(module,"#::","",hashcoloncolon_evalfn);
  kno_def_evalfn(module,"#:$","",hashcolondollar_evalfn);
  kno_def_evalfn(module,"#:?","",hashcolonquestion_evalfn);
#endif

  kno_def_evalfn(module,"REQ/LOG","",reqlog_evalfn);
  kno_defalias(module,"REQLOG","REQ/LOG");
  kno_defalias(module,"REQ/LOG!","REQ/LOG");

#if 0
  kno_idefn(module,kno_make_cprim0("REQ/GETLOG",reqgetlog_prim));
  kno_idefn(module,kno_make_cprim0("REQ/LOGLEN",reqloglen_prim));

  kno_idefn(module,kno_make_cprim0("REQ/DATA",reqdata_prim));
#endif

  kno_def_evalfn(module,"WITH/REQUEST","",withreq_evalfn);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/


static void init_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_PRIM("req/live?",req_livep_prim,0,scheme_module);
  KNO_LINK_PRIM("req/data",reqdata_prim,0,scheme_module);
  KNO_LINK_PRIM("req/push!",reqpush_prim,2,scheme_module);
  KNO_LINK_PRIM("req/drop!",reqdrop_prim,2,scheme_module);
  KNO_LINK_PRIM("req/add!",reqadd_prim,2,scheme_module);
  KNO_LINK_PRIM("req/store!",reqstore_prim,2,scheme_module);
  KNO_LINK_PRIM("req/test",reqtest_prim,2,scheme_module);
  KNO_LINK_PRIM("req/val",reqval_prim,2,scheme_module);
  KNO_LINK_PRIM("req/get",reqget_prim,2,scheme_module);
  KNO_LINK_PRIM("req/call",reqcall_prim,1,scheme_module);
  KNO_LINK_PRIM("req/getlog",reqgetlog_prim,0,scheme_module);
  KNO_LINK_PRIM("req/getloglen",reqloglen_prim,0,scheme_module);

  KNO_DECL_ALIAS("req/set!",reqstore_prim,scheme_module);
}
