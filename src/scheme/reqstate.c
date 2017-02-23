/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/frames.h"
#include "framerd/stream.h"
#include "framerd/dtypeio.h"
#include "framerd/ports.h"

#include <ctype.h>

static fdtype reqgetvar(fdtype cgidata,fdtype var)
{
  int noparse=
    ((FD_SYMBOLP(var))&&((FD_SYMBOL_NAME(var))[0]=='%'));
  fdtype name=((noparse)?(fd_intern(FD_SYMBOL_NAME(var)+1)):(var));
  fdtype val=((FD_TABLEP(cgidata))?(fd_get(cgidata,name,FD_VOID)):
              (fd_req_get(name,FD_VOID)));
  if (FD_VOIDP(val)) return val;
  else if ((noparse)&&(FD_STRINGP(val))) return val;
  else if (FD_STRINGP(val)) {
    u8_string data=FD_STRDATA(val);
    if (*data=='\0') return val;
    else if (strchr("@{#(",data[0])) {
      fdtype parsed=fd_parse_arg(data);
      fd_decref(val); return parsed;}
    else if (isdigit(data[0])) {
      fdtype parsed=fd_parse_arg(data);
      if (FD_NUMBERP(parsed)) {
        fd_decref(val); return parsed;}
      else {
        fd_decref(parsed); return val;}}
    else if (*data == ':')
      if (data[1]=='\0')
        return fdtype_string(data);
      else {
        fdtype arg=fd_parse(data+1);
        if (FD_ABORTP(arg)) {
          u8_log(LOG_WARN,fd_ParseArgError,"Bad colon spec arg '%s'",arg);
          fd_clear_errors(1);
          return fdtype_string(data);}
        else return arg;}
    else if (*data == '\\') {
      fdtype shorter=fdtype_string(data+1);
      fd_decref(val);
      return shorter;}
    else return val;}
  else if ((FD_CHOICEP(val))||(FD_ACHOICEP(val))) {
    fdtype result=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(v,val) {
      if (!(FD_STRINGP(v))) {
        fd_incref(v); FD_ADD_TO_CHOICE(result,v);}
      else {
        u8_string data=FD_STRDATA(v); fdtype parsed=v;
        if (*data=='\\') parsed=fdtype_string(data+1);
        else if ((*data==':')&&(data[1]=='\0')) {fd_incref(parsed);}
        else if (*data==':')
          parsed=fd_parse(data+1);
        else if ((isdigit(*data))||(*data=='+')||(*data=='-')||(*data=='.')) {
          parsed=fd_parse_arg(data);
          if (!(FD_NUMBERP(parsed))) {
            fd_decref(parsed); parsed=v; fd_incref(parsed);}}
        else if (strchr("@{#(",data[0]))
          parsed=fd_parse_arg(data);
        else fd_incref(parsed);
        if (FD_ABORTP(parsed)) {
          u8_log(LOG_WARN,fd_ParseArgError,"Bad LISP arg '%s'",data);
          fd_clear_errors(1);
          parsed=v; fd_incref(v);}
        FD_ADD_TO_CHOICE(result,parsed);}}
    fd_decref(val);
    return result;}
  else return val;
}

/* The init function */

static fdtype reqcall_prim(fdtype proc)
{
  fdtype value=FD_VOID;
  if (FD_SPROCP(proc))
    value=
      fd_xapply_sproc((fd_sproc)proc,(void *)FD_VOID,
                      (fdtype (*)(void *,fdtype))reqgetvar);
  else if (FD_APPLICABLEP(proc))
    value=fd_apply(proc,0,NULL);
  else value=fd_type_error("applicable","cgicall",proc);
  return value;
}

static fdtype reqget_prim(fdtype vars,fdtype dflt)
{
  fdtype results=FD_EMPTY_CHOICE; int found=0;
  FD_DO_CHOICES(var,vars) {
    fdtype name=((FD_STRINGP(var))?(fd_intern(FD_STRDATA(var))):(var));
    fdtype val=fd_req_get(name,FD_VOID);
    if (!(FD_VOIDP(val))) {
      found=1; FD_ADD_TO_CHOICE(results,val);}}
  if (found) return fd_simplify_choice(results);
  else if (FD_VOIDP(dflt)) return FD_EMPTY_CHOICE;
  else if (FD_QCHOICEP(dflt)) {
    struct FD_QCHOICE *qc=FD_XQCHOICE(dflt);
    return fd_make_simple_choice(qc->fd_choiceval);}
  else return fd_incref(dflt);
}

static fdtype reqval_prim(fdtype vars,fdtype dflt)
{
  fdtype results=FD_EMPTY_CHOICE; int found=0;
  FD_DO_CHOICES(var,vars) {
    fdtype val;
    if (FD_STRINGP(var)) var=fd_intern(FD_STRDATA(var));
    val=fd_req_get(var,FD_VOID);
    if (FD_VOIDP(val)) {}
    else if (FD_STRINGP(val)) {
      fdtype parsed=fd_parse_arg(FD_STRDATA(val));
      fd_decref(val);
      FD_ADD_TO_CHOICE(results,parsed);
      found=1;}
    else if (FD_CHOICEP(val)) {
      FD_DO_CHOICES(v,val) {
        if (FD_STRINGP(v)) {
          fdtype parsed=fd_parse_arg(FD_STRDATA(v));
          FD_ADD_TO_CHOICE(results,parsed);}
        else {
          fd_incref(v); FD_ADD_TO_CHOICE(results,v);}}
      fd_decref(val);
      found=1;}
    else {
      FD_ADD_TO_CHOICE(results,val);
      found=1;}}
  if (found) return results;
  else if (FD_VOIDP(dflt)) return FD_EMPTY_CHOICE;
  else if (FD_QCHOICEP(dflt)) {
    struct FD_QCHOICE *qc=FD_XQCHOICE(dflt);
    return fd_make_simple_choice(qc->fd_choiceval);}
  else return fd_incref(dflt);
}

static fdtype hashcolon_handler(fdtype expr,fd_lispenv env)
{
  fdtype var=fd_get_arg(expr,1);
  if (FD_VOIDP(var))
    return fd_err(fd_SyntaxError,"hashcolon_handler",NULL,expr);
  else return reqget_prim(var,FD_EMPTY_CHOICE);
}

static fdtype hashcoloncolon_handler(fdtype expr,fd_lispenv env)
{
  fdtype var=fd_get_arg(expr,1);
  if (FD_VOIDP(var))
    return fd_err(fd_SyntaxError,"hashcoloncolon_handler",NULL,expr);
  else {
    fdtype val=reqget_prim(var,FD_EMPTY_CHOICE);
    if (FD_STRINGP(val)) {
      fdtype result=fd_parse_arg(FD_STRDATA(val));
      fd_decref(val);
      return result;}
    else return val;}
}

static fdtype hashcolondollar_handler(fdtype expr,fd_lispenv env)
{
  fdtype var=fd_get_arg(expr,1);
  if (FD_VOIDP(var))
    return fd_err(fd_SyntaxError,"hashcoloncolon_handler",NULL,expr);
  else {
    fdtype val=reqget_prim(var,FD_VOID);
    if (FD_STRINGP(val)) {
      fdtype result=fd_parse_arg(FD_STRDATA(val));
      fd_decref(val);
      return result;}
    else if (FD_VOIDP(val))
      return fd_make_string(NULL,0,"");
    else {
      fdtype result; struct U8_OUTPUT out;
      U8_INIT_OUTPUT(&out,64); fd_unparse(&out,val);
      result=fd_stream_string(&out);
      u8_free(out.u8_outbuf);
      return result;}}
}

static fdtype hashcolonquestion_handler(fdtype expr,fd_lispenv env)
{
  fdtype var=fd_get_arg(expr,1);
  if (FD_VOIDP(var))
    return fd_err(fd_SyntaxError,"hashcoloncolon_handler",NULL,expr);
  else if (fd_req_test(var,FD_VOID))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype reqtest_prim(fdtype vars,fdtype val)
{
  FD_DO_CHOICES(var,vars) {
    fdtype name=((FD_STRINGP(var))?(fd_intern(FD_STRDATA(var))):(var));
    int retval=fd_req_test(name,val);
    if (retval<0) {
      FD_STOP_DO_CHOICES;
      return FD_ERROR_VALUE;}
    else if (retval) {
      FD_STOP_DO_CHOICES;
      return FD_TRUE;}
    else {}}
  return FD_FALSE;
}

static fdtype reqset_prim(fdtype vars,fdtype value)
{
  {FD_DO_CHOICES(var,vars) {
      fdtype name=((FD_STRINGP(var))?(fd_intern(FD_STRDATA(var))):(var));
      fd_req_store(name,value);}}
  return FD_VOID;
}

static fdtype reqadd_prim(fdtype vars,fdtype value)
{
  {FD_DO_CHOICES(var,vars) {
      fdtype name=((FD_STRINGP(var))?(fd_intern(FD_STRDATA(var))):(var));
      fd_req_add(name,value);}}
  return FD_VOID;
}

static fdtype reqdrop_prim(fdtype vars,fdtype value)
{
  {FD_DO_CHOICES(var,vars) {
      fdtype name=((FD_STRINGP(var))?(fd_intern(FD_STRDATA(var))):(var));
      fd_req_drop(name,value);}}
  return FD_VOID;
}

static fdtype reqpush_prim(fdtype vars,fdtype values)
{
  {FD_DO_CHOICES(var,vars) {
      fdtype name=((FD_STRINGP(var))?(fd_intern(FD_STRDATA(var))):(var));
      FD_DO_CHOICES(value,values) {
        fd_req_push(name,value);}}}
  return FD_VOID;
}

fdtype reqdata_prim()
{
  return fd_req_call(fd_deep_copy);
}

static fdtype withreq_handler(fdtype expr,fd_lispenv env)
{
  fdtype body=fd_get_body(expr,1), result=FD_VOID;
  fd_use_reqinfo(FD_TRUE); fd_reqlog(1);
  {FD_DOLIST(ex,body) {
      if (FD_ABORTP(result)) {
        fd_use_reqinfo(FD_EMPTY_CHOICE);
        fd_reqlog(-1);
        return result;}
      fd_decref(result);
      result=fd_eval(ex,env);}}
  fd_use_reqinfo(FD_EMPTY_CHOICE);
  fd_reqlog(-1);
  return result;
}

static fdtype req_livep_prim()
{
  if (fd_isreqlive()) return FD_TRUE;
  else return FD_FALSE;
}

FD_EXPORT fdtype reqgetlog_prim()
{
  struct U8_OUTPUT *log=fd_reqlog(0);
  if (!(log)) return FD_FALSE;
  else {
    int len=log->u8_write-log->u8_outbuf;
    if (len==0) return FD_FALSE;
    else return fd_make_string(NULL,len,log->u8_outbuf);}
}

FD_EXPORT fdtype reqloglen_prim()
{
  struct U8_OUTPUT *log=fd_reqlog(0);
  if (!(log)) return FD_FALSE;
  else {
    int len=log->u8_write-log->u8_outbuf;
    return FD_INT(len);}
}

FD_EXPORT fdtype reqlog_handler(fdtype expr,fd_lispenv env)
{
  struct U8_XTIME xt;
  struct U8_OUTPUT *reqout=fd_reqlog(1);
  u8_string cond=NULL, cxt=NULL; int body_off=1, level=-1;
  fdtype arg1=fd_get_arg(expr,1), arg2=fd_get_arg(expr,2);
  fdtype arg3=fd_get_arg(expr,3), body, outval;
  if (FD_FIXNUMP(arg1)) {
    level=FD_FIX2INT(arg1); body_off++;
    arg1=arg2; arg2=arg3; arg3=FD_VOID;}
  if (FD_SYMBOLP(arg1)) {
    cond=FD_SYMBOL_NAME(arg1); body_off++;
    arg1=arg2; arg2=arg3; arg3=FD_VOID;}
  if (FD_SYMBOLP(arg1)) {
    cxt=FD_SYMBOL_NAME(arg2); body_off++;}
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
  body=fd_get_body(expr,body_off);
  outval=fd_printout_to(reqout,body,env);
  u8_printf(reqout,"\n\t</message>");
  u8_printf(reqout,"\n</logentry>\n");
  fd_decref(body);
  if (FD_ABORTP(outval)) {
    return outval;}
  else return FD_VOID;
}

FD_EXPORT void fd_init_reqstate_c()
{
  fdtype module=fd_scheme_module;

  u8_register_source_file(_FILEINFO);

  fd_idefn(module,fd_make_cprim1("REQ/CALL",reqcall_prim,1));
  fd_idefn(module,fd_make_ndprim(fd_make_cprim2("REQ/GET",reqget_prim,1)));
  fd_idefn(module,fd_make_ndprim(fd_make_cprim2("REQ/VAL",reqval_prim,1)));
  fd_idefn(module,fd_make_ndprim(fd_make_cprim2("REQ/TEST",reqtest_prim,1)));
  fd_idefn(module,fd_make_ndprim(fd_make_cprim2("REQ/SET!",reqset_prim,2)));
  fd_idefn(module,fd_make_cprim2("REQ/ADD!",reqadd_prim,2));
  fd_idefn(module,fd_make_cprim2("REQ/DROP!",reqdrop_prim,1));
  fd_idefn(module,fd_make_cprim2("REQ/PUSH!",reqpush_prim,2));
  fd_idefn(module,fd_make_cprim0("REQ/LIVE?",req_livep_prim,2));
  fd_defspecial(module,"#:",hashcolon_handler);
  fd_defspecial(module,"#::",hashcoloncolon_handler);
  fd_defspecial(module,"#:$",hashcolondollar_handler);
  fd_defspecial(module,"#:?",hashcolonquestion_handler);

  fd_defspecial(module,"REQLOG",reqlog_handler);
  fd_defspecial(module,"REQ/LOG!",reqlog_handler);
  fd_idefn(module,fd_make_cprim0("REQ/GETLOG",reqgetlog_prim,0));
  fd_idefn(module,fd_make_cprim0("REQ/LOGLEN",reqloglen_prim,0));

  fd_idefn(module,fd_make_cprim0("REQ/DATA",reqdata_prim,0));

  fd_idefn(module,fd_make_cprim0("REQ/DATA",reqdata_prim,0));
  fd_defspecial(module,"WITH/REQUEST",withreq_handler);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
