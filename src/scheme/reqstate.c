/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/frames.h"
#include "framerd/dtypestream.h"
#include "framerd/dtypeio.h"

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
	FD_ADD_TO_CHOICE(result,v); fd_incref(v);}
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
  if (FD_PTR_TYPEP(proc,fd_sproc_type)) 
    value=
      fd_xapply_sproc((fd_sproc)proc,(void *)FD_VOID,
		      (fdtype (*)(void *,fdtype))reqgetvar);
  else if (FD_APPLICABLEP(proc))
    value=fd_apply(proc,0,NULL);
  else value=fd_type_error("applicable","cgicall",proc);
  return value;
}

static fdtype reqget_prim(fdtype var,fdtype dflt)
{
  fdtype name=((FD_STRINGP(var))?(fd_intern(FD_STRDATA(var))):(var));
  fdtype val=fd_req_get(name,FD_VOID);
  if (FD_VOIDP(val))
    if (FD_VOIDP(dflt)) return FD_EMPTY_CHOICE;
    else return fd_incref(dflt);
  else return val;
}

static fdtype reqval_prim(fdtype var,fdtype dflt)
{
  fdtype val;
  if (FD_STRINGP(var)) var=fd_intern(FD_STRDATA(var));
  val=fd_req_get(var,FD_VOID);
  if (FD_VOIDP(val))
    if (FD_VOIDP(dflt))
      return FD_EMPTY_CHOICE;
    else return fd_incref(dflt);
  else if (FD_STRINGP(val)) {
    fdtype parsed=fd_parse_arg(FD_STRDATA(val));
    fd_decref(val);
    return parsed;}
  else if (FD_CHOICEP(val)) {
    fdtype result=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(v,val)
      if (FD_STRINGP(v)) {
	fdtype parsed=fd_parse_arg(FD_STRDATA(v));
	FD_ADD_TO_CHOICE(result,parsed);}
      else {fd_incref(v); FD_ADD_TO_CHOICE(result,v);}
    fd_decref(val);
    return result;}
  else return val;
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
	/* fd_req_push is weird because it doesn't incref its arg */
	fd_incref(value); fd_req_push(name,value);}}}
  return FD_VOID;
}

fdtype reqdata_prim()
{
  return fd_req_call(fd_deep_copy);
}

static fdtype withreq_handler(fdtype expr,fd_lispenv env)
{
  fdtype body=fd_get_body(expr,1), result=FD_VOID;
  fd_use_reqinfo(FD_TRUE);
  {FD_DOLIST(ex,body) {
      if (FD_ABORTP(result)) return result;
      fd_decref(result);
      result=fd_eval(ex,env);}}
  fd_use_reqinfo(FD_EMPTY_CHOICE);
  return result;
}

static fdtype req_livep_prim()
{
  if (fd_isreqlive()) return FD_TRUE;
  else return FD_FALSE;
}

FD_EXPORT void fd_init_reqstate_c()
{
  fdtype module=fd_scheme_module;

  u8_register_source_file(_FILEINFO);

  fd_idefn(module,fd_make_cprim1("REQ/CALL",reqcall_prim,1));
  fd_idefn(module,fd_make_cprim2("REQ/GET",reqget_prim,1));
  fd_idefn(module,fd_make_cprim2("REQ/VAL",reqval_prim,1));
  fd_idefn(module,fd_make_ndprim(fd_make_cprim2("REQ/TEST",reqtest_prim,1)));
  fd_idefn(module,fd_make_ndprim(fd_make_cprim2("REQ/SET!",reqset_prim,2)));
  fd_idefn(module,fd_make_cprim2("REQ/ADD!",reqadd_prim,2));
  fd_idefn(module,fd_make_cprim2("REQ/DROP!",reqdrop_prim,1));
  fd_idefn(module,fd_make_cprim2("REQ/PUSH!",reqpush_prim,2));
  fd_idefn(module,fd_make_cprim0("REQ/LIVE?",req_livep_prim,2));

  fd_idefn(module,fd_make_cprim0("REQ/DATA",reqdata_prim,0));

  fd_idefn(module,fd_make_cprim0("REQ/DATA",reqdata_prim,0));
  fd_defspecial(module,"WITH/REQUEST",withreq_handler);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
