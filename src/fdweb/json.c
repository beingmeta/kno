/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2008 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define U8_INLINE_IO 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/tables.h"
#include "fdb/ports.h"
#include "fdb/fdweb.h"

#include <libu8/xfiles.h>

#include <ctype.h>
#define FD_JSON_IDS_OK 2
#define FD_JSON_INTERN_KEYS 4

static fd_exception JSON_Error="JSON Parsing Error";

static int readc(U8_INPUT *in)
{
  int c=u8_getc(in);
  if (c=='\\') {
    u8_ungetc(in,c);
    return fd_read_escape(in);}
  else return c;
}

static int skip_whitespace(U8_INPUT *in)
{
  int c=u8_getc(in);
  while ((c>0) && (u8_isspace(c))) c=u8_getc(in);
  if (c>0) u8_ungetc(in,c);
  return c;
}

static fdtype json_parse(U8_INPUT *in,int flags);
static fdtype json_vector(U8_INPUT *in,int flags);
static fdtype json_table(U8_INPUT *in,int flags);
static fdtype json_string(U8_INPUT *in,int flags);
static fdtype json_atom(U8_INPUT *in,int flags);

static fdtype json_parse(U8_INPUT *in,int flags)
{
  int c=skip_whitespace(in);
  if (c=='[') return json_vector(in,flags);
  else if (c=='{') return json_table(in,flags);
  else if (c=='"') return json_string(in,flags);
  else return json_atom(in,0);
}

static fdtype json_atom(U8_INPUT *in,int flags)
{
  fdtype result;
  struct U8_OUTPUT out; u8_byte _buf[256]; int c=readc(in);
  U8_INIT_OUTPUT_BUF(&out,256,_buf);
  while ((u8_isalnum(c)) || (c=='-') || (c=='_') || (c=='+')) {
    u8_putc(&out,c); c=readc(in);}
  if (c>=0) u8_ungetc(in,c);
  result=fd_parse(out.u8_outbuf);
  if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);
  return result;
}

static fdtype json_vector(U8_INPUT *in,int flags)
{
  int n_elts=0, max_elts=16, c, i;
  fdtype *elts;
  if (u8_getc(in)!='[') return FD_ERROR_VALUE;
  else elts=u8_alloc_n(16,fdtype);
  c=skip_whitespace(in);
  while (c>=0)
    if (c==']') {
      u8_getc(in); /* Absorb ] */
      return fd_init_vector(NULL,n_elts,elts);}
    else if (c==',') {
      c=u8_getc(in); c=skip_whitespace(in);}
    else {
      fdtype elt;
      if (n_elts==max_elts)  {
	int new_max=max_elts*2;
	fdtype *newelts=u8_realloc(elts,sizeof(fdtype)*new_max);
	if (newelts) {elts=newelts; max_elts=new_max;}
	else {
	  u8_seterr(fd_MallocFailed,"json_vector",NULL);
	  break;}}
      elts[n_elts++]=elt=json_parse(in,flags);
      if (FD_ABORTP(elt)) break;
      c=skip_whitespace(in);}
  i=0; while (i<n_elts) {fd_decref(elts[i]); i++;}
  return FD_ERROR_VALUE;
}

static fdtype json_table(U8_INPUT *in,int flags)
{
  int n_elts=0, max_elts=16, c, i;
  u8_condition err=NULL;
  struct FD_KEYVAL *kv;
  if (u8_getc(in)!='{') return FD_ERROR_VALUE;
  else kv=u8_alloc_n(16,struct FD_KEYVAL);
  c=skip_whitespace(in);
  while (c>=0)
    if (c=='}') {
      u8_getc(in); /* Absorb ] */
      return fd_init_slotmap(NULL,n_elts,kv);}
    else if (c==',') {
      c=u8_getc(in); c=skip_whitespace(in);}
    else {
      if (n_elts==max_elts)  {
	int new_max=max_elts*2;
	struct FD_KEYVAL *newelts=u8_realloc(kv,sizeof(fdtype)*new_max);
	if (newelts) {kv=newelts; max_elts=new_max;}
	else {
	  u8_seterr(fd_MallocFailed,"json_table",NULL);
	  break;}}
      kv[n_elts].key=json_parse(in,flags);
      if (FD_ABORTP(kv[n_elts].key)) break;
      c=skip_whitespace(in);
      if (c==':') c=u8_getc(in);
      else return FD_EOD;
      kv[n_elts].value=json_parse(in,flags);
      if (FD_ABORTP(kv[n_elts].value)) break;
      n_elts++; c=skip_whitespace(in);}
  i=0; while (i<n_elts) {
    fd_decref(kv[i].key); fd_decref(kv[i].value); i++;}
  return FD_ERROR_VALUE;
}

static fdtype json_string(U8_INPUT *in,int flags)
{
  struct U8_OUTPUT out; int c=readc(in); /* Skip '"' */
  U8_INIT_OUTPUT(&out,16);
  c=u8_getc(in);
  while (c>=0) {
    if (c=='"') break;
    else if (c=='\\') {
      c=fd_read_escape(in);
      u8_putc(&out,c); c=u8_getc(in);
      continue;}
    u8_putc(&out,c);
    c=u8_getc(in);}
  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
}

static fdtype jsonparseprim(fdtype in)
{
  if (FD_PRIM_TYPEP(in,fd_port_type)) {
    struct FD_PORT *p=FD_GET_CONS(in,fd_port_type,struct FD_PORT *);
    U8_INPUT *in=p->in;
    return json_parse(in,0);}
  else if (FD_STRINGP(in)) {
    struct U8_INPUT inport;
    U8_INIT_STRING_INPUT(&inport,FD_STRLEN(in),FD_STRDATA(in));
    return json_parse(&inport,0);}
  else if (FD_PACKETP(in)) {
    struct U8_INPUT inport;
    U8_INIT_STRING_INPUT(&inport,FD_PACKET_LENGTH(in),FD_PACKET_DATA(in));
    return json_parse(&inport,0);}
  else return fd_type_error("string or stream","jsonparse",in);
}

/* Module initialization */

FD_EXPORT void fd_init_json_c()
{
  fdtype module=fd_new_module("FDWEB",(FD_MODULE_SAFE));
  fdtype unsafe_module=fd_new_module("FDWEB",0);

  fd_idefn(module,fd_make_cprim1("JSONPARSE",jsonparseprim,1));
  fd_idefn(unsafe_module,fd_make_cprim1("JSONPARSE",jsonparseprim,1));

  fd_register_source_file(versionid);
}
