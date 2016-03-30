/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/eval.h"
#include "framerd/fdweb.h"
#include "framerd/ports.h"

#include <libu8/libu8io.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8netfns.h>

#if HAVE_RESOLV_H
#include <sys/types.h>
#if HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif
#if HAVE_ARPA_NAMESER_H
#include <arpa/nameser.h>
#endif
#include <resolv.h>
#endif

#if HAVE_RES_QUERY
static int get_type(const char *name);

static fdtype dnsreq_prim(fdtype name,fdtype rectype)
{
  char buf[1024];
  const char *typename=FD_SYMBOL_NAME(rectype);
  int type=get_type(typename);
  int retval=res_query(FD_STRDATA(name),ns_c_any,type,buf,1024);
  if (retval<0) return FD_FALSE;
  else return fdtype_string(buf);
}

static int get_type(const char *name)
{
  if (strcmp(name,"A")) return ns_t_a;
  else if (strcmp(name,"ADDR")==0) return ns_t_a;
  else if (strcmp(name,"CNAME")==0) return ns_t_cname;
  else if (strcmp(name,"NAMESERVER")==0) return ns_t_ns;
  else if (strcmp(name,"MAILDESTINATION")==0) return ns_t_md;
  else if (strcmp(name,"MAILFORWARDER")==0) return ns_t_mf;
  else if (strcmp(name,"MX")==0) return ns_t_mx;
  else if (strcmp(name,"TXT")==0) return ns_t_txt;
  else if (strcmp(name,"PERSON")==0) return ns_t_rp;
  else if (strcmp(name,"IPV6")==0) return ns_t_a6;
  else return -1;
}

static int dnsreq_initialized=0;

FD_EXPORT void fd_init_dnsreq_c()
{
  fdtype module;
  if (dnsreq_initialized) return;
  dnsreq_initialized=1;
  fd_init_fdscheme();
  res_init();

  module=fd_new_module("FDWEB",(0));

  fd_idefn(module,fd_make_cprim2x
	   ("DNS/REQ",dnsreq_prim,2,
	    fd_string_type,-1,fd_symbol_type,-1));


  u8_register_source_file(_FILEINFO);
}
#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/

