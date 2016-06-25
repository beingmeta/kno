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

#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/nameser.h>
#include <resolv.h>

static int dns_initialized=0;

#ifndef NSBUF_SIZE
#define NSBUF_SIZE 4096
#endif

struct TYPEMAP { u8_string name; int typecode; };
static struct TYPEMAP nstypemap[]=
  {{"A",ns_t_a},
   {"CNAME",ns_t_cname},
   {"AUTH",ns_t_ns},
   {"NS",ns_t_ns},
   {"NAMESERVER",ns_t_ns},
   {"TXT",ns_t_txt},
   {"MX",ns_t_mx},
   {NULL,-1}};

static int get_record_typecode(u8_string name)
{
  struct TYPEMAP *scan=nstypemap;
  while (scan->name) {
    if (strcasecmp(name,scan->name))
      return scan->typecode;
    else scan++;}
  return -1;
}


static fdtype dns_query(fdtype domain,fdtype type_arg)
{
  fdtype results=FD_EMPTY_CHOICE;
  int type=-1, rv=-1, n_msgs=0, i=0;
  char result[NSBUF_SIZE], outbuf[NSBUF_SIZE]; 
  ns_msg msg; ns_rr rr;

  if (FD_STRINGP(type_arg))
    type=get_record_typecode(FD_STRDATA(type_arg));
  else if (FD_SYMBOLP(type_arg))
    type=get_record_typecode(FD_SYMBOL_NAME(type_arg));
  else return fd_type_error("DNS type spec","dns_query",type_arg);
  if (type<0) return fd_type_error("DNS type spec","dns_query",type_arg);
  
  rv=res_query(FD_STRDATA(domain),ns_c_any,
               type,
               result,sizeof(result));
  if (rv < 0)
    return fd_err("DNSQueryError","dns_query",NULL,domain);
  /*
  ns_initparse( result, rv, &msg );
  n_msgs = ns_msg_count(msg, ns_s_an);
  i=0; while (i < n_msgs) {
    ns_parserr(&msg, ns_s_an, i, &rr);
    ns_sprintrr(&msg, &rr, NULL, NULL, outbuf, sizeof(outbuf));
    FD_ADD_TO_CHOICE(results,fdstring(outbuf));}
  */

  FD_ADD_TO_CHOICE(results,fdstring(result));

  return results;
}

FD_EXPORT void fd_init_dns_c(void) FD_LIBINIT_FN;

FD_EXPORT void fd_init_dns_c()
{
  fdtype module;
  if (dns_initialized) return;
  dns_initialized=1;
  fd_init_fdscheme();

  module=fd_new_module("FDWEB",(0));

  res_init();

  fd_idefn(module,"DNS/GET",fd_make_cprim2x
           (dns_query,1,fd_string_type,FD_VOID,-1,FD_VOID));

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
