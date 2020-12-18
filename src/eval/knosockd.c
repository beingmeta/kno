/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/defines.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/support.h"
#include "kno/tables.h"
#include "kno/streams.h"
#include "kno/services.h"
#include "kno/cprims.h"
#include "kno/eval.h"

#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8timefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>
#include <libu8/u8netfns.h>
#include <libu8/u8srvfns.h>
#include <libu8/u8rusage.h>

#include "kno/knosocks.h"

#include <strings.h>
#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <signal.h>
#include <stdio.h>

#if HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif

static lispval knosockd_symbol = KNO_VOID;

static void recycle_knosockd(void *vp)
{
  struct KNOSOCKS_SERVER *server = (knosocks_server) vp;
  knosocks_recycle(server,5.0);
}

static lispval get_listener_id(lispval ports)
{
  lispval id = KNO_VOID; ssize_t id_len = -1;
  DO_CHOICES(port,ports) {
    if (KNO_STRINGP(port)) {
      if (KNO_VOIDP(id)) {
	id = port;
	id_len = KNO_STRLEN(port);}
      else if ( (KNO_STRLEN(port)) > id_len ) {
	id = port;
	id_len = KNO_STRLEN(port);}
      else {}}}
  if (KNO_VOIDP(id)) {
    u8_byte buf[64];
    pid_t pid = getpid();
    double etime = u8_elapsed_time()*1000;
    int msecs = (int) trunc(etime);
    return knostring(u8_bprintf(buf,"server%d.%d",pid,msecs));}
  else return kno_incref(id);
}

DEFCPRIM("knosockd/listener",knosockd_listener_prim,
	 KNO_MAX_ARGS(4)|KNO_MIN_ARGS(1),
	 "Creates a knosocks server",
	 {"listen",kno_any_type,KNO_FALSE},
	 {"opts",kno_any_type,KNO_FALSE},
	 {"env",kno_any_type,KNO_VOID},
	 {"data",kno_any_type,KNO_VOID})
static lispval knosockd_listener_prim(lispval listen,lispval opts,
				      lispval env,lispval data)
{
  if (KNO_TABLEP(listen)) {
    data = env;
    env = opts;
    opts = listen;
    listen = KNO_FALSE;}
  if ( (KNO_VOIDP(data)) || (KNO_DEFAULTP(data)) )
    data = kno_make_slotmap(7,0,NULL);
  else kno_incref(data);
  lispval id = kno_getopt(opts,KNOSYM_ID,KNO_VOID);
  if (KNO_VOIDP(id)) id = get_listener_id(listen);
  struct KNOSOCKS_SERVER *server =
    new_knosocks_listener(KNO_CSTRING(id),listen,opts,env,data);
  kno_decref(id);
  kno_decref(data);
  if (server == NULL) return KNO_ERROR;
  lispval wrapped = kno_wrap_pointer(server,sizeof(struct KNOSOCKS_SERVER),
				     recycle_knosockd,
				     KNOSYM(knosockd),
				     KNO_CSTRING(id));
  server->server_wrapper = wrapped;
  return wrapped;
}

DEFCPRIM("knosockd/listen",knosockd_listen_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "Creates a knosocks server",
	 {"srv",kno_rawptr_type,KNO_VOID},
	 {"addrs",kno_any_type,KNO_VOID})
static lispval knosockd_listen_prim(lispval srv,lispval addrs)
{
  if (!(KNO_RAW_TYPEP(srv,knosockd_symbol)))
    return kno_type_error("knosockd","knosockd/listen",srv);
  struct KNOSOCKS_SERVER *server = (knosocks_server)(KNO_RAWPTR_VALUE(srv));
  int rv = knosocks_listen(server,addrs);
  if (rv<0) return KNO_ERROR;
  else if (rv) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("knosockd/run",knosockd_run_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Starts a knosocks server",
	 {"srv",kno_rawptr_type,KNO_VOID})
static lispval knosockd_run_prim(lispval srv)
{
  if (!(KNO_RAW_TYPEP(srv,knosockd_symbol)))
    return kno_type_error("knosockd","knosockd/listen",srv);
  struct KNOSOCKS_SERVER *server = (knosocks_server)(KNO_RAWPTR_VALUE(srv));
  int rv = knosocks_start(server);
  if (rv<0) return KNO_ERROR;
  else if (rv) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("knosockd/shutdown!",knosockd_shutdown_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "Shuts down a knosockd server",
	 {"srv",kno_rawptr_type,KNO_VOID},
	 {"grace_val",kno_flonum_type,KNO_VOID})
static lispval knosockd_shutdown_prim(lispval srv,lispval grace_val)
{
  if (!(KNO_RAW_TYPEP(srv,knosockd_symbol)))
    return kno_type_error("knosockd","knosockd/listen",srv);
  double grace = (KNO_VOIDP(grace_val)) ? (-1.0) : (KNO_FLONUM(grace_val));
  struct KNOSOCKS_SERVER *server = (knosocks_server)(KNO_RAWPTR_VALUE(srv));
  int rv = knosocks_shutdown(server,grace);
  if (rv<0) return KNO_ERROR;
  else if (rv) return KNO_TRUE;
  else return KNO_FALSE;
}

int knosocks_module_initialized = 0;

lispval knosocks_module;

static void link_cprims(lispval module)
{
  KNO_LINK_CPRIM("knosockd/listener",knosockd_listener_prim,4,module);
  KNO_LINK_CPRIM("knosockd/listen",knosockd_listen_prim,2,module);
  KNO_LINK_CPRIM("knosockd/run",knosockd_run_prim,1,module);
  KNO_LINK_CPRIM("knosockd/shutdown!",knosockd_shutdown_prim,2,module);
}

KNO_EXPORT void kno_init_knosockd_c()
{
  if (knosocks_module_initialized) return;
  knosocks_module_initialized = 1;
  knosocks_module = kno_new_cmodule
    ("knosocks",(0),kno_init_knosockd_c);

  knosockd_symbol=kno_intern("knosockd");

  link_cprims(knosocks_module);

  kno_finish_module(knosocks_module);

  u8_register_source_file(_FILEINFO);
}

static void link_local_cprims(){}
