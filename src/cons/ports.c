/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.

   This file implements the core parser and printer (unparser) functionality.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1
#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/ports.h"

#include <libu8/u8printf.h>
#include <libu8/u8streamio.h>
#include <libu8/u8convert.h>
#include <libu8/u8crypto.h>

#include <ctype.h>
#include <errno.h>
/* We include this for sscanf, but we're not using the FILE functions */
#include <stdio.h>

#include <stdarg.h>

fd_exception fd_CantOpenFile=_("Can't open file");
fd_exception fd_FileNotFound=_("File not found");

/* The port type */

static int unparse_port(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_PORT *p=fd_consptr(fd_port,x,fd_port_type);
  if ((p->fd_inport) && (p->fd_outport) && (p->fd_portid))
    u8_printf(out,"#<I/O Port (%s) #!%x>",p->fd_portid,x);
  else if ((p->fd_inport) && (p->fd_outport))
    u8_printf(out,"#<I/O Port #!%x>",x);
  else if ((p->fd_inport)&&(p->fd_portid))
    u8_printf(out,"#<Input Port (%s) #!%x>",p->fd_portid,x);
  else if (p->fd_inport)
    u8_printf(out,"#<Input Port #!%x>",x);
  else if (p->fd_portid)
    u8_printf(out,"#<Output Port (%s) #!%x>",p->fd_portid,x);
  else u8_printf(out,"#<Output Port #!%x>",x);
  return 1;
}

static void recycle_port(struct FD_RAW_CONS *c)
{
  struct FD_PORT *p=(struct FD_PORT *)c;
  if (p->fd_inport) {
    u8_close_input(p->fd_inport);}
  if (p->fd_outport) {
    u8_close_output(p->fd_outport);}
  if (p->fd_portid) u8_free(p->fd_portid);
 if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

FD_EXPORT void fd_init_ports_c()
{
  u8_register_source_file(_FILEINFO);

  fd_unparsers[fd_port_type]=unparse_port;
  fd_recyclers[fd_port_type]=recycle_port;
}
