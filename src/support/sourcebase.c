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
#include "framerd/numbers.h"
#include "framerd/apply.h"

#include <libu8/u8signals.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8logging.h>

#include <signal.h>
#include <sys/types.h>
#include <pwd.h>

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>
#if FD_FILECONFIG_ENABLED
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#endif

/* Tracking the current source base */

#if FD_USE_TLS
static u8_tld_key sourcebase_key;
FD_EXPORT u8_string fd_sourcebase()
{
  return u8_tld_get(sourcebase_key);
}
static u8_string bind_sourcebase(u8_string push)
{
  u8_string current = u8_tld_get(sourcebase_key);
  u8_tld_set(sourcebase_key,push);
  return current;
}
static void restore_sourcebase(u8_string old)
{
  u8_tld_set(sourcebase_key,old);
}
#else
static __thread u8_string sourcebase;
FD_EXPORT u8_string fd_sourcebase()
{
  return sourcebase;
}
static u8_string bind_sourcebase(u8_string push)
{
  u8_string current = sourcebase;
  sourcebase = push;
  return current;
}
static void restore_sourcebase(u8_string old)
{
  sourcebase = old;
}
#endif

FD_EXPORT
/* fd_bind_sourcebase:
      Arguments: a UTF-8 string
      Returns: a UTF-8 string
  This dynamically binds the sourcebase, which indicates
 the "current file" and is used by functions like load-component
 and get-component. */
u8_string fd_bind_sourcebase(u8_string sourcebase)
{
  return bind_sourcebase(sourcebase);
}

FD_EXPORT
/* fd_restore_sourcebase:
      Arguments: a UTF-8 string
      Returns: void
  Restores the previous sourcebase, passed as an argument. */
void fd_restore_sourcebase(u8_string sourcebase)
{
  restore_sourcebase(sourcebase);
}

FD_EXPORT
/* fd_get_component:
    Arguments: a utf8 string identifying a filename
    Returns: a utf8 string identifying a filename
  Interprets a relative pathname with respect to the directory
   of the current file being loaded.
*/
u8_string fd_get_component(u8_string spec)
{
  u8_string base = fd_sourcebase();
  if (base) return u8_realpath(spec,base);
  else return u8_strdup(spec);
}

/* Initialization */

static int support_sourcebase_c_init_done = 0;

void fd_init_sourcebase_c()
{
  if (support_sourcebase_c_init_done)
    return;
  else support_sourcebase_c_init_done=1;

#if FD_USE_TLS
  u8_new_threadkey(&sourcebase_key,NULL);
#endif

  u8_register_source_file(_FILEINFO);
}

