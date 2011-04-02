/* C Mode */

/* dbus.c
   This implements FramerD bindings to the dbus libraries.
   Copyright (C) 2007-2011 beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define U8_INLINE_IO 1

#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/sequences.h"
#include "framerd/texttools.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>
