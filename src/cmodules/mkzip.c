/* -*- Mode: C; -*- */

/* Copyright (C) 2007-2010 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id: crypto.c 4774 2010-01-15 14:43:07Z haase $";

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"
#include "fdb/indices.h"
#include "fdb/frames.h"
#include "fdb/numbers.h"

#include <libu8/libu8io.h>

#include <sys/types.h>
#include <zip.h>

