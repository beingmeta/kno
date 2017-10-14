/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_BUFIO 1
#define FD_INLINE_STREAMIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/streams.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>
#include <libu8/u8printf.h>
#include <libu8/libu8io.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <zlib.h>

/*
  A log file is a disk file consisting of a bunch of dtype representations
  but with a little more structure.

  The first 256 bytes consists of:
    A magic number (8 bytes)
    The number of entries (8 bytes)
    The end of valid data (8 bytes)
    The time that was written (8 bytes)

    After those 256 bytes are just dtype representations.

    Reading from a log file, opens it and reads the dtypes up to the
    end of valid data.
    Writing to a log file just appends to the end. Committing the log file
    updates the number of entries, time, and end of valid data.

    Pre-committing writes a .commit file with that data which can be
    written into it and also writes a .recovery file which can be used
    to rollback.

    Here are the functions:
     open(file) creates the log file if desired, returns an FD_LOGFILE structure.
     modify(logfile) locks the underlying file and opens a stream at the end
     add(logfile,objects) writes objects and increments the in-memory object count
     precommit(logfile) writes a .commit file with a new header
     commit(logfile) updates the actual file
 */

FD_EXPORT void fd_init_logfiles_c()
{
  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
