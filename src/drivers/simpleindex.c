/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"
#define KNO_INLINE_BUFIO KNO_DO_INLINE
#define KNO_INLINE_STREAMIO KNO_DO_INLINE

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/streams.h"

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

/* A simple index is a pretty minimal index driver.

   It consists of a 256-byte header, starting with a magic number.
   After the 256 byte header is any number of value blocks, each
   starting with an 8-byte length and followed by a dtype
   representation. At the end of the file, in a location specified in
   the header, is the dtype representation of a hashtable used as an
   index. The keys in the hashtable are the keys in the index and the
   values are pairs of file offsets and sizes. The hashtable lives in
   memory and fetching values gets the corresponding block. Committing
   the index writes the changed values, updates the offsets table, and
   writes it out at the end. It then writes a .commit file and a
   .rollback file with the new header.

   The offsets table can have choice values, which is a way to have
   easy adding.

*/

KNO_EXPORT void kno_init_simpleindex_c()
{
  u8_register_source_file(_FILEINFO);
}

