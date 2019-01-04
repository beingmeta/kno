!/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_BUFIO 1
#define FD_INLINE_STREAMIO 1
#include "framerd/components/storage_layer.h"

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

typedef struct FD_LOGFILE {
  FD_CONS_HEADER;
  u8_string filename;
  long long logfile_n_entries;
  size_t    logfile_valid_size;
  time_t    logfile_last_sync;
  u8_mutex  logfile_lock;
  struct FD_STREAM *stream;} *fd_logfile;
typedef struct FD_LOGFILE FD_LOGFILE;

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
  written into it and also writes a .rollback file which can be used
  to rollback.

  Here are the functions:
  open(file) creates the log file if desired, returns an FD_LOGFILE structure.
  modify(logfile) locks the underlying file and opens a stream at the end
  add(logfile,objects) writes objects and increments the in-memory object count
  precommit(logfile) writes a .commit file with a new header
  commit(logfile) updates the actual file
*/

struct FD_LOGFILE *fd_open_logfile(u8_string filename)
{
  fd_stream s;
  struct FD_LOGFILE *lf = u8_alloc(struct FD_LOGFILE);
  FD_INIT_CONS(lf,fd_logfile_type);
  lf->logfile_n_entries  = 0;
  lf->logfile_valid_size = 256;
  lf->logfile_last_sync  = time(NULL);
  lf->logfile_created    = time(NULL);
  u8_init_mutex(&(lf->logfile_lock));
  if (u8_file_existsp(filename)) {
    s = fd_init_file_stream(&(lf->logfile_stream),filename,FD_FILE_READ,-1,-1);
    long long magic_no = fd_read4bytes(s);
    unsigned long long n_entries = fd_read8bytes(s);
    ssize_t valid_size = fd_read8bytes(s);
    time_t last_sync = (time_t) fd_read8bytes(s);
    time_t created = (time_t) fd_read8bytes(s);
    if (magic_no != 0x106F11E0) {
      fd_close_stream(s);
      u8_free(lf->logfile_filename);
      u8_free(lf);
      fd_seterr("InvalidMagicNo","fd_open_logfile",filename,FD_INT(magic_no));
      return NULL;}
    lf->logfile_n_entries = n_entries;
    if (last_sync > 0) lf->logfile_last_sync = last_sync;
    if (created > 0) lf->logfile_created = created;
    if (valid_size > 0) lf->logfile_valid_size = valid_size;}
  else {
    s = fd_init_file_stream(&(lf->logfile_stream),filename,FD_FILE_CREATE,-1,-1);
    fd_write_4bytes(s,0x106F11E0);
    fd_write_8bytes(s,lf->logfile_n_entries);
    fd_write_8bytes(s,lf->logfile_valid_size);
    fd_write_8bytes(s,(long long) (lf->logfile_last_sync) );
    fd_write_8bytes(s,(long long) (lf->logfile_created) );}

  fd_open_file(filename,FD_FILE_READ);

  return lf;
}

struct FD_LOGFILE *fd_logfile_content(struct FD_LOGFILE *lf)
{
  long long n = lf->logfile_n_entries;
  fd_stream s = &(lf->logfile_stream);
  if (n == 0) return FD_EMPTY;
  else if (n==1) {
    fd_lock_stream(s);
    fd_setpos(s,256);
    u8_inbuf in = fd_intbuf(s);
    lispval v = fd_read_dtype(buf);
    fd_unlock_stream(s);
    return v;}
  else {
    int isatomic = 1;
    struct FD_CHOICE *result = fd_alloc_choice(n);
    lisvap *data  = FD_CHOICE_ELTS(result);
    lisvap *write = data;
    fd_lock_stream(s);
    fd_setpos(s,256);
    u8_inbuf in = fd_inbuf(s);
    int i =0; size_t valid_end = lf->logfile_valid_size;
    while (i < n) {
      lispval v = fd_read_dtype(in);
      ssize_t pos = fd_getpos(s);
      if ( (atomic) && (FD_CONSP(v)) ) atomic=0;
      if (pos >= valid_size) break;}
    fd_unlock_stream(s);
    return fd_init_choice(result,write-data,NULL,
                          ((atomic)?(FD_CHOICE_ISATOMIC):(FD_CHOICE_ISCONSES)) |
                          FD_CHOICE_DOSORT | FD_CHOICE_COMPRESS |
                          FD_CHOICE_REALLOC);}
}

FD_EXPORT void fd_init_logfiles_c()
{
  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
