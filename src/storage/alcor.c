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
#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/streams.h"
#include "framerd/storage.h"

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

u8_condition TruncatedHead=_("The head file has no data");

#define graberrno(context,details) u8_graberrno(context,u8_strdup(details))

FD_EXPORT ssize_t fd_save_head(u8_string source,u8_string dest,size_t head_len)
{
  int in = u8_open_fd(source,O_RDONLY,0644);
  if (in<0) {
    graberrno("fd_save_head/open",source);
    return -1;}
  else if (u8_lock_fd(in,0)<0) {
    graberrno("fd_save_head/lock",source);
    u8_close_fd(in);
    return -1;}
  else NO_ELSE;
  int out = u8_open_fd(dest,O_WRONLY|O_EXCL|O_CREAT,0644);
  if (out<0) {
    graberrno("fd_save_head",source);
    u8_close_fd(in);
    return -1;}
  else if (u8_lock_fd(out,1)<0) {
    graberrno("fd_save_head/lock",dest);
    u8_close_fd(in);
    u8_close_fd(out);
    return -1;}
  else NO_ELSE;
  struct stat info={0};
  if (fstat(in,&info)>=0) {
    ssize_t in_size = info.st_size;
    ssize_t chunk_size = 256 * 1024;
    unsigned char *buf = u8_malloc(chunk_size);
    ssize_t bytes_copied = 0;
    while (bytes_copied < head_len) {
      ssize_t needed = head_len - bytes_copied;
      ssize_t to_read = (needed < chunk_size) ? (needed) : (chunk_size);
      ssize_t delta = read(in,buf,to_read);
      if (delta<=0) {
        graberrno("fd_save_head",source);
        break;}
      else delta = write(out,buf,delta);
      if (delta<=0) {
        graberrno("fd_save_head",source);
        break;}
      bytes_copied += delta;}
    u8_free(buf);
    fsync(out);
    if (bytes_copied == head_len) {
      FD_DECL_OUTBUF(tmpbuf,30);
      fd_write_8bytes(&tmpbuf,in_size);
      ssize_t buffered = tmpbuf.bufwrite-tmpbuf.buffer;
      ssize_t rv = u8_writeall(out,tmpbuf.buffer,buffered);
      if (rv >= 0) {
        u8_unlock_fd(in); u8_unlock_fd(out);
        u8_close_fd(in); u8_close_fd(out);
        return head_len + buffered;}
      else graberrno("fd_save_head/write_trunc",dest);}}
  else graberrno("fd_save_head/stat",source);
  u8_unlock_fd(in); u8_unlock_fd(out);
  u8_close_fd(in);
  u8_close_fd(out);
  u8_removefile(dest);
  return -1;
}

FD_EXPORT ssize_t fd_apply_head(u8_string head,u8_string tofile)
{
  struct stat info={0};
  int in = u8_open_fd(head,O_RDONLY,0644);
  if (in<0) {
    graberrno("fd_apply_head/open",head);
    return -1;}
  else if (u8_lock_fd(in,0)<0) {
    graberrno("fd_apply_head/lock",head);
    u8_close_fd(in);
    return -1;}
  else NO_ELSE;
  int out = u8_open_fd(tofile,O_RDWR,0644);
  if (out<0) {
    graberrno("fd_apply_head/open",tofile);
    u8_close_fd(in);
    return -1;}
  else if (u8_lock_fd(out,1)<0) {
    graberrno("fd_apply_head/lock",tofile);
    u8_close_fd(in);
    u8_close_fd(out);
    return -1;}
  else NO_ELSE;
  if (fstat(in,&info)>=0) {
    ssize_t head_len = info.st_size - 8;
    ssize_t chunk_size = 256 * 1024;
    unsigned char *buf = u8_malloc(chunk_size);
    ssize_t bytes_copied = 0;
    while (bytes_copied < head_len) {
      ssize_t needed = head_len - bytes_copied;
      ssize_t to_read = (needed < chunk_size) ? (needed) : (chunk_size);
      ssize_t delta = read(in,buf,to_read);
      if (delta<=0) {
        graberrno("fd_save_head/read",head);
        break;}
      else delta = write(out,buf,delta);
      if (delta<=0) {
        graberrno("fd_save_head/write",tofile);
        break;}
      bytes_copied += delta;}
    u8_free(buf);
    if (bytes_copied == head_len) {
      int success = 1;
      unsigned char trunc_data[16];
      ssize_t trunc_read = read(in,trunc_data,8);
      if (trunc_read == 8) {
        struct FD_INBUF inbuf;
        FD_INIT_BYTE_INPUT(&inbuf,trunc_data,8);
        ssize_t trunc_len = fd_read_8bytes(&inbuf);
        if ( trunc_len >= head_len ) {
          int rv = ftruncate(out,trunc_len);
          if (rv < 0) {
            graberrno("fd_apply_head/trunc",tofile);
            u8_seterr("TruncateFailed","fd_apply_head",NULL);
            success = 0;}}
        else {
          u8_seterr("BadTruncData","fd_apply_head",u8_strdup(head));
          success = 0;}}
      else {
        u8_seterr("BadTruncData","fd_apply_head",u8_strdup(head));
        success = 0;}
      if (success) {
        u8_unlock_fd(in); u8_unlock_fd(out);
        u8_close_fd(in); u8_close_fd(out);
        return head_len;}}
    else u8_seterr("CopyFailed","fd_apply_head",
                   u8_mkstring("%s->%s",head,tofile));}
  u8_unlock_fd(in); u8_unlock_fd(out);
  u8_close_fd(in); u8_close_fd(out);
  return -1;
}

FD_EXPORT ssize_t fd_restore_head(u8_string source,u8_string dest)
{
  return fd_apply_head(dest,source);
}

FD_EXPORT void fd_init_alcor_c()
{
  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
