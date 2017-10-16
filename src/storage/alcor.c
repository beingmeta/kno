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

u8_condition TruncatedHead=_("The head file has no data");

FD_EXPORT ssize_t fd_save_head(u8_string source,u8_string dest,size_t head_len)
{
  struct stat info={0};
  char *src=u8_tolibc(source), *dst=u8_tolibc(dest);
  if ( (src==NULL) || (dst==NULL) )
    return -1;
  else if (stat(src,&info)<0) {
    u8_free(src); u8_free(dst);
    return -1;}
  else if (info.st_size<head_len) {
    u8_seterr(_("Source smaller than head length"),"fd_save_head",
              u8_strdup(source));
    u8_free(src); u8_free(dst);
    return -1;}
  int in=open(src,O_RDONLY), rv=0;
  u8_free(src);
  if (in<0)
    return in;
  else rv=u8_lock_fd(in,0);
  unsigned char *buf=u8_big_alloc(head_len);
  size_t bytes_read=0;
  while (bytes_read<head_len) {
    ssize_t delta=read(in,buf+bytes_read,head_len-bytes_read);
    if (delta<0) {
      u8_big_free(buf);
      close(in);
      return delta;}
    bytes_read += delta;}
  rv=u8_unlock_fd(in);
  rv=close(in);
  if (rv<0) {
    u8_big_free(buf);
    return rv;}
  int out=open(dst,O_WRONLY|O_CREAT|O_TRUNC,S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP);
  u8_free(dst);
  if (out<0) {
    u8_big_free(buf);
    return out;}
  else rv=u8_lock_fd(out,1);
  if (rv<0) {
    close(out);
    u8_big_free(buf);
    return rv;}
  size_t bytes_written=0;
  while (bytes_written<head_len) {
    ssize_t delta=write(out,buf+bytes_written,head_len-bytes_written);
    if (delta<0) {
      close(out);
      u8_big_free(buf);
      return delta;}
    bytes_written += delta;}
  u8_big_free(buf);
  rv=u8_unlock_fd(out);
  if (rv<0) {
    u8_log(LOGWARN,"RecoveryFileUnlockFailed",
           "Unlocking %s failed (errno=%d:%s)",
           dest,errno,u8_strerror(errno));
    errno=0;
    close(out);
    return rv;}
  else rv=close(out);
  if (rv<0)
    return rv;
  else return head_len;
}

FD_EXPORT ssize_t fd_restore_head(u8_string source,u8_string dest,ssize_t trunc_loc)
{
  char *src=u8_tolibc(source), *dst=u8_tolibc(dest);
  if ( (src==NULL) || (dst==NULL) )
    return -1;
  struct stat info={0};
  ssize_t head_len=-1;
  int rv;
  if ((rv=stat(src,&info))<0) {
    u8_free(src); u8_free(dst);
    return rv;}
  int in=open(src,O_RDONLY);
  if (in<0) {
    u8_free(src); u8_free(dst);
    return in;}
  else if ((rv=u8_lock_fd(in,0))<0) {
    u8_free(src); u8_free(dst);
    return rv;}
  else head_len=info.st_size;
  unsigned char *buf=u8_big_alloc(head_len);
  u8_free(src);
  size_t bytes_read=0;
  while (bytes_read < head_len) {
    ssize_t delta=read(in,buf+bytes_read,head_len-bytes_read);
    if (delta<0) {
      u8_big_free(buf);
      u8_free(dst);
      close(in);
      return delta;}
    bytes_read += delta;}
  if ((rv=u8_unlock_fd(in))<0) {
    u8_big_free(buf);
    u8_free(dst);
    close(in);
    return rv;}
  else rv=close(in);
  if (rv<0) {
    u8_big_free(buf);
    u8_free(dst);
    return rv;}
  int out=open(dst,O_RDWR);
  u8_free(dst);
  if (out<0) {
    u8_big_free(buf);
    return out;}
  rv=u8_lock_fd(out,1);
  if (rv<0) {
    close(out);
    u8_big_free(buf);
    return rv;}
  size_t bytes_written=0;
  while (bytes_written<head_len) {
    ssize_t delta=write(out,buf+bytes_written,head_len-bytes_written);
    if (delta<0) {
      close(out);
      u8_big_free(buf);
      return delta;}
    bytes_written += delta;}
  if ( (trunc_loc >= 0) && ( (trunc_loc+8) < head_len )) {
    unsigned char bytes[8];
    memcpy(bytes,buf+trunc_loc,8);
    size_t *trunc_ptr=(size_t *)&bytes;
    size_t restore_len=*trunc_ptr;
    if (restore_len>head_len)
      rv=ftruncate(out,restore_len);}
  if (rv<0) {
    u8_unlock_fd(out);
    close(out);
    u8_big_free(buf);
    return rv;}
  else rv=u8_unlock_fd(out);
  if (rv<0) {
    close(out);
    u8_big_free(buf);
    return rv;}
  else rv=close(out);
  u8_big_free(buf);
 if (rv<0)
    return rv;
  else return head_len;
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
