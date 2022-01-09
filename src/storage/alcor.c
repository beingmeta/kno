/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_BUFIO KNO_DO_INLINE
#define KNO_INLINE_STREAMIO KNO_DO_INLINE

#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/streams.h"
#include "kno/storage.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>
#include <libu8/u8printf.h>
#include <libu8/libu8io.h>
#include <libu8/u8rusage.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <zlib.h>

#ifndef KNO_ALCOR_BUFSIZE
#define KNO_ALCOR_BUFSIZE (2*1024*1024)
#endif

#ifndef MAP_POPULATE
#define MAP_POPULATE 0
#endif

#define USE_MMAP KNO_USE_MMAP

u8_condition TruncatedHead=_("The head file has no data");

#define graberrno(context,details) u8_graberrno(context,((details) ? (u8_strdup(details)) : (NULL)))

static size_t get_mmap_size(size_t len)
{
  ssize_t page_size = u8_getpagesize();
  if ( (len%page_size) == 0)
    return len;
  else return (len/page_size+1)*page_size;
}

#if USE_MMAP
static ssize_t save_head(int in,int out,size_t head_len,size_t source_len)
{
  int error = 0;
  int trunc_rv = ftruncate(out,head_len+8);
  if (trunc_rv<0) {
    u8_graberrno("save_head/trunc/grow",NULL);
    return -1;}
  unsigned char *inbuf = mmap(NULL,get_mmap_size(head_len),
			      PROT_READ,MAP_SHARED,in,0);
  if ( (inbuf == NULL) || (inbuf == MAP_FAILED) ) {
    u8_graberrno("save_head/inbuf",NULL);
    return -1;}
  unsigned char *outbuf =
    mmap(NULL,get_mmap_size(head_len+8),
	 PROT_READ|PROT_WRITE,MAP_POPULATE|MAP_SHARED,
	 out,0);
  if ( (outbuf == NULL) || (outbuf == MAP_FAILED) ) {
    u8_graberrno("save_head/outbuf",NULL);
    munmap(inbuf,get_mmap_size(head_len)); errno=0;
    return -1;}
  else {
    struct KNO_OUTBUF buf;
    if (memcpy(outbuf,inbuf,head_len) == outbuf) {
      KNO_INIT_OUTBUF(&buf,outbuf+head_len,8,0);
      kno_write_8bytes(&buf,source_len);
      if (msync(outbuf,head_len+8,MS_SYNC) < 0)
	error=1;}
    else error = 1;
    if (munmap(outbuf,get_mmap_size(head_len+8)) < 0) error=1;}
  if (munmap(inbuf,get_mmap_size(head_len)) < 0) error=1;
  if (error)
    return -1;
  else return head_len+8;
}
#else
static ssize_t save_head(int in,int out,size_t head_len,size_t source_len)
{
  ssize_t bufsize = KNO_ALCOR_BUFSIZE;
  unsigned char *buf = u8_malloc(bufsize);
  ssize_t bytes_copied = 0;
  while (bytes_copied < head_len) {
    ssize_t needed = head_len - bytes_copied;
    ssize_t to_read = (needed < bufsize) ? (needed) : (bufsize);
    ssize_t delta = read(in,buf,to_read);
    if (delta<=0) return delta;
    else delta = write(out,buf,delta);
    if (delta<=0) return delta;
    else bytes_copied += delta;}
  u8_free(buf);
  fsync(out);
  if (bytes_copied == head_len) {
    KNO_DECL_OUTBUF(tmpbuf,8);
    kno_write_8bytes(&tmpbuf,source_len);
    ssize_t buffered = tmpbuf.bufwrite-tmpbuf.buffer;
    ssize_t rv = u8_writeall(out,tmpbuf.buffer,buffered);
    if (rv<0) return rv;
    else return head_len + buffered;}
  else return -1;
}
#endif

#if USE_MMAP
static ssize_t apply_head(int in,int out,size_t head_len)
{
  int ok = 1;
  unsigned char *inbuf = mmap(NULL,get_mmap_size(head_len+8),
			      PROT_READ,MAP_SHARED,
			      in,0);
  if ( (inbuf == NULL) || (inbuf == MAP_FAILED) ) {
    u8_graberrno("apply_head",NULL);
    return -1;}
  unsigned char *outbuf =
    mmap(NULL,get_mmap_size(head_len),
	 PROT_READ|PROT_WRITE,MAP_SHARED,
	 out,0);
  if (! ( (outbuf == NULL) || (outbuf == MAP_FAILED) ) ) {
    struct KNO_INBUF buf;
    KNO_INIT_INBUF(&buf,inbuf+head_len,8,0);
    ssize_t trunc_len = kno_read_8bytes(&buf);
    if ( trunc_len < head_len ) {
      u8_seterr("MalformedHeadFile","apply_head",NULL);
      ok = 0;}
    else if (memcpy(outbuf,inbuf,head_len) != outbuf) {
      u8_graberrno("apply_head",NULL);
      ok = 0;}
    else if (msync(outbuf,head_len,MS_SYNC) < 0)
      ok=0;
    else {}
    if (munmap(outbuf,get_mmap_size(head_len)) < 0) ok=0;
    if (ok) {
      int rv = ftruncate(out,trunc_len);
      if (rv < 0) {
        u8_graberrno("apply_head/truncate",NULL);
        u8_seterr("TruncateFailed","apply_head",NULL);
        ok=0;}}}
  else ok=0;
  if (ok)
    return head_len;
  else return -1;
}
#else
static ssize_t read_trunc_len(int in)
{
  ssize_t cur_pos = lseek(in,0,SEEK_CUR);
  ssize_t rv = lseek(in,to_copy,SEEK_SET);
  ssize_t trunc_len = -1;
  unsigned char trunc_data[8];
  if (rv<0) {
    u8_graberrno("apply_head/read_trunclen/lseek",NULL);
    return -1;}
  ssize_t trunc_read = read(in,trunc_data,8);
  if (trunc_read == 8) {
    struct KNO_INBUF inbuf;
    KNO_INIT_BYTE_INPUT(&inbuf,trunc_data,8);
    trunc_len = kno_read_8bytes(&inbuf);}
  else u8_graberrno("apply_head/read_trunclen/lseek",NULL);
  if (lseek(in,0,SEEK_SET)<0)
    u8_graberrno("apply_head/read_trunclen",NULL);
  return trunc_len;
}
static ssize_t apply_head(int in,int out,size_t head_len)
{
  ssize_t bufsize = KNO_ALCOR_BUFSIZE;
  ssize_t to_copy = head_len - 8, bytes_copied = 0;
  ssize_t trunc_len = read_trunc_len(in);
  if ( trunc_len < copy_len ) {
    u8_seterr(InvalidHeadFile,"apply_head",NULL);
    return -1;}
  unsigned char *buf = u8_malloc(bufsize);
  while (bytes_copied < to_copy) {
    ssize_t needed = head_len - bytes_copied;
    ssize_t to_read = (needed < bufsize) ? (needed) : (bufsize);
    ssize_t delta = read(in,buf,to_read);
    if (delta=<0) {
      if (delta<0) u8_graberrno("apply_head/read",NULL);
      else u8_seterr("ReadFailed","apply_head",NULL);
      return -1;}
    else delta = write(out,buf,delta);
    if (delta=<0) {
      if (delta<0) u8_graberrno("apply_head/write",NULL);
      else u8_seterr("WriteFailed","apply_head",NULL);
      return -1;}
    else bytes_copied += delta;}
  u8_free(buf);
  int rv = ftruncate(out,trunc_len);
  if (rv < 0) {
    graberrno("kno_apply_head/truncate",NULL);
    u8_seterr("TruncateFailed","apply_head",NULL);
    return -1;}
  else return head_len+8;
}
#endif

KNO_EXPORT ssize_t kno_save_head(u8_string source,u8_string dest,
				 size_t head_len)
{
  int in = u8_open_fd(source,O_RDONLY,0644);
  if (in<0) {
    graberrno("kno_save_head/open",source);
    return -1;}
  else if (u8_lock_fd(in,0)<0) {
    graberrno("kno_save_head/lock",source);
    u8_close_fd(in);
    return -1;}
  else NO_ELSE;
  u8_byte tmp_dest[strlen(dest)+6];
  strcpy(tmp_dest,dest); strcat(tmp_dest,".part");
  int out = u8_open_fd(tmp_dest,O_RDWR|O_CREAT,0644);
  if (out<0) {
    graberrno("kno_save_head",source);
    u8_close_fd(in);
    return -1;}
  else if (u8_lock_fd(out,1)<0) {
    graberrno("kno_save_head/lock",dest);
    u8_close_fd(in);
    u8_close_fd(out);
    return -1;}
  else {}
  struct stat info={0};
  ssize_t rv = -1;
  if (fstat(in,&info)>=0) {
    ssize_t in_size = info.st_size;
    rv = save_head(in,out,head_len,in_size);
    if (rv<0) u8_graberrno("kno_save_head",u8_strdup(tmp_dest));}
  u8_unlock_fd(in); u8_unlock_fd(out);
  u8_close_fd(in);
  u8_close_fd(out);
  if (rv<0) u8_removefile(tmp_dest);
  else {
    rv = u8_movefile(tmp_dest,dest);
    if (rv<0) graberrno("kno_save_head/rename",dest);}
  return rv;
}

KNO_EXPORT ssize_t kno_apply_head(u8_string head,u8_string tofile)
{
  struct stat info={0};
  int in = u8_open_fd(head,O_RDONLY,0644);
  if (in<0) {
    graberrno("kno_apply_head/in/open",head);
    return -1;}
  else if (u8_lock_fd(in,0)<0) {
    graberrno("kno_apply_head/in/lock",head);
    u8_close_fd(in);
    return -1;}
  else NO_ELSE;
  int out = u8_open_fd(tofile,O_RDWR,0644);
  if (out<0) {
    graberrno("kno_apply_head/out/open",tofile);
    u8_close_fd(in);
    return -1;}
  else if (u8_lock_fd(out,1)<0) {
    graberrno("kno_apply_head/out/lock",tofile);
    u8_close_fd(in);
    u8_close_fd(out);
    return -1;}
  else NO_ELSE;
  ssize_t rv = -1;
  if (fstat(in,&info)>=0)
    rv = apply_head(in,out,info.st_size-8);
  else u8_graberr(errno,"kno_apply_head",u8_strdup("fstat failed"));
  if (rv<0)
    u8_seterr("ApplyHeadFailed","kno_apply_head",
              u8_mkstring("%s->%s",head,tofile));
  u8_unlock_fd(in); u8_unlock_fd(out);
  u8_close_fd(in); u8_close_fd(out);
  return rv;
}

KNO_EXPORT ssize_t kno_restore_head(u8_string source,u8_string dest)
{
  return kno_apply_head(dest,source);
}

KNO_EXPORT void kno_init_alcor_c()
{
  u8_register_source_file(_FILEINFO);
}

