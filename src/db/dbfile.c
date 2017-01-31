/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/dbfile.h"

#include <libu8/libu8io.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>

#include <stdio.h>

static fdtype rev_symbol, gentime_symbol, packtime_symbol, modtime_symbol;

int fd_acid_files=1;
size_t fd_filedb_bufsize=FD_FILEDB_BUFSIZE;

/* Opening file pools */

static struct FD_POOL_OPENER pool_openers[FD_N_POOL_OPENERS];
static int n_pool_openers=0;
#if FD_THREADS_ENABLED
static u8_mutex pool_openers_lock;
#endif

FD_EXPORT void fd_register_pool_opener
  (unsigned int id,
   fd_pool (*opener)(u8_string filename,int read_only),
   fdtype (*mdreader)(FD_DTYPE_STREAM *),
   fdtype (*mdwriter)(FD_DTYPE_STREAM *,fdtype))
{
  fd_lock_mutex(&pool_openers_lock);
  if (n_pool_openers<FD_N_POOL_OPENERS) {
    pool_openers[n_pool_openers].initial_word=id;
    pool_openers[n_pool_openers].opener=opener;
    pool_openers[n_pool_openers].read_metadata=mdreader;
    pool_openers[n_pool_openers].write_metadata=mdwriter;
    n_pool_openers++;
    fd_unlock_mutex(&pool_openers_lock);}
  else {
    fd_unlock_mutex(&pool_openers_lock);
    u8_raise("Too many pool openers",
             "fd_register_pool_opener",NULL);}
}

static u8_string get_pool_filename(u8_string spec)
{
  u8_string with_suffix=u8_string_append(spec,".pool",NULL);
  u8_string probe=u8_realpath(with_suffix,NULL);
  if ((probe) && (u8_file_existsp(probe))) {
    u8_free(with_suffix);
    return probe;}
  else {
    u8_free(with_suffix); u8_free(probe);
    probe=u8_realpath(spec,NULL);}
  if (u8_file_existsp(probe)) return probe;
  else {
    u8_free(probe);
    return NULL;}
}

static fd_pool open_file_pool(u8_string filename)
{
  u8_string pool_filename=get_pool_filename(filename);
  fd_pool p;
  if (pool_filename==NULL) {
    fd_seterr3(fd_FileNotFound,"open_file_pool",u8_strdup(filename));
    return NULL;}
  else p=fd_find_pool_by_cid(pool_filename);
  if (p) {
    u8_free(pool_filename);
    return p;}
  else {
    FILE *f=u8_fopen(pool_filename,"r+b");
    unsigned int i=0, word=0, read_only=0; unsigned char byte;
    if (f==NULL) {
      read_only=1; f=fopen(pool_filename,"rb");}
    if (f==NULL) {
      fd_seterr3(fd_CantOpenFile,"open_file_pool",pool_filename);
      return NULL;}
    byte=getc(f); word=word|(byte<<24);
    byte=getc(f); word=word|(byte<<16);
    byte=getc(f); word=word|(byte<<8);
    byte=getc(f); word=word|(byte<<0);
    fclose(f);
    i=0; while (i < n_pool_openers)
           if (pool_openers[i].initial_word == word) {
             p=pool_openers[i].opener(pool_filename,read_only);
             break;}
           else i++;
    if (p) {
      u8_free(pool_filename);
      fd_register_pool(p);
      return p;}
    else {
      fd_seterr3(fd_NotAFilePool,"open_file_pool",pool_filename);
      return NULL;}}
}

FD_EXPORT
fd_pool fd_unregistered_file_pool(u8_string filename)
{
  int i=0, byte, word=0;
  u8_string pool_filename=get_pool_filename(filename);
  FILE *f=fopen(pool_filename,"rb");
  if (f==NULL) {
    fd_seterr3(fd_CantOpenFile,"fd_unregistered_file_pool",pool_filename);
    return NULL;}
  byte=getc(f); word=word|(byte<<24);
  byte=getc(f); word=word|(byte<<16);
  byte=getc(f); word=word|(byte<<8);
  byte=getc(f); word=word|(byte<<0);
  while (i < n_pool_openers)
    if (pool_openers[i].initial_word == word) {
      return pool_openers[i].opener(pool_filename,0);
      break;}
    else i++;
  return NULL;
}


/* Handling file pool metadata */

static fdtype read_metadata(struct FD_DTYPE_STREAM *ds,fd_off_t mdblockpos)
{
  fdtype metadata=FD_VOID;
  struct U8_XTIME _gentime, _packtime, _modtime; int rev;
  int mdversion, timehi, timelo;
  fd_off_t mdpos;
  fd_setpos(ds,mdblockpos);
  mdversion=fd_dtsread_4bytes(ds);
  if (mdversion==0xFFFFFFFF) {
    rev=fd_dtsread_4bytes(ds);
    mdpos=fd_dtsread_4bytes(ds);
    if (mdpos) {
      fd_setpos(ds,mdpos);
      metadata=fd_dtsread_dtype(ds);}
    else metadata=fd_empty_slotmap();
    fd_store(metadata,rev_symbol,FD_INT(rev));
    return metadata;}
  else if (mdversion!=0xFFFFFFFE)
    return fd_err(fd_BadMetaData,NULL,u8_strdup(ds->id),FD_VOID);
  fd_dtsread_4bytes(ds); /* meta data length */
  rev=fd_dtsread_4bytes(ds);
  timehi=fd_dtsread_4bytes(ds);
  timelo=fd_dtsread_4bytes(ds);
  if (timehi)
    return fd_err(fd_BadMetaData,"time warp",u8_strdup(ds->id),FD_VOID);
  u8_init_xtime(&_gentime,timelo,u8_second,0,0,0);
  timehi=fd_dtsread_4bytes(ds);
  timelo=fd_dtsread_4bytes(ds);
  if (timehi)
    return fd_err(fd_BadMetaData,"time warp",u8_strdup(ds->id),FD_VOID);
  u8_init_xtime(&_packtime,timelo,u8_second,0,0,0);
  timehi=fd_dtsread_4bytes(ds);
  timelo=fd_dtsread_4bytes(ds);
  if (timehi)
    return fd_err(fd_BadMetaData,"time warp",u8_strdup(ds->id),FD_VOID);
  u8_init_xtime(&_modtime,timelo,u8_second,0,0,0);
  mdpos=fd_dtsread_4bytes(ds);
  if (mdpos) {
    fd_setpos(ds,mdpos);
    metadata=fd_dtsread_dtype(ds);}
  else metadata=fd_empty_slotmap();
  fd_store(metadata,rev_symbol,FD_INT(rev));
  fd_store(metadata,gentime_symbol,fd_make_timestamp(&_gentime));
  fd_store(metadata,packtime_symbol,fd_make_timestamp(&_packtime));
  fd_store(metadata,modtime_symbol,fd_make_timestamp(&_modtime));
  return metadata;
}

FD_EXPORT
fdtype fd_read_pool_metadata(struct FD_DTYPE_STREAM *ds)
{
  int capacity; fd_off_t mdblockpos;
  fd_off_t ret=fd_setpos(ds,12);
  if (ret<0) return FD_ERROR_VALUE;
  else capacity=fd_dtsread_4bytes(ds);
  mdblockpos=24+capacity*4;
  return read_metadata(ds,mdblockpos);
}

FD_EXPORT
fdtype fd_read_index_metadata(struct FD_DTYPE_STREAM *ds)
{
  int n_slots; fd_off_t mdblockpos;
  fd_off_t ret=fd_setpos(ds,4);
  if (ret<0) return FD_ERROR_VALUE;
  else n_slots=fd_dtsread_4bytes(ds);
  mdblockpos=8+n_slots*4;
  return read_metadata(ds,mdblockpos);
}

static void copy_timeinfo(struct U8_XTIME *tp,fdtype md,fdtype slotid)
{
  fdtype tval=fd_get(md,slotid,FD_VOID);
  if (FD_PTR_TYPEP(tval,fd_timestamp_type)) {
    struct FD_TIMESTAMP *tstamp=
      FD_GET_CONS(tval,fd_timestamp_type,struct FD_TIMESTAMP *);
    memcpy(tp,&(tstamp->xtime),sizeof(struct U8_XTIME));}
  else u8_init_xtime(tp,-1,u8_second,0,0,0);
}

static fdtype write_metadata(fd_dtype_stream ds,fd_off_t mdblockpos,fdtype metadata)
{
  struct U8_XTIME _gentime, _packtime, _modtime;
  int rev, mdpos, mdversion;
  copy_timeinfo(&_gentime,metadata,gentime_symbol);
  copy_timeinfo(&_packtime,metadata,packtime_symbol);
  copy_timeinfo(&_modtime,metadata,modtime_symbol);
  {
    fdtype lisp_revno=fd_get(metadata,rev_symbol,FD_VOID);
    if (FD_FIXNUMP(lisp_revno)) rev=FD_FIX2INT(lisp_revno)+1;
    else rev=1;}
  mdpos=fd_endpos(ds);
  fd_dtswrite_dtype(ds,metadata);
  fd_setpos(ds,mdblockpos);
  mdversion=fd_dtsread_4bytes(ds);
  if (mdversion != 0xFFFFFFFE)
    return fd_err(fd_BadMetaData,NULL,u8_strdup(ds->id),FD_VOID);
  fd_dtswrite_4bytes(ds,40);
  fd_dtswrite_4bytes(ds,rev);
  fd_dtswrite_4bytes(ds,0);
  fd_dtswrite_4bytes(ds,_gentime.u8_tick);
  fd_dtswrite_4bytes(ds,0);
  fd_dtswrite_4bytes(ds,_packtime.u8_tick);
  fd_dtswrite_4bytes(ds,0);
  fd_dtswrite_4bytes(ds,_modtime.u8_tick);
  fd_dtswrite_4bytes(ds,mdpos);
  return metadata;
}

FD_EXPORT
fdtype fd_write_pool_metadata(fd_dtype_stream ds,fdtype metadata)
{
  fd_off_t md_pos;
  fd_off_t ret=fd_setpos(ds,12);
  if (ret<0) return FD_ERROR_VALUE;
  else md_pos=fd_dtsread_4bytes(ds)*4+24;
  write_metadata(ds,md_pos,metadata);
  return metadata;
}

FD_EXPORT
fdtype fd_write_index_metadata(fd_dtype_stream ds,fdtype metadata)
{
  fd_off_t md_pos;
  fd_off_t ret=fd_setpos(ds,4);
  if (ret<0) return FD_ERROR_VALUE;
  else md_pos=4*fd_dtsread_4bytes(ds)+8;
  return write_metadata(ds,md_pos,metadata);
}

/* Making file pools */

FD_EXPORT
/* fd_make_file_pool:
    Arguments: a filename string, a magic number (usigned int), an FD_OID,
    a capacity, and a dtype pointer to a metadata description (a slotmap).
    Returns: -1 on error, 1 on success. */
int fd_make_file_pool
  (u8_string filename,unsigned int magicno,
   FD_OID base,unsigned int capacity,unsigned int load,
   fdtype metadata)
{
  int i, hi, lo;
  struct FD_DTYPE_STREAM _stream;
  struct FD_DTYPE_STREAM *stream=
    fd_init_dtype_file_stream(&_stream,filename,FD_DTSTREAM_CREATE,8192);
  if (stream==NULL) return -1;
  else if ((stream->flags)&FD_DTSTREAM_READ_ONLY) {
    fd_seterr3(fd_CantWrite,"fd_make_file_pool",u8_strdup(filename));
    fd_dtsclose(stream,1);
    return -1;}
  stream->mallocd=0;
  fd_setpos(stream,0);
  hi=FD_OID_HI(base); lo=FD_OID_LO(base);
  fd_dtswrite_4bytes(stream,magicno);
  fd_dtswrite_4bytes(stream,hi);
  fd_dtswrite_4bytes(stream,lo);
  fd_dtswrite_4bytes(stream,capacity);
  fd_dtswrite_4bytes(stream,load); /* load */
  fd_dtswrite_4bytes(stream,0); /* label pos */
  i=0; while (i<capacity) {fd_dtswrite_4bytes(stream,0); i++;}
  /* Write an initially empty metadata block */
  fd_dtswrite_4bytes(stream,0xFFFFFFFE);
  fd_dtswrite_4bytes(stream,40);
  i=0; while (i<8) {fd_dtswrite_4bytes(stream,0); i++;}
  if (FD_VOIDP(metadata))
    metadata=fd_empty_slotmap();
  fd_write_pool_metadata(stream,metadata);
  fd_dtsclose(stream,1);
  fd_decref(metadata);
  return 1;
}

/* Making file indices */

FD_EXPORT
/* fd_make_file_index:
    Arguments: a filename string, a magic number (usigned int), an FD_OID,
    a capacity, and a dtype pointer to a metadata description (a slotmap).
    Returns: -1 on error, 1 on success. */
int fd_make_file_index
  (u8_string filename,unsigned int magicno,int n_slots_arg,fdtype metadata)
{
  int i, n_slots;
  struct FD_DTYPE_STREAM _stream;
  struct FD_DTYPE_STREAM *stream=
    fd_init_dtype_file_stream(&_stream,filename,FD_DTSTREAM_CREATE,8192);
  if (stream==NULL) return -1;
  else if ((stream->flags)&FD_DTSTREAM_READ_ONLY) {
    fd_seterr3(fd_CantWrite,"fd_make_file_index",u8_strdup(filename));
    fd_dtsclose(stream,1);
    return -1;}
  stream->mallocd=0;
  if (n_slots_arg<0) n_slots=-n_slots_arg;
  else n_slots=fd_get_hashtable_size(n_slots_arg);
  fd_setpos(stream,0);
  fd_dtswrite_4bytes(stream,magicno);
  fd_dtswrite_4bytes(stream,n_slots);
  i=0; while (i<n_slots) {fd_dtswrite_4bytes(stream,0); i++;}
  /* Write an initially empty metadata block */
  fd_dtswrite_4bytes(stream,0xFFFFFFFE);
  fd_dtswrite_4bytes(stream,40);
  i=0; while (i<8) {fd_dtswrite_4bytes(stream,0); i++;}
  if (FD_VOIDP(metadata))
    metadata=fd_empty_slotmap();
  fd_write_index_metadata(stream,metadata);
  fd_decref(metadata);
  fd_dtsclose(stream,1);
  return 1;
}

/* Opening file indices */

static struct FD_INDEX_OPENER index_openers[FD_N_INDEX_OPENERS];
static int n_index_openers=0;
#if FD_THREADS_ENABLED
static u8_mutex index_openers_lock;
#endif

FD_EXPORT void fd_register_index_opener
  (unsigned int id,
   fd_index (*opener)(u8_string filename,int read_only,int consed),
   fdtype (*mdreader)(FD_DTYPE_STREAM *),
   fdtype (*mdwriter)(FD_DTYPE_STREAM *,fdtype))
{
  fd_lock_mutex(&index_openers_lock);
  if (n_index_openers<FD_N_INDEX_OPENERS) {
    index_openers[n_index_openers].initial_word=id;
    index_openers[n_index_openers].opener=opener;
    index_openers[n_index_openers].read_metadata=mdreader;
    index_openers[n_index_openers].write_metadata=mdwriter;
    n_index_openers++;
    fd_unlock_mutex(&index_openers_lock);}
  else {
    fd_unlock_mutex(&index_openers_lock);
    u8_raise("Too many index openers",
             "fd_register_index_opener",
             NULL);}
}

static u8_string get_index_filename(u8_string spec)
{
  u8_string with_suffix=u8_string_append(spec,".index",NULL);
  u8_string probe=u8_realpath(with_suffix,NULL);
  if ((probe) && (u8_file_existsp(probe))) {
    u8_free(with_suffix);
    return probe;}
  else {
    u8_free(with_suffix); u8_free(probe);
    probe=u8_realpath(spec,NULL);}
  if (u8_file_existsp(probe)) return probe;
  else return NULL;
}

static fd_index open_file_index(u8_string filename,int consed)
{
  u8_string index_filename=get_index_filename(filename);
  fd_index ix;
  if (index_filename==NULL) {
    fd_seterr3(fd_FileNotFound,"open_file_index",u8_strdup(filename));
    return NULL;}
  else ix=fd_find_index_by_cid(index_filename);
  if (ix) {
    u8_free(index_filename);
    return ix;}
  else {
    FILE *f=u8_fopen(index_filename,"r+b");
    unsigned int i=0, word=0, read_only=0; unsigned char byte;
    if (f==NULL) {
      read_only=1; f=fopen(index_filename,"rb");}
    if (f==NULL) {
      fd_seterr3(fd_CantOpenFile,"open_file_index",u8_strdup(index_filename));
      return NULL;}
    byte=getc(f); word=word|(byte<<24);
    byte=getc(f); word=word|(byte<<16);
    byte=getc(f); word=word|(byte<<8);
    byte=getc(f); word=word|(byte<<0);
    fclose(f);
    i=0; while (i < n_index_openers)
      if (((index_openers[i].initial_word&0xFF)==0) ?
          (index_openers[i].initial_word == (word&0xFFFFFF00)) :
          (index_openers[i].initial_word == word)) {
        ix=index_openers[i].opener(index_filename,read_only,consed);
        break;}
      else i++;
    if (ix) {
      if (ix->cid) {
        u8_free(ix->cid); ix->cid=NULL;}
      ix->cid=index_filename;
      ix->xid=u8_realpath(index_filename,NULL);
      fd_register_index(ix);
      return ix;}
    else {
      fd_seterr3(fd_NotAFileIndex,"open_file_index",u8_strdup(index_filename));
      return NULL;}}
}

FD_EXPORT
int fd_file_indexp(u8_string filename)
{
  FILE *f=u8_fopen(filename,"rb");
  unsigned int i=0, word=0, read_only=0; unsigned char byte;
  if (f==NULL) return 0;
  byte=getc(f); word=word|(byte<<24);
  byte=getc(f); word=word|(byte<<16);
  byte=getc(f); word=word|(byte<<8);
  byte=getc(f); word=word|(byte<<0);
  fclose(f);
  i=0; while (i < n_index_openers)
         if (((index_openers[i].initial_word&0xFF)==0) ?
             (index_openers[i].initial_word == (word&0xFFFFFF00)) :
             (index_openers[i].initial_word == word))
           return 1;
         else i++;
  return 0;
}

/* Reading memindices from disk */

static int memindex_commitfn(struct FD_MEM_INDEX *ix,u8_string file)
{
  struct FD_DTYPE_STREAM stream, *rstream;
  if ((ix->adds.n_keys>0) || (ix->edits.n_keys>0)) {
    rstream=fd_init_dtype_file_stream
      (&stream,file,FD_DTSTREAM_CREATE,fd_filedb_bufsize);
    if (rstream==NULL) return -1;
    stream.mallocd=0;
    fd_set_read(&stream,0);
    fd_write_dtype((fd_byte_output)&stream,(fdtype)&(ix->cache));
    fd_dtsclose(&stream,1);
    return 1;}
  else return 0;
}

static fd_index open_memindex(u8_string file,int read_only,int consed)
{
  struct FD_MEM_INDEX *mix=(fd_mem_index)fd_make_mem_index(consed);
  fdtype lispval; struct FD_HASHTABLE *h;
  struct FD_DTYPE_STREAM stream;
  fd_init_dtype_file_stream
    (&stream,file,FD_DTSTREAM_READ,fd_filedb_bufsize);
  stream.mallocd=0;
  lispval=fd_read_dtype((fd_byte_input)&stream);
  fd_dtsclose(&stream,1);
  if (FD_HASHTABLEP(lispval)) h=(fd_hashtable)lispval;
  else {
    fd_decref(lispval);
    return NULL;}
  if (mix->cid) u8_free(mix->cid);
  mix->source=mix->cid=u8_strdup(file);
  mix->commitfn=memindex_commitfn;
  mix->read_only=read_only;
  mix->cache.n_slots=h->n_slots;
  mix->cache.n_keys=h->n_keys;
  mix->cache.loading=h->loading;
  mix->cache.slots=h->slots;
  u8_free(h);
  return (fd_index)mix;
}

/* Initialization */

static int fddbfile_initialized=0;

FD_EXPORT void fd_init_hashdtype_c(void);
FD_EXPORT void fd_init_fileindices_c(void);
FD_EXPORT void fd_init_file_pools_c(void);
FD_EXPORT void fd_init_zpools_c(void);
FD_EXPORT void fd_init_zindices_c(void);
FD_EXPORT void fd_init_hashindices_c(void);
FD_EXPORT void fd_init_oidpools_c(void);


FD_EXPORT int fd_init_dbfile()
{
  if (fddbfile_initialized) return fddbfile_initialized;
  fddbfile_initialized=307*fd_init_db();

  u8_register_source_file(_FILEINFO);

  rev_symbol=fd_intern("REV");
  gentime_symbol=fd_intern("GENTIME");
  packtime_symbol=fd_intern("PACKTIME");
  modtime_symbol=fd_intern("MODTIME");

#if FD_THREADS_ENABLED
  fd_init_mutex(&pool_openers_lock);
  fd_init_mutex(&index_openers_lock);
#endif

  fd_init_hashdtype_c();
  fd_init_fileindices_c();
  fd_init_file_pools_c();
  fd_init_zpools_c();
  fd_init_zindices_c();
  fd_init_hashindices_c();
  fd_init_oidpools_c();

  fd_file_pool_opener=open_file_pool;
  fd_file_index_opener=open_file_index;

  fd_register_index_opener(0x42820000,open_memindex,NULL,NULL);

  fd_register_index_opener(0x42c20000,open_memindex,NULL,NULL);
  fd_register_index_opener(0x42c20100,open_memindex,NULL,NULL);
  fd_register_index_opener(0x42c20200,open_memindex,NULL,NULL);
  fd_register_index_opener(0x42c20300,open_memindex,NULL,NULL);
  fd_register_index_opener(0x42c20400,open_memindex,NULL,NULL);

  fd_register_config("ACIDFILES","Maintain acidity of individual file pools and indices",
                     fd_boolconfig_get,fd_boolconfig_set,&fd_acid_files);
  fd_register_config("DBFILEBUFSIZE",
                     "The size of file streams used in database files",
                     fd_sizeconfig_get,fd_sizeconfig_set,&fd_filedb_bufsize);


  return fddbfile_initialized;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
