/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/dbfile.h"

#include <libu8/libu8io.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>

#include <stdio.h>

static fdtype rev_symbol, gentime_symbol, packtime_symbol, modtime_symbol;

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
  u8_lock_mutex(&pool_openers_lock);
  if (n_pool_openers<FD_N_POOL_OPENERS) {
    pool_openers[n_pool_openers].initial_word=id;
    pool_openers[n_pool_openers].opener=opener;
    pool_openers[n_pool_openers].read_metadata=mdreader;
    pool_openers[n_pool_openers].write_metadata=mdwriter;
    n_pool_openers++;
    u8_unlock_mutex(&pool_openers_lock);}
  else {
    u8_unlock_mutex(&pool_openers_lock);
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
    fd_seterr3(fd_CantFindFile,"open_file_pool",u8_strdup(filename));
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
  int i=0, byte, word; 
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

static fdtype read_metadata(struct FD_DTYPE_STREAM *ds,off_t mdblockpos)
{
  fdtype metadata=FD_VOID;
  struct U8_XTIME _gentime, _packtime, _modtime; int rev;
  int capacity, mdversion, mdlen, repack, timehi, timelo;
  off_t mdpos;
  fd_setpos(ds,mdblockpos);
  mdversion=fd_dtsread_4bytes(ds);
  if (mdversion==0xFFFFFFFF) {
    rev=fd_dtsread_4bytes(ds);
    mdpos=fd_dtsread_4bytes(ds);
    if (mdpos) {
      fd_setpos(ds,mdpos);
      metadata=fd_dtsread_dtype(ds);}
    else metadata=fd_init_slotmap(NULL,0,NULL,NULL);
    fd_store(metadata,rev_symbol,FD_INT2DTYPE(rev));
    return metadata;}
  else if (mdversion!=0xFFFFFFFE)
    return fd_err(fd_BadMetaData,NULL,u8_strdup(ds->id),FD_VOID);
  mdlen=fd_dtsread_4bytes(ds);
  rev=fd_dtsread_4bytes(ds);
  timehi=fd_dtsread_4bytes(ds);
  timelo=fd_dtsread_4bytes(ds);
  if (timehi) 
    return fd_err(fd_BadMetaData,"time warp",u8_strdup(ds->id),FD_VOID);
  u8_offtime(&_gentime,timelo,0);
  timehi=fd_dtsread_4bytes(ds);
  timelo=fd_dtsread_4bytes(ds);
  if (timehi) 
    return fd_err(fd_BadMetaData,"time warp",u8_strdup(ds->id),FD_VOID);
  u8_offtime(&_packtime,timelo,0);
  timehi=fd_dtsread_4bytes(ds);
  timelo=fd_dtsread_4bytes(ds);
  if (timehi) 
    return fd_err(fd_BadMetaData,"time warp",u8_strdup(ds->id),FD_VOID);
  u8_offtime(&_modtime,timelo,0);
  mdpos=fd_dtsread_4bytes(ds);
  if (mdpos) {
    fd_setpos(ds,mdpos);
    metadata=fd_dtsread_dtype(ds);}
  else metadata=fd_init_slotmap(NULL,0,NULL,NULL);
  fd_store(metadata,rev_symbol,FD_INT2DTYPE(rev));
  fd_store(metadata,gentime_symbol,fd_make_timestamp(&_gentime,NULL));
  fd_store(metadata,packtime_symbol,fd_make_timestamp(&_packtime,NULL));
  fd_store(metadata,modtime_symbol,fd_make_timestamp(&_modtime,NULL));
  return metadata;
}

FD_EXPORT
fdtype fd_read_pool_metadata(struct FD_DTYPE_STREAM *ds)
{
  int capacity; off_t mdblockpos;
  int ret=fd_setpos(ds,12);
  if (ret<0) return fd_erreify();
  else capacity=fd_dtsread_4bytes(ds);
  mdblockpos=24+capacity*4;
  return read_metadata(ds,mdblockpos);
}

FD_EXPORT
fdtype fd_read_index_metadata(struct FD_DTYPE_STREAM *ds)
{
  int n_slots; off_t mdblockpos;
  int ret=fd_setpos(ds,4);
  if (ret<0) return fd_erreify();
  else n_slots=fd_dtsread_4bytes(ds);
  mdblockpos=8+n_slots*4;
  return read_metadata(ds,mdblockpos);
}

static void copy_timeinfo(struct U8_XTIME *tp,fdtype md,fdtype slotid)
{
  fdtype tval=fd_get(md,slotid,FD_VOID);
  if (FD_PRIM_TYPEP(tval,fd_timestamp_type)) {
    struct FD_TIMESTAMP *tstamp=
      FD_GET_CONS(tval,fd_timestamp_type,struct FD_TIMESTAMP *);
    memcpy(tp,&(tstamp->xtime),sizeof(struct U8_XTIME));}
  else u8_localtime(tp,time(NULL));
}

static write_metadata(fd_dtype_stream ds,off_t mdblockpos,fdtype metadata)
{
  struct U8_XTIME _gentime, _packtime, _modtime;
  int rev, capacity, mdpos, mdversion;
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
  fd_dtswrite_4bytes(ds,_gentime.u8_secs);
  fd_dtswrite_4bytes(ds,0);
  fd_dtswrite_4bytes(ds,_packtime.u8_secs);
  fd_dtswrite_4bytes(ds,0);
  fd_dtswrite_4bytes(ds,_modtime.u8_secs);
  fd_dtswrite_4bytes(ds,mdpos);
  return metadata;
}

FD_EXPORT
fdtype fd_write_pool_metadata(fd_dtype_stream ds,fdtype metadata)
{
  off_t md_pos;
  int ret=fd_setpos(ds,12);
  if (ret<0) return fd_erreify();
  else md_pos=fd_dtsread_4bytes(ds)*4+24;
  write_metadata(ds,md_pos,metadata);
}

FD_EXPORT
fdtype fd_write_index_metadata(fd_dtype_stream ds,fdtype metadata)
{
  off_t md_pos; 
  int ret=fd_setpos(ds,4);
  if (ret<0) return fd_erreify();
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
  (u8_string filename,unsigned int magicno,FD_OID base,int capacity,
   fdtype metadata)
{
  int i, hi, lo;
  struct FD_DTYPE_STREAM _stream;
  struct FD_DTYPE_STREAM *stream=
    fd_init_dtype_file_stream
    (&_stream,filename,FD_DTSTREAM_CREATE,8192,NULL,NULL);
  if (stream==NULL) return -1;
  else if (stream->bits&FD_DTSTREAM_READ_ONLY) {
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
  fd_dtswrite_4bytes(stream,0); /* load */
  fd_dtswrite_4bytes(stream,0); /* label pos */
  i=0; while (i<capacity) {fd_dtswrite_4bytes(stream,0); i++;}
  /* Write an initially empty metadata block */
  fd_dtswrite_4bytes(stream,0xFFFFFFFE);
  fd_dtswrite_4bytes(stream,40);
  i=0; while (i<8) {fd_dtswrite_4bytes(stream,0); i++;}
  if (FD_VOIDP(metadata))
    metadata=fd_init_slotmap(NULL,0,NULL,NULL);
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
  int i, hi, lo, n_slots;
  struct FD_DTYPE_STREAM _stream;
  struct FD_DTYPE_STREAM *stream=
    fd_init_dtype_file_stream
    (&_stream,filename,FD_DTSTREAM_CREATE,8192,NULL,NULL);
  if (stream==NULL) return -1;
  else if (stream->bits&FD_DTSTREAM_READ_ONLY) {
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
    metadata=fd_init_slotmap(NULL,0,NULL,NULL);
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
   fd_index (*opener)(u8_string filename,int read_only),
   fdtype (*mdreader)(FD_DTYPE_STREAM *),
   fdtype (*mdwriter)(FD_DTYPE_STREAM *,fdtype))
{
  u8_lock_mutex(&index_openers_lock);
  if (n_index_openers<FD_N_INDEX_OPENERS) {
    index_openers[n_index_openers].initial_word=id;
    index_openers[n_index_openers].opener=opener;
    index_openers[n_index_openers].read_metadata=mdreader;
    index_openers[n_index_openers].write_metadata=mdwriter;
    n_index_openers++;
    u8_unlock_mutex(&index_openers_lock);}
  else {
    u8_unlock_mutex(&index_openers_lock);
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

static fd_index open_file_index(u8_string filename)
{
  u8_string index_filename=get_index_filename(filename);
  fd_index ix;
  if (index_filename==NULL) {
    fd_seterr3(fd_CantFindFile,"open_file_index",u8_strdup(filename));
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
	ix=index_openers[i].opener(index_filename,read_only);
	break;}
      else i++;
    if (ix) {
      ix->cid=index_filename;
      ix->xid=u8_realpath(index_filename,NULL);
      fd_register_index(ix);
      return ix;}
    else {
      fd_seterr3(fd_NotAFileIndex,"open_file_index",u8_strdup(index_filename));
      return NULL;}}
}

/* Reading memindices from disk */

static int memindex_commitfn(struct FD_MEM_INDEX *ix,u8_string file)
{
  struct FD_DTYPE_STREAM stream, *rstream;
  if ((ix->adds.n_keys>0) || (ix->edits.n_keys>0)) {
    rstream=fd_init_dtype_file_stream
      (&stream,file,FD_DTSTREAM_CREATE,FD_FILEDB_BUFSIZE,NULL,NULL);
    if (rstream==NULL) return -1;
    stream.mallocd=0;
    fd_set_read(&stream,0);
    fd_write_dtype((fd_byte_output)&stream,(fdtype)&(ix->cache));
    fd_dtsclose(&stream,1);
    return 1;}
  else return 0;
}

static fd_index open_memindex(u8_string file,int read_only)
{
  struct FD_MEM_INDEX *mix=(fd_mem_index)fd_make_mem_index();
  fdtype lispval; struct FD_HASHTABLE *h;
  struct FD_DTYPE_STREAM stream;
  fd_init_dtype_file_stream
    (&stream,file,FD_DTSTREAM_READ,FD_FILEDB_BUFSIZE,NULL,NULL);
  stream.mallocd=0;
  lispval=fd_read_dtype((fd_byte_input)&stream,NULL);
  fd_dtsclose(&stream,1);
  if (FD_HASHTABLEP(lispval)) h=(fd_hashtable)lispval;
  else {
    fd_decref(lispval);
    return NULL;}
  u8_free(mix->cid); mix->source=mix->cid=u8_strdup(file);
  mix->commitfn=memindex_commitfn;
  mix->read_only=read_only;
  mix->cache.n_slots=h->n_slots;
  mix->cache.n_keys=h->n_keys;
  mix->cache.loading=h->loading;
  mix->cache.slots=h->slots;
  mix->cache.mpool=h->mpool;
  u8_free(h);
  return (fd_index)mix;
}

/* Initialization */

static int fddbfile_initialized=0;

FD_EXPORT int fd_init_dbfile()
{
  if (fddbfile_initialized) return fddbfile_initialized;
  fddbfile_initialized=307*fd_init_db();

  fd_register_source_file(versionid);

  rev_symbol=fd_intern("REV");
  gentime_symbol=fd_intern("GENTIME");
  packtime_symbol=fd_intern("PACKTIME");
  modtime_symbol=fd_intern("MODTIME");

#if FD_THREADS_ENABLED
  u8_init_mutex(&pool_openers_lock);
  u8_init_mutex(&index_openers_lock);  
#endif

  fd_init_hashdtype_c();
  fd_init_fileindices_c();
  fd_init_filepools_c();
  fd_init_zpools_c();
  fd_init_zindices_c();

  fd_file_pool_opener=open_file_pool;
  fd_file_index_opener=open_file_index;

  fd_register_index_opener(0x42c20000,open_memindex,NULL,NULL);
  fd_register_index_opener(0x42c20100,open_memindex,NULL,NULL);
  fd_register_index_opener(0x42c20200,open_memindex,NULL,NULL);

  return fddbfile_initialized;
}


/* The CVS log for this file
   $Log: dbfile.c,v $
   Revision 1.35  2006/01/31 13:47:23  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.34  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.33  2006/01/17 19:06:25  haase
   Fixed failure to copy error details in failed file pool open

   Revision 1.32  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.31  2006/01/07 14:01:13  haase
   Fixed some leaks

   Revision 1.30  2006/01/05 18:04:44  haase
   Made pool access check return values from fd_setpos

   Revision 1.29  2005/12/22 14:39:43  haase
   Removed some leaks

   Revision 1.28  2005/12/19 00:47:06  haase
   Added leak in empty metadata frame for empty file index creation

   Revision 1.27  2005/10/10 16:53:48  haase
   Fixes for new mktime/offtime functions

   Revision 1.26  2005/08/21 17:57:58  haase
   Moved file config code into fdlisp, controled by configure --enable-fileconfig

   Revision 1.25  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.24  2005/07/14 01:42:50  haase
   Added memindices which are completely in-memory tables

   Revision 1.23  2005/07/13 22:11:57  haase
   Fixed index modification to do the right thing with an uncached index

   Revision 1.22  2005/05/23 00:53:24  haase
   Fixes to header ordering to get off_t consistently defined

   Revision 1.21  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.20  2005/05/17 20:31:27  haase
   Fixed metadata writing bugs

   Revision 1.19  2005/05/12 03:20:46  haase
   Made the config search path settable and made the default not include the ~ path

   Revision 1.18  2005/04/26 01:40:33  haase
   Remove trailing newlines in file config content

   Revision 1.17  2005/04/25 02:32:12  haase
   Added file-oriented config handling

   Revision 1.16  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.15  2005/03/30 15:30:00  haase
   Made calls to new seterr do appropriate strdups

   Revision 1.14  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.13  2005/03/29 04:12:36  haase
   Added pool/index making primitives

   Revision 1.12  2005/03/28 19:19:35  haase
   Added metadata reading and writing and file pool/index creation

   Revision 1.11  2005/03/25 19:49:47  haase
   Removed base library for eframerd, deferring to libu8

   Revision 1.10  2005/03/18 02:25:13  haase
   Better error signalling

   Revision 1.9  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.8  2005/03/04 04:08:33  haase
   Fixes for minor libu8 changes

   Revision 1.7  2005/03/03 17:58:14  haase
   Moved stdio dependencies out of fddb and reorganized make structure

   Revision 1.6  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
