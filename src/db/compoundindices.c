/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[]=
  "$Id$";

#define FD_INLINE_DTYPEIO 1

#include "fdb/dtype.h"
#include "fdb/indices.h"

#include <libu8/u8.h>

static struct FD_INDEX_HANDLER compoundindex_handler;

static fdtype compound_fetch(fd_index ix,fdtype key)
{
  struct FD_COMPOUND_INDEX *cix=(struct FD_COMPOUND_INDEX *)ix;
  fdtype combined=FD_EMPTY_CHOICE;
  int i=0, lim;
  u8_lock_mutex(&(cix->lock));
  lim=cix->n_indices;
  while (i < lim) {
    fd_index eix=cix->indices[i++];
    fdtype value;
    if (eix->cache_level<0) {
      eix->cache_level=fd_default_cache_level;
      if (eix->handler->setcache)
	eix->handler->setcache(eix,fd_default_cache_level);}
    if (fd_hashtable_probe(&(eix->cache),key))
      value=fd_hashtable_get(&(eix->cache),key,FD_EMPTY_CHOICE);
    else if ((eix->adds.n_keys) || (eix->edits.n_keys))
      value=fd_index_get(eix,key);
    else value=eix->handler->fetch(eix,key);
    if (FD_EXCEPTIONP(value)) {
      fd_decref(combined); u8_unlock_mutex(&(cix->lock));
      return value;}
    else {FD_ADD_TO_CHOICE(combined,value);}}
  u8_unlock_mutex(&(cix->lock));
  return combined;
}

static int compound_prefetch(fd_index ix,fdtype keys)
{
  int n_fetches=0, i=0, lim;
  struct FD_COMPOUND_INDEX *cix=(struct FD_COMPOUND_INDEX *)ix;
  fdtype *keyv=u8_malloc(sizeof(fdtype)*FD_CHOICE_SIZE(keys));
  fdtype *valuev=u8_malloc(sizeof(fdtype)*FD_CHOICE_SIZE(keys));
  FD_DO_CHOICES(key,keys)
    if (!(fd_hashtable_probe(&(cix->cache),key))) {
      keyv[n_fetches]=key; valuev[n_fetches]=FD_EMPTY_CHOICE; n_fetches++;}
  if (n_fetches==0) {
    u8_free(keyv); u8_free(valuev);
    return 0;}
  u8_lock_mutex(&(cix->lock));
  lim=cix->n_indices;
  while (i < lim) {
    int j=0; fd_index eix=cix->indices[i];
    fdtype *values=
      eix->handler->fetchn(eix,n_fetches,keyv);
    if (values==NULL) {
      u8_free(keyv); u8_free(valuev);
      u8_unlock_mutex(&(cix->lock));
      return -1;}
    while (j<n_fetches) {
      FD_ADD_TO_CHOICE(valuev[j],values[j]); j++;}
    u8_free(values);
    i++;}
  /* The operation fd_table_add_empty_noref will create an entry even if the value
     is the empty choice. */
  fd_hashtable_iter(&(cix->cache),fd_table_add_empty_noref,n_fetches,keyv,valuev);
  u8_free(keyv); u8_free(valuev);
  u8_unlock_mutex(&(cix->lock));
  return n_fetches;
}

static fdtype compound_fetchkeys(fd_index ix)
{
  struct FD_COMPOUND_INDEX *cix=(struct FD_COMPOUND_INDEX *)ix;
  fdtype combined=FD_EMPTY_CHOICE;
  int i=0, lim;
  u8_lock_mutex(&(cix->lock));
  lim=cix->n_indices;
  while (i < lim) {
    fd_index eix=cix->indices[i++];
    fdtype keys=fd_index_keys(eix);
    if (FD_EXCEPTIONP(keys)) {
      fd_decref(combined);
      u8_unlock_mutex(&(cix->lock));
      return keys;}
    else {FD_ADD_TO_CHOICE(combined,keys);}}
  u8_unlock_mutex(&(cix->lock));
  return combined;
}

static u8_string get_compound_id(int n,fd_index *indices)
{
  if (n) {
    struct U8_OUTPUT out; int i=0;
    U8_INIT_OUTPUT(&out,80);
    while (i < n) {
      if (i) u8_puts(&out,"|");
      u8_puts(&out,indices[i]->cid); i++;}
    return out.bytes;}
  else return u8_strdup("compound");
}

FD_EXPORT fd_index fd_make_compound_index(int n_indices,fd_index *indices)
{
  struct FD_COMPOUND_INDEX *cix=u8_malloc_type(struct FD_COMPOUND_INDEX);
  u8_string cid=get_compound_id(n_indices,indices);
  fd_init_index((fd_index)cix,&compoundindex_handler,cid);
  u8_init_mutex(&(cix->lock)); u8_free(cid);
  cix->n_indices=n_indices; cix->indices=indices;
  fd_register_index((fd_index)cix);
  return (fd_index) cix;
}

FD_EXPORT int fd_add_to_compound_index(fd_compound_index cix,fd_index add)
{
  if (cix->handler == &compoundindex_handler) {
    int i=0, n=cix->n_indices;
    while (i < n)
      if (cix->indices[i] == add) {
	u8_unlock_mutex(&(cix->lock)); return 0;}
      else i++;
    if (cix->indices)
      cix->indices=
	u8_realloc(cix->indices,sizeof(fd_index)*(cix->n_indices+1));
    else cix->indices=u8_malloc(sizeof(fd_index)*(cix->n_indices));
    cix->indices[cix->n_indices++]=add;
    u8_free(cix->cid);
    cix->cid=cix->source=get_compound_id(cix->n_indices,cix->indices);
    fd_reset_hashtable(&(cix->cache),-1,1);
    return 1;}
  else return fd_reterr(fd_TypeError,("compound_index"),NULL,FD_VOID);
}

static struct FD_INDEX_HANDLER compoundindex_handler={
  "compoundindex", 1, sizeof(struct FD_COMPOUND_INDEX), 12,
  NULL, /* close */
  NULL, /* commit */
  NULL, /* setcache */
  NULL, /* setbuf */
  compound_fetch, /* fetch */
  NULL, /* fetchsize */
  compound_prefetch, /* prefetch */
  NULL, /* fetchn */
  compound_fetchkeys, /* fetchkeys */
  NULL, /* fetchsizes */
  NULL, /* metadata */
  NULL /* sync */
};

FD_EXPORT fd_init_compoundindices_c()
{
  fd_register_source_file(versionid);
}


/* The CVS log for this file
   $Log: compoundindices.c,v $
   Revision 1.27  2006/01/31 13:47:23  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.26  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.25  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.24  2006/01/02 22:40:22  haase
   Various prefetching fixes

   Revision 1.23  2005/11/11 04:31:53  haase
   Made compound indices have their caches reset when subindices are added to them.

   Revision 1.22  2005/08/21 20:10:26  haase
   Fixed bugs with not initializing the cache level for elements of compound indices

   Revision 1.21  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.20  2005/07/12 21:11:45  haase
   Fixed tricky race condition in compound indices with multiple threads

   Revision 1.19  2005/07/09 16:17:41  haase
   Fix compound index handler declaration

   Revision 1.18  2005/06/19 02:39:57  haase
   Initialize the compound index key values before prefetching

   Revision 1.17  2005/06/04 12:56:01  haase
   More error passing fixes

   Revision 1.16  2005/05/23 00:53:24  haase
   Fixes to header ordering to get off_t consistently defined

   Revision 1.15  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.14  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.13  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.12  2005/03/28 19:19:35  haase
   Added metadata reading and writing and file pool/index creation

   Revision 1.11  2005/03/06 19:26:44  haase
   Plug some leaks and some failures to return values

   Revision 1.10  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.9  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.8  2005/02/15 03:03:40  haase
   Updated to use the new libu8

   Revision 1.7  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
