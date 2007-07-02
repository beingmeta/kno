/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_INLINE_TABLES 1
#define FD_INLINE_POOLS 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_IPEVAL 1

#include "fdb/dtype.h"
#include "fdb/fddb.h"
#include "fdb/apply.h"

#include <stdarg.h>

fdtype fd_adjunct_slotids;

static int n_adjuncts;
static struct FD_KEYVAL *adjuncts=NULL;

static fd_index get_adjunct_index(fdtype slotid,fd_pool p)
{
  struct FD_KEYVAL *adji=fd_sortvec_get(slotid,p->adjuncts,p->n_adjuncts);
  if (adji)
    if (FD_PTR_TYPEP(adji->value,fd_index_type))
      return fd_lisp2index(adji->value);
    else return NULL;
  else return NULL;
}

static fd_index get_global_adjunct_index(fdtype slotid)
{
  struct FD_KEYVAL *adji=fd_sortvec_get(slotid,adjuncts,n_adjuncts);
  if (adji)
    if (FD_PTR_TYPEP(adji->value,fd_index_type))
      return fd_lisp2index(adji->value);
    else return NULL;
  else return NULL;
}

static int anonymous_oiderr(u8_string op,fdtype f)
{
  fd_seterr(fd_AnonymousOID,op,NULL,f);
  return -1;
}

FD_EXPORT int fd_set_adjunct(fd_index ix,fdtype slotid,fd_pool p)
{
  if (fd_overlapp(slotid,fd_adjunct_slotids)==0) {
    FD_ADD_TO_CHOICE(fd_adjunct_slotids,slotid);
    fd_adjunct_slotids=fd_simplify_choice(fd_adjunct_slotids);}
  if (p) {
    struct FD_KEYVAL *entry=fd_sortvec_insert(slotid,&(p->adjuncts),&(p->n_adjuncts));
    if (entry==NULL) {
      fd_seterr(fd_MallocFailed,"fd_set_adjunct",NULL,FD_VOID);
      return -1;}
    if (ix) entry->value=fd_index2lisp(ix);
    else entry->value=FD_VOID;}
  else {
    struct FD_KEYVAL *entry=fd_sortvec_insert(slotid,&adjuncts,&n_adjuncts);
    if (entry==NULL) {
      fd_seterr(fd_MallocFailed,"fd_set_adjunct",NULL,FD_VOID);
      return -1;}
    if (ix) entry->value=fd_index2lisp(ix);
    else entry->value=FD_VOID;}
  return 1;
}

static fd_index get_adjunct(fdtype slotid,fd_pool p)
{
  if (p) {
    struct FD_KEYVAL *entry=
      ((p->n_adjuncts) ? (fd_sortvec_get(slotid,p->adjuncts,p->n_adjuncts)) : (NULL));
    if ((entry==NULL) && (n_adjuncts))
      entry=fd_sortvec_get(slotid,adjuncts,n_adjuncts);
    if (entry)
      if (FD_INDEXP(entry->value)) return fd_lisp2index(entry->value);
      else return NULL;
    else return NULL;}
  else {
    struct FD_KEYVAL *entry=((n_adjuncts) ? (fd_sortvec_get(slotid,adjuncts,n_adjuncts)) : (NULL));
    if (entry)
      if (FD_INDEXP(entry->value)) return fd_lisp2index(entry->value);
      else return NULL;
    else return NULL;}
}

FD_EXPORT fd_index fd_get_adjunct(fdtype slotid,fd_pool p)
{
  return get_adjunct(slotid,p);
}

FD_EXPORT int fd_adjunctp(fdtype slotid,fd_pool p)
{
  if (p) {
    struct FD_KEYVAL *entry=
      ((p->n_adjuncts) ? (fd_sortvec_get(slotid,p->adjuncts,p->n_adjuncts)) : (NULL));
    if (((entry==NULL) || (!(FD_INDEXP(entry->value)))) && (n_adjuncts))
      entry=fd_sortvec_get(slotid,adjuncts,n_adjuncts);
    if ((entry) && (FD_INDEXP(entry->value))) return 1;
    else return 0;}
  else {
    struct FD_KEYVAL *entry=((n_adjuncts) ? (fd_sortvec_get(slotid,adjuncts,n_adjuncts)) : (NULL));
    if (entry)
      if (FD_INDEXP(entry->value)) return 1;
      else return 0;
    else return 0;}
}

/* Table operations on OIDs */

/* OIDs provide an intermediate data layer.  In general, getting a slotid from an OID gets
   the OID's value and then gets the slotid from that value (whatever type of object it is).
   The exception to this case is that certain pools can declare that certain keys (slotids)
   are stored in external indices.  These indices, called adjunct indices, allow another layer
   of description. */
FD_EXPORT fdtype fd_oid_get(fdtype f,fdtype slotid,fdtype dflt)
{
  fd_pool p=fd_oid2pool(f);
  if (FD_EXPECT_FALSE(p == NULL))
    if (fd_ignore_anonymous_oids) {
      fd_index adj=get_adjunct(slotid,p);
      if (adj) {
	fdtype result=fd_index_get(adj,f);
	if (FD_EMPTY_CHOICEP(result)) return fd_incref(dflt);
	else return result;}
      else return fd_incref(dflt);}
    else return fd_err(fd_AnonymousOID,NULL,NULL,f);
  else {
    fd_index adj=get_adjunct(slotid,p); fdtype smap;
    if (adj) return fd_index_get(adj,f);
    else smap=fd_fetch_oid(p,f);
    if (FD_EXPECT_FALSE(FD_ABORTP(smap)))
      return smap;
    else if (FD_SLOTMAPP(smap)) {
      fdtype value=fd_slotmap_get((fd_slotmap)smap,slotid,dflt);
      fd_decref(smap);
      return value;}
    else if (FD_SCHEMAPP(smap)) {
      fdtype value=fd_schemap_get((fd_schemap)smap,slotid,dflt);
      fd_decref(smap);
      return value;}
    else if (FD_TABLEP(smap)) {
      fdtype value=fd_get(smap,slotid,dflt);
      fd_decref(smap);
      return value;}
    else {
      fd_decref(smap);
      return fd_incref(dflt);}}
}

FD_EXPORT int fd_oid_add(fdtype f,fdtype slotid,fdtype value)
{
  fd_pool p=fd_oid2pool(f);
  if (FD_EXPECT_FALSE(p == NULL))
    return anonymous_oiderr("fd_oid_add",f);
  else {
    fdtype smap; int retval;
    fd_index adj=get_adjunct(slotid,p); 
    if (adj) return fd_index_add(adj,f,value);
    else smap=fd_locked_oid_value(p,f);
    if (FD_EXPECT_FALSE(FD_ABORTP(smap)))
      return fd_interr(smap);
    else if (FD_SLOTMAPP(smap))
      retval=fd_slotmap_add(FD_XSLOTMAP(smap),slotid,value);
    else if (FD_SCHEMAPP(smap))
      retval=fd_schemap_add(FD_XSCHEMAP(smap),slotid,value);
    else retval=fd_add(smap,slotid,value);
    fd_decref(smap);
    return retval;}
}

FD_EXPORT int fd_oid_store(fdtype f,fdtype slotid,fdtype value)
{
  fd_pool p=fd_oid2pool(f);
  if (FD_EXPECT_FALSE(p == NULL))
    return anonymous_oiderr("fd_oid_store",f);
  else {
    fdtype smap; int retval;
    fd_index adj=get_adjunct(slotid,p); 
    if (adj) return fd_index_store(adj,f,value);
    else smap=fd_locked_oid_value(p,f);
    if (FD_ABORTP(smap))
      return fd_interr(smap);
    else if (FD_SLOTMAPP(smap))
      if (FD_EMPTY_CHOICEP(value))
	retval=fd_slotmap_delete(FD_XSLOTMAP(smap),slotid);
      else retval=fd_slotmap_store(FD_XSLOTMAP(smap),slotid,value);
    else if (FD_SCHEMAPP(smap))
      retval=fd_schemap_store(FD_XSCHEMAP(smap),slotid,value);
    else retval=fd_store(smap,slotid,value);
    fd_decref(smap);
    return retval;}
}

FD_EXPORT int fd_oid_delete(fdtype f,fdtype slotid)
{
  fd_pool p=fd_oid2pool(f);
  if (FD_EXPECT_FALSE(p == NULL))
    return anonymous_oiderr("fd_oid_delete",f);
  else {
    fdtype smap; int retval;
    fd_index adj=get_adjunct(slotid,p); 
    if (adj) return fd_index_store(adj,f,FD_EMPTY_CHOICE);
    else smap=fd_locked_oid_value(p,f);
    if (FD_ABORTP(smap))
      return fd_interr(smap);
    else if (FD_SLOTMAPP(smap))
      retval=fd_slotmap_delete(FD_XSLOTMAP(smap),slotid);
    else if (FD_SCHEMAPP(smap))
      retval=fd_schemap_store(FD_XSCHEMAP(smap),slotid,FD_EMPTY_CHOICE);
    else retval=fd_store(smap,slotid,FD_EMPTY_CHOICE);
    fd_decref(smap);
    return retval;}
}

FD_EXPORT int fd_oid_drop(fdtype f,fdtype slotid,fdtype value)
{
  fd_pool p=fd_oid2pool(f);
  if (FD_EXPECT_FALSE(p == NULL))
    return anonymous_oiderr("fd_oid_drop",f);
  else {
    fdtype smap; int retval;
    fd_index adj=get_adjunct(slotid,p);
    if (adj) return fd_index_drop(adj,f,value);
    else smap=fd_locked_oid_value(p,f);
    if (FD_ABORTP(smap))
      return fd_interr(smap);
    else if (FD_SLOTMAPP(smap))
      retval=fd_slotmap_drop(FD_XSLOTMAP(smap),slotid,value);
    else if (FD_SCHEMAPP(smap))
      retval=fd_schemap_drop(FD_XSCHEMAP(smap),slotid,value);
    else retval=fd_drop(smap,slotid,value);
    fd_decref(smap);
    return retval;}
}

FD_EXPORT int fd_oid_test(fdtype f,fdtype slotid,fdtype value)
{
  fd_pool p=fd_oid2pool(f);
  if (FD_EXPECT_FALSE(p == NULL))
    return anonymous_oiderr("fd_oid_test",f);
  else if (FD_VOIDP(value)) {
    fdtype v=fd_oid_get(f,slotid,FD_VOID);
    if (FD_VOIDP(v)) return 0;
    else {fd_decref(v); return 1;}}
  else {
    fdtype smap; int retval;
    fd_index adj=get_adjunct(slotid,p); 
    if (adj) {
      fdtype current=fd_index_get(adj,f);
      int found=fd_overlapp(value,current);
      fd_decref(current);
      return found;}
    else smap=fd_fetch_oid(p,f);
    if (FD_ABORTP(smap)) 
      retval=fd_interr(smap);
    else if (FD_SLOTMAPP(smap))
      retval=fd_slotmap_test((fd_slotmap)smap,slotid,value);
    else if (FD_SCHEMAPP(smap))
      retval=fd_schemap_test((fd_schemap)smap,slotid,value);
    else if (FD_TABLEP(smap))
      retval=fd_test(smap,slotid,value);
    else {
      fd_decref(smap);
      return 0;}
    fd_decref(smap);
    return retval;}
}

FD_EXPORT fdtype fd_oid_keys(fdtype f)
{
  fd_pool p=fd_oid2pool(f);
  if (FD_EXPECT_FALSE(p == NULL))
    return fd_anonymous_oid("fd_oid_keys",f);
  else {
    fdtype smap=fd_fetch_oid(p,f);
    if (FD_ABORTP(smap)) return smap;
    else if (FD_TABLEP(smap)) {
      fdtype result=fd_getkeys(smap);
      fd_decref(smap);
      return result;}
    else return FD_EMPTY_CHOICE;}
}

/* Table operations on choices */

static fdtype choice_get(fdtype arg,fdtype slotid,fdtype dflt)
{
  fdtype results=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(each,arg) {
    fdtype v;
    if (FD_OIDP(each))
      v=fd_oid_get(each,slotid,FD_EMPTY_CHOICE);
    else v=fd_get(each,slotid,FD_EMPTY_CHOICE);
    FD_ADD_TO_CHOICE(results,v);}
  if (FD_EMPTY_CHOICEP(results))
    return fd_incref(dflt);
  else return fd_simplify_choice(results);
}

static int choice_add(fdtype arg,fdtype slotid,fdtype value)
{
  if (FD_EMPTY_CHOICEP(value)) return 0;
  else {
    FD_DO_CHOICES(each,arg)
      if (fd_add(each,slotid,value)<0) return -1;
    return 1;}
}

static int choice_store(fdtype arg,fdtype slotid,fdtype value)
{
  FD_DO_CHOICES(each,arg)
    if (fd_store(each,slotid,value)<0) return -1;
  return 1;
}

static int choice_drop(fdtype arg,fdtype slotid,fdtype value)
{
  FD_DO_CHOICES(each,arg)
    if (fd_drop(each,slotid,value)<0) return -1;
  return 1;
}

static int choice_test(fdtype arg,fdtype slotid,fdtype value)
{
  int result=0;
  FD_DO_CHOICES(each,arg) {
    if (result=fd_test(each,slotid,value)) return result;}
  return 0;
}

static fdtype choice_keys(fdtype arg)
{
  fdtype results=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(each,arg) {
    fdtype keys=fd_getkeys(each);
    if (FD_ABORTP(keys)) {
      fd_decref(results); return keys;}
    else {FD_ADD_TO_CHOICE(results,keys);}}
  return results;
}


/* Initialization */

FD_EXPORT void fd_init_xtables_c()
{
  fd_register_source_file(versionid);

  fd_adjunct_slotids=FD_EMPTY_CHOICE;

  fd_tablefns[fd_oid_type]=u8_malloc_type(struct FD_TABLEFNS);
  fd_tablefns[fd_oid_type]->get=fd_oid_get;
  fd_tablefns[fd_oid_type]->add=fd_oid_add;
  fd_tablefns[fd_oid_type]->drop=fd_oid_drop;
  fd_tablefns[fd_oid_type]->store=fd_oid_store;
  fd_tablefns[fd_oid_type]->test=fd_oid_test;
  fd_tablefns[fd_oid_type]->keys=fd_oid_keys;
  fd_tablefns[fd_oid_type]->getsize=NULL;

  fd_tablefns[fd_choice_type]=u8_malloc_type(struct FD_TABLEFNS);
  fd_tablefns[fd_choice_type]->get=choice_get;
  fd_tablefns[fd_choice_type]->add=choice_add;
  fd_tablefns[fd_choice_type]->drop=choice_drop;
  fd_tablefns[fd_choice_type]->store=choice_store;
  fd_tablefns[fd_choice_type]->test=choice_test;
  fd_tablefns[fd_choice_type]->keys=choice_keys;
  fd_tablefns[fd_choice_type]->getsize=NULL;
}
