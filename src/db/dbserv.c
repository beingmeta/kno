/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_INLINE_POOLS 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_IPEVAL 1

#include "fdb/dtype.h"
#include "fdb/pools.h"
#include "fdb/indices.h"
#include "fdb/apply.h"
#include "fdb/frames.h"

static int check_pool_validity=0;
static fd_pool primary_pool=NULL;
static fd_pool served_pools[FDBSERV_MAX_POOLS];
static int n_served_pools=0;
struct FD_COMPOUND_INDEX *primary_index=NULL;

fd_exception fd_PrivateOID=_("private OID");

static int served_poolp(fd_pool p)
{
  int i=0; while (i<n_served_pools)
	     if (served_pools[i]==p) return 1;
	     else i++;
  return 0;
}

/* pool DB methods */

/* Just doing reading for now. */

static fdtype server_get_load(fdtype oid_arg)
{
  if (FD_VOIDP(oid_arg))
    if (primary_pool)
      return fd_pool_load(primary_pool);
    else return fd_err(_("No primary pool"),"server_get_load",NULL,FD_VOID);
  else if (FD_OIDP(oid_arg)) {
    fd_pool p=fd_oid2pool(oid_arg);
    int load=fd_pool_load(p);
    if (load<0)
      return fd_err(_("No method for load"),"server_get_load",NULL,oid_arg);
    else return FD_INT2DTYPE(load);}
  else return fd_type_error("OID","server_get_load",oid_arg);
}

static fdtype server_oid_value(fdtype x)
{
  fd_pool p=fd_oid2pool(x);
  if (p==NULL)
    return fd_err(fd_AnonymousOID,"server_oid_value",NULL,x);
  else if (served_poolp(p))
    return fd_fetch_oid(p,x);
  else return fd_err(fd_PrivateOID,"server_oid_value",NULL,x);
}

static fdtype server_fetch_oids(fdtype oidvec)
{
  /* We assume here that all the OIDs in oidvec are in the same pool.  This should
     be the case because clients see the different pools and sort accordingly.  */
  fd_pool p=NULL;
  int n=FD_VECTOR_LENGTH(oidvec), fetchn=0;
  fdtype *elts=FD_VECTOR_DATA(oidvec), *results, *fetch;
  if (n==0)
    return fd_init_vector(NULL,0,NULL);
  else if (!(FD_OIDP(elts[0])))
    return fd_type_error(_("oid vector"),"server_fetch_oids",oidvec);
  else if (p=fd_oid2pool(elts[0]))
    if (served_poolp(p)) {
      fdtype *results=u8_malloc(sizeof(fdtype)*n);
      if (p->handler->fetchn) {
	fdtype *fetch=u8_malloc(sizeof(fdtype)*n);
	fd_hashtable cache=&(p->cache), locks=&(p->locks);
	int i=0; while (i<n)
		   if ((fd_hashtable_probe_novoid(cache,elts[i])==0) &&
		       (fd_hashtable_probe_novoid(locks,elts[i])==0))
		     fetch[fetchn++]=elts[i++];
		   else i++;
	p->handler->fetchn(p,fetchn,fetch);
	i=0; while (i<n) {
	  results[i]=fd_fetch_oid(p,elts[i]); i++;}
	return fd_init_vector(NULL,n,results);}
      else {
	int i=0; while (i<n) {
	  results[i]=fd_fetch_oid(p,elts[i]); i++;}
	return fd_init_vector(NULL,n,results);}}
    else return fd_err(fd_PrivateOID,"server_oid_value",NULL,elts[0]);
 else return fd_err(fd_AnonymousOID,"server_oid_value",NULL,elts[0]);
}

static fdtype server_pool_data(fdtype session_id)
{
  int len=n_served_pools;
  fdtype *elts=u8_malloc(sizeof(fdtype)*len);
  int i=0; while (i<len) {
    fd_pool p=served_pools[i];
    fdtype base=fd_make_oid(p->base);
    fdtype capacity=FD_INT2DTYPE(p->capacity);
    fdtype ro=((p->read_only) ? (FD_FALSE) : (FD_TRUE));
    elts[i++]=
      ((p->label) ? (fd_make_list(4,base,capacity,ro,fdtype_string(p->label))) :
       (fd_make_list(3,base,capacity,ro)));}
  return fd_init_vector(NULL,len,elts);
}

/* index DB methods */

static fdtype iserver_get(fdtype key)
{
  return fd_index_get((fd_index)(primary_index),key);
}
static fdtype iserver_bulk_get(fdtype keys)
{
  if (FD_VECTORP(keys)) {
    int i=0, n=FD_VECTOR_LENGTH(keys);
    fdtype *data=FD_VECTOR_DATA(keys), *results=u8_malloc(sizeof(fdtype)*n);
    fdtype aschoice=
      fd_make_choice(n,data,(FD_CHOICE_DOSORT|FD_CHOICE_ISATOMIC));
    fd_index_prefetch((fd_index)(primary_index),aschoice);
    while (i<n) {
      results[i]=fd_index_get((fd_index)(primary_index),data[i]); i++;}
    fd_decref(aschoice);
    return fd_init_vector(NULL,n,results);}
  else return fd_type_error("vector","iserver_bulk_get",keys);
}
static fdtype iserver_get_size(fdtype key)
{
  fdtype value=fd_index_get((fd_index)(primary_index),key);
  int size=FD_CHOICE_SIZE(value);
  fd_decref(value);
  return FD_INT2DTYPE(size);
}
static fdtype iserver_keys(fdtype key)
{
  return fd_index_keys((fd_index)(primary_index));
}
static fdtype iserver_sizes(fdtype key)
{
  return fd_index_sizes((fd_index)(primary_index));
}
static fdtype iserver_writablep()
{
  return FD_FALSE;
}

static fdtype ixserver_get(fdtype index,fdtype key)
{
  if (FD_INDEXP(index))
    return fd_index_get(fd_lisp2index(index),key);
  else return fd_type_error("index","ixserver_get",FD_VOID);
}
static fdtype ixserver_bulk_get(fdtype index,fdtype keys)
{
  if (FD_INDEXP(index)) 
    if (FD_VECTORP(keys)) {
      fd_index ix=fd_lisp2index(index);
      int i=0, n=FD_VECTOR_LENGTH(keys);
      fdtype *data=FD_VECTOR_DATA(keys),
	*results=u8_malloc(sizeof(fdtype)*n);
      fdtype aschoice=
	fd_make_choice(n,data,(FD_CHOICE_DOSORT|FD_CHOICE_ISATOMIC));
      fd_index_prefetch(ix,aschoice);
      while (i<n) {
	results[i]=fd_index_get(ix,data[i]); i++;}
      fd_decref(aschoice);
      return fd_init_vector(NULL,n,results);}
    else return fd_type_error("vector","ixserver_bulk_get",keys);
  else return fd_type_error("index","ixserver_get",FD_VOID);
}
static fdtype ixserver_get_size(fdtype index,fdtype key)
{
  if (FD_INDEXP(index)) {
    fdtype value=fd_index_get(fd_lisp2index(index),key);
    int size=FD_CHOICE_SIZE(value);
    fd_decref(value);
    return FD_INT2DTYPE(size);}
  else return fd_type_error("index","ixserver_get",FD_VOID);
}
static fdtype ixserver_keys(fdtype index)
{
  if (FD_INDEXP(index))
    return fd_index_keys(fd_lisp2index(index));
  else return fd_type_error("index","ixserver_get",FD_VOID);
}
static fdtype ixserver_sizes(fdtype index)
{
  if (FD_INDEXP(index))
    return fd_index_keys(fd_lisp2index(index));
  else return fd_type_error("index","ixserver_get",FD_VOID);
}
static fdtype ixserver_writablep(fdtype index)
{
  return FD_FALSE;
}

/* Configuration methods */

static int serve_pool(fdtype var,fdtype val,void *data)
{
  fd_pool p;
  if (FD_CHOICEP(val)) {
    FD_DO_CHOICES(v,val) {
      int retval=serve_pool(var,v,data);
      if (retval<0) return retval;}
    return 1;}
  else if (FD_POOLP(val)) p=fd_lisp2pool(val);
  else if (FD_STRINGP(val)) {
    u8_string spec=FD_STRDATA(val);
    if ((p=fd_name2pool(FD_STRDATA(val)))==NULL)
      p=fd_use_pool(FD_STRDATA(val));}
  else return fd_reterr(fd_NotAPool,"serve_pool",NULL,val);
  if (p)
    if (served_poolp(p)) return 0;
    else if (n_served_pools>=FDBSERV_MAX_POOLS) {
      fd_seterr(_("too many pools to serve"),"serve_pool",NULL,val);
      return -1;}
    else {
      u8_notify("SERVE_POOL","Serving objects from %s",p->cid);
      served_pools[n_served_pools++]=p;
      return n_served_pools;}
  else return fd_reterr(fd_NotAPool,"serve_pool",NULL,val);
}

static fdtype get_served_pools(fdtype var,void *data)
{
  fdtype result=FD_EMPTY_CHOICE;
  int i=0; while (i<n_served_pools) {
    fd_pool p=served_pools[i++];
    fdtype lp=fd_pool2lisp(p);
    FD_ADD_TO_CHOICE(result,lp);}
  return result;
}

static int serve_primary_pool(fdtype var,fdtype val,void *data)
{
  fd_pool p;
  if (FD_POOLP(val)) p=fd_lisp2pool(val);
  else if (FD_STRINGP(val)) {
    u8_string spec=FD_STRDATA(val);
    if ((p=fd_name2pool(FD_STRDATA(val)))==NULL)
      p=fd_use_pool(FD_STRDATA(val));}
  else return fd_reterr(fd_NotAPool,"serve_pool",NULL,val);
  if (p)
    if (p==primary_pool) return 0;
    else {primary_pool=p; return 1;}
  else return fd_reterr(fd_NotAPool,"serve_pool",NULL,val);
}

static fdtype get_primary_pool(fdtype var,void *data)
{
  if (primary_pool) return fd_pool2lisp(primary_pool);
  else return FD_EMPTY_CHOICE;
}

static int serve_index(fdtype var,fdtype val,void *data)
{
  fd_index ix=NULL;
  if (FD_CHOICEP(val)) {
    FD_DO_CHOICES(v,val) {
      int retval=serve_index(var,v,data);
      if (retval<0) return retval;}
    return 1;}
  else if (FD_INDEXP(val)) ix=fd_lisp2index(val);
  else if (FD_STRINGP(val)) 
    ix=fd_open_index(FD_STRDATA(val));
  else {}
  if (ix) {
    u8_notify("SERVE_INDEX","Serving index %s",ix->cid);
    fd_add_to_compound_index(primary_index,ix);
    return 1;}
  else return fd_reterr(fd_BadIndexSpec,"serve_index",NULL,val);
}

static fdtype get_served_indices(fdtype var,void *data)
{
  return fd_index2lisp((fd_index)(primary_index));
}

/* Initialization */

fdtype fd_fdbserv_module;

static int dbserv_init=0;

void fd_init_dbserv_c()
{
  fdtype module;

  if (dbserv_init) return; else dbserv_init=1;

  module=fd_make_hashtable(NULL,67,NULL);

  fd_defn(module,fd_make_cprim1("POOL-DATA",server_pool_data,0));
  fd_defn(module,fd_make_cprim1("OID-VALUE",server_oid_value,1));
  fd_defn(module,fd_make_cprim1("FETCH-OIDS",server_fetch_oids,1));
  fd_defn(module,fd_make_cprim1("GET-LOAD",server_get_load,0));

  fd_defn(module,fd_make_cprim1("ISERVER-GET",iserver_get,1));
  fd_defn(module,fd_make_cprim1x("ISERVER-BULK-GET",iserver_bulk_get,1,
				 fd_vector_type,FD_VOID));
  fd_defn(module,fd_make_cprim1("ISERVER-GET-SIZE",iserver_get_size,1));
  fd_defn(module,fd_make_cprim0("ISERVER-KEYS",iserver_keys,0));
  fd_defn(module,fd_make_cprim0("ISERVER-SIZES",iserver_sizes,0));
  fd_defn(module,fd_make_cprim0("ISERVER-WRITABLE?",iserver_writablep,0));

  fd_defn(module,fd_make_cprim2x("IXSERVER-GET",ixserver_get,2,
				 fd_index_type,FD_VOID,-1,FD_VOID));
  fd_defn(module,fd_make_cprim2x("IXSERVER-BULK-GET",ixserver_bulk_get,2,
				 fd_index_type,FD_VOID,
				 fd_vector_type,FD_VOID));
  fd_defn(module,fd_make_cprim2x("IXSERVER-GET-SIZE",ixserver_get_size,2,
				 fd_index_type,FD_VOID,-1,FD_VOID));
  fd_defn(module,fd_make_cprim1x("IXSERVER-KEYS",ixserver_keys,1,
				 fd_index_type,FD_VOID));
  fd_defn(module,fd_make_cprim1x("IXSERVER-SIZES",ixserver_sizes,1,
				 fd_index_type,FD_VOID));
  fd_defn(module,fd_make_cprim1x("IXSERVER-WRITABLE?",ixserver_writablep,1,
				 fd_index_type,FD_VOID));

  fd_register_config("SERVEPOOLS",
		     get_served_pools,
		     serve_pool,
		     NULL);
  fd_register_config("PRIMARYPOOL",
		     get_primary_pool,
		     serve_primary_pool,
		     NULL);
  fd_register_config("SERVEINDICES",
		     get_served_indices,
		     serve_index,
		     NULL);
  primary_index=(fd_compound_index)fd_make_compound_index(0,NULL);

  fd_fdbserv_module=module;
}

void fd_init_fddbserv()
{
  fd_init_dbserv_c();
}
