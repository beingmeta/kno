/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* mongodb.c
   This implements FramerD bindings to mongodb.
   Copyright (C) 2007-2016 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/eval.h"
#include "framerd/sequences.h"
#include "framerd/texttools.h"
#include "framerd/bigints.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>

#include "framerd/fdregex.h"
#include "framerd/mongodb.h"

#include <mongoc-ssl.h>

/* Initialization */

u8_condition fd_MongoDB_Error=_("MongoDB error");
u8_condition fd_BSON_Error=_("BSON conversion error");
u8_condition fd_MongoDB_Warning=_("MongoDB warning");
u8_condition fd_BSON_Input_Error=_("BSON input error");

static fdtype sslsym;
static int default_ssl=0;

fd_ptr_type fd_mongoc_server, fd_mongoc_collection, fd_mongoc_cursor;

static bool bson_append_keyval(struct FD_BSON_OUTPUT,fdtype,fdtype);
static bool bson_append_dtype(struct FD_BSON_OUTPUT,const char *,int,fdtype);
static fdtype idsym, maxkey, minkey;
static fdtype oidtag, mongofun, mongouser, mongomd5;
static fdtype bsonflags, raw, slotify, slotifyin, slotifyout;
static fdtype colonize, colonizein, colonizeout, choices, nochoices;
static fdtype skipsym, limitsym, batchsym, sortsym;
static fdtype fieldssym, upsertsym, newsym, removesym;
static fdtype max_clients_symbol, max_ready_symbol;
static fdtype mongomap_symbol, mongovec_symbol;

static void grab_mongodb_error(bson_error_t *error,u8_string caller)
{
  u8_condition cond=u8_strerror(error->code);
  u8_seterr(cond,caller,u8_strdup(error->message));
}

static int boolopt(fdtype opts,fdtype key)
{
  fdtype v=fd_get(opts,key,FD_VOID);
  if ((FD_VOIDP(v))||(FD_FALSEP(v)))
    return 0;
  else {
    fd_decref(v);
    return 1;}
}

static u8_string stropt(fdtype opts,fdtype key,u8_string dflt)
{
  fdtype v=fd_getopt(opts,key,FD_VOID);
  if ((FD_VOIDP(v))||(FD_FALSEP(v))) {
    if (dflt==NULL) return dflt;
    else return u8_strdup(dflt);}
  else if (FD_STRINGP(v)) 
    return u8_strdup(FD_STRDATA(v));
  else if (FD_PRIM_TYPEP(v,fd_secret_type))
    return u8_strdup(FD_STRDATA(v));
  else {
    u8_log(LOG_WARN,"Invalid string option","%q=%q",key,v);
    fd_decref(v);
    return NULL;}
}

/* Handling options and flags */

int mongodb_defaults=FD_MONGODB_DEFAULTS;

static int getflags(fdtype opts,int dflt)
{
  if ((FD_VOIDP(opts)||(FD_FALSEP(opts))||(FD_DEFAULTP(opts))))
    if (dflt<0) return mongodb_defaults;
    else return dflt;
  else if (FD_FIXNUMP(opts)) return FD_FIX2INT(opts);
  else if ((FD_CHOICEP(opts))||(FD_SYMBOLP(opts))) {
    int flags=FD_MONGODB_DEFAULTS;
    if (fd_overlapp(opts,raw)) flags=0;
    if (fd_overlapp(opts,slotify)) flags|=FD_MONGODB_SLOTIFY;
    if (fd_overlapp(opts,slotifyin)) flags|=FD_MONGODB_SLOTIFY_IN;
    if (fd_overlapp(opts,slotifyout)) flags|=FD_MONGODB_SLOTIFY_OUT;
    if (fd_overlapp(opts,colonize)) flags|=FD_MONGODB_COLONIZE;
    if (fd_overlapp(opts,colonizein)) flags|=FD_MONGODB_COLONIZE_IN;
    if (fd_overlapp(opts,colonizeout)) flags|=FD_MONGODB_COLONIZE_OUT;
    if (fd_overlapp(opts,choices)) flags|=FD_MONGODB_CHOICEVALS;
    if (fd_overlapp(opts,nochoices)) flags&=(~FD_MONGODB_CHOICEVALS);
    return flags;}
  else if (FD_TABLEP(opts)) {
    fdtype flagsv=fd_getopt(opts,bsonflags,FD_VOID);
    if (FD_VOIDP(flagsv)) {
      if (dflt<0) return FD_MONGODB_DEFAULTS;
      else return dflt;}
    else {
      int flags=getflags(flagsv,dflt);
      fd_decref(flagsv);
      return flags;}}
  else if (dflt<0) return FD_MONGODB_DEFAULTS;
  else return dflt;
}

static fdtype combine_opts(fdtype opts,fdtype clopts)
{
  if (opts==clopts) {
    fd_incref(opts); return opts;}
  else if (FD_PAIRP(opts)) {
    fd_incref(opts); return opts;}
  else if ((FD_TABLEP(opts))&&(FD_TABLEP(clopts))) {
    return fd_make_pair(opts,clopts);}
  else if (FD_VOIDP(opts)) {
    fd_incref(clopts); return clopts;}
  else {
    fd_incref(opts);
    return opts;}
}

/* Consing MongoDB clients, collections, and cursors */

static u8_string get_connection_spec(mongoc_uri_t *info);
static int setup_ssl(mongoc_ssl_opt_t *,mongoc_uri_t *,fdtype);
static u8_string modify_ssl(u8_string uri,int add);

/* This returns a MongoDB server object which wraps a MongoDB client
   pool. */
static fdtype mongodb_open(fdtype arg,fdtype opts)
{
  mongoc_client_pool_t *client_pool; int flags;
  mongoc_uri_t *info; u8_string uri;
  mongoc_ssl_opt_t ssl_opts={ 0 };
  int add_ssl=(fd_testopt(opts,sslsym,FD_VOID))?(boolopt(opts,sslsym)):(default_ssl);
  if ((FD_STRINGP(arg))||(FD_PRIM_TYPEP(arg,fd_secret_type))) {
    uri=modify_ssl(FD_STRDATA(arg),add_ssl);
    info=mongoc_uri_new(uri);}
  else if (FD_SYMBOLP(arg)) {
    fdtype conf_val=fd_config_get(FD_SYMBOL_NAME(arg));
    if (FD_VOIDP(conf_val))
      return fd_type_error("MongoDB URI config","mongodb_open",arg);
    else if ((FD_STRINGP(conf_val))||
             (FD_PRIM_TYPEP(conf_val,fd_secret_type))) {
      uri=modify_ssl(FD_STRDATA(conf_val),add_ssl);
      info=mongoc_uri_new(FD_STRDATA(conf_val));
      fd_decref(conf_val);}
    else return fd_type_error("MongoDB URI config val",
                              FD_SYMBOL_NAME(arg),conf_val);}
  else return fd_type_error("MongoDB URI","mongodb_open",arg);
  if (!(info))
    return fd_type_error("MongoDB client URI","mongodb_open",arg);
  flags=getflags(opts,mongodb_defaults);
  client_pool=mongoc_client_pool_new(info);
  if (client_pool) {
    struct FD_MONGODB_DATABASE *srv=u8_alloc(struct FD_MONGODB_DATABASE);
    u8_string dbname=mongoc_uri_get_database(info);
    if ((mongoc_uri_get_ssl(info))&& 
        (setup_ssl(&ssl_opts,info,opts)))
      mongoc_client_pool_set_ssl_opts(client_pool,&ssl_opts);
    FD_INIT_CONS(srv,fd_mongoc_server);
    srv->uri=uri; 
    if (dbname==NULL) 
      srv->dbname=NULL;
    else srv->dbname=u8_strdup(dbname);
    srv->spec=get_connection_spec(info);
    srv->info=info; srv->pool=client_pool;
    srv->opts=opts; fd_incref(opts);
    srv->flags=flags;
    return (fdtype)srv;}
  else {
    mongoc_uri_destroy(info); fd_decref(opts); u8_free(uri);
    return fd_type_error("MongoDB client URI","mongodb_open",arg);}
}
static void recycle_client(struct FD_CONS *c)
{
  struct FD_MONGODB_DATABASE *s=(struct FD_MONGODB_DATABASE *)c;
  mongoc_uri_destroy(s->info);
  mongoc_client_pool_destroy(s->pool);
  fd_decref(s->opts);
  u8_free(c);
}
static int unparse_server(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_MONGODB_DATABASE *srv=(struct FD_MONGODB_DATABASE *)x;
  u8_printf(out,"#<MongoDB/Server %s/%s>",srv->spec,srv->dbname);
  return 1;
}

static u8_string get_connection_spec(mongoc_uri_t *info)
{
  struct U8_OUTPUT out; 
  const mongoc_host_list_t *hosts=mongoc_uri_get_hosts(info);
  U8_INIT_OUTPUT(&out,256);
  u8_printf(&out,"%s@%s",mongoc_uri_get_username(info),hosts->host_and_port);
  return out.u8_outbuf;
}

static u8_string modify_ssl(u8_string uri,int add)
{
  if ((add)&&((strstr(uri,"?ssl=")==NULL)&&(strstr(uri,"&ssl=")==NULL))) {
    if (strchr(uri,'?'))
      return u8_string_append(uri,"&ssl=true",NULL);
    else return u8_string_append(uri,"?ssl=true",NULL);}
  else return u8_strdup(uri);
  
}

static fdtype pemsym, pempwd, cafilesym, cadirsym, crlsym;

static int setup_ssl(mongoc_ssl_opt_t *ssl_opts,
                     mongoc_uri_t *info,
                     fdtype opts)
{
  const mongoc_ssl_opt_t *default_opts=mongoc_ssl_opt_get_default();
  memcpy(ssl_opts,default_opts,sizeof(mongoc_ssl_opt_t));
  ssl_opts->pem_file=stropt(opts,pemsym,NULL);
  ssl_opts->pem_pwd=stropt(opts,pempwd,NULL);
  ssl_opts->ca_file=stropt(opts,cafilesym,NULL);
  ssl_opts->ca_dir=stropt(opts,cadirsym,NULL);
  ssl_opts->crl_file=stropt(opts,crlsym,NULL);
  return (!((ssl_opts->pem_file==NULL)&&
            (ssl_opts->pem_pwd==NULL)&&
            (ssl_opts->ca_file==NULL)&&
            (ssl_opts->ca_dir==NULL)&&
            (ssl_opts->crl_file==NULL)));
}

/* Creating collections */

/* Collection creation is actually deferred until the collection is
   used because we want the collection to be "thread safe" which means
   that it won't correspond to a single mongoc_collection_t object,
   but that each use of the collection will pop a client from the pool
   and create a collection with that client. */
static fdtype mongodb_collection(fdtype server,fdtype name_arg,fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *result;
  struct FD_MONGODB_DATABASE *srv;
  u8_string name=FD_STRDATA(name_arg), db_name=NULL, collection_name=NULL; 
  fdtype opts; int flags;
  if (FD_PRIM_TYPEP(server,fd_mongoc_server)) {
    srv=(struct FD_MONGODB_DATABASE *)server;
    flags=getflags(opts_arg,srv->flags);
    opts=combine_opts(opts_arg,srv->opts);
    fd_incref(server);}
  else if ((FD_STRINGP(server))||
           (FD_SYMBOLP(server))||
           (FD_PRIM_TYPEP(server,fd_secret_type))) {
    fdtype consed=mongodb_open(server,opts_arg);
    if (FD_ABORTP(consed)) return consed;
    server=consed; srv=(struct FD_MONGODB_DATABASE *)consed;
    flags=srv->flags; 
    opts=combine_opts(opts_arg,srv->opts);}
  else return fd_type_error("MongoDB client","mongodb_collection",server);
  if (strchr(name,'/')) {
    char *slash=strchr(name,'/');
    collection_name=slash+1;
    db_name=u8_slice(name,slash);}
  else if (srv->dbname==NULL) {
    return fd_err(_("MissingDBName"),"mongodb_open",NULL,server);}
  else {
    db_name=u8_strdup(srv->dbname);
    collection_name=u8_strdup(name);}
  result=u8_alloc(struct FD_MONGODB_COLLECTION);
  FD_INIT_CONS(result,fd_mongoc_collection);
  result->server=server;
  result->uri=u8_strdup(srv->uri);
  result->server_spec=u8_strdup(srv->spec);
  result->dbname=db_name; result->name=collection_name;
  result->opts=opts; result->flags=flags;
  return (fdtype) result;
}
static void recycle_collection(struct FD_CONS *c)
{
  struct FD_MONGODB_COLLECTION *collection=(struct FD_MONGODB_COLLECTION *)c;
  u8_free(collection->dbname); u8_free(collection->name);
  fd_decref(collection->server); fd_decref(collection->opts);
  u8_free(collection);
}
static int unparse_collection(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_MONGODB_COLLECTION *cl=(struct FD_MONGODB_COLLECTION *)x;
  u8_printf(out,"#<MongoDB/Collection %s/%s/%s>",
            cl->server_spec,cl->dbname,cl->name);
  return 1;
}

/* Using collections */

/*  This is where the collection actually gets created based on
    a client popped from the client pool for the server. */
mongoc_collection_t *open_collection(struct FD_MONGODB_COLLECTION *domain,
                                     mongoc_client_t **clientp,
                                     int flags)
{
  struct FD_MONGODB_DATABASE *server=
    (struct FD_MONGODB_DATABASE *)(domain->server);
  mongoc_client_t *client;
  if (flags&FD_MONGODB_NOBLOCK)
    client=mongoc_client_pool_try_pop(server->pool);
  else client=mongoc_client_pool_pop(server->pool);
  if (client) {
    mongoc_collection_t *collection=
      mongoc_client_get_collection(client,domain->dbname,domain->name);
    if (collection) {
      *clientp=client;
      return collection;}
    else {
      mongoc_client_pool_push(server->pool,client);
      return NULL;}}
  else return NULL;
}

/*  This combines using the collection with using an opts arg to
    generate flags and combined opts to use.  */
mongoc_collection_t *use_collection(struct FD_MONGODB_COLLECTION *domain,
                                    fdtype opts_arg,
                                    mongoc_client_t **clientp,
                                    fdtype *optsp,int *flagsp)
{
  struct FD_MONGODB_DATABASE *srv=
    (struct FD_MONGODB_DATABASE *)(domain->server);
  int flags=getflags(opts_arg,domain->flags);
  mongoc_collection_t *collection=open_collection(domain,clientp,flags);
  if (collection) {
    if (flagsp) *flagsp=flags;
    if (optsp) *optsp=combine_opts(opts_arg,domain->opts);
    return collection;}
  else return NULL;
}

/* This returns a client to a client pool.  The argument can be either
   a server or a collection (which is followed to its server). */
static void client_done(fdtype arg,mongoc_client_t *client)
{
  if (FD_PRIM_TYPEP(arg,fd_mongoc_server)) {
    struct FD_MONGODB_DATABASE *server=(struct FD_MONGODB_DATABASE *)arg;
    mongoc_client_pool_push(server->pool,client);}
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_collection)) {
    struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
    struct FD_MONGODB_DATABASE *server=
      (struct FD_MONGODB_DATABASE *)(domain->server);
    mongoc_client_pool_push(server->pool,client);}
  else {
    u8_log(LOG_WARN,"BAD client_done call","Wrong type for %q",arg);}
}

/* This destroys the collection returns a client to a client pool. */
static void collection_done(mongoc_collection_t *collection,
                            mongoc_client_t *client,
                            struct FD_MONGODB_COLLECTION *domain)
{
  struct FD_MONGODB_DATABASE *server=(fd_mongodb_database)domain->server;
  mongoc_collection_destroy(collection);
  mongoc_client_pool_push(server->pool,client);
}

/* Basic operations on collections */

static fdtype mongodb_insert(fdtype arg,fdtype obj,fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  int flags=getflags(opts_arg,domain->flags);
  mongoc_client_t *client=NULL; bool retval;
  mongoc_collection_t *collection=open_collection(domain,&client,flags);
  if (collection) {
    fdtype opts=combine_opts(opts_arg,domain->opts);
    bson_t reply; bson_error_t error;
    if (FD_CHOICEP(obj)) {
      fdtype result;
      mongoc_bulk_operation_t *bulk=
        mongoc_collection_create_bulk_operation(collection,true,NULL);
      FD_DO_CHOICES(elt,obj) {
        bson_t *doc=fd_dtype2bson(elt,flags,opts);
        mongoc_bulk_operation_insert(bulk,doc);
        bson_destroy(doc);}
      retval=mongoc_bulk_operation_execute(bulk,&reply,&error);
      mongoc_bulk_operation_destroy(bulk);
      result=fd_bson2dtype(&reply,flags,opts);
      bson_destroy(&reply);
      return result;}
    else {
      bson_t *doc=fd_dtype2bson(obj,flags,opts);
      if ((doc)&&
          (mongoc_collection_insert
           (collection,MONGOC_INSERT_NONE,doc,NULL,&error))) {
        collection_done(collection,client,domain);
      bson_destroy(doc); fd_decref(opts);
      return FD_TRUE;}
    collection_done(collection,client,domain);
    if (doc) bson_destroy(doc);
    fd_incref(obj); fd_decref(opts);
    fd_seterr(fd_MongoDB_Error,"mongodb_insert",
              u8_mkstring("%s>%s>%s:%s",
                          domain->uri,domain->dbname,domain->name,
                          error.message),
              obj);
    return FD_ERROR_VALUE;}}
  else return FD_ERROR_VALUE;
}

static fdtype mongodb_remove(fdtype arg,fdtype obj,fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  int flags=getflags(opts_arg,domain->flags);
  mongoc_client_t *client=NULL;
  mongoc_collection_t *collection=open_collection(domain,&client,flags);
  if (collection) {
    struct FD_BSON_OUTPUT q; bson_error_t error;
    fdtype opts=combine_opts(opts_arg,domain->opts);
    q.doc=bson_new(); q.opts=opts; q.flags=flags;
    if (FD_TABLEP(obj)) {
      fdtype id=fd_get(obj,idsym,FD_VOID);
      if (FD_VOIDP(id))
        return fd_err("No MongoDB _id","mongodb_remove",NULL,obj);
      bson_append_dtype(q,"_id",3,id);
      fd_decref(id);}
    else bson_append_dtype(q,"_id",3,obj);
    if (mongoc_collection_remove
        (collection,MONGOC_REMOVE_SINGLE_REMOVE,q.doc,NULL,&error)) {
      collection_done(collection,client,domain);
      bson_destroy(q.doc); fd_decref(opts);
      return FD_TRUE;}
    else {
      collection_done(collection,client,domain);
      bson_destroy(q.doc); fd_incref(obj); fd_decref(opts);
      fd_seterr(fd_MongoDB_Error,"mongodb_remove",
                u8_mkstring("%s>%s>%s:%s",
                            domain->uri,domain->dbname,domain->name,
                            error.message),
                obj);
      return FD_ERROR_VALUE;}}
  else return FD_ERROR_VALUE;
}

static fdtype mongodb_update(fdtype arg,fdtype query,fdtype update,
                             fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  int flags=getflags(opts_arg,domain->flags);
  mongoc_client_t *client=NULL;
  mongoc_collection_t *collection=open_collection(domain,&client,flags);
  if (collection) {
    fdtype opts=combine_opts(opts_arg,domain->opts);
    bson_t *q=fd_dtype2bson(query,flags,opts);
    bson_t *u=fd_dtype2bson(update,flags,opts);
    bson_error_t error; int success=0;
    if ((q)&&(u)&&
        (mongoc_collection_update
         (collection,MONGOC_UPDATE_NONE,q,u,NULL,&error)))
      success=1;
    collection_done(collection,client,domain);
    bson_destroy(q); bson_destroy(u); fd_decref(opts);
    if (success) return FD_TRUE;
    else if ((q)&&(u))
      fd_seterr(fd_MongoDB_Error,"mongodb_update",
                u8_mkstring("%s>%s>%s:%s",
                            domain->uri,domain->dbname,domain->name,
                            error.message),
                fd_make_pair(query,update));
    else fd_seterr(fd_BSON_Error,"mongodb_update",
                   u8_mkstring("%s>%s>%s:%s",
                               domain->uri,domain->dbname,domain->name),
                   fd_make_pair(query,update));
    return FD_ERROR_VALUE;}
  else return FD_ERROR_VALUE;
}

static fdtype mongodb_find(fdtype arg,fdtype query,fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  int flags=getflags(opts_arg,domain->flags);
  mongoc_client_t *client=NULL;
  mongoc_collection_t *collection=open_collection(domain,&client,flags);
  if (collection) {
    fdtype results=FD_EMPTY_CHOICE;
    fdtype opts=combine_opts(opts_arg,domain->opts);
    mongoc_cursor_t *cursor; bson_error_t error;
    const bson_t *doc;
    bson_t *q=fd_dtype2bson(query,flags,opts);
    if (q) cursor=mongoc_collection_find
             (collection,MONGOC_QUERY_NONE,0,0,0,q,NULL,NULL);
    if (cursor) {
      while (mongoc_cursor_next(cursor,&doc)) {
        /* u8_string json=bson_as_json(doc,NULL); */
        fdtype r=fd_bson2dtype((bson_t *)doc,flags,opts);
        FD_ADD_TO_CHOICE(results,r);}
      mongoc_cursor_destroy(cursor);}
    bson_destroy(q); fd_decref(opts);
    collection_done(collection,client,domain);
    return results;}
  else return FD_ERROR_VALUE;
}

/* Command execution */

static fdtype make_mongovec(fdtype vec);

static fdtype make_command(int n,fdtype *values)
{
  if ((n%2)==1)
    return fd_err(fd_SyntaxError,"mongomap_lexpr",
                  "Odd number of arguments",FD_VOID);
  else {
    int i=0, fix_vectors=0; while (i<n) { 
      fdtype value=values[i++];
      if (FD_VECTORP(value)) fix_vectors=1;
      fd_incref(value);}
    if (fix_vectors) {
      fdtype result=fd_init_compound_from_elts
        (NULL,mongomap_symbol,0,n,values);
      fdtype *elts=FD_COMPOUND_ELTS(result);
      int j=0, n_elts=FD_COMPOUND_LENGTH(result);
      while (j<n_elts) {
        fdtype elt=elts[j];
        if (FD_VECTORP(elt)) {
          elts[j++]=make_mongovec(elt);
          fd_decref(elt);}
        else j++;}
      return result;}
    else return fd_init_compound_from_elts(NULL,mongomap_symbol,0,n,values);}
}

static fdtype collection_command(fdtype arg,fdtype command,
                                 fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  int flags=getflags(opts_arg,domain->flags);
  fdtype opts=combine_opts(opts_arg,domain->opts);
  fdtype fields=fd_get(opts,fieldssym,FD_VOID);
  mongoc_client_t *client;
  mongoc_collection_t *collection=open_collection(domain,&client,flags);
  if (collection) {
    fdtype results=FD_EMPTY_CHOICE;
    bson_t *cmd=fd_dtype2bson(command,flags,opts);
    bson_t *flds=fd_dtype2bson(fields,flags,opts);    
    if (cmd) {
      const bson_t *doc; bson_error_t error;
      fdtype skip_arg=fd_getopt(opts,skipsym,FD_FIXZERO);
      fdtype limit_arg=fd_getopt(opts,limitsym,FD_FIXZERO);
      fdtype batch_arg=fd_getopt(opts,batchsym,FD_FIXZERO);
      mongoc_cursor_t *cursor=mongoc_collection_command
        (collection,MONGOC_QUERY_EXHAUST,
         ((FD_FIXNUMP(skip_arg))?(FD_FIX2INT(skip_arg)):(0)),
         ((FD_FIXNUMP(limit_arg))?(FD_FIX2INT(limit_arg)):(500)),
         ((FD_FIXNUMP(batch_arg))?(FD_FIX2INT(batch_arg)):(0)),
         cmd,flds,NULL);
      while (mongoc_cursor_next(cursor,&doc)) {
        fdtype r=fd_bson2dtype((bson_t *)doc,flags,opts);
        FD_ADD_TO_CHOICE(results,r);}
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      client_done(arg,client);
      bson_destroy(cmd); fd_decref(opts);
      return results;}
    else {
      return FD_ERROR_VALUE;}}
  else return FD_ERROR_VALUE;
}

static fdtype db_command(fdtype arg,fdtype command,
                         fdtype opts_arg)
{
  struct FD_MONGODB_DATABASE *srv=(struct FD_MONGODB_DATABASE *)arg;
  int flags=getflags(opts_arg,srv->flags);
  fdtype opts=combine_opts(opts_arg,srv->opts);
  fdtype fields=fd_getopt(opts,fieldssym,FD_VOID);
  mongoc_client_t *client=mongoc_client_pool_pop(srv->pool);
  if (client) {
    fdtype results=FD_EMPTY_CHOICE;
    bson_t *cmd=fd_dtype2bson(command,flags,opts);
    bson_t *flds=fd_dtype2bson(fields,flags,opts);    
    if (cmd) {
      const bson_t *doc; bson_error_t error;
      fdtype skip_arg=fd_getopt(opts,skipsym,FD_FIXZERO);
      fdtype limit_arg=fd_getopt(opts,limitsym,FD_FIXZERO);
      fdtype batch_arg=fd_getopt(opts,batchsym,FD_FIXZERO);
      mongoc_cursor_t *cursor=mongoc_client_command
        (client,srv->dbname,MONGOC_QUERY_EXHAUST,
         ((FD_FIXNUMP(skip_arg))?(FD_FIX2INT(skip_arg)):(0)),
         ((FD_FIXNUMP(limit_arg))?(FD_FIX2INT(limit_arg)):(500)),
         ((FD_FIXNUMP(batch_arg))?(FD_FIX2INT(batch_arg)):(0)),
         cmd,flds,NULL);
      while (mongoc_cursor_next(cursor,&doc)) {
        fdtype r=fd_bson2dtype((bson_t *)doc,flags,opts);
        FD_ADD_TO_CHOICE(results,r);}
      mongoc_cursor_destroy(cursor);
      client_done(arg,client);
      bson_destroy(cmd); fd_decref(opts);
      return results;}
    else {
      return FD_ERROR_VALUE;}}
  else return FD_ERROR_VALUE;
}

static fdtype mongodb_command(int n,fdtype *args)
{
  fdtype arg=args[0], opts=(n%2)?(FD_VOID):(args[1]);
  fdtype command=(n%2)?(make_command(n-1,args+1)):(make_command(n-2,args+2));
  fdtype result=FD_VOID;
  if (FD_PRIM_TYPEP(arg,fd_mongoc_server)) 
    result=db_command(arg,command,opts);
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_collection))
    result=collection_command(arg,command,opts);
  else result=fd_type_error(_("MongoDB"),"mongodb_command",arg);
  fd_decref(command);
  return result;
}

static fdtype collection_simple_command(fdtype arg,fdtype command,
                                        fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  int flags=getflags(opts_arg,domain->flags);
  fdtype opts=combine_opts(opts_arg,domain->opts);
  bson_t *cmd=fd_dtype2bson(command,flags,opts);
  if (cmd) {
    mongoc_client_t *client;
    mongoc_collection_t *collection=open_collection(domain,&client,flags);
    if (collection) {
      bson_t response; bson_error_t error;
      if (mongoc_collection_command_simple
          (collection,cmd,NULL,&response,&error)) {
        fdtype result=fd_bson2dtype(&response,flags,opts);
        collection_done(collection,client,domain);
        bson_destroy(cmd); fd_decref(opts);
        return result;}
      else {
        grab_mongodb_error(&error,"collection_simple_command"); 
        collection_done(collection,client,domain);
        bson_destroy(cmd); fd_decref(opts);
        return FD_ERROR_VALUE;}}
    else {
      bson_destroy(cmd); fd_decref(opts);
      return FD_ERROR_VALUE;}}
  else {
    fd_decref(opts);
    return FD_ERROR_VALUE;}
}

static fdtype db_simple_command(fdtype arg,fdtype command,
                                fdtype opts_arg)
{
  struct FD_MONGODB_DATABASE *srv=(struct FD_MONGODB_DATABASE *)arg;
  int flags=getflags(opts_arg,srv->flags);
  fdtype opts=combine_opts(opts_arg,srv->opts);
  mongoc_client_t *client=mongoc_client_pool_pop(srv->pool);
  if (client) {
    bson_t response; bson_error_t error;
    bson_t *cmd=fd_dtype2bson(command,flags,opts);
    if (cmd) {
      const bson_t *doc;
      if (mongoc_client_command_simple
          (client,srv->dbname,cmd,NULL,&response,&error)) {
        fdtype result=fd_bson2dtype(&response,flags,opts);
        client_done(arg,client);
        bson_destroy(cmd); fd_decref(opts);
        return result;}
      else {
        grab_mongodb_error(&error,"db_simple_command"); 
        client_done(arg,client);
        bson_destroy(cmd); fd_decref(opts);
        return FD_ERROR_VALUE;}}
    else {
      return FD_ERROR_VALUE;}}
  else return FD_ERROR_VALUE;
}

static fdtype mongodb_simple_command(int n,fdtype *args)
{
  fdtype arg=args[0], opts=(n%2)?(FD_VOID):(args[1]);
  fdtype command=(n%2)?(make_command(n-1,args+1)):(make_command(n-2,args+2));
  fdtype result=FD_VOID;
  if (FD_PRIM_TYPEP(arg,fd_mongoc_server)) 
    result=db_simple_command(arg,command,opts);
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_collection))
    result=collection_simple_command(arg,command,opts);
  else result=fd_type_error(_("MongoDB"),"mongodb_command",arg);
  fd_decref(command);
  return result;
}

/* Find and Modify */

static fdtype mongodb_modify(fdtype arg,fdtype query,fdtype update,
                             fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  int flags=getflags(opts_arg,domain->flags);
  mongoc_client_t *client;
  mongoc_collection_t *collection=open_collection(domain,&client,flags);
  if (collection) {
    fdtype opts=combine_opts(opts_arg,domain->opts);
    fdtype sort=fd_getopt(opts,sortsym,FD_VOID);
    fdtype fields=fd_getopt(opts,fieldssym,FD_VOID);
    fdtype upsert=fd_getopt(opts,upsertsym,FD_FALSE);
    fdtype remove=fd_getopt(opts,removesym,FD_FALSE);
    fdtype donew=fd_getopt(opts,newsym,FD_FALSE);
    bson_t *q=fd_dtype2bson(query,flags,opts);
    bson_t *u=fd_dtype2bson(update,flags,opts);
    bson_t *reply=bson_new(); bson_error_t error;
    if ((q==NULL)||(u==NULL)) return FD_ERROR_VALUE;
    if (mongoc_collection_find_and_modify
        (collection,
         q,fd_dtype2bson(sort,flags,opts),
         u,fd_dtype2bson(fields,flags,opts),
         ((FD_FALSEP(remove))?(false):(true)),
         ((FD_FALSEP(upsert))?(false):(true)),
         ((FD_FALSEP(donew))?(false):(true)),
         reply,&error)) {
      fdtype v=fd_bson2dtype(reply,flags,opts);
      bson_destroy(q); bson_destroy(u);
      bson_destroy(reply);
      fd_decref(opts);
      return v;}
    else {
      bson_destroy(q); bson_destroy(u);
      bson_destroy(reply);
      fd_decref(opts);
      fd_seterr(fd_MongoDB_Error,"mongodb_modify",
                u8_mkstring("%s>%s>%s:%s",
                            domain->uri,domain->dbname,domain->name,
                            error.message),
                fd_make_pair(query,update));
      return FD_ERROR_VALUE;}}
  else return FD_ERROR_VALUE;
}

/* Cursor creation */

static fdtype mongodb_cursor(fdtype arg,fdtype query,fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  int flags=getflags(opts_arg,domain->flags); fdtype opts=FD_VOID;
  mongoc_client_t *connection;
  bson_t *bq=NULL; mongoc_cursor_t *cursor=NULL; bson_error_t error;
  mongoc_collection_t *collection=
    use_collection(domain,opts_arg,&connection,&opts,NULL);
  if (collection) bq=fd_dtype2bson(query,flags,opts);
  if (bq) cursor=mongoc_collection_find
           (collection,MONGOC_QUERY_NONE,0,0,0,bq,NULL,NULL);
  if (cursor) {
    struct FD_MONGODB_CURSOR *consed=u8_alloc(struct FD_MONGODB_CURSOR);
    FD_INIT_CONS(consed,fd_mongoc_cursor);
    consed->domain=arg; fd_incref(arg);
    consed->server=domain->server; fd_incref(domain->server);
    consed->query=query; fd_incref(query);
    consed->opts=combine_opts(opts_arg,domain->opts);
    consed->flags=flags;
    consed->bsonquery=bq;
    consed->connection=connection;
    consed->collection=collection;
    consed->cursor=cursor;
   return (fdtype) consed;}
  else {
    fd_decref(opts);
    if (bq) bson_destroy(bq);
    if (collection) collection_done(collection,connection,domain);
    return FD_ERROR_VALUE;}
}
static void recycle_cursor(struct FD_CONS *c)
{
  struct FD_MONGODB_CURSOR *cursor=(struct FD_MONGODB_CURSOR *)c;
  struct FD_MONGODB_DATABASE *s=(struct FD_MONGODB_DATABASE *)cursor->server;
  mongoc_cursor_destroy(cursor->cursor);
  mongoc_collection_destroy(cursor->collection);
  mongoc_client_pool_push(s->pool,cursor->connection);
  fd_decref(cursor->domain);
  fd_decref(cursor->query);
  fd_decref(cursor->opts);
  bson_destroy(cursor->bsonquery);
  u8_free(cursor);
}
static int unparse_cursor(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_MONGODB_CURSOR *cursor=(struct FD_MONGODB_CURSOR *)x;
  struct FD_MONGODB_COLLECTION *domain=
    (struct FD_MONGODB_COLLECTION *)(cursor->domain);
  u8_printf(out,"#<MongoDB/Cursor '%s/%s' %q>",
            domain->dbname,domain->name,cursor->query);
  return 1;
}

/* Operations on cursors */

static fdtype mongodb_donep(fdtype cursor)
{
  struct FD_MONGODB_CURSOR *c=(struct FD_MONGODB_CURSOR *)cursor;
  if (mongoc_cursor_more(c->cursor))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype mongodb_skip(fdtype cursor,fdtype howmany)
{
  struct FD_MONGODB_CURSOR *c=(struct FD_MONGODB_CURSOR *)cursor;
  int n=FD_FIX2INT(howmany), i=0; const bson_t *doc;
  while ((i<n)&&(mongoc_cursor_more(c->cursor))) {
    mongoc_cursor_next(c->cursor,&doc); i++;}
  if (i==n) return FD_TRUE; else return FD_FALSE;
}

static fdtype mongodb_read(fdtype cursor,fdtype howmany,fdtype opts_arg)
{
  struct FD_MONGODB_CURSOR *c=(struct FD_MONGODB_CURSOR *)cursor;
  int n=FD_FIX2INT(howmany), i=0;
  fdtype opts=c->opts;
  if (n==0) return FD_EMPTY_CHOICE;
  else {
    fdtype results=FD_EMPTY_CHOICE;
    mongoc_cursor_t *scan=c->cursor; const bson_t *doc;
    int flags=c->flags; fdtype opts=c->opts;
    if (!(FD_VOIDP(opts_arg))) {
      flags=getflags(opts_arg,c->flags);
      opts=combine_opts(opts_arg,opts);}
    while ((i<n)&&(mongoc_cursor_next(scan,&doc))) {
      fdtype dtype=fd_bson2dtype((bson_t *)doc,flags,opts);
      FD_ADD_TO_CHOICE(results,dtype); i++;}
    if (!(FD_VOIDP(opts_arg))) fd_decref(opts);
    return results;}
}

static fdtype mongodb_readvec(fdtype cursor,fdtype howmany,fdtype opts_arg)
{
  struct FD_MONGODB_CURSOR *c=(struct FD_MONGODB_CURSOR *)cursor;
  int n=FD_FIX2INT(howmany), i=0;
  fdtype opts=c->opts;
  if (n==0) return fd_make_vector(0,NULL);
  else {
    fdtype result=fd_make_vector(n,NULL);
    mongoc_cursor_t *scan=c->cursor; const bson_t *doc;
    int flags=c->flags; fdtype opts=c->opts;
    if (!(FD_VOIDP(opts_arg))) {
      flags=getflags(opts_arg,c->flags);
      opts=combine_opts(opts_arg,opts);}
    while ((i<n)&&(mongoc_cursor_next(scan,&doc))) {
      fdtype dtype=fd_bson2dtype((bson_t *)doc,flags,opts);
      FD_VECTOR_SET(result,i,dtype); i++;}
    if (!(FD_VOIDP(opts_arg))) fd_decref(opts);
    if (i==n) return result;
    else {
      fdtype truncated=fd_make_vector(i,FD_VECTOR_DATA(result));
      u8_free((struct FD_CONS *)result);
      return truncated;}}
}

/* BSON output functions */

static bool bson_append_dtype(struct FD_BSON_OUTPUT b,
                              const char *key,int keylen,
                              fdtype val)
{
  bson_t *out=b.doc; int flags=b.flags; bool ok=true;
  if (FD_CONSP(val)) {
    fd_ptr_type ctype=FD_PTR_TYPE(val);
    switch (ctype) {
    case fd_string_type: {
      unsigned char _buf[64], *buf=_buf;
      u8_string str=FD_STRDATA(val); int len=FD_STRLEN(val);
      if ((flags&FD_MONGODB_COLONIZE)&&
          ((isdigit(str[0]))||(strchr(":(#@",str[0])!=NULL))) {
        if (len>62) buf=u8_malloc(len+2);
        buf[0]='\\'; strncpy(buf+1,str,len); buf[len+1]='\0';
        ok=bson_append_utf8(out,key,keylen,buf,len+1);
        if (buf!=_buf) u8_free(buf);}
      else ok=bson_append_utf8(out,key,keylen,str,len);
      break;}
    case fd_packet_type:
      ok=bson_append_binary(out,key,keylen,BSON_SUBTYPE_BINARY,
                            FD_PACKET_DATA(val),FD_PACKET_LENGTH(val));
      break;
    case fd_double_type: {
      double d=FD_FLONUM(val);
      ok=bson_append_double(out,key,keylen,d);
      break;}
    case fd_bigint_type: {
      fd_bigint b=FD_GET_CONS(val,fd_bigint_type,fd_bigint);
      if (fd_bigint_fits_in_word_p(b,32,1)) {
        long int b32=fd_bigint_to_long(b);
        ok=bson_append_int32(out,key,keylen,b32);}
      else if (fd_bigint_fits_in_word_p(b,65,1)) {
        long long int b64=fd_bigint_to_long_long(b);
        ok=bson_append_int64(out,key,keylen,b64);}
      else {
        u8_log(LOG_WARN,fd_MongoDB_Warning,
               "Can't save bigint value %q",val);
        ok=bson_append_int32(out,key,keylen,0);}
      break;}
    case fd_timestamp_type: {
      struct FD_TIMESTAMP *fdt=
        FD_GET_CONS(val,fd_timestamp_type,struct FD_TIMESTAMP* );
      unsigned long long millis=(fdt->xtime.u8_tick*1000)+
        ((fdt->xtime.u8_prec>u8_second)?(fdt->xtime.u8_nsecs/1000000):(0));
      ok=bson_append_date_time(out,key,keylen,millis);
      break;}
    case fd_uuid_type: {
      struct FD_UUID *uuid=FD_GET_CONS(val,fd_uuid_type,struct FD_UUID *);
      ok=bson_append_binary(out,key,keylen,BSON_SUBTYPE_UUID,uuid->uuid,16);
      break;}
    case fd_choice_type: case fd_achoice_type: {
      struct FD_BSON_OUTPUT rout;
      bson_t arr; char buf[16]; int i=0;
      ok=bson_append_array_begin(out,key,keylen,&arr);
      memset(&rout,0,sizeof(struct FD_BSON_OUTPUT));
      rout.doc=&arr; rout.flags=b.flags; rout.opts=b.opts;
      if (ok) {
        int i=0; FD_DO_CHOICES(v,val) {
          sprintf(buf,"%d",i++);
          ok=bson_append_dtype(rout,buf,strlen(buf),v);
          if (!(ok)) FD_STOP_DO_CHOICES;}}
      bson_append_document_end(out,&arr);
      break;}
    case fd_vector_type: case fd_rail_type: {
      struct FD_BSON_OUTPUT rout;
      bson_t arr, ch; char buf[16];
      struct FD_VECTOR *vec=(struct FD_VECTOR *)val;
      int i=0, lim=vec->length;
      fdtype *data=vec->data;
      int wrap_vector=((flags&FD_MONGODB_CHOICEVALS)&&(key[0]!='$'));
      if (wrap_vector) {
        ok=bson_append_array_begin(out,key,keylen,&ch);
        if (ok) ok=bson_append_array_begin(&ch,"0",1,&arr);}
      else ok=bson_append_array_begin(out,key,keylen,&arr);
      memset(&rout,0,sizeof(struct FD_BSON_OUTPUT));
      rout.doc=&arr; rout.flags=b.flags; rout.opts=b.opts;
      if (ok) while (i<lim) {
          fdtype v=data[i]; sprintf(buf,"%d",i++);
          ok=bson_append_dtype(rout,buf,strlen(buf),v);
          if (!(ok)) break;}
      if (wrap_vector) {
        bson_append_array_end(&ch,&arr);
        bson_append_array_end(out,&ch);}
      else bson_append_array_end(out,&arr);
      break;}
    case fd_slotmap_type: case fd_hashtable_type: {
      struct FD_BSON_OUTPUT rout;
      bson_t doc; char buf[16];
      fdtype keys=fd_getkeys(val);
      ok=bson_append_document_begin(out,key,keylen,&doc);
      memset(&rout,0,sizeof(struct FD_BSON_OUTPUT));
      rout.doc=&doc; rout.flags=b.flags; rout.opts=b.opts;
      if (ok) {
        FD_DO_CHOICES(key,keys) {
          fdtype value=fd_get(val,key,FD_VOID);
          if (!(FD_VOIDP(value))) {
            ok=bson_append_keyval(rout,key,value);
            fd_decref(value);
            if (!(ok)) FD_STOP_DO_CHOICES;}}}
      fd_decref(keys);
      bson_append_document_end(out,&doc);
      break;}
    case fd_regex_type: {
      struct FD_REGEX *fdrx=(struct FD_REGEX *)val;
      char opts[8], *write=opts; int flags=fdrx->flags;
      if (flags&REG_EXTENDED) *write++='x';
      if (flags&REG_ICASE) *write++='i';
      if (flags&REG_NEWLINE) *write++='m';
      *write++='\0';
      bson_append_regex(out,key,keylen,fdrx->src,opts);
      break;}
    case fd_compound_type: {
      struct FD_COMPOUND *compound=FD_XCOMPOUND(val);
      fdtype tag=compound->tag, *elts=FD_COMPOUND_ELTS(val);
      int len=FD_COMPOUND_LENGTH(val);
      struct FD_BSON_OUTPUT rout;
      bson_t doc; char buf[16];
      if (tag==mongovec_symbol)
        ok=bson_append_array_begin(out,key,keylen,&doc);
      else ok=bson_append_document_begin(out,key,keylen,&doc);
      memset(&rout,0,sizeof(struct FD_BSON_OUTPUT));
      rout.doc=&doc; rout.flags=b.flags; rout.opts=b.opts;
      if (tag==mongomap_symbol) {
        fdtype *scan=elts, *limit=scan+len;
        if ((len%2)==1) ok=0;
        else while (scan<limit) {
          fdtype key=*scan++, value=*scan++;
          ok=bson_append_keyval(rout,key,value);
          if (!(ok)) break;}}
      else if (tag==mongovec_symbol) {
        fdtype *scan=elts, *limit=scan+len; int i=0;
        while (scan<limit) {
          u8_byte buf[16]; sprintf(buf,"%d",i);
          ok=bson_append_dtype(rout,buf,strlen(buf),*scan);
          scan++; i++;
          if (!(ok)) break;}}
      else {
        int i=0; 
        ok=bson_append_dtype(rout,"%fdtag",6,tag);
        if (ok) while (i<len) {
            char buf[16]; sprintf(buf,"%d",i);
            ok=bson_append_dtype(rout,buf,strlen(buf),elts[i++]);
            if (!(ok)) break;}}
      if (tag==mongovec_symbol)
        bson_append_array_end(out,&doc);
      else bson_append_document_end(out,&doc);
      break;}
    default: break;}
    return ok;}
  else if (FD_FIXNUMP(val))
    return bson_append_int32(out,key,keylen,FD_FIX2INT(val));
  else if (FD_OIDP(val)) {
    unsigned char bytes[12];
    FD_OID addr=FD_OID_ADDR(val); bson_oid_t oid;
    unsigned int hi=FD_OID_HI(addr), lo=FD_OID_LO(addr);
    bytes[0]=bytes[1]=bytes[2]=bytes[3]=0;
    bytes[4]=((hi>>24)&0xFF); bytes[5]=((hi>>16)&0xFF);
    bytes[6]=((hi>>8)&0xFF); bytes[7]=(hi&0xFF);
    bytes[8]=((lo>>24)&0xFF); bytes[9]=((lo>>16)&0xFF);
    bytes[10]=((lo>>8)&0xFF); bytes[11]=(lo&0xFF);
    bson_oid_init_from_data(&oid,bytes);
    return bson_append_oid(out,key,keylen,&oid);}
  else if (FD_SYMBOLP(val)) {
    if (flags&FD_MONGODB_COLONIZE_OUT) {
      u8_string pname=FD_SYMBOL_NAME(val);
      u8_byte _buf[512], *buf=_buf;
      size_t len=strlen(pname); 
      if (len>510) buf=u8_malloc(len+2);
      buf[0]=':'; strcpy(buf+1,pname);
      if (buf!=_buf) {
        bool rval=bson_append_utf8(out,key,keylen,buf,len+1);
        u8_free(buf);
        return rval;}
      else return bson_append_utf8(out,key,keylen,buf,len+1);}
    else return bson_append_utf8(out,key,keylen,FD_SYMBOL_NAME(val),-1);}
  else if (FD_CHARACTERP(val)) {
    int code=FD_CHARCODE(val);
    if (code<128) {
      char c=code;
      return bson_append_utf8(out,key,keylen,&c,1);}
    else {
      struct U8_OUTPUT vout; unsigned char buf[16];
      U8_INIT_OUTPUT_BUF(&vout,16,buf);
      u8_putc(&vout,code);
      return bson_append_utf8(out,key,keylen,vout.u8_outbuf,
                              vout.u8_outptr-vout.u8_outbuf);}}
  else switch (val) {
    case FD_TRUE: case FD_FALSE:
      return bson_append_bool(out,key,keylen,(val==FD_TRUE));
    default: {
      struct U8_OUTPUT vout; unsigned char buf[128]; bool rv;
      U8_INIT_OUTPUT_BUF(&vout,128,buf);
      if (flags&FD_MONGODB_COLONIZE)
        u8_printf(&vout,":%q",val);
      else fd_unparse(&vout,val);
      rv=bson_append_utf8(out,key,keylen,vout.u8_outbuf,
                          vout.u8_outptr-vout.u8_outbuf);
      u8_close((u8_stream)&vout);
      return rv;}}
}

static bool bson_append_keyval(FD_BSON_OUTPUT b,fdtype key,fdtype val)
{
  bson_t *out=b.doc; int flags=b.flags;
  struct U8_OUTPUT keyout; unsigned char buf[256];
  const char *keystring; int keylen; bool ok=true;
  U8_INIT_OUTPUT_BUF(&keyout,256,buf);
  if (FD_VOIDP(val)) return 0;
  if (FD_SYMBOLP(key)) {
    if (flags&FD_MONGODB_SLOTIFY) {
      u8_string pname=FD_SYMBOL_NAME(key);
      const u8_byte *scan=pname; int c;
      while ((c=u8_sgetc(&scan))>=0) {
        u8_putc(&keyout,u8_tolower(c));}
      keystring=keyout.u8_outbuf;
      keylen=keyout.u8_outptr-keyout.u8_outbuf;}
    else {
      keystring=FD_SYMBOL_NAME(key);
      keylen=strlen(keystring);}}
  else if (FD_STRINGP(key)) {
    keystring=FD_STRDATA(key);
    if ((flags&FD_MONGODB_SLOTIFY)&&
        ((isdigit(keystring[0]))||
         (strchr(":(#@",keystring[0])!=NULL))) {
      u8_putc(&keyout,'\\'); u8_puts(&keyout,(u8_string)keystring);
      keystring=keyout.u8_outbuf;
      keylen=keyout.u8_outptr-keyout.u8_outbuf;}
    else keylen=FD_STRLEN(key);}
  else {
    keyout.u8_outptr=keyout.u8_outbuf;
    if ((flags&FD_MONGODB_SLOTIFY)&&
        (!((FD_OIDP(key))||(FD_VECTORP(key))||(FD_PAIRP(key)))))
      u8_putc(&keyout,':');
    fd_unparse(&keyout,key);
    keystring=keyout.u8_outbuf;
    keylen=keyout.u8_outptr-keyout.u8_outbuf;}
  ok=bson_append_dtype(b,keystring,keylen,val);
  u8_close((u8_stream)&keyout);
  return ok;
}

FD_EXPORT fdtype fd_bson_output(struct FD_BSON_OUTPUT out,fdtype obj)
{
  int ok=1;
  if (FD_VECTORP(obj)) {
    int i=0, len=FD_VECTOR_LENGTH(obj);
    fdtype *elts=FD_VECTOR_ELTS(obj);
    while (i<len) {
      fdtype elt=elts[i]; u8_byte buf[16];
      sprintf(buf,"%d",i++);
      if (ok)
        ok=bson_append_dtype(out,buf,strlen(buf),elt);}}
  else if (FD_CHOICEP(obj)) {
    int i=0; FD_DO_CHOICES(elt,obj) {
      u8_byte buf[16]; sprintf(buf,"%d",i++);
      if (ok) ok=bson_append_dtype(out,buf,strlen(buf),elt);}}
  else if (FD_TABLEP(obj)) {
    fdtype keys=fd_getkeys(obj);
    {FD_DO_CHOICES(key,keys) {
        fdtype val=fd_get(obj,key,FD_VOID);
        if (ok) ok=bson_append_keyval(out,key,val);
        fd_decref(val);}}
    fd_decref(keys);}
  else if (FD_COMPOUNDP(obj)) {
    struct FD_COMPOUND *compound=FD_XCOMPOUND(obj);
    fdtype tag=compound->tag, *elts=FD_COMPOUND_ELTS(obj);
    int len=FD_COMPOUND_LENGTH(obj);
    if (tag==mongomap_symbol) {
      fdtype *scan=elts, *limit=scan+len;
      if ((len%2)==1) {
        fd_seterr(fd_SyntaxError,"fd_bson_output",
                  u8_strdup("malformed mongomap"),
                  fd_incref(obj));
        ok=0;}
      else while (scan<limit) {
          fdtype key=*scan++, value=*scan++;
          ok=bson_append_keyval(out,key,value);
          if (!(ok)) break;}}
    else if (tag==mongovec_symbol) {
      fdtype *scan=elts, *limit=scan+len; int i=0;
      while (scan<limit) {
        u8_byte buf[16]; sprintf(buf,"%d",i);
        ok=bson_append_dtype(out,buf,strlen(buf),*scan);
        i++; scan++;
        if (!(ok)) break;}}
    else {
      int i=0; 
      ok=bson_append_dtype(out,"%fdtag",6,tag);
      if (ok) while (i<len) {
          char buf[16]; sprintf(buf,"%d",i);
          ok=bson_append_dtype(out,buf,strlen(buf),elts[i++]);
          if (!(ok)) break;}}}
  if (!(ok))
    return FD_ERROR_VALUE;
  else return FD_VOID;
}

FD_EXPORT bson_t *fd_dtype2bson(fdtype obj,int flags,fdtype opts)
{
  if (FD_VOIDP(obj)) return NULL;
  else if (FD_STRINGP(obj)) {
    u8_string json=FD_STRDATA(obj); int free_it=0;
    bson_error_t error; bson_t *result;
    if (strchr(json,'"')<0) {
      json=u8_string_subst(json,"'","\""); free_it=1;}
    result=bson_new_from_json(json,FD_STRLEN(obj),&error);
    if (free_it) u8_free(json);
    if (result) return result;
    fd_seterr("Bad JSON","fd_dtype2bson/json",
              u8_strdup(error.message),fd_incref(obj));
    return NULL;}
  else {
    struct FD_BSON_OUTPUT out;
    out.doc=bson_new();
    out.flags=((flags<0)?(getflags(opts,FD_MONGODB_DEFAULTS)):(flags));
    out.opts=opts;
    fd_bson_output(out,obj);
    return out.doc;}
}

/* BSON input functions */

#define slotify_char(c) \
    ((c=='_')||(c=='-')||(c=='.')||(c=='/')||(c=='$')||(u8_isalnum(c)))

/* -1 means don't slotify at all, 0 means symbolize, 1 means intern */
static int slotcode(u8_string s)
{
  const u8_byte *scan=s; int c, i=0, hasupper=0;
  while ((c=u8_sgetc(&scan))>=0) {
    if (i>32) return -1;
    if (!(slotify_char(c))) return -1; else i++;
    if (u8_isupper(c)) hasupper=1;}
  return hasupper;
}

static fdtype bson_read_vector(FD_BSON_INPUT b);
static fdtype bson_read_choice(FD_BSON_INPUT b);

static void bson_read_step(FD_BSON_INPUT b,fdtype into,fdtype *loc)
{
  bson_iter_t *in=b.iter; int flags=b.flags;
  const unsigned char *field=bson_iter_key(in);
  bson_type_t bt=bson_iter_type(in);
  fdtype slotid, value=FD_VOID;
  if ((flags&FD_MONGODB_SLOTIFY)&&(strchr(":(#@",field[0])!=NULL))
    slotid=fd_parse_arg((u8_string)field);
  else if (flags&FD_MONGODB_SLOTIFY) {
    int sc=slotcode((u8_string)field);
    if (sc<0) slotid=fd_make_string(NULL,-1,(unsigned char *)field);
    else if (sc==0) slotid=fd_symbolize((unsigned char *)field);
    else slotid=fd_intern((unsigned char *)field);}
  else slotid=fd_make_string(NULL,-1,(unsigned char *)field);
  switch (bt) {
  case BSON_TYPE_DOUBLE:
    value=fd_make_double(bson_iter_double(in)); break;
  case BSON_TYPE_BOOL:
    value=(bson_iter_bool(in))?(FD_TRUE):(FD_FALSE); break;
  case BSON_TYPE_REGEX: {
    const char *props, *src=bson_iter_regex(in,&props);
    int flags=0;
    if (strchr(props,'x')>=0) flags|=REG_EXTENDED;
    if (strchr(props,'i')>=0) flags|=REG_ICASE;
    if (strchr(props,'m')>=0) flags|=REG_NEWLINE;
    value=fd_make_regex((u8_string)src,flags);
    break;}
  case BSON_TYPE_UTF8: {
    int len=-1;
    const unsigned char *bytes=bson_iter_utf8(in,&len);
    if ((flags&FD_MONGODB_COLONIZE)&&(strchr(":(#@",bytes[0])!=NULL))
      value=fd_parse_arg((u8_string)(bytes));
    else if ((flags&FD_MONGODB_COLONIZE)&&(bytes[0]=='\\'))
      value=fd_make_string(NULL,((len>0)?(len-1):(-1)),
                           (unsigned char *)bytes+1);
    else value=fd_make_string(NULL,((len>0)?(len):(-1)),
                              (unsigned char *)bytes);
    break;}
  case BSON_TYPE_BINARY: {
    int len; bson_subtype_t st; const unsigned char *data;
    bson_iter_binary(in,&st,&len,&data);
    if (st==BSON_SUBTYPE_UUID) {
      struct FD_UUID *uuid=u8_alloc(struct FD_UUID);
      FD_INIT_CONS(uuid,fd_uuid_type);
      memcpy(uuid->uuid,data,len);
      value= (fdtype) uuid;}
    else {
      fdtype packet=fd_make_packet(NULL,len,(unsigned char *)data);
      if (st==BSON_SUBTYPE_BINARY)
        value=fd_make_packet(NULL,len,(unsigned char *)data);
      else if (st==BSON_SUBTYPE_USER)
        value=fd_init_compound(NULL,mongouser,0,1,packet);
      else if (st==BSON_SUBTYPE_MD5)
        value=fd_init_compound(NULL,mongomd5,0,1,packet);
      else if (st==BSON_SUBTYPE_FUNCTION)
        value=fd_init_compound(NULL,mongofun,0,1,packet);
      else value=packet;}
    break;}
  case BSON_TYPE_INT32:
    value=FD_INT(bson_iter_int32(in));
    break;
  case BSON_TYPE_INT64:
    value=FD_INT(bson_iter_int64(in));
    break;
  case BSON_TYPE_OID: {
    const bson_oid_t *oidval=bson_iter_oid(in);
    const unsigned char *bytes=oidval->bytes;
    if ((bytes[0]==0)&&(bytes[1]==0)&&(bytes[2]==0)&&(bytes[3]==0)) {
      FD_OID dtoid;
      unsigned int hi=
        (((((bytes[4]<<8)|(bytes[5]))<<8)|(bytes[6]))<<8)|(bytes[7]);
      unsigned int lo=
        (((((bytes[8]<<8)|(bytes[9]))<<8)|(bytes[10]))<<8)|(bytes[11]);
      FD_SET_OID_HI(dtoid,hi); FD_SET_OID_LO(dtoid,lo);
      value=fd_make_oid(dtoid);}
    else {
      fdtype packet=fd_make_packet(NULL,12,(unsigned char *)bytes);
      value=fd_init_compound(NULL,oidtag,0,1,packet);}
    break;}
  case BSON_TYPE_UNDEFINED:
    value=FD_VOID; break;
  case BSON_TYPE_NULL:
    value=FD_EMPTY_CHOICE; break;
  case BSON_TYPE_DATE_TIME: {
    unsigned long long millis=bson_iter_date_time(in);
    struct FD_TIMESTAMP *ts=u8_alloc(struct FD_TIMESTAMP);
    FD_INIT_CONS(ts,fd_timestamp_type);
    u8_init_xtime(&(ts->xtime),millis/1000,u8_millisecond,
                  ((millis%1000)*1000000),0,0);
    value=(fdtype)ts;
    break;}
  case BSON_TYPE_MAXKEY:
    value=maxkey; break;
  case BSON_TYPE_MINKEY:
    value=minkey; break;
  default:
    if (BSON_ITER_HOLDS_DOCUMENT(in)) {
      struct FD_BSON_INPUT r; bson_iter_t child;
      bson_iter_recurse(in,&child);
      r.iter=&child; r.flags=b.flags; r.opts=b.opts;
      value=fd_init_slotmap(NULL,0,NULL);
      while (bson_iter_next(&child))
        bson_read_step(r,value,NULL);}
    else if (BSON_ITER_HOLDS_ARRAY(in)) {
      unsigned int len; fdtype *data;
      int flags=b.flags, choicevals=(flags&FD_MONGODB_CHOICEVALS);
      if (choicevals) value=bson_read_choice(b);
      else value=bson_read_vector(b);}
    else {
      u8_log(LOGWARN,fd_BSON_Input_Error,
             "Can't handle BSON type %d",bt);
      return;}}
  if (!(FD_VOIDP(into))) fd_store(into,slotid,value);
  if (loc) *loc=value;
  else fd_decref(value);
}

static fdtype bson_read_vector(FD_BSON_INPUT b)
{
  struct FD_BSON_INPUT r; bson_iter_t child;
  fdtype result, *data=u8_alloc_n(16,fdtype), *write=data, *lim=data+16;
  bson_iter_recurse(b.iter,&child);
  r.iter=&child; r.flags=b.flags; r.opts=b.opts;
  while (bson_iter_next(&child)) {
    if (write>=lim) {
      int len=lim-data;
      int newlen=((len<16384)?(len*2):(len+16384));
      fdtype *newdata=u8_realloc_n(data,newlen,fdtype);
      if (!(newdata)) u8_raise(fd_MallocFailed,"mongodb_read_vector",NULL);
      write=newdata+(write-data); lim=newdata+newlen; data=newdata;}
    bson_read_step(r,FD_VOID,write); write++;}
  result=fd_make_vector(write-data,data);
  u8_free(data);
  return result;
}

static fdtype bson_read_choice(FD_BSON_INPUT b)
{
  struct FD_BSON_INPUT r; bson_iter_t child; int n=0;
  fdtype *data=u8_alloc_n(16,fdtype), *write=data, *lim=data+16;
  bson_iter_recurse(b.iter,&child);
  r.iter=&child; r.flags=b.flags; r.opts=b.opts;
  while (bson_iter_next(&child)) {
    if (write>=lim) {
      int len=lim-data;
      int newlen=((len<16384)?(len*2):(len+16384));
      fdtype *newdata=u8_realloc_n(data,newlen,fdtype);
      if (!(newdata)) u8_raise(fd_MallocFailed,"mongodb_read_vector",NULL);
      write=newdata+(write-data); lim=newdata+newlen; data=newdata;}
    if (BSON_ITER_HOLDS_ARRAY(&child)) {
      *write++=bson_read_vector(r);}
    else {
      bson_read_step(r,FD_VOID,write);
      write++;}}
  if (write==data) {
    u8_free(data);
    return FD_EMPTY_CHOICE;}
  else return fd_make_choice
         (write-data,data,
          FD_CHOICE_DOSORT|FD_CHOICE_COMPRESS|
          FD_CHOICE_FREEDATA|FD_CHOICE_REALLOC);
}

FD_EXPORT fdtype fd_bson2dtype(bson_t *in,int flags,fdtype opts)
{
  bson_iter_t iter; fdtype result;
  if (flags<0) flags=getflags(opts,FD_MONGODB_DEFAULTS);
  memset(&iter,0,sizeof(bson_iter_t));
  if (bson_iter_init(&iter,in)) {
    struct FD_BSON_INPUT b; memset(&b,0,sizeof(struct FD_BSON_INPUT));
    b.iter=&iter; b.flags=flags; b.opts=opts;
    result=fd_init_slotmap(NULL,0,NULL);
    while (bson_iter_next(&iter)) bson_read_step(b,result,NULL);
    return result;}
  else return fd_err(fd_BSON_Input_Error,"fd_bson2dtype",NULL,FD_VOID);
}

/* MONGOMAPS */

/* MONGOMAPs are used to describe documents whose fields are in a fixed
   order, which is used by parts of MONGODB.  They're represented as
   compounds with the tag %MONGOMAP. */

static fdtype mongomap_lexpr(int n,fdtype *values)
{
  if ((n%2)==1)
    return fd_err(fd_SyntaxError,"mongomap_lexpr",
                  "Odd number of arguments",FD_VOID);
  else {
    int i=0; while (i<n) { 
      fdtype value=values[i++]; fd_incref(value);}
    return fd_init_compound_from_elts(NULL,mongomap_symbol,0,n,values);}
}

static fdtype make_mongomap(fdtype table,fdtype ordered)
{
  if (!(FD_TABLEP(table)))
    return fd_type_error("table","make_mongomap",table);
  else {
    fdtype result=FD_VOID;
    int n_slots=FD_VECTOR_LENGTH(ordered), i=0;
    fdtype *values=u8_alloc_n(n_slots*2,fdtype), *write=values;
    while (i<n_slots) {
      fdtype key=FD_VECTOR_REF(ordered,i);
      fdtype value=fd_get(table,key,FD_EMPTY_CHOICE);
      *write++=key; fd_incref(key); *write++=value;
      i++;}
    result=fd_init_compound_from_elts
      (NULL,mongomap_symbol,0,n_slots*2,values);
    u8_free(values);
    return result;}
}

static fdtype mongovec_lexpr(int n,fdtype *values)
{
  int i=0; while (i<n) { 
    fdtype value=values[i++]; fd_incref(value);}
  return fd_init_compound_from_elts(NULL,mongovec_symbol,0,n,values);
}

static fdtype make_mongovec(fdtype vec)
{
  fdtype result=FD_VOID, *elts=FD_VECTOR_ELTS(vec);
  int n=FD_VECTOR_LENGTH(vec), i=0;
  while (i<n) {fdtype v=elts[i++]; fd_incref(v);}
  return fd_init_compound_from_elts(NULL,mongovec_symbol,0,n,elts);
}

/* MongoDB pools and indices */

/* Notes:

   1.  Pools are collections which map OID offsets into integer _id
   fields.

   2.  The _id:poolinfo records in the collection records the base
   OID, the capacity, and any names or aliases.

   3.  OID allocation is handled by a record which specifies a base, a
   range, and a load.  find/modify calls are used to pull this record,
   checking the available capacity, and increment the load.  By
   default, this record lives in the same collection as the pool
   itself, but it can be on any client/database/collection/id.  This
   allows the specification of sharded object pools where different
   shards have disintct alloc structures.

   4. Locking will be done by adding documents of the form:
        {_id: someobjid, lock: intoffset, expires: datetime}

*/

/*
static fdtype mongodb_pool_fetch(fd_pool p,fdtype oid)
{
  struct FD_MONGODB_POOL *mp=(struct FD_MONGODB_POOL *)p;
  FD_OID base=mp->base, addr=FD_OID_ADDR(oid);
  mongoc_client_t *client=mongoc_client_pool_pop(mp->clients);
  mongoc_collection_t *domain=
    mongoc_client_get_collection(client,mp->dbname,mp->collection);
  mongoc_cursor_t *cursor;
  bson_t *q=bson_new(); const bson_t *doc;
  fdtype fetched=FD_VOID;
  BSON_APPEND_INT32(q,"_id",FD_OID_LO(addr)-FD_OID_LO(base));
  cursor=mongoc_collection_find(domain,MONGOC_QUERY_NONE,0,0,0,q,NULL,NULL);
  if (mongoc_cursor_next(cursor,&doc))
    fetched=fd_bson2dtype((bson_t *)doc,mp->mdbflags,mp->mdbopts);
  bson_destroy(q);
  mongoc_collection_destroy(domain);
  mongoc_client_pool_push(mp->clients,client);
  return fetched;
}
*/

/*
static fdtype *mongodb_pool_fetchn(fd_pool p,int n,fdtype *oids)
{
  struct FD_MONGODB_POOL *mp=(struct FD_MONGODB_POOL *)p;
  mongoc_client_t *client=mongoc_client_pool_pop(mp->clients);
  mongoc_collection_t *domain=
    mongoc_client_get_collection(client,mp->dbname,mp->collection);
  mongoc_cursor_t *cursor;
  bson_t *q=bson_new(); const bson_t doc, ids, *response;

  fdtype fetched=fd_init_vector(NULL,n,NULL);
  FD_OID base=mp->base; int i=0;
  char keybuf[32];

  mongodb_append_document_begin(q,"_id",3,&doc);
  mongodb_append_array_begin(&doc,"$in",3,&ids);
  while (i<n) {
    fdtype oid=oids[i]; FD_OID addr=FD_OID_ADDR(oid);
    unsigned int oid_off=FD_OID_LO(addr)-FD_OID_LO(base);
    snprintf(buf,32,"%ud",i); i++;
    BSON_APPEND_INT32(ids,buf,-1,oid_off);}
  mongodb_append_array_end(&ids);
  mongodb_append_document_end(&doc);
  while (mongoc_cursor_next(cursor,&doc)) {
    fdtype fetched=fd_bson2dtype((bson_t *)doc,mp->mdbflags,mp->mdbopts);
    fdtype id=fd_get(fetched,id_symbol);
    if (FD_FIXNUMP(id)) {
      unsigned int off=FD_FIX2INT(id);
      struct FD_OID addr=FD_OID_PLUS(base,off);}}
  bson_destroy(q);
  mongoc_collection_destroy(domain);
  mongoc_client_pool_push(mp->clients,client);

  cursor=mongoc_collection_find(domain,MONGOC_QUERY_NONE,0,0,0,q,NULL,NULL);

  fdtype value=fd_dtcall(np->connpool,2,fetch_oids_symbol,oidvec);
  fd_decref(oidvec);
  if (FD_VECTORP(value)) {
    fdtype *values=u8_alloc_n(n,fdtype);
    memcpy(values,FD_VECTOR_ELTS(value),sizeof(fdtype)*n);
    return values;}
  else {
    fd_seterr(fd_BadServerResponse,"netpool_fetchn",
              u8_strdup(np->cid),fd_incref(value));
    return NULL;}
}
*/

/* Initialization */

FD_EXPORT int fd_init_mongodb(void) FD_LIBINIT_FN;
static long long int mongodb_initialized=0;

#define DEFAULT_FLAGS (FD_SHORT2DTYPE(FD_MONGODB_DEFAULTS))

FD_EXPORT int fd_init_mongodb()
{
  fdtype module;
  if (mongodb_initialized) return 0;
  mongodb_initialized=u8_millitime();

  module=fd_new_module("MONGODB",(FD_MODULE_SAFE));

  idsym=fd_intern("_ID");
  maxkey=fd_register_constant("mongomax");
  minkey=fd_register_constant("mongomin");
  oidtag=fd_register_constant("mongoid");
  mongofun=fd_register_constant("mongofun");
  mongouser=fd_register_constant("mongouser");
  mongomd5=fd_register_constant("md5hash");

  skipsym=fd_intern("SKIP");
  limitsym=fd_intern("LIMIT");
  batchsym=fd_intern("BATCH");
  sortsym=fd_intern("SORT");
  fieldssym=fd_intern("FIELDS");
  upsertsym=fd_intern("UPSERT");
  newsym=fd_intern("NEW");
  removesym=fd_intern("REMOVE");

  sslsym=fd_intern("SSL");
  pemsym=fd_intern("PEMFILE");
  pempwd=fd_intern("PEMPWD");
  cafilesym=fd_intern("CAFILE");
  cadirsym=fd_intern("CADIR");
  crlsym=fd_intern("CRLFILE");
  
  mongomap_symbol=fd_intern("%MONGOMAP");
  mongovec_symbol=fd_intern("%MONGOVEC");

  bsonflags=fd_intern("BSON");
  raw=fd_intern("RAW");
  slotify=fd_intern("SLOTIFY");
  slotifyin=fd_intern("SLOTIFY/IN");
  slotifyout=fd_intern("SLOTIFY/OUT");
  colonize=fd_intern("COLONIZE");
  colonizein=fd_intern("COLONIZE/IN");
  colonizeout=fd_intern("COLONIZE/OUT");
  choices=fd_intern("CHOICES");
  nochoices=fd_intern("NOCHOICES");

  fd_mongoc_server=fd_register_cons_type("MongoDB client");
  fd_mongoc_collection=fd_register_cons_type("MongoDB collection");
  fd_mongoc_cursor=fd_register_cons_type("MongoDB cursor");

  fd_type_names[fd_mongoc_server]="MongoDB client";
  fd_type_names[fd_mongoc_collection]="MongoDB collection";
  fd_type_names[fd_mongoc_cursor]="MongoDB cursor";

  fd_recyclers[fd_mongoc_server]=recycle_client;
  fd_recyclers[fd_mongoc_collection]=recycle_collection;
  fd_recyclers[fd_mongoc_cursor]=recycle_cursor;

  fd_unparsers[fd_mongoc_server]=unparse_server;
  fd_unparsers[fd_mongoc_collection]=unparse_collection;
  fd_unparsers[fd_mongoc_cursor]=unparse_cursor;

  fd_idefn(module,fd_make_cprim2x("MONGODB/OPEN",mongodb_open,1,
                                  -1,FD_VOID,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("MONGODB/COLLECTION",mongodb_collection,1,
                                  -1,FD_VOID,fd_string_type,FD_VOID,
                                  fd_string_type,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("MONGODB/CURSOR",mongodb_cursor,2,
                                  -1,FD_VOID,-1,FD_VOID,
                                  -1,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("MONGODB/INSERT!",mongodb_insert,2,
                                  fd_mongoc_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("MONGODB/REMOVE!",mongodb_remove,2,
                                  fd_mongoc_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim4x("MONGODB/UPDATE!",mongodb_update,2,
                                  fd_mongoc_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID,
                                  -1,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("MONGODB/FIND",mongodb_find,2,
                                  fd_mongoc_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim4x("MONGODB/MODIFY!",mongodb_modify,3,
                                  fd_mongoc_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID,-1,FD_VOID));

  fd_idefn(module,fd_make_cprimn("MONGODB/RESULTS",mongodb_command,2));
  fd_idefn(module,fd_make_cprimn("MONGODB/DO",mongodb_simple_command,2));

  fd_idefn(module,fd_make_cprim1x("MONGODB/DONE?",mongodb_donep,1,
                                  fd_mongoc_cursor,FD_VOID));
  fd_idefn(module,fd_make_cprim2x("MONGODB/SKIP",mongodb_skip,1,
                                  fd_mongoc_cursor,FD_VOID,
                                  fd_fixnum_type,FD_FIXNUM_ONE));
  fd_idefn(module,fd_make_cprim3x("MONGODB/READ",mongodb_read,1,
                                  fd_mongoc_cursor,FD_VOID,
                                  fd_fixnum_type,FD_FIXNUM_ONE,
                                  -1,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("MONGODB/READ->VECTOR",
                                  mongodb_readvec,1,
                                  fd_mongoc_cursor,FD_VOID,
                                  fd_fixnum_type,FD_FIXNUM_ONE,
                                  -1,FD_VOID));
  fd_idefn(module,fd_make_cprimn("MONGOMAP",mongomap_lexpr,0));
  fd_idefn(module,fd_make_cprim2x("->MONGOMAP",make_mongomap,2,
                                  -1,FD_VOID,fd_vector_type,FD_VOID));
  fd_idefn(module,fd_make_cprimn("MONGOVEC",mongovec_lexpr,0));
  fd_idefn(module,fd_make_cprim1x("->MONGOVEC",make_mongovec,1,
                                  fd_vector_type,FD_VOID));

  fd_register_config("MONGODB:FLAGS",
                     "Default flags (fixnum) for MongoDB/BSON processing",
                     fd_intconfig_get,fd_intconfig_set,&mongodb_defaults);

  fd_finish_module(module);
  fd_persist_module(module);

  mongoc_init();
  atexit(mongoc_cleanup);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
