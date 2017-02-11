/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* mongodb.c
   This implements FramerD bindings to mongodb.
   Copyright (C) 2007-2017 beingmeta, inc.
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
u8_condition fd_BSON_Compound_Overflow=_("BSON/FramerD compound overflow");

static fdtype sslsym;
static int default_ssl=0;
static int client_loglevel=LOG_INFO;
static int logops=0;

static fdtype dbname_symbol, username_symbol, auth_symbol, fdtag_symbol;
static fdtype hosts_symbol, connections_symbol, fieldmap_symbol, logopsym;

static struct FD_KEYVAL *mongo_opmap=NULL;
static int mongo_opmap_size=0, mongo_opmap_space=0;

fd_ptr_type fd_mongoc_server, fd_mongoc_collection, fd_mongoc_cursor;

static bool bson_append_keyval(struct FD_BSON_OUTPUT,fdtype,fdtype);
static bool bson_append_dtype(struct FD_BSON_OUTPUT,const char *,int,fdtype);
static fdtype idsym, maxkey, minkey;
static fdtype oidtag, mongofun, mongouser, mongomd5;
static fdtype bsonflags, raw, slotify, slotifyin, slotifyout, softfailsym;
static fdtype colonize, colonizein, colonizeout, choices, nochoices;
static fdtype skipsym, limitsym, batchsym, sortsym, sortedsym, writesym, readsym;
static fdtype fieldssym, upsertsym, newsym, removesym, singlesym, wtimeoutsym;
static fdtype max_clients_symbol, max_ready_symbol, returnsym, originalsym;
static fdtype primarysym, primarypsym, secondarysym, secondarypsym, nearestsym;
static fdtype poolmaxsym, poolminsym;
static fdtype mongomap_symbol, mongovec_symbol;

static void grab_mongodb_error(bson_error_t *error,u8_string caller)
{
  u8_condition cond=u8_strerror(error->code);
  u8_seterr(cond,caller,u8_strdup(error->message));
}

#define MONGODB_CLIENT_BLOCK 1
#define MONGODB_CLIENT_NOBLOCK 0

static mongoc_client_t *get_client(FD_MONGODB_DATABASE *server,int block)
{
  mongoc_client_t *client;
  if (block) {
    u8_log(client_loglevel,_("MongoDB/getclient"),
           "Getting client from server %llx (%s)",server->dbclients,server->dbspec);
    client=mongoc_client_pool_pop(server->dbclients);}
  else client=mongoc_client_pool_try_pop(server->dbclients);
  u8_log(client_loglevel,_("MongoDB/gotclient"),
         "Got client %llx from server %llx (%s)",
         client,server->dbclients,server->dbspec);
  return client;
}

static void release_client(FD_MONGODB_DATABASE *server,mongoc_client_t *client)
{
  u8_log(client_loglevel,_("MongoDB/freeclient"),
         "Releasing client %llx to server %llx (%s)",
         client,server->dbclients,server->dbspec);
  mongoc_client_pool_push(server->dbclients,client);
}

static int boolopt(fdtype opts,fdtype key,int dflt)
{
  if (FD_TABLEP(opts)) {
    fdtype v=fd_get(opts,key,FD_VOID);
    if (FD_VOIDP(v))
      return dflt;
    else if (FD_FALSEP(v))
      return 0;
    else {
      fd_decref(v);
      return 1;}}
  else return dflt;
}

static u8_string stropt(fdtype opts,fdtype key,u8_string dflt)
{
  if (FD_TABLEP(opts)) {
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
      return NULL;}}
  else if (dflt==NULL) return dflt;
  else return u8_strdup(dflt);
}

static bson_t *get_projection(fdtype opts,int flags)
{
  fdtype projection=fd_getopt(opts,returnsym,FD_VOID);
  if ((FD_CONSP(projection))&&(FD_TABLEP(projection))) {
    bson_t *fields=fd_dtype2bson(projection,flags,opts);
    fd_decref(projection);
    return fields;}
  else return NULL;
}

static int grow_dtype_vec(fdtype **vecp,size_t n,size_t *vlenp)
{
  fdtype *vec=*vecp; size_t vlen=*vlenp;
  if (n<vlen) return 1;
  else if (vec==NULL) {
    vec=u8_alloc_n(64,fdtype);
    if (vec) {
      *vlenp=64; *vecp=vec;
      return 1;}}
  else {
    size_t new_len=((vlen<8192)?(vlen*2):(vlen+8192));
    fdtype *new_vec=u8_realloc_n(vec,new_len,fdtype);
    if (new_vec) {
      *vecp=new_vec; *vlenp=new_len;
      return 1;}}
  if (vec) {
    int i=0; while (i<n) { fd_decref(vec[i++]); }
    u8_free(vec);}
  return 0;
}

static void free_dtype_vec(fdtype *vec,int n)
{
  if (vec==NULL) return;
  else {
    int i=0; while (i<n) { fd_decref(vec[i++]); }
    u8_free(vec);}
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
    if (fd_overlapp(opts,logopsym)) flags|=FD_MONGODB_LOGOPS;
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

static int get_write_flags(fdtype val)
{
  if (FD_VOIDP(val)) 
    return MONGOC_WRITE_CONCERN_W_DEFAULT;
  else if (FD_FALSEP(val))
    return MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED;
  else if (FD_TRUEP(val))
    return MONGOC_WRITE_CONCERN_W_MAJORITY;
  else if ((FD_FIXNUMP(val))&&(FD_FIX2INT(val)<0))
    return MONGOC_WRITE_CONCERN_W_ERRORS_IGNORED;
  else if ((FD_FIXNUMP(val))&&(FD_FIX2INT(val)>0))
    return FD_FIX2INT(val);
  else {
    u8_log(LOGWARN,"mongodb/get_write_concern","Bad MongoDB write concern %q",val);
    return MONGOC_WRITE_CONCERN_W_DEFAULT;}
}

static mongoc_write_concern_t *get_write_concern(fdtype opts)
{
  fdtype val=fd_getopt(opts,writesym,FD_VOID);
  fdtype wait=fd_getopt(opts,wtimeoutsym,FD_VOID);
  if ((FD_VOIDP(val))&&(FD_VOIDP(wait))) return NULL;
  else {
    mongoc_write_concern_t *wc=mongoc_write_concern_new();
    if (!(FD_VOIDP(val))) {
      int w=get_write_flags(val);
      mongoc_write_concern_set_w(wc,w);}
    if (FD_FIXNUMP(wait)) {
      int msecs=FD_FIX2INT(wait);
      mongoc_write_concern_set_wtimeout(wc,msecs);}
    fd_decref(wait);
    fd_decref(val);
    return wc;}
}

static int getreadmode(fdtype val)
{
  if (FD_EQ(val,primarysym))
    return MONGOC_READ_PRIMARY;
  else if (FD_EQ(val,primarypsym))
    return MONGOC_READ_PRIMARY_PREFERRED;
  else if (FD_EQ(val,secondarysym))
    return MONGOC_READ_SECONDARY;
  else if (FD_EQ(val,secondarypsym))
    return MONGOC_READ_SECONDARY_PREFERRED;
  else if (FD_EQ(val,nearestsym))
    return MONGOC_READ_NEAREST;
  else {
    u8_log(LOGWARN,"mongodb/getreadmode","Bad MongoDB read mode %q",val);
    return MONGOC_READ_PRIMARY;}
}

static mongoc_read_prefs_t *get_read_prefs(fdtype opts)
{
  fdtype spec=fd_getopt(opts,readsym,FD_VOID);
  if (FD_VOIDP(spec)) return NULL;
  else {
    mongoc_read_prefs_t *rp=mongoc_read_prefs_new(MONGOC_READ_PRIMARY);
    int flags=getflags(opts,mongodb_defaults);
    FD_DO_CHOICES(s,spec) {
      if (FD_SYMBOLP(s)) {
        int p=getreadmode(s);
        mongoc_read_prefs_set_mode(rp,p);}
      else if (FD_TABLEP(s)) {
        const bson_t *bson=fd_dtype2bson(s,flags,opts);
        mongoc_read_prefs_add_tag(rp,bson);}
      else {
        u8_log(LOGWARN,"mongodb/getreadmode","Bad MongoDB read preference %q",s);}}
    fd_decref(spec);
    return rp;}
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

static int mongodb_getflags(fdtype mongodb);

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
  int add_ssl=boolopt(opts,sslsym,default_ssl);
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
    fdtype poolmax=fd_getopt(opts,poolmaxsym,FD_VOID);
    fdtype poolmin=fd_getopt(opts,poolminsym,FD_VOID);
    if ((mongoc_uri_get_ssl(info))&& 
        (setup_ssl(&ssl_opts,info,opts)))
      mongoc_client_pool_set_ssl_opts(client_pool,&ssl_opts);
    if (FD_FIXNUMP(poolmax)) {
      int pmax=FD_FIX2INT(poolmax);
      mongoc_client_pool_max_size(client_pool,pmax);}
    if (FD_FIXNUMP(poolmin)) {
      int pmin=FD_FIX2INT(poolmin);
      mongoc_client_pool_min_size(client_pool,pmin);}
    fd_decref(poolmax); fd_decref(poolmin);
    FD_INIT_CONS(srv,fd_mongoc_server);
    srv->dburi=uri; 
    if (dbname==NULL) 
      srv->dbname=NULL;
    else srv->dbname=u8_strdup(dbname);
    srv->dbspec=get_connection_spec(info);
    srv->dburi_info=info; srv->dbclients=client_pool;
    srv->dbopts=opts; fd_incref(opts);
    srv->dbflags=flags;
    if ((logops)||(flags&FD_MONGODB_LOGOPS))
      u8_log(-LOG_INFO,"MongoDB/open",
             "Opened %s with %s",dbname,srv->dbspec);
    return (fdtype)srv;}
  else {
    mongoc_uri_destroy(info); fd_decref(opts); u8_free(uri);
    return fd_type_error("MongoDB client URI","mongodb_open",arg);}
}
static void recycle_server(struct FD_CONS *c)
{
  struct FD_MONGODB_DATABASE *s=(struct FD_MONGODB_DATABASE *)c;
  mongoc_uri_destroy(s->dburi_info);
  mongoc_client_pool_destroy(s->dbclients);
  fd_decref(s->dbopts);
  u8_free(c);
}
static int unparse_server(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_MONGODB_DATABASE *srv=(struct FD_MONGODB_DATABASE *)x;
  u8_printf(out,"#<MongoDB/Server %s/%s>",srv->dbspec,srv->dbname);
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
    flags=getflags(opts_arg,srv->dbflags);
    opts=combine_opts(opts_arg,srv->dbopts);
    fd_incref(server);}
  else if ((FD_STRINGP(server))||
           (FD_SYMBOLP(server))||
           (FD_PRIM_TYPEP(server,fd_secret_type))) {
    fdtype consed=mongodb_open(server,opts_arg);
    if (FD_ABORTP(consed)) return consed;
    server=consed; srv=(struct FD_MONGODB_DATABASE *)consed;
    flags=getflags(opts_arg,srv->dbflags);
    opts=combine_opts(opts_arg,srv->dbopts);}
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
  result->domain_db=server;
  result->domain_opts=opts;
  result->domain_flags=flags;
  return (fdtype) result;
}
static void recycle_collection(struct FD_CONS *c)
{
  struct FD_MONGODB_COLLECTION *collection=(struct FD_MONGODB_COLLECTION *)c;
  fd_decref(collection->domain_db);
  fd_decref(collection->domain_opts);
  u8_free(collection);
}
static int unparse_collection(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_MONGODB_COLLECTION *coll=(struct FD_MONGODB_COLLECTION *)x;
  struct FD_MONGODB_DATABASE *db=
    (struct FD_MONGODB_DATABASE *) (coll->domain_db);
  u8_printf(out,"#<MongoDB/Collection %s/%s/%s>",
            db->dbspec,db->dbname,coll->collection_name);
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
    (struct FD_MONGODB_DATABASE *)(domain->domain_db);
  u8_string dbname=server->dbname;
  u8_string collection_name=domain->collection_name;
  mongoc_client_t *client=get_client(server,(!(flags&FD_MONGODB_NOBLOCK)));
  if (client) {
    mongoc_collection_t *collection=
      mongoc_client_get_collection(client,dbname,collection_name);
    if (collection) {
      *clientp=client;
      return collection;}
    else {
      release_client(server,client);
      return NULL;}}
  else return NULL;
}

/* This returns a client to a client pool.  The argument can be either
   a server or a collection (which is followed to its server). */
static void client_done(fdtype arg,mongoc_client_t *client)
{
  if (FD_PRIM_TYPEP(arg,fd_mongoc_server)) {
    struct FD_MONGODB_DATABASE *server=(struct FD_MONGODB_DATABASE *)arg;
    release_client(server,client);}
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_collection)) {
    struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
    struct FD_MONGODB_DATABASE *server=
      (struct FD_MONGODB_DATABASE *)(domain->domain_db);
    release_client(server,client);}
  else {
    u8_log(LOG_WARN,"BAD client_done call","Wrong type for %q",arg);}
}

/* This destroys the collection returns a client to a client pool. */
static void collection_done(mongoc_collection_t *collection,
                            mongoc_client_t *client,
                            struct FD_MONGODB_COLLECTION *domain)
{
  struct FD_MONGODB_DATABASE *server=
    (fd_mongodb_database)domain->domain_db;
  mongoc_collection_destroy(collection);
  release_client(server,client);
}

/* Basic operations on collections */

static fdtype mongodb_insert(fdtype arg,fdtype obj,fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  struct FD_MONGODB_DATABASE *db=
    (struct FD_MONGODB_DATABASE *) (domain->domain_db);
  if (FD_EMPTY_CHOICEP(obj))
    return FD_EMPTY_CHOICE;
  else {
    fdtype result;
    int flags=getflags(opts_arg,domain->domain_flags);
    fdtype opts=combine_opts(opts_arg,db->dbopts);
    mongoc_client_t *client=NULL; bool retval;
    mongoc_collection_t *collection=open_collection(domain,&client,flags);
    if (collection) {
      bson_t reply; bson_error_t error;
      mongoc_write_concern_t *wc=get_write_concern(opts);
      if ((logops)||(flags&FD_MONGODB_LOGOPS))
        u8_log(-LOG_INFO,"MongoDB/insert",
               "Inserting %d items into %q",FD_CHOICE_SIZE(obj),arg);
      if (FD_CHOICEP(obj)) {
        mongoc_bulk_operation_t *bulk=
          mongoc_collection_create_bulk_operation(collection,true,wc);
        FD_DO_CHOICES(elt,obj) {
          bson_t *doc=fd_dtype2bson(elt,flags,opts);
          if (doc) {
            mongoc_bulk_operation_insert(bulk,doc);
            bson_destroy(doc);}}
        retval=mongoc_bulk_operation_execute(bulk,&reply,&error);
        mongoc_bulk_operation_destroy(bulk);
        if (retval) {
          result=fd_bson2dtype(&reply,flags,opts);}
        else {
          fd_seterr(fd_MongoDB_Error,"mongodb_insert",
                    u8_mkstring("%s>%s>%s:%s",
                                db->dburi,db->dbname,
                                domain->collection_name,
                                error.message),
                    fd_incref(obj));
          result=FD_ERROR_VALUE;}
        bson_destroy(&reply);}
      else {
        bson_t *doc=fd_dtype2bson(obj,flags,opts);
        if ((doc)&&
            (retval=mongoc_collection_insert
             (collection,MONGOC_INSERT_NONE,doc,wc,&error))) {
          result=FD_TRUE;}
        else {
          if (doc) bson_destroy(doc);
          fd_seterr(fd_MongoDB_Error,"mongodb_insert",
                    u8_mkstring("%s>%s>%s:%s",
                                db->dburi,db->dbname,
                                domain->collection_name,
                                error.message),
                    fd_incref(obj));
          result=FD_ERROR_VALUE;}}
      if (wc) mongoc_write_concern_destroy(wc);
      collection_done(collection,client,domain);}
    else result=FD_ERROR_VALUE;
    fd_decref(opts);
    return result;}
}

static fdtype mongodb_remove(fdtype arg,fdtype obj,fdtype opts_arg)
{
  fdtype result=FD_VOID;
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  struct FD_MONGODB_DATABASE *db=DOMAIN2DB(domain);
  fdtype opts=combine_opts(opts_arg,db->dbopts);
  int flags=getflags(opts_arg,domain->domain_flags), hasid=1;
  mongoc_client_t *client=NULL;
  mongoc_collection_t *collection=open_collection(domain,&client,flags);
  if (collection) {
    struct FD_BSON_OUTPUT q; bson_error_t error;
    mongoc_write_concern_t *wc=get_write_concern(opts);
    q.bson_doc=bson_new();
    q.bson_opts=opts;
    q.bson_flags=flags;
    q.bson_fieldmap=FD_VOID;
    if (FD_TABLEP(obj)) {
      fdtype id=fd_get(obj,idsym,FD_VOID);
      if (FD_VOIDP(id)) {
        q.bson_fieldmap=fd_getopt(opts,fieldmap_symbol,FD_VOID);
        fd_bson_output(q,obj);
        hasid=0;}
      else {
        bson_append_dtype(q,"_id",3,id);
        fd_decref(id);}}
    else bson_append_dtype(q,"_id",3,obj);
    if ((logops)||(flags&FD_MONGODB_LOGOPS))
      u8_log(-LOG_INFO,"MongoDB/remove","Removing %q items from %q",obj,arg);
    if (mongoc_collection_remove(collection,
                                 ((hasid)?(MONGOC_REMOVE_SINGLE_REMOVE):
                                  (MONGOC_REMOVE_NONE)),
                                 q.bson_doc,wc,&error)) {
      result=FD_TRUE;}
    else {
      fd_seterr(fd_MongoDB_Error,"mongodb_remove",
                u8_mkstring("%s>%s>%s:%s",
                            db->dburi,db->dbname,domain->collection_name,
                            error.message),
                fd_incref(obj));
      result=FD_ERROR_VALUE;}
    collection_done(collection,client,domain);
    if (wc) mongoc_write_concern_destroy(wc);
    fd_decref(q.bson_fieldmap);
    bson_destroy(q.bson_doc);}
  else result=FD_ERROR_VALUE;
  fd_decref(opts);
  return result;
}

static fdtype mongodb_update(fdtype arg,fdtype query,fdtype update,
                             fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  struct FD_MONGODB_DATABASE *db=DOMAIN2DB(domain);
  int flags=getflags(opts_arg,domain->domain_flags);
  fdtype opts=combine_opts(opts_arg,domain->domain_opts);
  mongoc_client_t *client=NULL;
  mongoc_collection_t *collection=open_collection(domain,&client,flags);
  if (collection) {
    bson_t *q=fd_dtype2bson(query,flags,opts);
    bson_t *u=fd_dtype2bson(update,flags,opts);
    mongoc_write_concern_t *wc=get_write_concern(opts);
    bson_error_t error;
    int success=0, no_error=boolopt(opts,softfailsym,0);
    mongoc_update_flags_t update_flags=
      (MONGOC_UPDATE_NONE) ||
      ((boolopt(opts,upsertsym,0))?(MONGOC_UPDATE_UPSERT):(0)) ||
      ((boolopt(opts,singlesym,0))?(0):(MONGOC_UPDATE_MULTI_UPDATE));
    if ((logops)||(flags&FD_MONGODB_LOGOPS))
      u8_log(-LOG_INFO,"MongoDB/update",
             "Updating matches to %q with %q in %q",query,update,arg);
    if ((q)&&(u))
      success=mongoc_collection_update(collection,update_flags,q,u,wc,&error);
    collection_done(collection,client,domain);
    if (q) bson_destroy(q); 
    if (u) bson_destroy(u);
    if (wc) mongoc_write_concern_destroy(wc);
    fd_decref(opts);
    if (success) return FD_TRUE;
    else if (no_error) {
      if ((q)&&(u))
        u8_log(LOG_WARN,"mongodb_update",
               "Error %s on %s>%s>%s with query\nquery=  %q\nupdate=  %q\nflags= %q",
               error.message,db->dburi,db->dbname,
               domain->collection_name,query,update,opts);
      else u8_log(LOG_WARN,"mongodb_update",
                  "Error %s on %s>%s>%s with query\nquery=  %q\nupdate=  %q\nflags= %q",
                  error.message,db->dburi,db->dbname,
                  domain->collection_name,query,update,opts);
      return FD_FALSE;}
    else if ((q)&&(u))
      fd_seterr(fd_MongoDB_Error,"mongodb_update/call",
                u8_mkstring("%s>%s>%s:%s",
                            db->dburi,db->dbname,domain->collection_name,
                            error.message),
                fd_make_pair(query,update));
    else fd_seterr(fd_BSON_Error,"mongodb_update/prep",
                   u8_mkstring("%s>%s>%s:%s",
                               db->dburi,db->dbname,domain->collection_name),
                   fd_make_pair(query,update));
    return FD_ERROR_VALUE;}
  else {
    fd_decref(opts);
    return FD_ERROR_VALUE;}
}

static fdtype mongodb_find(fdtype arg,fdtype query,fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  struct FD_MONGODB_DATABASE *db=DOMAIN2DB(domain);
  int flags=getflags(opts_arg,domain->domain_flags);
  fdtype opts=combine_opts(opts_arg,domain->domain_opts);
  mongoc_client_t *client=NULL;
  mongoc_collection_t *collection=open_collection(domain,&client,flags);
  if (collection) {
    fdtype results=FD_EMPTY_CHOICE;
    mongoc_cursor_t *cursor=NULL;
    const bson_t *doc;
    fdtype skip_arg=fd_getopt(opts,skipsym,FD_FIXZERO);
    fdtype limit_arg=fd_getopt(opts,limitsym,FD_FIXZERO);
    fdtype batch_arg=fd_getopt(opts,batchsym,FD_FIXZERO);
    int sort_results=fd_testopt(opts,sortedsym,FD_VOID);
    fdtype *vec=NULL; size_t n=0, max=0;
    if ((FD_FIXNUMP(skip_arg))&&(FD_FIXNUMP(limit_arg))&&(FD_FIXNUMP(batch_arg))) {
      bson_t *q=fd_dtype2bson(query,flags,opts);
      bson_t *fields=get_projection(opts,flags);
      mongoc_read_prefs_t *rp=get_read_prefs(opts);
      if ((logops)||(flags&FD_MONGODB_LOGOPS))
        u8_log(-LOG_INFO,"MongoDB/find","Matches to %q in %q",query,arg);
      if (q) cursor=mongoc_collection_find
               (collection,MONGOC_QUERY_NONE,
                FD_FIX2INT(skip_arg),FD_FIX2INT(limit_arg),FD_FIX2INT(batch_arg),
                q,fields,rp);
      if (cursor) {
        while (mongoc_cursor_next(cursor,&doc)) {
          /* u8_string json=bson_as_json(doc,NULL); */
          fdtype r=fd_bson2dtype((bson_t *)doc,flags,opts);
          if (FD_ABORTP(r)) {
            fd_decref(results);
            free_dtype_vec(vec,n);
            results=FD_ERROR_VALUE;
            sort_results=0;}
          else if (sort_results) {
            if (n>=max) {
              if (!(grow_dtype_vec(&vec,n,&max))) {
                free_dtype_vec(vec,n);
                results=FD_ERROR_VALUE;
                sort_results=0;
                break;}}
            vec[n++]=r;}
          else {
            FD_ADD_TO_CHOICE(results,r);}}
        if (rp) mongoc_read_prefs_destroy(rp);
        mongoc_cursor_destroy(cursor);}
      else results=fd_err(fd_MongoDB_Error,"mongodb_find","couldn't get cursor",opts);
      if (q) bson_destroy(q);
      if (fields) bson_destroy(fields);}
    else {
      results=fd_err(fd_TypeError,"mongodb_find","bad skip/limit/batch",opts);
      sort_results=0;}
    collection_done(collection,client,domain);
    fd_decref(opts);
    if (sort_results) {
      if ((vec==NULL)||(n==0)) return fd_make_vector(0,NULL);
      else results=fd_make_vector(n,vec);
      if (vec) u8_free(vec);}
    return results;}
  else {
    fd_decref(opts);
    return FD_ERROR_VALUE;}
}

static fdtype mongodb_get(fdtype arg,fdtype query,fdtype opts_arg)
{
  fdtype result=FD_EMPTY_CHOICE;
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  struct FD_MONGODB_DATABASE *db=DOMAIN2DB(domain);
  int flags=getflags(opts_arg,domain->domain_flags);
  fdtype opts=combine_opts(opts_arg,domain->domain_opts);
  mongoc_client_t *client=NULL;
  mongoc_collection_t *collection=open_collection(domain,&client,flags);
  if (collection) {
    mongoc_cursor_t *cursor;
    const bson_t *doc;
    bson_t *q, *fields=get_projection(opts,flags);
    mongoc_read_prefs_t *rp=get_read_prefs(opts);
    if ((!(FD_OIDP(query)))&&(FD_TABLEP(query)))
      q=fd_dtype2bson(query,flags,opts);
    else {
      struct FD_BSON_OUTPUT out;
      out.bson_doc=bson_new();
      out.bson_flags=((flags<0)?(getflags(opts,FD_MONGODB_DEFAULTS)):(flags));
      out.bson_opts=opts;
      bson_append_dtype(out,"_id",3,query);
      q=out.bson_doc;}
    if ((logops)||(flags&FD_MONGODB_LOGOPS))
      u8_log(-LOG_INFO,"MongoDB/get","Matches to %q in %q",query,arg);
    if (q) cursor=mongoc_collection_find
             (collection,MONGOC_QUERY_NONE,0,1,0,q,fields,NULL);
    if ((cursor)&&(mongoc_cursor_next(cursor,&doc))) {
      result=fd_bson2dtype((bson_t *)doc,flags,opts);}
    if (cursor) mongoc_cursor_destroy(cursor);
    if (rp) mongoc_read_prefs_destroy(rp); 
    if (q) bson_destroy(q); 
    if (fields) bson_destroy(fields); 
    fd_decref(opts);
    collection_done(collection,client,domain);
    return result;}
  else {
    fd_decref(opts);
    return FD_ERROR_VALUE;}
}

/* Find and Modify */

static int getnewopt(fdtype opts,int dflt);

static fdtype mongodb_modify(fdtype arg,fdtype query,fdtype update,
                             fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  struct FD_MONGODB_DATABASE *db=DOMAIN2DB(domain);
  int flags=getflags(opts_arg,domain->domain_flags);
  fdtype opts=combine_opts(opts_arg,domain->domain_opts);
  mongoc_client_t *client;
  mongoc_collection_t *collection=open_collection(domain,&client,flags);
  if (collection) {
    fdtype result=FD_VOID;
    fdtype sort=fd_getopt(opts,sortsym,FD_VOID);
    fdtype fields=fd_getopt(opts,fieldssym,FD_VOID);
    fdtype upsert=fd_getopt(opts,upsertsym,FD_FALSE);
    fdtype remove=fd_getopt(opts,removesym,FD_FALSE);
    fdtype donew=getnewopt(opts,1);
    bson_t *q=fd_dtype2bson(query,flags,opts);
    bson_t *u=fd_dtype2bson(update,flags,opts);
    bson_t *reply=bson_new(); bson_error_t error;
    if ((q==NULL)||(u==NULL)) return FD_ERROR_VALUE;
    if ((logops)||(flags&FD_MONGODB_LOGOPS))
      u8_log(-LOG_INFO,"MongoDB/find+modify","Matches to %q using %q in %q",
             query,update,arg);
    if (mongoc_collection_find_and_modify
        (collection,
         q,fd_dtype2bson(sort,flags,opts),
         u,fd_dtype2bson(fields,flags,opts),
         ((FD_FALSEP(remove))?(false):(true)),
         ((FD_FALSEP(upsert))?(false):(true)),
         ((FD_FALSEP(donew))?(false):(true)),
         reply,&error)) {
      result=fd_bson2dtype(reply,flags,opts);}
    else {
      fd_seterr(fd_MongoDB_Error,"mongodb_modify",
                u8_mkstring("%s>%s>%s:%s",
                            db->dburi,db->dbname,domain->collection_name,
                            error.message),
                fd_make_pair(query,update));
      result=FD_ERROR_VALUE;}
    collection_done(collection,client,domain);
    bson_destroy(q); bson_destroy(u);
    bson_destroy(reply);
    fd_decref(opts);
    return result;}
  else {
    fd_decref(opts);
    return FD_ERROR_VALUE;}
}

static int getnewopt(fdtype opts,int dflt)
{
  fdtype v=fd_getopt(opts,newsym,FD_VOID);
  if (FD_VOIDP(v)) {
    v=fd_getopt(opts,originalsym,FD_VOID);
    if (FD_VOIDP(v)) return dflt;
    else if (FD_FALSEP(v)) return 1;
    else {
      fd_decref(v);
      return 0;}}
  else if (FD_FALSEP(v)) return 0;
  else {
    fd_decref(v);
    return 1;}
}

/* Command execution */

static fdtype make_mongovec(fdtype vec);

static fdtype make_command(int n,fdtype *values)
{
  if ((n%2)==1)
    return fd_err(fd_SyntaxError,"mongomap_command",
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

static fdtype collection_command(fdtype arg,fdtype command,fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  struct FD_MONGODB_DATABASE *db=DOMAIN2DB(domain);
  int flags=getflags(opts_arg,domain->domain_flags);
  fdtype opts=combine_opts(opts_arg,domain->domain_opts);
  fdtype fields=fd_get(opts,fieldssym,FD_VOID);
  mongoc_client_t *client;
  mongoc_collection_t *collection=open_collection(domain,&client,flags);
  if (collection) {
    fdtype results=FD_EMPTY_CHOICE;
    bson_t *cmd=fd_dtype2bson(command,flags,opts);
    bson_t *flds=fd_dtype2bson(fields,flags,opts);
    if (cmd) {
      const bson_t *doc;
      fdtype skip_arg=fd_getopt(opts,skipsym,FD_FIXZERO);
      fdtype limit_arg=fd_getopt(opts,limitsym,FD_FIXZERO);
      fdtype batch_arg=fd_getopt(opts,batchsym,FD_FIXZERO);
      if ((FD_FIXNUMP(skip_arg))&&(FD_FIXNUMP(limit_arg))&&(FD_FIXNUMP(batch_arg))) {
        mongoc_cursor_t *cursor=mongoc_collection_command
          (collection,MONGOC_QUERY_EXHAUST,
           (FD_FIX2INT(skip_arg)),(FD_FIX2INT(limit_arg)),(FD_FIX2INT(batch_arg)),
           cmd,flds,NULL);
        while (mongoc_cursor_next(cursor,&doc)) {
          fdtype r=fd_bson2dtype((bson_t *)doc,flags,opts);
          FD_ADD_TO_CHOICE(results,r);}
        mongoc_cursor_destroy(cursor);}
      else results=fd_err(fd_TypeError,"collection_command","bad skip/limit/batch",opts);
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
  int flags=getflags(opts_arg,srv->dbflags);
  fdtype opts=combine_opts(opts_arg,srv->dbopts);
  fdtype fields=fd_getopt(opts,fieldssym,FD_VOID);
  mongoc_client_t *client=get_client(srv,MONGODB_CLIENT_BLOCK);
  if (client) {
    fdtype results=FD_EMPTY_CHOICE;
    bson_t *cmd=fd_dtype2bson(command,flags,opts);
    bson_t *flds=fd_dtype2bson(fields,flags,opts);    
    if (cmd) {
      const bson_t *doc;
      fdtype skip_arg=fd_getopt(opts,skipsym,FD_FIXZERO);
      fdtype limit_arg=fd_getopt(opts,limitsym,FD_FIXZERO);
      fdtype batch_arg=fd_getopt(opts,batchsym,FD_FIXZERO);
      if ((FD_FIXNUMP(skip_arg))&&(FD_FIXNUMP(limit_arg))&&(FD_FIXNUMP(batch_arg))) {
        mongoc_cursor_t *cursor=mongoc_client_command
          (client,srv->dbname,MONGOC_QUERY_EXHAUST,
           (FD_FIX2INT(skip_arg)),(FD_FIX2INT(limit_arg)),(FD_FIX2INT(batch_arg)),
           cmd,flds,NULL);
        while (mongoc_cursor_next(cursor,&doc)) {
          fdtype r=fd_bson2dtype((bson_t *)doc,flags,opts);
          FD_ADD_TO_CHOICE(results,r);}
        mongoc_cursor_destroy(cursor);}
      else results=fd_err(fd_TypeError,"collection_command","bad skip/limit/batch",opts);
      client_done(arg,client);
      bson_destroy(cmd); fd_decref(opts);
      return results;}
    else {
      return FD_ERROR_VALUE;}}
  else return FD_ERROR_VALUE;
}

static fdtype mongodb_command(int n,fdtype *args)
{
  fdtype arg=args[0], opts=FD_VOID, command=FD_VOID, result=FD_VOID;
  int flags=mongodb_getflags(arg);
  if (flags<0)
    return fd_type_error(_("MongoDB"),"mongodb_command",arg);
  else if (n==2) {
    command=args[1]; fd_incref(command); opts=FD_VOID;}
  else if ((n==3)&&(FD_TABLEP(args[1]))) {
    command=args[1]; fd_incref(command); opts=args[2];}
  else if (n%2) {
    command=make_command(n-1,args+1);}
  else {
    command=make_command(n-2,args+2);
    opts=args[1];}
  if ((logops)||(flags&FD_MONGODB_LOGOPS)) {
    u8_log(-LOG_INFO,"MongoDB/RESULTS","At %q: %q",arg,command);}
  if (FD_PRIM_TYPEP(arg,fd_mongoc_server)) 
    result=db_command(arg,command,opts);
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_collection))
    result=collection_command(arg,command,opts);
  else {}
  fd_decref(command);
  return result;
}

static fdtype collection_simple_command(fdtype arg,fdtype command,
                                        fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  struct FD_MONGODB_DATABASE *db=DOMAIN2DB(domain);
  int flags=getflags(opts_arg,domain->domain_flags);
  fdtype opts=combine_opts(opts_arg,domain->domain_opts);
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
  int flags=getflags(opts_arg,srv->dbflags);
  fdtype opts=combine_opts(opts_arg,srv->dbopts);
  mongoc_client_t *client=get_client(srv,MONGODB_CLIENT_BLOCK);
  if (client) {
    bson_t response; bson_error_t error;
    bson_t *cmd=fd_dtype2bson(command,flags,opts);
    if (cmd) {
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
  fdtype arg=args[0], opts=FD_VOID, command=FD_VOID, result=FD_VOID;
  int flags=mongodb_getflags(arg);
  if (flags<0) return fd_type_error(_("MongoDB"),"mongodb_command",arg);
  else if (n==2) {
    command=args[1]; fd_incref(command); opts=FD_VOID;}
  else if ((n==3)&&(FD_TABLEP(args[1]))) {
    command=args[1]; fd_incref(command); opts=args[2];}
  else if (n%2) {
    command=make_command(n-1,args+1);}
  else {
    command=make_command(n-2,args+2);
    opts=args[1];}
  if ((logops)||(flags&FD_MONGODB_LOGOPS)) {
    u8_log(-LOG_INFO,"MongoDB/DO","At %q: %q",arg,command);}
  if (FD_PRIM_TYPEP(arg,fd_mongoc_server)) 
    result=db_simple_command(arg,command,opts);
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_collection))
    result=collection_simple_command(arg,command,opts);
  else {}
  fd_decref(command);
  return result;
}

/* Cursor creation */

static fdtype mongodb_cursor(fdtype arg,fdtype query,fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain=(struct FD_MONGODB_COLLECTION *)arg;
  struct FD_MONGODB_DATABASE *db=DOMAIN2DB(domain);
  int flags=getflags(opts_arg,domain->domain_flags);
  fdtype opts=combine_opts(opts_arg,domain->domain_opts);
  mongoc_client_t *connection;
  mongoc_cursor_t *cursor=NULL;
  mongoc_collection_t *collection=open_collection(domain,&connection,flags);
  fdtype skip_arg=fd_getopt(opts,skipsym,FD_FIXZERO);
  fdtype limit_arg=fd_getopt(opts,limitsym,FD_FIXZERO);
  fdtype batch_arg=fd_getopt(opts,batchsym,FD_FIXZERO);
  bson_t *bq=fd_dtype2bson(query,flags,opts);
  bson_t *fields=get_projection(opts,flags);
  mongoc_read_prefs_t *rp=get_read_prefs(opts);
  if ((collection)&&
      (FD_FIXNUMP(skip_arg))&&
      (FD_FIXNUMP(limit_arg))&&
      (FD_FIXNUMP(batch_arg))) {
    cursor=mongoc_collection_find
      (collection,MONGOC_QUERY_NONE,
       FD_FIX2INT(skip_arg),FD_FIX2INT(limit_arg),FD_FIX2INT(batch_arg),
       bq,fields,rp);}
  if (cursor) {
    struct FD_MONGODB_CURSOR *consed=u8_alloc(struct FD_MONGODB_CURSOR);
    FD_INIT_CONS(consed,fd_mongoc_cursor);
    consed->cursor_domain=arg; fd_incref(arg);
    consed->cursor_db=domain->domain_db;
    fd_incref(domain->domain_db);
    consed->cursor_query=query; fd_incref(query);
    consed->cursor_query_bson=bq;
    consed->cursor_fields_bson=fields;
    consed->cursor_readprefs=rp;
    consed->cursor_connection=connection;
    consed->cursor_collection=collection;
    consed->mongoc_cursor=cursor;
   return (fdtype) consed;}
  else {
    fd_decref(opts);
    if (bq) bson_destroy(bq);
    if (collection) collection_done(collection,connection,domain);
    return FD_ERROR_VALUE;}
}
static void recycle_cursor(struct FD_CONS *c)
{
  struct FD_MONGODB_CURSOR *cursor= (struct FD_MONGODB_CURSOR *)c;
  struct FD_MONGODB_COLLECTION *domain= CURSOR2DOMAIN(cursor);
  struct FD_MONGODB_DATABASE *s= DOMAIN2DB(domain);
  mongoc_cursor_destroy(cursor->mongoc_cursor);
  mongoc_collection_destroy(cursor->cursor_collection);
  release_client(s,cursor->cursor_connection);
  fd_decref(cursor->cursor_domain);
  fd_decref(cursor->cursor_query);
  fd_decref(cursor->cursor_opts);
  bson_destroy(cursor->cursor_query_bson);
  if (cursor->cursor_fields_bson)
    bson_destroy(cursor->cursor_fields_bson);
  if (cursor->cursor_readprefs)
    mongoc_read_prefs_destroy(cursor->cursor_readprefs);
  u8_free(cursor);
}
static int unparse_cursor(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_MONGODB_CURSOR *cursor=(struct FD_MONGODB_CURSOR *)x;
  struct FD_MONGODB_COLLECTION *domain=CURSOR2DOMAIN(cursor);
  struct FD_MONGODB_DATABASE *db=DOMAIN2DB(domain);
  u8_printf(out,"#<MongoDB/Cursor '%s/%s' %q>",
            db->dbname,domain->collection_name,cursor->cursor_query);
  return 1;
}

/* Operations on cursors */

static fdtype mongodb_donep(fdtype cursor)
{
  struct FD_MONGODB_CURSOR *c=(struct FD_MONGODB_CURSOR *)cursor;
  if (mongoc_cursor_more(c->mongoc_cursor))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype mongodb_skip(fdtype cursor,fdtype howmany)
{
  struct FD_MONGODB_CURSOR *c=(struct FD_MONGODB_CURSOR *)cursor;
  int n=FD_FIX2INT(howmany), i=0; const bson_t *doc;
  while ((i<n)&&(mongoc_cursor_more(c->mongoc_cursor))) {
    mongoc_cursor_next(c->mongoc_cursor,&doc); i++;}
  if (i==n) return FD_TRUE; else return FD_FALSE;
}

static fdtype mongodb_read(fdtype cursor,fdtype howmany,fdtype opts_arg)
{
  struct FD_MONGODB_CURSOR *c=(struct FD_MONGODB_CURSOR *)cursor;
  int n=FD_FIX2INT(howmany), i=0;
  if (n==0) return FD_EMPTY_CHOICE;
  else {
    fdtype results=FD_EMPTY_CHOICE, *vec=NULL, opts=c->cursor_opts;
    mongoc_cursor_t *scan=c->mongoc_cursor; const bson_t *doc;
    int flags=c->cursor_flags; size_t n=0, vlen=0;
    int sorted=fd_testopt(opts,sortedsym,FD_VOID);
    if (!(FD_VOIDP(opts_arg))) {
      flags=getflags(opts_arg,c->cursor_flags);
      opts=combine_opts(opts_arg,opts);
      sorted=fd_testopt(opts,sortedsym,FD_VOID);}
    while ((i<n)&&(mongoc_cursor_next(scan,&doc))) {
      /* u8_string json=bson_as_json(doc,NULL); */
      fdtype r=fd_bson2dtype((bson_t *)doc,flags,opts);
      if (FD_ABORTP(r)) {
        fd_decref(results);
        free_dtype_vec(vec,i);
        results=FD_ERROR_VALUE;
        sorted=0;}
      else if (sorted) {
        if (n>=vlen) {
          if (!(grow_dtype_vec(&vec,n,&vlen))) {
            free_dtype_vec(vec,n);
            results=FD_ERROR_VALUE;
            sorted=0;
            break;}}
        vec[i]=r;}
      else {
        FD_ADD_TO_CHOICE(results,r);}
      i++;}
    if (!(FD_VOIDP(opts_arg))) fd_decref(opts);
    if (sorted) {
      if ((vec==NULL)||(n==0)) return fd_make_vector(0,NULL);
      else results=fd_make_vector(n,vec);
      if (vec) u8_free(vec);}
    return results;}
}

static fdtype mongodb_readvec(fdtype cursor,fdtype howmany,fdtype opts_arg)
{
  struct FD_MONGODB_CURSOR *c=(struct FD_MONGODB_CURSOR *)cursor;
  int n=FD_FIX2INT(howmany), i=0;
  if (n==0) return fd_make_vector(0,NULL);
  else {
    fdtype result=fd_make_vector(n,NULL);
    mongoc_cursor_t *scan=c->mongoc_cursor; const bson_t *doc;
    fdtype opts=c->cursor_opts;
    int flags=c->cursor_flags;
    if (!(FD_VOIDP(opts_arg))) {
      flags=getflags(opts_arg,c->cursor_flags);
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
  bson_t *out=b.bson_doc; int flags=b.bson_flags; bool ok=true;
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
    case fd_flonum_type: {
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
      unsigned long long millis=(fdt->fd_u8xtime.u8_tick*1000)+
        ((fdt->fd_u8xtime.u8_prec>u8_second)?(fdt->fd_u8xtime.u8_nsecs/1000000):(0));
      ok=bson_append_date_time(out,key,keylen,millis);
      break;}
    case fd_uuid_type: {
      struct FD_UUID *uuid=FD_GET_CONS(val,fd_uuid_type,struct FD_UUID *);
      ok=bson_append_binary(out,key,keylen,BSON_SUBTYPE_UUID,uuid->fd_uuid16,16);
      break;}
    case fd_choice_type: case fd_achoice_type: {
      struct FD_BSON_OUTPUT rout;
      bson_t arr; char buf[16];
      ok=bson_append_array_begin(out,key,keylen,&arr);
      memset(&rout,0,sizeof(struct FD_BSON_OUTPUT));
      rout.bson_doc=&arr; rout.bson_flags=b.bson_flags; 
      rout.bson_opts=b.bson_opts; rout.bson_fieldmap=b.bson_fieldmap;
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
      int i=0, lim=vec->fd_veclen;
      fdtype *data=vec->fd_vecelts;
      int wrap_vector=((flags&FD_MONGODB_CHOICEVALS)&&(key[0]!='$'));
      if (wrap_vector) {
        ok=bson_append_array_begin(out,key,keylen,&ch);
        if (ok) ok=bson_append_array_begin(&ch,"0",1,&arr);}
      else ok=bson_append_array_begin(out,key,keylen,&arr);
      memset(&rout,0,sizeof(struct FD_BSON_OUTPUT));
      rout.bson_doc=&arr; rout.bson_flags=b.bson_flags; 
      rout.bson_opts=b.bson_opts; rout.bson_fieldmap=b.bson_fieldmap;
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
      bson_t doc;
      fdtype keys=fd_getkeys(val);
      ok=bson_append_document_begin(out,key,keylen,&doc);
      memset(&rout,0,sizeof(struct FD_BSON_OUTPUT));
      rout.bson_doc=&doc; rout.bson_flags=b.bson_flags; 
      rout.bson_opts=b.bson_opts; rout.bson_fieldmap=b.bson_fieldmap;
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
      char opts[8], *write=opts; int flags=fdrx->fd_rxflags;
      if (flags&REG_EXTENDED) *write++='x';
      if (flags&REG_ICASE) *write++='i';
      if (flags&REG_NEWLINE) *write++='m';
      *write++='\0';
      bson_append_regex(out,key,keylen,fdrx->fd_rxsrc,opts);
      break;}
    case fd_compound_type: {
      struct FD_COMPOUND *compound=FD_XCOMPOUND(val);
      fdtype tag=compound->fd_typetag, *elts=FD_COMPOUND_ELTS(val);
      int len=FD_COMPOUND_LENGTH(val);
      struct FD_BSON_OUTPUT rout;
      bson_t doc;
      if (tag==mongovec_symbol)
        ok=bson_append_array_begin(out,key,keylen,&doc);
      else ok=bson_append_document_begin(out,key,keylen,&doc);
      memset(&rout,0,sizeof(struct FD_BSON_OUTPUT));
      rout.bson_doc=&doc; rout.bson_flags=b.bson_flags; 
      rout.bson_opts=b.bson_opts; rout.bson_fieldmap=b.bson_fieldmap;
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
                              vout.u8_write-vout.u8_outbuf);}}
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
                          vout.u8_write-vout.u8_outbuf);
      u8_close((u8_stream)&vout);
      return rv;}}
}

static bool bson_append_keyval(FD_BSON_OUTPUT b,fdtype key,fdtype val)
{
  int flags=b.bson_flags;
  struct U8_OUTPUT keyout; unsigned char buf[256];
  const char *keystring=NULL; int keylen; bool ok=true;
  fdtype fieldmap=b.bson_fieldmap, store_value=val;
  U8_INIT_OUTPUT_BUF(&keyout,256,buf);
  if (FD_VOIDP(val)) return 0;
  if (FD_SYMBOLP(key)) {
    if (flags&FD_MONGODB_SLOTIFY) {
      struct FD_KEYVAL *opmap=fd_sortvec_get(key,mongo_opmap,mongo_opmap_size);
      if (FD_EXPECT_FALSE(opmap!=NULL))  {
        if (FD_STRINGP(opmap->fd_keyval)) {
          fdtype mapped=opmap->fd_keyval;
          keystring=FD_STRDATA(mapped);
          keylen=FD_STRLEN(mapped);}}
      if (keystring==NULL) {
        u8_string pname=FD_SYMBOL_NAME(key);
        const u8_byte *scan=pname; int c;
        while ((c=u8_sgetc(&scan))>=0) {
          u8_putc(&keyout,u8_tolower(c));}
        keystring=keyout.u8_outbuf;
        keylen=keyout.u8_write-keyout.u8_outbuf;}}
    else {
      keystring=FD_SYMBOL_NAME(key);
      keylen=strlen(keystring);}
    if (!(FD_VOIDP(fieldmap))) {
      fdtype mapfn=fd_get(fieldmap,key,FD_VOID);
      if (FD_VOIDP(mapfn)) {}
      else if (FD_APPLICABLEP(mapfn))
        store_value=fd_apply(mapfn,1,&val);
      else if (FD_TABLEP(mapfn))
        store_value=fd_get(mapfn,val,FD_VOID);
      else if (FD_TRUEP(mapfn)) {
        struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
        fd_unparse(&out,val);
        store_value=fd_stream2string(&out);}
      else {}}}
  else if (FD_STRINGP(key)) {
    keystring=FD_STRDATA(key);
    if ((flags&FD_MONGODB_SLOTIFY)&&
        ((isdigit(keystring[0]))||
         (strchr(":(#@",keystring[0])!=NULL))) {
      u8_putc(&keyout,'\\'); u8_puts(&keyout,(u8_string)keystring);
      keystring=keyout.u8_outbuf;
      keylen=keyout.u8_write-keyout.u8_outbuf;}
    else keylen=FD_STRLEN(key);}
  else {
    keyout.u8_write=keyout.u8_outbuf;
    if ((flags&FD_MONGODB_SLOTIFY)&&
        (!((FD_OIDP(key))||(FD_VECTORP(key))||(FD_PAIRP(key)))))
      u8_putc(&keyout,':');
    fd_unparse(&keyout,key);
    keystring=keyout.u8_outbuf;
    keylen=keyout.u8_write-keyout.u8_outbuf;}
  if (store_value==val)
    ok=bson_append_dtype(b,keystring,keylen,val);
  else {
    ok=bson_append_dtype(b,keystring,keylen,store_value);
    fd_decref(store_value);}
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
    fdtype tag=compound->fd_typetag, *elts=FD_COMPOUND_ELTS(obj);
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
    out.bson_doc=bson_new();
    out.bson_flags=((flags<0)?(getflags(opts,FD_MONGODB_DEFAULTS)):(flags));
    out.bson_opts=opts;
    out.bson_fieldmap=fd_getopt(opts,fieldmap_symbol,FD_VOID);
    fd_bson_output(out,obj);
    fd_decref(out.bson_fieldmap);
    return out.bson_doc;}
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
  bson_iter_t *in=b.bson_iter; int flags=b.bson_flags, symbolized=0;
  const unsigned char *field=bson_iter_key(in);
  bson_type_t bt=bson_iter_type(in);
  fdtype slotid, value=FD_VOID;
  if ((flags&FD_MONGODB_SLOTIFY)&&(strchr(":(#@",field[0])!=NULL))
    slotid=fd_parse_arg((u8_string)field);
  else if (flags&FD_MONGODB_SLOTIFY) {
    int sc=slotcode((u8_string)field);
    if (sc<0) slotid=fd_make_string(NULL,-1,(unsigned char *)field);
    else if (sc==0) {
      slotid=fd_symbolize((unsigned char *)field);
      symbolized=1;}
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
      memcpy(uuid->fd_uuid16,data,len);
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
      memset(&dtoid,0,sizeof(dtoid));
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
    u8_init_xtime(&(ts->fd_u8xtime),millis/1000,u8_millisecond,
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
      r.bson_iter=&child; r.bson_flags=b.bson_flags; 
      r.bson_opts=b.bson_opts; r.bson_fieldmap=b.bson_fieldmap;
      value=fd_init_slotmap(NULL,0,NULL);
      while (bson_iter_next(&child))
        bson_read_step(r,value,NULL);
      if (fd_test(value,fdtag_symbol,FD_VOID)) {
        fdtype tag=fd_get(value,fdtag_symbol,FD_VOID), compound;
        struct FD_COMPOUND_TYPEINFO *entry=fd_lookup_compound(tag);
        fdtype fields[16], keys=fd_getkeys(value);
        int max=-1, i=0, n, ok=1; 
        while (i<16) fields[i++]=FD_VOID;
        {FD_DO_CHOICES(key,keys) {
            if (FD_FIXNUMP(key)) {
              int index=FD_FIX2INT(key);
              if ((index<0) || (index>=16)) {
                i=0; while (i<16) {
                  fdtype value=fields[i++];
                  fd_decref(value);}
                u8_log(LOG_WARN,fd_BSON_Compound_Overflow,
                       "Compound of type %q: %q",tag,value);
                FD_STOP_DO_CHOICES;
                ok=0;
                break;}
              if (index>max) max=index;
              fields[index]=fd_get(value,key,FD_VOID);}}}
        if (ok) {
          n=max+1;
          if ((entry)&&(entry->fd_compound_parser))
            compound=entry->fd_compound_parser(n,fields,entry);
          else {
            struct FD_COMPOUND *c=
              u8_malloc(sizeof(struct FD_COMPOUND)+(n*sizeof(fdtype)));
            fdtype *cdata=&(c->fd_elt0); fd_init_compound(c,tag,0,0);
            c->fd_n_elts=n;
            memcpy(cdata,fields,n);
            compound=FDTYPE_CONS(c);}
          fd_decref(value); 
          value=compound;}
        fd_decref(keys); fd_decref(tag);}}
    else if (BSON_ITER_HOLDS_ARRAY(in)) {
      int flags=b.bson_flags, choicevals=(flags&FD_MONGODB_CHOICEVALS);
      if ((choicevals)&&(symbolized))
        value=bson_read_choice(b);
      else value=bson_read_vector(b);}
    else {
      u8_log(LOGWARN,fd_BSON_Input_Error,
             "Can't handle BSON type %d",bt);
      return;}}
  if (!(FD_VOIDP(b.bson_fieldmap))) {
    struct FD_STRING _tempkey;
    fdtype tempkey=fd_init_string(&_tempkey,strlen(field),field);
    fdtype mapfn=FD_VOID, new_value=FD_VOID;
    fdtype fieldmap=b.bson_fieldmap;
    FD_INIT_STACK_CONS(tempkey,fd_string_type);
    mapfn=fd_get(fieldmap,tempkey,FD_VOID);
    if (FD_VOIDP(mapfn)) {}
    else if (FD_APPLICABLEP(mapfn)) 
      new_value=fd_apply(mapfn,1,&value);
    else if (FD_TABLEP(mapfn))
      new_value=fd_get(mapfn,value,FD_VOID);
    else if ((FD_TRUEP(mapfn))&&(FD_STRINGP(value))) 
      new_value=fd_parse(FD_STRDATA(value));
    if (FD_ABORTP(new_value)) {
      fd_clear_errors(1);
      new_value=FD_VOID;}
    else if (new_value!=FD_VOID) {
      fdtype old_value=value; 
      value=new_value;
      fd_decref(old_value);}
    else {}}
  if (!(FD_VOIDP(into))) fd_store(into,slotid,value);
  if (loc) *loc=value;
  else fd_decref(value);
}

static fdtype bson_read_vector(FD_BSON_INPUT b)
{
  struct FD_BSON_INPUT r; bson_iter_t child;
  fdtype result, *data=u8_alloc_n(16,fdtype), *write=data, *lim=data+16;
  bson_iter_recurse(b.bson_iter,&child);
  r.bson_iter=&child; r.bson_flags=b.bson_flags; 
  r.bson_opts=b.bson_opts; r.bson_fieldmap=b.bson_fieldmap;
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
  struct FD_BSON_INPUT r; bson_iter_t child;
  fdtype *data=u8_alloc_n(16,fdtype), *write=data, *lim=data+16;
  bson_iter_recurse(b.bson_iter,&child);
  r.bson_iter=&child; r.bson_flags=b.bson_flags; 
  r.bson_opts=b.bson_opts; r.bson_fieldmap=b.bson_fieldmap;
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
  bson_iter_t iter;
  if (flags<0) flags=getflags(opts,FD_MONGODB_DEFAULTS);
  memset(&iter,0,sizeof(bson_iter_t));
  if (bson_iter_init(&iter,in)) {
    fdtype result, fieldmap=fd_getopt(opts,fieldmap_symbol,FD_VOID);
    struct FD_BSON_INPUT b;
    memset(&b,0,sizeof(struct FD_BSON_INPUT));
    b.bson_iter=&iter; b.bson_flags=flags; 
    b.bson_opts=opts; b.bson_fieldmap=fieldmap;
    result=fd_init_slotmap(NULL,0,NULL);
    while (bson_iter_next(&iter)) bson_read_step(b,result,NULL);
    fd_decref(fieldmap);
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
    int i=0, skip=0; while (i<n) { 
      fdtype field=values[i++]; 
      fdtype value=values[i++]; 
      if ((FD_EMPTY_CHOICEP(field))||(FD_EMPTY_CHOICEP(value))) skip++;
      else {fd_incref(field); fd_incref(value);}}
    if (skip) {
      fdtype *reduced=u8_alloc_n(n-(skip*2),fdtype), result;
      int j=0; i=0; while (i<n) {
        fdtype field=values[i++]; 
        fdtype value=values[i++]; 
        if (!((FD_EMPTY_CHOICEP(field))||(FD_EMPTY_CHOICEP(value)))) {
          reduced[j++]=field; reduced[j++]=value;}}
      result=fd_init_compound_from_elts(NULL,mongomap_symbol,0,j,reduced);
      u8_free(reduced);
      return result;}
    else return fd_init_compound_from_elts
           (NULL,mongomap_symbol,0,n,values);}
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

static fdtype mongomapp(fdtype arg)
{
  if (FD_COMPOUND_TYPEP(arg,mongomap_symbol))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype mongovec_lexpr(int n,fdtype *values)
{
  int i=0; while (i<n) { 
    fdtype value=values[i++]; fd_incref(value);}
  return fd_init_compound_from_elts(NULL,mongovec_symbol,0,n,values);
}

static fdtype make_mongovec(fdtype vec)
{
  fdtype *elts=FD_VECTOR_ELTS(vec);
  int n=FD_VECTOR_LENGTH(vec), i=0;
  while (i<n) {fdtype v=elts[i++]; fd_incref(v);}
  return fd_init_compound_from_elts(NULL,mongovec_symbol,0,n,elts);
}

static fdtype mongovecp(fdtype arg)
{
  if (FD_COMPOUND_TYPEP(arg,mongomap_symbol))
    return FD_TRUE;
  else return FD_FALSE;
}

/* MongoDB pools and indices */

/* These are now implemented in Scheme */


/* The MongoDB OPMAP */

/* The OPMAP translates symbols that correspond to MongoDB
   operations. Since the default slot transformation lowercases the
   symbol name, this table only needs to include the ones that have
   embedded uppercase letters.  */

static void add_to_mongo_opmap(u8_string keystring)
{
  fdtype key=fd_symbolize(keystring);
  struct FD_KEYVAL *entry=
    fd_sortvec_insert(key,&mongo_opmap,
                      &mongo_opmap_size,
                      &mongo_opmap_space,
                      1);
  if (entry) 
    entry->fd_keyval=fdtype_string(keystring);
  else u8_log(LOG_WARN,"Couldn't add %s to the mongo opmap",keystring);
}

static void init_mongo_opmap()
{
  mongo_opmap=u8_alloc_n(32,struct FD_KEYVAL);
  mongo_opmap_space=32;
  mongo_opmap_size=0;
  add_to_mongo_opmap("$elemMatch");
  add_to_mongo_opmap("$ifNull");
  add_to_mongo_opmap("$setOnInsert");
  add_to_mongo_opmap("$currentDate");
  add_to_mongo_opmap("$indexStats");
  add_to_mongo_opmap("$addToSet");
  add_to_mongo_opmap("$setEquals");
  add_to_mongo_opmap("$setIntersection");
  add_to_mongo_opmap("$setUnion");
  add_to_mongo_opmap("$setDifference");
  add_to_mongo_opmap("$setIsSubset");
  add_to_mongo_opmap("$anyElementTrue");
  add_to_mongo_opmap("$allElementsTrue");
  add_to_mongo_opmap("$stdDevPop");
  add_to_mongo_opmap("$stdDevSamp");
  add_to_mongo_opmap("$toLower");
  add_to_mongo_opmap("$toUpper");
  add_to_mongo_opmap("$arrayElemAt");
  add_to_mongo_opmap("$concatArrays");
  add_to_mongo_opmap("$isArray");
  add_to_mongo_opmap("$dayOfYear");
  add_to_mongo_opmap("$dayOfMonth");
  add_to_mongo_opmap("$dayOfWeek");
  add_to_mongo_opmap("$pullAll");
  add_to_mongo_opmap("$pushAll");
  add_to_mongo_opmap("$comment");
  add_to_mongo_opmap("$geoNear");
  add_to_mongo_opmap("$geoWithin");
  add_to_mongo_opmap("$geoInserts");
  add_to_mongo_opmap("$nearSphere");
  add_to_mongo_opmap("$bitsAllSet");
  add_to_mongo_opmap("$bitsAllClear");
  add_to_mongo_opmap("$bitsAnySet");
  add_to_mongo_opmap("$bitsAnyClear");
  
  add_to_mongo_opmap("$maxScan");
  add_to_mongo_opmap("$maxTimeMS");
  add_to_mongo_opmap("$returnKey");
  add_to_mongo_opmap("$showDiskLoc");

}

/* Getting 'meta' information from connections */

static struct FD_MONGODB_DATABASE *getdb(fdtype arg,u8_context cxt)
{
  if (FD_PRIM_TYPEP(arg,fd_mongoc_server)) {
    return FD_GET_CONS(arg,fd_mongoc_server,struct FD_MONGODB_DATABASE *);}
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_collection)) {
    struct FD_MONGODB_COLLECTION *collection=
      FD_GET_CONS(arg,fd_mongoc_collection,struct FD_MONGODB_COLLECTION *);
    return (struct FD_MONGODB_DATABASE *)collection->domain_db;}
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_cursor)) {
    struct FD_MONGODB_CURSOR *cursor=
      FD_GET_CONS(arg,fd_mongoc_cursor,struct FD_MONGODB_CURSOR *);
    struct FD_MONGODB_COLLECTION *collection=CURSOR2DOMAIN(cursor);
    return DOMAIN2DB(collection);}
  else {
    fd_seterr(fd_TypeError,cxt,"MongoDB object",arg);
    return NULL;}
}

static fdtype mongodb_dbname(fdtype arg)
{
  struct FD_MONGODB_DATABASE *db=getdb(arg,"mongodb_dbname");
  if (db==NULL) return FD_ERROR_VALUE;
  else return fdtype_string(db->dbname);
}

static fdtype mongodb_spec(fdtype arg)
{
  struct FD_MONGODB_DATABASE *db=getdb(arg,"mongodb_spec");
  if (db==NULL) return FD_ERROR_VALUE;
  else return fdtype_string(db->dbspec);
}

static fdtype mongodb_uri(fdtype arg)
{
  struct FD_MONGODB_DATABASE *db=getdb(arg,"mongodb_uri");
  if (db==NULL) return FD_ERROR_VALUE;
  else return fdtype_string(db->dburi);
}

static fdtype mongodb_server(fdtype arg)
{
  struct FD_MONGODB_DATABASE *db=getdb(arg,"mongodb_uri");
  if (db==NULL) return FD_ERROR_VALUE;
  else {
    fdtype server=(fdtype)db;
    fd_incref(server);
    return server;}
}

static fdtype mongodb_opts(fdtype arg)
{
  fdtype opts=FD_VOID;
  if (FD_PRIM_TYPEP(arg,fd_mongoc_server)) {
    opts=(FD_GET_CONS(arg,fd_mongoc_server,struct FD_MONGODB_DATABASE *))->dbopts;}
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_collection)) {
    struct FD_MONGODB_COLLECTION *collection=
      FD_GET_CONS(arg,fd_mongoc_collection,struct FD_MONGODB_COLLECTION *);
    opts=collection->domain_opts;}
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_cursor)) {
    struct FD_MONGODB_CURSOR *cursor=
      FD_GET_CONS(arg,fd_mongoc_cursor,struct FD_MONGODB_CURSOR *);
    opts=cursor->cursor_opts;}
  else {
    fd_seterr(fd_TypeError,"mongodb_opts","MongoDB object",arg);
    return FD_ERROR_VALUE;}
  fd_incref(opts);
  return opts;
}

static fdtype mongodb_collection_name(fdtype arg)
{
  struct FD_MONGODB_COLLECTION *collection=NULL;
  if (FD_PRIM_TYPEP(arg,fd_mongoc_collection)) 
    collection=FD_GET_CONS(arg,fd_mongoc_collection,struct FD_MONGODB_COLLECTION *);
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_cursor)) {
    struct FD_MONGODB_CURSOR *cursor=
      FD_GET_CONS(arg,fd_mongoc_cursor,struct FD_MONGODB_CURSOR *);
    collection=(struct FD_MONGODB_COLLECTION *)cursor->cursor_domain;}
  else return fd_type_error("MongoDB collection/cursor","mongodb_dbname",arg);
  if (collection)
    return fd_make_string(NULL,-1,collection->collection_name);
  else return FD_FALSE;
}

static fdtype mongodb_getdb(fdtype arg)
{
  struct FD_MONGODB_COLLECTION *collection=NULL;
  if (FD_PRIM_TYPEP(arg,fd_mongoc_server)) 
    return fd_incref(arg);
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_collection)) 
    collection=FD_GET_CONS(arg,fd_mongoc_collection,struct FD_MONGODB_COLLECTION *);
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_cursor)) {
    struct FD_MONGODB_CURSOR *cursor=
      FD_GET_CONS(arg,fd_mongoc_cursor,struct FD_MONGODB_CURSOR *);
    collection=(struct FD_MONGODB_COLLECTION *)cursor->cursor_domain;}
  else return fd_type_error("MongoDB collection/cursor","mongodb_dbname",arg);
  if (collection)
    return fd_incref(collection->domain_db);
  else return FD_FALSE;
}

static int mongodb_getflags(fdtype arg)
{
  if (FD_PRIM_TYPEP(arg,fd_mongoc_server)) {
    struct FD_MONGODB_DATABASE *server=(struct FD_MONGODB_DATABASE *)arg;
    return server->dbflags;}
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_collection)) {
    struct FD_MONGODB_COLLECTION *collection=
      FD_GET_CONS(arg,fd_mongoc_collection,struct FD_MONGODB_COLLECTION *);
    return collection->domain_flags;}
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_cursor)) {
    struct FD_MONGODB_CURSOR *cursor=
      FD_GET_CONS(arg,fd_mongoc_cursor,struct FD_MONGODB_CURSOR *);
    return cursor->cursor_flags;}
  else return -1;
}

static fdtype mongodb_getcollection(fdtype arg)
{
  if (FD_PRIM_TYPEP(arg,fd_mongoc_collection)) 
    return fd_incref(arg);
  else if (FD_PRIM_TYPEP(arg,fd_mongoc_cursor)) {
    struct FD_MONGODB_CURSOR *cursor=
      FD_GET_CONS(arg,fd_mongoc_cursor,struct FD_MONGODB_CURSOR *);
    return fd_incref(cursor->cursor_domain);}
  else return fd_type_error("MongoDB collection/cursor","mongodb_dbname",arg);
}

static fdtype mongodbp(fdtype arg)
{
  if (FD_PRIM_TYPEP(arg,fd_mongoc_server))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype mongodb_collectionp(fdtype arg)
{
  if (FD_PRIM_TYPEP(arg,fd_mongoc_collection))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype mongodb_cursorp(fdtype arg)
{
  if (FD_PRIM_TYPEP(arg,fd_mongoc_cursor))
    return FD_TRUE;
  else return FD_FALSE;
}

static void add_string(fdtype result,fdtype field,u8_string value)
{
  if (value==NULL) return;
  else {
    fdtype stringval=fdtype_string(value);
    fd_add(result,field,stringval);
    fd_decref(stringval);
    return;}
}

static fdtype mongodb_getinfo(fdtype mongodb,fdtype field)
{
  fdtype result=fd_make_slotmap(10,0,NULL);
  struct FD_MONGODB_DATABASE *db=
    FD_GET_CONS(mongodb,fd_mongoc_server,struct FD_MONGODB_DATABASE *);
  mongoc_uri_t *info=db->dburi_info;
  u8_string tmpstring;
  if ((tmpstring=mongoc_uri_get_database(info)))
    add_string(result,dbname_symbol,tmpstring);
  if ((tmpstring=mongoc_uri_get_username(info)))
    add_string(result,username_symbol,tmpstring);
  if ((tmpstring=mongoc_uri_get_auth_mechanism(info)))
    add_string(result,auth_symbol,tmpstring);
  if ((tmpstring=mongoc_uri_get_auth_source(info)))
    add_string(result,auth_symbol,tmpstring);
  {
    const mongoc_host_list_t *scan=mongoc_uri_get_hosts(info);
    while (scan) {
      add_string(result,hosts_symbol,scan->host);
      add_string(result,connections_symbol,scan->host_and_port);
      scan=scan->next;}
  }
  if (mongoc_uri_get_ssl(info)) fd_store(result,sslsym,FD_TRUE);
  
  if ((FD_VOIDP(field))||(FD_FALSEP(field)))
    return result;
  else {
    fdtype v=fd_get(result,field,FD_EMPTY_CHOICE);
    fd_decref(result);
    return v;}
}

/* MongoDB logging */

static int getu8loglevel(mongoc_log_level_t l);
static int mongodb_loglevel=-1;
static int mongodb_ignore_loglevel=-1;

void mongoc_logger(mongoc_log_level_t l,const char *d,const char *m,void *u)
{
  int u8l=getu8loglevel(l);
  if (u8l<=LOG_CRIT)
    u8_logger(u8l,d,m);
  else if ((mongodb_loglevel >= 0)&&
           (u8l <= mongodb_loglevel))
    u8_logger(-u8l,d,m);
  else if ((mongodb_ignore_loglevel >= 0)&&
           (u8l > mongodb_ignore_loglevel))
    return;
  else u8_logger(u8l,d,m);
}

static int getu8loglevel(mongoc_log_level_t l)
{
  switch (l) {
  case MONGOC_LOG_LEVEL_ERROR:
    return LOG_ERR;
  case MONGOC_LOG_LEVEL_CRITICAL:
    return LOG_CRIT;
  case MONGOC_LOG_LEVEL_WARNING:
    return LOG_WARN;
  case MONGOC_LOG_LEVEL_MESSAGE:
    return LOG_NOTICE;
  case MONGOC_LOG_LEVEL_INFO:
    return LOG_INFO;
  case MONGOC_LOG_LEVEL_DEBUG:
    return LOG_DEBUG;
  case MONGOC_LOG_LEVEL_TRACE:
    return LOG_DETAIL;
  default:
    return LOG_WARN;}
}

/* Initialization */

FD_EXPORT int fd_init_mongodb(void) FD_LIBINIT_FN;
static long long int mongodb_initialized=0;

#define DEFAULT_FLAGS (FD_SHORT2DTYPE(FD_MONGODB_DEFAULTS))

FD_EXPORT int fd_init_mongodb()
{
  fdtype module;
  if (mongodb_initialized) return 0;
  mongodb_initialized=u8_millitime();

  init_mongo_opmap();

  module=fd_new_module("MONGODB",(FD_MODULE_SAFE));

  idsym=fd_intern("_ID");
  maxkey=fd_intern("mongomax");
  minkey=fd_intern("mongomin");
  oidtag=fd_intern("mongoid");
  mongofun=fd_intern("mongofun");
  mongouser=fd_intern("mongouser");
  mongomd5=fd_intern("md5hash");

  skipsym=fd_intern("SKIP");
  limitsym=fd_intern("LIMIT");
  batchsym=fd_intern("BATCH");
  sortsym=fd_intern("SORT");
  fieldssym=fd_intern("FIELDS");
  upsertsym=fd_intern("UPSERT");
  singlesym=fd_intern("SINGLE");
  wtimeoutsym=fd_intern("WTIMEOUT");
  writesym=fd_intern("WRITE");
  readsym=fd_intern("READ");
  returnsym=fd_intern("RETURN");
  originalsym=fd_intern("ORIGINAL");
  newsym=fd_intern("NEW");
  removesym=fd_intern("REMOVE");
  sortedsym=fd_intern("SORTED");
  softfailsym=fd_intern("SOFTFAIL");

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
  logopsym=fd_intern("LOGOPS");
  choices=fd_intern("CHOICES");
  nochoices=fd_intern("NOCHOICES");
  fieldmap_symbol=fd_intern("FIELDMAP");

  dbname_symbol=fd_intern("DBNAME");
  username_symbol=fd_intern("USERNAME");
  auth_symbol=fd_intern("AUTHENTICATION");
  hosts_symbol=fd_intern("HOSTS");
  connections_symbol=fd_intern("CONNECTIONS");
  fdtag_symbol=fd_intern("%FDTAG");

  primarysym=fd_intern("PRIMARY");
  primarypsym=fd_intern("PRIMARY+");
  secondarysym=fd_intern("SECONDARY");
  secondarypsym=fd_intern("SECONDARY+");
  nearestsym=fd_intern("NEAREST");

  poolmaxsym=fd_intern("POOLMAX");
  poolminsym=fd_intern("POOLMIN");

  fd_mongoc_server=fd_register_cons_type("MongoDB client");
  fd_mongoc_collection=fd_register_cons_type("MongoDB collection");
  fd_mongoc_cursor=fd_register_cons_type("MongoDB cursor");

  fd_type_names[fd_mongoc_server]="MongoDB server";
  fd_type_names[fd_mongoc_collection]="MongoDB collection";
  fd_type_names[fd_mongoc_cursor]="MongoDB cursor";

  fd_recyclers[fd_mongoc_server]=recycle_server;
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
  fd_idefn(module,fd_make_cprim4x("MONGODB/MODIFY",mongodb_modify,3,
                                  fd_mongoc_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID,-1,FD_VOID));
  fd_defalias(module,"MONGODB/MODIFY!","MONGODB/MODIFY");

  fd_idefn(module,fd_make_cprim3x("MONGODB/GET",mongodb_get,2,
                                  fd_mongoc_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID));

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
  fd_idefn(module,fd_make_ndprim(fd_make_cprimn("MONGOMAP",mongomap_lexpr,0)));
  fd_idefn(module,fd_make_cprim2x("->MONGOMAP",make_mongomap,2,
                                  -1,FD_VOID,fd_vector_type,FD_VOID));
  fd_idefn(module,fd_make_cprim1("MONGOMAP?",mongomapp,1));

  fd_idefn(module,fd_make_ndprim(fd_make_cprimn("MONGOVEC",mongovec_lexpr,0)));
  fd_idefn(module,fd_make_cprim1x("->MONGOVEC",make_mongovec,1,
                                  fd_vector_type,FD_VOID));
  fd_idefn(module,fd_make_cprim1("MONGOVEC?",mongovecp,1));

  fd_idefn(module,fd_make_cprim1x("MONGODB/NAME",mongodb_dbname,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("MONGODB/SPEC",mongodb_spec,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("MONGODB/URI",mongodb_uri,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("MONGODB/OPTS",mongodb_opts,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("MONGODB/SERVER",
                                  mongodb_server,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("MONGODB/GETCOLLECTION",
                                  mongodb_getcollection,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("COLLECTION/NAME",
                                  mongodb_collection_name,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("MONGODB/GETDB",mongodb_getdb,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim2x("MONGODB/INFO",mongodb_getinfo,1,
                                  fd_mongoc_server,FD_VOID,
                                  -1,FD_VOID));

  fd_idefn(module,fd_make_cprim1x("MONGODB?",mongodbp,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("MONGODB/COLLECTION?",
                                  mongodb_collectionp,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("MONGODB/CURSOR?",
                                  mongodb_cursorp,1,-1,FD_VOID));

  fd_register_config("MONGODB:FLAGS",
                     "Default flags (fixnum) for MongoDB/BSON processing",
                     fd_intconfig_get,fd_intconfig_set,&mongodb_defaults);

  fd_register_config("MONGODB:LOGCLIENT",
                     "Default flags (fixnum) for MongoDB/BSON processing",
                     fd_intconfig_get,fd_intconfig_set,&client_loglevel);
  fd_register_config("MONGODB:LOGOPS",
                     "Default flags (fixnum) for MongoDB/BSON processing",
                     fd_boolconfig_get,fd_boolconfig_set,&logops);

  fd_finish_module(module);
  fd_persist_module(module);

  mongoc_init();
  atexit(mongoc_cleanup);

  mongoc_log_set_handler(mongoc_logger,NULL);
  fd_register_config("MONGODB:LOGLEVEL",
                     "Controls log levels for which messages are always shown",
                     fd_intconfig_get,fd_intconfig_set,&mongodb_loglevel);
  fd_register_config("MONGODB:MAXLOG",
                     "Controls which log messages are always discarded",
                     fd_intconfig_get,fd_intconfig_set,
                     &mongodb_ignore_loglevel);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
