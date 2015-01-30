/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* mongodb.c
   This implements FramerD bindings to mongodb.
   Copyright (C) 2007-2013 beingmeta, inc.
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

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>

#include "framerd/mongodb.h"

/* Initialization */

u8_condition fd_MongoDB_Error=_("MongoDB error");
u8_condition fd_MongoDB_Warning=_("MongoDB warning");
u8_condition fd_BSON_Input_Error=_("BSON input error");

fd_ptr_type fd_mongo_client, fd_mongo_collection, fd_mongo_cursor;

static bool bson_append_keyval(bson_t *,int,fdtype,fdtype);
static bool bson_append_dtype(bson_t *,int,const char *,int,fdtype);
static fdtype idsym, maxkey, minkey;
static fdtype oidtag, mongofun, mongouser, mongomd5;

/* Consing MongoDB clients, collections, and cursors */

static fdtype mongodb_open(fdtype arg)
{
  char *uri; mongoc_client_t *client;
  if (FD_STRINGP(arg))
    uri=u8_strdup(FD_STRDATA(arg));
  else if (FD_SYMBOLP(arg)) {
    fdtype conf_val=fd_config_get(FD_SYMBOL_NAME(arg));
    if (FD_VOIDP(conf_val))
      return fd_type_error("MongoDB URI config","mongodb_open",arg);
    else if (FD_STRINGP(conf_val)) {
      uri=FD_STRDATA(conf_val); uri=u8_strdup(uri);
      fd_decref(conf_val);}
    else return fd_type_error("MongoDB URI config val",
                              FD_SYMBOL_NAME(arg),conf_val);}
  else return fd_type_error("MongoDB URI","mongodb_open",arg);
  client=mongoc_client_new(uri);
  if (client) {
    struct FD_MONGODB_CLIENT *cl=u8_alloc(struct FD_MONGODB_CLIENT);
    FD_INIT_CONS(cl,fd_mongo_client);
    cl->uri=u8_strdup(uri);
    cl->client=client;}
  else return fd_type_error("MongoDB client URI","mongodb_open",arg);
}
static void recycle_client(struct FD_CONS *c)
{
  struct FD_MONGODB_CLIENT *cl=(struct FD_MONGODB_CLIENT *)c;
  mongoc_client_destroy(cl->client);
  u8_free(c);
}
static int unparse_client(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_MONGODB_CLIENT *cl=(struct FD_MONGODB_CLIENT *)x;
  u8_printf(out,"#<MongoDB/CLIENT %s>",cl->uri);
  return 1;
}
static fdtype mongodb_collection(fdtype client,fdtype dbname,fdtype cname)
{
  struct FD_MONGODB_CLIENT *cl;
  mongoc_collection_t *coll;
  if (FD_PRIM_TYPEP(client,fd_mongo_client)) {
    cl=(struct FD_MONGODB_CLIENT *)client;
    fd_incref(client);}
  else {
    client=mongodb_open(client);
    if (FD_ABORTP(client)) return client;
    cl=(struct FD_MONGODB_CLIENT *)client;}
  coll=mongoc_client_get_collection
    (cl->client,FD_STRDATA(dbname),FD_STRDATA(cname));
  if (coll) {
    struct FD_MONGODB_COLLECTION *result=
      u8_alloc(struct FD_MONGODB_COLLECTION);
    FD_INIT_CONS(result,fd_mongo_collection);
    result->client=client;
    result->uri=u8_strdup(cl->uri);
    result->dbname=u8_strdup(FD_STRDATA(dbname));
    result->name=u8_strdup(FD_STRDATA(cname));
    result->collection=coll;
    return (fdtype) result;}
  else {
    fd_seterr("MongoDB/nocollection","mongodb_collection",
              u8_mkstring("%s>%s>%s",cl->uri,FD_STRDATA(dbname),
                          FD_STRDATA(cname)),
              client);
    return FD_ERROR_VALUE;}
}
static void recycle_collection(struct FD_CONS *c)
{
  struct FD_MONGODB_COLLECTION *cl=(struct FD_MONGODB_COLLECTION *)c;
  mongoc_collection_destroy(cl->collection);
  fd_decref(cl->client);
  u8_free(c);
}
static int unparse_collection(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_MONGODB_COLLECTION *cl=(struct FD_MONGODB_COLLECTION *)x;
  u8_printf(out,"#<MongoDB/COLLECTION '%s/%s' %s>",
            cl->dbname,cl->name,cl->uri);
  return 1;
}
static fdtype mongodb_cursor(fdtype coll,fdtype query)
{
  struct FD_MONGODB_COLLECTION *fc=(struct FD_MONGODB_COLLECTION *)coll;
  struct FD_MONGODB_CURSOR *consed=u8_alloc(struct FD_MONGODB_CURSOR);
  mongoc_collection_t *c=fc->collection;
  bson_t *q=fd_dtype2bson(query,0);
  mongoc_cursor_t *cursor;
  bson_error_t error;
  cursor=mongoc_collection_find(c,MONGOC_QUERY_NONE,0,0,0,q,NULL,NULL);
  FD_INIT_CONS(consed,fd_mongo_cursor);
  consed->collection=coll; fd_incref(coll);
  consed->query=query; fd_incref(query);
  consed->bsonquery=q;
  consed->cursor=cursor;
  return (fdtype) consed;
}
static void recycle_cursor(struct FD_CONS *c)
{
  struct FD_MONGODB_CURSOR *cr=(struct FD_MONGODB_CURSOR *)c;
  mongoc_cursor_destroy(cr->cursor);
  fd_decref(cr->collection); fd_decref(cr->query);
  bson_destroy(cr->bsonquery);
  u8_free(c);
}
static int unparse_cursor(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_MONGODB_CURSOR *cr=(struct FD_MONGODB_CURSOR *)x;
  struct FD_MONGODB_COLLECTION *cl=
    (struct FD_MONGODB_COLLECTION *)(cr->collection);
  u8_printf(out,"#<MongoDB/CURSOR '%s/%s' %q>",
            cl->dbname,cl->name,cr->query);
  return 1;
}

static fdtype mongodb_insert(fdtype coll,fdtype obj)
{
  struct FD_MONGODB_COLLECTION *c=(struct FD_MONGODB_COLLECTION *)coll;
  bson_error_t error;
  bson_t *doc=fd_dtype2bson(obj,0);
  if (mongoc_collection_insert
      (c->collection,MONGOC_INSERT_NONE,doc,NULL,&error)) {
    bson_destroy(doc);
    return FD_TRUE;}
  else {
    bson_destroy(doc); fd_incref(obj);
    fd_seterr(fd_MongoDB_Error,"mongodb_insert",
              u8_mkstring("%s>%s>%s:%s",
                          c->uri,c->dbname,c->name,
                          error.message),
              obj);
    return FD_ERROR_VALUE;}
}

static fdtype mongodb_delete(fdtype coll,fdtype obj)
{
  struct FD_MONGODB_COLLECTION *c=(struct FD_MONGODB_COLLECTION *)coll;
  bson_error_t error;
  bson_t *doc=bson_new();
  if (FD_TABLEP(obj)) {
    fdtype id=fd_get(obj,idsym,FD_VOID);
    if (FD_VOIDP(id))
      return fd_err("No MongoDB _id","mongodb_delete",NULL,obj);
    bson_append_dtype(doc,0,"_id",3,id);
    fd_decref(id);}
  else bson_append_dtype(doc,0,"_id",3,obj);
  if (mongoc_collection_remove
      (c->collection,MONGOC_DELETE_SINGLE_REMOVE,doc,NULL,&error)) {
    bson_destroy(doc);
    return FD_TRUE;}
  else {
    bson_destroy(doc); fd_incref(obj);
    fd_seterr(fd_MongoDB_Error,"mongodb_delete",
              u8_mkstring("%s>%s>%s:%s",
                          c->uri,c->dbname,c->name,
                          error.message),
              obj);
    return FD_ERROR_VALUE;}
}

static fdtype mongodb_update(fdtype coll,fdtype obj,fdtype id)
{
  struct FD_MONGODB_COLLECTION *c=(struct FD_MONGODB_COLLECTION *)coll;
  bson_error_t error;
  bson_t *query=bson_new(), *action;
  if (FD_VOIDP(id)) {
    if (FD_TABLEP(obj)) {
      id=fd_get(obj,idsym,FD_VOID);
      if (FD_VOIDP(id))
        return fd_err("No MongoDB _id","mongodb_update",NULL,obj);}}
  else fd_incref(id);
  bson_append_dtype(query,0,"_id",3,id);
  bson_append_dtype(action,0,"$set",4,obj);
  if (mongoc_collection_update
      (c->collection,MONGOC_UPDATE_NONE,query,action,NULL,&error)) {
    bson_destroy(query); bson_destroy(action);
    return FD_TRUE;}
  else {
    bson_destroy(query); bson_destroy(action); fd_incref(obj);
    fd_seterr(fd_MongoDB_Error,"mongodb_delete",
              u8_mkstring("%s>%s>%s(%q):%s",
                          c->uri,c->dbname,c->name,id,
                          error.message),
              obj);
    return FD_ERROR_VALUE;}
}

static fdtype mongodb_find(fdtype coll,fdtype query)
{
  struct FD_MONGODB_COLLECTION *fc=(struct FD_MONGODB_COLLECTION *)coll;
  mongoc_collection_t *c=fc->collection;
  fdtype results=FD_EMPTY_CHOICE;
  bson_t *q=fd_dtype2bson(query,0);
  const bson_t *doc;
  mongoc_cursor_t *cursor;
  bson_error_t error;
  cursor=mongoc_collection_find(c,MONGOC_QUERY_NONE,0,0,0,q,NULL,NULL);
  while (mongoc_cursor_next(cursor,&doc)) {
    /* u8_string json=bson_as_json(doc,NULL);
       fdtype r=fd_block_string(-1,json); */
    fdtype r=fd_bson2dtype((bson_t *)doc,0);
    FD_ADD_TO_CHOICE(results,r);
    /* bson_free(json); */}
  bson_destroy(q);
  mongoc_cursor_destroy(cursor);
  return results;
}

/* BSON output functions */

static bool bson_append_dtype(bson_t *out,int flags,
                              const char *key,int keylen,
                              fdtype val)
{
  bool ok=true;
  if (FD_CONSP(val)) {
    fd_ptr_type ctype=FD_PTR_TYPE(val);
    switch (ctype) {
    case fd_string_type:
      ok=bson_append_utf8(out,key,keylen,FD_STRDATA(val),FD_STRLEN(val));
      break;
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
      bson_t arr; char buf[16];
      ok=bson_append_array_begin(out,key,keylen,&arr);
      if (ok) {
        int i=0; FD_DO_CHOICES(v,val) {
          sprintf(buf,"%d",i++);
          ok=bson_append_dtype(out,flags,buf,strlen(buf),v);
          if (!(ok)) FD_STOP_DO_CHOICES;}}
      bson_append_array_end(out,&arr);
      break;}
    case fd_vector_type: case fd_rail_type: {
      struct FD_VECTOR *vec=(struct FD_VECTOR *)val;
      bson_t arr; char buf[16];
      int i=0, lim=vec->length;
      fdtype *data=vec->data;
      ok=bson_append_array_begin(out,key,keylen,&arr);
      if (ok) while (i<lim) {
          fdtype v=data[i]; sprintf(buf,"%d",i++);
          ok=bson_append_dtype(out,flags,buf,strlen(buf),v);
          if (!(ok)) break;}
      bson_append_array_end(out,&arr);
      break;}
    case fd_slotmap_type: case fd_hashtable_type: {
      bson_t doc; char buf[16];
      fdtype keys=fd_getkeys(val);
      ok=bson_append_document_begin(out,key,keylen,&doc);
      if (ok) {
        FD_DO_CHOICES(key,keys) {
          fdtype value=fd_get(val,key,FD_VOID);
          if (!(FD_VOIDP(value))) {
            ok=bson_append_keyval(&doc,flags,key,value);
            fd_decref(value);
            if (!(ok)) FD_STOP_DO_CHOICES;}}}
      fd_decref(keys);
      bson_append_document_end(out,&doc);
      break;}
    default: break;}
    return ok;}
  else if (FD_FIXNUMP(val))
    return bson_append_int32(out,key,keylen,FD_FIX2INT(val));
  else if (FD_OIDP(val)) {
    unsigned char bytes[12];
    FD_OID addr=FD_OID_ADDR(val); bson_oid_t oid;
    unsigned int hi=FD_OID_HI(addr), lo=FD_OID_LO(addr);
    bytes[11]=bytes[10]=bytes[9]=bytes[8]=0;
    bytes[7]=((hi>>24)&0xFF); bytes[6]=((hi>>16)&0xFF);
    bytes[5]=((hi>>8)&0xFF); bytes[4]=(hi&0xFF);
    bytes[3]=((lo>>24)&0xFF); bytes[2]=((lo>>16)&0xFF);
    bytes[1]=((lo>>8)&0xFF); bytes[0]=(lo&0xFF);
    bson_oid_init_from_data(&oid,bytes);
    return bson_append_oid(out,key,keylen,&oid);}
  else if (FD_SYMBOLP(val)) 
    return bson_append_utf8
      (out,key,keylen,FD_SYMBOL_NAME(val),-1);
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
    default: return true;}
}

static bool bson_append_keyval(bson_t *out,int flags,fdtype key,fdtype val)
{
  struct U8_OUTPUT keyout; unsigned char buf[256];
  const char *keystring; int keylen; bool ok=true;
  U8_INIT_OUTPUT_BUF(&keyout,256,buf);
  if (FD_VOIDP(val)) return;
  if (FD_SYMBOLP(key)) {
    u8_string pname=FD_SYMBOL_NAME(key);
    u8_byte *scan=pname; int c;
    while ((c=u8_sgetc(&scan))>=0) {
      u8_putc(&keyout,u8_tolower(c));}
    keystring=keyout.u8_outbuf;
    keylen=keyout.u8_outptr-keyout.u8_outbuf;}
  else if (FD_STRINGP(key)) {
    keystring=FD_STRDATA(key);
    keylen=FD_STRLEN(key);}
  else {
    keyout.u8_outptr=keyout.u8_outbuf;
    u8_putc(&keyout,':');
    fd_unparse(&keyout,key);
    keystring=keyout.u8_outbuf;
    keylen=keyout.u8_outptr-keyout.u8_outbuf;}
  ok=bson_append_dtype(out,flags,keystring,keylen,val);
  u8_close((u8_stream)&keyout);
  return ok;
}

FD_EXPORT fdtype fd_bson_write(bson_t *out,int flags,fdtype in)
{
  fdtype keys=fd_getkeys(in);
  {FD_DO_CHOICES(key,keys) {
      fdtype val=fd_get(in,key,FD_VOID);
      bson_append_keyval(out,flags,key,val);
      fd_decref(val);}}
  fd_decref(keys);
  return FD_VOID;
}

FD_EXPORT bson_t *fd_dtype2bson(fdtype in,int flags)
{
  bson_t *doc;
  doc = bson_new ();
  fd_bson_write(doc,flags,in);
  return doc;
}

/* BSON input functions */

static int bson_reader(fdtype into,bson_iter_t *in,int flags)
{
  if (bson_iter_next(in)) {
    const unsigned char *field=bson_iter_key(in);
    bson_type_t bt=bson_iter_type(in);
    fdtype slotid=((field[0]=='_')||(field[0]=='$'))?
      (fd_symbolize((unsigned char *)field)):
      (fd_block_string(-1,(unsigned char *)field));
    fdtype value=FD_VOID;
    switch (bt) {
    case BSON_TYPE_DOUBLE:
      value=fd_make_double(bson_iter_double(in)); break;
    case BSON_TYPE_BOOL:
      value=(bson_iter_bool(in))?(FD_TRUE):(FD_FALSE); break;
    case BSON_TYPE_UTF8: {
      int len; const unsigned char *bytes=bson_iter_utf8(in,&len);
      value=fd_block_string(len,(unsigned char *)bytes);
      break;}
    case BSON_TYPE_BINARY: {
      int len; bson_subtype_t st; const unsigned char *data;
      bson_iter_binary(in,&st,&len,&data);
      if (st==BSON_SUBTYPE_UUID) {
        struct FD_UUID *uuid=u8_alloc(struct FD_UUID);
        FD_INIT_CONS(uuid,fd_uuid_type);
        memcpy(uuid->uuid,data,len);
        value= (fdtype) uuid;}
      else if (st==BSON_SUBTYPE_BINARY)
        value=fd_make_packet(NULL,len,(unsigned char *)data);
      /* Probably should really make a record here */
      else {
        fdtype packet=fd_make_packet(NULL,len,(unsigned char *)data);
        if (st==BSON_SUBTYPE_USER)
          value=fd_init_compound(NULL,mongouser,0,1,packet);
        else if (st==BSON_SUBTYPE_MD5)
          value=fd_init_compound(NULL,mongomd5,0,1,packet);
        else if (st==BSON_SUBTYPE_FUNCTION)
          value=fd_init_compound(NULL,mongofun,0,1,packet);
        else value=packet;}
      break;}
    case BSON_TYPE_INT32:
      value=FD_INT2DTYPE(bson_iter_int32(in));
      break;
    case BSON_TYPE_INT64:
      value=FD_INT2DTYPE(bson_iter_int64(in));
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
      value=(fdtype)ts;}
    case BSON_TYPE_MAXKEY:
      value=maxkey; break;
    case BSON_TYPE_MINKEY:
      value=minkey; break;
    case BSON_TYPE_DOCUMENT: case BSON_TYPE_ARRAY: {
      bson_iter_t child;
      value=fd_init_slotmap(NULL,0,NULL);
      bson_iter_recurse(in,&child);
      while (bson_reader(value,&child,flags)) {}
      return 1;}
    default: {
      u8_log(LOGWARN,fd_BSON_Input_Error,
             "Can't handle BSON type %d",bt);
      return 1;}}
    fd_store(into,slotid,value);
    fd_decref(value);
    return 1;}
  else return 0;
}

FD_EXPORT fdtype fd_bson2dtype(bson_t *in,int flags)
{
  bson_iter_t iter; fdtype doc;
  if (bson_iter_init(&iter,in)) {
    doc=fd_init_slotmap(NULL,0,NULL);
    while (bson_reader(doc,&iter,flags)) {}
    return doc;}
  else return fd_err(fd_BSON_Input_Error,"fd_bson2dtype",NULL,FD_VOID);
}

/* Initialization */

FD_EXPORT int fd_init_mongodb(void) FD_LIBINIT_FN;
static int mongodb_initialized=0;

FD_EXPORT int fd_init_mongodb()
{
  fdtype module;
  if (mongodb_initialized) return 0;
  mongodb_initialized=1;

  module=fd_new_module("MONGODB",(FD_MODULE_SAFE));

  idsym=fd_intern("_ID");
  maxkey=fd_register_constant("mongomax");
  minkey=fd_register_constant("mongomin");
  oidtag=fd_register_constant("mongoid");
  mongofun=fd_register_constant("mongofun");
  mongouser=fd_register_constant("mongouser");
  mongomd5=fd_register_constant("md5hash");

  fd_mongo_client=fd_register_cons_type("MongoDB client");
  fd_mongo_collection=fd_register_cons_type("MongoDB collection");
  fd_mongo_cursor=fd_register_cons_type("MongoDB cursor");

  fd_type_names[fd_mongo_client]="MongoDB client";
  fd_type_names[fd_mongo_collection]="MongoDB collection";
  fd_type_names[fd_mongo_cursor]="MongoDB cursor";

  fd_recyclers[fd_mongo_client]=recycle_client;
  fd_recyclers[fd_mongo_collection]=recycle_collection;
  fd_recyclers[fd_mongo_cursor]=recycle_cursor;

  fd_unparsers[fd_mongo_client]=unparse_client;
  fd_unparsers[fd_mongo_collection]=unparse_collection;
  fd_unparsers[fd_mongo_cursor]=unparse_cursor;

  fd_idefn(module,fd_make_cprim1x("MONGODB/OPEN",mongodb_open,1,
                                  fd_string_type,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("MONGODB/COLLECTION",mongodb_collection,1,
                                  -1,FD_VOID,fd_string_type,FD_VOID,
                                  fd_string_type,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("MONGODB/COLLECTION",mongodb_collection,3,
                                  -1,FD_VOID,fd_string_type,FD_VOID,
                                  fd_string_type,FD_VOID));
  fd_idefn(module,fd_make_cprim2x("MONGODB/INSERT!",mongodb_insert,2,
                                  fd_mongo_collection,FD_VOID,
                                  -1,FD_VOID));
  fd_idefn(module,fd_make_cprim2x("MONGODB/DELETE!",mongodb_delete,2,
                                  fd_mongo_collection,FD_VOID,
                                  -1,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("MONGODB/UPDATE!",mongodb_update,2,
                                  fd_mongo_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim2x("MONGODB/FIND",mongodb_find,2,
                                  fd_mongo_collection,FD_VOID,
                                  -1,FD_VOID));

  fd_finish_module(module);
  fd_persist_module(module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
