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

#include "framerd/fdregex.h"
#include "framerd/mongodb.h"

/* Initialization */

u8_condition fd_MongoDB_Error=_("MongoDB error");
u8_condition fd_MongoDB_Warning=_("MongoDB warning");
u8_condition fd_BSON_Input_Error=_("BSON input error");

fd_ptr_type fd_mongo_client, fd_mongo_collection, fd_mongo_cursor;

static bool bson_append_keyval(struct FD_BSON_OUTPUT,fdtype,fdtype);
static bool bson_append_dtype(struct FD_BSON_OUTPUT,const char *,int,fdtype);
static fdtype idsym, maxkey, minkey;
static fdtype oidtag, mongofun, mongouser, mongomd5;
static fdtype bsonflags, raw, slotify, slotifyin, slotifyout;
static fdtype colonize, colonizein, colonizeout, choices, nochoices;

int mongodb_defaults=FD_MONGODB_DEFAULTS;

static int getbsonflags(fdtype opts,int dflt)
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
      int flags=getbsonflags(flagsv,dflt);
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

static fdtype mongodb_open(fdtype arg,fdtype opts)
{
  char *uri; mongoc_client_t *client; int flags;
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
  flags=getbsonflags(opts,mongodb_defaults);
  client=mongoc_client_new(uri);
  if (client) {
    struct FD_MONGODB_CLIENT *cl=u8_alloc(struct FD_MONGODB_CLIENT);
    FD_INIT_CONS(cl,fd_mongo_client);
    cl->uri=u8_strdup(uri);
    cl->client=client;
    cl->opts=opts; fd_incref(opts);
    if (FD_VOIDP(flags)) cl->flags=FD_MONGODB_DEFAULTS;
    else cl->flags=FD_FIX2INT(flags);
    return (fdtype)cl;}
  else return fd_type_error("MongoDB client URI","mongodb_open",arg);
}
static void recycle_client(struct FD_CONS *c)
{
  struct FD_MONGODB_CLIENT *cl=(struct FD_MONGODB_CLIENT *)c;
  mongoc_client_destroy(cl->client);
  fd_decref(cl->opts);
  u8_free(c);
}
static int unparse_client(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_MONGODB_CLIENT *cl=(struct FD_MONGODB_CLIENT *)x;
  u8_printf(out,"#<MongoDB/CLIENT %s>",cl->uri);
  return 1;
}
static fdtype mongodb_collection(fdtype client,fdtype dbname,fdtype cname,
                                 fdtype opts)
{
  struct FD_MONGODB_CLIENT *cl; fdtype clopts;
  mongoc_collection_t *coll;
  if (FD_PRIM_TYPEP(client,fd_mongo_client)) {
    cl=(struct FD_MONGODB_CLIENT *)client;
    fd_incref(client);}
  else {
    client=mongodb_open(client,opts);
    if (FD_ABORTP(client)) return client;
    cl=(struct FD_MONGODB_CLIENT *)client;}
  coll=mongoc_client_get_collection
    (cl->client,FD_STRDATA(dbname),FD_STRDATA(cname));
  clopts=cl->opts;
  if (coll) {
    struct FD_MONGODB_COLLECTION *result=
      u8_alloc(struct FD_MONGODB_COLLECTION);
    FD_INIT_CONS(result,fd_mongo_collection);
    result->client=client;
    result->uri=u8_strdup(cl->uri);
    result->dbname=u8_strdup(FD_STRDATA(dbname));
    result->name=u8_strdup(FD_STRDATA(cname));
    result->collection=coll;
    result->opts=combine_opts(opts,clopts);
    result->flags=getbsonflags(opts,cl->flags);
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
  fd_decref(cl->client); fd_decref(cl->opts);
  u8_free(c);
}
static int unparse_collection(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_MONGODB_COLLECTION *cl=(struct FD_MONGODB_COLLECTION *)x;
  u8_printf(out,"#<MongoDB/COLLECTION '%s/%s' %s>",
            cl->dbname,cl->name,cl->uri);
  return 1;
}
static fdtype mongodb_cursor(fdtype coll,fdtype query,fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *fc=(struct FD_MONGODB_COLLECTION *)coll;
  struct FD_MONGODB_CURSOR *consed=u8_alloc(struct FD_MONGODB_CURSOR);
  mongoc_collection_t *c=fc->collection;
  bson_t *q; mongoc_cursor_t *cursor; bson_error_t error;
  int flags=getbsonflags(opts_arg,fc->flags);
  fdtype opts=combine_opts(opts_arg,fc->opts);
  q=fd_dtype2bson(query,flags,opts);
  cursor=mongoc_collection_find(c,MONGOC_QUERY_NONE,0,0,0,q,NULL,NULL);
  FD_INIT_CONS(consed,fd_mongo_cursor);
  consed->collection=coll; fd_incref(coll);
  consed->query=query; fd_incref(query);
  consed->opts=opts; consed->flags=flags;
  consed->bsonquery=q;
  consed->cursor=cursor;
  return (fdtype) consed;
}
static void recycle_cursor(struct FD_CONS *c)
{
  struct FD_MONGODB_CURSOR *cr=(struct FD_MONGODB_CURSOR *)c;
  mongoc_cursor_destroy(cr->cursor);
  fd_decref(cr->collection); fd_decref(cr->query); fd_decref(cr->opts);
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

static fdtype mongodb_insert(fdtype coll,fdtype obj,fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *c=(struct FD_MONGODB_COLLECTION *)coll;
  bson_error_t error; bson_t *doc;
  int flags=getbsonflags(opts_arg,c->flags);
  fdtype opts=combine_opts(opts_arg,c->opts);
  doc=fd_dtype2bson(obj,flags,opts);
  if (mongoc_collection_insert
      (c->collection,MONGOC_INSERT_NONE,doc,NULL,&error)) {
    bson_destroy(doc); fd_decref(opts);
    return FD_TRUE;}
  else {
    bson_destroy(doc); fd_incref(obj); fd_decref(opts);
    fd_seterr(fd_MongoDB_Error,"mongodb_insert",
              u8_mkstring("%s>%s>%s:%s",
                          c->uri,c->dbname,c->name,
                          error.message),
              obj);
    return FD_ERROR_VALUE;}
}

static fdtype mongodb_delete(fdtype coll,fdtype obj,fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *c=(struct FD_MONGODB_COLLECTION *)coll;
  struct FD_BSON_OUTPUT q; bson_error_t error;
  int flags=getbsonflags(opts_arg,c->flags);
  fdtype opts=combine_opts(opts_arg,c->opts);
  q.doc=bson_new(); q.opts=opts; q.flags=flags;
  if (FD_TABLEP(obj)) {
    fdtype id=fd_get(obj,idsym,FD_VOID);
    if (FD_VOIDP(id))
      return fd_err("No MongoDB _id","mongodb_delete",NULL,obj);
    bson_append_dtype(q,"_id",3,id);
    fd_decref(id);}
  else bson_append_dtype(q,"_id",3,obj);
  if (mongoc_collection_remove
      (c->collection,MONGOC_DELETE_SINGLE_REMOVE,q.doc,NULL,&error)) {
    bson_destroy(q.doc); fd_decref(opts);
    return FD_TRUE;}
  else {
    bson_destroy(q.doc); fd_incref(obj); fd_decref(opts);
    fd_seterr(fd_MongoDB_Error,"mongodb_delete",
              u8_mkstring("%s>%s>%s:%s",
                          c->uri,c->dbname,c->name,
                          error.message),
              obj);
    return FD_ERROR_VALUE;}
}

static fdtype mongodb_update(fdtype coll,fdtype obj,fdtype id,fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *c=(struct FD_MONGODB_COLLECTION *)coll;
  struct FD_BSON_OUTPUT q, a; bson_error_t error;
  int flags=getbsonflags(opts_arg,c->flags);
  fdtype opts=combine_opts(opts_arg,c->opts);
  q.doc=bson_new(); q.opts=opts; q.flags=flags;
  a.doc=bson_new(); a.opts=opts; a.flags=flags;
  if ((FD_VOIDP(id))||(FD_FALSEP(id))||(FD_DEFAULTP(id))) {
    if (FD_TABLEP(obj)) {
      id=fd_get(obj,idsym,FD_VOID);
      if (FD_VOIDP(id))
        return fd_err("No MongoDB _id","mongodb_update",NULL,obj);}}
  else fd_incref(id);
  bson_append_dtype(q,"_id",3,id);
  bson_append_dtype(a,"$set",4,obj);
  if (mongoc_collection_update
      (c->collection,MONGOC_UPDATE_NONE,q.doc,a.doc,NULL,&error)) {
    bson_destroy(q.doc); bson_destroy(a.doc); fd_decref(opts);
    return FD_TRUE;}
  else {
    bson_destroy(q.doc); bson_destroy(a.doc);
    fd_incref(obj); fd_decref(opts);
    fd_seterr(fd_MongoDB_Error,"mongodb_delete",
              u8_mkstring("%s>%s>%s(%q):%s",
                          c->uri,c->dbname,c->name,id,
                          error.message),
              obj);
    return FD_ERROR_VALUE;}
}

static fdtype mongodb_find(fdtype coll,fdtype query,fdtype opts_arg)
{
  struct FD_MONGODB_COLLECTION *c=(struct FD_MONGODB_COLLECTION *)coll;
  fdtype results=FD_EMPTY_CHOICE;
  bson_t *q; const bson_t *doc; mongoc_cursor_t *cursor; bson_error_t error;
  int flags=getbsonflags(opts_arg,c->flags);
  fdtype opts=combine_opts(opts_arg,c->opts);
  q=fd_dtype2bson(query,opts,flags);
  cursor=mongoc_collection_find
    (c->collection,MONGOC_QUERY_NONE,0,0,0,q,NULL,NULL);
  while (mongoc_cursor_next(cursor,&doc)) {
    /* u8_string json=bson_as_json(doc,NULL); */
    fdtype r=fd_bson2dtype((bson_t *)doc,flags,opts);
    FD_ADD_TO_CHOICE(results,r);}
  bson_destroy(q); fd_decref(opts);
  mongoc_cursor_destroy(cursor);
  return results;
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
         buf[0]='\\'; strncpy(buf+1,str,len+1);
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
       if (flags&&FD_MONGODB_CHOICEVALS) {
         ok=bson_append_array_begin(out,key,keylen,&ch);
         if (ok) ok=bson_append_array_begin(&ch,"0",1,&arr);}
       else ok=bson_append_array_begin(out,key,keylen,&arr);
       memset(&rout,0,sizeof(struct FD_BSON_OUTPUT));
       rout.doc=&arr; rout.flags=b.flags; rout.opts=b.opts;
       if (ok) while (i<lim) {
           fdtype v=data[i]; sprintf(buf,"%d",i++);
           ok=bson_append_dtype(rout,buf,strlen(buf),v);
           if (!(ok)) break;}
       if (flags&&FD_MONGODB_CHOICEVALS) {
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
      u8_byte *scan=pname; int c;
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
  fdtype keys=fd_getkeys(obj);
  {FD_DO_CHOICES(key,keys) {
      fdtype val=fd_get(obj,key,FD_VOID);
      bson_append_keyval(out,key,val);
      fd_decref(val);}}
  fd_decref(keys);
  return FD_VOID;
}

FD_EXPORT bson_t *fd_dtype2bson(fdtype obj,int flags,fdtype opts)
{
  struct FD_BSON_OUTPUT out;
  out.doc=bson_new();
  out.flags=((flags<0)?(getbsonflags(opts,FD_MONGODB_DEFAULTS)):(flags));
  out.opts=opts;
  fd_bson_output(out,obj);
  return out.doc;
}

/* BSON input functions */

#define slotify_char(c) \
    ((c=='_')||(c=='-')||(c=='.')||(c=='/')||(c=='$')||(u8_isalnum(c)))

/* -1 means don't slotify at all, 0 means symbolize, 1 means intern */
static int slotcode(u8_string s)
{
  u8_byte *scan=s; int c, i=0, isupper=0;
  while ((c=u8_sgetc(&scan))>=0) {
    if (i>32) return -1;
    if (!(slotify_char(c))) return -1; else i++;
    if (u8_isupper(c)) isupper=1;}
  return isupper;
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
    if ((flags&FD_MONGODB_COLONIZE)&&(strchr(":(#@",field[0])!=NULL))
      value=fd_parse_arg((u8_string)(bytes+1));
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
    if (BSON_ITER_HOLDS_ARRAY(&child)) {
      *write++=bson_read_vector(r);}
    else {
      bson_read_step(r,FD_VOID,write);
      write++;}}
  return fd_make_choice
    (write-data,data,
     FD_CHOICE_DOSORT|FD_CHOICE_COMPRESS|
     FD_CHOICE_FREEDATA|FD_CHOICE_REALLOC);
  return result;
}

FD_EXPORT fdtype fd_bson2dtype(bson_t *in,int flags,fdtype opts)
{
  bson_iter_t iter; fdtype result;
  if (flags<0) flags=getbsonflags(opts,FD_MONGODB_DEFAULTS);
  memset(&iter,0,sizeof(bson_iter_t));
  if (bson_iter_init(&iter,in)) {
    struct FD_BSON_INPUT b; memset(&b,0,sizeof(struct FD_BSON_INPUT));
    b.iter=&iter; b.flags=flags; b.opts=opts;
    result=fd_init_slotmap(NULL,0,NULL);
    while (bson_iter_next(&iter)) bson_read_step(b,result,NULL);
    return result;}
  else return fd_err(fd_BSON_Input_Error,"fd_bson2dtype",NULL,FD_VOID);
}

/* Initialization */

FD_EXPORT int fd_init_mongodb(void) FD_LIBINIT_FN;
static int mongodb_initialized=0;

#define DEFAULT_FLAGS (FD_SHORT2DTYPE(FD_MONGODB_DEFAULTS))

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

  fd_idefn(module,fd_make_cprim2x("MONGODB/OPEN",mongodb_open,1,
                                  fd_string_type,FD_VOID,
                                  fd_fixnum_type,DEFAULT_FLAGS));
  fd_idefn(module,fd_make_cprim4x("MONGODB/COLLECTION",mongodb_collection,1,
                                  -1,FD_VOID,fd_string_type,FD_VOID,
                                  fd_string_type,FD_VOID,
                                  fd_fixnum_type,DEFAULT_FLAGS));
  fd_idefn(module,fd_make_cprim3x("MONGODB/CURSOR",mongodb_cursor,2,
                                  -1,FD_VOID,-1,FD_VOID,
                                  fd_fixnum_type,DEFAULT_FLAGS));
  fd_idefn(module,fd_make_cprim3x("MONGODB/INSERT!",mongodb_insert,2,
                                  fd_mongo_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("MONGODB/DELETE!",mongodb_delete,2,
                                  fd_mongo_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim4x("MONGODB/UPDATE!",mongodb_update,2,
                                  fd_mongo_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID,
                                  -1,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("MONGODB/FIND",mongodb_find,2,
                                  fd_mongo_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID,
                                  -1,FD_VOID));

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
