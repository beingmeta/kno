/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* mongodb.c
   This implements FramerD bindings to mongodb.
   Copyright (C) 2007-2018 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

static int mongodb_loglevel;
#define U8_LOGLEVEL mongodb_loglevel

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/compounds.h"
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

#include <math.h>

/* Initialization */

u8_condition fd_MongoDB_Error=_("MongoDB error");
u8_condition fd_BSON_Error=_("BSON conversion error");
u8_condition fd_MongoDB_Warning=_("MongoDB warning");
u8_condition fd_BSON_Input_Error=_("BSON input error");
u8_condition fd_BSON_Compound_Overflow=_("BSON/FramerD compound overflow");

#define FD_FIND_MATCHES  1
#define FD_COUNT_MATCHES 0

static lispval sslsym, smoketest_sym;
static int mongodb_loglevel = LOG_NOTICE;
static int logops = 0;

static int default_ssl = 0;
static u8_string default_cafile = NULL;
static u8_string default_cadir = NULL;
static u8_string default_certfile = NULL;

static lispval dbname_symbol, username_symbol, auth_symbol, fdtag_symbol;
static lispval hosts_symbol, connections_symbol, fieldmap_symbol, logopsym;
static lispval fdparse_symbol;

/* The mongo_opmap translates symbols for mongodb operators (like
   $addToSet) into the correctly capitalized strings to use in
   operations. */
static struct FD_KEYVAL *mongo_opmap = NULL;
static int mongo_opmap_size = 0, mongo_opmap_space = 0;
#ifndef MONGO_OPMAP_MAX
#define MONGO_OPMAP_MAX 8000
#endif

#define MONGO_VECSLOTS_MAX 2032
static lispval vecslots[MONGO_VECSLOTS_MAX];
static int n_vecslots = 0;
static u8_mutex vecslots_lock;

/* These are the new cons types introducted for mongodb */
fd_ptr_type fd_mongoc_server, fd_mongoc_collection, fd_mongoc_cursor;

static bool bson_append_keyval(struct FD_BSON_OUTPUT,lispval,lispval);
static bool bson_append_dtype(struct FD_BSON_OUTPUT,const char *,int,
                              lispval,int);
static lispval idsym, maxkey, minkey;
static lispval oidtag, mongofun, mongouser, mongomd5;
static lispval bsonflags, raw, slotify, slotifyin, slotifyout, softfailsym;
static lispval colonize, colonizein, colonizeout, choices, nochoices;
static lispval skipsym, limitsym, batchsym, writesym, readsym;;
static lispval fieldssym, upsertsym, newsym, removesym, singlesym, wtimeoutsym;
static lispval returnsym, originalsym;
static lispval primarysym, primarypsym, secondarysym, secondarypsym;
static lispval nearestsym, poolmaxsym;
static lispval mongovec_symbol;

static lispval mongodb_oidref(lispval oid)
{
  if (FD_OIDP(oid)) {
    FD_OID addr = FD_OID_ADDR(oid);
    unsigned int hi = FD_OID_HI(addr), lo = FD_OID_LO(addr);
    u8_string rep = u8_mkstring("ObjectId(\"00000000%08x%08x\")",hi,lo);
    return fd_init_string(NULL,-1,rep);}
  else return fd_err("NotAnOID","mongodb_oidref",NULL,oid);
}

static void grab_mongodb_error(bson_error_t *error,u8_string caller)
{
  u8_seterr(fd_MongoDB_Error,caller,u8_strdup(error->message));
}

/*#define HAVE_MONGOC_OPTS_FUNCTIONS (MONGOC_CHECK_VERSION(1,7,0)) */
#define HAVE_MONGOC_OPTS_FUNCTIONS (MONGOC_CHECK_VERSION(1,5,0))
#define HAVE_MONGOC_COUNT_DOCUMENTS (MONGOC_CHECK_VERSION(1,12,0))
#define HAVE_MONGOC_COUNT_WITH_OPTS (MONGOC_CHECK_VERSION(1,6,0))
#define HAVE_MONGOC_BULK_OPERATION_WITH_OPTS (MONGOC_CHECK_VERSION(1,9,0))
#define HAVE_MONGOC_URI_SET_DATABASE (MONGOC_CHECK_VERSION(1,4,0))

#define MONGODB_CLIENT_BLOCK 1
#define MONGODB_CLIENT_NOBLOCK 0

static mongoc_client_t *get_client(FD_MONGODB_DATABASE *server,int block)
{
  mongoc_client_t *client;
  if (block) {
    u8_logf(LOG_DEBUG,_("MongoDB/getclient"),
            "Getting client from server %llx (%s)",server->dbclients,server->dbspec);
    client = mongoc_client_pool_pop(server->dbclients);}
  else client = mongoc_client_pool_try_pop(server->dbclients);
  u8_logf(LOG_DEBUG,_("MongoDB/gotclient"),
          "Got client %llx from server %llx (%s)",
          client,server->dbclients,server->dbspec);
  return client;
}

static void release_client(FD_MONGODB_DATABASE *server,mongoc_client_t *client)
{
  u8_logf(LOG_DEBUG,_("MongoDB/freeclient"),
          "Releasing client %llx to server %llx (%s)",
          client,server->dbclients,server->dbspec);
  mongoc_client_pool_push(server->dbclients,client);
}

static int boolopt(lispval opts,lispval key,int dflt)
{
  if (FD_TABLEP(opts)) {
    lispval v = fd_get(opts,key,FD_VOID);
    if (FD_VOIDP(v))
      return dflt;
    else if (FD_FALSEP(v))
      return 0;
    else {
      fd_decref(v);
      return 1;}}
  else return dflt;
}

static u8_string stropt(lispval opts,lispval key,u8_string dflt)
{
  if (FD_TABLEP(opts)) {
    lispval v = fd_getopt(opts,key,FD_VOID);
    if ((FD_VOIDP(v))||(FD_FALSEP(v))) {
      if (dflt == NULL) return dflt;
      else return u8_strdup(dflt);}
    else if (FD_STRINGP(v))
      return u8_strdup(FD_CSTRING(v));
    else if (FD_TYPEP(v,fd_secret_type))
      return u8_strdup(FD_CSTRING(v));
    else {
      u8_logf(LOG_ERR,"Invalid string option","%q=%q",key,v);
      fd_decref(v);
      return NULL;}}
  else if (dflt == NULL) return dflt;
  else return u8_strdup(dflt);
}

U8_MAYBE_UNUSED static bson_t *get_projection(lispval opts,int flags)
{
  lispval projection = fd_getopt(opts,returnsym,FD_VOID);
  if (!(FD_CONSP(projection)))
    return NULL;
  else if (FD_SLOTMAPP(projection)) {
    bson_t *fields = fd_lisp2bson(projection,flags,opts);
    fd_decref(projection);
    return fields;}
  else if ( (FD_SYMBOLP(projection)) ||
            (FD_STRINGP(projection)) ) {
    struct FD_KEYVAL kv[1];
    kv[0].kv_key = projection;
    kv[0].kv_val = FD_INT(1);
    fd_incref(projection);
    lispval map = fd_make_slotmap(1,1,kv);
    bson_t *fields = fd_lisp2bson(map,flags,opts);
    fd_decref(map);
    return fields;}
  else if (FD_CHOICEP(projection)) {
    int i = 0, len = FD_CHOICE_SIZE(projection);
    struct FD_KEYVAL kv[len];
    FD_DO_CHOICES(field,projection) {
      if ( (FD_STRINGP(field)) )
        fd_incref(field);
      else if (FD_SYMBOLP(field)) {}
      else field=VOID;
      if (!(FD_VOIDP(field))) {
        kv[i].kv_key = field;
        kv[i].kv_val = FD_INT(1);
        i++;}}
    lispval map = fd_make_slotmap(i,i,kv);
    bson_t *fields = fd_lisp2bson(map,flags,opts);
    fd_decref(map);
    fd_decref(projection);
    return fields;}
  else return NULL;
}

static int grow_dtype_vec(lispval **vecp,size_t n,size_t *vlenp)
{
  lispval *vec = *vecp; size_t vlen = *vlenp;
  if (n<vlen) return 1;
  else if (vec == NULL) {
    vec = u8_alloc_n(64,lispval);
    if (vec) {
      *vlenp = 64; *vecp = vec;
      return 1;}}
  else {
    size_t new_len = ((vlen<8192)?(vlen*2):(vlen+8192));
    lispval *new_vec = u8_realloc_n(vec,new_len,lispval);
    if (new_vec) {
      *vecp = new_vec; *vlenp = new_len;
      return 1;}}
  if (vec) {
    int i = 0; while (i<n) { fd_decref(vec[i++]); }
    u8_free(vec);}
  return 0;
}

static void free_dtype_vec(lispval *vec,int n)
{
  if (vec == NULL) return;
  else {
    int i = 0; while (i<n) { fd_decref(vec[i++]); }
    u8_free(vec);}
}

/* Handling options and flags */

int mongodb_defaults = FD_MONGODB_DEFAULTS;

static int getflags(lispval opts,int dflt)
{
  if ((FD_VOIDP(opts)||(FD_FALSEP(opts))||(FD_DEFAULTP(opts))))
    if (dflt<0) return mongodb_defaults;
    else return dflt;
  else if (FD_UINTP(opts)) return FD_FIX2INT(opts);
  else if ((FD_CHOICEP(opts))||(FD_SYMBOLP(opts))) {
    int flags = FD_MONGODB_DEFAULTS;
    if (fd_overlapp(opts,raw)) flags = 0;
    if (fd_overlapp(opts,slotify)) flags |= FD_MONGODB_SLOTIFY;
    if (fd_overlapp(opts,slotifyin)) flags |= FD_MONGODB_SLOTIFY_IN;
    if (fd_overlapp(opts,slotifyout)) flags |= FD_MONGODB_SLOTIFY_OUT;
    if (fd_overlapp(opts,colonize)) flags |= FD_MONGODB_COLONIZE;
    if (fd_overlapp(opts,colonizein)) flags |= FD_MONGODB_COLONIZE_IN;
    if (fd_overlapp(opts,colonizeout)) flags |= FD_MONGODB_COLONIZE_OUT;
    if (fd_overlapp(opts,choices)) flags |= FD_MONGODB_CHOICEVALS;
    if (fd_overlapp(opts,logopsym)) flags |= FD_MONGODB_LOGOPS;
    if (fd_overlapp(opts,nochoices)) flags &= (~FD_MONGODB_CHOICEVALS);
    return flags;}
  else if (FD_TABLEP(opts)) {
    lispval flagsv = fd_getopt(opts,bsonflags,FD_VOID);
    if (FD_VOIDP(flagsv)) {
      if (dflt<0) return FD_MONGODB_DEFAULTS;
      else return dflt;}
    else {
      int flags = getflags(flagsv,dflt);
      fd_decref(flagsv);
      return flags;}}
  else if (dflt<0) return FD_MONGODB_DEFAULTS;
  else return dflt;
}

static int get_write_flags(lispval val)
{
  if (FD_VOIDP(val))
    return MONGOC_WRITE_CONCERN_W_DEFAULT;
  else if (FD_FALSEP(val))
    return MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED;
  else if (FD_TRUEP(val))
    return MONGOC_WRITE_CONCERN_W_MAJORITY;
  else if ((FD_FIXNUMP(val))&&(FD_FIX2INT(val)<0))
    return MONGOC_WRITE_CONCERN_W_ERRORS_IGNORED;
  else if ((FD_UINTP(val))&&(FD_FIX2INT(val)>0))
    return FD_FIX2INT(val);
  else {
    u8_logf(LOG_ERR,"mongodb/get_write_concern","Bad MongoDB write concern %q",val);
    return MONGOC_WRITE_CONCERN_W_DEFAULT;}
}

static mongoc_write_concern_t *get_write_concern(lispval opts)
{
  lispval val = fd_getopt(opts,writesym,FD_VOID);
  lispval wait = fd_getopt(opts,wtimeoutsym,FD_VOID);
  if ((FD_VOIDP(val))&&(FD_VOIDP(wait))) return NULL;
  else {
    mongoc_write_concern_t *wc = mongoc_write_concern_new();
    if (!(FD_VOIDP(val))) {
      int w = get_write_flags(val);
      mongoc_write_concern_set_w(wc,w);}
    if (FD_UINTP(wait)) {
      int msecs = FD_FIX2INT(wait);
      mongoc_write_concern_set_wtimeout(wc,msecs);}
    fd_decref(wait);
    fd_decref(val);
    return wc;}
}

static int getreadmode(lispval val)
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
    u8_logf(LOG_ERR,"mongodb/getreadmode",
            "Bad MongoDB read mode %q",val);
    return MONGOC_READ_PRIMARY;}
}

static mongoc_read_prefs_t *get_read_prefs(lispval opts)
{
  lispval spec = fd_getopt(opts,readsym,FD_VOID);
  if (FD_VOIDP(spec)) return NULL;
  else {
    mongoc_read_prefs_t *rp = mongoc_read_prefs_new(MONGOC_READ_PRIMARY);
    int flags = getflags(opts,mongodb_defaults);
    FD_DO_CHOICES(s,spec) {
      if (FD_SYMBOLP(s)) {
        int p = getreadmode(s);
        mongoc_read_prefs_set_mode(rp,p);}
      else if (FD_TABLEP(s)) {
        const bson_t *bson = fd_lisp2bson(s,flags,opts);
        mongoc_read_prefs_add_tag(rp,bson);}
      else {
        u8_logf(LOG_ERR,"mongodb/getreadmode",
                "Bad MongoDB read preference %q",s);}}
    fd_decref(spec);
    return rp;}
}

static lispval combine_opts(lispval opts,lispval clopts)
{
  if (opts == clopts) {
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

static U8_MAYBE_UNUSED bson_t *get_search_opts(lispval opts,int flags,int for_find)
{
  lispval skip_arg = fd_getopt(opts,skipsym,FD_FIXZERO);
  lispval limit_arg = fd_getopt(opts,limitsym,FD_VOID);
  lispval sort_arg   = fd_getopt(opts,FDSYM_SORTED,FD_VOID);
  lispval batch_arg = (for_find) ? (fd_getopt(opts,batchsym,FD_FIXZERO)) : (FD_VOID);
  lispval projection = (for_find) ? (fd_getopt(opts,returnsym,FD_VOID)) : (FD_VOID);
  struct FD_BSON_OUTPUT out;
  bson_t *doc = bson_new();
  out.bson_doc = doc;
  out.bson_opts = opts;
  out.bson_flags = flags;
  out.bson_fieldmap = fd_getopt(opts,fieldmap_symbol,FD_VOID);
  if (FD_FIXNUMP(skip_arg))
    bson_append_dtype(out,"skip",4,skip_arg,0);
  else fd_decref(skip_arg);
  if (FD_FIXNUMP(limit_arg))
    bson_append_dtype(out,"limit",5,limit_arg,0);
  else fd_decref(limit_arg);
  if (FD_FIXNUMP(batch_arg))
    bson_append_dtype(out,"batchSize",9,batch_arg,0);
  else fd_decref(batch_arg);
  if (FD_TABLEP(sort_arg))
    bson_append_dtype(out,"sort",4,sort_arg,0);
  fd_decref(sort_arg);
  if ( (FD_SYMBOLP(projection)) || (FD_CONSP(projection)) ) {
    struct FD_BSON_OUTPUT fields; bson_t proj;
    int ok = bson_append_document_begin(doc,"projection",10,&proj);
    if (ok) {
      fields.bson_doc = &proj;
      fields.bson_flags = ((flags<0)?(getflags(opts,FD_MONGODB_DEFAULTS)):(flags));
      fields.bson_opts = opts;
      fields.bson_fieldmap = out.bson_fieldmap;
      if (FD_SLOTMAPP(projection))
        fd_bson_output(fields,projection);
      else if (FD_SYMBOLP(projection))
        bson_append_keyval(fields,projection,FD_INT(1));
      else if (FD_STRINGP(projection))
        bson_append_keyval(fields,projection,FD_INT(1));
      else if (FD_CHOICEP(projection)) {
        FD_DO_CHOICES(key,projection) {
          if ( (FD_SYMBOLP(key)) || (FD_STRINGP(key)) )
            bson_append_keyval(fields,key,FD_INT(1));}}
      else {}
      bson_append_document_end(doc,&proj);}
    else {
      fd_seterr(fd_BSON_Error,"get_search_opts(mongodb)",NULL,opts);
      fd_decref(out.bson_fieldmap);
      return NULL;}}
  return out.bson_doc;
}

static U8_MAYBE_UNUSED bson_t *getbulkopts(lispval opts,int flags)
{
  struct FD_BSON_OUTPUT out;
  bson_t *doc = bson_new();
  out.bson_doc = doc;
  out.bson_opts = opts;
  out.bson_flags = flags;
  out.bson_fieldmap = fd_getopt(opts,fieldmap_symbol,FD_VOID);
  lispval ordered_arg = fd_getopt(opts,FDSYM_SORTED,FD_FALSE);
  if (!(FD_FALSEP(ordered_arg))) {
    bson_append_dtype(out,"ordered",4,ordered_arg,0);}

  lispval wcval = fd_getopt(opts,writesym,FD_VOID);
  lispval wcwait = fd_getopt(opts,wtimeoutsym,FD_VOID);

  if (!(FD_VOIDP(wcval))) {
    bson_append_dtype(out,"writeConcern",4,wcval,0);}
  if (!(FD_VOIDP(wcwait))) {
    bson_append_dtype(out,"wtimeout",4,wcwait,0);}

  fd_decref(ordered_arg);
  fd_decref(wcval);
  fd_decref(wcwait);

  return out.bson_doc;
}

static int mongodb_getflags(lispval mongodb);

/* MongoDB vecslots */

/* These are slots which should always have vector (array) values, so
   if their value is a choice, it is rendered as an array, but if it's
   a singleton, it's rendered as an array of one value. */

static int get_vecslot(lispval slot)
{
  int i = 0, n = n_vecslots;
  while (i<n)
    if (vecslots[i] == slot)
      return i;
    else i++;
  return -1;
}

static int add_vecslot(lispval slot)
{
  int off = get_vecslot(slot);
  if (off>=0) return off;
  else {
    u8_lock_mutex(&vecslots_lock);
    int i = 0, n = n_vecslots;
    while (i<n)
      if (vecslots[i] == slot) {
        u8_unlock_mutex(&vecslots_lock);
        return i;}
      else i++;
    if (i >= MONGO_VECSLOTS_MAX) {
      u8_unlock_mutex(&vecslots_lock);
      return -1;}
    vecslots[i] = slot;
    n_vecslots++;
    u8_unlock_mutex(&vecslots_lock);
    return i;}
}

static lispval vecslots_config_get(lispval var,void *data)
{
  lispval result = FD_EMPTY;
  int i = 0, n = n_vecslots;
  while (i < n) {
    lispval slot = vecslots[i++];
    FD_ADD_TO_CHOICE(result,slot);}
  return result;
}

static int vecslots_config_add(lispval var,lispval val,void *data)
{
  lispval sym = FD_VOID;
  if (FD_SYMBOLP(val))
    sym = val;
  else if (FD_STRINGP(val)) {
    u8_string upper = u8_upcase(FD_CSTRING(val));
    sym = fd_intern(upper);
    u8_free(upper);}
  else {
    fd_seterr("Not symbolic","mongodb/config_add_vecslots",
              NULL,val);
    return -1;}
  int rv = add_vecslot(sym);
  if (rv < 0) {
    char buf[64];
    fd_seterr("Too many vecslots declared","mongodb/config_add_vecslots",
              u8_sprintf(buf,sizeof(buf),"%d",MONGO_VECSLOTS_MAX),
              val);
    return rv;}
  else return rv;
}

/* Consing MongoDB clients, collections, and cursors */

static u8_string get_connection_spec(mongoc_uri_t *info);
static int setup_ssl(mongoc_ssl_opt_t *,mongoc_uri_t *,lispval);

#if MONGOC_CHECK_VERSION(1,6,0)
static int set_uri_opt(mongoc_uri_t *uri,const char *option,lispval val)
{
  if (FD_FALSEP(val))
    return mongoc_uri_set_option_as_bool(uri,option,0);
  if (FD_TRUEP(val))
    return mongoc_uri_set_option_as_bool(uri,option,1);
  else if (FD_STRINGP(val))
    return mongoc_uri_set_option_as_utf8(uri,option,FD_CSTRING(val));
  else if (FD_SYMBOLP(val))
    return mongoc_uri_set_option_as_utf8(uri,option,FD_SYMBOL_NAME(val));
  else if (FD_UINTP(val))
    return mongoc_uri_set_option_as_int32(uri,option,FD_FIX2INT(val));
  else if (FD_FLONUMP(val)) {
    long long msecs = (int) floor(FD_FLONUM(val)*1000.0);
    if (msecs > 0) {
      if (msecs > INT_MAX) msecs=INT_MAX;
      return mongoc_uri_set_option_as_int32(uri,option,msecs);}}
  else NO_ELSE;
  fd_seterr("BadOptionValue","mongodb/set_uri_opt",option,val);
  return -1;
}
#else
static void escape_uri(u8_output out,u8_string s,int len);
static u8_string add_to_query(u8_string qstring,const char *option,lispval val)
{
  u8_string result = qstring;
  size_t option_len = strlen(option);
  char optbuf[option_len+2];
  strcpy(optbuf,option); strcat(optbuf,"=");
  u8_string poss_start = strstr(qstring,optbuf);
  if ( (poss_start) && (poss_start>qstring) &&
       ( (poss_start[-1] == '?') || (poss_start[-1] == '&') ) )
    return qstring;
  u8_string tail = qstring+strlen(qstring)-1;
  u8_string sep = (*tail == '&') ? ("") :
    (*tail == '?') ? ("") : (strchr(qstring,'?')) ? ("&") : ("?");
  if (FD_FALSEP(val))
    result = u8_mkstring("%s%s%s=false&",qstring,sep,option);
  else if (FD_TRUEP(val))
    result = u8_mkstring("%s%s%s=true&",qstring,sep,option);
  else if (FD_STRINGP(val)) {
    struct U8_OUTPUT out; unsigned char buf[200];
    U8_INIT_OUTPUT_BUF(&out,200,buf);
    escape_uri(&out,FD_CSTRING(val),FD_STRLEN(val));
    result = u8_mkstring("%s%s%s=%s&",qstring,sep,out.u8_outbuf);
    u8_close_output(&out);}
  else if (FD_SYMBOLP(val)) {
    struct U8_OUTPUT out; unsigned char buf[200];
    u8_string pname = FD_SYMBOL_NAME(val);
    U8_INIT_OUTPUT_BUF(&out,200,buf);
    escape_uri(&out,pname,strlen(pname));
    result = u8_mkstring("%s%s%s=%s&",qstring,sep,out.u8_outbuf);
    u8_close_output(&out);}
  else if (FD_UINTP(val))
    result = u8_mkstring("%s%s%s=%d&",qstring,sep,FD_FIX2INT(val));
  else if (FD_FLONUMP(val)) {
    long long msecs = (int) floor(FD_FLONUM(val)*1000.0);
    if (msecs > 0) {
      if (msecs > INT_MAX) msecs=INT_MAX;
      result  = u8_mkstring("%s%s%s=%lld&",qstring,sep,msecs);}}
  else NO_ELSE;
  if (result != qstring) u8_free(qstring);
  return result;
}
static void escape_uri(u8_output out,u8_string s,int len)
{
  u8_string lim = ((len<0)?(NULL):(s+len));
  while ((lim)?(s<lim):(*s))
    if (((*s)>=0x80)||(isspace(*s))||
        (*s=='+')||(*s=='%')||(*s=='=')||
        (*s=='&')||(*s=='#')||(*s==';')||
        (!((isalnum(*s))||(strchr("-_.~",*s)!=NULL)))) {
      char buf[8];
      sprintf(buf,"%%%02x",*s);
      u8_puts(out,buf); s++;}
    else {u8_putc(out,*s); s++;}
}
#endif

static mongoc_uri_t *setup_mongoc_uri(mongoc_uri_t *info,lispval opts)
{
  if (info == NULL) return info;
  u8_string dbname = mongoc_uri_get_database(info);
  lispval dbarg   = fd_getopt(opts,fd_intern("DBNAME"),FD_VOID);
  if ( (dbname) &&
       ((FD_VOIDP(dbarg)) || (FD_FALSEP(dbarg)) || (FD_DEFAULTP(dbarg)) ) ) {}
  else if ((FD_VOIDP(dbarg)) || (FD_FALSEP(dbarg)) || (FD_DEFAULTP(dbarg)) ) {
    fd_seterr("NoDBName","setup_mongoc_uri",
              mongoc_uri_get_string(info),FD_VOID);
    fd_decref(dbarg);
    return NULL;}
#if HAVE_MONGOC_URI_SET_DATABASE
  else if (!(FD_STRINGP(dbarg))) {
    fd_seterr("Invalid MongoDBName","setup_mongoc_uri",
              mongoc_uri_get_string(info),
              dbarg);
    fd_decref(dbarg);
    return NULL;}
  else if ( (dbname) && (strcmp(dbname,FD_CSTRING(dbarg))) ) {}
  else {
    mongoc_uri_set_database(info,FD_CSTRING(dbarg));}
#else
  else if (dbname == NULL) {
    fd_seterr("NoDBName","setup_mongoc_uri",
              mongoc_uri_get_string(info),FD_VOID);
    fd_decref(dbarg);
    return NULL;}
  else if ( (FD_STRINGP(dbarg)) && (strcmp(dbname,FD_CSTRING(dbarg))) == 0) {}
  else {
    fd_seterr("CantSetDB","setup_mongoc_uri",
              "Can't set/modify database in this version of MongoDB",
              FD_VOID);
    fd_decref(dbarg);
    return NULL;}
#endif

  lispval appname = fd_getopt(opts,fd_intern("APPNAME"),FD_VOID);
  lispval timeout = fd_getopt(opts,fd_intern("TIMEOUT"),FD_VOID);
  lispval ctimeout = fd_getopt(opts,fd_intern("CTIMEOUT"),FD_VOID);
  lispval stimeout = fd_getopt(opts,fd_intern("STIMEOUT"),FD_VOID);
  lispval maxpool = fd_getopt(opts,fd_intern("MAXPOOL"),FD_VOID);
#if MONGOC_CHECK_VERSION(1,6,0)
  if (!(FD_VOIDP(timeout))) {
    set_uri_opt(info,MONGOC_URI_SOCKETTIMEOUTMS,timeout);
    if (FD_VOIDP(ctimeout))
      set_uri_opt(info,MONGOC_URI_CONNECTTIMEOUTMS,timeout);
    if (FD_VOIDP(stimeout))
      set_uri_opt(info,MONGOC_URI_SERVERSELECTIONTIMEOUTMS,timeout);}
  if (!(FD_VOIDP(ctimeout)))
    set_uri_opt(info,MONGOC_URI_CONNECTTIMEOUTMS,ctimeout);
  if (!(FD_VOIDP(stimeout)))
    set_uri_opt(info,MONGOC_URI_SERVERSELECTIONTIMEOUTMS,stimeout);
  if (!(FD_VOIDP(maxpool)))
    set_uri_opt(info,MONGOC_URI_MAXPOOLSIZE,ctimeout);
  if ( (FD_STRINGP(appname)) || (FD_SYMBOLP(appname)) )
    set_uri_opt(info,MONGOC_URI_APPNAME,appname);
  mongoc_uri_set_option_as_utf8(info,MONGOC_URI_APPNAME,u8_appid());
  if (boolopt(opts,sslsym,default_ssl)) {
    mongoc_uri_set_option_as_bool(info,MONGOC_URI_SSL,1);}
#else
  u8_string uri = mongoc_uri_get_string(info);
  u8_string qmark = strchr(uri,'?');
  u8_string qstring = (qmark) ? (u8_strdup(qmark)) : (u8_strdup("?"));
  if (!(FD_VOIDP(timeout))) {
    qstring = add_to_query(qstring,"socketTimeoutMS",timeout);
    if (FD_VOIDP(ctimeout))
      qstring = add_to_query(qstring,"connectTimeoutMS",timeout);
    if (FD_VOIDP(stimeout))
      qstring = add_to_query(qstring,"serverSelectionTimeoutMS",timeout);}
  if (!(FD_VOIDP(ctimeout)))
    qstring = add_to_query(qstring,"connectTimeoutMS",ctimeout);
  if (!(FD_VOIDP(stimeout)))
    qstring = add_to_query(qstring,"serverSelectionTimeoutMS",stimeout);
  if (!(FD_VOIDP(maxpool)))
    qstring = add_to_query(qstring,"maxPoolSize",ctimeout);
  if (boolopt(opts,sslsym,default_ssl)) {
    qstring = add_to_query(qstring,"ssl",FD_TRUE);}
  size_t base_len = (qmark==NULL) ? (strlen(uri)) : (qmark-uri);
  unsigned char newbuf[base_len+strlen(qstring)+1];
  strcpy(newbuf,uri);
  strcpy(newbuf+base_len,qstring);
  mongoc_uri_t *new_info = mongoc_uri_new(newbuf);
  mongoc_uri_destroy(info);
  info = new_info;
#endif
  fd_decref(appname);
  fd_decref(timeout);
  fd_decref(ctimeout);
  fd_decref(maxpool);
  return info;
}

static u8_string mongodb_check(mongoc_client_pool_t *client_pool)
{
  mongoc_client_t *probe = mongoc_client_pool_pop(client_pool);
  if (probe) {
    bson_t *command = BCON_NEW("ping",BCON_INT32(1)), reply;
    bson_error_t error;
    bool rv = mongoc_client_command_simple(probe,"admin",command,NULL,&reply,&error);
    mongoc_client_pool_push(client_pool,probe);
    bson_destroy(command);
    if (! rv )
      return u8_strdup(error.message);
    else {
      bson_destroy(&reply);
      return NULL;}}
  else return u8_strdup("mongoc_client_pool_pop failed");
}

#if MONGOC_CHECK_VERSION(1,4,0)
#define mongoc_uri_info(s,e) mongoc_uri_new_with_error((s),e)
#else
#define mongoc_uri_info(s,e) mongoc_uri_new((s))
#endif

/* This returns a MongoDB server object which wraps a MongoDB client
   pool. */
static lispval mongodb_open(lispval arg,lispval opts)
{
  mongoc_client_pool_t *client_pool;
  mongoc_uri_t *info;
  mongoc_ssl_opt_t ssl_opts={ 0 };
  bson_error_t error = { 0 };
  int smoke_test = boolopt(opts,smoketest_sym,1);
  int flags = getflags(opts,mongodb_defaults);;

  if ((FD_STRINGP(arg))||(FD_TYPEP(arg,fd_secret_type))) {
    info = mongoc_uri_info(FD_CSTRING(arg),&error);}
  else if (FD_SYMBOLP(arg)) {
    lispval conf_val = fd_config_get(FD_SYMBOL_NAME(arg));
    if (FD_VOIDP(conf_val))
      return fd_type_error("MongoDB URI config","mongodb_open",arg);
    else if ((FD_STRINGP(conf_val))||
             (FD_TYPEP(conf_val,fd_secret_type))) {
      info = mongoc_uri_info(FD_CSTRING(conf_val),&error);
      fd_decref(conf_val);}
    else return fd_type_error("MongoDB URI config val",
                              FD_SYMBOL_NAME(arg),conf_val);}
  else return fd_type_error("MongoDB URI","mongodb_open",arg);

  if (info == NULL)
    return fd_err("MongoDB URI spec","mongodb_open",NULL,arg);
  else info = setup_mongoc_uri(info,opts);

  if (!(info))
    return fd_err(fd_MongoDB_Error,"mongodb_open",error.message,arg);
  else client_pool = mongoc_client_pool_new(info);

  if (client_pool == NULL) {
    mongoc_uri_destroy(info);
    return fd_type_error("MongoDB client URI","mongodb_open",arg);}
  else if ( (setup_ssl(&ssl_opts,info,opts)) ) {
    mongoc_client_pool_set_ssl_opts(client_pool,&ssl_opts);
    u8_free(ssl_opts.pem_file);
    u8_free(ssl_opts.pem_pwd);
    u8_free(ssl_opts.ca_file);
    u8_free(ssl_opts.ca_dir);
    u8_free(ssl_opts.crl_file);}
  else NO_ELSE;

  if (smoke_test) {
    u8_string errmsg = mongodb_check(client_pool);
    if (errmsg) {
      fd_seterr("MongoDB/ConnectFailed","mongodb_open",errmsg,
                fd_lispstring(mongoc_uri_get_string(info)));
      mongoc_client_pool_destroy(client_pool);
      mongoc_uri_destroy(info);
      u8_free(errmsg);
      return FD_ERROR;}}

  u8_string uri = u8_strdup(mongoc_uri_get_string(info));
  struct FD_MONGODB_DATABASE *srv = u8_alloc(struct FD_MONGODB_DATABASE);
  u8_string dbname = mongoc_uri_get_database(info);
  lispval poolmax = fd_getopt(opts,poolmaxsym,FD_VOID);
  if (FD_UINTP(poolmax)) {
    int pmax = FD_FIX2INT(poolmax);
    mongoc_client_pool_max_size(client_pool,pmax);}
  fd_decref(poolmax);
  FD_INIT_CONS(srv,fd_mongoc_server);
  srv->dburi = uri;
  if (dbname == NULL)
    srv->dbname = NULL;
  else srv->dbname = u8_strdup(dbname);
  srv->dbspec = get_connection_spec(info);
  srv->dburi_info = info;
  srv->dbclients = client_pool;
  srv->dbopts = fd_incref(opts);
  srv->dbflags = flags;
  if ((logops)||(flags&FD_MONGODB_LOGOPS))
    u8_logf(LOG_INFO,"MongoDB/open",
            "Opened %s with %s",dbname,srv->dbspec);
  return (lispval)srv;
}
static void recycle_server(struct FD_RAW_CONS *c)
{
  struct FD_MONGODB_DATABASE *s = (struct FD_MONGODB_DATABASE *)c;
  mongoc_uri_destroy(s->dburi_info);
  mongoc_client_pool_destroy(s->dbclients);
  fd_decref(s->dbopts);
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}
static int unparse_server(struct U8_OUTPUT *out,lispval x)
{
  struct FD_MONGODB_DATABASE *srv = (struct FD_MONGODB_DATABASE *)x;
  u8_printf(out,"#<MongoDB/Server %s/%s>",srv->dbspec,srv->dbname);
  return 1;
}

static u8_string get_connection_spec(mongoc_uri_t *info)
{
  const mongoc_host_list_t *hosts = mongoc_uri_get_hosts(info);
  u8_string username = mongoc_uri_get_username(info), result = NULL;
  u8_string server_name = "unknown";
  if (hosts) server_name = hosts->host_and_port;
  else {
#if MONGOC_CHECK_VERSION(1,9,0)
    server_name = mongoc_uri_get_service(info);
#endif
  }
  if (server_name == NULL) server_name="unknown";
  if (username) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,256);
    u8_printf(&out,"%s@%s",username,server_name);
    result = out.u8_outbuf;}
  else result = u8_strdup(server_name);
  return result;
}

static lispval certfile, certpass, cafilesym, cadirsym, crlsym;

static int setup_ssl(mongoc_ssl_opt_t *ssl_opts,
                     mongoc_uri_t *info,
                     lispval opts)
{
  if ( (mongoc_uri_get_ssl(info)) ||
       (boolopt(opts,sslsym,default_ssl)) ||
       ( (fd_testopt(opts,cafilesym,FD_VOID)) &&
         (!(fd_testopt(opts,cafilesym,FD_FALSE)))) ) {
    const mongoc_ssl_opt_t *default_opts = mongoc_ssl_opt_get_default();
    memcpy(ssl_opts,default_opts,sizeof(mongoc_ssl_opt_t));
    ssl_opts->pem_file = stropt(opts,certfile,default_certfile);
    ssl_opts->pem_pwd = stropt(opts,certpass,NULL);
    ssl_opts->ca_file = stropt(opts,cafilesym,default_cafile);
    ssl_opts->ca_dir = stropt(opts,cadirsym,default_cadir);
    ssl_opts->crl_file = stropt(opts,crlsym,NULL);
    return (!((ssl_opts->pem_file == NULL)&&
              (ssl_opts->pem_pwd == NULL)&&
              (ssl_opts->ca_file == NULL)&&
              (ssl_opts->ca_dir == NULL)&&
              (ssl_opts->crl_file == NULL)));}
  else return 0;
}

/* Creating collections */

/* Collection creation is actually deferred until the collection is
   used because we want the collection to be "thread safe" which means
   that it won't correspond to a single mongoc_collection_t object,
   but that each use of the collection will pop a client from the pool
   and create a collection with that client. */
static lispval mongodb_collection(lispval server,lispval name_arg,
                                  lispval opts_arg)
{
  struct FD_MONGODB_COLLECTION *result;
  struct FD_MONGODB_DATABASE *srv;
  u8_string name = FD_CSTRING(name_arg), collection_name = NULL;
  lispval opts; int flags;
  if (FD_TYPEP(server,fd_mongoc_server)) {
    srv = (struct FD_MONGODB_DATABASE *)server;
    flags = getflags(opts_arg,srv->dbflags);
    opts = combine_opts(opts_arg,srv->dbopts);
    fd_incref(server);}
  else if ((FD_STRINGP(server))||
           (FD_SYMBOLP(server))||
           (FD_TYPEP(server,fd_secret_type))) {
    lispval consed = mongodb_open(server,opts_arg);
    if (FD_ABORTP(consed)) return consed;
    server = consed; srv = (struct FD_MONGODB_DATABASE *)consed;
    flags = getflags(opts_arg,srv->dbflags);
    opts = combine_opts(opts_arg,srv->dbopts);}
  else return fd_type_error("MongoDB client","mongodb_collection",server);
  if (strchr(name,'/')) {
    char *slash = strchr(name,'/');
    collection_name = slash+1;}
  else if (srv->dbname == NULL) {
    return fd_err(_("MissingDBName"),"mongodb_open",NULL,server);}
  else {
    collection_name = u8_strdup(name);}
  result = u8_alloc(struct FD_MONGODB_COLLECTION);
  FD_INIT_CONS(result,fd_mongoc_collection);
  result->domain_db = server;
  result->domain_opts = opts;
  result->domain_flags = flags;
  result->collection_name = collection_name;
  return (lispval) result;
}
static void recycle_collection(struct FD_RAW_CONS *c)
{
  struct FD_MONGODB_COLLECTION *collection = (struct FD_MONGODB_COLLECTION *)c;
  fd_decref(collection->domain_db);
  fd_decref(collection->domain_opts);
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}
static int unparse_collection(struct U8_OUTPUT *out,lispval x)
{
  struct FD_MONGODB_COLLECTION *coll = (struct FD_MONGODB_COLLECTION *)x;
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
  u8_string dbname = server->dbname;
  u8_string collection_name = domain->collection_name;
  mongoc_client_t *client = get_client(server,(!(flags&FD_MONGODB_NOBLOCK)));
  if (client) {
    mongoc_collection_t *collection=
      mongoc_client_get_collection(client,dbname,collection_name);
    if (collection) {
      *clientp = client;
      return collection;}
    else {
      release_client(server,client);
      return NULL;}}
  else return NULL;
}

/* This returns a client to a client pool.  The argument can be either
   a server or a collection (which is followed to its server). */
static void client_done(lispval arg,mongoc_client_t *client)
{
  if (FD_TYPEP(arg,fd_mongoc_server)) {
    struct FD_MONGODB_DATABASE *server = (struct FD_MONGODB_DATABASE *)arg;
    release_client(server,client);}
  else if (FD_TYPEP(arg,fd_mongoc_collection)) {
    struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
    struct FD_MONGODB_DATABASE *server=
      (struct FD_MONGODB_DATABASE *)(domain->domain_db);
    release_client(server,client);}
  else {
    u8_logf(LOG_ERR,"BAD client_done call","Wrong type for %q",arg);}
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

#if HAVE_MONGOC_BULK_OPERATION_WITH_OPTS
static lispval mongodb_insert(lispval arg,lispval objects,lispval opts_arg)
{
  if (FD_EMPTY_CHOICEP(objects))
    return FD_EMPTY_CHOICE;
  if (FD_CHOICEP(arg)) {
    lispval results = FD_EMPTY;
    FD_DO_CHOICES(collection,arg) {
      lispval rv = mongodb_insert(collection,objects,opts_arg);
      if (FD_ABORTP(rv)) {
        fd_decref(results);
        FD_STOP_DO_CHOICES;
        return rv;}
      else {
        FD_ADD_TO_CHOICE(results,rv);}}
    return results;}
  struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
  struct FD_MONGODB_DATABASE *db=
    (struct FD_MONGODB_DATABASE *) (domain->domain_db);
  lispval result;
  int flags = getflags(opts_arg,domain->domain_flags);
  lispval opts = combine_opts(opts_arg,db->dbopts);
  bson_t *bulkopts = getbulkopts(opts,flags);
  mongoc_client_t *client = NULL; bool retval;
  mongoc_collection_t *collection = open_collection(domain,&client,flags);
  if (collection) {
    bson_t reply;
    bson_error_t error = { 0 };
    if ((logops)||(flags&FD_MONGODB_LOGOPS))
      u8_logf(LOG_DETAIL,"MongoDB/insert",
              "Inserting %d items into %q",FD_CHOICE_SIZE(objects),arg);
    if (FD_CHOICEP(objects)) {
      mongoc_bulk_operation_t *bulk=
        mongoc_collection_create_bulk_operation_with_opts
        (collection,bulkopts);
      FD_DO_CHOICES(elt,objects) {
        bson_t *doc = fd_lisp2bson(elt,flags,opts);
        if (doc) {
          mongoc_bulk_operation_insert(bulk,doc);
          bson_destroy(doc);}}
      retval = mongoc_bulk_operation_execute(bulk,&reply,&error);
      mongoc_bulk_operation_destroy(bulk);
      if (retval) {
        result = fd_bson2dtype(&reply,flags,opts);}
      else {
        u8_byte buf[1000];
        if (errno) u8_graberrno("mongodb_insert",NULL);
        fd_seterr(fd_MongoDB_Error,"mongodb_insert",
                  u8_sprintf(buf,1000,"%s (%s>%s)",
                             error.message,db->dburi,domain->collection_name),
                  fd_incref(objects));
        result = FD_ERROR_VALUE;}
      bson_destroy(&reply);}
    else {
      bson_t *doc = fd_lisp2bson(objects,flags,opts);
      mongoc_write_concern_t *wc = get_write_concern(opts);

      retval = (doc==NULL) ? (0) :
        (mongoc_collection_insert
         (collection,MONGOC_INSERT_NONE,doc,wc,&error));
      if (retval) {
        result = FD_TRUE;}
      else {
        u8_byte buf[1000];
        if (doc) bson_destroy(doc);
        if (errno) u8_graberrno("mongodb_insert",NULL);
        fd_seterr(fd_MongoDB_Error,"mongodb_insert",
                  u8_sprintf(buf,1000,"%s (%s>%s)",
                             error.message,db->dburi,domain->collection_name),
                  fd_incref(objects));
        result = FD_ERROR_VALUE;}
      if (wc) mongoc_write_concern_destroy(wc);}
    collection_done(collection,client,domain);}
  else result = FD_ERROR_VALUE;
  fd_decref(opts);
  U8_CLEAR_ERRNO();
  return result;
}
#else
static lispval mongodb_insert(lispval arg,lispval objects,lispval opts_arg)
{
  if (FD_EMPTY_CHOICEP(objects))
    return FD_EMPTY_CHOICE;
  else if (FD_CHOICEP(arg)) {
    lispval results = FD_EMPTY;
    FD_DO_CHOICES(collection,arg) {
      lispval rv = mongodb_insert(collection,objects,opts_arg);
      if (FD_ABORTP(rv)) {
        fd_decref(results);
        FD_STOP_DO_CHOICES;
        return rv;}
      else {
        FD_ADD_TO_CHOICE(results,rv);}}
    return results;}
  struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
  struct FD_MONGODB_DATABASE *db =
    (struct FD_MONGODB_DATABASE *) (domain->domain_db);
  lispval result;
  int flags = getflags(opts_arg,domain->domain_flags);
  lispval opts = combine_opts(opts_arg,db->dbopts);
  mongoc_client_t *client = NULL; bool retval;
  mongoc_collection_t *collection = open_collection(domain,&client,flags);
  if (collection) {
    bson_t reply;
    bson_error_t error;
    mongoc_write_concern_t *wc = get_write_concern(opts);
    if ((logops)||(flags&FD_MONGODB_LOGOPS))
      u8_logf(LOG_DETAIL,"MongoDB/insert",
              "Inserting %d items into %q",FD_CHOICE_SIZE(objects),arg);
    if (FD_CHOICEP(objects)) {
      mongoc_bulk_operation_t *bulk=
        mongoc_collection_create_bulk_operation(collection,true,wc);
      FD_DO_CHOICES(elt,objects) {
        bson_t *doc = fd_lisp2bson(elt,flags,opts);
        if (doc) {
          mongoc_bulk_operation_insert(bulk,doc);
          bson_destroy(doc);}}
      retval = mongoc_bulk_operation_execute(bulk,&reply,&error);
      mongoc_bulk_operation_destroy(bulk);
      if (retval) {
        result = fd_bson2dtype(&reply,flags,opts);}
      else {
        u8_byte buf[1000];
        if (errno) u8_graberrno("mongodb_insert",NULL);
        fd_seterr(fd_MongoDB_Error,"mongodb_insert",
                  u8_sprintf(buf,1000,"%s (%s>%s)",
                             error.message,db->dburi,domain->collection_name),
                  fd_incref(objects));
        result = FD_ERROR_VALUE;}
      bson_destroy(&reply);}
    else {
      bson_t *doc = fd_lisp2bson(objects,flags,opts);
      retval = (doc==NULL) ? (0) :
        (mongoc_collection_insert(collection,MONGOC_INSERT_NONE,doc,wc,&error));
      if (retval) {
        result = FD_TRUE;}
      else {
        u8_byte buf[1000];
        if (doc) bson_destroy(doc);
        if (errno) u8_graberrno("mongodb_insert",NULL);
        fd_seterr(fd_MongoDB_Error,"mongodb_insert",
                  u8_sprintf(buf,1000,"%s (%s>%s)",
                             error.message,db->dburi,domain->collection_name),
                  fd_incref(objects));
        result = FD_ERROR_VALUE;}}
    if (wc) mongoc_write_concern_destroy(wc);
    collection_done(collection,client,domain);}
  else result = FD_ERROR_VALUE;
  fd_decref(opts);
  U8_CLEAR_ERRNO();
  return result;
}
#endif

static lispval mongodb_remove(lispval arg,lispval obj,lispval opts_arg)
{
  lispval result = FD_VOID;
  struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
  struct FD_MONGODB_DATABASE *db = DOMAIN2DB(domain);
  lispval opts = combine_opts(opts_arg,db->dbopts);
  int flags = getflags(opts_arg,domain->domain_flags), hasid = 1;
  mongoc_client_t *client = NULL;
  mongoc_collection_t *collection = open_collection(domain,&client,flags);
  if (collection) {
    struct FD_BSON_OUTPUT q; bson_error_t error;
    mongoc_write_concern_t *wc = get_write_concern(opts);
    q.bson_doc = bson_new();
    q.bson_opts = opts;
    q.bson_flags = flags;
    q.bson_fieldmap = FD_VOID;
    if (FD_TABLEP(obj)) {
      lispval id = fd_get(obj,idsym,FD_VOID);
      if (FD_VOIDP(id)) {
        q.bson_fieldmap = fd_getopt(opts,fieldmap_symbol,FD_VOID);
        fd_bson_output(q,obj);
        hasid = 0;}
      else {
        bson_append_dtype(q,"_id",3,id,0);
        fd_decref(id);}}
    else bson_append_dtype(q,"_id",3,obj,0);
    if ((logops)||(flags&FD_MONGODB_LOGOPS))
      u8_logf(LOG_DETAIL,"MongoDB/remove","Removing %q items from %q",obj,arg);
    if (mongoc_collection_remove(collection,
                                 ((hasid)?(MONGOC_REMOVE_SINGLE_REMOVE):
                                  (MONGOC_REMOVE_NONE)),
                                 q.bson_doc,wc,&error)) {
      result = FD_TRUE;}
    else {
      u8_byte buf[1000];
      fd_seterr(fd_MongoDB_Error,"mongodb_remove",
                  u8_sprintf(buf,1000,"%s (%s>%s)",
                             error.message,db->dburi,domain->collection_name),
                fd_incref(obj));
      result = FD_ERROR_VALUE;}
    collection_done(collection,client,domain);
    if (wc) mongoc_write_concern_destroy(wc);
    fd_decref(q.bson_fieldmap);
    if (q.bson_doc) bson_destroy(q.bson_doc);}
  else result = FD_ERROR_VALUE;
  fd_decref(opts);
  return result;
}

static lispval mongodb_updater(lispval arg,lispval query,lispval update,
                               mongoc_update_flags_t add_update_flags,
                               lispval opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
  struct FD_MONGODB_DATABASE *db = DOMAIN2DB(domain);
  int flags = getflags(opts_arg,domain->domain_flags);
  lispval opts = combine_opts(opts_arg,domain->domain_opts);
  mongoc_client_t *client = NULL;
  mongoc_collection_t *collection = open_collection(domain,&client,flags);
  if (collection) {
    bson_t *q = fd_lisp2bson(query,flags,opts);
    bson_t *u = fd_lisp2bson(update,flags,opts);
    mongoc_write_concern_t *wc = get_write_concern(opts);
    mongoc_update_flags_t update_flags=
      (MONGOC_UPDATE_NONE) | (add_update_flags) |
      ((boolopt(opts,upsertsym,0))?(MONGOC_UPDATE_UPSERT):(0)) |
      ((boolopt(opts,singlesym,0))?(0):(MONGOC_UPDATE_MULTI_UPDATE));
    bson_error_t error;
    int success = 0, no_error = boolopt(opts,softfailsym,0);
    if ((logops)||(flags&FD_MONGODB_LOGOPS))
      u8_logf(LOG_DETAIL,"MongoDB/update",
             "Updating matches to %q with %q in %q",query,update,arg);
    if ((q)&&(u))
      success = mongoc_collection_update(collection,update_flags,q,u,wc,&error);
    collection_done(collection,client,domain);
    if (q) bson_destroy(q);
    if (u) bson_destroy(u);
    if (wc) mongoc_write_concern_destroy(wc);
    fd_decref(opts);
    if (success) return FD_TRUE;
    else if (no_error) {
      if ((q)&&(u))
        u8_logf(LOG_ERR,"mongodb_update",
               "Error on %s>%s: %s\n\twith query\nquery =  %q\nupdate =  %q\nflags = %q",
                db->dburi,domain->collection_name,error.message,query,update,opts);
      else u8_logf(LOG_ERR,"mongodb_update",
                   "Error on %s>%s:  %s\n\twith query\nquery =  %q\nupdate =  %q\nflags = %q",
                   db->dburi,domain->collection_name,error.message,query,update,opts);
      return FD_FALSE;}
    else if ((q)&&(u)) {
      u8_byte buf[1000];
      fd_seterr(fd_MongoDB_Error,"mongodb_update/call",
                u8_sprintf(buf,1000,"%s (%s>%s)",
                           error.message,db->dburi,domain->collection_name),
                fd_make_pair(query,update));}
    else {
      u8_byte buf[1000];
      fd_seterr(fd_BSON_Error,"mongodb_update/prep",
                u8_sprintf(buf,1000,"%s (%s>%s)",
                           error.message,db->dburi,domain->collection_name),
                fd_make_pair(query,update));}
    return FD_ERROR_VALUE;}
  else {
    fd_decref(opts);
    return FD_ERROR_VALUE;}
}

static lispval mongodb_update(lispval arg,lispval query,lispval update,
                              lispval opts_arg)
{
  return mongodb_updater(arg,query,update,0,opts_arg);
}


static lispval mongodb_upsert(lispval arg,lispval query,lispval update,
                              lispval opts_arg)
{
  return mongodb_updater(arg,query,update,(MONGOC_UPDATE_UPSERT),opts_arg);
}

#if HAVE_MONGOC_OPTS_FUNCTIONS
static lispval mongodb_find(lispval arg,lispval query,lispval opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,domain->domain_flags);
  lispval opts = combine_opts(opts_arg,domain->domain_opts);
  mongoc_client_t *client = NULL;
  mongoc_collection_t *collection = open_collection(domain,&client,flags);
  if (collection) {
    lispval results = FD_EMPTY_CHOICE;
    mongoc_cursor_t *cursor = NULL;
    const bson_t *doc;
    bson_t *q = fd_lisp2bson(query,flags,opts);
    bson_t *findopts = get_search_opts(opts,flags,FD_FIND_MATCHES);
    mongoc_read_prefs_t *rp = get_read_prefs(opts);
    lispval *vec = NULL; size_t n = 0, max = 0;
    int sort_results = fd_testopt(opts,FDSYM_SORTED,FD_VOID);
    if ((logops)||(flags&FD_MONGODB_LOGOPS))
      u8_logf(LOG_DETAIL,"MongoDB/find","Matches to %q in %q",query,arg);
    if (q)
      cursor = mongoc_collection_find_with_opts(collection,q,findopts,rp);
    if (cursor) {
      while (mongoc_cursor_next(cursor,&doc)) {
        /* u8_string json = bson_as_json(doc,NULL); */
        lispval r = fd_bson2dtype((bson_t *)doc,flags,opts);
        if (FD_ABORTP(r)) {
          fd_decref(results);
          free_dtype_vec(vec,n);
          results = FD_ERROR_VALUE;
          sort_results = 0;}
        else if (sort_results) {
          if (n>=max) {
            if (!(grow_dtype_vec(&vec,n,&max))) {
              free_dtype_vec(vec,n);
              results = FD_ERROR_VALUE;
              sort_results = 0;
              break;}}
          vec[n++]=r;}
        else {
          FD_ADD_TO_CHOICE(results,r);}}
      bson_error_t err;
      bool trouble = mongoc_cursor_error(cursor,&err);
      if (trouble) {
        free_dtype_vec(vec,n);
        grab_mongodb_error(&err,"mongodb_bind");
        mongoc_cursor_destroy(cursor);
        fd_decref(opts);
        return FD_ERROR;}
      else mongoc_cursor_destroy(cursor);}
    else {
      u8_byte buf[1000];
      fd_seterr(fd_MongoDB_Error,"mongodb_find",
                u8_sprintf(buf,1000,
                           "couldn't get query cursor over %q with options:\n%Q",
                           arg,opts),
                fd_incref(query));
      results = FD_ERROR_VALUE;}
    if (rp) mongoc_read_prefs_destroy(rp);
    if (q) bson_destroy(q);
    if (findopts) bson_destroy(findopts);
    collection_done(collection,client,domain);
    fd_decref(opts);
    U8_CLEAR_ERRNO();
    if (FD_ABORTED(results)) {}
    else if (sort_results) {
      if ((vec == NULL)||(n==0)) return fd_make_vector(0,NULL);
      else results = fd_make_vector(n,vec);
      if (vec) u8_free(vec);}
    return results;}
  else {
    fd_decref(opts);
    U8_CLEAR_ERRNO();
    return FD_ERROR_VALUE;}
}
#else
static lispval mongodb_find(lispval arg,lispval query,lispval opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,domain->domain_flags);
  lispval opts = combine_opts(opts_arg,domain->domain_opts);
  mongoc_client_t *client = NULL;
  mongoc_collection_t *collection = open_collection(domain,&client,flags);
  if (collection) {
    lispval results = FD_EMPTY_CHOICE;
    mongoc_cursor_t *cursor = NULL;
    const bson_t *doc;
    lispval skip_arg = fd_getopt(opts,skipsym,FD_FIXZERO);
    lispval limit_arg = fd_getopt(opts,limitsym,FD_FIXZERO);
    lispval batch_arg = fd_getopt(opts,batchsym,FD_FIXZERO);
    int sort_results = fd_testopt(opts,FDSYM_SORTED,FD_VOID);
    lispval *vec = NULL; size_t n = 0, max = 0;
    if ((FD_UINTP(skip_arg))&&(FD_UINTP(limit_arg))&&(FD_UINTP(batch_arg))) {
      bson_t *q = fd_lisp2bson(query,flags,opts);
      bson_t *fields = get_projection(opts,flags);
      mongoc_read_prefs_t *rp = get_read_prefs(opts);
      if ((logops)||(flags&FD_MONGODB_LOGOPS))
        u8_logf(LOG_DETAIL,"MongoDB/find","Matches to %q in %q",query,arg);
      if (q) cursor = mongoc_collection_find
               (collection,MONGOC_QUERY_NONE,
                FD_FIX2INT(skip_arg),
                FD_FIX2INT(limit_arg),
                FD_FIX2INT(batch_arg),
                q,
                fields,
                rp);
      if (cursor) {
        while (mongoc_cursor_next(cursor,&doc)) {
          /* u8_string json = bson_as_json(doc,NULL); */
          lispval r = fd_bson2dtype((bson_t *)doc,flags,opts);
          if (FD_ABORTP(r)) {
            fd_decref(results);
            free_dtype_vec(vec,n);
            results = FD_ERROR_VALUE;
            sort_results = 0;}
          else if (sort_results) {
            if (n>=max) {
              if (!(grow_dtype_vec(&vec,n,&max))) {
                free_dtype_vec(vec,n);
                results = FD_ERROR_VALUE;
                sort_results = 0;
                break;}}
            vec[n++]=r;}
          else {
            FD_ADD_TO_CHOICE(results,r);}}
        mongoc_cursor_destroy(cursor);}
      else results = fd_err(fd_MongoDB_Error,"mongodb_find","couldn't get cursor",opts);
      if (rp) mongoc_read_prefs_destroy(rp);
      if (q) bson_destroy(q);
      if (fields) bson_destroy(fields);}
    else {
      results = fd_err(fd_TypeError,"mongodb_find","bad skip/limit/batch",opts);
      sort_results = 0;}
    collection_done(collection,client,domain);
    U8_CLEAR_ERRNO();
    fd_decref(opts);
    fd_decref(skip_arg);
    fd_decref(limit_arg);
    fd_decref(batch_arg);
    if (sort_results) {
      if ((vec == NULL)||(n==0)) return fd_make_vector(0,NULL);
      else results = fd_make_vector(n,vec);
      if (vec) u8_free(vec);}
    return results;}
  else {
    fd_decref(opts);
    U8_CLEAR_ERRNO();
    return FD_ERROR_VALUE;}
}
#endif

#if HAVE_MONGOC_COUNT_DOCUMENTS
static lispval mongodb_count(lispval arg,lispval query,lispval opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,domain->domain_flags);
  lispval opts = combine_opts(opts_arg,domain->domain_opts);
  mongoc_client_t *client = NULL;
  mongoc_collection_t *collection = open_collection(domain,&client,flags);
  if (collection) {
    lispval result = FD_VOID;
    long n_documents = -1;
    bson_error_t error = { 0 };
    bson_t *q = fd_lisp2bson(query,flags,opts);
    if (q == NULL) {
      collection_done(collection,client,domain);
      fd_decref(opts);
      return FD_ERROR_VALUE;}
    bson_t *findopts = get_search_opts(opts,flags,FD_COUNT_MATCHES);
    mongoc_read_prefs_t *rp = get_read_prefs(opts);
    if ((logops)||(flags&FD_MONGODB_LOGOPS))
      u8_logf(LOG_DETAIL,"MongoDB/count","Counting matches to %q in %q",query,
              arg);
    n_documents = mongoc_collection_count_documents
      (collection,q,findopts,rp,NULL,&error);
    if (n_documents>=0) 
      result = FD_INT(n_documents);
    else {
      u8_byte buf[1000];
      fd_seterr(fd_MongoDB_Error,"mongodb_count",
                u8_sprintf(buf,1000,
                           "(%s) couldn't count documents in %s matching\n"
                           "%Q\n given options:\n%Q",
                           error.message,domain->collection_name,
                           query,opts),
                fd_incref(query));
      result = FD_ERROR_VALUE;}
    if (rp) mongoc_read_prefs_destroy(rp);
    if (q) bson_destroy(q);
    if (findopts) bson_destroy(findopts);
    collection_done(collection,client,domain);
    fd_decref(opts);
    U8_CLEAR_ERRNO();
    return result;}
  else {
    fd_decref(opts);
    U8_CLEAR_ERRNO();
    return FD_ERROR_VALUE;}
}
#else
static lispval mongodb_count(lispval arg,lispval query,lispval opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
  struct FD_MONGODB_DATABASE *db = DOMAIN2DB(domain);
  int flags = getflags(opts_arg,domain->domain_flags);
  lispval opts = combine_opts(opts_arg,domain->domain_opts);
  mongoc_client_t *client = NULL;
  mongoc_collection_t *collection = open_collection(domain,&client,flags);
  if (collection) {
    lispval result = FD_VOID;
    long n_documents = -1;
    bson_error_t err = { 0 };
    bson_t *q = fd_lisp2bson(query,flags,opts);
    if (q == NULL) {
      collection_done(collection,client,domain);
      fd_decref(opts);
      return FD_ERROR;}
    lispval skip_arg = fd_getopt(opts,skipsym,FD_FIXZERO);
    lispval limit_arg = fd_getopt(opts,limitsym,FD_FIXZERO);
     if ((FD_UINTP(skip_arg))&&(FD_UINTP(limit_arg))) {
      bson_t *fields = get_projection(opts,flags);
      mongoc_read_prefs_t *rp = get_read_prefs(opts);
      if ((logops)||(flags&FD_MONGODB_LOGOPS))
        u8_logf(LOG_DETAIL,"MongoDB/find","Matches to %q in %q",query,arg);
      n_documents = mongoc_collection_count
        (collection,MONGOC_QUERY_NONE,
         q,
         FD_FIX2INT(skip_arg),
         FD_FIX2INT(limit_arg),
         rp,
         &err);
      if (n_documents >= 0)
        result = FD_INT(n_documents);
      else {
        u8_byte buf[1000];
        if (errno) u8_graberrno("mongodb_count",NULL);
        fd_seterr(fd_MongoDB_Error,"mongodb_count",
                  u8_sprintf(buf,1000,"%s (%s>%s)",
                             err.message,db->dburi,domain->collection_name),
                  fd_incref(query));
        result = FD_ERROR_VALUE;}
      if (rp) mongoc_read_prefs_destroy(rp);
      if (q) bson_destroy(q);
      if (fields) bson_destroy(fields);}
    else result = fd_err(fd_TypeError,"mongodb_find","bad skip/limit/batch",opts);
    collection_done(collection,client,domain);
    U8_CLEAR_ERRNO();
    fd_decref(opts);
    fd_decref(skip_arg);
    fd_decref(limit_arg);
    return result;}
  else {
    fd_decref(opts);
    U8_CLEAR_ERRNO();
    return FD_ERROR_VALUE;}
}
#endif

#if HAVE_MONGOC_OPTS_FUNCTIONS
static lispval mongodb_get(lispval arg,lispval query,lispval opts_arg)
{
  lispval result = FD_EMPTY_CHOICE;
  struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,domain->domain_flags);
  lispval opts = combine_opts(opts_arg,domain->domain_opts);
  mongoc_client_t *client = NULL;
  mongoc_collection_t *collection = open_collection(domain,&client,flags);
  if (collection) {
    mongoc_cursor_t *cursor;
    const bson_t *doc;
    bson_t *q, *findopts = get_search_opts(opts,flags,FD_FIND_MATCHES);
    mongoc_read_prefs_t *rp = get_read_prefs(opts);
    if ((!(FD_OIDP(query)))&&(FD_TABLEP(query)))
      q = fd_lisp2bson(query,flags,opts);
    else {
      struct FD_BSON_OUTPUT out;
      out.bson_doc = bson_new();
      out.bson_flags = ((flags<0)?(getflags(opts,FD_MONGODB_DEFAULTS)):(flags));
      out.bson_opts = opts;
      bson_append_dtype(out,"_id",3,query,0);
      q = out.bson_doc;}
    if ((logops)||(flags&FD_MONGODB_LOGOPS))
      u8_logf(LOG_DETAIL,"MongoDB/get","Matches to %q in %q",query,arg);
    if (q) 
      cursor = mongoc_collection_find_with_opts(collection,q,findopts,rp);
    else cursor=NULL;
    if ((cursor)&&(mongoc_cursor_next(cursor,&doc))) {
      result = fd_bson2dtype((bson_t *)doc,flags,opts);}
    if (cursor) mongoc_cursor_destroy(cursor);
    if (rp) mongoc_read_prefs_destroy(rp);
    if (findopts) bson_destroy(findopts);
    if (q) bson_destroy(q);
    fd_decref(opts);
    collection_done(collection,client,domain);
    U8_CLEAR_ERRNO();
    return result;}
  else {
    fd_decref(opts);
    U8_CLEAR_ERRNO();
    return FD_ERROR_VALUE;}
}
#else
static lispval mongodb_get(lispval arg,lispval query,lispval opts_arg)
{
  lispval result = FD_EMPTY_CHOICE;
  struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,domain->domain_flags);
  lispval opts = combine_opts(opts_arg,domain->domain_opts);
  mongoc_client_t *client = NULL;
  mongoc_collection_t *collection = open_collection(domain,&client,flags);
  if (collection) {
    mongoc_cursor_t *cursor;
    const bson_t *doc;
    bson_t *q, *fields = get_projection(opts,flags);
    mongoc_read_prefs_t *rp = get_read_prefs(opts);
    if ((!(FD_OIDP(query)))&&(FD_TABLEP(query)))
      q = fd_lisp2bson(query,flags,opts);
    else {
      struct FD_BSON_OUTPUT out;
      out.bson_doc = bson_new();
      out.bson_flags = ((flags<0)?(getflags(opts,FD_MONGODB_DEFAULTS)):(flags));
      out.bson_opts = opts;
      bson_append_dtype(out,"_id",3,query,0);
      q = out.bson_doc;}
    if ((logops)||(flags&FD_MONGODB_LOGOPS))
      u8_logf(LOG_DETAIL,"MongoDB/get","Matches to %q in %q",query,arg);
    if (q) cursor = mongoc_collection_find
             (collection,MONGOC_QUERY_NONE,0,1,0,q,fields,NULL);
    if ((cursor)&&(mongoc_cursor_next(cursor,&doc))) {
      result = fd_bson2dtype((bson_t *)doc,flags,opts);}
    if (cursor) mongoc_cursor_destroy(cursor);
    if (rp) mongoc_read_prefs_destroy(rp);
    if (q) bson_destroy(q);
    if (fields) bson_destroy(fields);
    fd_decref(opts);
    collection_done(collection,client,domain);
    U8_CLEAR_ERRNO();
    return result;}
  else {
    fd_decref(opts);
     U8_CLEAR_ERRNO();
    return FD_ERROR_VALUE;}
}
#endif

/* Find and Modify */

static int getnewopt(lispval opts,int dflt);

static lispval mongodb_modify(lispval arg,lispval query,lispval update,
                             lispval opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
  struct FD_MONGODB_DATABASE *db = DOMAIN2DB(domain);
  int flags = getflags(opts_arg,domain->domain_flags);
  lispval opts = combine_opts(opts_arg,domain->domain_opts);
  mongoc_client_t *client;
  mongoc_collection_t *collection = open_collection(domain,&client,flags);
  if (collection) {
    lispval result = FD_VOID;
    lispval sort = fd_getopt(opts,FDSYM_SORT,FD_VOID);
    lispval fields = fd_getopt(opts,fieldssym,FD_VOID);
    lispval upsert = fd_getopt(opts,upsertsym,FD_FALSE);
    lispval remove = fd_getopt(opts,removesym,FD_FALSE);
    int return_new = getnewopt(opts,1);
    bson_t *q = fd_lisp2bson(query,flags,opts);
    bson_t *u = fd_lisp2bson(update,flags,opts);
    if ((q == NULL)||(u == NULL)) {
      U8_CLEAR_ERRNO();
      return FD_ERROR_VALUE;}
    if ((logops)||(flags&FD_MONGODB_LOGOPS))
      u8_logf(LOG_DETAIL,"MongoDB/find+modify","Matches to %q using %q in %q",
             query,update,arg);
    bson_t reply; bson_error_t error = { 0 };
    if (mongoc_collection_find_and_modify
        (collection,
         q,fd_lisp2bson(sort,flags,opts),
         u,fd_lisp2bson(fields,flags,opts),
         ((FD_FALSEP(remove))?(false):(true)),
         ((FD_FALSEP(upsert))?(false):(true)),
         ((return_new)?(true):(false)),
         &reply,&error)) {
      result = fd_bson2dtype(&reply,flags,opts);}
    else {
      u8_byte buf[1000];
      fd_seterr(fd_MongoDB_Error,"mongodb_modify",
                u8_sprintf(buf,1000,"%s (%s>%s)",
                           error.message,db->dburi,domain->collection_name),
                fd_make_pair(query,update));
      result = FD_ERROR_VALUE;}
    collection_done(collection,client,domain);
    if (q) bson_destroy(q);
    if (u) bson_destroy(u);
    bson_destroy(&reply);
    fd_decref(opts);
    fd_decref(fields);
    fd_decref(upsert);
    fd_decref(remove);
    U8_CLEAR_ERRNO();
    return result;}
  else {
    fd_decref(opts);
    U8_CLEAR_ERRNO();
    return FD_ERROR_VALUE;}
}

static int getnewopt(lispval opts,int dflt)
{
  lispval v = fd_getopt(opts,newsym,FD_VOID);
  if (FD_VOIDP(v)) {
    v = fd_getopt(opts,originalsym,FD_VOID);
    if (FD_VOIDP(v))
      return dflt;
    else if (FD_FALSEP(v))
      return 1;
    else {
      fd_decref(v);
      return 0;}}
  else if (FD_FALSEP(v))
    return 0;
  else {
    fd_decref(v);
    return 1;}
}

/* Command execution */

static lispval make_mongovec(lispval vec);

static lispval make_command(int n,lispval *values)
{
  if ((n%2)==1)
    return fd_err(fd_SyntaxError,"make_command","Odd number of arguments",FD_VOID);
  else {
    lispval result = fd_make_slotmap(n/2,n/2,NULL);
    struct FD_KEYVAL *keyvals = FD_SLOTMAP_KEYVALS(result);
    int n_slots=n/2;
    int i = 0; while (i<n_slots) {
      lispval key = values[i*2];
      lispval value = values[i*2+1];
      keyvals[i].kv_key=fd_incref(key);
      if (FD_VECTORP(value))
        keyvals[i].kv_val=make_mongovec(value);
      else keyvals[i].kv_val=fd_incref(value);
      i++;}
    return result;}
}

static lispval collection_command(lispval arg,lispval command,lispval opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,domain->domain_flags);
  lispval opts = combine_opts(opts_arg,domain->domain_opts);
  lispval fields = fd_get(opts,fieldssym,FD_VOID);
  mongoc_client_t *client;
  mongoc_collection_t *collection = open_collection(domain,&client,flags);
  if (collection) {U8_CLEAR_ERRNO();}
  if (collection) {
    lispval results = FD_EMPTY_CHOICE;
    bson_t *cmd = fd_lisp2bson(command,flags,opts);
    bson_t *flds = fd_lisp2bson(fields,flags,opts);
    if (cmd) {
      const bson_t *doc;
      lispval skip_arg = fd_getopt(opts,skipsym,FD_FIXZERO);
      lispval limit_arg = fd_getopt(opts,limitsym,FD_FIXZERO);
      lispval batch_arg = fd_getopt(opts,batchsym,FD_FIXZERO);
      if ((FD_UINTP(skip_arg))&&
          (FD_UINTP(limit_arg))&&
          (FD_UINTP(batch_arg))) {
        mongoc_cursor_t *cursor = mongoc_collection_command
          (collection,MONGOC_QUERY_EXHAUST,
           (FD_FIX2INT(skip_arg)),
           (FD_FIX2INT(limit_arg)),
           (FD_FIX2INT(batch_arg)),
           cmd,flds,NULL);
        if (cursor) {
          U8_CLEAR_ERRNO();
          while ((cursor) && (mongoc_cursor_next(cursor,&doc))) {
            lispval r = fd_bson2dtype((bson_t *)doc,flags,opts);
            FD_ADD_TO_CHOICE(results,r);}
          mongoc_cursor_destroy(cursor);}
        else results=FD_ERROR_VALUE;}
      else results = fd_err(fd_TypeError,"collection_command",
                            "bad skip/limit/batch",opts);
      mongoc_collection_destroy(collection);
      client_done(arg,client);
      if (cmd) bson_destroy(cmd);
      if (flds) bson_destroy(flds);
      fd_decref(opts);
      fd_decref(fields);
      U8_CLEAR_ERRNO();
      return results;}
    else {
      return FD_ERROR_VALUE;}}
  else return FD_ERROR_VALUE;
}

static lispval db_command(lispval arg,lispval command,
                         lispval opts_arg)
{
  struct FD_MONGODB_DATABASE *srv = (struct FD_MONGODB_DATABASE *)arg;
  int flags = getflags(opts_arg,srv->dbflags);
  lispval opts = combine_opts(opts_arg,srv->dbopts);
  lispval fields = fd_getopt(opts,fieldssym,FD_VOID);
  mongoc_client_t *client = get_client(srv,MONGODB_CLIENT_BLOCK);
  if (client) {U8_CLEAR_ERRNO();}
  if (client) {
    lispval results = FD_EMPTY_CHOICE;
    bson_t *cmd = fd_lisp2bson(command,flags,opts);
    bson_t *flds = fd_lisp2bson(fields,flags,opts);
    if (cmd) {
      const bson_t *doc;
      lispval skip_arg = fd_getopt(opts,skipsym,FD_FIXZERO);
      lispval limit_arg = fd_getopt(opts,limitsym,FD_FIXZERO);
      lispval batch_arg = fd_getopt(opts,batchsym,FD_FIXZERO);
      if ((FD_UINTP(skip_arg))&&
          (FD_UINTP(limit_arg))&&
          (FD_UINTP(batch_arg))) {
        mongoc_cursor_t *cursor = mongoc_client_command
          (client,srv->dbname,MONGOC_QUERY_EXHAUST,
           (FD_FIX2INT(skip_arg)),
           (FD_FIX2INT(limit_arg)),
           (FD_FIX2INT(batch_arg)),
           cmd,flds,NULL);
        if (cursor) {
          U8_CLEAR_ERRNO();
          while (mongoc_cursor_next(cursor,&doc)) {
            lispval r = fd_bson2dtype((bson_t *)doc,flags,opts);
            FD_ADD_TO_CHOICE(results,r);}
          mongoc_cursor_destroy(cursor);}
        else {
          fd_decref(results);
          results=FD_ERROR_VALUE;}}
      else results = fd_err(fd_TypeError,"collection_command",
                            "bad skip/limit/batch",opts);
      fd_decref(skip_arg);
      fd_decref(limit_arg);
      fd_decref(batch_arg);
      client_done(arg,client);
      if (cmd) bson_destroy(cmd);
      if (flds) bson_destroy(flds);
      fd_decref(opts);
      fd_decref(fields);
      return results;}
    else {
      return FD_ERROR_VALUE;}}
  else return FD_ERROR_VALUE;
}

static lispval mongodb_command(int n,lispval *args)
{
  lispval arg = args[0], opts = FD_VOID, command = FD_VOID, result = FD_VOID;
  int flags = mongodb_getflags(arg);
  if (flags<0)
    return fd_type_error(_("MongoDB"),"mongodb_command",arg);
  else if (n==2) {
    command = args[1]; fd_incref(command); opts = FD_VOID;}
  else if ((n==3)&&(FD_TABLEP(args[1]))) {
    command = args[1]; fd_incref(command); opts = args[2];}
  else if (n%2) {
    command = make_command(n-1,args+1);}
  else {
    command = make_command(n-2,args+2);
    opts = args[1];}
  if ((logops)||(flags&FD_MONGODB_LOGOPS)) {
    u8_logf(LOG_DEBUG,"MongoDB/RESULTS","At %q: %q",arg,command);}
  if (FD_TYPEP(arg,fd_mongoc_server))
    result = db_command(arg,command,opts);
  else if (FD_TYPEP(arg,fd_mongoc_collection))
    result = collection_command(arg,command,opts);
  else {}
  fd_decref(command);
  return result;
}

static lispval collection_simple_command(lispval arg,lispval command,
                                        lispval opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,domain->domain_flags);
  lispval opts = combine_opts(opts_arg,domain->domain_opts);
  bson_t *cmd = fd_lisp2bson(command,flags,opts);
  if (cmd) {
    mongoc_client_t *client;
    mongoc_collection_t *collection = open_collection(domain,&client,flags);
    if (collection) {U8_CLEAR_ERRNO();}
    if (collection) {
      bson_t response; bson_error_t error;
      if (mongoc_collection_command_simple
          (collection,cmd,NULL,&response,&error)) {
        lispval result = fd_bson2dtype(&response,flags,opts);
        collection_done(collection,client,domain);
        bson_destroy(cmd);
        fd_decref(opts);
        return result;}
      else {
        grab_mongodb_error(&error,"collection_simple_command");
        collection_done(collection,client,domain);
        bson_destroy(cmd);
        fd_decref(opts);
        return FD_ERROR_VALUE;}}
    else {
      bson_destroy(cmd);
      fd_decref(opts);
      return FD_ERROR_VALUE;}}
  else {
    fd_decref(opts);
    return FD_ERROR_VALUE;}
}

static lispval db_simple_command(lispval arg,lispval command,
                                lispval opts_arg)
{
  struct FD_MONGODB_DATABASE *srv = (struct FD_MONGODB_DATABASE *)arg;
  int flags = getflags(opts_arg,srv->dbflags);
  lispval opts = combine_opts(opts_arg,srv->dbopts);
  mongoc_client_t *client = get_client(srv,MONGODB_CLIENT_BLOCK);
  if (client) {
    bson_t response; bson_error_t error;
    bson_t *cmd = fd_lisp2bson(command,flags,opts);
    if (cmd) {
      if (mongoc_client_command_simple
          (client,srv->dbname,cmd,NULL,&response,&error)) {
        lispval result = fd_bson2dtype(&response,flags,opts);
        client_done(arg,client);
        bson_destroy(cmd);
        fd_decref(opts);
        return result;}
      else {
        grab_mongodb_error(&error,"db_simple_command");
        client_done(arg,client);
        bson_destroy(cmd);
        fd_decref(opts);
        return FD_ERROR_VALUE;}}
    else {
      return FD_ERROR_VALUE;}}
  else return FD_ERROR_VALUE;
}

static lispval mongodb_simple_command(int n,lispval *args)
{
  lispval arg = args[0], opts = FD_VOID, command = FD_VOID, result = FD_VOID;
  int flags = mongodb_getflags(arg);
  if (flags<0) return fd_type_error(_("MongoDB"),"mongodb_command",arg);
  else if (n==2) {
    command = args[1]; fd_incref(command); opts = FD_VOID;}
  else if ((n==3)&&(FD_TABLEP(args[1]))) {
    command = args[1]; fd_incref(command); opts = args[2];}
  else if (n%2) {
    command = make_command(n-1,args+1);}
  else {
    command = make_command(n-2,args+2);
    opts = args[1];}
  if ((logops)||(flags&FD_MONGODB_LOGOPS)) {
    u8_logf(LOG_DEBUG,"MongoDB/DO","At %q: %q",arg,command);}
  if (FD_TYPEP(arg,fd_mongoc_server))
    result = db_simple_command(arg,command,opts);
  else if (FD_TYPEP(arg,fd_mongoc_collection))
    result = collection_simple_command(arg,command,opts);
  else {}
  fd_decref(command);
  return result;
}

/* Cursor creation */

#if HAVE_MONGOC_OPTS_FUNCTIONS
static lispval mongodb_cursor(lispval arg,lispval query,lispval opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,domain->domain_flags);
  lispval opts = combine_opts(opts_arg,domain->domain_opts);
  mongoc_client_t *connection;
  mongoc_cursor_t *cursor = NULL;
  mongoc_collection_t *collection = open_collection(domain,&connection,flags);
  bson_t *bq = fd_lisp2bson(query,flags,opts);
  bson_t *findopts = get_search_opts(opts,flags,FD_FIND_MATCHES);
  mongoc_read_prefs_t *rp = get_read_prefs(opts);
  if (collection) {
    cursor = mongoc_collection_find_with_opts
      (collection,bq,findopts,rp);}
  if (cursor) {
    struct FD_MONGODB_CURSOR *consed = u8_alloc(struct FD_MONGODB_CURSOR);
    FD_INIT_CONS(consed,fd_mongoc_cursor);
    consed->cursor_domain = arg; fd_incref(arg);
    consed->cursor_db = domain->domain_db;
    fd_incref(domain->domain_db);
    consed->cursor_query = query; fd_incref(query);
    consed->cursor_query_bson = bq;
    consed->cursor_value_bson = NULL;
    consed->cursor_readprefs = rp;
    consed->cursor_flags = flags;
    consed->cursor_done  = 0;
    consed->cursor_opts = opts;
    consed->cursor_connection = connection;
    consed->cursor_collection = collection;
    consed->mongoc_cursor = cursor;
   return (lispval) consed;}
  else {
    fd_decref(opts);
    if (rp) mongoc_read_prefs_destroy(rp);
    if (findopts) bson_destroy(findopts);
    if (bq) bson_destroy(bq);
    if (collection) collection_done(collection,connection,domain);
    return FD_ERROR_VALUE;}
}
#else
static lispval mongodb_cursor(lispval arg,lispval query,lispval opts_arg)
{
  struct FD_MONGODB_COLLECTION *domain = (struct FD_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,domain->domain_flags);
  lispval opts = combine_opts(opts_arg,domain->domain_opts);
  mongoc_client_t *connection;
  mongoc_cursor_t *cursor = NULL;
  mongoc_collection_t *collection = open_collection(domain,&connection,flags);
  lispval skip_arg = fd_getopt(opts,skipsym,FD_FIXZERO);
  lispval limit_arg = fd_getopt(opts,limitsym,FD_FIXZERO);
  lispval batch_arg = fd_getopt(opts,batchsym,FD_FIXZERO);
  bson_t *bq = fd_lisp2bson(query,flags,opts);
  bson_t *fields = get_projection(opts,flags);
  mongoc_read_prefs_t *rp = get_read_prefs(opts);
  if ((collection)&&
      (FD_UINTP(skip_arg))&&
      (FD_UINTP(limit_arg))&&
      (FD_UINTP(batch_arg))) {
    cursor = mongoc_collection_find
      (collection,MONGOC_QUERY_NONE,
       FD_FIX2INT(skip_arg),
       FD_FIX2INT(limit_arg),
       FD_FIX2INT(batch_arg),
       bq,fields,rp);}
  if (cursor) {
    struct FD_MONGODB_CURSOR *consed = u8_alloc(struct FD_MONGODB_CURSOR);
    FD_INIT_CONS(consed,fd_mongoc_cursor);
    consed->cursor_domain = arg; fd_incref(arg);
    consed->cursor_db = domain->domain_db;
    fd_incref(domain->domain_db);
    consed->cursor_query = query; fd_incref(query);
    consed->cursor_query_bson = bq;
    consed->cursor_value_bson = NULL;
    consed->cursor_opts_bson = fields;
    consed->cursor_readprefs = rp;
    consed->cursor_connection = connection;
    consed->cursor_collection = collection;
    consed->mongoc_cursor = cursor;
   return (lispval) consed;}
  else {
    fd_decref(skip_arg); fd_decref(limit_arg); fd_decref(batch_arg);
    fd_decref(opts);
    if (bq) bson_destroy(bq);
    if (fields) bson_destroy(fields);
    if (collection) collection_done(collection,connection,domain);
    return FD_ERROR_VALUE;}
}
#endif

static void recycle_cursor(struct FD_RAW_CONS *c)
{
  struct FD_MONGODB_CURSOR *cursor = (struct FD_MONGODB_CURSOR *)c;
  struct FD_MONGODB_COLLECTION *domain = CURSOR2DOMAIN(cursor);
  struct FD_MONGODB_DATABASE *s = DOMAIN2DB(domain);
  mongoc_cursor_destroy(cursor->mongoc_cursor);
  mongoc_collection_destroy(cursor->cursor_collection);
  release_client(s,cursor->cursor_connection);
  fd_decref(cursor->cursor_domain);
  fd_decref(cursor->cursor_query);
  fd_decref(cursor->cursor_opts);
  if (cursor->cursor_query_bson)
    bson_destroy(cursor->cursor_query_bson);
  if (cursor->cursor_opts_bson)
    bson_destroy(cursor->cursor_opts_bson);
  if (cursor->cursor_readprefs)
    mongoc_read_prefs_destroy(cursor->cursor_readprefs);
  cursor->cursor_value_bson = NULL;
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}
static int unparse_cursor(struct U8_OUTPUT *out,lispval x)
{
  struct FD_MONGODB_CURSOR *cursor = (struct FD_MONGODB_CURSOR *)x;
  struct FD_MONGODB_COLLECTION *domain = CURSOR2DOMAIN(cursor);
  struct FD_MONGODB_DATABASE *db = DOMAIN2DB(domain);
  u8_printf(out,"#<MongoDB/Cursor '%s/%s' %q>",
            db->dbname,domain->collection_name,cursor->cursor_query);
  return 1;
}

/* Operations on cursors */

static int cursor_advance(struct FD_MONGODB_CURSOR *c,u8_context caller)
{
  if (c->cursor_done) return 0;
  bool ok = mongoc_cursor_next(c->mongoc_cursor,&(c->cursor_value_bson));
  if (ok) return 1;
  bson_error_t err;
  ok = mongoc_cursor_error(c->mongoc_cursor,&err);
  if (ok) {
    grab_mongodb_error(&err,caller);
    return -1;}
  else {
    c->cursor_done = 1;
    return 0;}
}

static lispval mongodb_donep(lispval cursor)
{
  struct FD_MONGODB_CURSOR *c = (struct FD_MONGODB_CURSOR *)cursor;
  if (c->cursor_value_bson)
    return FD_TRUE;
  int rv = cursor_advance(c,"mongodb_donep");
  if (rv == 0)
    return FD_TRUE;
  else if (rv == 1)
    return FD_FALSE;
  else return FD_ERROR;
}

static lispval mongodb_skip(lispval cursor,lispval howmany)
{
  struct FD_MONGODB_CURSOR *c = (struct FD_MONGODB_CURSOR *)cursor;
  if (!(FD_UINTP(howmany)))
    return fd_type_error("uint","mongodb_skip",howmany);
  int n = FD_FIX2INT(howmany), i = 0;
  while  ((i<n) && (cursor_advance(c,"mongodb_skip") > 0)) i++;
  if (i == n)
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval mongodb_cursor_reader(lispval cursor,lispval howmany,
                                     lispval opts_arg,int sorted)
{
  struct FD_MONGODB_CURSOR *c = (struct FD_MONGODB_CURSOR *)cursor;
  if (!(FD_UINTP(howmany)))
    return fd_type_error("uint","mongodb_skip",howmany);
  int i = 0, n = FD_FIX2INT(howmany);
  int flags = getflags(opts_arg,c->cursor_flags);
  if (sorted < 0) sorted = fd_testopt(opts_arg,FDSYM_SORTED,FD_VOID);
  if (n == 0) {
    if (sorted)
      return fd_make_vector(0,NULL);
    else return FD_EMPTY_CHOICE;}
  else if ( (n == 1) && (c->cursor_value_bson != NULL) ) {
    lispval r = fd_bson2dtype((bson_t *)c->cursor_value_bson,flags,opts_arg);
    c->cursor_value_bson = NULL;
    if (sorted)
      return fd_make_vector(1,&r);
    else return r;}
  else {
    lispval vec[n];
    mongoc_cursor_t *scan = c->mongoc_cursor;
    const bson_t *doc;
    lispval opts = combine_opts(opts_arg,c->cursor_opts);
    if (c->cursor_value_bson != NULL) {
      lispval v = fd_bson2dtype(((bson_t *)c->cursor_value_bson),flags,opts);
      c->cursor_value_bson=NULL;
      vec[i++] = v;}
    while ( (i < n) && (mongoc_cursor_next(scan,&doc)) ) {
      /* u8_string json = bson_as_json(doc,NULL); */
      lispval r = fd_bson2dtype((bson_t *)doc,flags,opts);
      if (!(FD_VOIDP(opts_arg))) fd_decref(opts);
      if (FD_ABORTP(r)) {
        fd_decref_elts(vec,i);
        return FD_ERROR;}
      else vec[i++] = r;}
    if (i < n) {
      bson_error_t err;
      int cursor_err = mongoc_cursor_error(scan,&err);
      if (cursor_err) {
        grab_mongodb_error(&err,"mongodb");
        fd_decref_vec(vec,i);
        return FD_ERROR;}
      else c->cursor_done=1;}
    if (sorted) {
      if (i == 0)
        return fd_make_vector(0,NULL);
      else return fd_make_vector(i,vec);}
    else if (i == 0)
      return FD_EMPTY_CHOICE;
    else if (i == 1)
      return vec[0];
    else return fd_init_choice(NULL,i,vec,FD_CHOICE_DOSORT|FD_CHOICE_COMPRESS);}
}

static lispval mongodb_cursor_read(lispval cursor,lispval howmany,
                                   lispval opts_arg)
{
  return mongodb_cursor_reader(cursor,howmany,opts_arg,-1);
}


static lispval mongodb_cursor_read_vector(lispval cursor,lispval howmany,
                                          lispval opts_arg)
{
  return mongodb_cursor_reader(cursor,howmany,opts_arg,1);
}

/* BSON output functions */

static bool bson_append_dtype(struct FD_BSON_OUTPUT b,
                              const char *key,int keylen,
                              lispval val,
                              int vecslot)
{
  bson_t *out = b.bson_doc;
  int flags = b.bson_flags;
  bool ok = true;
  if ( (vecslot) && (FD_CHOICEP(val)) ) vecslot = 0;
  if (vecslot) {
    struct FD_BSON_OUTPUT wrapper_out = { 0 };
    bson_t values;
    ok = bson_append_array_begin(out,key,keylen,&values);
    wrapper_out.bson_doc = &values;
    wrapper_out.bson_flags = b.bson_flags;
    wrapper_out.bson_opts = b.bson_opts;
    wrapper_out.bson_fieldmap = b.bson_fieldmap;
    if (ok) ok = bson_append_dtype(wrapper_out,"0",1,val,0);
    if (ok) ok = bson_append_document_end(out,&values);
    return ok;}
  else if (FD_CONSP(val)) {
    fd_ptr_type ctype = FD_PTR_TYPE(val);
    switch (ctype) {
    case fd_string_type: {
      unsigned char _buf[64], *buf=_buf;
      u8_string str = FD_CSTRING(val); int len = FD_STRLEN(val);
      if ((flags&FD_MONGODB_COLONIZE)&&
          ((isdigit(str[0]))||(strchr(":(#@",str[0])!=NULL))) {
        if (len>62) buf = u8_malloc(len+2);
        buf[0]='\\'; strncpy(buf+1,str,len); buf[len+1]='\0';
        ok = bson_append_utf8(out,key,keylen,buf,len+1);
        if (buf!=_buf) u8_free(buf);}
      else ok = bson_append_utf8(out,key,keylen,str,len);
      break;}
    case fd_packet_type:
      ok = bson_append_binary(out,key,keylen,BSON_SUBTYPE_BINARY,
                              FD_PACKET_DATA(val),FD_PACKET_LENGTH(val));
      break;
    case fd_flonum_type: {
      double d = FD_FLONUM(val);
      ok = bson_append_double(out,key,keylen,d);
      break;}
    case fd_bigint_type: {
      fd_bigint b = fd_consptr(fd_bigint,val,fd_bigint_type);
      if (fd_bigint_fits_in_word_p(b,32,1)) {
        long int b32 = fd_bigint_to_long(b);
        ok = bson_append_int32(out,key,keylen,b32);}
      else if (fd_bigint_fits_in_word_p(b,65,1)) {
        long long int b64 = fd_bigint_to_long_long(b);
        ok = bson_append_int64(out,key,keylen,b64);}
      else {
        u8_logf(LOG_CRIT,fd_MongoDB_Warning,
               "Can't save bigint value %q",val);
        ok = bson_append_int32(out,key,keylen,0);}
      break;}
    case fd_timestamp_type: {
      struct FD_TIMESTAMP *fdt=
        fd_consptr(struct FD_TIMESTAMP* ,val,fd_timestamp_type);
      unsigned long long millis = (fdt->u8xtimeval.u8_tick*1000)+
        ((fdt->u8xtimeval.u8_prec>u8_second)?(fdt->u8xtimeval.u8_nsecs/1000000):(0));
      ok = bson_append_date_time(out,key,keylen,millis);
      break;}
    case fd_uuid_type: {
      struct FD_UUID *uuid = fd_consptr(struct FD_UUID *,val,fd_uuid_type);
      ok = bson_append_binary(out,key,keylen,BSON_SUBTYPE_UUID,uuid->uuid16,16);
      break;}
    case fd_choice_type: case fd_prechoice_type: {
      struct FD_BSON_OUTPUT rout;
      bson_t arr; char buf[16];
      ok = bson_append_array_begin(out,key,keylen,&arr);
      memset(&rout,0,sizeof(struct FD_BSON_OUTPUT));
      rout.bson_doc = &arr; rout.bson_flags = b.bson_flags;
      rout.bson_opts = b.bson_opts; rout.bson_fieldmap = b.bson_fieldmap;
      if (ok) {
        int i = 0; FD_DO_CHOICES(v,val) {
          sprintf(buf,"%d",i++);
          ok = bson_append_dtype(rout,buf,strlen(buf),v,0);
          if (!(ok)) FD_STOP_DO_CHOICES;}}
      bson_append_document_end(out,&arr);
      break;}
    case fd_vector_type: case fd_code_type: {
      struct FD_BSON_OUTPUT rout;
      bson_t arr, ch; char buf[16];
      struct FD_VECTOR *vec = (struct FD_VECTOR *)val;
      int i = 0, lim = vec->vec_length;
      lispval *data = vec->vec_elts;
      int wrap_vector = ((flags&FD_MONGODB_CHOICEVALS)&&(key[0]!='$'));
      if (wrap_vector) {
        ok = bson_append_array_begin(out,key,keylen,&ch);
        if (ok) ok = bson_append_array_begin(&ch,"0",1,&arr);}
      else ok = bson_append_array_begin(out,key,keylen,&arr);
      memset(&rout,0,sizeof(struct FD_BSON_OUTPUT));
      rout.bson_doc = &arr; rout.bson_flags = b.bson_flags;
      rout.bson_opts = b.bson_opts; rout.bson_fieldmap = b.bson_fieldmap;
      if (ok) while (i<lim) {
          lispval v = data[i]; sprintf(buf,"%d",i++);
          ok = bson_append_dtype(rout,buf,strlen(buf),v,0);
          if (!(ok)) break;}
      if (wrap_vector) {
        bson_append_array_end(&ch,&arr);
        bson_append_array_end(out,&ch);}
      else bson_append_array_end(out,&arr);
      break;}
    case fd_slotmap_type: case fd_hashtable_type: {
      struct FD_BSON_OUTPUT rout;
      bson_t doc;
      lispval keys = fd_getkeys(val);
      ok = bson_append_document_begin(out,key,keylen,&doc);
      memset(&rout,0,sizeof(struct FD_BSON_OUTPUT));
      rout.bson_doc = &doc; rout.bson_flags = b.bson_flags;
      rout.bson_opts = b.bson_opts; rout.bson_fieldmap = b.bson_fieldmap;
      if (ok) {
        FD_DO_CHOICES(key,keys) {
          lispval value = fd_get(val,key,FD_VOID);
          if (!(FD_VOIDP(value))) {
            ok = bson_append_keyval(rout,key,value);
            fd_decref(value);
            if (!(ok)) FD_STOP_DO_CHOICES;}}}
      fd_decref(keys);
      bson_append_document_end(out,&doc);
      break;}
    case fd_regex_type: {
      struct FD_REGEX *rx = (struct FD_REGEX *)val;
      char opts[8], *write = opts; int flags = rx->rxflags;
      if (flags&REG_EXTENDED) *write++='x';
      if (flags&REG_ICASE) *write++='i';
      if (flags&REG_NEWLINE) *write++='m';
      *write++='\0';
      bson_append_regex(out,key,keylen,rx->rxsrc,opts);
      break;}
    case fd_compound_type: {
      struct FD_COMPOUND *compound = FD_XCOMPOUND(val);
      lispval tag = compound->compound_typetag, *elts = FD_COMPOUND_ELTS(val);
      int len = FD_COMPOUND_LENGTH(val);
      struct FD_BSON_OUTPUT rout;
      bson_t doc;
      if (tag == mongovec_symbol)
        ok = bson_append_array_begin(out,key,keylen,&doc);
      else ok = bson_append_document_begin(out,key,keylen,&doc);
      memset(&rout,0,sizeof(struct FD_BSON_OUTPUT));
      rout.bson_doc = &doc; rout.bson_flags = b.bson_flags;
      rout.bson_opts = b.bson_opts; rout.bson_fieldmap = b.bson_fieldmap;
      if (tag == mongovec_symbol) {
        lispval *scan = elts, *limit = scan+len; int i = 0;
        while (scan<limit) {
          u8_byte buf[16]; sprintf(buf,"%d",i);
          ok = bson_append_dtype(rout,buf,strlen(buf),*scan,0);
          scan++; i++;
          if (!(ok)) break;}}
      else {
        int i = 0;
        ok = bson_append_dtype(rout,"%fdtag",6,tag,0);
        if (ok) while (i<len) {
            char buf[16]; sprintf(buf,"%d",i);
            ok = bson_append_dtype(rout,buf,strlen(buf),elts[i++],0);
            if (!(ok)) break;}}
      if (tag == mongovec_symbol)
        bson_append_array_end(out,&doc);
      else bson_append_document_end(out,&doc);
      break;}
    default: {
      struct U8_OUTPUT vout; unsigned char buf[128]; 
      U8_INIT_OUTPUT_BUF(&vout,128,buf);
      vout.u8_streaminfo |= U8_STREAM_VERBOSE;
      if (flags&FD_MONGODB_COLONIZE)
        u8_printf(&vout,":%q",val);
      else fd_unparse(&vout,val);
      ok = bson_append_utf8(out,key,keylen,vout.u8_outbuf,
                            vout.u8_write-vout.u8_outbuf);
      u8_close((u8_stream)&vout);}}
    return ok;}
  else if (FD_INTP(val))
    return bson_append_int32(out,key,keylen,((int)(FD_FIX2INT(val))));
  else if (FD_FIXNUMP(val))
    return bson_append_int64(out,key,keylen,FD_FIX2INT(val));
  else if (FD_OIDP(val)) {
    unsigned char bytes[12];
    FD_OID addr = FD_OID_ADDR(val); bson_oid_t oid;
    unsigned int hi = FD_OID_HI(addr), lo = FD_OID_LO(addr);
    bytes[0]=bytes[1]=bytes[2]=bytes[3]=0;
    bytes[4]=((hi>>24)&0xFF); bytes[5]=((hi>>16)&0xFF);
    bytes[6]=((hi>>8)&0xFF); bytes[7]=(hi&0xFF);
    bytes[8]=((lo>>24)&0xFF); bytes[9]=((lo>>16)&0xFF);
    bytes[10]=((lo>>8)&0xFF); bytes[11]=(lo&0xFF);
    bson_oid_init_from_data(&oid,bytes);
    return bson_append_oid(out,key,keylen,&oid);}
  else if (FD_SYMBOLP(val)) {
    if (flags&FD_MONGODB_COLONIZE_OUT) {
      u8_string pname = FD_SYMBOL_NAME(val);
      u8_byte _buf[512], *buf=_buf;
      size_t len = strlen(pname);
      if (len>510) buf = u8_malloc(len+2);
      buf[0]=':'; strcpy(buf+1,pname);
      if (buf!=_buf) {
        bool rval = bson_append_utf8(out,key,keylen,buf,len+1);
        u8_free(buf);
        return rval;}
      else return bson_append_utf8(out,key,keylen,buf,len+1);}
    else return bson_append_utf8(out,key,keylen,FD_SYMBOL_NAME(val),-1);}
  else if (FD_CHARACTERP(val)) {
    int code = FD_CHARCODE(val);
    if (code<128) {
      char c = code;
      return bson_append_utf8(out,key,keylen,&c,1);}
    else {
      struct U8_OUTPUT vout; unsigned char buf[16];
      U8_INIT_OUTPUT_BUF(&vout,16,buf);
      u8_putc(&vout,code);
      return bson_append_utf8(out,key,keylen,vout.u8_outbuf,
                              vout.u8_write-vout.u8_outbuf);}}
  else switch (val) {
    case FD_TRUE: case FD_FALSE:
      return bson_append_bool(out,key,keylen,(val == FD_TRUE));
    default: {
      struct U8_OUTPUT vout; unsigned char buf[128]; bool rv;
      U8_INIT_OUTPUT_BUF(&vout,128,buf);
      vout.u8_streaminfo |= U8_STREAM_VERBOSE;
      if (flags&FD_MONGODB_COLONIZE)
        u8_printf(&vout,":%q",val);
      else fd_unparse(&vout,val);
      rv = bson_append_utf8(out,key,keylen,vout.u8_outbuf,
                            vout.u8_write-vout.u8_outbuf);
      u8_close((u8_stream)&vout);
      return rv;}}
}

static bool bson_append_keyval(FD_BSON_OUTPUT b,lispval key,lispval val)
{
  int flags = b.bson_flags;
  struct U8_OUTPUT keyout; unsigned char buf[1000];
  const char *keystring = NULL; int keylen; bool ok = true;
  lispval fieldmap = b.bson_fieldmap, store_value = val;
  int vecslot = (!(FD_SYMBOLP(key))) ? (0) : 
    (FD_CHOICEP(val)) ? (0) :
    (FD_VECTORP(val)) ? (0) :
    (get_vecslot(key) >= 0);
  U8_INIT_OUTPUT_BUF(&keyout,1000,buf);
  if (FD_VOIDP(val)) return 0;
  if (FD_SYMBOLP(key)) {
    if (flags&FD_MONGODB_SLOTIFY) {
      struct FD_KEYVAL *opmap = fd_sortvec_get
        (key,mongo_opmap,mongo_opmap_size);
      if (FD_EXPECT_FALSE(opmap!=NULL))  {
        if (FD_STRINGP(opmap->kv_val)) {
          lispval mapped = opmap->kv_val;
          keystring = FD_CSTRING(mapped);
          keylen = FD_STRLEN(mapped);}}
      if (keystring == NULL) {
        u8_string pname = FD_SYMBOL_NAME(key);
        const u8_byte *scan = pname; int c;
        while ((c = u8_sgetc(&scan))>=0) {
          u8_putc(&keyout,u8_tolower(c));}
        keystring = keyout.u8_outbuf;
        keylen = keyout.u8_write-keyout.u8_outbuf;}}
    else {
      keystring = FD_SYMBOL_NAME(key);
      keylen = strlen(keystring);}
    if (!(FD_VOIDP(fieldmap))) {
      lispval mapfn = fd_get(fieldmap,key,FD_VOID);
      if (FD_VOIDP(mapfn)) {}
      else if (FD_APPLICABLEP(mapfn))
        store_value = fd_apply(mapfn,1,&val);
      else if (FD_TABLEP(mapfn))
        store_value = fd_get(mapfn,val,FD_VOID);
      else if (FD_TRUEP(mapfn)) {
        struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
        out.u8_streaminfo |= U8_STREAM_VERBOSE;
        fd_unparse(&out,val);
        store_value = fd_stream2string(&out);}
      else {}}}
  else if (FD_STRINGP(key)) {
    keystring = FD_CSTRING(key);
    if ((flags&FD_MONGODB_SLOTIFY)&&
        ((isdigit(keystring[0]))||
         (strchr(":(#@",keystring[0])!=NULL))) {
      u8_putc(&keyout,'\\'); u8_puts(&keyout,(u8_string)keystring);
      keystring = keyout.u8_outbuf;
      keylen = keyout.u8_write-keyout.u8_outbuf;}
    else keylen = FD_STRLEN(key);}
  else {
    keyout.u8_write = keyout.u8_outbuf;
    if ((flags&FD_MONGODB_SLOTIFY)&&
        (!((FD_OIDP(key))||(FD_VECTORP(key))||(FD_PAIRP(key)))))
      u8_putc(&keyout,':');
    fd_unparse(&keyout,key);
    keystring = keyout.u8_outbuf;
    keylen = keyout.u8_write-keyout.u8_outbuf;}
  /* Keys can't have periods in them, so we need to replace the
     periods on write and replace them back on read. */
  size_t  max_len = keylen+1;
  u8_byte newbuf[max_len], *write = newbuf;
  u8_string dotted = strchr(keystring,'.');
  if ( (dotted) || (keystring[0] == '+') || (keystring[0] == '/') )  {
    const u8_byte *scan = keystring, *limit = scan+keylen;
    while (scan < limit) {
      unsigned char c = *scan++;
      if (c == '.') *write++ = 0x02;
      else *write++ = c;}
    keystring = newbuf;}
  if (store_value == val)
    ok = bson_append_dtype(b,keystring,keylen,val,vecslot);
  else {
    ok = bson_append_dtype(b,keystring,keylen,store_value,vecslot);
    fd_decref(store_value);}
  u8_close((u8_stream)&keyout);
  return ok;
}

FD_EXPORT lispval fd_bson_output(struct FD_BSON_OUTPUT out,lispval obj)
{
  int ok = 1;
  if (FD_VECTORP(obj)) {
    int i = 0, len = FD_VECTOR_LENGTH(obj);
    lispval *elts = FD_VECTOR_ELTS(obj);
    while (i<len) {
      lispval elt = elts[i]; u8_byte buf[16];
      sprintf(buf,"%d",i++);
      if (ok)
        ok = bson_append_dtype(out,buf,strlen(buf),elt,0);}}
  else if (FD_CHOICEP(obj)) {
    int i = 0; FD_DO_CHOICES(elt,obj) {
      u8_byte buf[16]; sprintf(buf,"%d",i++);
      if (ok) ok = bson_append_dtype(out,buf,strlen(buf),elt,0);}}
  else if (FD_SLOTMAPP(obj)) {
    struct FD_SLOTMAP *smap = (fd_slotmap) obj;
    int i = 0, n = smap->n_slots;
    struct FD_KEYVAL *keyvals = smap->sm_keyvals;
    while (i < n) {
      lispval key = keyvals[i].kv_key;
      lispval val = keyvals[i].kv_val;
      ok = bson_append_keyval(out,key,val);
      i++;}}
  else if (FD_TABLEP(obj)) {
    lispval keys = fd_getkeys(obj);
    {FD_DO_CHOICES(key,keys) {
        lispval val = fd_get(obj,key,FD_VOID);
        if (ok) ok = bson_append_keyval(out,key,val);
        fd_decref(val);}}
    fd_decref(keys);}
  else if (FD_COMPOUNDP(obj)) {
    struct FD_COMPOUND *compound = FD_XCOMPOUND(obj);
    lispval tag = compound->compound_typetag, *elts = FD_COMPOUND_ELTS(obj);
    int len = FD_COMPOUND_LENGTH(obj);
    if (tag == mongovec_symbol) {
      lispval *scan = elts, *limit = scan+len; int i = 0;
      while (scan<limit) {
        u8_byte buf[16]; sprintf(buf,"%d",i);
        ok = bson_append_dtype(out,buf,strlen(buf),*scan,0);
        i++; scan++;
        if (!(ok)) break;}}
    else {
      int i = 0;
      ok = bson_append_dtype(out,"%fdtag",6,tag,0);
      if (ok) while (i<len) {
          char buf[16]; sprintf(buf,"%d",i);
          ok = bson_append_dtype(out,buf,strlen(buf),elts[i++],0);
          if (!(ok)) break;}}}
  if (!(ok))
    return FD_ERROR_VALUE;
  else return FD_VOID;
}

FD_EXPORT bson_t *fd_lisp2bson(lispval obj,int flags,lispval opts)
{
  if (FD_VOIDP(obj)) return NULL;
  else if ( (FD_STRINGP(obj)) && (strchr(FD_CSTRING(obj),'{')) ) {
    u8_string json = FD_CSTRING(obj); int free_it = 0;
    bson_error_t error; bson_t *result;
    if (strchr(json,'"')<0) {
      json = u8_string_subst(json,"'","\""); free_it = 1;}
    result = bson_new_from_json(json,FD_STRLEN(obj),&error);
    if (free_it) u8_free(json);
    if (result) return result;
    fd_seterr("Bad JSON","fd_lisp2bson/json",error.message,fd_incref(obj));
    return NULL;}
  else {
    struct FD_BSON_OUTPUT out;
    out.bson_doc = bson_new();
    out.bson_flags = ((flags<0)?(getflags(opts,FD_MONGODB_DEFAULTS)):(flags));
    out.bson_opts = opts;
    out.bson_fieldmap = fd_getopt(opts,fieldmap_symbol,FD_VOID);
    fd_bson_output(out,obj);
    fd_decref(out.bson_fieldmap);
    return out.bson_doc;}
}

/* BSON input functions */

#define slotify_char(c) \
    ((c=='_')||(c=='-')||(c=='%')||(c=='.')||(c=='/')||(c=='$')||(u8_isalnum(c)))

/* -1 means don't slotify at all, 0 means symbolize, 1 means intern */
static int slotcode(u8_string s)
{
  const u8_byte *scan = s; int c, i = 0, hasupper = 0;
  while ((c = u8_sgetc(&scan))>=0) {
    if (i>32) return -1;
    if (!(slotify_char(c))) return -1; else i++;
    if (u8_isupper(c)) hasupper = 1;}
  return hasupper;
}

static lispval bson_read_vector(FD_BSON_INPUT b);
static lispval bson_read_choice(FD_BSON_INPUT b);

static void bson_read_step(FD_BSON_INPUT b,lispval into,lispval *loc)
{
  bson_iter_t *in = b.bson_iter; int flags = b.bson_flags, symbolized = 0;
  const unsigned char *field = bson_iter_key(in);
  const size_t field_len = strlen(field);
  unsigned char tmpbuf[field_len+1];
  if (strchr(field,0x02)) {
    const unsigned char *read = field, *limit = read+field_len;
    unsigned char *write = tmpbuf;
    while ( read < limit ) {
      unsigned char c = *read++;
      if (c == 0x02) *write++='.'; else *write++=c;}
    *write++='\0';
    field = tmpbuf;}
  bson_type_t bt = bson_iter_type(in);
  lispval slotid, value;
  if ( (flags&FD_MONGODB_SLOTIFY) && (strchr(":(#@",field[0])!=NULL) )
    slotid = fd_parse_arg((u8_string)field);
  else if (flags&FD_MONGODB_SLOTIFY) {
    int sc = slotcode((u8_string)field);
    if (sc<0) slotid = fd_make_string(NULL,-1,(unsigned char *)field);
    else if (sc==0) {
      slotid = fd_symbolize((unsigned char *)field);
      symbolized = 1;}
    else slotid = fd_intern((unsigned char *)field);}
  else slotid = fd_make_string(NULL,-1,(unsigned char *)field);
  switch (bt) {
  case BSON_TYPE_DOUBLE:
    value = fd_make_double(bson_iter_double(in)); break;
  case BSON_TYPE_BOOL:
    value = (bson_iter_bool(in))?(FD_TRUE):(FD_FALSE); break;
  case BSON_TYPE_REGEX: {
    const char *props, *src = bson_iter_regex(in,&props);
    int flags = 0;
    if (strchr(props,'x')>=0) flags |= REG_EXTENDED;
    if (strchr(props,'i')>=0) flags |= REG_ICASE;
    if (strchr(props,'m')>=0) flags |= REG_NEWLINE;
    value = fd_make_regex((u8_string)src,flags);
    break;}
  case BSON_TYPE_UTF8: {
    int len = -1;
    const unsigned char *bytes = bson_iter_utf8(in,&len);
    if ((flags&FD_MONGODB_COLONIZE)&&(strchr(":(#@",bytes[0])!=NULL))
      value = fd_parse_arg((u8_string)(bytes));
    else if ((flags&FD_MONGODB_COLONIZE)&&(bytes[0]=='\\'))
      value = fd_make_string(NULL,((len>0)?(len-1):(-1)),
                           (unsigned char *)bytes+1);
    else value = fd_make_string(NULL,((len>0)?(len):(-1)),
                              (unsigned char *)bytes);
    break;}
  case BSON_TYPE_BINARY: {
    int len; bson_subtype_t st; const unsigned char *data;
    bson_iter_binary(in,&st,&len,&data);
    if (st == BSON_SUBTYPE_UUID) {
      struct FD_UUID *uuid = u8_alloc(struct FD_UUID);
      FD_INIT_CONS(uuid,fd_uuid_type);
      memcpy(uuid->uuid16,data,len);
      value = (lispval) uuid;}
    else {
      lispval packet = fd_make_packet(NULL,len,(unsigned char *)data);
      if (st == BSON_SUBTYPE_BINARY)
        value = packet;
      else if (st == BSON_SUBTYPE_USER)
        value = fd_init_compound(NULL,mongouser,0,1,packet);
      else if (st == BSON_SUBTYPE_MD5)
        value = fd_init_compound(NULL,mongomd5,0,1,packet);
      else if (st == BSON_SUBTYPE_FUNCTION)
        value = fd_init_compound(NULL,mongofun,0,1,packet);
      else value = packet;}
    break;}
  case BSON_TYPE_INT32:
    value = FD_INT(bson_iter_int32(in));
    break;
  case BSON_TYPE_INT64:
    value = FD_INT(bson_iter_int64(in));
    break;
  case BSON_TYPE_OID: {
    const bson_oid_t *oidval = bson_iter_oid(in);
    const unsigned char *bytes = oidval->bytes;
    if ((bytes[0]==0)&&(bytes[1]==0)&&(bytes[2]==0)&&(bytes[3]==0)) {
      FD_OID dtoid;
      unsigned int hi=
        (((((bytes[4]<<8)|(bytes[5]))<<8)|(bytes[6]))<<8)|(bytes[7]);
      unsigned int lo=
        (((((bytes[8]<<8)|(bytes[9]))<<8)|(bytes[10]))<<8)|(bytes[11]);
      memset(&dtoid,0,sizeof(dtoid));
      FD_SET_OID_HI(dtoid,hi); FD_SET_OID_LO(dtoid,lo);
      value = fd_make_oid(dtoid);}
    else {
      lispval packet = fd_make_packet(NULL,12,(unsigned char *)bytes);
      value = fd_init_compound(NULL,oidtag,0,1,packet);}
    break;}
  case BSON_TYPE_UNDEFINED:
    value = FD_VOID; break;
  case BSON_TYPE_NULL:
    value = FD_EMPTY_CHOICE; break;
  case BSON_TYPE_DATE_TIME: {
    unsigned long long millis = bson_iter_date_time(in);
    struct FD_TIMESTAMP *ts = u8_alloc(struct FD_TIMESTAMP);
    FD_INIT_CONS(ts,fd_timestamp_type);
    u8_init_xtime(&(ts->u8xtimeval),millis/1000,u8_millisecond,
                  ((millis%1000)*1000000),0,0);
    value = (lispval)ts;
    break;}
  case BSON_TYPE_MAXKEY:
    value = maxkey; break;
  case BSON_TYPE_MINKEY:
    value = minkey; break;
  default:
    if (BSON_ITER_HOLDS_DOCUMENT(in)) {
      struct FD_BSON_INPUT r; bson_iter_t child;
      bson_iter_recurse(in,&child);
      r.bson_iter = &child; r.bson_flags = b.bson_flags;
      r.bson_opts = b.bson_opts; r.bson_fieldmap = b.bson_fieldmap;
      value = fd_init_slotmap(NULL,0,NULL);
      while (bson_iter_next(&child))
        bson_read_step(r,value,NULL);
      if (fd_test(value,fdtag_symbol,FD_VOID)) {
        lispval tag = fd_get(value,fdtag_symbol,FD_VOID), compound;
        struct FD_COMPOUND_TYPEINFO *entry = fd_lookup_compound(tag);
        lispval fields[16] = { FD_VOID }, keys = fd_getkeys(value);
        int max = -1, i = 0, n, ok = 1;
        {FD_DO_CHOICES(key,keys) {
            if (FD_FIXNUMP(key)) {
              long long index = FD_FIX2INT(key);
              if ((index<0) || (index>=16)) {
                i = 0; while (i<16) {
                  lispval value = fields[i++];
                  fd_decref(value);}
                u8_logf(LOG_ERR,fd_BSON_Compound_Overflow,
                       "Compound of type %q: %q",tag,value);
                FD_STOP_DO_CHOICES;
                ok = 0;
                break;}
              if (index>max) max = index;
              fields[index]=fd_get(value,key,FD_VOID);}}}
        if (ok) {
          n = max+1;
          if ((entry)&&(entry->compound_parser))
            compound = entry->compound_parser(n,fields,entry);
          else {
            struct FD_COMPOUND *c=
              u8_malloc(sizeof(struct FD_COMPOUND)+(n*LISPVAL_LEN));
            lispval *cdata = &(c->compound_0);
            fd_init_compound(c,tag,0,0);
            c->compound_length = n;
            memcpy(cdata,fields,n);
            compound = LISP_CONS(c);}
          fd_decref(value);
          value = compound;}
        fd_decref(keys);
        fd_decref(tag);}
      else {}}
    else if (BSON_ITER_HOLDS_ARRAY(in)) {
      int flags = b.bson_flags, choicevals = (flags&FD_MONGODB_CHOICEVALS);
      if ((choicevals)&&(symbolized))
        value = bson_read_choice(b);
      else value = bson_read_vector(b);}
    else {
      u8_logf(LOG_ERR,fd_BSON_Input_Error,
             "Can't handle BSON type %d",bt);
      return;}}
  if (!(FD_VOIDP(b.bson_fieldmap))) {
    struct FD_STRING _tempkey;
    lispval tempkey = fd_init_string(&_tempkey,strlen(field),field);
    lispval mapfn = FD_VOID, new_value = FD_VOID;
    lispval fieldmap = b.bson_fieldmap;
    FD_INIT_STACK_CONS(tempkey,fd_string_type);
    mapfn = fd_get(fieldmap,tempkey,FD_VOID);
    if (FD_VOIDP(mapfn)) {}
    else if (FD_APPLICABLEP(mapfn))
      new_value = fd_apply(mapfn,1,&value);
    else if (FD_TABLEP(mapfn))
      new_value = fd_get(mapfn,value,FD_VOID);
    else if ((FD_TRUEP(mapfn))&&(FD_STRINGP(value)))
      new_value = fd_parse(FD_CSTRING(value));
    if (FD_ABORTP(new_value)) {
      fd_clear_errors(1);
      new_value = FD_VOID;}
    else if (new_value!=FD_VOID) {
      lispval old_value = value;
      value = new_value;
      fd_decref(old_value);}
    else {}}
  if (!(FD_VOIDP(into))) fd_store(into,slotid,value);
  if (loc) *loc = value;
  else fd_decref(value);
}

static lispval bson_read_vector(FD_BSON_INPUT b)
{
  struct FD_BSON_INPUT r; bson_iter_t child;
  lispval result, *data = u8_alloc_n(16,lispval), *write = data, *lim = data+16;
  bson_iter_recurse(b.bson_iter,&child);
  r.bson_iter = &child; r.bson_flags = b.bson_flags;
  r.bson_opts = b.bson_opts; r.bson_fieldmap = b.bson_fieldmap;
  while (bson_iter_next(&child)) {
    if (write>=lim) {
      int len = lim-data;
      int newlen = ((len<16384)?(len*2):(len+16384));
      lispval *newdata = u8_realloc_n(data,newlen,lispval);
      if (!(newdata)) u8_raise(fd_MallocFailed,"mongodb_cursor_read_vector",NULL);
      write = newdata+(write-data); lim = newdata+newlen; data = newdata;}
    bson_read_step(r,FD_VOID,write); write++;}
  result = fd_make_vector(write-data,data);
  u8_free(data);
  return result;
}

static lispval bson_read_choice(FD_BSON_INPUT b)
{
  struct FD_BSON_INPUT r; bson_iter_t child;
  lispval *data = u8_alloc_n(16,lispval), *write = data, *lim = data+16;
  bson_iter_recurse(b.bson_iter,&child);
  r.bson_iter = &child; r.bson_flags = b.bson_flags;
  r.bson_opts = b.bson_opts; r.bson_fieldmap = b.bson_fieldmap;
  while (bson_iter_next(&child)) {
    if (write>=lim) {
      int len = lim-data;
      int newlen = ((len<16384)?(len*2):(len+16384));
      lispval *newdata = u8_realloc_n(data,newlen,lispval);
      if (!(newdata)) u8_raise(fd_MallocFailed,"mongodb_cursor_read_vector",NULL);
      write = newdata+(write-data); lim = newdata+newlen; data = newdata;}
    if (BSON_ITER_HOLDS_ARRAY(&child)) {
      *write++=bson_read_vector(r);}
    else {
      bson_read_step(r,FD_VOID,write);
      write++;}}
  if (write == data) {
    u8_free(data);
    return FD_EMPTY_CHOICE;}
  else return fd_make_choice
         (write-data,data,
          FD_CHOICE_DOSORT|FD_CHOICE_COMPRESS|
          FD_CHOICE_FREEDATA|FD_CHOICE_REALLOC);
}

FD_EXPORT lispval fd_bson2dtype(bson_t *in,int flags,lispval opts)
{
  bson_iter_t iter;
  if (flags<0) flags = getflags(opts,FD_MONGODB_DEFAULTS);
  memset(&iter,0,sizeof(bson_iter_t));
  if (bson_iter_init(&iter,in)) {
    lispval result, fieldmap = fd_getopt(opts,fieldmap_symbol,FD_VOID);
    struct FD_BSON_INPUT b;
    memset(&b,0,sizeof(struct FD_BSON_INPUT));
    b.bson_iter = &iter; b.bson_flags = flags;
    b.bson_opts = opts; b.bson_fieldmap = fieldmap;
    result = fd_init_slotmap(NULL,0,NULL);
    while (bson_iter_next(&iter)) bson_read_step(b,result,NULL);
    fd_decref(fieldmap);
    return result;}
  else return fd_err(fd_BSON_Input_Error,"fd_bson2dtype",NULL,FD_VOID);
}

static lispval mongovec_lexpr(int n,lispval *values)
{
  return fd_init_compound_from_elts
    (NULL,mongovec_symbol,FD_COMPOUND_INCREF|FD_COMPOUND_SEQUENCE,n,values);
}

static lispval make_mongovec(lispval vec)
{
  lispval *elts = FD_VECTOR_ELTS(vec);
  int n = FD_VECTOR_LENGTH(vec);
  return fd_init_compound_from_elts
    (NULL,mongovec_symbol,FD_COMPOUND_INCREF|FD_COMPOUND_SEQUENCE,n,elts);
}

static lispval mongovecp(lispval arg)
{
  if (FD_COMPOUND_TYPEP(arg,mongovec_symbol))
    return FD_TRUE;
  else return FD_FALSE;
}

/* The MongoDB OPMAP */

/* The OPMAP translates symbols that correspond to MongoDB
   operations. Since the default slot transformation lowercases the
   symbol name, this table only needs to include the ones that have
   embedded uppercase letters.  */

static void add_to_mongo_opmap(u8_string keystring)
{
  lispval key = fd_symbolize(keystring);
  struct FD_KEYVAL *entry=
    fd_sortvec_insert(key,&mongo_opmap,
                      &mongo_opmap_size,
                      &mongo_opmap_space,
                      MONGO_OPMAP_MAX,
                      1);
  if (entry)
    entry->kv_val = lispval_string(keystring);
  else u8_logf(LOG_ERR,"Couldn't add %s to the mongo opmap",keystring);
}

static void init_mongo_opmap()
{
  mongo_opmap = u8_alloc_n(32,struct FD_KEYVAL);
  mongo_opmap_space = 32;
  mongo_opmap_size = 0;
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

  add_to_mongo_opmap("$gt");
  add_to_mongo_opmap("$gte");
  add_to_mongo_opmap("$lt");
  add_to_mongo_opmap("$lte");

  add_to_mongo_opmap("$eq");
  add_to_mongo_opmap("$ne");
  add_to_mongo_opmap("$nin");
  add_to_mongo_opmap("$in");

  add_to_mongo_opmap("$maxScan");
  add_to_mongo_opmap("$maxTimeMS");
  add_to_mongo_opmap("$returnKey");
  add_to_mongo_opmap("$showDiskLoc");

}

/* Getting 'meta' information from connections */

static struct FD_MONGODB_DATABASE *getdb(lispval arg,u8_context cxt)
{
  if (FD_TYPEP(arg,fd_mongoc_server)) {
    return fd_consptr(struct FD_MONGODB_DATABASE *,arg,fd_mongoc_server);}
  else if (FD_TYPEP(arg,fd_mongoc_collection)) {
    struct FD_MONGODB_COLLECTION *collection=
      fd_consptr(struct FD_MONGODB_COLLECTION *,arg,fd_mongoc_collection);
    return (struct FD_MONGODB_DATABASE *)collection->domain_db;}
  else if (FD_TYPEP(arg,fd_mongoc_cursor)) {
    struct FD_MONGODB_CURSOR *cursor=
      fd_consptr(struct FD_MONGODB_CURSOR *,arg,fd_mongoc_cursor);
    struct FD_MONGODB_COLLECTION *collection = CURSOR2DOMAIN(cursor);
    return DOMAIN2DB(collection);}
  else {
    fd_seterr(fd_TypeError,cxt,"MongoDB object",arg);
    return NULL;}
}

static lispval mongodb_dbname(lispval arg)
{
  struct FD_MONGODB_DATABASE *db = getdb(arg,"mongodb_dbname");
  if (db == NULL) return FD_ERROR_VALUE;
  else return lispval_string(db->dbname);
}

static lispval mongodb_spec(lispval arg)
{
  struct FD_MONGODB_DATABASE *db = getdb(arg,"mongodb_spec");
  if (db == NULL) return FD_ERROR_VALUE;
  else return lispval_string(db->dbspec);
}

static lispval mongodb_uri(lispval arg)
{
  struct FD_MONGODB_DATABASE *db = getdb(arg,"mongodb_uri");
  if (db == NULL) return FD_ERROR_VALUE;
  else return lispval_string(db->dburi);
}

static lispval mongodb_getopts(lispval arg)
{
  lispval opts = FD_VOID;
  if (FD_TYPEP(arg,fd_mongoc_server)) {
    opts = (fd_consptr(struct FD_MONGODB_DATABASE *,arg,fd_mongoc_server))
      ->dbopts;}
  else if (FD_TYPEP(arg,fd_mongoc_collection)) {
    struct FD_MONGODB_COLLECTION *collection=
      fd_consptr(struct FD_MONGODB_COLLECTION *,arg,fd_mongoc_collection);
    opts = collection->domain_opts;}
  else if (FD_TYPEP(arg,fd_mongoc_cursor)) {
    struct FD_MONGODB_CURSOR *cursor=
      fd_consptr(struct FD_MONGODB_CURSOR *,arg,fd_mongoc_cursor);
    opts = cursor->cursor_opts;}
  else {
    fd_seterr(fd_TypeError,"mongodb_opts","MongoDB object",arg);
    return FD_ERROR_VALUE;}
  fd_incref(opts);
  return opts;
}

static lispval mongodb_getdb(lispval arg)
{
  struct FD_MONGODB_COLLECTION *collection = NULL;
  if (FD_TYPEP(arg,fd_mongoc_server))
    return fd_incref(arg);
  else if (FD_TYPEP(arg,fd_mongoc_collection))
    collection = fd_consptr(struct FD_MONGODB_COLLECTION *,arg,fd_mongoc_collection);
  else if (FD_TYPEP(arg,fd_mongoc_cursor)) {
    struct FD_MONGODB_CURSOR *cursor=
      fd_consptr(struct FD_MONGODB_CURSOR *,arg,fd_mongoc_cursor);
    collection = (struct FD_MONGODB_COLLECTION *)cursor->cursor_domain;}
  else return fd_type_error("MongoDB collection/cursor","mongodb_dbname",arg);
  if (collection)
    return fd_incref(collection->domain_db);
  else return FD_FALSE;
}

static lispval mongodb_collection_name(lispval arg)
{
  struct FD_MONGODB_COLLECTION *collection = NULL;
  if (FD_TYPEP(arg,fd_mongoc_collection))
    collection = fd_consptr(struct FD_MONGODB_COLLECTION *,arg,fd_mongoc_collection);
  else if (FD_TYPEP(arg,fd_mongoc_cursor)) {
    struct FD_MONGODB_CURSOR *cursor=
      fd_consptr(struct FD_MONGODB_CURSOR *,arg,fd_mongoc_cursor);
    collection = (struct FD_MONGODB_COLLECTION *)cursor->cursor_domain;}
  else return fd_type_error("MongoDB collection/cursor","mongodb_dbname",arg);
  if (collection)
    return fd_make_string(NULL,-1,collection->collection_name);
  else return FD_FALSE;
}

static int mongodb_getflags(lispval arg)
{
  if (FD_TYPEP(arg,fd_mongoc_server)) {
    struct FD_MONGODB_DATABASE *server = (struct FD_MONGODB_DATABASE *)arg;
    return server->dbflags;}
  else if (FD_TYPEP(arg,fd_mongoc_collection)) {
    struct FD_MONGODB_COLLECTION *collection=
      fd_consptr(struct FD_MONGODB_COLLECTION *,arg,fd_mongoc_collection);
    return collection->domain_flags;}
  else if (FD_TYPEP(arg,fd_mongoc_cursor)) {
    struct FD_MONGODB_CURSOR *cursor=
      fd_consptr(struct FD_MONGODB_CURSOR *,arg,fd_mongoc_cursor);
    return cursor->cursor_flags;}
  else return -1;
}

static lispval mongodb_getcollection(lispval arg)
{
  if (FD_TYPEP(arg,fd_mongoc_collection))
    return fd_incref(arg);
  else if (FD_TYPEP(arg,fd_mongoc_cursor)) {
    struct FD_MONGODB_CURSOR *cursor=
      fd_consptr(struct FD_MONGODB_CURSOR *,arg,fd_mongoc_cursor);
    return fd_incref(cursor->cursor_domain);}
  else return fd_type_error("MongoDB collection/cursor","mongodb_dbname",arg);
}

static lispval mongodbp(lispval arg)
{
  if (FD_TYPEP(arg,fd_mongoc_server))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval mongodb_collectionp(lispval arg)
{
  if (FD_TYPEP(arg,fd_mongoc_collection))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval mongodb_cursorp(lispval arg)
{
  if (FD_TYPEP(arg,fd_mongoc_cursor))
    return FD_TRUE;
  else return FD_FALSE;
}

static void add_string(lispval result,lispval field,u8_string value)
{
  if (value == NULL) return;
  else {
    lispval stringval = lispval_string(value);
    fd_add(result,field,stringval);
    fd_decref(stringval);
    return;}
}

static lispval mongodb_getinfo(lispval mongodb,lispval field)
{
  lispval result = fd_make_slotmap(10,0,NULL);
  struct FD_MONGODB_DATABASE *db=
    fd_consptr(struct FD_MONGODB_DATABASE *,mongodb,fd_mongoc_server);
  mongoc_uri_t *info = db->dburi_info;
  u8_string tmpstring;
  if ((tmpstring = mongoc_uri_get_database(info)))
    add_string(result,dbname_symbol,tmpstring);
  if ((tmpstring = mongoc_uri_get_username(info)))
    add_string(result,username_symbol,tmpstring);
  if ((tmpstring = mongoc_uri_get_auth_mechanism(info)))
    add_string(result,auth_symbol,tmpstring);
  if ((tmpstring = mongoc_uri_get_auth_source(info)))
    add_string(result,auth_symbol,tmpstring);
  {
    const mongoc_host_list_t *scan = mongoc_uri_get_hosts(info);
    while (scan) {
      add_string(result,hosts_symbol,scan->host);
      add_string(result,connections_symbol,scan->host_and_port);
      scan = scan->next;}
  }
  if (mongoc_uri_get_ssl(info))
    fd_store(result,sslsym,FD_TRUE);

  if ((FD_VOIDP(field))||(FD_FALSEP(field)))
    return result;
  else {
    lispval v = fd_get(result,field,FD_EMPTY_CHOICE);
    fd_decref(result);
    return v;}
}

/* MongoDB logging */

static int getu8loglevel(mongoc_log_level_t l);
static int mongodb_ignore_loglevel = -1;

void mongoc_logger(mongoc_log_level_t l,const char *d,const char *m,void *u)
{
  int u8l = getu8loglevel(l);
  if (u8l <= LOG_CRIT)
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

static u8_byte mongoc_version_string[100];
static u8_byte *mongoc_version = mongoc_version_string;

FD_EXPORT int fd_init_mongodb(void) FD_LIBINIT_FN;
static long long int mongodb_initialized = 0;

#define DEFAULT_FLAGS (FD_SHORT2DTYPE(FD_MONGODB_DEFAULTS))

static void init_old_mongodb(lispval module);

FD_EXPORT int fd_init_mongodb()
{
  lispval module;
  if (mongodb_initialized) return 0;
  mongodb_initialized = u8_millitime();

  init_mongo_opmap();

  module = fd_new_cmodule("MONGODB",(FD_MODULE_SAFE),fd_init_mongodb);

  idsym = fd_intern("_ID");
  maxkey = fd_intern("mongomax");
  minkey = fd_intern("mongomin");
  oidtag = fd_intern("mongoid");
  mongofun = fd_intern("mongofun");
  mongouser = fd_intern("mongouser");
  mongomd5 = fd_intern("md5hash");

  skipsym = fd_intern("SKIP");
  limitsym = fd_intern("LIMIT");
  batchsym = fd_intern("BATCH");
  fieldssym = fd_intern("FIELDS");
  upsertsym = fd_intern("UPSERT");
  singlesym = fd_intern("SINGLE");
  wtimeoutsym = fd_intern("WTIMEOUT");
  writesym = fd_intern("WRITE");
  readsym = fd_intern("READ");
  returnsym = fd_intern("RETURN");
  originalsym = fd_intern("ORIGINAL");
  newsym = fd_intern("NEW");
  removesym = fd_intern("REMOVE");
  softfailsym = fd_intern("SOFTFAIL");

  sslsym = fd_intern("SSL");
  smoketest_sym = fd_intern("SMOKETEST");
  certfile = fd_intern("CERTFILE");
  certpass = fd_intern("CERTPASS");
  cafilesym = fd_intern("CAFILE");
  cadirsym = fd_intern("CADIR");
  crlsym = fd_intern("CRLFILE");

  mongovec_symbol = fd_intern("%MONGOVEC");

  bsonflags = fd_intern("BSON");
  raw = fd_intern("RAW");
  slotify = fd_intern("SLOTIFY");
  slotifyin = fd_intern("SLOTIFY/IN");
  slotifyout = fd_intern("SLOTIFY/OUT");
  colonize = fd_intern("COLONIZE");
  colonizein = fd_intern("COLONIZE/IN");
  colonizeout = fd_intern("COLONIZE/OUT");
  logopsym = fd_intern("LOGOPS");
  choices = fd_intern("CHOICES");
  nochoices = fd_intern("NOCHOICES");
  fieldmap_symbol = fd_intern("FIELDMAP");

  dbname_symbol = fd_intern("DBNAME");
  username_symbol = fd_intern("USERNAME");
  auth_symbol = fd_intern("AUTHENTICATION");
  hosts_symbol = fd_intern("HOSTS");
  connections_symbol = fd_intern("CONNECTIONS");
  fdtag_symbol = fd_intern("%FDTAG");
  fdparse_symbol = fd_intern("%FDPARSE");

  primarysym = fd_intern("PRIMARY");
  primarypsym = fd_intern("PRIMARY+");
  secondarysym = fd_intern("SECONDARY");
  secondarypsym = fd_intern("SECONDARY+");
  nearestsym = fd_intern("NEAREST");

  poolmaxsym = fd_intern("POOLMAX");

  fd_mongoc_server = fd_register_cons_type("MongoDB client");
  fd_mongoc_collection = fd_register_cons_type("MongoDB collection");
  fd_mongoc_cursor = fd_register_cons_type("MongoDB cursor");

  fd_type_names[fd_mongoc_server]="MongoDB server";
  fd_type_names[fd_mongoc_collection]="MongoDB collection";
  fd_type_names[fd_mongoc_cursor]="MongoDB cursor";

  fd_recyclers[fd_mongoc_server]=recycle_server;
  fd_recyclers[fd_mongoc_collection]=recycle_collection;
  fd_recyclers[fd_mongoc_cursor]=recycle_cursor;

  fd_unparsers[fd_mongoc_server]=unparse_server;
  fd_unparsers[fd_mongoc_collection]=unparse_collection;
  fd_unparsers[fd_mongoc_cursor]=unparse_cursor;

  fd_idefn(module,fd_make_cprim2x("MONGO/OPEN",mongodb_open,1,
                                  -1,FD_VOID,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("COLLECTION/OPEN",mongodb_collection,1,
                                  -1,FD_VOID,fd_string_type,FD_VOID,
                                  -1,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("CURSOR/OPEN",mongodb_cursor,2,
                                  -1,FD_VOID,-1,FD_VOID,
                                  -1,FD_VOID));
  fd_defalias(module,"MONGO/COLLECTION","COLLECTION/OPEN");
  fd_defalias(module,"MONGO/CURSOR","CURSOR/OPEN");

  fd_idefn(module,fd_make_cprim1x("MONGO/OID",mongodb_oidref,1,-1,FD_VOID));


  fd_idefn(module,fd_make_cprim1x("MONGODB?",mongodbp,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("MONGO/COLLECTION?",mongodb_collectionp,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("MONGO/CURSOR?",mongodb_cursorp,1,-1,FD_VOID));
  fd_defalias(module,"CURSOR?","MONGO/CURSOR?");
  fd_defalias(module,"COLLECTION?","MONGO/COLLECTION?");

  fd_idefn3(module,"COLLECTION/INSERT!",mongodb_insert,
            FD_NEEDS_2_ARGS|FD_NDCALL,
            "(COLLECTION/INSERT! *collection* *objects* *opts*)",
            -1,FD_VOID,-1,FD_VOID,-1,FD_FALSE);
  fd_idefn(module,fd_make_cprim3x("COLLECTION/REMOVE!",mongodb_remove,2,
                                  fd_mongoc_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID));

  fd_idefn4(module,"COLLECTION/UPDATE!",mongodb_update,FD_NEEDS_2_ARGS,
            "(COLLECTION/UPDATE! *collection* *object* *opts*)",
            fd_mongoc_collection,FD_VOID,
            -1,FD_VOID,-1,FD_VOID,
            -1,FD_VOID);
  fd_idefn4(module,"COLLECTION/UPSERT!",mongodb_upsert,FD_NEEDS_2_ARGS,
            "(COLLECTION/UPSERT! *collection* *object* *opts*)",
            fd_mongoc_collection,FD_VOID,
            -1,FD_VOID,-1,FD_VOID,
            -1,FD_VOID);
  fd_idefn(module,fd_make_cprim4x("COLLECTION/MODIFY!",mongodb_modify,3,
                                  fd_mongoc_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID,-1,FD_VOID));
  fd_defalias(module,"COLLECTION/MODIFY","COLLECTION/MODIFY!");

  fd_idefn(module,fd_make_cprim3x("COLLECTION/FIND",mongodb_find,2,
                                  fd_mongoc_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("COLLECTION/COUNT",mongodb_count,2,
                                  fd_mongoc_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("COLLECTION/GET",mongodb_get,2,
                                  fd_mongoc_collection,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID));

  fd_idefn(module,fd_make_cprimn("MONGO/RESULTS",mongodb_command,2));
  fd_idefn(module,fd_make_cprimn("MONGO/CMD",mongodb_simple_command,2));

  fd_idefn(module,fd_make_cprim1x("CURSOR/DONE?",mongodb_donep,1,
                                  fd_mongoc_cursor,FD_VOID));
  fd_idefn(module,fd_make_cprim2x("CURSOR/SKIP",mongodb_skip,1,
                                  fd_mongoc_cursor,FD_VOID,
                                  fd_fixnum_type,FD_FIXNUM_ONE));

  fd_idefn(module,fd_make_cprim3x("CURSOR/READ",mongodb_cursor_read,1,
                                  fd_mongoc_cursor,FD_VOID,
                                  fd_fixnum_type,FD_FIXNUM_ONE,
                                  -1,FD_VOID));

  fd_idefn(module,fd_make_cprim3x("CURSOR/READVEC",
                                  mongodb_cursor_read_vector,1,
                                  fd_mongoc_cursor,FD_VOID,
                                  fd_fixnum_type,FD_FIXNUM_ONE,
                                  -1,FD_VOID));

  fd_idefn(module,fd_make_ndprim(fd_make_cprimn("MONGOVEC",mongovec_lexpr,0)));
  fd_idefn(module,fd_make_cprim1x("->MONGOVEC",make_mongovec,1,
                                  fd_vector_type,FD_VOID));
  fd_idefn(module,fd_make_cprim1("MONGOVEC?",mongovecp,1));

  fd_idefn(module,fd_make_cprim1x("MONGO/GETDB",mongodb_getdb,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim2x("MONGO/DBINFO",mongodb_getinfo,1,
                                  fd_mongoc_server,FD_VOID,
                                  -1,FD_VOID));

  fd_idefn(module,fd_make_cprim1x("MONGO/DBNAME",mongodb_dbname,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("MONGO/DBSPEC",mongodb_spec,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("MONGO/DBURI",mongodb_uri,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("MONGO/GETOPTS",mongodb_getopts,1,-1,FD_VOID));

  fd_idefn(module,fd_make_cprim1x("MONGO/GETCOLLECTION",
                                  mongodb_getcollection,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("COLLECTION/NAME",
                                  mongodb_collection_name,1,-1,FD_VOID));

  fd_register_config("MONGODB:FLAGS",
                     "Default flags (fixnum) for MongoDB/BSON processing",
                     fd_intconfig_get,fd_intconfig_set,&mongodb_defaults);

  fd_register_config("MONGODB:LOGLEVEL",
                     "Default flags (fixnum) for MongoDB/BSON processing",
                     fd_intconfig_get,fd_intconfig_set,&mongodb_loglevel);
  fd_register_config("MONGO:MAXLOG",
                     "Controls which log messages are always discarded",
                     fd_intconfig_get,fd_intconfig_set,
                     &mongodb_ignore_loglevel);
  fd_register_config("MONGODB:LOGOPS",
                     "Default flags (fixnum) for MongoDB/BSON processing",
                     fd_boolconfig_get,fd_boolconfig_set,&logops);

  fd_register_config("MONGODB:SSL",
                     "Whether to default to SSL for MongoDB connections",
                     fd_boolconfig_get,fd_boolconfig_set,
                     &default_ssl);
  fd_register_config("MONGODB:CERT",
                     "Default certificate file to use for mongodb",
                     fd_sconfig_get,fd_realpath_config_set,
                     &default_certfile);
  fd_register_config("MONGODB:CAFILE",
                     "Default certificate file for use with MongoDB",
                     fd_sconfig_get,fd_realpath_config_set,
                     &default_cafile);
  fd_register_config("MONGODB:CADIR",
                     "Default certificate file directory for use with MongoDB",
                     fd_sconfig_get,fd_realdir_config_set,
                     &default_cadir);

  fd_register_config("MONGODB:VECSLOTS",
                     "Which slots whould always have vector values",
                     vecslots_config_get,vecslots_config_add,NULL);

  add_vecslot(fd_intern("$EACH"));
  add_vecslot(fd_intern("$IN"));
  add_vecslot(fd_intern("$NIN"));
  add_vecslot(fd_intern("$ALL"));
  add_vecslot(fd_intern("$AND"));
  add_vecslot(fd_intern("$OR"));
  add_vecslot(fd_intern("$NOR"));

  fd_finish_module(module);

  mongoc_init();
  atexit(mongoc_cleanup);

  strcpy(mongoc_version_string,"libmongoc ");
  strcat(mongoc_version_string,MONGOC_VERSION_S);
  u8_register_source_file(mongoc_version_string);

  mongoc_log_set_handler(mongoc_logger,NULL);
  fd_register_config("MONGO:VERSION",
                     "The MongoDB C library version string",
                     fd_sconfig_get,NULL,
                     &mongoc_version);

  u8_register_source_file(_FILEINFO);

  strcpy(mongoc_version_string,"libmongoc ");
  strcat(mongoc_version_string,MONGOC_VERSION_S);
  u8_register_source_file(mongoc_version_string);

  init_old_mongodb(module);

  return 1;
}

static void init_old_mongodb(lispval module)
{
  fd_defalias(module,"MONGODB/OPEN","MONGO/OPEN");
  fd_defalias(module,"MONGODB/COLLECTION","COLLECTION/OPEN");
  fd_defalias(module,"MONGODB/CURSOR","CURSOR/OPEN");
  fd_defalias(module,"MONGODB/INSERT!","COLLECTION/INSERT!");
  fd_defalias(module,"MONGODB/REMOVE!","COLLECTION/REMOVE!");
  fd_defalias(module,"MONGODB/UPDATE!","COLLECTION/UPDATE!");
  fd_defalias(module,"MONGODB/FIND","COLLECTION/FIND");
  fd_defalias(module,"MONGODB/MODIFY","COLLECTION/MODIFY!");
  fd_defalias(module,"MONGODB/MODIFY!","COLLECTION/MODIFY!");
  fd_defalias(module,"MONGODB/GET","COLLECTION/GET");

  fd_defalias(module,"MONGODB/RESULTS","MONGODB/RESULTS");
  fd_defalias(module,"MONGODB/DO","MONGODB/DO");

  fd_defalias(module,"MONGODB/DONE?","CURSOR/DONE?");
  fd_defalias(module,"MONGODB/SKIP","CURSOR/SKIP");
  fd_defalias(module,"MONGODB/READ","CURSOR/READ");
  fd_defalias(module,"MONGODB/READ->VECTOR","CURSOR/READVEC");

  fd_defalias(module,"->MONGOVEC","->MONGOVEC");
  fd_defalias(module,"MONGOVEC?","MONGOVEC?");

  fd_defalias(module,"MONGODB/NAME","MONGO/DBNAME");
  fd_defalias(module,"MONGODB/SPEC","MONGO/DBSPEC");
  fd_defalias(module,"MONGODB/URI","MONGO/DBURI");
  fd_defalias(module,"MONGODB/OPTS","MONGO/GETOPTS");
  fd_defalias(module,"MONGODB/SERVER","MONGODB/SERVER");

  fd_defalias(module,"MONGODB/GETCOLLECTION","MONGO/GETCOLLECTION");

  fd_defalias(module,"MONGODB/GETDB","MONGO/GETDB");
  fd_defalias(module,"MONGODB/INFO","MONGO/DBINFO");
  fd_defalias(module,"MONGODB?","MONGODB?");
  fd_defalias(module,"MONGODB/COLLECTION?","MONGO/COLLECTION?");
  fd_defalias(module,"MONGODB/CURSOR?","MONGO/CURSOR?");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
