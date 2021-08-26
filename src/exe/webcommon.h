KNO_EXPORT void kno_init_webtools(void);
KNO_EXPORT void kno_init_texttools(void);

/* Logging declarations */
static u8_mutex log_lock;
static u8_string urllogname = NULL;
static FILE *urllog = NULL;
static u8_string reqlogname = NULL;
static kno_stream reqlog = NULL;
static int reqloglevel = 0;
static int traceweb = 0;
static int webdebug = 0;
static int weballowdebug = 1;
static int logstack = 0;

#define MU U8_MAYBE_UNUSED

static MU int cgitrace = 0;
static MU int trace_cgidata = 0;

static MU int use_threadcache = 0;

/* When non-null, this overrides the document root coming from the
   server.  It is for cases where knocgi is running on a different
   machine than the HTTP server. */
static u8_string docroot = NULL;

/* When the server started, used by UPTIME */
static struct U8_XTIME boot_time;

/* Files that may be used */
static char *portfile = NULL;
static char *pidfile = NULL;
static int U8_MAYBE_UNUSED pid_fd = -1;

#define KNO_REQERRS 1 /* records only transactions which return errors */
#define KNO_ALLREQS 2 /* records all requests */
#define KNO_ALLRESP 3 /* records all requests and the response set back */

static kno_lexenv server_env = NULL;

static MU lispval main_symbol, webmain_symbol, setup_symbol, loadtime_symbol;
static MU lispval cgisymbol, script_filename, reqstart_symbol, reqtick_symbol;
static MU lispval uri_slotid, response_symbol, err_symbol, status_symbol;
static MU lispval browseinfo_symbol, threadcache_symbol;
static MU lispval http_headers, html_headers, doctype_slotid, xmlpi_slotid;
static MU lispval http_accept_language, http_accept_encoding, http_accept_charset;
static MU lispval http_cookie, request_method, http_referer, http_accept;
static MU lispval auth_type, remote_host, remote_user, remote_addr, remote_port;
static MU lispval content_slotid, content_type, query_string, reqdata_symbol;
static MU lispval query_string, script_name, path_info, remote_info, document_root;
static MU lispval content_type, content_length, post_data;
static MU lispval server_port, server_name, path_translated, script_filename;
static MU lispval query_slotid, referer_slotid, forcelog_slotid, tracep_slotid;
static MU lispval webdebug_symbol, output_symbol, error_symbol, cleanup_slotid;
static MU lispval errorpage_symbol, crisispage_symbol;
static MU lispval redirect_slotid, xredirect_slotid;
static MU lispval sendfile_slotid, filedata_slotid;

static lispval default_errorpage = KNO_VOID;
static lispval default_crisispage = KNO_VOID;
static lispval default_notfoundpage = KNO_VOID;
static lispval default_nocontentpage = KNO_VOID;

static lispval webmain = KNO_VOID;

static void init_webcommon_symbols()
{
  uri_slotid = kno_intern("request_uri");
  query_slotid = kno_intern("query_string");
  main_symbol = kno_intern("main");
  reqstart_symbol = kno_intern("_reqstart");
  reqtick_symbol = kno_intern("_reqtick");
  loadtime_symbol = kno_intern("_loadtime");
  webmain_symbol = kno_intern("webmain");
  setup_symbol = kno_intern("setup");
  cgisymbol = kno_intern("cgidata");
  script_filename = kno_intern("script_filename");
  document_root = kno_intern("document_root");
  doctype_slotid = kno_intern("doctype");
  xmlpi_slotid = kno_intern("xmlpi");
  content_type = kno_intern("content-type");
  content_slotid = kno_intern("content");
  sendfile_slotid = kno_intern("_sendfile");
  cleanup_slotid = kno_intern("cleanup");
  html_headers = kno_intern("html-headers");
  http_headers = kno_intern("http-headers");
  tracep_slotid = kno_intern("tracep");
  err_symbol = kno_intern("%err");
  status_symbol = kno_intern("status");
  response_symbol = kno_intern("%response");
  browseinfo_symbol = kno_intern("browseinfo");
  threadcache_symbol = kno_intern("%threadcache");
  referer_slotid = kno_intern("http_referer");
  remote_info = kno_intern("remote_info");
  forcelog_slotid = kno_intern("forcelog");
  webdebug_symbol = kno_intern("webdebug");
  errorpage_symbol = kno_intern("errorpage");
  crisispage_symbol = kno_intern("crisispage");
  output_symbol = kno_intern("output");
  error_symbol = kno_intern("reqerror");
  reqdata_symbol = kno_intern("reqdata");
  request_method = kno_intern("request_method");
  redirect_slotid = kno_intern("_redirect");
  xredirect_slotid = kno_intern("_xredirect");
  filedata_slotid = kno_intern("_filedata");
  remote_port = kno_intern("remote_port");
  remote_host = kno_intern("remote_host");
  remote_user = kno_intern("remote_user");
  remote_addr = kno_intern("remote_addr");
}

/* Preflight/postflight */

static lispval preflight = KNO_EMPTY_LIST;

static int preflight_set(lispval var,lispval val,void *data)
{
  struct KNO_FUNCTION *vf;
  if (!(KNO_APPLICABLEP(val)))
    return kno_reterr(kno_TypeError,"preflight_set",u8_strdup("applicable"),val);
  if (KNO_FUNCTIONP(val)) {
    vf = KNO_FUNCTION_INFO(val);
    if ((vf)&&(vf->fcn_name)&&(vf->fcn_filename)) {
      lispval scan = preflight; while (KNO_PAIRP(scan)) {
        lispval fn = KNO_CAR(scan);
        if (val == fn) return 0;
        else if (KNO_FUNCTIONP(fn)) {
          struct KNO_FUNCTION *f = KNO_FUNCTION_INFO(fn);
          if ((f->fcn_name)&&(f->fcn_filename)&&
              (strcmp(f->fcn_name,vf->fcn_name)==0)&&
              (strcmp(f->fcn_filename,vf->fcn_filename)==0)) {
            struct KNO_PAIR *p = kno_consptr(struct KNO_PAIR *,scan,kno_pair_type);
            p->car = val; kno_incref(val); kno_decref(fn);
            return 0;}}
        scan = KNO_CDR(scan);}}
    preflight = kno_conspair(val,preflight);
    kno_incref(val);
    return 1;}
  else {
    lispval scan = preflight; while (KNO_PAIRP(scan)) {
      lispval fn = KNO_CAR(scan);
      if (val == fn) return 0;
      scan = KNO_CDR(scan);}
    preflight = kno_conspair(val,preflight);
    kno_incref(val);
    return 1;}
}

static lispval preflight_get(lispval var,void *data)
{
  return kno_incref(preflight);
}

static MU lispval run_preflight()
{
  KNO_DOLIST(fn,preflight) {
    lispval v = kno_apply(fn,0,NULL);
    if (KNO_ABORTP(v)) return v;
    else if (KNO_VOIDP(v)) {}
    else if (KNO_EMPTY_CHOICEP(v)) {}
    else if (KNO_FALSEP(v)) {}
    else return v;}
  return KNO_FALSE;
}

static lispval postflight = KNO_EMPTY_LIST;

static int postflight_set(lispval var,lispval val,void *data)
{
  struct KNO_FUNCTION *vf;
  if (!(KNO_APPLICABLEP(val)))
    return kno_reterr(kno_TypeError,"postflight_set",u8_strdup("applicable"),val);
  if (KNO_FUNCTIONP(val)) {
    vf = KNO_FUNCTION_INFO(val);
    if ((vf)&&(vf->fcn_name)&&(vf->fcn_filename)) {
      lispval scan = postflight; while (KNO_PAIRP(scan)) {
        lispval fn = KNO_CAR(scan);
        if (val == fn) return 0;
        else if (KNO_FUNCTIONP(fn)) {
          struct KNO_FUNCTION *f = KNO_FUNCTION_INFO(fn);
          if ((f->fcn_name)&&(f->fcn_filename)&&
              (strcmp(f->fcn_name,vf->fcn_name)==0)&&
              (strcmp(f->fcn_filename,vf->fcn_filename)==0)) {
            struct KNO_PAIR *p = kno_consptr(struct KNO_PAIR *,scan,kno_pair_type);
            p->car = val; kno_incref(val); kno_decref(fn);
            return 0;}}
        scan = KNO_CDR(scan);}}
    postflight = kno_conspair(val,postflight);
    kno_incref(val);
    return 1;}
  else {
    lispval scan = postflight; while (KNO_PAIRP(scan)) {
      lispval fn = KNO_CAR(scan);
      if (val == fn) return 0;
      scan = KNO_CDR(scan);}
    postflight = kno_conspair(val,postflight);
    kno_incref(val);
    return 1;}
}

static lispval postflight_get(lispval var,void *data)
{
  return kno_incref(postflight);
}

static MU void run_postflight()
{
  KNO_DOLIST(fn,postflight) {
    lispval v = kno_apply(fn,0,NULL);
    if (KNO_ABORTP(v)) {
      u8_log(LOG_CRIT,"postflight","Error from postflight %q",fn);
      kno_clear_errors(1);}
    kno_decref(v);}
}

/* Miscellaneous Server functions */

static lispval get_boot_time()
{
  return kno_make_timestamp(&boot_time);
}

static lispval get_uptime()
{
  struct U8_XTIME now; u8_now(&now);
  return kno_init_double(NULL,u8_xtime_diff(&now,&boot_time));
}

/* URLLOG config */

/* The urllog is a plain text (UTF-8) file which contains all of the
   URLs passed to the servlet.  This can be used for generating new load tests
   or for general debugging. */

static int urllog_set(lispval var,lispval val,void *data)
{
  if (KNO_STRINGP(val)) {
    u8_string filename = KNO_CSTRING(val);
    u8_lock_mutex(&log_lock);
    if (urllog) {
      fclose(urllog); urllog = NULL;
      u8_free(urllogname); urllogname = NULL;}
    urllog = u8_fopen_locked(filename,"a");
    if (urllog) {
      u8_string tmp;
      urllogname = u8_strdup(filename);
      tmp = u8_mkstring("# Log open %*lt for %s\n",u8_sessionid());
      fputs(tmp,urllog);
      u8_unlock_mutex(&log_lock);
      u8_free(tmp);
      return 1;}
    else return -1;}
  else if (KNO_FALSEP(val)) {
    u8_lock_mutex(&log_lock);
    if (urllog) {
      fclose(urllog); urllog = NULL;
      u8_free(urllogname); urllogname = NULL;}
    u8_unlock_mutex(&log_lock);
    return 0;}
  else return kno_reterr
         (kno_TypeError,"config_set_urllog",u8_strdup(_("string")),val);
}

static lispval urllog_get(lispval var,void *data)
{
  if (urllog)
    return kno_mkstring(urllogname);
  else return KNO_FALSE;
}

/* REQLOG config */

/* The reqlog is a binary (DTYPE) file containing detailed debugging information.
   It contains the CGI data record processed for each transaction, together with
   any error objects returned and the actual text string sent back to the client.

   The reqloglevel determines which transactions are put in the reqlog.  There
   are currently three levels:
    1 (KNO_REQERRS) records only transactions which return errors.
    2 (KNO_ALLREQS) records all requests.
    3 (KNO_ALLRESP) records all requests and the response set back.
*/

static int reqlog_set(lispval var,lispval val,void *data)
{
  if (KNO_STRINGP(val)) {
    u8_string filename = KNO_CSTRING(val);
    u8_lock_mutex(&log_lock);
    if ((reqlogname) && (strcmp(filename,reqlogname)==0)) {
      kno_flush_stream(reqlog);
      u8_unlock_mutex(&log_lock);
      return 0;}
    else if (reqlog) {
      kno_close_stream(reqlog,0); reqlog = NULL;
      u8_free(reqlogname); reqlogname = NULL;}
    reqlog = u8_alloc(struct KNO_STREAM);
    if (kno_init_file_stream(reqlog,filename,
                            KNO_FILE_WRITE,-1,
                            30000)) {
      u8_string logstart=
        u8_mkstring("# Log open %*lt for %s",u8_sessionid());
      lispval logstart_entry = kno_wrapstring(logstart);
      kno_endpos(reqlog);
      reqlogname = u8_strdup(filename);
      kno_write_dtype(kno_writebuf(reqlog),logstart_entry);
      kno_decref(logstart_entry);
      u8_unlock_mutex(&log_lock);
      return 1;}
    else {
      u8_unlock_mutex(&log_lock);
      u8_free(reqlog); return -1;}}
  else if (KNO_FALSEP(val)) {
    u8_lock_mutex(&log_lock);
    if (reqlog) {
      kno_close_stream(reqlog,0); reqlog = NULL;
      u8_free(reqlogname); reqlogname = NULL;}
    u8_unlock_mutex(&log_lock);
    return 0;}
  else return kno_reterr
         (kno_TypeError,"config_set_urllog",u8_strdup(_("string")),val);
}

static lispval reqlog_get(lispval var,void *data)
{
  if (reqlog)
    return kno_mkstring(reqlogname);
  else return KNO_FALSE;
}

/* Logging primitive */

static void dolog
  (lispval cgidata,lispval val,u8_string response,size_t len,double exectime)
{
  u8_lock_mutex(&log_lock);
  if (trace_cgidata) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,1024);
    kno_pprint(&out,cgidata,NULL,2,0,50);
    fputs(out.u8_outbuf,stderr); fputc('\n',stderr);
    u8_free(out.u8_outbuf);}
  if (KNO_NULLP(val)) {
    /* This is pre execution */
    if (urllog) {
      lispval uri = kno_get(cgidata,uri_slotid,KNO_VOID);
      u8_string tmp = u8_mkstring(">%s\n@%*lt %g/%g\n",
                                KNO_CSTRING(uri),
                                exectime,
                                u8_elapsed_time());
      fputs(tmp,urllog); u8_free(tmp);
      kno_decref(uri);}}
  else if (KNO_ABORTP(val)) {
    if (urllog) {
      lispval uri = kno_get(cgidata,uri_slotid,KNO_VOID); u8_string tmp;
      if (KNO_TROUBLEP(val)) {
        u8_exception ex = u8_erreify();
        if (ex == NULL)
          tmp = u8_mkstring("!%s\n@%*lt %g/%g (mystery error)\n",KNO_CSTRING(uri),
                          exectime,u8_elapsed_time());

        else if (ex->u8x_context)
          tmp = u8_mkstring("!%s\n@%*lt %g/%g %s %s\n",KNO_CSTRING(uri),
                          exectime,u8_elapsed_time(),
                          ex->u8x_cond,ex->u8x_context);
        else tmp = u8_mkstring("!%s\n@%*lt %g/%g %s\n",KNO_CSTRING(uri),
                             exectime,u8_elapsed_time(),ex->u8x_cond);}
      else tmp = u8_mkstring("!%s\n@%*lt %g/%g %q\n",KNO_CSTRING(uri),
                           exectime,u8_elapsed_time(),val);
      fputs(tmp,urllog); u8_free(tmp);}
    if (reqlog) {
      kno_store(cgidata,err_symbol,val);
      kno_write_dtype(kno_writebuf(reqlog),cgidata);}}
  else {
    if (urllog) {
      lispval uri = kno_get(cgidata,uri_slotid,KNO_VOID);
      u8_string tmp = u8_mkstring("<%s\n@%*lt %ld %g/%g\n",KNO_CSTRING(uri),
                                len,exectime,u8_elapsed_time());
      fputs(tmp,urllog); u8_free(tmp);}
    if ((reqlog) && (reqloglevel>2))
      kno_store(cgidata,response_symbol,kno_mkstring(response));
    if ((reqlog) && (reqloglevel>1))
      kno_write_dtype(kno_writebuf(reqlog),cgidata);}
  u8_unlock_mutex(&log_lock);
}

/* Preloads */

struct KNO_PRELOAD_LIST {
  u8_string preload_filename;
  time_t preload_mtime;
  struct KNO_PRELOAD_LIST *next_preload;} *preloads = NULL;

static u8_mutex preload_lock;

static lispval preload_get(lispval var,void *ignored)
{
  lispval results = KNO_EMPTY_LIST; struct KNO_PRELOAD_LIST *scan;
  u8_lock_mutex(&preload_lock);
  scan = preloads; while (scan) {
    results = kno_conspair(kno_mkstring(scan->preload_filename),results);
    scan = scan->next_preload;}
  u8_unlock_mutex(&preload_lock);
  return results;
}

static int preload_set(lispval var,lispval val,void *ignored)
{
  if (!(KNO_STRINGP(val)))
    return kno_reterr
      (kno_TypeError,"preload_config_set",u8_strdup("string"),val);
  else if (KNO_STRLEN(val)==0)
    return 0;
  else {
    struct KNO_PRELOAD_LIST *scan;
    u8_string filename = KNO_CSTRING(val), abspath;
    time_t mtime;
    if (!(u8_file_existsp(filename)))
      return kno_reterr(kno_FileNotFound,"preload_config_set",
                       u8_strdup(filename),KNO_VOID);
    u8_lock_mutex(&preload_lock);
    scan = preloads; while (scan) {
      if (strcmp(filename,scan->preload_filename)==0) {
        mtime = u8_file_mtime(filename);
        if (mtime>scan->preload_mtime) break;
        u8_unlock_mutex(&preload_lock);
        return 0;}
      else scan = scan->next_preload;}
    if (server_env == NULL) server_env = kno_working_lexenv();
    scan = u8_alloc(struct KNO_PRELOAD_LIST);
    scan->preload_filename = abspath = u8_abspath(filename,NULL);
    scan->preload_mtime = (time_t)-1;
    scan->next_preload = preloads;
    preloads = scan;
    u8_unlock_mutex(&preload_lock);
    u8_log(LOG_NOTICE,"WebPreload","Preloading '%s'",abspath);
    return 1;}
}

double last_preload_update = -1.0;

static int update_preloads()
{
  if ((last_preload_update<0) ||
      ((u8_elapsed_time()-last_preload_update)>1.0)) {
    struct KNO_PRELOAD_LIST *scan; int n_reloads = 0;
    u8_lock_mutex(&preload_lock);
    if ((u8_elapsed_time()-last_preload_update)<1.0) {
      u8_unlock_mutex(&preload_lock);
      return 0;}
    scan = preloads; while (scan) {
      time_t mtime = u8_file_mtime(scan->preload_filename);
      if (mtime>scan->preload_mtime) {
        lispval load_result;
        u8_unlock_mutex(&preload_lock);
        load_result = kno_load_source(scan->preload_filename,server_env,"auto");
        if (KNO_ABORTP(load_result)) {
          return kno_interr(load_result);}
        n_reloads++;
        kno_decref(load_result);
        u8_lock_mutex(&preload_lock);
        if (mtime>scan->preload_mtime)
          scan->preload_mtime = mtime;}
      scan = scan->next_preload;}
    last_preload_update = u8_elapsed_time();
    u8_unlock_mutex(&preload_lock);
    return n_reloads;}
  else return 0;
}

static lispval get_web_handler(kno_lexenv env,u8_string src)
{
  lispval handler = kno_symeval(webmain_symbol,env);
  if ( (KNO_DEFAULTP(handler)) ||
       (KNO_VOIDP(handler))    ||
       (KNO_UNBOUNDP(handler)) ||
       (KNO_ABORTP(handler)) )
    handler = kno_symeval(main_symbol,env);
  if ( (KNO_DEFAULTP(handler)) ||
       (KNO_VOIDP(handler))    ||
       (KNO_UNBOUNDP(handler)) ||
       (KNO_ABORTP(handler)) )
    handler = kno_incref(webmain);
  if (!(KNO_APPLICABLEP(handler))) {
      u8_log(LOG_CRIT,"ServletMainNotApplicable",
             "From default environment: %q",handler);
      return kno_err("ServletMainNotApplicable","get_web_handler",
                     src,handler);}
  else return handler;
}

/* Getting content for pages */

static u8_mutex pagemap_lock;
static KNO_HASHTABLE pagemap;

static int whitespace_stringp(u8_string s)
{
  if (s == NULL) return 1;
  else {
    int c = u8_sgetc(&s);
    while (c>0)
      if (u8_isspace(c)) c = u8_sgetc(&s);
      else return 0;
    return 1;}
}

static lispval loadcontent(lispval path)
{
  u8_string pathname = KNO_CSTRING(path), oldsource;
  double load_start = u8_elapsed_time();
  u8_string content = u8_filestring(pathname,NULL);
  if (traceweb>0)
    u8_log(LOG_NOTICE,"LOADING","Loading %s",pathname);
  if (!(content)) {
    u8_seterr(kno_FileNotFound,"loadcontent",u8_strdup(pathname));
    return KNO_ERROR_VALUE;}
  if (content[0]=='<') {
    U8_INPUT in; KNO_XML *xml; kno_lexenv env;
    lispval lenv, ldata, parsed;
    U8_INIT_STRING_INPUT(&in,strlen(content),content);
    oldsource = kno_bind_sourcebase(pathname);
    xml = kno_read_knoml(&in,(KNO_SLOPPY_XML|KNO_XML_KEEP_RAW));
    kno_restore_sourcebase(oldsource);
    if (xml == NULL) {
      u8_free(content);
      if (u8_current_exception == NULL) {
        u8_seterr("BadKNOML","loadconfig/knoml",u8_strdup(pathname));}
      u8_log(LOG_CRIT,Startup,"ERROR","Error parsing %s",pathname);
      return KNO_ERROR_VALUE;}
    parsed = xml->xml_head;
    while ((KNO_PAIRP(parsed)) &&
           (KNO_STRINGP(KNO_CAR(parsed))) &&
           (whitespace_stringp(KNO_CSTRING(KNO_CAR(parsed))))) {
      struct KNO_PAIR *old_parsed = (struct KNO_PAIR *)parsed;
      parsed = KNO_CDR(parsed);
      old_parsed->cdr = KNO_EMPTY_LIST;}
    ldata = parsed;
    env = (kno_lexenv)xml->xml_data; lenv = (lispval)env;
    if (traceweb>0)
      u8_log(LOG_NOTICE,"LOADED","Loaded %s in %f secs",
                pathname,u8_elapsed_time()-load_start);
    kno_incref(ldata); kno_incref(lenv);
    u8_free(content);
    kno_free_xml_node(xml);
    u8_free(xml);

    return kno_conspair(ldata,lenv);}
  else {
    kno_lexenv newenv=
      ((server_env) ? (kno_make_env(kno_make_hashtable(NULL,17),server_env)) :
       (kno_working_lexenv()));
    lispval load_result = kno_load_source(pathname,newenv,NULL);
    /* We reload the file.  There should really be an API call to
       evaluate a source string (kno_eval_source?).  This could then
       use that. */
    u8_free(content);
    if (KNO_TROUBLEP(load_result)) {
      if (u8_current_exception == NULL) {
        u8_seterr("LoadSourceFailed","loadcontent/scheme",
                  u8_strdup(pathname));}
      return load_result;}
    kno_decref(load_result);
    lispval main_proc = get_web_handler(newenv,pathname);
    if (traceweb>0)
      u8_log(LOG_NOTICE,"LOADED","Loaded %s in %f secs, \nwebmain=%q",
             pathname,u8_elapsed_time()-load_start,main_proc);
    return kno_conspair(main_proc,(lispval)newenv);}
}

static lispval update_pagemap(lispval path)
{
  struct stat fileinfo; struct U8_XTIME mtime;
  char *lpath = u8_localpath(KNO_CSTRING(path));
  int retval = stat(lpath,&fileinfo);
  if (retval<0) {
    u8_log(LOG_CRIT,"StatFailed","Stat on %s failed (errno=%d)",
           lpath,errno);
    u8_graberrno("getcontent",lpath);
    return KNO_ERROR_VALUE;}
  u8_init_xtime(&mtime,fileinfo.st_mtime,u8_second,0,0,0);
  lispval content = loadcontent(path);
  if (KNO_ABORTP(content)) {
    u8_free(lpath);
    return content;}
  lispval pagemap_value =
    kno_conspair(kno_make_timestamp(&mtime),
                kno_incref(content));
  kno_hashtable_store(&pagemap,path,pagemap_value);
  kno_decref(pagemap_value);
  u8_free(lpath);
  return content;
}

static lispval getcontent(lispval path)
{
  if ( (KNO_STRINGP(path)) &&
       (u8_file_existsp(KNO_CSTRING(path)))) {
    struct stat fileinfo; struct U8_XTIME mtime;
    char *lpath = u8_localpath(KNO_CSTRING(path));
    int retval = stat(lpath,&fileinfo);
    if (retval<0) {
      u8_log(LOG_CRIT,"StatFailed","Stat on %s failed (errno=%d)",
             lpath,errno);
      u8_graberrno("getcontent",lpath);
      return KNO_ERROR_VALUE;}
    else u8_init_xtime(&mtime,fileinfo.st_mtime,u8_second,0,0,0);
    lispval pagemap_value = kno_hashtable_get(&pagemap,path,KNO_VOID);
    if (KNO_VOIDP(pagemap_value)) {
      u8_lock_mutex(&pagemap_lock);
      pagemap_value = kno_hashtable_get(&pagemap,path,KNO_VOID);
      if (KNO_VOIDP(pagemap_value)) {
        lispval content = update_pagemap(path);
        u8_unlock_mutex(&pagemap_lock);
        return content;}
      else {
        lispval content=KNO_CDR(pagemap_value);
        kno_incref(content);
        kno_decref(pagemap_value);
        u8_unlock_mutex(&pagemap_lock);
        return content;}}
    lispval tval = KNO_CAR(pagemap_value), cval = KNO_CDR(pagemap_value);
    struct KNO_TIMESTAMP *lmtime=
      kno_consptr(kno_timestamp,tval,kno_timestamp_type);
    if ( (fileinfo.st_mtime) <= (lmtime->u8xtimeval.u8_tick) )
      /* Loaded version up to date */
      return kno_incref(cval);
    u8_lock_mutex(&pagemap_lock);
    kno_decref(pagemap_value);
    pagemap_value = kno_hashtable_get(&pagemap,path,KNO_VOID);
    if (KNO_VOIDP(pagemap_value)) {
      /* This *should* never happen, but we check anyway */
      lispval content = update_pagemap(path);
      u8_unlock_mutex(&pagemap_lock);
      return content;}
    tval = KNO_CAR(pagemap_value);
    cval = KNO_CDR(pagemap_value);
    lmtime = kno_consptr(kno_timestamp,tval,kno_timestamp_type);
    if ( (fileinfo.st_mtime) <= (lmtime->u8xtimeval.u8_tick) ) {
      /* Loaded version made up to date before while we got the lock  */
      kno_incref(cval);
      u8_unlock_mutex(&pagemap_lock);
      u8_free(lpath);
      return cval;}
    else {
      lispval content=update_pagemap(path);
      u8_unlock_mutex(&pagemap_lock);
      u8_free(lpath);
      return content;}}
  else if (KNO_STRINGP(path)) {
    lispval handler = (server_env) ? (get_web_handler(server_env,NULL)) :
      (KNO_VOID);
    if (KNO_ABORTP(handler)) 
      return handler;
    else if (KNO_APPLICABLEP(handler))
      return handler;
    else {
      u8_log(LOG_CRIT,"FileNotFound","Content file %s",KNO_CSTRING(path));
      return kno_err(kno_FileNotFound,"getcontent",NULL,path);}}
  else {
    u8_log(LOG_CRIT,"BadPathArg","To getcontent");
    return kno_type_error("pathname","getcontent",path);}
}

/* Check threadcache */

static MU struct KNO_THREAD_CACHE *checkthreadcache(kno_lexenv env)
{
  lispval tcval = kno_symeval(threadcache_symbol,env);
  if (KNO_FALSEP(tcval)) return NULL;
  else if ((KNO_VOIDP(tcval))&&(!(use_threadcache)))
    return NULL;
  else return kno_use_threadcache();
}

/* Init configs */

static void init_webcommon_data()
{
  KNO_INIT_STATIC_CONS(&pagemap,kno_hashtable_type);
  kno_make_hashtable(&pagemap,0);
  u8_init_mutex(&pagemap_lock);
}

static void init_webcommon_configs()
{
  kno_register_config("TRACEWEB",_("Trace all web transactions"),
                     kno_boolconfig_get,kno_boolconfig_set,&traceweb);
  kno_register_config("WEBDEBUG",_("Show backtraces on errors"),
                     kno_boolconfig_get,kno_boolconfig_set,&webdebug);
  kno_register_config("WEBALLOWDEBUG",_("Allow requests to specify debugging"),
                     kno_boolconfig_get,kno_boolconfig_set,&weballowdebug);
  kno_register_config("LOGSTACK",_("Log error stacktraces"),
                     kno_boolconfig_get,kno_boolconfig_set,&logstack);
  kno_register_config("ERRORPAGE",_("Default error page for web errors"),
                     kno_lconfig_get,kno_lconfig_set,&default_errorpage);
  kno_register_config
    ("CRISISPAGE",
     _("Default crisis page (for when the error page yields an error)"),
     kno_lconfig_get,kno_lconfig_set,&default_crisispage);
  kno_register_config
    ("NOTFOUNDPAGE",
     _("Default not found page (for when specified content isn't found)"),
     kno_lconfig_get,kno_lconfig_set,&default_notfoundpage);
  kno_register_config
    ("NOCONTENTPAGE",
     _("Default no content page (for when content fetch failed generically)"),
     kno_lconfig_get,kno_lconfig_set,&default_nocontentpage);

  kno_register_config("PRELOAD",
                     _("Files to preload into the shared environment"),
                     preload_get,preload_set,NULL);
  kno_register_config("URLLOG",_("Where to write URLs where were requested"),
                     urllog_get,urllog_set,NULL);
  kno_register_config("REQLOG",_("Where to write request objects"),
                     reqlog_get,reqlog_set,NULL);
  kno_register_config("REQLOGLEVEL",_("Level of transaction logging"),
                     kno_intconfig_get,kno_intconfig_set,&reqloglevel);
  kno_register_config("THREADCACHE",_("Use per-request thread cache"),
                     kno_boolconfig_get,kno_boolconfig_set,&use_threadcache);
  kno_register_config("TRACECGI",_("Whether to log all cgidata"),
                     kno_boolconfig_get,kno_boolconfig_set,&trace_cgidata);
  kno_register_config("DOCROOT",
                     _("File base (directory) for resolving requests"),
                     kno_sconfig_get,kno_sconfig_set,&docroot);
  kno_register_config("PREFLIGHT",
                     _("Preflight (before request handling) procedures"),
                     preflight_get,preflight_set,NULL);
  kno_register_config("POSTFLIGHT",
                     _("POSTFLIGHT (before request handling) procedures"),
                     postflight_get,postflight_set,NULL);

  kno_register_config("WEBMAIN",
                     _("WEBMAIN default procedure for handling requests"),
                     kno_lconfig_get,kno_lconfig_set,&webmain);

  kno_autoload_config("WEBMOD","WEBLOAD","WEBINITS");
}

static void shutdown_server(void);

static void webcommon_shutdown(u8_condition why)
{
  u8_string exit_filename = kno_runbase_filename(".exit");
  FILE *exitfile = u8_fopen(exit_filename,"w");
  u8_log(LOG_CRIT,"web_shutdown","Shutting down server on %s",
         ((why == NULL)?((u8_condition)"a whim"):(why)));
  if (portfile)
    if (remove(portfile)>=0) {
      u8_free(portfile); portfile = NULL;}
  if (pidfile) {
    u8_removefile(pidfile);}
  pidfile = NULL;
  if (pagemap.conshead)
    kno_recycle_hashtable(&pagemap);
  if (exitfile) {
    struct U8_XTIME xt; struct U8_OUTPUT out;
    char timebuf[64]; double elapsed = u8_elapsed_time();
    u8_now(&xt); U8_INIT_FIXED_OUTPUT(&out,sizeof(timebuf),timebuf);
    u8_xtime_to_iso8601(&out,&xt);
    if (why)
      fprintf(exitfile,"%d@%s(%f) %s\n",getpid(),timebuf,elapsed,why);
    else fprintf(exitfile,"%d@%s(%f)\n",getpid(),timebuf,elapsed);
    fclose(exitfile);}
  u8_free(exit_filename);
}

static u8_byte shutdown_buf[64];
static u8_condition shutdown_reason = NULL;
static int server_shutdown = 0;

static void shutdown_onsignal(int sig,siginfo_t *info,void *data)
{
  char buf[64];
  if (server_shutdown) {
    u8_log(LOG_CRIT,"shutdown_server_onsignal",
           "Already shutdown but received signal %d",
           sig);
    return;}
#ifdef SIGHUP
  if (sig == SIGHUP) {
    server_shutdown = 1;
    shutdown_reason = "SIGHUP";
    shutdown_server();
    return;}
#endif
#ifdef SIGHUP
  if (sig == SIGQUIT) {
    server_shutdown = 1;
    shutdown_reason = "SIGQUIT";
    shutdown_server();
    return;}
#endif
#ifdef SIGHUP
  if (sig == SIGTERM) {
    server_shutdown = 1;
    shutdown_reason = "SIGTERM";
    shutdown_server();
    return;}
#endif
  sprintf(shutdown_buf,"SIG%d",sig);
  server_shutdown = 1;
  shutdown_reason = (u8_condition)buf;
  shutdown_server();
  return;
}

static struct sigaction sigaction_ignore;
static struct sigaction sigaction_shutdown;

static void shutdown_on_exit()
{
  if (server_shutdown) return;
  server_shutdown = 1;
  shutdown_reason = "EXIT";
  shutdown_server();
}

static void init_webcommon_finalize()
{
  atexit(shutdown_on_exit);

  memset(&sigaction_ignore,0,sizeof(sigaction_ignore));
  sigaction_ignore.sa_handler = SIG_IGN;

  memset(&sigaction_shutdown,0,sizeof(sigaction_ignore));
  sigaction_shutdown.sa_sigaction = shutdown_onsignal;
  sigaction_shutdown.sa_flags = SA_SIGINFO;

#ifdef SIGHUP
  sigaction(SIGHUP,&sigaction_shutdown,NULL);
#endif
#ifdef SIGTERM
  sigaction(SIGTERM,&sigaction_shutdown,NULL);
#endif
#ifdef SIGQUIT
  sigaction(SIGQUIT,&sigaction_shutdown,NULL);
#endif

  /* Set signal masks */

#if HAVE_SIGPROCMASK
  {
    sigset_t newset, oldset, newer;
    sigemptyset(&newset);
    sigemptyset(&oldset);
    sigemptyset(&newer);
#if SIGQUIT
    sigaddset(&newset,SIGQUIT);
#endif
#if SIGTERM
    sigaddset(&newset,SIGTERM);
#endif
#if SIGHUP
    sigaddset(&newset,SIGHUP);
#endif
#if SIGKILL
    sigaddset(&newset,SIGKILL);
#endif
    if (sigprocmask(SIG_UNBLOCK,&newset,&oldset)<0)
      u8_log(LOG_WARN,"Sigerror","Error setting signal mask");
  }
#elif HAVE_SIGSETMASK
  /* We set this here because otherwise, it will often inherit
     the signal mask of its apache parent, which is inappropriate. */
  sigsetmask(0);
#endif

}

/* Fixing CGIDATA for a different docroot */

static lispval webcommon_adjust_docroot(lispval cgidata,u8_string docroot)
{
  if (docroot) {
    lispval incoming_docroot = kno_get(cgidata,document_root,KNO_VOID);
    if (KNO_STRINGP(incoming_docroot)) {
      lispval scriptname = kno_get(cgidata,script_filename,KNO_VOID);
      lispval lisp_docroot = kno_mkstring(docroot);
      kno_store(cgidata,document_root,lisp_docroot);
      if ((KNO_STRINGP(scriptname))&&
          ((strncmp(KNO_CSTRING(scriptname),KNO_CSTRING(incoming_docroot),
                    KNO_STRLEN(incoming_docroot)))==0)) {
        u8_string local_scriptname = u8_string_append
          (docroot,KNO_CSTRING(scriptname)+KNO_STRLEN(incoming_docroot),NULL);
        lispval new_scriptname = kno_init_string(NULL,-1,local_scriptname);
        kno_store(cgidata,script_filename,new_scriptname);
        kno_decref(new_scriptname);}
      kno_decref(lisp_docroot); kno_decref(scriptname);}
    kno_decref(incoming_docroot);
    return cgidata;}
  else return cgidata;
}


