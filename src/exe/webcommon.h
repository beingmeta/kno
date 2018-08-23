FD_EXPORT void fd_init_fdweb(void);
FD_EXPORT void fd_init_texttools(void);

/* Logging declarations */
static u8_mutex log_lock;
static u8_string urllogname = NULL;
static FILE *urllog = NULL;
static u8_string reqlogname = NULL;
static fd_stream reqlog = NULL;
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
   server.  It is for cases where fdservlet is running on a different
   machine than the HTTP server. */
static u8_string docroot = NULL;

/* When the server started, used by UPTIME */
static struct U8_XTIME boot_time;

/* Files that may be used */
static char *portfile = NULL;
static char *pidfile = NULL;
static int U8_MAYBE_UNUSED pid_fd = -1;

#define FD_REQERRS 1 /* records only transactions which return errors */
#define FD_ALLREQS 2 /* records all requests */
#define FD_ALLRESP 3 /* records all requests and the response set back */

static fd_lexenv server_env = NULL;

static MU lispval main_symbol, webmain_symbol, setup_symbol;
static MU lispval cgisymbol, script_filename;
static MU lispval uri_slotid, response_symbol, err_symbol, status_symbol;
static MU lispval browseinfo_symbol, threadcache_symbol;
static MU lispval http_headers, html_headers, doctype_slotid, xmlpi_slotid;
static MU lispval http_accept_language, http_accept_encoding, http_accept_charset;
static MU lispval http_cookie, request_method, http_referer, http_accept;
static MU lispval auth_type, remote_host, remote_user, remote_port;
static MU lispval content_slotid, content_type, query_string, reqdata_symbol;
static MU lispval query_string, script_name, path_info, remote_info, document_root;
static MU lispval content_type, content_length, post_data;
static MU lispval server_port, server_name, path_translated, script_filename;
static MU lispval query_slotid, referer_slotid, forcelog_slotid, tracep_slotid;
static MU lispval webdebug_symbol, output_symbol, error_symbol, cleanup_slotid;
static MU lispval errorpage_symbol, crisispage_symbol;
static MU lispval redirect_slotid, xredirect_slotid;
static MU lispval sendfile_slotid, filedata_slotid;

static lispval default_errorpage = FD_VOID;
static lispval default_crisispage = FD_VOID;
static lispval default_notfoundpage = FD_VOID;
static lispval default_nocontentpage = FD_VOID;

static lispval webmain = FD_VOID;

static void init_webcommon_symbols()
{
  uri_slotid = fd_intern("REQUEST_URI");
  query_slotid = fd_intern("QUERY_STRING");
  main_symbol = fd_intern("MAIN");
  webmain_symbol = fd_intern("WEBMAIN");
  setup_symbol = fd_intern("SETUP");
  cgisymbol = fd_intern("CGIDATA");
  script_filename = fd_intern("SCRIPT_FILENAME");
  document_root = fd_intern("DOCUMENT_ROOT");
  doctype_slotid = fd_intern("DOCTYPE");
  xmlpi_slotid = fd_intern("XMLPI");
  content_type = fd_intern("CONTENT-TYPE");
  content_slotid = fd_intern("CONTENT");
  sendfile_slotid = fd_intern("_SENDFILE");
  cleanup_slotid = fd_intern("CLEANUP");
  html_headers = fd_intern("HTML-HEADERS");
  http_headers = fd_intern("HTTP-HEADERS");
  tracep_slotid = fd_intern("TRACEP");
  err_symbol = fd_intern("%ERR");
  status_symbol = fd_intern("STATUS");
  response_symbol = fd_intern("%RESPONSE");
  browseinfo_symbol = fd_intern("BROWSEINFO");
  threadcache_symbol = fd_intern("%THREADCACHE");
  referer_slotid = fd_intern("HTTP_REFERER");
  remote_info = fd_intern("REMOTE_INFO");
  forcelog_slotid = fd_intern("FORCELOG");
  webdebug_symbol = fd_intern("WEBDEBUG");
  errorpage_symbol = fd_intern("ERRORPAGE");
  crisispage_symbol = fd_intern("CRISISPAGE");
  output_symbol = fd_intern("OUTPUT");
  error_symbol = fd_intern("REQERROR");
  reqdata_symbol = fd_intern("REQDATA");
  request_method = fd_intern("REQUEST_METHOD");
  redirect_slotid = fd_intern("_REDIRECT");
  xredirect_slotid = fd_intern("_XREDIRECT");
  filedata_slotid = fd_intern("_FILEDATA");
}

/* Preflight/postflight */

static lispval preflight = FD_EMPTY_LIST;

static int preflight_set(lispval var,lispval val,void *data)
{
  struct FD_FUNCTION *vf;
  if (!(FD_APPLICABLEP(val)))
    return fd_reterr(fd_TypeError,"preflight_set",u8_strdup("applicable"),val);
  if (FD_FUNCTIONP(val)) {
    vf = FD_DTYPE2FCN(val);
    if ((vf)&&(vf->fcn_name)&&(vf->fcn_filename)) {
      lispval scan = preflight; while (FD_PAIRP(scan)) {
        lispval fn = FD_CAR(scan);
        if (val == fn) return 0;
        else if (FD_FUNCTIONP(fn)) {
          struct FD_FUNCTION *f = FD_DTYPE2FCN(fn);
          if ((f->fcn_name)&&(f->fcn_filename)&&
              (strcmp(f->fcn_name,vf->fcn_name)==0)&&
              (strcmp(f->fcn_filename,vf->fcn_filename)==0)) {
            struct FD_PAIR *p = fd_consptr(struct FD_PAIR *,scan,fd_pair_type);
            p->car = val; fd_incref(val); fd_decref(fn);
            return 0;}}
        scan = FD_CDR(scan);}}
    preflight = fd_conspair(val,preflight);
    fd_incref(val);
    return 1;}
  else {
    lispval scan = preflight; while (FD_PAIRP(scan)) {
      lispval fn = FD_CAR(scan);
      if (val == fn) return 0;
      scan = FD_CDR(scan);}
    preflight = fd_conspair(val,preflight);
    fd_incref(val);
    return 1;}
}

static lispval preflight_get(lispval var,void *data)
{
  return fd_incref(preflight);
}

static MU lispval run_preflight()
{
  FD_DOLIST(fn,preflight) {
    lispval v = fd_apply(fn,0,NULL);
    if (FD_ABORTP(v)) return v;
    else if (FD_VOIDP(v)) {}
    else if (FD_EMPTY_CHOICEP(v)) {}
    else if (FD_FALSEP(v)) {}
    else return v;}
  return FD_FALSE;
}

static lispval postflight = FD_EMPTY_LIST;

static int postflight_set(lispval var,lispval val,void *data)
{
  struct FD_FUNCTION *vf;
  if (!(FD_APPLICABLEP(val)))
    return fd_reterr(fd_TypeError,"postflight_set",u8_strdup("applicable"),val);
  if (FD_FUNCTIONP(val)) {
    vf = FD_DTYPE2FCN(val);
    if ((vf)&&(vf->fcn_name)&&(vf->fcn_filename)) {
      lispval scan = postflight; while (FD_PAIRP(scan)) {
        lispval fn = FD_CAR(scan);
        if (val == fn) return 0;
        else if (FD_FUNCTIONP(fn)) {
          struct FD_FUNCTION *f = FD_DTYPE2FCN(fn);
          if ((f->fcn_name)&&(f->fcn_filename)&&
              (strcmp(f->fcn_name,vf->fcn_name)==0)&&
              (strcmp(f->fcn_filename,vf->fcn_filename)==0)) {
            struct FD_PAIR *p = fd_consptr(struct FD_PAIR *,scan,fd_pair_type);
            p->car = val; fd_incref(val); fd_decref(fn);
            return 0;}}
        scan = FD_CDR(scan);}}
    postflight = fd_conspair(val,postflight);
    fd_incref(val);
    return 1;}
  else {
    lispval scan = postflight; while (FD_PAIRP(scan)) {
      lispval fn = FD_CAR(scan);
      if (val == fn) return 0;
      scan = FD_CDR(scan);}
    postflight = fd_conspair(val,postflight);
    fd_incref(val);
    return 1;}
}

static lispval postflight_get(lispval var,void *data)
{
  return fd_incref(postflight);
}

static MU void run_postflight()
{
  FD_DOLIST(fn,postflight) {
    lispval v = fd_apply(fn,0,NULL);
    if (FD_ABORTP(v)) {
      u8_log(LOG_CRIT,"postflight","Error from postflight %q",fn);
      fd_clear_errors(1);}
    fd_decref(v);}
}

/* Miscellaneous Server functions */

static lispval get_boot_time()
{
  return fd_make_timestamp(&boot_time);
}

static lispval get_uptime()
{
  struct U8_XTIME now; u8_now(&now);
  return fd_init_double(NULL,u8_xtime_diff(&now,&boot_time));
}

/* URLLOG config */

/* The urllog is a plain text (UTF-8) file which contains all of the
   URLs passed to the servlet.  This can be used for generating new load tests
   or for general debugging. */

static int urllog_set(lispval var,lispval val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_string filename = FD_CSTRING(val);
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
  else if (FD_FALSEP(val)) {
    u8_lock_mutex(&log_lock);
    if (urllog) {
      fclose(urllog); urllog = NULL;
      u8_free(urllogname); urllogname = NULL;}
    u8_unlock_mutex(&log_lock);
    return 0;}
  else return fd_reterr
         (fd_TypeError,"config_set_urllog",u8_strdup(_("string")),val);
}

static lispval urllog_get(lispval var,void *data)
{
  if (urllog)
    return lispval_string(urllogname);
  else return FD_FALSE;
}

/* REQLOG config */

/* The reqlog is a binary (DTYPE) file containing detailed debugging information.
   It contains the CGI data record processed for each transaction, together with
   any error objects returned and the actual text string sent back to the client.

   The reqloglevel determines which transactions are put in the reqlog.  There
   are currently three levels:
    1 (FD_REQERRS) records only transactions which return errors.
    2 (FD_ALLREQS) records all requests.
    3 (FD_ALLRESP) records all requests and the response set back.
*/

static int reqlog_set(lispval var,lispval val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_string filename = FD_CSTRING(val);
    u8_lock_mutex(&log_lock);
    if ((reqlogname) && (strcmp(filename,reqlogname)==0)) {
      fd_flush_stream(reqlog);
      u8_unlock_mutex(&log_lock);
      return 0;}
    else if (reqlog) {
      fd_close_stream(reqlog,0); reqlog = NULL;
      u8_free(reqlogname); reqlogname = NULL;}
    reqlog = u8_alloc(struct FD_STREAM);
    if (fd_init_file_stream(reqlog,filename,
                            FD_FILE_WRITE,-1,
                            30000)) {
      u8_string logstart=
        u8_mkstring("# Log open %*lt for %s",u8_sessionid());
      lispval logstart_entry = fd_lispstring(logstart);
      fd_endpos(reqlog);
      reqlogname = u8_strdup(filename);
      fd_write_dtype(fd_writebuf(reqlog),logstart_entry);
      fd_decref(logstart_entry);
      u8_unlock_mutex(&log_lock);
      return 1;}
    else {
      u8_unlock_mutex(&log_lock);
      u8_free(reqlog); return -1;}}
  else if (FD_FALSEP(val)) {
    u8_lock_mutex(&log_lock);
    if (reqlog) {
      fd_close_stream(reqlog,0); reqlog = NULL;
      u8_free(reqlogname); reqlogname = NULL;}
    u8_unlock_mutex(&log_lock);
    return 0;}
  else return fd_reterr
         (fd_TypeError,"config_set_urllog",u8_strdup(_("string")),val);
}

static lispval reqlog_get(lispval var,void *data)
{
  if (reqlog)
    return lispval_string(reqlogname);
  else return FD_FALSE;
}

/* Logging primitive */

static void dolog
  (lispval cgidata,lispval val,u8_string response,size_t len,double exectime)
{
  u8_lock_mutex(&log_lock);
  if (trace_cgidata) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,1024);
    fd_pprint(&out,cgidata,NULL,2,0,50);
    fputs(out.u8_outbuf,stderr); fputc('\n',stderr);
    u8_free(out.u8_outbuf);}
  if (FD_NULLP(val)) {
    /* This is pre execution */
    if (urllog) {
      lispval uri = fd_get(cgidata,uri_slotid,FD_VOID);
      u8_string tmp = u8_mkstring(">%s\n@%*lt %g/%g\n",
                                FD_CSTRING(uri),
                                exectime,
                                u8_elapsed_time());
      fputs(tmp,urllog); u8_free(tmp);
      fd_decref(uri);}}
  else if (FD_ABORTP(val)) {
    if (urllog) {
      lispval uri = fd_get(cgidata,uri_slotid,FD_VOID); u8_string tmp;
      if (FD_TROUBLEP(val)) {
        u8_exception ex = u8_erreify();
        if (ex == NULL)
          tmp = u8_mkstring("!%s\n@%*lt %g/%g (mystery error)\n",FD_CSTRING(uri),
                          exectime,u8_elapsed_time());

        else if (ex->u8x_context)
          tmp = u8_mkstring("!%s\n@%*lt %g/%g %s %s\n",FD_CSTRING(uri),
                          exectime,u8_elapsed_time(),
                          ex->u8x_cond,ex->u8x_context);
        else tmp = u8_mkstring("!%s\n@%*lt %g/%g %s\n",FD_CSTRING(uri),
                             exectime,u8_elapsed_time(),ex->u8x_cond);}
      else tmp = u8_mkstring("!%s\n@%*lt %g/%g %q\n",FD_CSTRING(uri),
                           exectime,u8_elapsed_time(),val);
      fputs(tmp,urllog); u8_free(tmp);}
    if (reqlog) {
      fd_store(cgidata,err_symbol,val);
      fd_write_dtype(fd_writebuf(reqlog),cgidata);}}
  else {
    if (urllog) {
      lispval uri = fd_get(cgidata,uri_slotid,FD_VOID);
      u8_string tmp = u8_mkstring("<%s\n@%*lt %ld %g/%g\n",FD_CSTRING(uri),
                                len,exectime,u8_elapsed_time());
      fputs(tmp,urllog); u8_free(tmp);}
    if ((reqlog) && (reqloglevel>2))
      fd_store(cgidata,response_symbol,lispval_string(response));
    if ((reqlog) && (reqloglevel>1))
      fd_write_dtype(fd_writebuf(reqlog),cgidata);}
  u8_unlock_mutex(&log_lock);
}

/* Preloads */

struct FD_PRELOAD_LIST {
  u8_string preload_filename;
  time_t preload_mtime;
  struct FD_PRELOAD_LIST *next_preload;} *preloads = NULL;

static u8_mutex preload_lock;

static lispval preload_get(lispval var,void *ignored)
{
  lispval results = FD_EMPTY_LIST; struct FD_PRELOAD_LIST *scan;
  u8_lock_mutex(&preload_lock);
  scan = preloads; while (scan) {
    results = fd_conspair(lispval_string(scan->preload_filename),results);
    scan = scan->next_preload;}
  u8_unlock_mutex(&preload_lock);
  return results;
}

static int preload_set(lispval var,lispval val,void *ignored)
{
  if (!(FD_STRINGP(val)))
    return fd_reterr
      (fd_TypeError,"preload_config_set",u8_strdup("string"),val);
  else if (FD_STRLEN(val)==0)
    return 0;
  else {
    struct FD_PRELOAD_LIST *scan;
    u8_string filename = FD_CSTRING(val); time_t mtime;
    if (!(u8_file_existsp(filename)))
      return fd_reterr(fd_FileNotFound,"preload_config_set",
                       u8_strdup(filename),FD_VOID);
    u8_lock_mutex(&preload_lock);
    scan = preloads; while (scan) {
      if (strcmp(filename,scan->preload_filename)==0) {
        mtime = u8_file_mtime(filename);
        if (mtime>scan->preload_mtime) break;
        u8_unlock_mutex(&preload_lock);
        return 0;}
      else scan = scan->next_preload;}
    if (server_env == NULL) server_env = fd_working_lexenv();
    scan = u8_alloc(struct FD_PRELOAD_LIST);
    scan->preload_filename = u8_strdup(filename);
    scan->preload_mtime = (time_t)-1;
    scan->next_preload = preloads;
    preloads = scan;
    u8_unlock_mutex(&preload_lock);
    return 1;}
}

double last_preload_update = -1.0;

static int update_preloads()
{
  if ((last_preload_update<0) ||
      ((u8_elapsed_time()-last_preload_update)>1.0)) {
    struct FD_PRELOAD_LIST *scan; int n_reloads = 0;
    u8_lock_mutex(&preload_lock);
    if ((u8_elapsed_time()-last_preload_update)<1.0) {
      u8_unlock_mutex(&preload_lock);
      return 0;}
    scan = preloads; while (scan) {
      time_t mtime = u8_file_mtime(scan->preload_filename);
      if (mtime>scan->preload_mtime) {
        lispval load_result;
        u8_unlock_mutex(&preload_lock);
        load_result = fd_load_source(scan->preload_filename,server_env,"auto");
        if (FD_ABORTP(load_result)) {
          return fd_interr(load_result);}
        n_reloads++;
        fd_decref(load_result);
        u8_lock_mutex(&preload_lock);
        if (mtime>scan->preload_mtime)
          scan->preload_mtime = mtime;}
      scan = scan->next_preload;}
    last_preload_update = u8_elapsed_time();
    u8_unlock_mutex(&preload_lock);
    return n_reloads;}
  else return 0;
}

static lispval get_web_handler(fd_lexenv env,u8_string src)
{
  lispval handler = fd_symeval(webmain_symbol,env);
  if ( (FD_DEFAULTP(handler)) ||
       (FD_VOIDP(handler))    ||
       (FD_UNBOUNDP(handler)) ||
       (FD_ABORTP(handler)) )
    handler = fd_symeval(main_symbol,env);
  if ( (FD_DEFAULTP(handler)) ||
       (FD_VOIDP(handler))    ||
       (FD_UNBOUNDP(handler)) ||
       (FD_ABORTP(handler)) )
    handler = fd_incref(webmain);
  if (!(FD_APPLICABLEP(handler))) {
      u8_log(LOG_CRIT,"ServletMainNotApplicable",
             "From default environment: %q",handler);
      fd_seterr("ServletMainNotApplicable","get_web_handler",
                src,handler);
      return FD_ERROR;}
  else return handler;
}

/* Getting content for pages */

static u8_mutex pagemap_lock;
static FD_HASHTABLE pagemap;

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
  u8_string pathname = FD_CSTRING(path), oldsource;
  double load_start = u8_elapsed_time();
  u8_string content = u8_filestring(pathname,NULL);
  if (traceweb>0)
    u8_log(LOG_NOTICE,"LOADING","Loading %s",pathname);
  if (!(content)) {
    u8_seterr(fd_FileNotFound,"loadcontent",u8_strdup(pathname));
    return FD_ERROR_VALUE;}
  if (content[0]=='<') {
    U8_INPUT in; FD_XML *xml; fd_lexenv env;
    lispval lenv, ldata, parsed;
    U8_INIT_STRING_INPUT(&in,strlen(content),content);
    oldsource = fd_bind_sourcebase(pathname);
    xml = fd_read_fdxml(&in,(FD_SLOPPY_XML|FD_XML_KEEP_RAW));
    fd_restore_sourcebase(oldsource);
    if (xml == NULL) {
      u8_free(content);
      if (u8_current_exception == NULL) {
        u8_seterr("BadFDXML","loadconfig/fdxml",u8_strdup(pathname));}
      u8_log(LOG_CRIT,Startup,"ERROR","Error parsing %s",pathname);
      return FD_ERROR_VALUE;}
    parsed = xml->xml_head;
    while ((FD_PAIRP(parsed)) &&
           (FD_STRINGP(FD_CAR(parsed))) &&
           (whitespace_stringp(FD_CSTRING(FD_CAR(parsed))))) {
      struct FD_PAIR *old_parsed = (struct FD_PAIR *)parsed;
      parsed = FD_CDR(parsed);
      old_parsed->cdr = FD_EMPTY_LIST;}
    ldata = parsed;
    env = (fd_lexenv)xml->xml_data; lenv = (lispval)env;
    if (traceweb>0)
      u8_log(LOG_NOTICE,"LOADED","Loaded %s in %f secs",
                pathname,u8_elapsed_time()-load_start);
    fd_incref(ldata); fd_incref(lenv);
    u8_free(content);
    fd_free_xml_node(xml);
    u8_free(xml);

    return fd_conspair(ldata,lenv);}
  else {
    fd_lexenv newenv=
      ((server_env) ? (fd_make_env(fd_make_hashtable(NULL,17),server_env)) :
       (fd_working_lexenv()));
    lispval load_result = fd_load_source(pathname,newenv,NULL);
    /* We reload the file.  There should really be an API call to
       evaluate a source string (fd_eval_source?).  This could then
       use that. */
    u8_free(content);
    if (FD_TROUBLEP(load_result)) {
      if (u8_current_exception == NULL) {
        u8_seterr("LoadSourceFailed","loadcontent/scheme",
                  u8_strdup(pathname));}
      return load_result;}
    fd_decref(load_result);
    lispval main_proc = get_web_handler(newenv,pathname);
    if (traceweb>0)
      u8_log(LOG_NOTICE,"LOADED","Loaded %s in %f secs, webmain=%q",
             pathname,u8_elapsed_time()-load_start,main_proc);
    return fd_conspair(main_proc,(lispval)newenv);}
}

static lispval update_pagemap(lispval path)
{
  struct stat fileinfo; struct U8_XTIME mtime;
  char *lpath = u8_localpath(FD_CSTRING(path));
  int retval = stat(lpath,&fileinfo);
  if (retval<0) {
    u8_log(LOG_CRIT,"StatFailed","Stat on %s failed (errno=%d)",
           lpath,errno);
    u8_graberrno("getcontent",lpath);
    return FD_ERROR_VALUE;}
  u8_init_xtime(&mtime,fileinfo.st_mtime,u8_second,0,0,0);
  lispval content = loadcontent(path);
  if (FD_ABORTP(content)) {
    u8_free(lpath);
    return content;}
  lispval pagemap_value =
    fd_conspair(fd_make_timestamp(&mtime),
                fd_incref(content));
  fd_hashtable_store(&pagemap,path,pagemap_value);
  fd_decref(pagemap_value);
  u8_free(lpath);
  return content;
}

static lispval getcontent(lispval path)
{
  if ( (FD_STRINGP(path)) &&
       (u8_file_existsp(FD_CSTRING(path)))) {
    struct stat fileinfo; struct U8_XTIME mtime;
    char *lpath = u8_localpath(FD_CSTRING(path));
    int retval = stat(lpath,&fileinfo);
    if (retval<0) {
      u8_log(LOG_CRIT,"StatFailed","Stat on %s failed (errno=%d)",
             lpath,errno);
      u8_graberrno("getcontent",lpath);
      return FD_ERROR_VALUE;}
    else u8_init_xtime(&mtime,fileinfo.st_mtime,u8_second,0,0,0);
    lispval pagemap_value = fd_hashtable_get(&pagemap,path,FD_VOID);
    if (FD_VOIDP(pagemap_value)) {
      u8_lock_mutex(&pagemap_lock);
      pagemap_value = fd_hashtable_get(&pagemap,path,FD_VOID);
      if (FD_VOIDP(pagemap_value)) {
        lispval content = update_pagemap(path);
        u8_unlock_mutex(&pagemap_lock);
        return content;}
      else {
        lispval content=FD_CDR(pagemap_value);
        fd_incref(content);
        fd_decref(pagemap_value);
        u8_unlock_mutex(&pagemap_lock);
        return content;}}
    lispval tval = FD_CAR(pagemap_value), cval = FD_CDR(pagemap_value);
    struct FD_TIMESTAMP *lmtime=
      fd_consptr(fd_timestamp,tval,fd_timestamp_type);
    if ( (fileinfo.st_mtime) <= (lmtime->u8xtimeval.u8_tick) )
      /* Loaded version up to date */
      return fd_incref(cval);
    u8_lock_mutex(&pagemap_lock);
    fd_decref(pagemap_value);
    pagemap_value = fd_hashtable_get(&pagemap,path,FD_VOID);
    if (FD_VOIDP(pagemap_value)) {
      /* This *should* never happen, but we check anyway */
      lispval content = update_pagemap(path);
      u8_unlock_mutex(&pagemap_lock);
      return content;}
    tval = FD_CAR(pagemap_value);
    cval = FD_CDR(pagemap_value);
    lmtime = fd_consptr(fd_timestamp,tval,fd_timestamp_type);
    if ( (fileinfo.st_mtime) <= (lmtime->u8xtimeval.u8_tick) ) {
      /* Loaded version made up to date before while we got the lock  */
      fd_incref(cval);
      u8_unlock_mutex(&pagemap_lock);
      u8_free(lpath);
      return cval;}
    else {
      lispval content=update_pagemap(path);
      u8_unlock_mutex(&pagemap_lock);
      u8_free(lpath);
      return content;}}
  else if (FD_STRINGP(path)) {
    lispval handler = (server_env) ? (get_web_handler(server_env,NULL)) :
      (FD_VOID);
    if (FD_ABORTP(handler)) 
      return handler;
    else if (FD_APPLICABLEP(handler))
      return handler;
    else {
      u8_log(LOG_CRIT,"FileNotFound","Content file %s",FD_CSTRING(path));
      return fd_err(fd_FileNotFound,"getcontent",NULL,path);}}
  else {
    u8_log(LOG_CRIT,"BadPathArg","To getcontent");
    return fd_type_error("pathname","getcontent",path);}
}

/* Check threadcache */

static MU struct FD_THREAD_CACHE *checkthreadcache(fd_lexenv env)
{
  lispval tcval = fd_symeval(threadcache_symbol,env);
  if (FD_FALSEP(tcval)) return NULL;
  else if ((FD_VOIDP(tcval))&&(!(use_threadcache)))
    return NULL;
  else return fd_use_threadcache();
}

/* Init configs */

static void init_webcommon_data()
{
  FD_INIT_STATIC_CONS(&pagemap,fd_hashtable_type);
  fd_make_hashtable(&pagemap,0);
  u8_init_mutex(&pagemap_lock);
}

static void init_webcommon_configs()
{
  fd_register_config("TRACEWEB",_("Trace all web transactions"),
                     fd_boolconfig_get,fd_boolconfig_set,&traceweb);
  fd_register_config("WEBDEBUG",_("Show backtraces on errors"),
                     fd_boolconfig_get,fd_boolconfig_set,&webdebug);
  fd_register_config("WEBALLOWDEBUG",_("Allow requests to specify debugging"),
                     fd_boolconfig_get,fd_boolconfig_set,&weballowdebug);
  fd_register_config("LOGSTACK",_("Log error stacktraces"),
                     fd_boolconfig_get,fd_boolconfig_set,&logstack);
  fd_register_config("ERRORPAGE",_("Default error page for web errors"),
                     fd_lconfig_get,fd_lconfig_set,&default_errorpage);
  fd_register_config
    ("CRISISPAGE",
     _("Default crisis page (for when the error page yields an error)"),
     fd_lconfig_get,fd_lconfig_set,&default_crisispage);
  fd_register_config
    ("NOTFOUNDPAGE",
     _("Default not found page (for when specified content isn't found)"),
     fd_lconfig_get,fd_lconfig_set,&default_notfoundpage);
  fd_register_config
    ("NOCONTENTPAGE",
     _("Default no content page (for when content fetch failed generically)"),
     fd_lconfig_get,fd_lconfig_set,&default_nocontentpage);

  fd_register_config("PRELOAD",
                     _("Files to preload into the shared environment"),
                     preload_get,preload_set,NULL);
  fd_register_config("URLLOG",_("Where to write URLs where were requested"),
                     urllog_get,urllog_set,NULL);
  fd_register_config("REQLOG",_("Where to write request objects"),
                     reqlog_get,reqlog_set,NULL);
  fd_register_config("REQLOGLEVEL",_("Level of transaction logging"),
                     fd_intconfig_get,fd_intconfig_set,&reqloglevel);
  fd_register_config("THREADCACHE",_("Use per-request thread cache"),
                     fd_boolconfig_get,fd_boolconfig_set,&use_threadcache);
  fd_register_config("TRACECGI",_("Whether to log all cgidata"),
                     fd_boolconfig_get,fd_boolconfig_set,&trace_cgidata);
  fd_register_config("DOCROOT",
                     _("File base (directory) for resolving requests"),
                     fd_sconfig_get,fd_sconfig_set,&docroot);
  fd_register_config("PREFLIGHT",
                     _("Preflight (before request handling) procedures"),
                     preflight_get,preflight_set,NULL);
  fd_register_config("POSTFLIGHT",
                     _("POSTFLIGHT (before request handling) procedures"),
                     postflight_get,postflight_set,NULL);

  fd_register_config("WEBMAIN",
                     _("WEBMAIN default procedure for handling requests"),
                     fd_lconfig_get,fd_lconfig_set,&webmain);

  fd_autoload_config("WEBMOD","WEBLOAD","WEBINITS");
}

static void shutdown_server(void);

static void webcommon_shutdown(u8_condition why)
{
  u8_string exit_filename = fd_runbase_filename(".exit");
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
    fd_recycle_hashtable(&pagemap);
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
    lispval incoming_docroot = fd_get(cgidata,document_root,FD_VOID);
    if (FD_STRINGP(incoming_docroot)) {
      lispval scriptname = fd_get(cgidata,script_filename,FD_VOID);
      lispval lisp_docroot = lispval_string(docroot);
      fd_store(cgidata,document_root,lisp_docroot);
      if ((FD_STRINGP(scriptname))&&
          ((strncmp(FD_CSTRING(scriptname),FD_CSTRING(incoming_docroot),
                    FD_STRLEN(incoming_docroot)))==0)) {
        u8_string local_scriptname = u8_string_append
          (docroot,FD_CSTRING(scriptname)+FD_STRLEN(incoming_docroot),NULL);
        lispval new_scriptname = fd_init_string(NULL,-1,local_scriptname);
        fd_store(cgidata,script_filename,new_scriptname);
        fd_decref(new_scriptname);}
      fd_decref(lisp_docroot); fd_decref(scriptname);}
    fd_decref(incoming_docroot);
    return cgidata;}
  else return cgidata;
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
