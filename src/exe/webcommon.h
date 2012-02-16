FD_EXPORT void fd_init_fdweb(void);
FD_EXPORT void fd_init_texttools(void);
FD_EXPORT void fd_init_tagger(void);

/* Logging declarations */
static u8_mutex log_lock;
static u8_string urllogname=NULL;
static FILE *urllog=NULL;
static u8_string reqlogname=NULL;
static fd_dtype_stream reqlog=NULL;
static int reqloglevel=0;
static int traceweb=0;

static int cgitrace=0;

static int use_threadcache=0;

/* When the server started, used by UPTIME */
static struct U8_XTIME boot_time;

#define FD_REQERRS 1 /* records only transactions which return errors */
#define FD_ALLREQS 2 /* records all requests */
#define FD_ALLRESP 3 /* records all requests and the response set back */

static fd_lispenv server_env=NULL;

static fdtype cgisymbol, main_symbol, setup_symbol, script_filename, uri_symbol;
static fdtype response_symbol, err_symbol;
static fdtype browseinfo_symbol, threadcache_symbol;
static fdtype http_headers, html_headers, doctype_slotid, xmlpi_slotid;
static fdtype content_slotid, content_type, tracep_slotid, query_string;
static fdtype query_string, script_name, path_info, remote_info;
static fdtype http_referer, http_accept;
static fdtype http_accept_language, http_accept_encoding, http_accept_charset;
static fdtype content_type, content_length, post_data;
static fdtype server_port, server_name, path_translated, script_filename;
static fdtype auth_type, remote_host, remote_user, remote_port;
static fdtype http_cookie, request_method, retfile_slotid, cleanup_slotid;
static fdtype query_symbol, referer_symbol, forcelog_symbol;

static void init_symbols()
{
  uri_symbol=fd_intern("REQUEST_URI");
  query_symbol=fd_intern("QUERY_STRING");
  main_symbol=fd_intern("MAIN");
  setup_symbol=fd_intern("SETUP");
  cgisymbol=fd_intern("CGIDATA");
  script_filename=fd_intern("SCRIPT_FILENAME");
  doctype_slotid=fd_intern("DOCTYPE");
  xmlpi_slotid=fd_intern("XMLPI");
  content_type=fd_intern("CONTENT-TYPE");
  content_slotid=fd_intern("CONTENT");
  retfile_slotid=fd_intern("RETFILE");
  cleanup_slotid=fd_intern("CLEANUP");
  html_headers=fd_intern("HTML-HEADERS");
  http_headers=fd_intern("HTTP-HEADERS");
  tracep_slotid=fd_intern("TRACEP");
  err_symbol=fd_intern("%ERR");
  response_symbol=fd_intern("%RESPONSE");
  browseinfo_symbol=fd_intern("BROWSEINFO");
  threadcache_symbol=fd_intern("%THREADCACHE");
  referer_symbol=fd_intern("HTTP_REFERER");
  remote_info=fd_intern("REMOTE_INFO");
  forcelog_symbol=fd_intern("FORCELOG");
}

/* Utility functions */

/* Miscellaneous Server functions */

static fdtype get_boot_time()
{
  return fd_make_timestamp(&boot_time);
}

static fdtype get_uptime()
{
  struct U8_XTIME now; u8_now(&now);
  return fd_init_double(NULL,u8_xtime_diff(&now,&boot_time));
}

/* URLLOG config */

/* The urllog is a plain text (UTF-8) file which contains all of the
   URLs passed to the servlet.  This can be used for generating new load tests
   or for general debugging. */

static int urllog_set(fdtype var,fdtype val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_string filename=FD_STRDATA(val);
    fd_lock_mutex(&log_lock);
    if (urllog) {
      fclose(urllog); urllog=NULL;
      u8_free(urllogname); urllogname=NULL;}
    urllog=u8_fopen_locked(filename,"a");
    if (urllog) {
      u8_string tmp;
      urllogname=u8_strdup(filename);
      tmp=u8_mkstring("# Log open %*lt for %s\n",u8_sessionid());
      fputs(tmp,urllog);
      fd_unlock_mutex(&log_lock);
      u8_free(tmp);
      return 1;}
    else return -1;}
  else if (FD_FALSEP(val)) {
    fd_lock_mutex(&log_lock);
    if (urllog) {
      fclose(urllog); urllog=NULL;
      u8_free(urllogname); urllogname=NULL;}
    fd_unlock_mutex(&log_lock);
    return 0;}
  else return fd_reterr
	 (fd_TypeError,"config_set_urllog",u8_strdup(_("string")),val);
}

static fdtype urllog_get(fdtype var,void *data)
{
  if (urllog)
    return fdtype_string(urllogname);
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

static int reqlog_set(fdtype var,fdtype val,void *data)
{
  if (FD_STRINGP(val)) {
    u8_string filename=FD_STRDATA(val);
    fd_lock_mutex(&log_lock);
    if ((reqlogname) && (strcmp(filename,reqlogname)==0)) {
      fd_dtsflush(reqlog);
      fd_unlock_mutex(&log_lock);
      return 0;}
    else if (reqlog) {
      fd_dtsclose(reqlog,1); reqlog=NULL;
      u8_free(reqlogname); reqlogname=NULL;}
    reqlog=u8_alloc(struct FD_DTYPE_STREAM);
    if (fd_init_dtype_file_stream(reqlog,filename,FD_DTSTREAM_WRITE,16384)) {
      u8_string logstart=
	u8_mkstring("# Log open %*lt for %s",u8_sessionid());
      fdtype logstart_entry=fd_lispstring(logstart);
      fd_endpos(reqlog);
      reqlogname=u8_strdup(filename);
      fd_dtswrite_dtype(reqlog,logstart_entry);
      fd_decref(logstart_entry);
      fd_unlock_mutex(&log_lock);
      return 1;}
    else {
      fd_unlock_mutex(&log_lock);
      u8_free(reqlog); return -1;}}
  else if (FD_FALSEP(val)) {
    fd_lock_mutex(&log_lock);
    if (reqlog) {
      fd_dtsclose(reqlog,1); reqlog=NULL;
      u8_free(reqlogname); reqlogname=NULL;}
    fd_unlock_mutex(&log_lock);
    return 0;}
  else return fd_reterr
	 (fd_TypeError,"config_set_urllog",u8_strdup(_("string")),val);
}

static fdtype reqlog_get(fdtype var,void *data)
{
  if (reqlog)
    return fdtype_string(reqlogname);
  else return FD_FALSE;
}

/* Logging primitive */

static void dolog
  (fdtype cgidata,fdtype val,u8_string response,double exectime)
{
  fd_lock_mutex(&log_lock);
  if (FD_NULLP(val)) {
    /* This is pre execution */
    if (urllog) {
      fdtype uri=fd_get(cgidata,uri_symbol,FD_VOID);
      u8_string tmp=u8_mkstring(">%s\n@%*lt %g/%g\n",
				FD_STRDATA(uri),
				exectime,
				u8_elapsed_time());
      fputs(tmp,urllog); u8_free(tmp);
      fd_decref(uri);}}
  else if (FD_ABORTP(val)) {
    if (urllog) {
      fdtype uri=fd_get(cgidata,uri_symbol,FD_VOID); u8_string tmp;
      if (FD_TROUBLEP(val)) {
	u8_exception ex=u8_erreify();
	if (ex==NULL)
	  tmp=u8_mkstring("!%s\n@%*lt %g/%g (mystery error)\n",FD_STRDATA(uri),
			  exectime,u8_elapsed_time());
	
	else if (ex->u8x_context)
	  tmp=u8_mkstring("!%s\n@%*lt %g/%g %s %s\n",FD_STRDATA(uri),
			  exectime,u8_elapsed_time(),
			  ex->u8x_cond,ex->u8x_context);
	else tmp=u8_mkstring("!%s\n@%*lt %g/%g %s\n",FD_STRDATA(uri),
			     exectime,u8_elapsed_time(),ex->u8x_cond);}
      else tmp=u8_mkstring("!%s\n@%*lt %g/%g %q\n",FD_STRDATA(uri),
			   exectime,u8_elapsed_time(),val);
      fputs(tmp,urllog); u8_free(tmp);}
    if (reqlog) {
      fd_store(cgidata,err_symbol,val);
      fd_dtswrite_dtype(reqlog,cgidata);}}
  else {
    if (urllog) {
      fdtype uri=fd_get(cgidata,uri_symbol,FD_VOID);
      u8_string tmp=u8_mkstring("<%s\n@%*lt %g/%g\n",FD_STRDATA(uri),
				exectime,u8_elapsed_time());
      fputs(tmp,urllog); u8_free(tmp);}
    if ((reqlog) && (reqloglevel>2)) 
      fd_store(cgidata,response_symbol,fdtype_string(response));
    if ((reqlog) && (reqloglevel>1))
      fd_dtswrite_dtype(reqlog,cgidata);}
  fd_unlock_mutex(&log_lock);
}

/* Preloads */

struct FD_PRELOAD_LIST {
  u8_string filename; time_t mtime;
  struct FD_PRELOAD_LIST *next;} *preloads=NULL;

#if FD_THREADS_ENABLED
static u8_mutex preload_lock;
#endif

static fdtype preload_get(fdtype var,void *ignored)
{
  fdtype results=FD_EMPTY_LIST; struct FD_PRELOAD_LIST *scan;
  fd_lock_mutex(&preload_lock);
  scan=preloads; while (scan) {
    results=fd_init_pair(NULL,fdtype_string(scan->filename),results);
    scan=scan->next;}
  fd_unlock_mutex(&preload_lock);
  return results;
}

static int preload_set(fdtype var,fdtype val,void *ignored)
{
  if (!(FD_STRINGP(val)))
    return fd_reterr
      (fd_TypeError,"preload_config_set",u8_strdup("string"),val);
  else {
    struct FD_PRELOAD_LIST *scan;
    u8_string filename=FD_STRDATA(val); time_t mtime;
    fd_lock_mutex(&preload_lock);
    scan=preloads; while (scan) {
      if (strcmp(filename,scan->filename)==0) {
	mtime=u8_file_mtime(filename);
	if (mtime>scan->mtime) {
	  fd_load_source(filename,server_env,"auto");
	  scan->mtime=mtime;}
	fd_unlock_mutex(&preload_lock);
	return 0;}
      else scan=scan->next;}
    if (server_env==NULL) server_env=fd_working_environment();
    scan=u8_alloc(struct FD_PRELOAD_LIST);
    scan->filename=u8_strdup(filename);
    scan->mtime=(time_t)-1;
    scan->next=preloads;
    preloads=scan;
    fd_unlock_mutex(&preload_lock);
    return 1;}
}

double last_preload_update=-1.0;

static int update_preloads()
{
  if ((last_preload_update<0) ||
      ((u8_elapsed_time()-last_preload_update)>1.0)) {
    struct FD_PRELOAD_LIST *scan; int n_reloads=0;
    fd_lock_mutex(&preload_lock);
    if ((u8_elapsed_time()-last_preload_update)<1.0) {
      fd_unlock_mutex(&preload_lock);
      return 0;}
    scan=preloads; while (scan) {
      time_t mtime=u8_file_mtime(scan->filename);
      if (mtime>scan->mtime) {
	fdtype load_result=fd_load_source(scan->filename,server_env,"auto");
	if (FD_ABORTP(load_result)) {
	  fd_unlock_mutex(&preload_lock);
	  return fd_interr(load_result);}
	n_reloads++; fd_decref(load_result);
	scan->mtime=mtime;}
      scan=scan->next;}
    last_preload_update=u8_elapsed_time();
    fd_unlock_mutex(&preload_lock);
    return n_reloads;}
  else return 0;
}


/* Getting content for pages */

static FD_HASHTABLE pagemap;

static int whitespace_stringp(u8_string s)
{
  int c=u8_sgetc(&s);
  while (c>0)
    if (u8_isspace(c)) c=u8_sgetc(&s);
    else return 0;
  return 1;
}

static fdtype loadcontent(fdtype path)
{
  u8_string pathname=FD_STRDATA(path), oldsource;
  double load_start=u8_elapsed_time();
  u8_string content=u8_filestring(pathname,NULL);
  if (traceweb>0)
    u8_log(LOG_NOTICE,"LOADING","Loading %s",pathname);
  if (content[0]=='<') {
    U8_INPUT in; FD_XML *xml; fd_lispenv env;
    fdtype lenv, ldata, parsed;
    U8_INIT_STRING_INPUT(&in,strlen(content),content);
    oldsource=fd_bind_sourcebase(pathname);
    xml=fd_read_fdxml(&in,(FD_SLOPPY_XML|FD_XML_KEEP_RAW));
    fd_restore_sourcebase(oldsource);
    if (xml==NULL) {
      u8_free(content);
      u8_log(LOG_WARN,Startup,"ERROR","Error parsing %s",pathname);
      return FD_ERROR_VALUE;}
    parsed=xml->head;
    while ((FD_PAIRP(parsed)) &&
	   (FD_STRINGP(FD_CAR(parsed))) &&
	   (whitespace_stringp(FD_STRDATA(FD_CAR(parsed))))) {
      struct FD_PAIR *old_parsed=(struct FD_PAIR *)parsed;
      parsed=FD_CDR(parsed);
      old_parsed->cdr=FD_EMPTY_LIST;}
    env=(fd_lispenv)xml->data;
    lenv=(fdtype)env; ldata=parsed;
    if (traceweb>0)
      u8_log(LOG_NOTICE,"LOADED","Loaded %s in %f secs",
		pathname,u8_elapsed_time()-load_start);
    u8_free(content); u8_free(xml);
    return fd_init_pair(NULL,ldata,lenv);}
  else {
    fd_environment newenv=
      ((server_env) ? (fd_make_env(fd_make_hashtable(NULL,17),server_env)) :
       (fd_working_environment()));
    fdtype main_proc, load_result;
    /* We reload the file.  There should really be an API call to
       evaluate a source string (fd_eval_source?).  This could then
       use that. */
    u8_free(content);
    load_result=fd_load_source(pathname,newenv,NULL);
    if (FD_TROUBLEP(load_result)) return load_result;
    fd_decref(load_result);
    main_proc=fd_eval(main_symbol,newenv);
    if (traceweb>0)
      u8_log(LOG_NOTICE,"LOADED","Loaded %s in %f secs",
		pathname,u8_elapsed_time()-load_start);
    return fd_init_pair(NULL,main_proc,(fdtype)newenv);}
}

static fdtype getcontent(fdtype path)
{
  if (FD_STRINGP(path)) {
    fdtype value=fd_hashtable_get(&pagemap,path,FD_VOID);
    if (FD_VOIDP(value)) {
      fdtype table_value, content;
      struct stat fileinfo; struct U8_XTIME mtime;
      char *lpath=u8_localpath(FD_STRDATA(path));
      int retval=stat(lpath,&fileinfo);
      if (retval<0) {
	u8_graberr(-1,"getcontent",lpath);
	return FD_ERROR_VALUE;}
      u8_init_xtime(&mtime,fileinfo.st_mtime,u8_second,0,0,0);
      content=loadcontent(path);
      if (FD_ABORTP(content)) {
	u8_free(lpath);
	return content;}
      table_value=fd_init_pair(NULL,fd_make_timestamp(&mtime),
			       fd_incref(content));
      fd_hashtable_store(&pagemap,path,table_value);
      u8_free(lpath);
      fd_decref(table_value);
      return content;}
    else {
      fdtype tval=FD_CAR(value), cval=FD_CDR(value);
      struct FD_TIMESTAMP *lmtime=
	FD_GET_CONS(tval,fd_timestamp_type,FD_TIMESTAMP *);
      struct stat fileinfo;
      char *lpath=u8_localpath(FD_STRDATA(path));
      if (stat(lpath,&fileinfo)<0) {
	u8_graberr(-1,"getcontent",u8_strdup(FD_STRDATA(path)));
	u8_free(lpath); fd_decref(value);
	return FD_ERROR_VALUE;}
      else if (fileinfo.st_mtime>lmtime->xtime.u8_tick) {
	fdtype new_content=loadcontent(path);
	struct U8_XTIME mtime;
	fdtype content_record;
	u8_init_xtime(&mtime,fileinfo.st_mtime,u8_second,0,0,0);
	content_record=
	  fd_init_pair(NULL,fd_make_timestamp(&mtime),
		       fd_incref(new_content));
	fd_hashtable_store(&pagemap,path,content_record);
	u8_free(lpath); fd_decref(content_record);
	if ((FD_PAIRP(value)) && (FD_PAIRP(FD_CDR(value))) &&
	    (FD_PRIM_TYPEP((FD_CDR(FD_CDR(value))),fd_environment_type))) {
	  fd_lispenv env=(fd_lispenv)(FD_CDR(FD_CDR(value)));
	  if (FD_HASHTABLEP(env->bindings))
	    fd_reset_hashtable((fd_hashtable)(env->bindings),0,1);}
	fd_decref(value);
	return new_content;}
      else {
	fdtype retval=fd_incref(cval);
	fd_decref(value);
	u8_free(lpath);
	return retval;}}}
  else {
    return fd_type_error("pathname","getcontent",path);}
}

/* Check threadcache */

static struct FD_THREAD_CACHE *checkthreadcache(fd_lispenv env)
{
  fdtype tcval=fd_symeval(threadcache_symbol,env);
  if (FD_FALSEP(tcval)) return NULL;
  else if ((FD_VOIDP(tcval))&&(!(use_threadcache)))
    return NULL;
  else return fd_use_threadcache();
}

/* Init configs */

static void init_webcommon_config()
{
  fd_register_config("TRACEWEB",_("Trace all web transactions"),
		     fd_boolconfig_get,fd_boolconfig_set,&traceweb);
  fd_register_config("PRELOAD",_("Files to preload into the shared environment"),
		     preload_get,preload_set,NULL);
  fd_register_config("URLLOG",_("Where to write URLs where were requested"),
		     urllog_get,urllog_set,NULL);
  fd_register_config("REQLOG",_("Where to write request objects"),
		     reqlog_get,reqlog_set,NULL);
  fd_register_config("REQLOGLEVEL",_("Level of transaction logging"),
		     fd_intconfig_get,fd_intconfig_set,&reqloglevel);
  fd_register_config("THREADCACHE",_("Use per-request thread cache"),
		     fd_boolconfig_get,fd_boolconfig_set,&use_threadcache);
}
