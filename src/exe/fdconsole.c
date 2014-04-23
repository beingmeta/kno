/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2014 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/fddb.h"
#include "framerd/eval.h"
#include "framerd/history.h"
#include "framerd/ports.h"
#include "framerd/sequences.h"
#include "framerd/fileprims.h"

#if FD_HTMLDUMP_ENABLED
#include "framerd/fdweb.h"
#endif

#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8netfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8stdio.h>

#if ((FD_WITH_EDITLINE) && (HAVE_HISTEDIT_H))
#include <histedit.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include <locale.h>
#include <strings.h>
#include <sys/time.h>
#if HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#include <time.h>
#include <ctype.h>

#include "main.h"

#define EVAL_PROMPT ";; Eval: "

#include "main.c"

int skip_whitespace(u8_input in)
{
  int c=u8_getc(in);
  while ((c>=0) && (isspace(c))) c=u8_getc(in);
  if (c==';') {
    while ((c>=0) && (c != '\n')) c=u8_getc(in);
    if (c<0) return c;
    else return skip_whitespace(in);}
  else return c;
}

static fd_dtype_stream eval_server=NULL;

static u8_input inconsole=NULL;
static u8_output outconsole=NULL;
static u8_output errconsole=NULL;
static fd_lispenv console_env=NULL;
static u8_string bugdumps=NULL;
static u8_string bugurlbase=NULL;
static u8_string bugbrowse=NULL;

static int html_backtrace=FD_HTMLDUMP_ENABLED;
static int lisp_backtrace=(!(FD_HTMLDUMP_ENABLED));
static int dtype_backtrace=0;
static int text_backtrace=0;
static int show_backtrace=0;
static int save_backtrace=1;
static int dotload=1;

static u8_exception last_exception=NULL;

static int debug_maxelts=32, debug_maxchars=80;

static void close_consoles()
{
  if (inconsole) {
    u8_close((u8_stream)inconsole);
    inconsole=NULL;}
  if (outconsole) {
    u8_close((u8_stream)outconsole);
    outconsole=NULL;}
  if (errconsole) {
    u8_close((u8_stream)errconsole);
    errconsole=NULL;}
  if (console_env) {
    fd_recycle_environment(console_env);
    console_env=NULL;}
}

/* Returns 1 if x is worth adding to the history. */
static int historicp(fdtype x)
{
  if ((FD_STRINGP(x)) && (FD_STRING_LENGTH(x)<32)) return 0;
  else if ((FD_SYMBOLP(x)) || (FD_CHARACTERP(x))) return 0;
  else if (FD_FIXNUMP(x)) return 0;
  else return 1;
}

static double showtime_threshold=1.0;

static u8_string stats_message=
  _(";; Done in %f seconds, with %d/%d object/index loads\n");
static u8_string stats_message_w_history=
   _(";; ##%d computed in %f seconds, %d/%d object/index loads\n");
static u8_string stats_message_w_history_and_sym=
   _(";; ##%d (%ls) computed in %f seconds, %d/%d object/index loads\n");

static double startup_time=-1.0;
static double run_start=-1.0;

static int console_width=80, quiet_console=0, show_elts=5;

static int fits_consolep(fdtype elt)
{
  struct U8_OUTPUT tmpout; u8_byte buf[1024];
  U8_INIT_FIXED_OUTPUT(&tmpout,1024,buf);
  fd_unparse(&tmpout,elt);
  if (((tmpout.u8_outptr-tmpout.u8_outbuf)>=1024) ||
      ((tmpout.u8_outptr-tmpout.u8_outbuf)>=console_width))
    return 0;
  else return 1;
}

static void output_element(u8_output out,fdtype elt)
{
  if (historicp(elt))
    if ((console_width==0) || (fits_consolep(elt)))
      u8_printf(out,"\n  %q ;=##%d",elt,fd_histpush(elt));
    else {
      struct U8_OUTPUT tmpout;
      u8_printf(out,"\n  ;; ##%d=\n  ",fd_histpush(elt),elt);
      U8_INIT_OUTPUT(&tmpout,512);
      fd_pprint(&tmpout,elt,"  ",2,2,console_width,1);
      u8_puts(out,tmpout.u8_outbuf);
      u8_free(tmpout.u8_outbuf);
      u8_flush(out);}
  else u8_printf(out,"\n  %q",elt);
}

static int list_length(fdtype scan)
{
  int len=0;
  while (1)
    if (FD_EMPTY_LISTP(scan)) return len;
    else if (FD_PAIRP(scan)) {
      scan=FD_CDR(scan); len++;}
    else return len+1;
}

static int result_size(fdtype result)
{
  if (FD_ATOMICP(result)) return 1;
  else if (FD_CHOICEP(result))
    return FD_CHOICE_SIZE(result);
  else if (FD_VECTORP(result))
    return FD_VECTOR_LENGTH(result);
  else if (FD_PAIRP(result))
    return fd_seq_length(result);
  else return 1;
}

static char *letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ";
static int random_symbol_tries=7;
static fdtype random_symbol()
{
  int tries=0;
  int l1=(random())%26, l2=(random())%26, l3=(random())%26;
  fdtype symbol;
  while (tries<random_symbol_tries) {
    char buf[4]; fdtype sym; 
    int l1=(random())%26, l2=(random())%26, l3=(random())%26;
    buf[0]=letters[l1]; buf[1]=letters[l2]; buf[2]=letters[l3]; buf[3]='\0';
    sym=fd_probe_symbol(buf,3);
    if (FD_VOIDP(sym))
      return fd_intern(buf);
    else tries++;}
  return FD_VOID;
}

static fdtype bind_random_symbol(fdtype result,fd_lispenv env)
{
  fdtype symbol=random_symbol();
  if (!(FD_VOIDP(symbol))) {
    fd_bind_value(symbol,result,env);
    return symbol;}
  else return FD_VOID;
}

static int output_result(u8_output out,fdtype result,
			 int histref,int showall)
{
  if (FD_VOIDP(result)) {}
  else if (((FD_VECTORP(result)) || (FD_PAIRP(result))) &&
	   (result_size(result)<8) && (fits_consolep(result))) {
    if (histref<0)
      u8_printf(out,"%q\n",result);
    else u8_printf(out,"%q  ;; =##%d\n",result,histref);}
  else if ((showall)&&(FD_OIDP(result))) {
    fdtype v=fd_oid_value(result);
    if (FD_TABLEP(v)) {
      U8_OUTPUT out; U8_INIT_OUTPUT(&out,4096);
      u8_printf(&out,"%q:\n",result);
      fd_display_table(&out,v,FD_VOID);
      fputs(out.u8_outbuf,stdout); u8_free(out.u8_outbuf);
      fflush(stdout);}
    else u8_printf(out,"OID value: %q\n",v);
    fd_decref(v);}
  else if (!((FD_CHOICEP(result)) || (FD_VECTORP(result)) ||
	     (FD_PAIRP(result))))
    if (histref<0)
      u8_printf(out,"%Q\n",result);
    else if (console_width<=0)
      u8_printf(out,"%q\n;; =##%d\n",result,histref);
    else {
      struct U8_OUTPUT tmpout;
      U8_INIT_OUTPUT(&tmpout,512);
      fd_pprint(&tmpout,result,"  ",2,2,console_width,1);
      u8_puts(out,tmpout.u8_outbuf); u8_putc(out,'\n');
      u8_free(tmpout.u8_outbuf);
      u8_flush(out);
      return 1;}
  else {
    u8_string start_with=NULL, end_with=NULL;
    int count=0, max_elts, n_elts=0;
    if (FD_CHOICEP(result)) {
      start_with="{"; end_with="}"; n_elts=FD_CHOICE_SIZE(result);}
    else if (FD_VECTORP(result)) {
      start_with="#("; end_with=")"; n_elts=FD_VECTOR_LENGTH(result);}
    else if (FD_PAIRP(result)) {
      start_with="("; end_with=")"; n_elts=list_length(result);}
    else {}
    if ((showall==0) && ((show_elts>0) && (n_elts>(show_elts*2))))
      max_elts=show_elts;
    else max_elts=n_elts;
    if (max_elts<n_elts) {
      if (histref<0)
	u8_printf(out,_("%s ;; (%d/%d items)"),start_with,max_elts,n_elts);
      else u8_printf(out,_("%s ;; ##%d= (%d/%d items)"),
		     start_with,histref,max_elts,n_elts);}
    else if (histref<0)
      u8_printf(out,_("%s ;; (%d items)"),start_with,n_elts);
    else u8_printf(out,_("%s ;; ##%d= (%d items)"),start_with,histref,n_elts);
    if (FD_CHOICEP(result)) {
      FD_DO_CHOICES(elt,result) {
	if ((max_elts>0) && (count<max_elts)) {
	  output_element(out,elt); count++;}
	else {FD_STOP_DO_CHOICES; break;}}}
    else if (FD_VECTORP(result)) {
      fdtype *elts=FD_VECTOR_DATA(result);
      while (count<max_elts) {
	output_element(out,elts[count]); count++;}}
    else if (FD_PAIRP(result)) {
      fdtype scan=result;
      while (count<max_elts) 
	if (FD_PAIRP(scan)) {
	  output_element(out,FD_CAR(scan));
	  count++; scan=FD_CDR(scan);}
	else {
	  u8_printf(out,"\n  . ;; improper list");
	  output_element(out,scan);
	  count++; scan=FD_VOID;
	  break;}}
    else {}
    if (max_elts<n_elts) {
      u8_printf(out,"\n  ;; ....... %d more items .......",n_elts-max_elts);
      if (histref<0)
	u8_printf(out,"\n%s ;; (%d/%d items)\n",end_with,max_elts,n_elts);
      else u8_printf(out,"\n%s ;; ==##%d (%d/%d items)\n",
		     end_with,histref,max_elts,n_elts);}
    else if (histref<0)
      u8_printf(out,"\n%s ;; (%d items)\n",end_with,n_elts);
    else u8_printf(out,"\n%s ;; ==##%d (%d items)\n",end_with,histref,n_elts);}
  return 0;
}

static fdtype top_parse(u8_input in)
{
  fdtype expr=fd_parser(in);
  if ((expr==FD_EOX)||(expr==FD_EOF)) {
    if (in->u8_inptr>in->u8_inbuf)
      return fd_err(fd_ParseError,"top_parse",NULL,expr);
    else return expr;}
  else return expr;
}

static void exit_fdconsole()
{
  if (!(quiet_console)) {
    if (run_start<0)
      u8_message("<%ld> Exiting FramerD (%s) console before we even started!",
		 (long)getpid(),FRAMERD_REVISION);
    else {
      double wall_time=u8_elapsed_time()-run_start;
      u8_string units="seconds";
      if (wall_time<120) {}
      else if (wall_time<7200) {
	units="minutes"; wall_time=wall_time/60;}
      else {
	units="hours"; wall_time=wall_time/3600;}
      u8_message("<%ld> Exiting FramerD (%s) console after %f %s",
		 (long)getpid(),FRAMERD_REVISION,wall_time,units);}}
  close_consoles();
}

static fdtype that_symbol, histref_symbol;

#if HAVE_CHMOD
#define changemode(f,m) chmod(f,m)
#else
#define changemode(f,m) 
#endif

static void dump_backtrace(u8_exception ex,u8_string dumpfile)
{
  u8_string abspath=((dumpfile)?(u8_abspath(dumpfile,NULL)):(fd_tempdir(NULL,0)));
  if (u8_directoryp(abspath)) {
    changemode(abspath,0755);
    if (html_backtrace) {
      u8_string btfile=u8_mkpath(abspath,"backtrace.html");
      dump_backtrace(ex,btfile);
      changemode(btfile,0444);
      u8_free(btfile);}
    if (dtype_backtrace) {
      u8_string btfile=u8_mkpath(abspath,"backtrace.dtype");
      dump_backtrace(ex,btfile);
      changemode(btfile,0444);
      u8_free(btfile);}
    if (lisp_backtrace) {
      u8_string btfile=u8_mkpath(abspath,"backtrace.lispdata");
      dump_backtrace(ex,btfile);
      changemode(btfile,0444);
      u8_free(btfile);}
    if ((text_backtrace)||
	(!((dtype_backtrace)||(lisp_backtrace)||
	   ((html_backtrace)&&(FD_HTMLDUMP_ENABLED))))) {
      u8_string btfile=u8_mkpath(abspath,"backtrace.text");
      dump_backtrace(ex,btfile);
      changemode(btfile,0444);
      u8_free(btfile);}}
  else if ((u8_has_suffix(dumpfile,".scm",1))||
	   (u8_has_suffix(dumpfile,".lisp",1))||
	   (u8_has_suffix(dumpfile,".lispdata",1))) {
    u8_output outfile=(u8_output)u8_open_output_file(abspath,NULL,O_RDWR|O_CREAT,0600);
    fdtype backtrace=fd_exception_backtrace(ex);
    fd_pprint(outfile,backtrace,NULL,0,0,120,1);
    u8_close((u8_stream)outfile);
    changemode(abspath,0444);
    u8_log(LOG_ERROR,ex->u8x_cond,"Backtrace object written to %s",abspath);}
#if FD_HTMLDUMP_ENABLED
  else if ((u8_has_suffix(dumpfile,".html",1))||(u8_has_suffix(dumpfile,".htm",1))) {
    u8_output outfile=(u8_output)u8_open_output_file(abspath,NULL,O_RDWR|O_CREAT,0600);
    u8_string bugurl=NULL;
    fd_xhtmldebugpage(outfile,ex);
    u8_close((u8_stream)outfile);
    changemode(abspath,0444);
    if ((bugdumps)&&(bugurlbase)&&(u8_has_prefix(abspath,bugdumps,0))) 
      bugurl=u8_mkpath(bugurlbase,abspath+strlen(bugdumps));
    else bugurl=u8_string_append("file://",abspath,NULL);
    u8_log(LOG_ERROR,ex->u8x_cond,"HTML backtrace written to %s",bugurl);
    if (bugbrowse) {
      struct U8_OUTPUT cmd; U8_INIT_OUTPUT(&cmd,256); int retval;
      u8_printf(&cmd,"%s %s",bugbrowse,bugurl);
      retval=system(cmd.u8_outbuf);
      if (retval!=0)
	u8_log(LOG_WARN,"There was an error browsing the backtrace with '%s'",
	       cmd.u8_outbuf);
      u8_free(cmd.u8_outbuf);}
    if (bugurl) u8_free(bugurl);}
#else
  else if ((u8_has_suffix(dumpfile,".html",1))||(u8_has_suffix(dumpfile,".htm",1))) {
    u8_log(LOG_WARN,"BACKTRACE","No built-in support for HTML backtraces");}
#endif
  else if (u8_has_suffix(dumpfile,".dtype",1)) {
    struct FD_DTYPE_STREAM *out; int bytes=0;
    fdtype backtrace=fd_exception_backtrace(ex);
    u8_string temp_name=u8_mkstring("%s.part",abspath);
    out=fd_dtsopen(temp_name,FD_DTSTREAM_CREATE);
    if (out==NULL) {
      u8_log(LOG_ERROR,"BACKTRACE","Can't open %s to write %s",
	     temp_name,abspath);
      u8_free(temp_name);}
    else bytes=fd_dtswrite_dtype(out,backtrace);
    if (bytes<0) {
      fd_dtsclose(out,FD_DTSCLOSE_FULL);
      u8_free(temp_name);
      u8_log(LOG_ERROR,"BACKTRACE","Can't open %s to write %s",
	     temp_name,abspath);}
    else {
      fd_dtsclose(out,FD_DTSCLOSE_FULL);
      u8_movefile(temp_name,abspath);
      u8_free(temp_name);
      changemode(abspath,0444);
      u8_log(LOG_ERROR,ex->u8x_cond,"DType backtrace written to %s",abspath);}}
  else {
    u8_output outfile=(u8_output)u8_open_output_file(abspath,NULL,O_RDWR|O_CREAT,0600);
    fdtype backtrace=fd_exception_backtrace(ex);
    fd_pprint(outfile,backtrace,NULL,0,0,120,1);
    u8_close((u8_stream)outfile);
    changemode(abspath,0444);
    u8_log(LOG_ERROR,ex->u8x_cond,"Plaintext backtrace written to %s",abspath);}
  u8_free(abspath);
}

static fdtype backtrace_prim(fdtype arg)
{
  if (!(last_exception)) {
    u8_log(LOG_WARN,"backtrace_prim","No outstanding exceptions!");
    return FD_VOID;}
  if (FD_VOIDP(arg))
    dump_backtrace(last_exception,NULL);
  else if (FD_STRINGP(arg)) 
    dump_backtrace(last_exception,FD_STRDATA(arg));
  else if (FD_TRUEP(arg))
    return fd_exception_backtrace(last_exception);
  else if (FD_FALSEP(arg)) {
    u8_free_exception(last_exception,1); last_exception=NULL;}
  return FD_VOID;
}

/* Module and loading config */

static fdtype module_list=FD_EMPTY_LIST;

static int module_config_set(fdtype var,fdtype vals,void *d)
{
  FD_DO_CHOICES(val,vals) {
    fdtype modname=((FD_SYMBOLP(val))?(val):
		    (FD_STRINGP(val))?
		    (fd_parse(FD_STRDATA(val))):
		    (FD_VOID));
    fdtype module=FD_VOID, used;
    if (FD_VOIDP(modname)) {
      fd_seterr(fd_TypeError,"module_config_set","module",val);
      return -1;}
    else if (!(FD_SYMBOLP(modname))) {
      fd_seterr(fd_TypeError,"module_config_set","module name",val);
      fd_decref(modname);
      return -1;}
    module=fd_find_module(modname,0,0);
    if (FD_VOIDP(module)) {
      fd_seterr(fd_NoSuchModule,"module_config_set",FD_SYMBOL_NAME(modname),val);
      fd_decref(modname);
      return -1;}
    used=fd_use_module(console_env,module);
    if (FD_ABORTP(used)) {
      fd_decref(modname); fd_decref(module); fd_decref(used);
      return -1;}
    else {
      module_list=fd_init_pair(NULL,modname,module_list);
      fd_decref(module); fd_decref(used);
      return 1;}}
}

static fdtype module_config_get(fdtype var,void *d)
{
  return fd_incref(module_list);
}

static fdtype loadfile_list=FD_EMPTY_LIST;

static int loadfile_config_set(fdtype var,fdtype vals,void *d)
{
  FD_DO_CHOICES(val,vals) {
    u8_string loadpath; fdtype loadval;
    if (!(FD_STRINGP(val))) {
      fd_seterr(fd_TypeError,"loadfile_config_set","filename",val);
      return -1;}
    else if (!(strchr(FD_STRDATA(val),':')))
      loadpath=u8_abspath(FD_STRDATA(val),NULL);
    else loadpath=u8_strdup(FD_STRDATA(val));
    loadval=fd_load_source(loadpath,console_env,NULL);
    if (FD_ABORTP(loadval)) {
      fd_seterr(_("load error"),"loadfile_config_set",loadpath,val);
      return -1;}
    else {
      loadfile_list=fd_init_pair(NULL,fdtype_string(loadpath),loadfile_list);
      u8_free(loadpath);
      return 1;}}
}

static fdtype loadfile_config_get(fdtype var,void *d)
{
  return fd_incref(loadfile_list);
}

/* Load dot files into the console */

static void dotloader(u8_string file,fd_lispenv env)
{
  u8_string abspath=u8_abspath(file,NULL);
  if (u8_file_existsp(abspath)) {
    char *kind=((env==NULL)?("CONFIG"):("INIT"));
    double started=u8_elapsed_time(), elapsed; int err=0;
    u8_message("%s(%s)",kind,abspath);
    if (env==NULL) {
      int retval=fd_load_config(abspath);
      elapsed=u8_elapsed_time()-started;
      if (retval<0) err=1;}
    else {
      fdtype val=fd_load_source(abspath,env,NULL);
      elapsed=u8_elapsed_time()-started;
      if (FD_ABORTP(val)) err=1;
      fd_decref(val);}
    if (err) {
      u8_message("Error for %s(%s)",kind,abspath);
      fd_clear_errors(1);}
    else if (elapsed<0.1) {}
    else if (elapsed>1)
      u8_message("%0.3fs for %s(%s)",elapsed,kind,abspath);
    else u8_message("%0.3fms for %s(%s)",(elapsed*1000),kind,abspath);}
  u8_free(abspath);
}

int main(int argc,char **argv)
{
  int i=1, c;
  time_t boot_time=time(NULL);
  fdtype expr=FD_VOID, result=FD_VOID, lastval=FD_VOID;
  u8_encoding enc=u8_get_default_encoding();
  u8_input in=(u8_input)u8_open_xinput(0,enc);
  u8_output out=(u8_output)u8_open_xoutput(1,enc);
  u8_output err=(u8_output)u8_open_xoutput(2,enc);
  u8_string source_file=NULL; /* The file loaded, if any */
  /* This is the environment the console will start in */
  fd_lispenv env=fd_working_environment();

  if ((getenv("UNDERGDB"))||(getenv("FD_DONTDOTLOAD"))) dotload=0;

  /* INITIALIZING MODULES */
  /* Normally, modules have initialization functions called when
     dynamically loaded.  However, if we are statically linked, or we
     don't have the "constructor attributes" use to declare init functions,
     we need to call some initializers explicitly. */

  /* Initialize the libu8 stdio library if it won't happen automatically. */
#if (!(HAVE_CONSTRUCTOR_ATTRIBUTES))
  u8_initialize_u8stdio();
  u8_init_chardata_c();
#endif

  /* Initialize builtin scheme modules.
     These include all modules specified by (e.g.):
     configure --enable-fdweb */
#if ((HAVE_CONSTRUCTOR_ATTRIBUTES) && (!(FD_TESTCONFIG)))
  FD_INIT_SCHEME_BUILTINS();
#else
  /* If we're a "test" executable (FD_TESTCONFIG), we're
     statically linked, so we need to initialize some modules
     explicitly (since the "onload" initializers may not be invoked). */
  fd_init_schemeio();
  fd_init_texttools();
  fd_init_tagger();
  fd_init_fdweb();
#endif

  /* Register configuration parameters */
  fd_register_config("SHOWTIME",_("Threshold for displaying execution time"),
		     fd_dblconfig_get,fd_dblconfig_set,&showtime_threshold);
  fd_register_config
    ("DEBUGMAXCHARS",
     _("Max number of string characters to display in debug message"),
     fd_intconfig_get,fd_intconfig_set,&debug_maxchars);
  fd_register_config
    ("DEBUGMAXELTS",
     _("Max number of sequence/choice elements to display in debug message"),
     fd_intconfig_get,fd_intconfig_set,&debug_maxelts);
  fd_register_config
    ("QUIET",_("Whether to minimize console output"),
     fd_boolconfig_get,fd_boolconfig_set,&quiet_console);
  fd_register_config
    ("CONSOLEWIDTH",
     _("Number of characters available for pretty-printing console output"),
     fd_intconfig_get,fd_intconfig_set,&console_width);
  fd_register_config
    ("SHOWELTS",
     _("Number of elements to initially show in displaying results"),
     fd_intconfig_get,fd_intconfig_set,&show_elts);
  fd_register_config
    ("BUGDUMPS",_("Where to create directories for saving backtraces"),
     fd_sconfig_get,fd_sconfig_set,&bugdumps);
  fd_register_config
    ("BUGURLBASE",_("The base URL for referencing HTML backtraces"),
     fd_sconfig_get,fd_sconfig_set,&bugurlbase);
  fd_register_config
    ("BUGBROWSE",_("The command to use to open saved backtraces"),
     fd_sconfig_get,fd_sconfig_set,&bugbrowse);
  fd_register_config
    ("HTMLDUMPS",_("Whether to dump HTML versions of backtraces"),
     fd_boolconfig_get,fd_boolconfig_set,&html_backtrace);
  fd_register_config
    ("LISPDUMPS",_("Whether to dump LISP object versions of backtraces"),
     fd_boolconfig_get,fd_boolconfig_set,&lisp_backtrace);
  fd_register_config
    ("DTYPEDUMPS",_("Whether to dump binary DTYPE versions of backtraces"),
     fd_boolconfig_get,fd_boolconfig_set,&dtype_backtrace);
  fd_register_config
    ("TEXTDUMPS",_("Whether to plaintext versions of backtraces"),
     fd_boolconfig_get,fd_boolconfig_set,&dtype_backtrace);
  fd_register_config
    ("SAVEBACKTRACE",_("Whether to always save backtraces in the history"),
     fd_boolconfig_get,fd_boolconfig_set,&save_backtrace);
  fd_register_config
    ("SHOWBACKTRACE",_("Whether to always output backtraces to stderr"),
     fd_boolconfig_get,fd_boolconfig_set,&show_backtrace);
  fd_register_config
    ("DOTLOAD",_("Whether load .fdconsole or other dot files"),
     fd_boolconfig_get,fd_boolconfig_set,&dotload);

  /* Initialize console streams */
  inconsole=in;
  outconsole=out;
  errconsole=err;
  console_env=env;
  atexit(exit_fdconsole);

  fd_register_config
    ("MODULES",_("Which modules to load"),
     module_config_get,module_config_set,&module_list);
  fd_register_config
    ("LOADFILE",_("Which files to load"),
     loadfile_config_get,loadfile_config_set,&loadfile_list);


  if (u8_has_suffix(argv[0],"/fdconsole",0))
    u8_default_appid("fdconsole");
  else if (u8_has_suffix(argv[0],"/fdsh",0))
    u8_default_appid("fdsh");
  else if (u8_has_suffix(argv[0],"/fdshell",0))
    u8_default_appid("fdshell");
  else u8_default_appid(argv[0]);
  fd_config_set("OIDDISPLAY",FD_INT2DTYPE(3));
  setlocale(LC_ALL,"");
  that_symbol=fd_intern("THAT");
  histref_symbol=fd_intern("%HISTREF");

  /* Process config fields in the arguments,
     storing the first non config field as a source file. */
  while (i<argc)
    if (strchr(argv[i],'=')) 
      fd_config_assignment(argv[i++]);
    else if (source_file) i++;
    else source_file=argv[i++];

  if (!(quiet_console)) fd_boot_message();

  if (source_file==NULL) {}
  else if (strchr(source_file,'@')) {
    int sock=u8_connect(source_file);
    struct FD_DTYPE_STREAM *newstream;
    if (sock<0) {
      u8_log(LOG_WARN,"Connection failed","Couldn't open connection to %s",
	     source_file);
      exit(-1);}
    newstream=u8_alloc(struct FD_DTYPE_STREAM);
    fd_init_dtype_stream(newstream,sock,65536);
    fd_use_pool(source_file);
    fd_use_index(source_file);
    eval_server=newstream;}
  else {
    fdtype sourceval=fdtype_string(u8_realpath(source_file,NULL));
    fd_config_set("SOURCE",sourceval); fd_decref(sourceval);
    fd_load_source(source_file,env,NULL);}

  /* This is argv[0], the name of the executable by which we
     entered fdconsole. */
  {
    fdtype interpval=fd_lispstring(u8_fromlibc(argv[0]));
    fd_config_set("INTERPRETER",interpval); fd_decref(interpval);}

  fd_idefn((fdtype)env,fd_make_cprim1("BACKTRACE",backtrace_prim,0));
  fd_defalias((fdtype)env,"%","BACKTRACE");

  /* Announce preamble, suppressed by quiet_config */
  fd_config_set("BOOTED",fd_time2timestamp(boot_time));
  run_start=u8_elapsed_time();

  if (!(quiet_console)) {
    double startup_time=u8_elapsed_time()-fd_load_start;
    char *units="s";
    if (startup_time>1) {}
    else if (startup_time>0.001) {
      startup_time=startup_time*1000; units="ms";}
    else {startup_time=startup_time*1000000; units="us";}
    u8_message("FramerD %s booted in %0.3f%s, %d/%d pools/indices",
	       u8_appid(),startup_time,units,fd_n_pools,
	       fd_n_primary_indices+fd_n_secondary_indices);}
  if (dotload) {
    dotloader("~/.fdconfig",NULL);
    dotloader(".fdconfig",NULL);
    dotloader("~/.fdconsole",env);
    dotloader(".fdconsole",env);}
  fd_histinit(0);
  u8_printf(out,EVAL_PROMPT);
  u8_flush(out);
  while ((c=skip_whitespace((u8_input)in))>=0) {
    int start_icache, finish_icache;
    int start_ocache, finish_ocache;
    double start_time, finish_time;
    int histref=-1, stat_line=0, is_histref=0;
    start_ocache=fd_object_cache_load();
    start_icache=fd_index_cache_load();
    if (c == '=') {
      fdtype sym=fd_parser((u8_input)in);
      if (FD_SYMBOLP(sym)) {
	fd_bind_value(sym,lastval,env);
	u8_printf(out,_(";; Assigned %s\n"),FD_SYMBOL_NAME(sym));}
      else u8_printf(out,_(";; Bad assignment expression\n"));
      u8_printf(out,EVAL_PROMPT);
      u8_flush(out);
      fd_decref(sym); continue;}
    else u8_ungetc(((u8_input)in),c);
    expr=top_parse(in);
    if ((FD_EOFP(expr)) || (FD_EOXP(expr))) {
      fd_decref(result); break;}
    /* Clear the buffer (should do more?) */
    if (((FD_PAIRP(expr)) && ((FD_EQ(FD_CAR(expr),histref_symbol)))) ||
	(FD_EQ(expr,that_symbol))) {
      if (!(FD_EQ(expr,that_symbol)))
	is_histref=1;
	histref=FD_FIX2INT(FD_CAR(FD_CDR(expr)));}
    if (FD_OIDP(expr)) {
      fdtype v=fd_oid_value(expr);
      if (FD_TABLEP(v)) {
	U8_OUTPUT out; U8_INIT_OUTPUT(&out,4096);
	u8_printf(&out,"%q:\n",expr);
	fd_display_table(&out,v,FD_VOID);
	fputs(out.u8_outbuf,stdout); u8_free(out.u8_outbuf);
	fflush(stdout);}
      else u8_printf(out,"OID value: %q\n",v);
      fd_decref(v);
      u8_printf(out,EVAL_PROMPT);
      u8_flush(out);
      continue;}
    start_time=u8_elapsed_time();
    if (FD_ABORTP(expr)) {
      result=fd_incref(expr);
      u8_printf(out,";; Flushing input, parse error @%d\n",
		in->u8_inptr-in->u8_inbuf);
      u8_flush_input((u8_input)in);
      u8_flush((u8_output)out);}
    else {
      if (eval_server) {
	fd_dtswrite_dtype(eval_server,expr);
	fd_dtsflush(eval_server);
	result=fd_dtsread_dtype(eval_server);}
      else result=fd_eval(expr,env);}
    if (FD_ACHOICEP(result)) result=fd_simplify_choice(result);
    finish_time=u8_elapsed_time();
    finish_ocache=fd_object_cache_load();
    finish_icache=fd_index_cache_load();
    if ((FD_PAIRP(expr))&&
	(!((FD_CHECK_PTR(result)==0) || (is_histref) ||
	   (FD_VOIDP(result)) || (FD_EMPTY_CHOICEP(result)) ||
	   (FD_TRUEP(result)) || (FD_FALSEP(result)) ||
	   (FD_ABORTP(result)) || (FD_FIXNUMP(result))))) {
      int ref=fd_histpush(result);
      if (ref>=0) histref=ref;}
    if (FD_ABORTP(result)) stat_line=0;
    else if ((showtime_threshold>=0.0) &&
	     (((finish_time-start_time)>showtime_threshold) ||
	      (finish_ocache!=start_ocache) ||
	      (finish_icache!=start_icache)))
      stat_line=1;
    fd_decref(expr); expr=FD_VOID;
    if (FD_CHECK_PTR(result)==0) {
      fprintf(stderr,";;; The expression returned an invalid pointer!!!!\n");}
    else if (FD_TROUBLEP(result)) {
      u8_exception ex=u8_erreify(), root=ex;
      if (ex) {
	{U8_OUTPUT out; U8_INIT_OUTPUT(&out,512);
	  int old_maxelts=fd_unparse_maxelts, old_maxchars=fd_unparse_maxchars;
	  while (root->u8x_prev) root=root->u8x_prev;
	  fd_unparse_maxchars=debug_maxchars; fd_unparse_maxelts=debug_maxelts;
	  fd_print_exception(&out,root);
	  fd_summarize_backtrace(&out,ex);
	  u8_printf(&out,"\n");
	  fputs(out.u8_outbuf,stderr);
	  out.u8_outptr=out.u8_outbuf; out.u8_outbuf[0]='\0';
	  if (show_backtrace) {
	    fd_print_backtrace(&out,ex,80);
	    fputs(out.u8_outbuf,stderr);}
	  fd_unparse_maxelts=old_maxelts;
	  fd_unparse_maxchars=old_maxchars;
	  u8_free(out.u8_outbuf);}
	if (save_backtrace) {
	  fdtype bt=fd_exception_backtrace(ex);
	  u8_fprintf(stderr,";; Saved backtrace into ##%d\n",fd_histpush(bt));
	  fd_decref(bt);}
	if (fd_dump_backtrace) {
	  U8_OUTPUT btout; U8_INIT_OUTPUT(&btout,4096);
	  fd_print_backtrace(&btout,ex,120);
	  fd_dump_backtrace(btout.u8_outbuf);
	  u8_free(btout.u8_outbuf);}
	if (bugdumps) {
	  u8_string dumpdir=u8_tempdir(bugdumps);
	  dump_backtrace(ex,dumpdir);}
	if (last_exception) u8_free_exception(last_exception,1);
	last_exception=ex;}
      else fprintf(stderr,
		   ";;; The expression generated a mysterious error!!!!\n");}
    else if (stat_line)
      output_result(out,result,histref,is_histref);
    else if (FD_VOIDP(result)) {}
    else if (histref<0) 
      stat_line=output_result(out,result,histref,is_histref);
    else {
      output_result(out,result,histref,is_histref);
      stat_line=1;}
    if (stat_line) {
      if (histref<0)
	u8_printf (out,stats_message,
		   (finish_time-start_time),
		   finish_ocache-start_ocache,
		   finish_icache-start_icache);
      else {
	fdtype sym=bind_random_symbol(result,env);
	if (FD_VOIDP(sym))
	  u8_printf(out,stats_message_w_history,
		    histref,(finish_time-start_time),
		    finish_ocache-start_ocache,
		    finish_icache-start_icache);
	else u8_printf(out,stats_message_w_history_and_sym,
		       histref,FD_SYMBOL_NAME(sym),
		       (finish_time-start_time),
		       finish_ocache-start_ocache,
		       finish_icache-start_icache);}}
    fd_clear_errors(1);
    fd_decref(lastval);
    lastval=result; result=FD_VOID;
    if ((FD_CHECK_PTR(lastval)) &&
	(!(FD_ABORTP(lastval))) &&
	(!(FDTYPE_CONSTANTP(lastval))))
      fd_bind_value(that_symbol,lastval,env);
    u8_printf(out,EVAL_PROMPT);
    u8_flush(out);}
  if (eval_server) fd_dtsclose(eval_server,1);
  u8_free(eval_server);
  fd_decref(lastval);
  fd_decref(result);
  /* Hollow out the environment, which should let you reclaim it.
     This patches around the classic issue with circular references in
     a reference counting garbage collector.  If the
     working_environment contains procedures which are closed in the
     working environment, it will not be GC'd because of those
     circular pointers. */
  if (FD_PRIM_TYPEP(env->bindings,fd_hashtable_type))
    fd_reset_hashtable((fd_hashtable)(env->bindings),0,1);
  /* Freed as console_env */
  /* fd_recycle_environment(env); */
  exit(0);
  return 0;
}

