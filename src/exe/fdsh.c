/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2011 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/fddb.h"
#include "fdb/eval.h"
#include "fdb/history.h"
#include "fdb/ports.h"

#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8netfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8stdio.h>

#if 1 /* ((FD_WITH_EDITLINE) && (HAVE_HISTEDIT_H)) */
#include <histedit.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include <locale.h>
#include <strings.h>
#include <sys/time.h>
#include <time.h>
#include <ctype.h>

#include "revision.h"

FD_EXPORT void fd_init_fdweb(void);
FD_EXPORT void fd_init_texttools(void);
FD_EXPORT void fd_init_tagger(void);

#define EVAL_PROMPT ";; Eval: "

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

#if 0
static fdtype direct_console_read()
{
  int c=skip_whitespace((u8_input)in);
  if (c<0) return FD_EOF;
  else if (c=='=') {
    fdtype sym=fd_parse_expr(in);
    if (FD_SYMBOLP(sym)) {
      fd_bind_value(sym,lastval,env);
      u8_printf(out,_(";; Assigned %s\n"),FD_SYMBOL_NAME(sym));}
    else u8_printf(out,_(";; Bad assignment expression\n"));
    return FD_VOID;}
  else return fd_parse_expr(in);

}
#endif

/* Returns 1 if x is worth adding to the history. */
static int historicp(fdtype x)
{
  if ((FD_STRINGP(x)) && (FD_STRING_LENGTH(x)<32)) return 0;
  else if ((FD_SYMBOLP(x)) || (FD_CHARACTERP(x))) return 0;
  else if (FD_FIXNUMP(x)) return 0;
  else return 1;
}

static time_t boot_time;

static double showtime_threshold=1.0;

static u8_string stats_message=
  _(";; Done in %f seconds, with %d/%d object/index loads\n");
static u8_string stats_message_w_history=
   _(";; ##%d computed in %f seconds, %d/%d object/index loads\n");

static double startup_time=-1.0;

static int el_skip_whitespace(EditLine *e)
{
  char ch='\0'; int retval;
  while (((retval=el_getc(e,&ch))>0) && (isspace(ch)));
  return ch;
}

static fdtype el_parser(EditLine *e)
{
  int len; const char *s=el_gets(e,&len);
  if (s==NULL) return FD_EOF;
  else {
    fdtype object=fd_parse((char *)s);
    return object;}
}

static char *promptfn(EditLine *e)
{
  return EVAL_PROMPT;
}

int main(int argc,char **argv)
{
  unsigned char data[1024]; const char *input;
  time_t boot_time=time(NULL);
  fd_lispenv env=fd_working_environment();
  fdtype expr=FD_VOID, result=FD_VOID, lastval=FD_VOID, that_symbol, histref_symbol;
  fdtype quiet_config=FD_VOID;
  EditLine *console=el_init("fdshell",stdin,stdout,stderr);
  History *consolehistory=history_init();
  u8_encoding enc=u8_get_default_encoding();
  /* Maybe risky using both of these, but we'll see. */
  u8_input in=(u8_input)u8_open_xinput(0,enc);
  u8_output out=(u8_output)u8_open_xoutput(1,enc);
  u8_output err=(u8_output)u8_open_xoutput(2,enc);
  int i=1, c, n_chars=0;
  u8_string source_file=NULL;

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

#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (FD_TESTCONFIG))
  fd_init_schemeio();
  fd_init_texttools();
  fd_init_tagger();
  fd_init_fdweb();
#else
  FD_INIT_SCHEME_BUILTINS();
#endif

  /* We keep this outside of the include because it will force
     the module to be linked in. */
  fd_init_fdscheme();

  fd_register_config("SHOWTIME",_("Threshold for displaying execution time"),
		     fd_dblconfig_get,fd_dblconfig_set,
		     &showtime_threshold);
  fd_register_config("DEBUGMAXCHARS",_("Max number of string characters to display in debug message"),
		     fd_intconfig_get,fd_intconfig_set,
		     &debug_maxchars);
  fd_register_config("DEBUGMAXELTS",_("Max number of list/vector/choice elements to display in debug message"),
		     fd_intconfig_get,fd_intconfig_set,
		     &debug_maxelts);
  fd_config_set("BOOTED",fd_time2timestamp(boot_time));
  inconsole=in;
  outconsole=out;
  errconsole=err;
  console_env=env;
  atexit(close_consoles);
  u8_identify_application(argv[0]);
  fd_config_set("OIDDISPLAY",FD_INT2DTYPE(3));
  setlocale(LC_ALL,"");
  that_symbol=fd_intern("THAT");
  histref_symbol=fd_intern("%HISTREF");
  while (i<argc)
    if (strchr(argv[i],'=')) 
      fd_config_assignment(argv[i++]);
    else if (source_file) i++;
    else source_file=argv[i++];
  if (source_file==NULL) {}
  else if (strchr(source_file,'@')) {
    int sock=u8_connect(source_file);
    struct FD_DTYPE_STREAM *newstream;
    if (sock<0) {
      u8_log(LOG_WARN,"Connection failed","Couldn't open connection to %s",source_file);
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
  {
    fdtype interpval=fd_init_string(NULL,-1,u8_fromlibc(argv[0]));
    fd_config_set("INTERPRETER",interpval); fd_decref(interpval);}
  quiet_config=fd_config_get("QUIET");
  fd_histinit(0);
  startup_time=u8_elapsed_time()-fd_load_start;
  if (FD_VOIDP(quiet_config))  {
    u8_message("FramerD (r%s) booted in %f seconds, %d/%d pools/indices",
	       FDB_SVNREV,startup_time,fd_n_pools,
	       fd_n_primary_indices+fd_n_secondary_indices);
    u8_message("beingmeta FramerD, (C) beingmeta 2004-2011, all rights reserved");
    fd_decref(quiet_config);}
  el_set(console,EL_PROMPT,promptfn);
  el_set(console,EL_EDITOR,"emacs");
  el_set(console,EL_HIST,history,consolehistory);
  /* history(consolehistory,NULL,H_SETSIZE,256); */
  /* u8_printf(out,EVAL_PROMPT);*/
  while ((input=el_gets(console,&n_chars))!=NULL) {
    int start_icache, finish_icache;
    int start_ocache, finish_ocache;
    double start_time, finish_time;
    int histref=-1, stat_line=0, is_histref=0;
    start_ocache=fd_object_cache_load();
    start_icache=fd_index_cache_load();
    if (*input == '=') {
      fdtype sym=fd_parse((char *)(input+1));
      if (FD_SYMBOLP(sym)) {
	fd_bind_value(sym,lastval,env);
	u8_printf(out,_(";; Assigned %s\n"),FD_SYMBOL_NAME(sym));}
      else u8_printf(out,_(";; Bad assignment expression\n"));
      /* u8_printf(out,EVAL_PROMPT); */
      u8_flush(out);
      fd_decref(sym); continue;}
    /* history(consolehistory,NULL,H_ENTER,input); */
    expr=fd_parse((char *)input);
    if ((FD_EOFP(expr)) || (FD_EOXP(expr))) {
      fd_decref(result); break;}
    /* Clear the buffer (should do more?) */
    if (((FD_PAIRP(expr)) && ((FD_EQ(FD_CAR(expr),histref_symbol)))) ||
	(FD_EQ(expr,that_symbol))) {
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
      /* u8_printf(out,EVAL_PROMPT); */
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
    if (!((FD_CHECK_PTR(result)==0) || (is_histref) ||
	  (FD_VOIDP(result)) || (FD_EMPTY_CHOICEP(result)) ||
	  (FD_TRUEP(result)) || (FD_FALSEP(result)) ||
	  (FD_ABORTP(result)) || (FD_FIXNUMP(result))))
      histref=fd_histpush(result);
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
      int old_maxelts=fd_unparse_maxelts, old_maxchars=fd_unparse_maxchars;
      U8_OUTPUT out; U8_INIT_OUTPUT(&out,512);
      while (root->u8x_prev) root=root->u8x_prev;
      fd_unparse_maxchars=debug_maxchars; fd_unparse_maxelts=debug_maxelts;
      fd_print_exception(&out,root);
      fd_print_backtrace(&out,ex,80);
      fd_unparse_maxelts=old_maxelts; fd_unparse_maxchars=old_maxchars;
      fputs(out.u8_outbuf,stderr);
      u8_free(out.u8_outbuf);
      u8_free_exception(ex,1);}
    else if (FD_VOIDP(result)) {}
    else if ((FD_CHOICEP(result)) || (FD_ACHOICEP(result)))
      /* u8_printf(out,"%q\n",result); */
      if ((FD_CHOICE_SIZE(result)>16) && (is_histref==0)) {
	/* Truncated output for large result sets. */
	int n=FD_CHOICE_SIZE(result), count=0;
	u8_printf(out,_("{ ;; ##%d= (9/%d results)\n"),histref,n);
	{FD_DO_CHOICES(elt,result) {
	  if (count>8) {FD_STOP_DO_CHOICES; break;}
	  else {
	    if (historicp(elt))
	      u8_printf(out,"  %q ;=##%d\n",elt,fd_histpush(elt));
	    else u8_printf(out,"  %q\n",elt);
	    count++;}}
	u8_printf(out,"  ;; ...................\n");
	u8_printf(out,_("} ;; =##%d (%d results)\n"),histref,n);}}
      else {
	int n=FD_CHOICE_SIZE(result);
	u8_printf(out,_("{ ;; ##%d= (%d results)\n"),histref,n);
	fd_prefetch_oids(result);
	{FD_DO_CHOICES(elt,result) {
	  if (historicp(elt))
	    u8_printf(out,"  %q ;=##%d\n",elt,fd_histpush(elt));
	  else u8_printf(out,"  %q\n",elt);}}
	u8_printf(out,_("} ;; =##%d (%d results)\n"),histref,n);}
    else if (histref<0)
      u8_printf(out,"%q\n",result);
    else u8_printf(out,"%q  ;; =##%d\n",result,histref);
    if  (stat_line)
      if (histref<0)
	u8_printf (out,stats_message,
		   (finish_time-start_time),
		   finish_ocache-start_ocache,
		   finish_icache-start_icache);
      else u8_printf(out,stats_message_w_history,
		     histref,(finish_time-start_time),
		     finish_ocache-start_ocache,
		     finish_icache-start_icache);
    fd_clear_errors(1);
    fd_decref(lastval);
    lastval=result; result=FD_VOID;
    if ((FD_CHECK_PTR(lastval)) &&
	(!(FD_ABORTP(lastval))) &&
	(!(FDTYPE_CONSTANTP(lastval))))
      fd_bind_value(that_symbol,lastval,env);
    /* u8_printf(out,EVAL_PROMPT); */
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
  return 0;
}
