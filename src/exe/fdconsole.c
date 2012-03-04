/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2012 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/tables.h"
#include "framerd/fddb.h"
#include "framerd/eval.h"
#include "framerd/history.h"
#include "framerd/ports.h"
#include "framerd/sequences.h"

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

static int output_result(u8_output out,fdtype result,int histref,int showall)
{
  if (FD_VOIDP(result)) {}
  else if (((FD_VECTORP(result)) || (FD_PAIRP(result))) &&
	   (result_size(result)<8) && (fits_consolep(result)))
    if (histref<0)
      u8_printf(out,"%q\n",result);
    else u8_printf(out,"%q  ;; =##%d\n",result,histref);
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
    if (max_elts<n_elts)
      u8_printf(out,_("%s ;; ##%d= (%d/%d items)"),
		start_with,histref,max_elts,n_elts);
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
      u8_printf(out,"\n%s ;; ==##%d (%d/%d items)\n",end_with,histref,max_elts,n_elts);}
    else u8_printf(out,"\n%s ;; ==##%d (%d items)\n",end_with,histref,n_elts);}
  return 0;
}

static fdtype top_parse(u8_input in)
{
  return fd_parser(in);
}

static void exit_fdconsole()
{
  if (!(quiet_console)) {
    if (run_start<0)
      u8_message("Exiting FramerD (%s) console before we even started!",
		 FRAMERD_REV);
    else {
      double wall_time=u8_elapsed_time()-run_start;
      u8_string units="seconds";
      if (wall_time<120) {}
      else if (wall_time<7200) {
	units="minutes"; wall_time=wall_time/60;}
      else {
	units="hours"; wall_time=wall_time/3600;}
      u8_message("Exiting FramerD (%s) console after %f %s",
		 FRAMERD_REV,wall_time,units);}}
  close_consoles();
}

int main(int argc,char **argv)
{
  int i=1, c;
  time_t boot_time=time(NULL);
  fdtype expr=FD_VOID, result=FD_VOID, lastval=FD_VOID;
  fdtype that_symbol, histref_symbol;
  u8_encoding enc=u8_get_default_encoding();
  u8_input in=(u8_input)u8_open_xinput(0,enc);
  u8_output out=(u8_output)u8_open_xoutput(1,enc);
  u8_output err=(u8_output)u8_open_xoutput(2,enc);
  u8_string source_file=NULL; /* The file loaded, if any */
  /* This is the environment the console will start in */
  fd_lispenv env=fd_working_environment();

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
    ("QUIET",
     _("Whether to be minimalist in console interaction"),
     fd_boolconfig_get,fd_boolconfig_set,&quiet_console);
  fd_register_config
    ("CONSOLEWIDTH",
     _("Number of characters available for pretty-printing console output"),
     fd_intconfig_get,fd_intconfig_set,&console_width);
  fd_register_config
    ("SHOWELTS",
     _("Number of elements to initially show in displaying results"),
     fd_intconfig_get,fd_intconfig_set,&show_elts);

  /* Initialize console streams */
  inconsole=in;
  outconsole=out;
  errconsole=err;
  console_env=env;
  atexit(exit_fdconsole);

  /* Other initialization for FramerD libraries */
  u8_identify_application(argv[0]);
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

  /* Announce preamble, suppressed by quiet_config */
  fd_config_set("BOOTED",fd_time2timestamp(boot_time));
  run_start=u8_elapsed_time();
  startup_time=run_start-fd_load_start;
  if (!(quiet_console))  {
    u8_message("FramerD (%s) booted in %f seconds, %d/%d pools/indices",
	       FRAMERD_REV,startup_time,fd_n_pools,
	       fd_n_primary_indices+fd_n_secondary_indices);
    u8_message
      ("beingmeta FramerD, (C) beingmeta 2004-2012, all rights reserved");}

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
      if (ex) {
	int old_maxelts=fd_unparse_maxelts, old_maxchars=fd_unparse_maxchars;
	U8_OUTPUT out; U8_INIT_OUTPUT(&out,512);
	while (root->u8x_prev) root=root->u8x_prev;
	fd_unparse_maxchars=debug_maxchars; fd_unparse_maxelts=debug_maxelts;
	fd_print_exception(&out,root);
	fd_summarize_backtrace(&out,ex);
	u8_printf(&out,"\n");
	if (fd_dump_backtrace==NULL)
	  fd_print_backtrace(&out,ex,80);
	fd_unparse_maxelts=old_maxelts; fd_unparse_maxchars=old_maxchars;
	fputs(out.u8_outbuf,stderr);
	if (fd_dump_backtrace) {
	  out.u8_outptr=out.u8_outbuf;
	  fd_print_backtrace(&out,ex,120);
	  fd_dump_backtrace(out.u8_outbuf);}
	u8_free(out.u8_outbuf);
	u8_free_exception(ex,1);}
      else fprintf(stderr,";;; The expression generated a mysterious error!!!!\n");}
    else if (stat_line)
      output_result(out,result,histref,is_histref);
    else stat_line=output_result(out,result,histref,is_histref);
    if (stat_line) {
      if (histref<0)
	u8_printf (out,stats_message,
		   (finish_time-start_time),
		   finish_ocache-start_ocache,
		   finish_icache-start_icache);
      else u8_printf(out,stats_message_w_history,
		     histref,(finish_time-start_time),
		     finish_ocache-start_ocache,
		     finish_icache-start_icache);}
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
  return 0;
}

