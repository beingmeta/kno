/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/tables.h"
#include "kno/fddb.h"
#include "kno/eval.h"
#include "kno/history.h"
#include "kno/ports.h"

#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8netfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8stdio.h>

#if 1 /* ((KNO_WITH_EDITLINE) && (HAVE_HISTEDIT_H)) */
#include <histedit.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include <locale.h>
#include <strings.h>
#include <sys/time.h>
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

static kno_bytestream eval_server=NULL;

static u8_input inconsole=NULL;
static u8_output outconsole=NULL;
static u8_output errconsole=NULL;
static kno_lispenv console_env=NULL;

static int console_width=80, quiet_console=0, show_elts=5;

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
    kno_recycle_environment(console_env);
    console_env=NULL;}
}

static int fits_consolep(lispval elt)
{
  struct U8_OUTPUT tmpout; u8_byte buf[1024];
  U8_INIT_FIXED_OUTPUT(&tmpout,1024,buf);
  kno_unparse(&tmpout,elt);
  if (((tmpout.u8_write-tmpout.u8_outbuf)>=1024) ||
      ((tmpout.u8_write-tmpout.u8_outbuf)>=console_width))
    return 0;
  else return 1;
}

static void output_element(u8_output out,lispval elt)
{
  if (historicp(elt))
    if ((console_width==0) || (fits_consolep(elt)))
      u8_printf(out,"\n  %q ;=##%d",elt,kno_histpush(elt));
    else {
      struct U8_OUTPUT tmpout;
      u8_printf(out,"\n  ;; ##%d=\n  ",kno_histpush(elt),elt);
      U8_INIT_STATIC_OUTPUT(tmpout,512);
      kno_pprint(&tmpout,elt,"  ",2,2,console_width,1);
      u8_puts(out,tmpout.u8_outbuf);
      u8_free(tmpout.u8_outbuf);
      u8_flush(out);}
  else u8_printf(out,"\n  %q",elt);
}

static int list_length(lispval scan)
{
  int len=0;
  while (1)
    if (NILP(scan)) return len;
    else if (PAIRP(scan)) {
      scan=KNO_CDR(scan); len++;}
    else return len+1;
}

static int result_size(lispval result)
{
  if (ATOMICP(result)) return 1;
  else if (CHOICEP(result))
    return KNO_CHOICE_SIZE(result);
  else if (VECTORP(result))
    return VEC_LEN(result);
  else if (PAIRP(result))
    return kno_seq_length(result);
  else return 1;
}

static int output_result(u8_output out,lispval result,int histref,int showall)
{
  if (VOIDP(result)) {}
  else if (((VECTORP(result)) || (PAIRP(result))) &&
           (result_size(result)<8) && (fits_consolep(result)))
    if (histref<0)
      u8_printf(out,"%q\n",result);
    else u8_printf(out,"%q  ;; =##%d\n",result,histref);
  else if ((showall)&&(OIDP(result))) {
    lispval v=kno_oid_value(result);
    if (TABLEP(v)) {
      U8_OUTPUT out; U8_INIT_STATIC_OUTPUT(out,4096);
      u8_printf(&out,"%q:\n",result);
      kno_display_table(&out,v,VOID);
      fputs(out.u8_outbuf,stdout); u8_free(out.u8_outbuf);
      fflush(stdout);}
    else u8_printf(out,"OID value: %q\n",v);
    kno_decref(v);}
  else if (!((CHOICEP(result)) || (VECTORP(result)) ||
             (PAIRP(result))))
    if (histref<0)
      u8_printf(out,"%Q\n",result);
    else if (console_width<=0)
      u8_printf(out,"%q\n;; =##%d\n",result,histref);
    else {
      struct U8_OUTPUT tmpout;
      U8_INIT_STATIC_OUTPUT(tmpout,512);
      kno_pprint(&tmpout,result,"  ",2,2,console_width,1);
      u8_puts(out,tmpout.u8_outbuf); u8_putc(out,'\n');
      u8_free(tmpout.u8_outbuf);
      u8_flush(out);
      return 1;}
  else {
    u8_string start_with=NULL, end_with=NULL;
    int count=0, max_elts, n_elts=0;
    if (CHOICEP(result)) {
      start_with="{"; end_with="}"; n_elts=KNO_CHOICE_SIZE(result);}
    else if (VECTORP(result)) {
      start_with="#("; end_with=")"; n_elts=VEC_LEN(result);}
    else if (PAIRP(result)) {
      start_with="("; end_with=")"; n_elts=list_length(result);}
    else {}
    if ((showall==0) && ((show_elts>0) && (n_elts>(show_elts*2))))
      max_elts=show_elts;
    else max_elts=n_elts;
    if (max_elts<n_elts)
      u8_printf(out,_("%s ;; ##%d= (%d/%d items)"),
                start_with,histref,max_elts,n_elts);
    else u8_printf(out,_("%s ;; ##%d= (%d items)"),start_with,histref,n_elts);
    if (CHOICEP(result)) {
      DO_CHOICES(elt,result) {
        if ((max_elts>0) && (count<max_elts)) {
          output_element(out,elt); count++;}
        else {KNO_STOP_DO_CHOICES; break;}}}
    else if (VECTORP(result)) {
      lispval *elts=VEC_DATA(result);
      while (count<max_elts) {
        output_element(out,elts[count]); count++;}}
    else if (PAIRP(result)) {
      lispval scan=result;
      while (count<max_elts)
        if (PAIRP(scan)) {
          output_element(out,KNO_CAR(scan));
          count++; scan=KNO_CDR(scan);}
        else {
          u8_printf(out,"\n  . ;; improper list");
          output_element(out,scan);
          count++; scan=VOID;
          break;}}
    else {}
    if (max_elts<n_elts) {
      u8_printf(out,"\n  ;; ....... %d more items .......",n_elts-max_elts);
      u8_printf(out,"\n%s ;; ==##%d (%d/%d items)\n",end_with,histref,max_elts,
                n_elts);}
    else u8_printf(out,"\n%s ;; ==##%d (%d items)\n",end_with,histref,n_elts);}
  return 0;
}

#if 0
static lispval direct_console_read()
{
  int c=skip_whitespace((u8_input)in);
  if (c<0) return KNO_EOF;
  else if (c=='=') {
    lispval sym=kno_parse_expr(in);
    if (SYMBOLP(sym)) {
      kno_bind_value(sym,lastval,env);
      u8_printf(out,_(";; Assigned %s\n"),SYM_NAME(sym));}
    else u8_printf(out,_(";; Bad assignment expression\n"));
    return VOID;}
  else return kno_parse_expr(in);

}
#endif

/* Returns 1 if x is worth adding to the history. */
static int historicp(lispval x)
{
  if ((STRINGP(x)) && (KNO_STRING_LENGTH(x)<32)) return 0;
  else if ((SYMBOLP(x)) || (KNO_CHARACTERP(x))) return 0;
  else if (FIXNUMP(x)) return 0;
  else return 1;
}

static time_t boot_time;

static double showtime_threshold=1.0;

static u8_string stats_message=
  _(";; Done in %f seconds, with %d/%d object/index loads\n");
static u8_string stats_message_w_history=
   _(";; ##%d computed in %f seconds, %d/%d object/index loads\n");

static int el_skip_whitespace(EditLine *e)
{
  char ch='\0'; int retval;
  while (((retval=el_getc(e,&ch))>0) && (isspace(ch)));
  return ch;
}

static lispval el_parser(EditLine *e)
{
  int len; const char *s=el_gets(e,&len);
  if (s==NULL) return KNO_EOF;
  else {
    lispval object=kno_parse((char *)s);
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
  kno_lispenv env=kno_working_environment();
  lispval expr=VOID, result=VOID, lastval=VOID, that_symbol, histref_symbol;
  EditLine *console=el_init("fdshell",stdin,stdout,stderr);
  History *consolehistory=history_init();
  u8_encoding enc=u8_get_default_encoding();
  /* Maybe risky using both of these, but we'll see. */
  u8_input in=(u8_input)u8_open_xinput(0,enc);
  u8_output out=(u8_output)u8_open_xoutput(1,enc);
  u8_output err=(u8_output)u8_open_xoutput(2,enc);
  int i=1, c, n_chars=0;
  /* Mask of args which we handle */
  unsigned char arg_mask[argc];  memset(arg_mask,0,argc);
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

#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (KNO_TESTCONFIG))
  kno_init_schemeio();
  kno_init_texttools();
  kno_init_fdweb();
#else
  KNO_INIT_SCHEME_BUILTINS();
#endif

  /* We keep this outside of the include because it will force
     the module to be linked in. */
  kno_init_fdscheme();

  kno_register_config
    ("SHOWTIME",_("Threshold for displaying execution time"),
     kno_dblconfig_get,kno_dblconfig_set,
     &showtime_threshold);
  kno_register_config
    ("DEBUGMAXCHARS",
     _("Max number of string chars to display in debug message"),
     kno_intconfig_get,kno_intconfig_set,
     &debug_maxchars);
  kno_register_config
    ("DEBUGMAXELTS",
     _("Max number of sequence/choice elements to display in debug message"),
     kno_intconfig_get,kno_intconfig_set,
     &debug_maxelts);
  kno_set_config("BOOTED",kno_time2timestamp(boot_time));
  inconsole=in;
  outconsole=out;
  errconsole=err;
  console_env=env;
  atexit(close_consoles);

  if (u8_has_suffix(argv[0],"/knosh",0))
    u8_default_appid("knosh");
  else if (u8_has_suffix(argv[0],"/fdsh",0))
    u8_default_appid("fdsh");
  else if (u8_has_suffix(argv[0],"/fdshell",0))
    u8_default_appid("fdshell");
  else u8_default_appid(argv[0]);

  kno_set_config("OIDDISPLAY",KNO_INT(3));
  setlocale(LC_ALL,"");
  that_symbol=kno_intern("THAT");
  histref_symbol=kno_intern("%HISTREF");
  while (i<argc) {
    if (isconfig(argv[i])) i++;
    else if (source_file) i++;
    else {
      arg_mask[i] = 'X';
      source_file=argv[i++];}}

  kno_handle_argv(argc,argv,arg_mask,NULL);

  if (!(quiet_console)) kno_boot_message();

  if (source_file==NULL) {}
  else if (strchr(source_file,'@')) {
    int sock=u8_connect(source_file);
    struct KNO_BYTESTREAM *newstream;
    if (sock<0) {
      u8_log(LOG_WARN,"Connection failed","Couldn't open connection to %s",
             source_file);
      exit(-1);}
    newstream=u8_alloc(struct KNO_BYTESTREAM);
    kno_init_bytestream(newstream,sock,65536);
    kno_use_pool(source_file,0);
    kno_use_index(source_file,0);
    eval_server=newstream;}
  else {
    lispval sourceval=fdstring(u8_realpath(source_file,NULL));
    kno_set_config("SOURCE",sourceval); kno_decref(sourceval);
    kno_load_source(source_file,env,NULL);}
  {
    lispval interpval=kno_lispstring(u8_fromlibc(argv[0]));
    kno_set_config("INTERPRETER",interpval); kno_decref(interpval);}
  kno_histinit(0);
  if (!(quiet_console)) {
    double startup_time=u8_elapsed_time()-kno_load_start;
    char *units="s";
    if (startup_time>1) {}
    else if (startup_time>0.001) {
      startup_time=startup_time*1000; units="ms";}
    else {startup_time=startup_time*1000000; units="ms";}
    u8_message("Kno %s booted in %0.3f%s, %d/%d pools/indices",
               u8_appid(),startup_time,units,kno_n_pools,
               kno_n_primary_indices+kno_n_secondary_indices);
    kno_decref(quiet_config);}
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
    start_ocache=kno_object_cache_load();
    start_icache=kno_index_cache_load();
    if (*input == '=') {
      lispval sym=kno_parse((char *)(input+1));
      if (SYMBOLP(sym)) {
        kno_bind_value(sym,lastval,env);
        u8_printf(out,_(";; Assigned %s\n"),SYM_NAME(sym));}
      else u8_printf(out,_(";; Bad assignment expression\n"));
      /* u8_printf(out,EVAL_PROMPT); */
      u8_flush(out);
      kno_decref(sym); continue;}
    /* history(consolehistory,NULL,H_ENTER,input); */
    expr=kno_parse((char *)input);
    if ((KNO_EOFP(expr)) || (KNO_EOXP(expr))) {
      kno_decref(result); break;}
    /* Clear the buffer (should do more?) */
    if (((PAIRP(expr)) && ((KNO_EQ(KNO_CAR(expr),histref_symbol)))) ||
        (KNO_EQ(expr,that_symbol))) {
      is_histref=1;
      histref=FIX2INT(KNO_CAR(KNO_CDR(expr)));}
    if (OIDP(expr)) {
      lispval v=kno_oid_value(expr);
      if (TABLEP(v)) {
        U8_OUTPUT out; U8_INIT_STATIC_OUTPUT(out,4096);
        u8_printf(&out,"%q:\n",expr);
        kno_display_table(&out,v,VOID);
        fputs(out.u8_outbuf,stdout); u8_free(out.u8_outbuf);
        fflush(stdout);}
      else u8_printf(out,"OID value: %q\n",v);
      kno_decref(v);
      /* u8_printf(out,EVAL_PROMPT); */
      u8_flush(out);
      continue;}
    start_time=u8_elapsed_time();
    if (KNO_ABORTP(expr)) {
      result=kno_incref(expr);
      u8_printf(out,";; Flushing input, parse error @%d\n",
                in->u8_read-in->u8_inbuf);
      u8_flush_input((u8_input)in);
      u8_flush((u8_output)out);}
    else {
      if (eval_server) {
        kno_bytestream_write_dtype(eval_server,expr);
        kno_bytestream_flush(eval_server);
        result=kno_bytestream_read_dtype(eval_server);}
      else result=kno_eval(expr,env);}
    if (KNO_ACHOICEP(result)) result=kno_simplify_choice(result);
    finish_time=u8_elapsed_time();
    finish_ocache=kno_object_cache_load();
    finish_icache=kno_index_cache_load();
    if (!((KNO_CHECK_PTR(result)==0) || (is_histref) ||
          (VOIDP(result)) || (EMPTYP(result)) ||
          (KNO_TRUEP(result)) || (FALSEP(result)) ||
          (KNO_ABORTP(result)) || (FIXNUMP(result))))
      histref=kno_histpush(result);
    if (KNO_ABORTP(result)) stat_line=0;
    else if ((showtime_threshold>=0.0) &&
             (((finish_time-start_time)>showtime_threshold) ||
              (finish_ocache!=start_ocache) ||
              (finish_icache!=start_icache)))
      stat_line=1;
    kno_decref(expr); expr=VOID;
    if (KNO_CHECK_PTR(result)==0) {
      fprintf(stderr,";;; The expression returned an invalid pointer!!!!\n");}
    else if (KNO_TROUBLEP(result)) {
      u8_exception ex=u8_erreify(), root=ex;
      int old_maxelts=kno_unparse_maxelts, old_maxchars=kno_unparse_maxchars;
      U8_OUTPUT out; U8_INIT_STATIC_OUTPUT(out,512);
      while (root->u8x_prev) root=root->u8x_prev;
      kno_unparse_maxchars=debug_maxchars; kno_unparse_maxelts=debug_maxelts;
      kno_print_exception(&out,root);
      kno_print_backtrace(&out,ex,80);
      kno_unparse_maxelts=old_maxelts; kno_unparse_maxchars=old_maxchars;
      fputs(out.u8_outbuf,stderr);
      u8_free(out.u8_outbuf);
      u8_free_exception(ex,1);}
    else if (VOIDP(result)) {}
    else if ((CHOICEP(result)) || (KNO_ACHOICEP(result)))
      /* u8_printf(out,"%q\n",result); */
      if ((KNO_CHOICE_SIZE(result)>16) && (is_histref==0)) {
        /* Truncated output for large result sets. */
        int n=KNO_CHOICE_SIZE(result), count=0;
        u8_printf(out,_("{ ;; ##%d= (9/%d results)\n"),histref,n);
        {DO_CHOICES(elt,result) {
          if (count>8) {KNO_STOP_DO_CHOICES; break;}
          else {
            if (historicp(elt))
              u8_printf(out,"  %q ;=##%d\n",elt,kno_histpush(elt));
            else u8_printf(out,"  %q\n",elt);
            count++;}}
        u8_printf(out,"  ;; ...................\n");
        u8_printf(out,_("} ;; =##%d (%d results)\n"),histref,n);}}
      else {
        int n=KNO_CHOICE_SIZE(result);
        u8_printf(out,_("{ ;; ##%d= (%d results)\n"),histref,n);
        kno_prefetch_oids(result);
        {DO_CHOICES(elt,result) {
          if (historicp(elt))
            u8_printf(out,"  %q ;=##%d\n",elt,kno_histpush(elt));
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
    kno_clear_errors(1);
    kno_decref(lastval);
    lastval=result; result=VOID;
    if ((KNO_CHECK_PTR(lastval)) &&
        (!(KNO_ABORTP(lastval))) &&
        (!(FDTYPE_CONSTANTP(lastval))))
      kno_bind_value(that_symbol,lastval,env);
    /* u8_printf(out,EVAL_PROMPT); */
    u8_flush(out);}
  if (eval_server)
    kno_bytestream_close(eval_server,KNO_BYTESTREAM_FREE);
  u8_free(eval_server);
  kno_decref(lastval);
  kno_decref(result);
  /* Hollow out the environment, which should let you reclaim it.
     This patches around the classic issue with circular references in
     a reference counting garbage collector.  If the
     working_environment contains procedures which are closed in the
     working environment, it will not be GC'd because of those
     circular pointers. */
  if (HASHTABLEP(env->env_bindings))
    kno_reset_hashtable((kno_hashtable)(env->env_bindings),0,1);
  /* Freed as console_env */
  /* kno_recycle_environment(env); */
  return 0;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
