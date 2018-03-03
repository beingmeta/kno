/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/defines.h"
#include "framerd/dtype.h"
#include "framerd/cons.h"
#include "framerd/tables.h"
#include "framerd/storage.h"
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

#if ((FD_WITH_EDITLINE) && (HAVE_HISTEDIT_H) && (HAVE_LIBEDIT))
#include <histedit.h>
#define USING_EDITLINE 1
static EditLine *editconsole;
static History *edithistory;
#else
#define USING_EDITLINE 0
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

#define EVAL_PROMPT "#|fdconsole>|# "
static u8_string eval_prompt = EVAL_PROMPT;
static int set_prompt(lispval ignored,lispval v,void *vptr)
{
  u8_string *ptr = vptr, cur = *ptr;
  if (STRINGP(v)) {
    u8_string data = CSTRING(v), scan = data;
    int c = u8_sgetc(&scan);
    while (u8_isspace(c)) c = u8_sgetc(&scan);
    if (cur) u8_free(cur);
    if ((c==';')||((c=='#')&&(*scan=='|')))
      *ptr = u8_strdup(data);
    else *ptr = u8_string_append("#|",data,"|# ",NULL);
    return 1;}
  else {
    fd_seterr(fd_TypeError,"set_prompt",_("prompt is not a string"),v);
    return -1;}
}

#include "main.c"

static u8_string stop_file=NULL;
static u8_string console_bugdir=NULL;

static int use_editline = 0;

#if USING_EDITLINE
static char *editline_promptfn(EditLine *ignored)
{
  return (char *) eval_prompt;
}
#endif

int skip_whitespace(u8_input in)
{
  int c = u8_getc(in);
  while ((c>=0) && (isspace(c))) c = u8_getc(in);
  if (c==';') {
    while ((c>=0) && (c != '\n')) c = u8_getc(in);
    if (c<0) return c;
    else return skip_whitespace(in);}
  else return c;
}

int swallow_whitespace(u8_input in)
{
  int c = u8_peekc(in);
  if (c<0) return c;
  while ((c>=0) && (isspace(c))) {
    u8_getc(in); c = u8_peekc(in);}
  if (c==';') {
    while ((c>=0) && (c != '\n')) c = u8_getc(in);
    if (c<0) return c;
    else return skip_whitespace(in);}
  else return c;
}

static fd_stream eval_server = NULL;

static u8_input inconsole = NULL;
static u8_output outconsole = NULL;
static u8_output errconsole = NULL;
static fd_lexenv console_env = NULL;
static u8_string bugdumps = NULL;
static u8_string bugurlbase = NULL;
static u8_string bugbrowse = NULL;

static int html_backtrace = FD_HTMLDUMP_ENABLED;
static int lisp_backtrace = (!(FD_HTMLDUMP_ENABLED));
static int dtype_backtrace = 0;
static int text_backtrace = 0;
static int show_backtrace = 1;
static int save_backtrace = 1;
static int dotload = 1;

static u8_exception last_exception = NULL;

static int debug_maxelts = 32, debug_maxchars = 80;

static void close_consoles()
{
  if (inconsole) {
    u8_close((u8_stream)inconsole);
    inconsole = NULL;}
  if (outconsole) {
    u8_close((u8_stream)outconsole);
    outconsole = NULL;}
  if (errconsole) {
    u8_close((u8_stream)errconsole);
    errconsole = NULL;}
  if (console_env) {
    fd_recycle_lexenv(console_env);
    console_env = NULL;}
}

/* History primitives */

static lispval history_symbol, histref_symbol;

static lispval histref_prim(int n,lispval *args)
{
  lispval history = fd_thread_get(history_symbol);
  if (FD_ABORTP(history))
    return history;
  else if (VOIDP(history)) {
    fd_seterr("NoActiveHistory","histref_prim",NULL,FD_VOID);
    return -1;}
  else NO_ELSE;
  lispval root = args[0];
  lispval val = fd_history_ref(history,root);
  if (FD_ABORTP(val))
    return val;
  else if (FD_VOIDP(val)) {
    fd_seterr("No reference","histref_prim",NULL,root);
    fd_decref(history);
    return FD_EMPTY;}
  else fd_decref(history);
  lispval scan = fd_incref(val);
  int i = 1; while ( (i < n) && (!(FD_VOIDP(scan))) ) {
    lispval path = args[i];
    if (FD_FIXNUMP(path)) {
      int rel_off = FD_FIX2INT(path);
      if (FD_CHOICEP(scan)) {
	ssize_t n_choices = FD_CHOICE_SIZE(scan);
	size_t off = (rel_off>=0) ?  (rel_off) : (n_choices + rel_off);
	if ( (off < 0) || (off > n_choices) )
	  return fd_err(fd_RangeError,"histref_prim",NULL,path);
	else {
	  lispval new_scan = FD_CHOICE_ELTS(scan)[off];
	  fd_incref(new_scan); fd_decref(scan);
	  scan=new_scan;}}
      else if (FD_SEQUENCEP(scan)) {
	ssize_t n_elts = fd_seq_length(scan);
	size_t off = (rel_off>=0) ?  (rel_off) : (n_elts + rel_off);
	if ( (off < 0) || (off > n_elts) )
	  return fd_err(fd_RangeError,"histref_prim",NULL,path);
	else {
	  lispval new_scan = fd_seq_elt(scan,off);
	  fd_decref(scan);
	  scan=new_scan;}}
      else scan = FD_VOID;}
    else if (FD_STRINGP(path)) {
      if (FD_TABLEP(scan)) {
	lispval v = fd_get(scan,path,FD_VOID);
	if (FD_VOIDP(v)) {
	  u8_string upper = u8_upcase(FD_CSTRING(scan));
	  lispval sym = fd_probe_symbol(upper,-1);
	  if (FD_SYMBOLP(sym))
	    v = fd_get(scan,sym,FD_VOID);}
	if (FD_VOIDP(v))
	  fd_seterr("NoSuchKey","histref_prim",FD_CSTRING(path),scan);
	scan = v;}
      else scan = FD_VOID;}
    else if (FD_SYMBOLP(path)) {
      if (FD_TABLEP(scan)) {
	lispval v = fd_get(scan,path,FD_VOID);
	if (FD_VOIDP(v))
	  fd_seterr("NoSuchKey","histref_prim",FD_CSTRING(path),scan);
	fd_decref(scan);
	scan = v;}
      else scan = FD_VOID;}
    else scan = FD_VOID;
    i++;}
  if (FD_VOIDP(scan)) {
    fd_seterr("InvalidHistRef","histref_prim",NULL,root);
    fd_decref(root);
    return FD_ERROR;}
  else {
    fd_incref(scan);
    fd_decref(val);
    return scan;}
}

static lispval history_prim()
{
  return fd_thread_get(history_symbol);
}

static double showtime_threshold = 1.0;

static u8_string stats_message=
  _(";; Done in %f seconds, with %d/%d object/index loads\n");
static u8_string stats_message_w_history=
   _(";; %s computed in %f seconds, %d/%d object/index loads\n");
static u8_string stats_message_w_history_and_sym=
   _(";; %s (%ls) computed in %f seconds, %d/%d object/index loads\n");

static double run_start = -1.0;

static int console_width = 80, quiet_console = 0, show_elts = 5;
static double time_startup = 1;


static void output_element(u8_output out,lispval elt,u8_string histref,int path)
{
  if (OIDP(elt)) {
    /* Fetch OID values which you display */
    lispval val = fd_oid_value(elt);
    if (FD_ABORTP(val)) fd_clear_errors(0);
    fd_decref(val);}

  if ( (histref >= 0) && (path >= 0) ) {
    U8_STATIC_OUTPUT(tmp,1000);
    fd_unparse(tmpout,elt);
    if ((tmp.u8_write-tmp.u8_outbuf)<console_width) {
      u8_printf(out,"\n  %s ;=%s.%d",tmp.u8_outbuf,histref,path);
      u8_close_output(tmpout);
      return;}
    u8_printf(out,"\n  ;; %s.%d=\n  ",histref,path);
    tmp.u8_write=tmp.u8_outbuf; tmp.u8_outbuf[0]='\0';
    fd_pprint(tmpout,elt,NULL,3,3,console_width);
    u8_puts(out,tmp.u8_outbuf);
    u8_close_output(tmpout);
    u8_flush(out);}
  else u8_printf(out,"\n  %Q",elt);
}

static int list_length(lispval scan)
{
  int len = 0;
  while (1)
    if (NILP(scan)) return len;
    else if (FD_PAIRP(scan)) {
      scan = FD_CDR(scan); len++;}
    else return len+1;
}

static char *letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ";
static int random_symbol_tries = 7;
static lispval random_symbol()
{
  int tries = 0;
  while (tries<random_symbol_tries) {
    char buf[4]; lispval sym;
    int l1 = (random())%26, l2 = (random())%26, l3 = (random())%26;
    buf[0]=letters[l1]; buf[1]=letters[l2]; buf[2]=letters[l3]; buf[3]='\0';
    sym = fd_probe_symbol(buf,3);
    if (VOIDP(sym))
      return fd_intern(buf);
    else tries++;}
  return VOID;
}

static lispval bind_random_symbol(lispval result,fd_lexenv env)
{
  lispval symbol = random_symbol();
  if (!(VOIDP(symbol))) {
    fd_bind_value(symbol,result,env);
    return symbol;}
  else return VOID;
}

static int output_result(u8_output out,lispval result,
                         u8_string histref,int showall)
{
  if (OIDP(result)) {
    lispval v = fd_oid_value(result);
    fd_decref(v);}
  if (VOIDP(result))
    return 0;
  else if ((showall)&&(OIDP(result))) {
    lispval v = fd_oid_value(result);
    if (FD_TABLEP(v)) {
      U8_OUTPUT out; U8_INIT_STATIC_OUTPUT(out,4096);
      u8_printf(&out,"%q:\n",result);
      fd_display_table(&out,v,VOID);
      fputs(out.u8_outbuf,stdout); u8_free(out.u8_outbuf);
      fflush(stdout);}
    else u8_printf(out,"OID value: %q\n",v);
    fd_decref(v);
    return 1;}
  else if ((FD_CHOICEP(result)) || (FD_VECTORP(result)) ||
           (PAIRP(result)))  {
    u8_string start_with = NULL, end_with = NULL;
    int count = 0, max_elts, n_elts = 0;

    if (CHOICEP(result)) {
      start_with="{"; end_with="}"; n_elts = FD_CHOICE_SIZE(result);}
    else if (VECTORP(result)) {
      start_with="#("; end_with=")"; n_elts = VEC_LEN(result);}
    else if (PAIRP(result)) {
      start_with="("; end_with=")"; n_elts = list_length(result);}
    else {/* Never reached */}

    if ((showall==0) && ((show_elts>0) && (n_elts>(show_elts*2))))
      max_elts = show_elts;
    else max_elts = n_elts;

    if ( (max_elts<n_elts) && (histref) )
      u8_printf(out,_("%s ;; %s = (%d/%d items)"),start_with,histref,max_elts,n_elts);
    else if (max_elts<n_elts)
      u8_printf(out,_("%s ;; %s = (%d/%d items)"),start_with,histref,max_elts,n_elts);
    else if (histref)
      u8_printf(out,_("%s ;; %s = (%d items)"),start_with,histref,n_elts);
    else u8_printf(out,_("%s ;; (%d items)"),start_with,max_elts,n_elts);

    if (CHOICEP(result)) {
      FD_DO_CHOICES(elt,result) {
        if ((max_elts>0) && (count<max_elts)) {
          output_element(out,elt,histref,count);
          count++;}
        else {FD_STOP_DO_CHOICES; break;}}}
    else if (VECTORP(result)) {
      lispval *elts = VEC_DATA(result);
      while (count<max_elts) {
        output_element(out,elts[count],histref,count);
        count++;}}
    else if (PAIRP(result)) {
      lispval scan = result;
      while (count<max_elts)
        if (PAIRP(scan)) {
          output_element(out,FD_CAR(scan),histref,count);
          count++; scan = FD_CDR(scan);}
        else {
          u8_printf(out,"\n  . ;; improper list");
          output_element(out,scan,NULL,count);
          count++; scan = VOID;
          break;}}
    else {}

    if (max_elts<n_elts)
      u8_printf(out,"\n  ;; ....... %d more items .......",n_elts-max_elts);
    if ( (max_elts<n_elts) && (histref) )
      u8_printf(out,"\n%s ;; ==%s (%d/%d items)\n",
                end_with,histref,max_elts,n_elts);
    else if (max_elts<n_elts)
      u8_printf(out,"\n%s ;; (%d/%d items)\n",end_with,max_elts,n_elts);
    else if (histref)
      u8_printf(out,"\n%s ;; ==%s(%d items)\n",end_with,histref,n_elts);
    else u8_printf(out,"\n%s ;; (%d items)\n",end_with,n_elts);
    return 1;
  } else {
    if (histref == NULL)
      u8_printf(out,"%Q\n",result);
    else if (console_width<=0)
      u8_printf(out,"%q\n;; =%s\n",result,histref);
    else {
      struct U8_OUTPUT tmpout;
      U8_INIT_STATIC_OUTPUT(tmpout,500);
      u8_puts(&tmpout,"    ");
      fd_pprint(&tmpout,result,NULL,4,4,console_width);
      u8_puts(out,tmpout.u8_outbuf);
      u8_close_output(&tmpout);
      u8_putc(out,'\n');
      u8_flush(out);}
    return 1;}
}

/* Command line design */
/*
  :command params*
  To read a param:
   skip whitepsace
   if at :({#" call parse_arg and wrap in quasiquote if needed;
   if at , just call read;
   otherwise read string until space,
    using \ and | as escapes;

  Model A: just combine command name (as symbol) with args into an expression
  Model B; model A, but add a suffix/prefix to command name (e.g. foo-command)
  Model C: return a rail and lookup the command handler, gather
   arguments (with prompts) as needed.

  Using temp file state.
  Have ==foo store the last value into the file FOO.dtype
   in a temporary directory (FDCONSOLETMP)
  Have #=foo fetch that value if it exists.

*/

static lispval stream_read(u8_input in,fd_lexenv env)
{
  lispval expr; int c;
  u8_puts(outconsole,eval_prompt); u8_flush(outconsole);
  c = skip_whitespace(in);
  if (c<0) return FD_EOF;
  else if (c=='=') {
    lispval sym = fd_parser(in);
    if (SYMBOLP(sym)) {
      swallow_whitespace(in);
      return fd_make_nrail(2,FDSYM_EQUALS,sym);}
    else {
      fd_decref(sym);
      u8_printf(errconsole,_(";; Bad assignment expression!\n"));
      return VOID;}}
  /* Handle command parsing here */
  /* else if ((c==':')||(c==',')) {} */
  else u8_ungetc(in,c);
  expr = fd_parser(in);
  if ((expr == FD_EOX)||(expr == FD_EOF)) {
    if (in->u8_read>in->u8_inbuf)
      return fd_err(fd_ParseError,"stream_read",NULL,expr);
    else return expr;}
  else {
    swallow_whitespace(in);
    return expr;}
}

static lispval console_read(u8_input in,fd_lexenv env)
{
#if USING_EDITLINE
  if ((use_editline)&&(in == inconsole)) {
    struct U8_INPUT scan; lispval expr; int n_bytes;
    const char *line = el_gets(editconsole,&n_bytes);
    if (!(line)) return FD_EOF;
    else while (isspace(*line)) line++;
    if (line[0]=='=') {
      U8_INIT_STRING_INPUT(&scan,n_bytes-1,line+1);
      expr = fd_parser(&scan);
      return fd_make_nrail(2,FDSYM_EQUALS,expr);}
    /* Handle command line parsing here */
    /* else if ((line[0]=='=')||(line[0]==',')) {} */
    else {
      struct HistEvent tmp;
      history(edithistory,&tmp,H_ENTER,line);
      U8_INIT_STRING_INPUT(&scan,n_bytes,line);
      expr = fd_parser(&scan);
      if (FD_ABORTP(expr)) {
        el_reset(editconsole);
        return expr;}
      else return expr;}}
  else return stream_read(in,env);
#endif
  return stream_read(in,env);
}

static void exit_fdconsole()
{
  if (!(quiet_console)) {
    if (run_start<0)
      u8_message("<%ld> Exiting FramerD (%s) console before we even started!",
                 (long)getpid(),FRAMERD_REVISION);
    else if (!(fd_be_vewy_quiet))
      fd_log_status("Exit(fdconsole)");
    else {}}
  close_consoles();
#if USING_EDITLINE
  if (editconsole) el_end(editconsole);
  if (edithistory) history_end(edithistory);
#endif
}

static lispval that_symbol, histref_symbol;

#if HAVE_CHMOD
#define changemode(f,m) chmod(f,m)
#else
#define changemode(f,m)
#endif

static void dump_backtrace(u8_exception ex,u8_string dumpfile)
{
  u8_string abspath = ((dumpfile)?
                     (u8_abspath(dumpfile,NULL)):
                     (fd_tempdir(NULL,0)));
  if (u8_directoryp(abspath)) {
    changemode(abspath,0755);
    if (html_backtrace) {
      u8_string btfile = u8_mkpath(abspath,"backtrace.html");
      dump_backtrace(ex,btfile);
      changemode(btfile,0444);
      u8_free(btfile);}
    if (dtype_backtrace) {
      u8_string btfile = u8_mkpath(abspath,"backtrace.dtype");
      dump_backtrace(ex,btfile);
      changemode(btfile,0444);
      u8_free(btfile);}
    if (lisp_backtrace) {
      u8_string btfile = u8_mkpath(abspath,"backtrace.lispdata");
      dump_backtrace(ex,btfile);
      changemode(btfile,0444);
      u8_free(btfile);}
    if ((text_backtrace)||
        (!((dtype_backtrace)||(lisp_backtrace)||
           ((html_backtrace)&&(FD_HTMLDUMP_ENABLED))))) {
      u8_string btfile = u8_mkpath(abspath,"backtrace.text");
      dump_backtrace(ex,btfile);
      changemode(btfile,0444);
      u8_free(btfile);}}
  else if ((u8_has_suffix(dumpfile,".scm",1))||
           (u8_has_suffix(dumpfile,".lisp",1))||
           (u8_has_suffix(dumpfile,".lispdata",1))) {
    u8_output outfile = (u8_output)
      u8_open_output_file(abspath,NULL,O_RDWR|O_CREAT,0600);
    lispval backtrace = FD_U8X_STACK(ex);
    fd_pprint(outfile,backtrace,NULL,0,0,120);
    u8_close((u8_stream)outfile);
    changemode(abspath,0444);
    u8_log(LOG_ERROR,ex->u8x_cond,
           "Backtrace object written to %s",abspath);}
#if FD_HTMLDUMP_ENABLED
  else if ((u8_has_suffix(dumpfile,".html",1))||
           (u8_has_suffix(dumpfile,".htm",1))) {
    u8_output outfile = (u8_output)
      u8_open_output_file(abspath,NULL,O_RDWR|O_CREAT,0600);
    u8_string bugurl = NULL;
    fd_xhtmldebugpage(outfile,ex);
    u8_close((u8_stream)outfile);
    changemode(abspath,0444);
    if ((bugdumps)&&(bugurlbase)&&(u8_has_prefix(abspath,bugdumps,0)))
      bugurl = u8_mkpath(bugurlbase,abspath+strlen(bugdumps));
    else bugurl = u8_string_append("file://",abspath,NULL);
    u8_log(LOG_ERROR,ex->u8x_cond,"HTML backtrace written to %s",bugurl);
    if (bugbrowse) {
      struct U8_OUTPUT cmd; U8_INIT_STATIC_OUTPUT(cmd,256); int retval;
      u8_printf(&cmd,"%s %s",bugbrowse,bugurl);
      retval = system(cmd.u8_outbuf);
      if (retval!=0)
        u8_log(LOG_WARN,"There was an error browsing the backtrace with '%s'",
               cmd.u8_outbuf);
      u8_free(cmd.u8_outbuf);}
    if (bugurl) u8_free(bugurl);}
#else
  else if ((u8_has_suffix(dumpfile,".html",1))||
           (u8_has_suffix(dumpfile,".htm",1))) {
    u8_log(LOG_WARN,"BACKTRACE","No built-in support for HTML backtraces");}
#endif
  else if (u8_has_suffix(dumpfile,".dtype",1)) {
    struct FD_STREAM *out; int bytes = 0;
    lispval backtrace = FD_U8X_STACK(ex);
    u8_string temp_name = u8_mkstring("%s.part",abspath);
    out = fd_open_file(temp_name,FD_FILE_CREATE);
    if (out == NULL) {
      u8_log(LOG_ERROR,"BACKTRACE","Can't open %s to write %s",
             temp_name,abspath);
      u8_free(temp_name);}
    else bytes = fd_write_dtype(fd_writebuf(out),backtrace);
    if (bytes<0) {
      fd_close_stream(out,FD_STREAM_CLOSE_FULL);
      u8_free(temp_name);
      u8_log(LOG_ERROR,"BACKTRACE","Can't open %s to write %s",
             temp_name,abspath);}
    else {
      fd_close_stream(out,FD_STREAM_CLOSE_FULL);
      u8_movefile(temp_name,abspath);
      u8_free(temp_name);
      changemode(abspath,0444);
      u8_log(LOG_ERROR,ex->u8x_cond,"DType backtrace written to %s",abspath);}}
  else {
    u8_output outfile = (u8_output)u8_open_output_file
      (abspath,NULL,O_RDWR|O_CREAT,0600);
    lispval backtrace = FD_U8X_STACK(ex);
    fd_pprint(outfile,backtrace,NULL,0,0,120);
    u8_close((u8_stream)outfile);
    changemode(abspath,0444);
    u8_log(LOG_ERROR,ex->u8x_cond,"Plaintext backtrace written to %s",
           abspath);}
  u8_free(abspath);
}

static lispval backtrace_prim(lispval arg)
{
  if (!(last_exception)) {
    u8_log(LOG_WARN,"backtrace_prim","No outstanding exceptions!");
    return VOID;}
  if (VOIDP(arg))
    dump_backtrace(last_exception,NULL);
  else if (STRINGP(arg))
    dump_backtrace(last_exception,CSTRING(arg));
  else if (FD_TRUEP(arg)) {
    lispval backtrace = FD_U8X_STACK(last_exception);
    return fd_incref(backtrace);}
  else if (FALSEP(arg)) {
    u8_free_exception(last_exception,1);
    last_exception = NULL;}
  return VOID;
}

static int bugdir_config_set(lispval var,lispval val,void *d)
{
  u8_string *bugdir = (u8_string *) d;
  u8_string old_dir = *bugdir;
  if (FD_FALSEP(val)) {
    *bugdir=NULL;
    if (old_dir) u8_free(old_dir);
    return 0;}
  else if (FD_TRUEP(val)) {
    *bugdir = u8_getcwd();
    if (old_dir) u8_free(old_dir);
    return 1;}
  else if ( (FD_STRINGP(val)) && (FD_STRLEN(val) == 0) ) {
    *bugdir = u8_strdup("");
    if (old_dir) u8_free(old_dir);
    return 1;}
  else if ( (FD_STRINGP(val)) &&
            (u8_directoryp(FD_CSTRING(val))) ) {
    *bugdir = u8_abspath(FD_CSTRING(val),NULL);
    if (old_dir) u8_free(old_dir);
    return 1;}
  else if (FD_STRINGP(val)) {
    fd_seterr("BadConsoleBugDir","bugdir_config_set",FD_CSTRING(val),val);
    return -1;}
  else {
    fd_seterr("BadConsoleBugDir","bugdir_config_set",NULL,val);
    return -1;}
}

/* Load dot files into the console */

static void dotloader(u8_string file,fd_lexenv env)
{
  u8_string abspath = u8_abspath(file,NULL);
  if (u8_file_existsp(abspath)) {
    char *kind = ((env == NULL)?("CONFIG"):("INIT"));
    double started = u8_elapsed_time(), elapsed; int err = 0;
    if (!(quiet_console)) u8_message("%s(%s)",kind,abspath);
    if (env == NULL) {
      int retval = fd_load_config(abspath);
      elapsed = u8_elapsed_time()-started;
      if (retval<0) err = 1;}
    else {
      lispval val = fd_load_source(abspath,env,NULL);
      elapsed = u8_elapsed_time()-started;
      if (FD_ABORTP(val)) err = 1;
      fd_decref(val);}
    if (err) {
      u8_message("Error for %s(%s)",kind,abspath);
      fd_clear_errors(1);}
    else if (quiet_console) {}
    else if (elapsed<0.1) {}
    else if (elapsed>1)
      u8_message("%0.3fs for %s(%s)",elapsed,kind,abspath);
    else u8_message("%0.3fms for %s(%s)",(elapsed*1000),kind,abspath);}
  u8_free(abspath);
}

int main(int argc,char **argv)
{
  int i = 1;
  unsigned int arg_mask = 0; /* Bit map of args to skip */
  time_t boot_time = time(NULL);
  lispval expr = VOID, result = VOID, lastval = VOID;
  u8_encoding enc = u8_get_default_encoding();
  u8_input in = (u8_input)u8_open_xinput(0,enc);
  u8_output out = (u8_output)u8_open_xoutput(1,enc);
  u8_output err = (u8_output)u8_open_xoutput(2,enc);
  u8_string source_file = NULL; /* The file loaded, if any */
  /* This is the environment the console will start in */
  fd_lexenv env = fd_working_lexenv();

  fd_set_app_env(env);

  fd_main_errno_ptr = &errno;
  FD_INIT_CSTACK();

  if (getenv("FD_SKIP_DOTLOAD")) dotload = 0;

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
  fd_init_scheme();
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
  fd_init_fdweb();
#endif

  eval_prompt = u8_strdup(EVAL_PROMPT);

  /* Register configuration parameters */
  fd_register_config("SHOWTIME",_("Threshold for displaying execution time"),
                     fd_dblconfig_get,fd_dblconfig_set,&showtime_threshold);
  fd_register_config("PROMPT",_("Eval prompt (within #||#s)"),
                     fd_sconfig_get,set_prompt,&eval_prompt);
  fd_register_config
    ("DBGMAXCHARS",
     _("Max number of string characters to display in debug message"),
     fd_intconfig_get,fd_intconfig_set,&debug_maxchars);
  fd_register_config
    ("DBGMAXELTS",
     _("Max number of sequence/choice elements to display in debug message"),
     fd_intconfig_get,fd_intconfig_set,&debug_maxelts);
  fd_register_config
    ("QUIET",_("Whether to minimize console output"),
     fd_boolconfig_get,fd_boolconfig_set,&quiet_console);
  fd_register_config
    ("STARTUPTIME",_("Report startup times when longer than N seconds"),
     fd_dblconfig_get,fd_dblconfig_set,&time_startup);
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
  fd_register_config
    ("BUGLOG",_("Where to dump console errors"),
     fd_sconfig_get,bugdir_config_set,&console_bugdir);

  if ( (console_bugdir == NULL) && (u8_directoryp("./_bugjar")) )
    console_bugdir = u8_abspath("./_bugjar",NULL);

  /* Initialize console streams */
  inconsole = in;
  outconsole = out;
  errconsole = err;
  atexit(exit_fdconsole);

  fd_autoload_config("LOADMOD","LOADFILE","INITS");

  if (u8_has_suffix(argv[0],"/fdconsole",0))
    u8_default_appid("fdconsole");
  else if (u8_has_suffix(argv[0],"/fdsh",0))
    u8_default_appid("fdsh");
  else if (u8_has_suffix(argv[0],"/fdshell",0))
    u8_default_appid("fdshell");
  else u8_default_appid(argv[0]);
  setlocale(LC_ALL,"");
  that_symbol = fd_intern("THAT");
  histref_symbol = fd_intern("%HISTREF");

  /* Process config fields in the arguments,
     storing the first non config field as a source file. */
  while (i<argc) {
    if (isconfig(argv[i]))
      u8_log(LOG_DEBUG,"Config","    %s",argv[i++]);
    else if (source_file) i++;
    else {
      if (u8_file_existsp(argv[i])) {
        if (i<32) arg_mask = arg_mask | (1<<i);}
      source_file = argv[i++];}}

  fd_handle_argv(argc,argv,arg_mask,NULL);

  FD_NEW_STACK(((struct FD_STACK *)NULL),"fdconsole",NULL,VOID);
  _stack->stack_label=u8_strdup(u8_appid());
  _stack->stack_free_label=1;

  stop_file=fd_runbase_filename(".stop");
  fd_register_config
    ("STOPFILE",
     _("File to wait to exist before starting"),
     fd_sconfig_get,fd_sconfig_set,
     &stop_file);

  /* Announce preamble, suppressed by quiet_console */
  if (!(quiet_console)) {
    if (fd_boot_message()) {
      /* Extra stuff, if desired */}}

  if (source_file == NULL) {}
  else if (strchr(source_file,'@')) {
    int sock = u8_connect(source_file);
    struct FD_STREAM *newstream;
    if (sock<0) {
      u8_log(LOG_WARN,"Connection failed","Couldn't open connection to %s",
             source_file);
      exit(-1);}
    newstream = u8_alloc(struct FD_STREAM);
    fd_init_stream(newstream,source_file,sock,
                   FD_STREAM_SOCKET|FD_STREAM_DOSYNC,
                   fd_network_bufsize);
    fd_use_pool(source_file,0,VOID);
    fd_use_index(source_file,0,VOID);
    eval_server = newstream;}
  else if (u8_file_existsp(source_file)) {
    lispval sourceval = fdstring(u8_realpath(source_file,NULL));
    fd_set_config("SOURCE",sourceval); fd_decref(sourceval);
    fd_load_source(source_file,env,NULL);}
  else {}

  /* This is argv[0], the name of the executable by which we
     entered fdconsole. */
  {
    lispval interpval = fd_lispstring(u8_fromlibc(argv[0]));
    fd_set_config("INTERPRETER",interpval);
    fd_decref(interpval);}

  fd_idefn((lispval)env,fd_make_cprim1("BACKTRACE",backtrace_prim,0));
  fd_defalias((lispval)env,"%","BACKTRACE");

  fd_idefnN((lispval)env,"%HISTREF",histref_prim,1,
            "Returns a history reference, possibly "
            "following a number/string path");
  fd_idefn0((lispval)env,"%HISTORY",history_prim,
            "Returns the current history object");
  history_symbol = fd_intern("%HISTORY");
  histref_symbol = fd_intern("%HISTREF");

  fd_set_config("BOOTED",fd_time2timestamp(boot_time));
  run_start = u8_elapsed_time();

  if (!(quiet_console)) {
    double startup_time = u8_elapsed_time()-fd_load_start;
    double show_startup_time = startup_time;
    char *units="s";
    if (show_startup_time>1) {}
    else if (show_startup_time>0.001) {
      show_startup_time = show_startup_time*1000; units="ms";}
    else {show_startup_time = show_startup_time*1000000; units="us";}
    if (startup_time > time_startup)
      u8_message("FramerD %s loaded in %0.3f%s, %d/%d pools/indexes",
                 u8_appid(),show_startup_time,units,fd_n_pools,
                 fd_n_primary_indexes+fd_n_secondary_indexes);}
  if (dotload) {
    u8_string home_config = u8_realpath("~/.fdconfig",NULL);
    u8_string cwd_config = u8_realpath(".fdconfig",NULL);
    int not_in_kansas = strcmp(home_config,cwd_config);
    dotloader("~/.fdconfig",NULL);
    if (not_in_kansas) dotloader(".fdconfig",NULL);
    dotloader("~/.fdconsole",env);
    if (not_in_kansas) dotloader(".fdconsole",env);
    u8_free(home_config);
    u8_free(cwd_config);}
  else u8_message("Warning: .fdconfig/.fdconsole files are suppressed");

#if USING_EDITLINE
  if (!(getenv("INSIDE_EMACS"))) {
    struct HistEvent tmp;
    edithistory = history_init();
    history(edithistory,&tmp,H_SETSIZE,256);
    editconsole = el_init(u8_appid(),stdin,stdout,stderr);
    el_set(editconsole,EL_PROMPT,editline_promptfn);
    el_set(editconsole,EL_HIST,history,edithistory);
    el_set(editconsole,EL_EDITOR,"emacs");
    use_editline = 1;}
#endif

  fd_register_config
    ("EDITLINE",_("Whether to use EDITLINE for the console"),
     fd_boolconfig_get,fd_boolconfig_set,&use_editline);

  /* This is the REPL value history, not the editline history */
  fd_histinit(0);

  fd_set_config("SIGRAISE",FD_INT(SIGINT));

  while (1) { /* ((c = skip_whitespace((u8_input)in))>=0) */
    int start_icache, finish_icache;
    int start_ocache, finish_ocache;
    double start_time, finish_time;
    int histref = -1, stat_line = 0, is_histref = 0;
    u8_byte histref_buf[100];
    start_ocache = fd_object_cache_load();
    start_icache = fd_index_cache_load();
    u8_flush(out);
    expr = console_read(in,env);
    if (TYPEP(expr,fd_code_type)) {
      /* Handle commands */
      lispval head = FD_VECTOR_REF(expr,0);
      if ((head == FDSYM_EQUALS)&&
          (VEC_LEN(expr)==2)&&
          (SYMBOLP(VEC_REF(expr,1)))) {
        lispval sym = VEC_REF(expr,1);
        fd_bind_value(sym,lastval,env);
        u8_printf(out,_(";; Bound %q\n"),sym);}
      else u8_printf(out,_(";; Bad command result %q\n"),expr);
      fd_decref(expr);
      continue;}
    if ((FD_EOFP(expr)) || (FD_EOXP(expr))) {
      fd_decref(result);
      break;}
    /* Clear the buffer (should do more?) */
    if (((PAIRP(expr)) &&
         ((FD_EQ(FD_CAR(expr),histref_symbol))) &&
         (PAIRP(FD_CDR(expr))) && (FD_UINTP(FD_CADR(expr)))) ||
        (FD_EQ(expr,that_symbol))) {
      if (!(FD_EQ(expr,that_symbol)))
        is_histref = 1;
      histref = FIX2INT(FD_CAR(FD_CDR(expr)));}
    else if (OIDP(expr)) {
      lispval v = fd_oid_value(expr);
      if (CHOICEP(v))
        u8_printf(out,"OID value: %q\n",v);
      else if (TABLEP(v)) {
        U8_OUTPUT out; U8_INIT_STATIC_OUTPUT(out,4096);
        u8_printf(&out,"%q:\n",expr);
        fd_display_table(&out,v,VOID);
        fputs(out.u8_outbuf,stdout); u8_free(out.u8_outbuf);
        fflush(stdout);}
      else u8_printf(out,"OID value: %q\n",v);
      fd_decref(v);
      continue;}
    start_time = u8_elapsed_time();
    if (errno) {
      u8_log(LOG_WARN,u8_strerror(errno),"Unexpected errno after read");
      errno = 0;}
    if (FD_ABORTP(expr)) {
      result = fd_incref(expr);
      u8_printf(out,";; Flushing input, parse error @%d\n",
                in->u8_read-in->u8_inbuf);
      u8_flush_input((u8_input)in);
      u8_flush((u8_output)out);}
    else {
      if (eval_server) {
        fd_write_dtype(fd_writebuf(eval_server),expr);
        fd_flush_stream(eval_server);
        result = fd_read_dtype(fd_readbuf(eval_server));}
      else result = fd_eval(expr,env);}
    if (errno) {
      u8_log(LOG_WARN,u8_strerror(errno),"Unexpected errno after eval");
      errno = 0;}
    if (PRECHOICEP(result))
      result = fd_simplify_choice(result);
    finish_time = u8_elapsed_time();
    finish_ocache = fd_object_cache_load();
    finish_icache = fd_index_cache_load();
    if ((PAIRP(expr))&&
        (!((FD_CHECK_PTR(result)==0) || (is_histref) ||
           (VOIDP(result)) || (EMPTYP(result)) ||
           (FD_TRUEP(result)) || (FALSEP(result)) ||
           (FD_ABORTP(result)) || (FIXNUMP(result))))) {
      int ref = fd_histpush(result);
      if (ref>=0) {
        histref = ref;
        u8_sprintf(histref_buf,100,"##%d",ref);}}
    else if ((SYMBOLP(expr))&&
             ((CHOICEP(result))||
              (VECTORP(result))||
              (PAIRP(result)))) {
      int ref = fd_histpush(result);
      if (ref>=0) {
        histref = ref;
        u8_sprintf(histref_buf,100,"##%d",ref);}}
    else {}
    if (FD_ABORTP(result)) stat_line = 0;
    else if ((showtime_threshold >= 0.0) &&
             (((finish_time-start_time) > showtime_threshold) ||
              (finish_ocache != start_ocache) ||
              (finish_icache != start_icache)))
      /* Whether to show a stat line (time/dbs/etc) */
      stat_line = 1;
    fd_decref(expr); expr = VOID;
    if (FD_CHECK_PTR(result)==0) {
      fprintf(stderr,";;; An invalid pointer 0x%llx was returned!\n",
              (unsigned long long)result);}
    else if (FD_TROUBLEP(result)) {
      u8_exception ex = u8_erreify();
      if (ex) {
        u8_fprintf(stderr,
                   ";;!!; There was an unexpected error %m <%s> (%s)\n",
                   ex->u8x_cond,
                   U8ALT(ex->u8x_context,"no caller"),
                   U8ALT(ex->u8x_details,"no details"));
        lispval exo = fd_get_exception(ex);
        if (!(FD_VOIDP(exo))) {
          if (save_backtrace)
            u8_fprintf(stderr,";; The exception was saved in ##%d\n",
                       fd_histpush(exo));
          if (console_bugdir) {
            if (*console_bugdir) fd_dump_bug(exo,console_bugdir);}
          else if (fd_dump_exception)
            fd_dump_exception(exo);
          else NO_ELSE;
          /* Note that u8_free_exception will decref exo, so we don't
             need to do so. */
          u8_free_exception(ex,1);}}
      else fprintf(stderr,
                   ";;; The expression generated a mysterious error!!!!\n");}
    else if (stat_line)
      output_result(out,result,histref_buf,is_histref);
    else if (VOIDP(result)) {}
    else if (histref<0)
      stat_line = output_result(out,result,histref_buf,is_histref);
    else {
      output_result(out,result,histref_buf,is_histref);
      stat_line = 1;}
    if (errno) {
      u8_log(LOG_WARN,u8_strerror(errno),"Unexpected errno after output");
      errno = 0;}
    if (stat_line) {
      if (histref<0)
        u8_printf (out,stats_message,
                   (finish_time-start_time),
                   finish_ocache-start_ocache,
                   finish_icache-start_icache);
      else {
        lispval sym = bind_random_symbol(result,env);
        if (VOIDP(sym))
          u8_printf(out,stats_message_w_history,
                    histref_buf,(finish_time-start_time),
                    finish_ocache-start_ocache,
                    finish_icache-start_icache);
        else u8_printf(out,stats_message_w_history_and_sym,
                       histref_buf,SYM_NAME(sym),
                       (finish_time-start_time),
                       finish_ocache-start_ocache,
                       finish_icache-start_icache);}}
    fd_clear_errors(1);
    fd_decref(lastval);
    lastval = result; result = VOID;
    if ((FD_CHECK_PTR(lastval)) &&
        (!(FD_ABORTP(lastval))) &&
        (!(FD_CONSTANTP(lastval))))
      fd_bind_value(that_symbol,lastval,env);}
  if (eval_server)
    fd_close_stream(eval_server,FD_STREAM_FREEDATA);
  if ( ! fd_fast_exit ) {
    u8_free(eval_server);
    fd_decref(lastval);
    fd_decref(result);}
  fd_pop_stack(_stack);
  fd_doexit(FD_FALSE);
  exit(0);
  return 0;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
