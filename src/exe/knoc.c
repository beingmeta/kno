/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/defines.h"
#include "kno/lisp.h"
#include "kno/cons.h"
#include "kno/tables.h"
#include "kno/storage.h"
#include "kno/eval.h"
#include "kno/history.h"
#include "kno/ports.h"
#include "kno/sequences.h"
#include "kno/fileprims.h"
#include "kno/cprims.h"

#if KNO_HTMLDUMP_ENABLED
#include "kno/webtools.h"
#endif

#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8netfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8stdio.h>

#if ((KNO_WITH_EDITLINE) && (HAVE_HISTEDIT_H) && (HAVE_LIBEDIT))
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

static lispval command_tag;

#define EVAL_PROMPT "#|kno>|# "
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
  else return KNO_ERR(-1,kno_TypeError,"set_prompt",
                      _("prompt is not a string"),v);
}

static int use_void_marker = 0;

#include "main.c"

#define KNO_MAX_COMMAND_LENGTH 128

static double showtime_threshold = 1.0;

static u8_string stats_message=
  _(";; Done in %f seconds, with %d/%d object/index loads\n");
static u8_string stats_message_w_history=
  _(";; %s computed in %f seconds, %d/%d object/index loads\n");

static double run_start = -1.0;

static int console_width = 80, quiet_console = 0, result_max_elts = 5;
static double time_startup = 1;

static u8_string stop_file=NULL;
static u8_string console_bugdir=NULL;

static int use_editline = 0;

#if USING_EDITLINE
static char *editline_promptfn(EditLine *ignored)
{
  return (char *) eval_prompt;
}
#endif

/* This skips whitespace and returns the first non-whitepsace
   character, which it consumes; */
int skip_whitespace(u8_input in)
{
  int c = u8_getc(in);
  while ((c>=0) && (u8_isspace(c))) c = u8_getc(in);
  if (c==';') {
    /* Handle comments */
    while ((c>=0) && (c != '\n')) c = u8_getc(in);
    if (c<0) return c;
    else return skip_whitespace(in);}
  else return c;
}

/* This is just like skip_whitespace, but doesn't consume the
   non-whitespace character it consumes. */
int swallow_whitespace(u8_input in)
{
  int c = u8_peekc(in);
  if (c<0) return c;
  while ((c>=0) && (u8_isspace(c))) {
    u8_getc(in); c = u8_peekc(in);}
  if (c==';') {
    while ((c>=0) && (c != '\n')) c = u8_getc(in);
    if (c<0) return c;
    else return swallow_whitespace(in);}
  else return c;
}

int swallow_hspace(u8_input in)
{
  int c = u8_peekc(in);
  if (c<0) return c;
  while ((c>=0) && (u8_ishspace(c))) {
    u8_getc(in); c = u8_peekc(in);}
  if (c==';') {
    while ((c>=0) && (c != '\n')) c = u8_getc(in);
    if (c<0) return c;
    else return swallow_hspace(in);}
  else return c;
}

static lispval parser(u8_input in)
{
  lispval result = kno_parser(in);
  while (result == KNO_BLANK_PARSE) result = kno_parser(in);
  return result;
}

static kno_stream eval_server = NULL;

static u8_input inconsole = NULL;
static u8_output outconsole = NULL;
static u8_output errconsole = NULL;
static kno_lexenv console_env = NULL;
static u8_string bugdumps = NULL;
static u8_string bugurlbase = NULL;
static u8_string bugbrowse = NULL;

static int html_backtrace = KNO_HTMLDUMP_ENABLED;
static int lisp_backtrace = (!(KNO_HTMLDUMP_ENABLED));
static int dtype_backtrace = 0;
static int text_backtrace = 0;
static int show_backtrace = 1;
static int save_backtrace = 1;
static int dotload = 1;

static u8_exception last_exception = NULL;

static int debug_maxelts = 32, debug_maxchars = 80;

static u8_mutex console_lock;
static int console_oid_display_level = 3;


static void close_consoles()
{
  u8_lock_mutex(&console_lock);
  u8_input in = inconsole;
  u8_output out = outconsole;
  u8_output err = errconsole;
  kno_lexenv env = console_env;
  inconsole = NULL;
  outconsole = errconsole = NULL;
  console_env = NULL;
  u8_unlock_mutex(&console_lock);
  if (in) {
    u8_close_input(in);}
  if (out) {
    u8_close_output(out);}
  if (err) {
    u8_close_output(err);}
  if (env) {
    kno_recycle_lexenv(env);}
}

static lispval oid_listfn(lispval item)
{
  if (KNO_OIDP(item)) {
    int display_level = console_oid_display_level;
    if ( display_level < kno_oid_display_level)
      display_level = kno_oid_display_level;
    if (display_level<2) return KNO_VOID;
    kno_pool p = kno_oid2pool(item);
    if (p == NULL) return KNO_VOID;
    if (kno_hashtable_probe(&(p->pool_cache),item))
      return KNO_VOID;
    else if (display_level<3) return KNO_VOID;
    lispval v = kno_oid_value(item);
    if (KNO_ABORTED(v)) u8_pop_exception();
    kno_decref(v);}
  return KNO_VOID;
}

static int output_result(struct U8_OUTPUT *out,lispval result,
                         u8_string histref,int width,int showall)
{
  int detail = (showall) ? (-(1+((result_max_elts)/2))) : (result_max_elts);
  if (width < 0) width = console_width;
  if (KNO_VOIDP(result)) {
    if (use_void_marker) {
      u8_puts(out,"#|<void|#\n");
      return 1;}
    else return 0;}
  if (KNO_OIDP(result)) {
    kno_pool p = kno_oid2pool(result);
    if (p) {
      lispval v = kno_oid_value(result);
      u8_byte _label[64];
      u8_string label = (histref) ?
        (u8_bprintf(_label,"%q %s",result,histref)) :
        (u8_bprintf(_label,"%q",result));
      /* Just list the value */
      u8_printf(out,"#|=>| %q\n#|frame\n  ",result);
      kno_list_object(out,v,label,histref,"  ",oid_listfn,width,
                      (showall)?(-4):(7));
      u8_printf(out,"\n|#\n");
      u8_flush(out);
      return 1;}}
  u8_puts(out,"#|=>| ");
  kno_list_object(out,result,NULL,histref,"      ",oid_listfn,width,detail);
  u8_putc(out,'\n');
  u8_flush(out);
  return 1;
}

/* History primitives */

static lispval history_symbol, histref_symbol;

static lispval histref_evalfn(lispval expr,kno_lexenv env,
			      kno_stack _stack)
{
  lispval history = kno_thread_get(history_symbol);
  if (KNO_ABORTP(history))
    return history;
  else if (VOIDP(history))
    return kno_err("NoActiveHistory","histref_evalfn",NULL,KNO_VOID);
  else NO_ELSE;
  lispval root = kno_get_arg(expr,1);
  lispval val = kno_history_ref(history,root);
  lispval paths = kno_get_body(expr,2);
  lispval scan = kno_incref(val);
  while ( (KNO_PAIRP(paths)) && (!(KNO_VOIDP(scan))) ) {
    lispval path = KNO_CAR(paths); paths = KNO_CDR(paths);
    if (KNO_FIXNUMP(path)) {
      int rel_off = KNO_FIX2INT(path);
      if (KNO_CHOICEP(scan)) {
        ssize_t n_choices = KNO_CHOICE_SIZE(scan);
        ssize_t off = (rel_off>=0) ?  (rel_off) : (n_choices + rel_off);
        if ( (off < 0) || (off >= n_choices) )
          return kno_err(kno_RangeError,"histref_evalfn",NULL,path);
        else {
          lispval new_scan = KNO_CHOICE_ELTS(scan)[off];
          kno_incref(new_scan); kno_decref(scan);
          scan=new_scan;}}
      else if (KNO_SEQUENCEP(scan)) {
        ssize_t n_elts = kno_seq_length(scan);
        ssize_t off = (rel_off>=0) ?  (rel_off) : (n_elts + rel_off);
        if ( (off < 0) || (off > n_elts) )
          return kno_err(kno_RangeError,"histref_evalfn",NULL,path);
        else {
          lispval new_scan = kno_seq_elt(scan,off);
          kno_decref(scan);
          scan=new_scan;}}
      else scan = KNO_VOID;}
    else if (KNO_STRINGP(path)) {
      if (KNO_TABLEP(scan)) {
        lispval v = kno_get(scan,path,KNO_VOID);
        if (KNO_VOIDP(v)) {
          u8_string upper = u8_upcase(KNO_CSTRING(scan));
          lispval sym = kno_probe_symbol(upper,-1);
          if (KNO_SYMBOLP(sym))
            v = kno_get(scan,sym,KNO_VOID);}
        if (KNO_VOIDP(v))
          kno_seterr("NoSuchKey","histref_evalfn",KNO_CSTRING(path),scan);
        scan = v;}
      else scan = KNO_VOID;}
    else if (KNO_SYMBOLP(path)) {
      if (KNO_TABLEP(scan)) {
        lispval v = kno_get(scan,path,KNO_VOID);
        if (KNO_VOIDP(v))
          kno_seterr("NoSuchKey","histref_evalfn",KNO_SYMBOL_NAME(path),scan);
        kno_decref(scan);
        scan = v;}
      else scan = KNO_VOID;}
    else scan = KNO_VOID;}
  if (KNO_VOIDP(scan)) {
    kno_seterr("InvalidHistRef","histref_evalfn",NULL,root);
    kno_decref(root);
    return KNO_ERROR;}
  else {
    kno_incref(scan);
    kno_decref(val);
    return scan;}
}

static lispval history_prim()
{
  return kno_thread_get(history_symbol);
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
  in a temporary directory (KNOCTMP)
  Have #=foo fetch that value if it exists.

*/

static lispval read_command(u8_input in,int iscmd,kno_lexenv env)
{
  lispval cmds[KNO_MAX_COMMAND_LENGTH];
  int n = 0, nextc = swallow_hspace(in);
  while ( (nextc > 0) && (nextc != '\n') ) {
    lispval arg = VOID;
    if ( (n == 0) ||
         (nextc == '#') || (nextc == '(') ||
         (nextc == '"') || (nextc == '\'') ||
         (nextc == '+') || (nextc == '-') ||
	 (nextc == '{') || (nextc == '[') ||
         (u8_isdigit(nextc)) )
      arg = parser(in);
    else if (nextc == ':') {
      u8_getc(in);
      arg = parser(in);}
    else {
      struct U8_OUTPUT argout;
      U8_INIT_OUTPUT(&argout,100);
      int ac = u8_getc(in);
      while ( (ac > 0) && (!(u8_isspace(ac))) ) {
        if (ac == '\\') {
          int nextac = u8_getc(in);
          if ( (nextac == 'u') || (nextac == 'U') ) {
            u8_putc(&argout,'\\');
            u8_putc(&argout,nextac);}
          else u8_putc(&argout,nextac);}
        else u8_putc(&argout,ac);
        ac = u8_getc(in);}
      arg = kno_init_string(NULL,argout.u8_write-argout.u8_outbuf,
                            argout.u8_outbuf);}
    if (n >= KNO_MAX_COMMAND_LENGTH) {
      kno_decref_elts(cmds,n);
      return kno_err("TooManyCommandArgs","stream_read",NULL,KNO_VOID);}
    cmds[n++] = arg;
    /* Add environment */
    if (n == 1) cmds[n++] = kno_incref((lispval)env);
    nextc = swallow_hspace(in);}
  if (n == 0)
    return KNO_EOX;
  else if (iscmd)
    return kno_init_compound_from_elts
      (NULL,command_tag,KNO_COMPOUND_SEQUENCE,n,cmds);
  else {
    lispval expr = KNO_NIL;
    int j = n-1; while (j>1) {
      /* Skip command name and environment */
      expr = kno_init_pair(NULL,cmds[j],expr); j--;}
    if (KNO_SYMBOLP(cmds[0])) {
      u8_string pname = KNO_SYMBOL_NAME(cmds[0]);
      u8_byte name_buf[strlen(pname)+16];
      strcpy(name_buf,pname);
      strcat(name_buf,".command");
      lispval cmd_symbol = kno_intern(name_buf);
      return kno_init_pair(NULL,cmd_symbol,expr);}
    else return kno_init_pair(NULL,cmds[0],expr);}
}

static lispval stream_read(u8_input in,kno_lexenv env)
{
  lispval expr; int c;
  u8_puts(outconsole,eval_prompt); u8_flush(outconsole);
  c = skip_whitespace(in);
  if (c<0) return KNO_EOF;
  else if (c=='=') {
    lispval expr = parser(in);
    swallow_hspace(in);
    return kno_init_compound
      (NULL,command_tag,KNO_COMPOUND_SEQUENCE,2,KNOSYM_EQUALS,expr);}
  else if ( (c==',') || (c == ':') ) {
    lispval cmds[KNO_MAX_COMMAND_LENGTH];
    int n = 0, nextc = swallow_hspace(in);
    while ( (nextc > 0) && (nextc != '\n') ) {
      lispval arg = VOID;
      if ( (n == 0) ||
           (nextc == '#') || (nextc == '(') ||
           (nextc == '"') || (nextc == '\'') ||
           (nextc == '+') || (nextc == '-') ||
           (u8_isdigit(nextc)) )
        arg = parser(in);
      else if (nextc == ':') {
        u8_getc(in); arg = parser(in);}
      else {
        struct U8_OUTPUT argout;
        U8_INIT_OUTPUT(&argout,100);
        int ac = u8_getc(in);
        while ( (ac > 0) && (!(u8_isspace(ac))) ) {
          if (ac == '\\') {
            int nextac = u8_getc(in);
            if ( (nextac == 'u') || (nextac == 'U') ) {
              u8_putc(&argout,'\\');
              u8_putc(&argout,nextac);}
            else u8_putc(&argout,nextac);}
          else u8_putc(&argout,ac);
          ac = u8_getc(in);}
        arg = kno_init_string(NULL,argout.u8_write-argout.u8_outbuf,
                              argout.u8_outbuf);}
      if (n >= KNO_MAX_COMMAND_LENGTH) {
        kno_decref_elts(cmds,n);
        return kno_err("TooManyCommandArgs","stream_read",NULL,KNO_VOID);}
      cmds[n++] = arg;
      nextc = swallow_hspace(in);}
    if (n == 0)
      return stream_read(in,env);
    else if (c == ':')
      return kno_init_compound_from_elts
        (NULL,command_tag,KNO_COMPOUND_SEQUENCE,n,cmds);
    else {
      lispval expr = KNO_NIL;
      int j = n-1; while (j>0) {
        expr = kno_init_pair(NULL,cmds[j],expr); j--;}
      if (KNO_SYMBOLP(cmds[0])) {
        u8_string pname = KNO_SYMBOL_NAME(cmds[0]);
        u8_byte name_buf[strlen(pname)+16];
        strcpy(name_buf,pname);
        strcat(name_buf,".command");
        lispval cmd_symbol = kno_intern(name_buf);
        return kno_init_pair(NULL,cmd_symbol,expr);}
      else return kno_init_pair(NULL,cmds[0],expr);}}
  /* Handle command parsing here */
  /* else if ((c==':')||(c==',')) {} */
  else u8_ungetc(in,c);
  expr = parser(in);
  if ((expr == KNO_EOX)||(expr == KNO_EOF)) {
    if (in->u8_read>in->u8_inbuf)
      return kno_err(kno_ParseError,"stream_read",NULL,expr);
    else return expr;}
  else {
    swallow_whitespace(in);
    return expr;}
}

static lispval console_read(u8_input in,kno_lexenv env)
{
#if USING_EDITLINE
  if ( (use_editline) && (in == inconsole) ) {
    struct U8_INPUT scan; lispval expr; int n_bytes;
    const char *line = el_gets(editconsole,&n_bytes);
    if (!(line)) return KNO_EOF;
    else while (isspace(*line)) line++;
    if (line[0] == '\0')
      return KNO_VOID;
    else if (line[0]=='=')  {
      U8_INIT_STRING_INPUT(&scan,n_bytes-1,line+1);
      lispval expr = parser(&scan);
      return kno_init_compound
        (NULL,command_tag,KNO_COMPOUND_SEQUENCE,2,KNOSYM_EQUALS,expr);}
    else if ( (line[0] == ',') || (line[0] == ':') ) {
      U8_INIT_STRING_INPUT(&scan,n_bytes-1,line+1);
      lispval cmd = read_command(&scan,(line[0]==':'),env);
      if (cmd != KNO_EOX) return cmd;}
    /* Handle command line parsing here */
    /* else if ((line[0]=='=')||(line[0]==',')) {} */
    struct HistEvent tmp;
    U8_INIT_STRING_INPUT(&scan,n_bytes,line);
    expr = parser(&scan);
    if ( (KNO_EOXP(expr)) || (KNO_EOFP(expr)) ) {
      lispval readpoint = kno_make_string(NULL,-1,scan.u8_inbuf);
      u8_byte edge[42];
      size_t n_bytes = ( (scan.u8_inlim-scan.u8_read) > 40) ? (40) :
        (scan.u8_inlim-scan.u8_read);
      strncpy(edge,scan.u8_read,n_bytes);
      edge[n_bytes] = '\0';
      lispval err = kno_err(kno_ParseError,"console_read",edge,readpoint);
      kno_decref(readpoint);
      return err;}
    else {
      history(edithistory,&tmp,H_ENTER,line);
      el_reset(editconsole);
      return expr;}
  }
  else return stream_read(in,env);
#endif
  return stream_read(in,env);
}

static void exit_knoc()
{
  if (!(quiet_console)) {
    if (run_start<0)
      u8_message("<%ld> Exiting Kno (%s) console before we even started!",
                 (long)getpid(),KNO_REVISION);
    else if (!(kno_be_vewy_quiet))
      kno_log_status("Exit(knoc)");
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
                       (kno_tempdir(NULL,0)));
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
           ((html_backtrace)&&(KNO_HTMLDUMP_ENABLED))))) {
      u8_string btfile = u8_mkpath(abspath,"backtrace.text");
      dump_backtrace(ex,btfile);
      changemode(btfile,0444);
      u8_free(btfile);}}
  else if ((u8_has_suffix(dumpfile,".scm",1))||
           (u8_has_suffix(dumpfile,".lisp",1))||
           (u8_has_suffix(dumpfile,".lispdata",1))) {
    u8_output outfile = (u8_output)
      u8_open_output_file(abspath,NULL,O_RDWR|O_CREAT,0600);
    lispval backtrace = KNO_U8X_STACK(ex);
    kno_pprint(outfile,backtrace,NULL,0,0,120);
    u8_close((u8_stream)outfile);
    changemode(abspath,0444);
    u8_log(LOG_ERROR,ex->u8x_cond,
           "Backtrace object written to %s",abspath);}
#if KNO_HTMLDUMP_ENABLED
  else if ((u8_has_suffix(dumpfile,".html",1))||
           (u8_has_suffix(dumpfile,".htm",1))) {
    u8_output outfile = (u8_output)
      u8_open_output_file(abspath,NULL,O_RDWR|O_CREAT,0600);
    u8_string bugurl = NULL;
    kno_xhtmldebugpage(outfile,ex);
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
    struct KNO_STREAM *out; int bytes = 0;
    lispval backtrace = KNO_U8X_STACK(ex);
    u8_string temp_name = u8_mkstring("%s.part",abspath);
    out = kno_open_file(temp_name,KNO_FILE_CREATE);
    if (out == NULL) {
      u8_log(LOG_ERROR,"BACKTRACE","Can't open %s to write %s",
             temp_name,abspath);
      u8_free(temp_name);}
    else bytes = kno_write_dtype(kno_writebuf(out),backtrace);
    if (bytes<0) {
      kno_close_stream(out,KNO_STREAM_CLOSE_FULL);
      u8_free(temp_name);
      u8_log(LOG_ERROR,"BACKTRACE","Can't open %s to write %s",
             temp_name,abspath);}
    else {
      kno_close_stream(out,KNO_STREAM_CLOSE_FULL);
      u8_movefile(temp_name,abspath);
      u8_free(temp_name);
      changemode(abspath,0444);
      u8_log(LOG_ERROR,ex->u8x_cond,"DType backtrace written to %s",abspath);}}
  else {
    u8_output outfile = (u8_output)u8_open_output_file
      (abspath,NULL,O_RDWR|O_CREAT,0600);
    lispval backtrace = KNO_U8X_STACK(ex);
    kno_pprint(outfile,backtrace,NULL,0,0,120);
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
  else if (KNO_TRUEP(arg)) {
    lispval backtrace = KNO_U8X_STACK(last_exception);
    return kno_incref(backtrace);}
  else if (FALSEP(arg)) {
    u8_free_exception(last_exception,1);
    last_exception = NULL;}
  return VOID;
}

static int bugdir_config_set(lispval var,lispval val,void *d)
{
  u8_string *bugdir = (u8_string *) d;
  u8_string old_dir = *bugdir;
  if (KNO_FALSEP(val)) {
    *bugdir=NULL;
    if (old_dir) u8_free(old_dir);
    return 0;}
  else if (KNO_TRUEP(val)) {
    *bugdir = u8_getcwd();
    if (old_dir) u8_free(old_dir);
    return 1;}
  else if ( (KNO_STRINGP(val)) && (KNO_STRLEN(val) == 0) ) {
    *bugdir = u8_strdup("");
    if (old_dir) u8_free(old_dir);
    return 1;}
  else if ( (KNO_STRINGP(val)) &&
            (u8_directoryp(KNO_CSTRING(val))) ) {
    *bugdir = u8_abspath(KNO_CSTRING(val),NULL);
    if (old_dir) u8_free(old_dir);
    return 1;}
  else if (KNO_STRINGP(val))
    return KNO_ERR(-1,"BadConsoleBugDir","bugdir_config_set",KNO_CSTRING(val),val);
  else return KNO_ERR(-1,"BadConsoleBugDir","bugdir_config_set",NULL,val);
}

/* Local cprims */

static void link_local_cprims()
{
}

/* Load dot files into the console */

static void dotloader(u8_string file,kno_lexenv env)
{
  u8_string abspath = u8_abspath(file,NULL);
  if (u8_file_existsp(abspath)) {
    char *kind = ((env == NULL)?("CONFIG"):("INIT"));
    double started = u8_elapsed_time(), elapsed; int err = 0;
    if (!(quiet_console)) u8_message("%s(%s)",kind,abspath);
    if (env == NULL) {
      int retval = kno_load_config(abspath);
      elapsed = u8_elapsed_time()-started;
      if (retval<0) err = 1;}
    else {
      lispval val = kno_load_source(abspath,env,NULL);
      elapsed = u8_elapsed_time()-started;
      if (KNO_ABORTP(val)) err = 1;
      kno_decref(val);}
    if (err) {
      u8_message("Error for %s(%s)",kind,abspath);
      kno_clear_errors(1);}
    else if (quiet_console) {}
    else if (elapsed<0.1) {}
    else if (elapsed>1)
      u8_message("%0.3fs for %s(%s)",elapsed,kind,abspath);
    else u8_message("%0.3fms for %s(%s)",(elapsed*1000),kind,abspath);}
  u8_free(abspath);
}

DEF_KNOSYM(reqinit); DEF_KNOSYM(console_env); DEF_KNOSYM(lastval);

int main(int argc,char **argv)
{
  int i = 1;
  /* Mask of args which we handle */
  unsigned char arg_mask[argc];  memset(arg_mask,0,argc);
  time_t boot_time = time(NULL);
  u8_string source_file = NULL; /* The file loaded, if any */

  /* Initialize the libu8 stdio library if it won't happen automatically. */
#if (!(HAVE_CONSTRUCTOR_ATTRIBUTES))
  u8_initialize();
  u8_initialize_u8stdio();
  u8_init_chardata_c();
#endif

  u8_stdout_loglevel = U8_LOG_WARN;

  init_libraries();

#if ((!(HAVE_CONSTRUCTOR_ATTRIBUTES)) || (KNO_TESTCONFIG))
  kno_init_scheme();
#endif

  /* INITIALIZING MODULES */
  /* Normally, modules have initialization functions called when
     dynamically loaded.  However, if we are statically linked, or we
     don't have the "constructor attributes" use to declare init functions,
     we need to call some initializers explicitly. */

  /* Initialize builtin scheme modules.
     These include all modules specified by (e.g.):
     configure --enable-webtools */
#if ((HAVE_CONSTRUCTOR_ATTRIBUTES) && (!(KNO_TESTCONFIG)))
  KNO_INIT_SCHEME_BUILTINS();
#else
  /* If we're a "test" executable (KNO_TESTCONFIG), we're statically linked, so we need to
     initialize some modules explicitly (since the shared library initializers may not be
     invoked). */
  kno_init_texttools();
  kno_init_webtools();
#endif

#if KNO_TESTCONFIG && KNO_STATIC
  kno_init_texttools();
  kno_init_webtools();
#endif

  /*  kno_set_config("HISTREFS",KNO_TRUE); */

  lispval expr = VOID, result = VOID, lastval = VOID;
  u8_encoding enc = u8_get_default_encoding();
  u8_input in = (u8_input)u8_open_xinput(0,enc);
  u8_output out = (u8_output)u8_open_xoutput(1,enc);
  u8_output err = (u8_output)u8_open_xoutput(2,enc);
  if (out) out->u8_streaminfo |= U8_HUMAN_OUTPUT;
  if (err) err->u8_streaminfo |= U8_HUMAN_OUTPUT;

  /* This is the environment the console will start in */
  lispval init_reqinfo = kno_make_slotmap(7,0,NULL);
  kno_lexenv env = kno_working_lexenv();
  kno_store(init_reqinfo,KNOSYM(console_env),(lispval)env);
  lispval reqinfo = kno_deep_copy(init_reqinfo);
  kno_store(reqinfo,KNOSYM(reqinit),init_reqinfo);
  kno_set_app_env(env);
  kno_decref(init_reqinfo);
  kno_use_reqinfo(reqinfo);

  if (getenv("KNO_QUIET")) quiet_console=1;

  /* Announce preamble, suppressed by quiet_console */
  if (!(quiet_console)) {
    if (kno_boot_message()) {
      /* Extra stuff, if desired */}}


  kno_main_errno_ptr = &errno;
  KNO_INIT_CSTACK();

  if (getenv("KNO_SKIP_DOTLOAD")) dotload = 0;

  eval_prompt = u8_strdup(EVAL_PROMPT);

  /* Register configuration parameters */
  kno_register_config("SHOWTIME",_("Threshold for displaying execution time"),
                      kno_dblconfig_get,kno_dblconfig_set,&showtime_threshold);
  kno_register_config("PROMPT",_("Eval prompt (within #||#s)"),
                      kno_sconfig_get,set_prompt,&eval_prompt);
  kno_register_config
    ("CONSOLE:OID:DISPLAY",_("OID display level for the KNO console app"),
     kno_intconfig_get,kno_intconfig_set,
     &console_oid_display_level);
  kno_register_config
    ("KNOC:OID:DISPLAY",_("OID display level for the KNO console app"),
     kno_intconfig_get,kno_intconfig_set,
     &console_oid_display_level);
  kno_register_config
    ("DBGMAXCHARS",
     _("Max number of string characters to display in debug message"),
     kno_intconfig_get,kno_intconfig_set,&debug_maxchars);
  kno_register_config
    ("DBGMAXELTS",
     _("Max number of sequence/choice elements to display in debug message"),
     kno_intconfig_get,kno_intconfig_set,&debug_maxelts);
  kno_register_config
    ("QUIET",_("Whether to minimize console output"),
     kno_boolconfig_get,kno_boolconfig_set,&quiet_console);
  kno_register_config
    ("STARTUPTIME",_("Report startup times when longer than N seconds"),
     kno_dblconfig_get,kno_dblconfig_set,&time_startup);
  kno_register_config
    ("CONSOLEWIDTH",
     _("Number of characters available for pretty-printing console output"),
     kno_intconfig_get,kno_intconfig_set,&console_width);
  kno_register_config
    ("KNOC:SHOWELTS",
     _("Number of elements to initially show in displaying results"),
     kno_intconfig_get,kno_intconfig_set,&result_max_elts);
  kno_register_config
    ("SHOWELTS",
     _("Number of elements to initially show in displaying results"),
     kno_intconfig_get,kno_intconfig_set,&result_max_elts);
  kno_register_config
    ("BUGDUMPS",_("Where to create directories for saving backtraces"),
     kno_sconfig_get,kno_sconfig_set,&bugdumps);
  kno_register_config
    ("BUGURLBASE",_("The base URL for referencing HTML backtraces"),
     kno_sconfig_get,kno_sconfig_set,&bugurlbase);
  kno_register_config
    ("BUGBROWSE",_("The command to use to open saved backtraces"),
     kno_sconfig_get,kno_sconfig_set,&bugbrowse);
  kno_register_config
    ("HTMLDUMPS",_("Whether to dump HTML versions of backtraces"),
     kno_boolconfig_get,kno_boolconfig_set,&html_backtrace);
  kno_register_config
    ("LISPDUMPS",_("Whether to dump LISP object versions of backtraces"),
     kno_boolconfig_get,kno_boolconfig_set,&lisp_backtrace);
  kno_register_config
    ("DTYPEDUMPS",_("Whether to dump binary DTYPE versions of backtraces"),
     kno_boolconfig_get,kno_boolconfig_set,&dtype_backtrace);
  kno_register_config
    ("TEXTDUMPS",_("Whether to plaintext versions of backtraces"),
     kno_boolconfig_get,kno_boolconfig_set,&dtype_backtrace);
  kno_register_config
    ("SAVEBACKTRACE",_("Whether to always save backtraces in the history"),
     kno_boolconfig_get,kno_boolconfig_set,&save_backtrace);
  kno_register_config
    ("SHOWBACKTRACE",_("Whether to always output backtraces to stderr"),
     kno_boolconfig_get,kno_boolconfig_set,&show_backtrace);
  kno_register_config
    ("DOTLOAD",_("Whether load .knoc or other dot files"),
     kno_boolconfig_get,kno_boolconfig_set,&dotload);
  kno_register_config
    ("BUGLOG",_("Where to dump console errors"),
     kno_sconfig_get,bugdir_config_set,&console_bugdir);

  if ( (console_bugdir == NULL) && (u8_directoryp("./_bugjar")) )
    console_bugdir = u8_abspath("./_bugjar",NULL);

  /* Initialize console streams */
  u8_init_mutex(&console_lock);
  inconsole = in;
  outconsole = out;
  errconsole = err;
  atexit(exit_knoc);

  if (u8_has_suffix(argv[0],"/knoc",0))
    u8_default_appid("knoc");
  else if (u8_has_suffix(argv[0],"/knosh",0))
    u8_default_appid("knosh");
  else if (u8_has_suffix(argv[0],"/knoshell",0))
    u8_default_appid("knoshell");
  else u8_default_appid(argv[0]);
  setlocale(LC_ALL,"");
  that_symbol = kno_intern("that");
  histref_symbol = kno_intern("%histref");
  command_tag = kno_intern(".command.");

  /* Process config fields in the arguments,
     storing the first non config field as a source file. */
  while (i<argc) {
    if (isconfig(argv[i]))
      u8_log(LOG_DEBUG,"Config","    %s",argv[i++]);
    else if (source_file) i++;
    else {
      if (u8_file_existsp(argv[i])) {
        arg_mask[i] = 'X';}
      source_file = argv[i++];}}

  kno_handle_argv(argc,argv,arg_mask,NULL);
  kno_init_configs();

  KNO_INIT_STACK_ROOT();

  stop_file=kno_runbase_filename(".stop");
  kno_register_config
    ("STOPFILE",
     _("File to wait to exist before starting"),
     kno_sconfig_get,kno_sconfig_set,
     &stop_file);

  if (dotload) {
    u8_string home_config = u8_realpath("~/.knoconfig",NULL);
    u8_string home_configs = u8_realpath("~/.knoconfigs",NULL);
    dotloader("~/.knoconfig",NULL);
    dotloader("~/.knoc",env);
    if (u8_directoryp(home_configs)) {
      lispval strval = knostring(home_configs);
      kno_set_config("CONFIGSRC",strval);
      kno_decref(strval);}
    u8_free(home_config);
    u8_free(home_configs);}
  else u8_message("Warning: .knoconfig/.knoc files are suppressed");

  /* Load some standard console modules */
  kno_set_config("APPMODS",kno_intern("kno/sessions"));
  kno_set_config("APPMODS",kno_intern("kno/debug"));

  kno_autoload_config("LOADMOD","LOADFILE","INITS");

  if (source_file == NULL) {}
  else if (strchr(source_file,'@')) {
    int sock = u8_connect(source_file);
    struct KNO_STREAM *newstream;
    if (sock<0) {
      u8_log(LOG_WARN,"Connection failed","Couldn't open connection to %s",
             source_file);
      exit(-1);}
    newstream = u8_alloc(struct KNO_STREAM);
    kno_init_stream(newstream,source_file,sock,
                    KNO_STREAM_SOCKET|KNO_STREAM_DOSYNC,
                    kno_network_bufsize);
    kno_use_pool(source_file,0,VOID);
    kno_use_index(source_file,0,VOID);
    eval_server = newstream;}
  else if (u8_file_existsp(source_file)) {
    u8_string real = u8_realpath(source_file,NULL);
    lispval sourceval = knostring(real);
    kno_set_config("SOURCE",sourceval);
    kno_decref(sourceval);
    u8_free(real);
    kno_load_source(source_file,env,NULL);}
  else {}

  /* This is argv[0], the name of the executable by which we
     entered knoc. */
  {
    lispval interpval = kno_wrapstring(u8_fromlibc(argv[0]));
    kno_set_config("INTERPRETER",interpval);
    kno_decref(interpval);}

  lispval btprim = kno_make_cprim1("BACKTRACE",backtrace_prim,MIN_ARGS(0),
				   "Dumps a backtrace of the last error, either "
				   "into a file or to the console");
  kno_defn((lispval)env,btprim);
  kno_defalias((lispval)env,"%","BACKTRACE");
  kno_decref(btprim);

  lispval histprim = kno_make_cprim0("%HISTORY",history_prim,MIN_ARGS(0),
				     "Gets the current value history");
  kno_defn((lispval)env,histprim);
  kno_decref(histprim);

  kno_def_evalfn((lispval)env,"%HISTREF",histref_evalfn,"");

  link_local_cprims();

  history_symbol = kno_intern("%history");
  histref_symbol = kno_intern("%histref");

  kno_set_config("BOOTED",kno_time2timestamp(boot_time));
  run_start = u8_elapsed_time();

  if (!(quiet_console)) {
    double startup_time = u8_elapsed_time()-kno_load_start;
    double show_startup_time = startup_time;
    char *units="s";
    if (show_startup_time>1) {}
    else if (show_startup_time>0.001) {
      show_startup_time = show_startup_time*1000; units="ms";}
    else {show_startup_time = show_startup_time*1000000; units="us";}
    if (startup_time > time_startup)
      u8_message("Kno %s loaded in %0.3f%s, %d/%d pools/indexes",
                 u8_appid(),show_startup_time,units,kno_n_pools,
                 kno_n_primary_indexes);}

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

  kno_register_config
    ("EDITLINE",_("Whether to use EDITLINE for the console"),
     kno_boolconfig_get,kno_boolconfig_set,&use_editline);

  kno_register_config
    ("VOIDMARKER",_("Emit void marker (#|<void|#) when void is returned"),
     kno_boolconfig_get,kno_boolconfig_set,&use_void_marker);

  /* This is the REPL value history, not the editline history */
  lispval history = kno_hist_init(0);

  kno_set_config("SIGRAISE",KNO_INT(SIGINT));

  lispval _err_symbol = kno_intern("_err");
  kno_bind_value(_err_symbol,KNO_FALSE,env);

  while (1) { /* ((c = skip_whitespace((u8_input)in))>=0) */
    int start_icache, finish_icache;
    int start_ocache, finish_ocache;
    double start_time, finish_time;
    int histref = -1, stat_line = 0, showall = 0;
    u8_byte histref_buf[100];
    u8_string histref_string = NULL;
    start_ocache = kno_object_cache_load();
    start_icache = kno_index_cache_load();
    u8_flush(out);
    expr = console_read(in,env);
    expr = kno_eval_histrefs(expr,history);
    if (KNO_COMPOUND_TYPEP(expr,command_tag)) {
      /* Handle commands */
      lispval head = KNO_COMPOUND_REF(expr,0);
      if ( (head == KNOSYM_EQUALS ) &&
           ( (KNO_COMPOUND_LENGTH(expr)) == 2) &&
           ( SYMBOLP(KNO_COMPOUND_REF(expr,1)) ) ) {
        lispval sym = KNO_COMPOUND_REF(expr,1);
        kno_bind_value(sym,lastval,env);
        u8_printf(out,_(";; Bound %q\n"),sym);}
      else u8_printf(out,_(";; Bad command result %q\n"),expr);
      kno_decref(expr);
      continue;}
    if (KNO_EOFP(expr)) {
      kno_decref(result);
      break;}
    else if (KNO_EOXP(expr))
      expr = kno_err(kno_ParseError,"stream_read",NULL,expr);
    else NO_ELSE;
    /* Clear the buffer (should do more?) */
    if (((PAIRP(expr)) && ((KNO_EQ(KNO_CAR(expr),KNOSYM_QUOTE))))) {
      showall = 1;}
    start_time = u8_elapsed_time();
    if (errno) {
      u8_log(LOG_WARN,u8_strerror(errno),"Unexpected errno after read");
      errno = 0;}
    if (KNO_ABORTP(expr)) {
      result = kno_incref(expr);
      u8_printf(out,";; Flushing input, parse error @%d\n",
                in->u8_read-in->u8_inbuf);
      u8_flush_input((u8_input)in);
      u8_flush((u8_output)out);}
    else {
      if (eval_server) {
        kno_write_dtype(kno_writebuf(eval_server),expr);
        kno_flush_stream(eval_server);
        result = kno_read_dtype(kno_readbuf(eval_server));}
      else result = kno_eval(expr,env,_stack);}
    if (errno) {
      u8_log(LOG_WARN,u8_strerror(errno),"Unexpected errno after eval");
      errno = 0;}
    if (KNO_CHECK_PTR(result)==0) {
      fprintf(stderr,";;; An invalid pointer 0x%llx was returned!\n",
              (unsigned long long)result);
      result = KNO_VOID;}
    else if (PRECHOICEP(result))
      result = kno_simplify_choice(result);
    else if ( (KNO_CONSP(result)) && (KNO_STATIC_CONSP(result)) ) {
      u8_log(LOG_WARN,"StaticCons",
             "A static, potentially ephemeral cons was returned, copying...");
      result = kno_copy(result);}
    else NO_ELSE;
    finish_time = u8_elapsed_time();
    finish_ocache = kno_object_cache_load();
    finish_icache = kno_index_cache_load();
    if (((PAIRP(expr))&&
         (!((KNO_CHECK_PTR(result)==0) ||
            (VOIDP(result)) || (EMPTYP(result)) ||
            (KNO_TRUEP(result)) || (FALSEP(result)) ||
            (KNO_ABORTP(result)) || (FIXNUMP(result))))) ||
        (KNO_OIDP(expr)) || (KNO_CHOICEP(expr)) ||
        (KNO_VECTORP(expr)) || (KNO_COMPOUNDP(expr)) ||
        (KNO_SLOTMAPP(expr)) || (KNO_SCHEMAPP(expr))) {
      int ref = kno_history_push(history,result);
      if (ref>=0) {
        histref = ref;
        u8_sprintf(histref_buf,100,"#%d",ref);
        histref_string = histref_buf;}}
    else if ((SYMBOLP(expr)) && (KNO_CONSP(result))) {
      int ref = kno_history_push(history,result);
      if (ref>=0) {
        histref = ref;
        u8_sprintf(histref_buf,100,"#%d",ref);
        histref_string = histref_buf;}}
    else if ( (KNO_EQUALP(expr,result)) && (KNO_CONSP(expr)) ) {
      int ref = kno_history_push(history,result);
      if (ref>=0) {
        histref = ref;
        u8_sprintf(histref_buf,100,"#%d",ref);
        histref_string = histref_buf;}}
    else {
      histref = -1;
      histref_string = NULL;}
    if (KNO_ABORTP(result)) stat_line = 0;
    else if ((showtime_threshold >= 0.0) &&
             (((finish_time-start_time) > showtime_threshold) ||
              (finish_ocache != start_ocache) ||
              (finish_icache != start_icache)))
      /* Whether to show a stat line (time/dbs/etc) */
      stat_line = 1;
    kno_decref(expr); expr = VOID;
    if (KNO_TROUBLEP(result)) {
      u8_exception ex = u8_erreify();
      if (ex) {
	u8_byte tmpbuf[100];
	lispval irritant = kno_get_irritant(ex);
	u8_string irritation = (KNO_VOIDP(irritant)) ? (NULL) :
	  (u8_bprintf(tmpbuf,"%q",irritant));
	  u8_fprintf(stderr,
		     ";;!!; There was an unexpected error %m <%s> (%s)\n",
		     ex->u8x_cond,
		     U8ALT(ex->u8x_context,"no caller"),
		     U8ALT(ex->u8x_details,"no details"));
	  if (irritation)
	    u8_fprintf(stderr, ";;!!;\t irritant=%s\n",irritation);
	  lispval exo = kno_get_exception(ex);
	  if (!(KNO_VOIDP(exo))) {
	    if (save_backtrace)
	      u8_fprintf(stderr,";; The exception was saved in #%d\n",
			 kno_history_push(history,exo));
	    kno_assign_value(_err_symbol,exo,env);
	    kno_req_store(_err_symbol,exo);
	    if (console_bugdir) {
	      if (*console_bugdir) kno_dump_bug(exo,console_bugdir);}
	    else if (kno_dump_exception)
	      kno_dump_exception(exo);
	    else NO_ELSE;
	    /* Note that u8_free_exception will decref exo, so we don't
	       need to do so. */
	    u8_free_exception(ex,1);}}
      else fprintf(stderr,";;; The expression generated a mysterious error!!!!\n");}
    else if (stat_line)
      output_result(out,result,histref_string,console_width,showall);
    else if (VOIDP(result)) {}
    else if (histref<0) {
      output_result(out,result,histref_string,console_width,showall);
      stat_line = 1;}
    else {
      output_result(out,result,histref_string,console_width,showall);
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
        u8_printf(out,stats_message_w_history,
                  histref_string,(finish_time-start_time),
                  finish_ocache-start_ocache,
                  finish_icache-start_icache);}}
    kno_clear_errors(1);
    kno_decref(lastval);
    lastval = result; result = VOID;
    if ((KNO_CHECK_PTR(lastval)) &&
        (!(KNO_ABORTP(lastval))) &&
	(!(KNO_CONSTANTP(lastval)))) {
      kno_bind_value(that_symbol,lastval,env);
      kno_store(reqinfo,KNOSYM(lastval),lastval);}}

  if (eval_server)
    kno_close_stream(eval_server,KNO_STREAM_FREEDATA);
  if ( ! kno_fast_exit ) {
    u8_free(eval_server);
    kno_decref(lastval);
    kno_decref(result);}
  kno_decref(reqinfo);
  /* Run registered thread cleanup handlers.
     Note that since the main thread wasn't started with a function which
     calls u8_threadexit(), we do it here. */
  u8_threadexit();
  kno_pop_stack(_stack);
  kno_doexit(KNO_FALSE);

  /* Call this here, where it might be easier to debug, even
     though it's alos an atexit handler */
  _kno_finish_threads();

  exit(0);
  return 0;
}

