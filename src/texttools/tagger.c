/* C Mode */

#define U8_INLINE_IO 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"
#include "fdb/indices.h"
#include "fdb/dtypestream.h"

#include "fdb/tagger.h"

#include <libu8/u8ctype.h>
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>

#include <time.h>
#include <stdlib.h>
#include <string.h>
#if defined(WIN32)
#include <windows.h>
#endif

#define ZEROP(x) ((fd_getint(x)) == 0)

static fd_exception TooManyWords=_("Too many words");
static fd_exception ParseFailed=_("Parser failed");
static fd_exception InternalParseError=_("Internal parse error");
static fd_exception LateConfiguration=_("LEXDATA has already been configured");
static fd_exception NoGrammar=_("No grammar identified");


/* Miscellaneous defines */

#define DEBUGGING 0
#define TRACING 1

#ifndef PATH_MAX
#define PATH_MAX 1023
#endif

#define next_input(pc,i) ((pc->input)[i].next)
#define prev_input(pc,i) ((pc->input)[i].previous)

static u8_string utf8_next(u8_string s)
{
  if (*s < 0x80) return s+1;
  else {
    u8_sgetc(&s); return s;}
}

static int utf8_point(u8_string s)
{
  if (*s < 0x80) return *s;
  else return u8_sgetc(&s);
}

#define isquote(c) \
   ((c == '"') || (c == '\'') || (c == '`'))

static int capitalizedp(fdtype x)
{
  if (FD_STRINGP(x)) {
    u8_string s=FD_STRDATA(x); int c=u8_sgetc(&s);
    return u8_isupper(c);}
  else if (FD_PAIRP(x))
    return capitalizedp(FD_CAR(x));
  else return 0;
}

static int textgetc(u8_string *scanner)
{
  int c; while ((c=u8_sgetc(scanner))>=0)
    if (c=='&')
      if (strncmp(*scanner,"nbsp;",5)==0) {
	*scanner=*scanner+5;
	return ' ';}
      else {
	u8_byte *end=NULL;
	int code=u8_parse_entity(*scanner,&end);
	if (code<=0)
	  return '&';
	else {
	  *scanner=end;
	  return code;}}
    else if (c=='\r') {
      /* Convert CRLFs */
      u8_string scan=*scanner;
      int nextc=u8_sgetc(&scan);
      if (nextc=='\n') {
	*scanner=scan;
	return '\n';}
      else return '\r';}
    else if (c==0x2019) /* Convert weird apostrophes */
      return '\'';
    else return c;
}


/* Global declarations */

static int word_limit=-1;
static int tracing_tagger=0;

#if FD_THREADS_ENABLED
static u8_mutex parser_stats_lock;
#endif

static int max_states=0, total_states=0;
static int max_inputs=0, max_phrases=0;
static int total_inputs=0, total_sentences=0;
static double total_parse_time=0.0;

/* Declarations for lexicon access */

static fdtype noun_symbol, verb_symbol, name_symbol, compound_symbol;
static fdtype lexget_symbol, verb_root_symbol, noun_root_symbol;
static fdtype nouns_symbol, verbs_symbol, names_symbol, grammar_symbol;
static fdtype heads_symbol, mods_symbol;

/* These simple syntactic categories are used when words aren't
   in the lexicon. */
static fdtype sstrange_word, sproper_name, sdashed_word, sdashed_sword;
static fdtype ed_word, ing_word, ly_word, punctuation_symbol, stime_ref;
static fdtype sscore, snumber, sdollars, spossessive, sproper_possessive;
static fdtype sentence_end_symbol;

static fdtype parse_failed_symbol;

/* Default grammar */

static u8_string lexdata_source=NULL;
static struct FD_GRAMMAR *default_grammar=NULL;

static int chopper_initialized=0;
static u8_mutex default_grammar_lock;

static struct FD_GRAMMAR *get_default_grammar()
{
  u8_lock_mutex(&default_grammar_lock);
  if (default_grammar) {
    u8_unlock_mutex(&default_grammar_lock);
    return default_grammar;}
  else if (lexdata_source==NULL) {
    u8_unlock_mutex(&default_grammar_lock);
    return NULL;}
  else {
    default_grammar=fd_open_grammar(lexdata_source);
    u8_unlock_mutex(&default_grammar_lock);
    return default_grammar;}
}

FD_EXPORT
struct FD_GRAMMAR *fd_default_grammar()
{
  return get_default_grammar();
}

static fdtype config_get_lexdata(fdtype var)
{
  if (lexdata_source) 
    return fdtype_string(lexdata_source);
  else return FD_EMPTY_CHOICE;
}
static int config_set_lexdata(fdtype var,fdtype val)
{
  if (!(FD_STRINGP(val)))
    return fd_reterr(fd_TypeError,"config_set_lexdata",
		     u8_strdup(_("string")),fd_incref(val));
  else if (default_grammar)
    if (strcmp(FD_STRDATA(val),lexdata_source)==0) return 0;
    else {
      fd_seterr(LateConfiguration,"config_set_lexdata",
		u8_strdup(lexdata_source),FD_VOID);
      return -1;}
  else {
    if (lexdata_source) u8_free(lexdata_source);
    lexdata_source=u8_strdup(FD_STRDATA(val));
    return 1;}
}


/* Parse Contexts */

FD_EXPORT
fd_parse_context fd_init_parse_context
  (fd_parse_context pc,struct FD_GRAMMAR *grammar)
{
  unsigned int i=0;
  pc->grammar=grammar; pc->flags=FD_TAGGER_DEFAULT_FLAGS;
  /* Initialize inter-parse fields. */
  pc->n_calls=0; pc->cumulative_inputs=0;
  pc->cumulative_states=0; pc->cumulative_runtime=0.0;
  /* Initialize parsing data */
  pc->input=malloc(sizeof(struct FD_WORD)*FD_INITIAL_N_INPUTS);
  pc->n_inputs=0; pc->max_n_inputs=FD_INITIAL_N_INPUTS;
  pc->states=malloc(sizeof(struct FD_PARSER_STATE)*FD_INITIAL_N_STATES);
  pc->n_states=0; pc->max_n_states=FD_INITIAL_N_STATES;
  pc->queue=-1; pc->last=-1; pc->runtime=0.0;
  pc->cache=malloc(sizeof(fd_parse_state *)*FD_INITIAL_N_INPUTS);
  while (i < FD_INITIAL_N_INPUTS) {
    fd_parse_state *vec=malloc(sizeof(fd_parse_state)*pc->grammar->n_nodes);
    unsigned int j=0;
    while (j < pc->grammar->n_nodes) vec[j++]=-1;
    pc->cache[i++]=vec;}
  pc->end=pc->start=pc->buf=NULL; 
  return pc;
}

FD_EXPORT
void fd_reset_parse_context(fd_parse_context pcxt)
{
  int i=0, max_inputs=pcxt->max_n_inputs;
  /* Update global parser stats */
  u8_lock_mutex(&parser_stats_lock);
  total_states=total_states+pcxt->n_states;
  total_inputs=total_inputs+pcxt->n_inputs;
  total_parse_time=total_parse_time+pcxt->runtime;
  total_sentences++;
  if (pcxt->n_states > max_states) max_states=pcxt->n_states;
  if (pcxt->n_inputs > max_inputs) max_inputs=pcxt->n_inputs;
  u8_unlock_mutex(&parser_stats_lock);
  /* Report timing info if requested. */
  if (pcxt->flags&FD_TAGGER_VERBOSE_TIMER)
    u8_message("Parsed %d inputs, exploring %d states in %f seconds",
	       pcxt->n_inputs,pcxt->n_states,pcxt->runtime);
  /* Update stats for this parse context. */
  pcxt->cumulative_inputs=pcxt->cumulative_inputs+pcxt->n_inputs;
  pcxt->cumulative_states=pcxt->cumulative_states+pcxt->n_states;
  pcxt->cumulative_runtime=pcxt->cumulative_runtime+pcxt->runtime;
  /* Free input data from the last parse. */
  i=0; while (i < pcxt->n_inputs) {
    fd_decref(pcxt->input[i].lstr);
    pcxt->input[i].lstr=FD_VOID;
    fd_decref(pcxt->input[i].compounds);
    pcxt->input[i].compounds=FD_EMPTY_CHOICE;
    i++;}
  /* Reset the per-parse state variables */
  pcxt->n_states=0; pcxt->n_inputs=0; pcxt->runtime=0.0;
  pcxt->queue=-1; pcxt->last=-1;
  /* Reset the state/input cache. */
  i=0; while (i < max_inputs) {
    fd_parse_state *vec=pcxt->cache[i++];
    unsigned int j=0, n_nodes=pcxt->grammar->n_nodes;
    while (j < n_nodes) vec[j++]=-1;}
}

FD_EXPORT
void fd_free_parse_context(fd_parse_context pcxt)
{
  int i=0;
  /* Update global parser stats */
  u8_lock_mutex(&parser_stats_lock);
  total_states=total_states+pcxt->n_states;
  total_inputs=total_inputs+pcxt->n_inputs;
  total_parse_time=total_parse_time+pcxt->runtime;
  total_sentences++;
  if (pcxt->n_states > max_states) max_states=pcxt->n_states;
  if (pcxt->n_inputs > max_inputs) max_inputs=pcxt->n_inputs;
  u8_unlock_mutex(&parser_stats_lock);
  /* Report timing info if requested. */
  if (pcxt->flags&FD_TAGGER_VERBOSE_TIMER)
    u8_message("Parsed %d inputs, exploring %d states in %f seconds",
	       pcxt->n_inputs,pcxt->n_states,pcxt->runtime);
  /* Free memory structures for inputs */
  i=0; while (i < pcxt->n_inputs) {
    /* Freed by fd_decref */
    /* free(pcxt->input[i].spelling); */
    fd_decref(pcxt->input[i].lstr);
    fd_decref(pcxt->input[i].compounds);
    i++;}
  /* Free internal state variables */
  free(pcxt->input); free(pcxt->states); 
  pcxt->input=NULL; pcxt->states=NULL;
  u8_free(pcxt->buf); pcxt->start=NULL; pcxt->end=NULL;
  /* Free the state/input cache. */
  i=0; while (i < pcxt->max_n_inputs) free(pcxt->cache[i++]);
  free(pcxt->cache); pcxt->cache=NULL;
}

static void grow_table
  (void **table,unsigned int *max_size,
   unsigned int delta,unsigned int elt_size)
{
  void *new=realloc(*table,elt_size*(*max_size+delta));
  if (new == NULL) exit(1);
  *table=new; *max_size=*max_size+delta;
}

static void grow_inputs(fd_parse_context pc)
{
  int i=pc->max_n_inputs;
  grow_table((void **)(&(pc->input)),&(pc->max_n_inputs),
	     FD_INITIAL_N_INPUTS,sizeof(struct FD_WORD));
  pc->cache=
    realloc(pc->cache,sizeof(fd_parse_state *)*pc->max_n_inputs);
  while (i < pc->max_n_inputs) {
    fd_parse_state *vec=u8_malloc(sizeof(fd_parse_state)*pc->grammar->n_nodes);
    unsigned int j=0; while (j < pc->grammar->n_nodes) vec[j++]=-1;
    pc->cache[i++]=vec;}
}


/* OFSM operations */

static void init_ofsm_data(struct FD_GRAMMAR *g,fdtype vector)
{
  fdtype nnodes; unsigned int i=0;
  g->arc_names=fd_incref(FD_VECTOR_REF(vector,0));
  g->n_arcs=FD_VECTOR_LENGTH(g->arc_names);
  nnodes=fd_incref(FD_VECTOR_REF(vector,1));
  g->n_nodes=fd_getint(nnodes);
  /* Get some builtin tags */
  {
    fdtype anything_symbol=fd_intern("ANYTHING");
    fdtype punctuation_symbol=fd_intern("PUNCTUATION");
    fdtype possessive_symbol=fd_intern("POSSESSIVE");
    fdtype arcs=g->arc_names;
    int j=0, lim=g->n_arcs;
    while (j<lim)
      if (FD_EQ(FD_VECTOR_REF(arcs,j),anything_symbol)) 
	g->anything_tag=j++;
      else if (FD_EQ(FD_VECTOR_REF(arcs,j),punctuation_symbol))
	g->punctuation_tag=j++;
      else if (FD_EQ(FD_VECTOR_REF(arcs,j),possessive_symbol))
	g->possessive_tag=j++;
      else if (FD_EQ(FD_VECTOR_REF(arcs,j),sentence_end_symbol))
	g->sentence_end_tag=j++;
      else j++;}
  while (i < g->n_nodes) {
    fdtype spec=fd_incref(FD_VECTOR_REF(vector,i+2)); unsigned int j;
    g->nodes[i].name=FD_VECTOR_REF(spec,0); 
    g->nodes[i].index=i;
    /* ASSERT(i==fd_getint(FD_VECTOR_REF(spec,1))); */
    g->nodes[i].terminal=FD_VECTOR_REF(spec,2); 
    j=0; while (j < g->n_arcs) {
      fdtype arcs=FD_VECTOR_REF(spec,3+j);
      g->nodes[i].arcs[j].n_entries=0;
      if (FD_EMPTY_LISTP(arcs)) j++;
      else {
	int size=fd_getint(FD_CAR(arcs)); fdtype ptr=FD_CDR(arcs);
	g->nodes[i].arcs[j].n_entries=size;
	if (size)
	  g->nodes[i].arcs[j].entries=calloc(size,sizeof(struct FD_OFSM_ARC));
	else g->nodes[i].arcs[j].entries=NULL;
	{int k=0; while (k < size) {
	  fdtype head=FD_CAR(ptr); ptr=FD_CDR(ptr);
	  g->nodes[i].arcs[j].entries[k].measure=fd_getint(FD_CAR(head));
	  g->nodes[i].arcs[j].entries[k].target=
	    &(g->nodes[fd_getint(FD_CDR(head))]); k++;}}
	j++;}}
      i++;}
}


/* Preprocessing text */

static char abbrevs[]=
  "Corp.Calif.Mass.Ariz.Wash.Mich.Kans.Colo.Neva.Penn.Okla.Sept.Gov.Sen.Rep.Dr.Lt.Col.Gen.Mr.Mrs.Miss.Ms.Co.Inc.Ltd.Jan.Feb.Mar.Apr.Jun.Jul.Aug.Sep.Sept.Oct.Nov.Dec.";

#define isnamepart(x) ((isalnum(x) && (isupper(x))) || (x == '&'))
#define issep(c) (!(isalnum(c)  || \
		    (c == '-')  || (c == '_') || \
                    (c == '\'') || (c == '`') || \
                    (c == '.')  || (c == ',') || \
                    (c == '$')  || (c == ':') || \
                    (c == '&')  || (c == '%')))

static int hashset_strget(fd_hashset hs,u8_string data,int i)
{
  struct FD_STRING stringdata;
  FD_INIT_STACK_CONS(&stringdata,fd_string_type);
  stringdata.bytes=data; stringdata.length=i;
  return fd_hashset_get(hs,(fdtype)(&stringdata));
}

/* Takes the word which strings string and checks if it is in
   the set words. */
static int check_word(char *string,fd_hashset words)
{
  char buf[32]; int i=0; char *scan=string;
  while ((*scan) && (!(isspace(*scan))) && (i < 32))
    buf[i++]=tolower(*scan++);
  if (i < 32) {
    buf[i]='\0';
    if (hashset_strget(words,buf,i)) return 1;
    else return 0;}
  else return 0;
}

/* possessivep: (static)
    Arguments: a character string
    Returns: 1 or 0 if string looks like a possessive
*/
static int possessivep(u8_byte *s)
{
  int c=u8_sgetc(&s);
  while (c>=0)
    if ((c=='\'') || (c==0x2019))
      if ((*s=='\0') || (*s=='s') || (*s=='S')) return 1;
      else c=u8_sgetc(&s);
    else c=u8_sgetc(&s);
  return 0;
}


/* Getting root forms */

static fdtype lower_string(u8_string string)
{
  struct U8_OUTPUT ls; u8_string scan=string; int c;
  U8_INIT_OUTPUT(&ls,32);
  while ((c=u8_sgetc(&scan))>=0) {
    u8_putc(&ls,u8_tolower(c));}
  return fd_init_string(NULL,ls.u8_outptr-ls.u8_outbuf,ls.u8_outbuf);
}

static fdtype lower_compound(fdtype compound)
{
  int needs_lower=0;
  FD_DOLIST(word,compound)
    if (FD_STRINGP(word)) {
      u8_string sdata=FD_STRDATA(word);
      int c=u8_sgetc(&sdata);
      if (u8_isupper(c)) {needs_lower=1; break;}}
  if (needs_lower) {
    fdtype head=FD_EMPTY_LIST, *tail=&head, newpair;
    FD_DOLIST(word,compound) {
      fdtype new_elt;
      if (FD_STRINGP(word)) {
	u8_string sdata=FD_STRDATA(word);
	int c=u8_sgetc(&sdata);
	if (u8_isupper(c)) new_elt=tolower(sdata);
	else new_elt=fd_incref(word);}
      else new_elt=fd_incref(word);
      newpair=fd_init_pair(NULL,new_elt,FD_EMPTY_LIST);
      *tail=newpair; tail=&(FD_CDR(newpair));}
    return head;}
  else return fd_incref(compound);
}

/* lexicon_fetch: (static)
    Arguments: a fdtype string
    Returns: Fetches the vector stored in the lexicon for the string.
*/
static fdtype lexicon_fetch(fd_index ix,fdtype key)
{
  return fd_index_get(ix,key);
}

static fdtype lexicon_fetch_lower(fd_index ix,u8_string string)
{
  struct U8_OUTPUT out; int ch; u8_string in=string; fdtype key, value;
  U8_INIT_OUTPUT(&out,128);
  while ((ch=u8_sgetc(&in))>=0)
    if (u8_isupper(ch)) u8_putc(&out,u8_tolower(ch));
    else u8_putc(&out,ch);
  key=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
  value=lexicon_fetch(ix,key); fd_decref(key);
  return value;
}

static fdtype get_noun_root(fd_grammar g,fdtype key) 
{
  fdtype answer=fd_index_get(g->noun_roots,key);
  if (FD_EMPTY_CHOICEP(answer)) return answer;
  else if (FD_STRINGP(answer)) return answer;
  else {
    fdtype result=FD_VOID;
    FD_DO_CHOICES(elt,answer)
      if (FD_VOIDP(answer)) result=elt;
      else if ((FD_STRINGP(elt)) && ((strchr(FD_STRDATA(elt),' ')) == NULL))
	result=elt;
    fd_incref(result); fd_decref(answer);
    if (FD_VOIDP(result))
      return FD_EMPTY_CHOICE;
    else return result;}
}

static fdtype get_verb_root(fd_grammar g,fdtype key) 
{
  fdtype answer=fd_index_get(g->verb_roots,key);
  if (FD_EMPTY_CHOICEP(answer)) return answer;
  else if (FD_STRINGP(answer)) return answer;
  else {
    fdtype result=FD_VOID;
    FD_DO_CHOICES(elt,answer)
      if (FD_VOIDP(answer)) result=elt;
      else if ((FD_STRINGP(elt)) && ((strchr(FD_STRDATA(elt),' ')) == NULL))
	result=elt;
    fd_incref(result); fd_decref(answer);
    if (FD_VOIDP(result))
      return FD_EMPTY_CHOICE;
    else return result;}
}


/* Sentences into words */

static int add_input(fd_parse_context pc,u8_string s,u8_byte *bufp);
static void add_punct(fd_parse_context pc,u8_string s,u8_byte *bufp);

static u8_string process_word(fd_parse_context pc,u8_string input)
{
  struct U8_OUTPUT word_stream; 
  u8_string tmp=input, start=input;
  int ch=u8_sgetc(&input), abbrev=0;
  U8_INIT_OUTPUT(&word_stream,16);
  while (ch>=0) 
    if (u8_isalnum(ch)) { /* Keep going */
      u8_putc(&word_stream,ch); tmp=input; ch=u8_sgetc(&input);}
    else if (u8_isspace(ch)) { /* Unambiguous word terminator */
      if ((word_stream.u8_outptr-word_stream.u8_outbuf) < 40)
	add_input(pc,word_stream.u8_outbuf,start);
      else free(word_stream.u8_outbuf);
      return tmp;}
    else if (ch=='<') {
      while ((ch>=0) && (ch != '>')) ch=u8_sgetc(&input);
      ch=u8_sgetc(&input);}
    else { /* Ambiguous word terminator (punctuation) */
      int next_char=u8_sgetc(&input);
      if (u8_isalnum(next_char)) {
	/* If the next token is alpha numeric, keep it as one word */
	if (ch=='.') abbrev=1;
	u8_putc(&word_stream,ch); u8_putc(&word_stream,next_char);
	tmp=input; ch=u8_sgetc(&input);}
      else if ((ch=='.') &&
	       ((abbrev) ||
		((word_stream.u8_outptr-word_stream.u8_outbuf)==1)) &&
	       (u8_isspace(next_char))) {
	u8_putc(&word_stream,ch);
	/* Go back and just read the period. */
	input=tmp; u8_sgetc(&input);
	if ((word_stream.u8_outptr-word_stream.u8_outbuf) < 40)
	  add_input(pc,word_stream.u8_outbuf,start);
	else free(word_stream.u8_outbuf);
	return input;}
      else { /* Otherwise, record the word you just saw and return
		a pointer to the start of the terminator */
	if ((word_stream.u8_outptr-word_stream.u8_outbuf) < 40)
	  add_input(pc,word_stream.u8_outbuf,start);
	else free(word_stream.u8_outbuf);
	return tmp;}}
  if ((word_stream.u8_outptr-word_stream.u8_outbuf) < 40)
    add_input(pc,word_stream.u8_outbuf,start);
  else free(word_stream.u8_outbuf);
  return tmp;
}

static int ispunct_sorta(int ch)
{
  return
    ((ch>0) && (!((ch=='<') || u8_isalnum(ch) || (u8_isspace(ch)))));
}

static u8_string process_punct(fd_parse_context pc,u8_string input)
{
  struct U8_OUTPUT word_stream; 
  u8_string start=input;
  int ch=u8_sgetc(&input);
  u8_string tmp=input; 
  U8_INIT_OUTPUT(&word_stream,16);
  while ((ch>0) && (ispunct_sorta(ch))) {
    tmp=input; u8_putc(&word_stream,ch); ch=u8_sgetc(&input);}
  add_punct(pc,word_stream.u8_outbuf,start);
  /* if (word_stream.size > 5)  
     else free(word_stream.ptr); */
  return tmp;
}

static u8_string skip_whitespace(u8_string input)
{
  if (input==NULL) return input;
  else {
    u8_string tmp=input; int ch=u8_sgetc(&input);
    while ((ch>=0) && (u8_isspace(ch))) {
      tmp=input; ch=u8_sgetc(&input);}
    return tmp;}
}

static u8_string skip_markup(u8_string input)
{
  if (input==NULL) return input;
  else {
    u8_string tmp=input; int ch=u8_sgetc(&input);
    if (ch<0) return NULL;
    else if (ch=='<')
      if (strncmp(input,"!--",3)==0) {
	u8_string next=strstr(input,"-->");
	if (next) {
	  input=next+3;}
	else return NULL;}
      else {
	while ((ch>=0) && (ch!='>')) {
	  tmp=input; ch=u8_sgetc(&input);}
	input=tmp;}
    else return input;
    if (input==NULL) return input;
    else if (*input) return skip_markup(input);
    else return NULL;}
}

static u8_string skip_wordbreak(u8_string input,int xml)
{
  u8_string point=input;
  if (input==NULL) return input;
  else while (point) {
    int ch=u8_string_ref(point);
    if (ch<=0) return point;
    else if ((xml) && (ch=='<')) point=skip_markup(point);
    else if (u8_isspace(ch)) point=skip_whitespace(point);
    else return point;}
  return point;
}

static void lexer(fd_parse_context pc,u8_string start,u8_string end)
{
  u8_string scan=start;
  int ch=u8_sgetc(&scan), skip_markup=(pc->flags&FD_TAGGER_SKIP_MARKUP), len;
  if (pc->n_inputs>0) fd_reset_parse_context(pc);
  pc->start=start; pc->end=end;
  scan=start; while ((ch>=0) && (scan<end)) {
    u8_string tmp;
    if (u8_isspace(ch))
      scan=skip_wordbreak(scan,skip_markup);
    else if ((skip_markup) && (ch=='<'))
      scan=skip_wordbreak(scan,skip_markup);
    else if (u8_isalnum(ch)) scan=process_word(pc,scan);
    else scan=process_punct(pc,scan);
    if (scan==NULL) break;
    tmp=scan; ch=u8_sgetc(&scan); scan=tmp;}
  pc->start=end;
}


/* Identifying compounds */

static fdtype make_compound(fd_parse_context pc,int start,int end,int lower)
{
  fdtype elts[16], compound=FD_EMPTY_LIST, lcompound=FD_EMPTY_LIST, lexentry;
  int i=0, lim=end-start; while (i<lim) {
    int fc=u8_string_ref(pc->input[start+i].spelling);
    if ((lower) && (u8_isupper(fc)))
      elts[i]=lower_string(pc->input[start+i].spelling);
    else elts[i]=fd_incref(pc->input[start+i].lstr);
    i++;}
  i--; while (i>=0)
    compound=fd_init_pair(NULL,elts[i--],compound);
  return compound;
}

static fdtype probe_compound
  (fd_parse_context pc,int start,int end,int lim,int lower)
{
  if (end>lim) return FD_EMPTY_CHOICE;
  else {
    fdtype compound=make_compound(pc,start,end,lower);
    fdtype lexdata=lexicon_fetch(pc->grammar->lexicon,compound);
    if (FD_EMPTY_CHOICEP(lexdata)) {
      fd_decref(compound); return FD_EMPTY_CHOICE;}
    else {
      if (FD_VECTORP(lexdata)) {
	fdtype results=fd_init_pair(NULL,compound,lexdata);
	fdtype more_results=probe_compound(pc,start,end+1,lim,lower);
	FD_ADD_TO_CHOICE(results,more_results);
	return results;}
      else if (FD_FALSEP(lexdata)) {
	fd_decref(compound);
	return probe_compound(pc,start,end+1,lim,lower);}
      else {
	fd_decref(lexdata);
	fd_decref(compound);
	return FD_EMPTY_CHOICE;}}}
}

static void bump_weights_for_capitalization(fd_parse_context pc,int word);

static void identify_compounds(fd_parse_context pc)
{
  int strange_capitalization=(pc->flags&FD_TAGGER_ALLCAPS);
  int start=1, i=0, lim=pc->n_inputs-1;
  while (i < lim) {
    fdtype compounds=FD_EMPTY_CHOICE, tmp;
    u8_string scan=pc->input[i].spelling;
    int fc=u8_sgetc(&scan), c2=u8_sgetc(&scan);
    if (strchr(".;!?:\"'`.",fc)) {
      start=1; i++; continue;}
    tmp=probe_compound(pc,i,i+1,pc->n_inputs,0);
    FD_ADD_TO_CHOICE(compounds,tmp);
    if ((u8_isupper(fc)) &&
	(start|strange_capitalization|u8_isupper(c2))) {
      fdtype lowered=lower_string(pc->input[i].spelling);
      fdtype lexdata=lexicon_fetch(pc->grammar->lexicon,lowered);
      if (FD_EMPTY_CHOICEP(lexdata)) {fd_decref(lowered);}
      else {
	fdtype entry=fd_init_pair(NULL,fd_make_list(1,lowered),lexdata);
	bump_weights_for_capitalization(pc,i);
	FD_ADD_TO_CHOICE(compounds,entry);}
      tmp=probe_compound(pc,i,i+1,pc->n_inputs,1);
      FD_ADD_TO_CHOICE(compounds,tmp);}
    FD_ADD_TO_CHOICE(pc->input[i].compounds,compounds);
    start=0;
    i++;}
}


/* Simple text conversion */

FD_EXPORT
void fd_parser_set_text(struct FD_PARSE_CONTEXT *pcxt,u8_string in)
{
  u8_string scan=in; int c;
  if (pcxt->n_inputs>0) fd_reset_parse_context(pcxt);
  pcxt->n_calls++;
  pcxt->buf=pcxt->start=u8_strdup(in);
  pcxt->end=pcxt->buf+u8_strlen(pcxt->buf);
}


/* Strings into sentences */

static int atspace(u8_string s)
{
  int c=u8_sgetc(&s);
  return u8_isspace(c);
}

static int atupper(u8_string s)
{
  int c=u8_sgetc(&s);
  return u8_isupper(c);
}

static int atalnum(u8_string s)
{
  int c=u8_sgetc(&s);
  return u8_isalnum(c);
}

static u8_string find_sentence_end(u8_string string)
{
  u8_string start=string; int in_upper=atupper(string);
  if (string == NULL) return NULL;
  else if (*string == '\0') return NULL;
  else if (*string == '<')
    if (strncmp(string,"<!--",4)==0) {
      u8_string next=strstr(string,"-->");
      if (next) string=next+3; else return NULL;}
    else {
      int c=u8_sgetc(&string); while ((c>0) && (c!='>'))
	c=u8_sgetc(&string);
      start=string;}
  else string++;
  /* Go till you get to a possible terminator */
  while (*string)
    if (*string=='<')
      if ((string[1]=='!') && (string[2]=='-') && (string[3]=='-')) {
	u8_string next=strstr(string,"-->");
	if (next) string=next+3; else return NULL;}
      else if (string[1]=='/')
	if (((string[2]=='P') || (string[2]=='p')) && (atspace(string+2)))
	  return string;
	else if (((string[2]=='H') || (string[2]=='h')) && (isdigit(string[3])))
	  return string;
	else if ((strncasecmp(string,"</div",5)==0) &&
		 ((string[5]=='>') || (isspace(string[5]))))
	  return string;
	else if ((strncasecmp(string,"</dd",4)==0)  &&
		 ((string[4]=='>') || (isspace(string[4]))))
	  return string;
	else while ((*string) && (*string != '>')) string++;
      else if (((string[1]=='P') || (string[1]=='p')) && (atspace(string+2)))
	return string;
      else if (((string[1]=='H') || (string[1]=='h')) && (isdigit(string[2])))
	return string;
      else if ((strncasecmp(string,"<div",4)==0) &&
	       ((string[4]=='>') || (isspace(string[4]))))
	return string;
      else if ((strncasecmp(string,"<dd",3)==0)  &&
	       ((string[3]=='>') || (isspace(string[3]))))
	return string;
      else while ((*string) && (*string != '>')) string++;
    else if (*string=='\n') {
      u8_string scan=string+1;
      while ((*scan==' ') || (*scan == '\t')) scan++;
      if (*scan=='\n') return scan+1;
      else string=scan;}
    else if (*string == '.')
      if (string[1]=='\0') return string+1;
      else if (string[1]=='<') return string+1;
      else if (atalnum(string+1)) string++;
      else if (in_upper) string++;
      else return string+1;
    else if (strchr("?!;:",*string))
      return string+1;
    else if ((*string<0x80) && (isspace(*string)))
      in_upper=atupper(++string);
    else string++;
  return string;
}


/* Lexicon access */

/* add_input: (static)
    Arguments: a parse context and a word string
    Returns: Adds the string to the end of the current sentence.
*/
static int add_input(fd_parse_context pc,u8_string spelling,u8_byte *bufp)
{
  u8_string s=strdup(spelling);
  int first_char=u8_sgetc(&spelling);
  int capitalized=0, capitalized_in_lexicon=0, i, slen, ends_in_s=0;
  struct FD_GRAMMAR *g=pc->grammar; fd_index lex=g->lexicon;
  fdtype ls=fd_init_string(NULL,-1,s), key, value=lexicon_fetch(lex,ls);
  if ((word_limit > 0) && (((int)pc->n_inputs) >= word_limit))
    return fd_reterr(TooManyWords,"add_input",NULL,FD_VOID);
  if (pc->n_inputs+4 >= pc->max_n_inputs) grow_inputs(pc);
  slen=FD_STRLEN(ls);
  if ((s[slen-1]=='s') || (s[slen-1]=='S')) ends_in_s=1;
  /* Initialize this word's weights */
  i=0; while (i < FD_MAX_ARCS) pc->input[pc->n_inputs].weights[i++]=255;
  if (!((FD_VECTORP(value)) || (FD_PACKETP(value)))) {
    fd_decref(value); value=FD_EMPTY_CHOICE;}
  if (u8_isupper(first_char))
    capitalized=1;
  if (!(FD_EMPTY_CHOICEP(value))) capitalized_in_lexicon=1;
  /* Here are the default rules for getting POS data */
  else if (possessivep(spelling))
    if (capitalized) {
      value=lexicon_fetch(lex,sproper_possessive); 
      capitalized_in_lexicon=1;}
    else value=lexicon_fetch(lex,spossessive);
  else if (isdigit(first_char))
    if (strchr(spelling,'-')) value=lexicon_fetch(lex,sscore);
    else value=lexicon_fetch(lex,snumber);
  else if ((first_char == '$') && (u8_isdigit(u8_sgetc(&spelling))))
    value=lexicon_fetch(lex,sdollars);
  else if (capitalized) 
    value=lexicon_fetch(lex,sproper_name);
  else if ((slen>2) && (strcmp(s+(slen-2),"ed")==0))
    value=lexicon_fetch(lex,ed_word);
  else if ((slen>2) && (strcmp(s+(slen-2),"ly")==0))
    value=lexicon_fetch(lex,ly_word);
  else if ((slen>3) && (strcmp(s+(slen-3),"ing")==0))
    value=lexicon_fetch(lex,ing_word);
  else if (ends_in_s)
    if (strchr(spelling,'-'))
      value=lexicon_fetch(lex,sdashed_word);
    else value=lexicon_fetch(lex,sdashed_sword);
  else if (strchr(spelling,'-'))
    value=lexicon_fetch(lex,sdashed_sword);  
  else value=lexicon_fetch(lex,sstrange_word);
  if (FD_PAIRP(value)) value=FD_CAR(value);
  if (!((FD_VECTORP(value)) || (FD_PACKETP(value))))
    value=lexicon_fetch(lex,sstrange_word);
  if (FD_VECTORP(value)) {
    i=0; while (i < pc->grammar->n_arcs) {
      fdtype wt=FD_VECTOR_REF(value,i);
      if (FD_FIXNUMP(wt))
	pc->input[pc->n_inputs].weights[i++]=FD_FIX2INT(wt);
      else if (FD_TRUEP(wt))
	pc->input[pc->n_inputs].weights[i++]=0;
      else pc->input[pc->n_inputs].weights[i++]=255;}}
  else if (FD_PACKETP(value)) {
    memcpy(pc->input[pc->n_inputs].weights,
	   FD_PACKET_DATA(value),
	   pc->grammar->n_arcs);}
  pc->input[pc->n_inputs].spelling=s;
  pc->input[pc->n_inputs].bufptr=bufp;
  pc->input[pc->n_inputs].lstr=ls;
  pc->input[pc->n_inputs].compounds=FD_EMPTY_CHOICE;  
  pc->input[pc->n_inputs].cap=capitalized_in_lexicon; 
  pc->input[pc->n_inputs].next=pc->n_inputs+1;
  if ((capitalized) && (!(capitalized_in_lexicon)) &&
      (pc->n_inputs>0)) {
    int i=0; while (i < pc->grammar->n_arcs)
      if ((1) && /* ?? use retrieved info */
	  (pc->input[pc->n_inputs].weights[i]==255)) {
	pc->input[pc->n_inputs].weights[i]=4; i++;}
      else i++;}
  if (!(u8_isalnum(first_char)))
    pc->input[pc->n_inputs].weights[pc->grammar->punctuation_tag]=1;
  pc->n_inputs++;
  return 1;
}

static void bump_weights_for_capitalization(fd_parse_context pc,int word)
{
  /* If it was capitalized in the lexicon, don't bump it for
     capitalization. */
  if (pc->input[word].cap) return;
  /* Otherwise, bump each of the weights by 1 to bias towards
     a proper name assumption. */
  int i=0; while (i < pc->grammar->n_arcs) {
    unsigned char *weights=pc->input[word].weights;
    if (weights[i]==255) i++;
    else {weights[i]=weights[i]+1; i++;}}
}


/* add_punct: (static)
   Arguments: a parse context and a punctuation string
   Returns: Adds the string to the end of the current sentence.
*/
static void add_punct(fd_parse_context pc,u8_string spelling,u8_byte *bufptr)
{
  u8_string s=strdup(spelling); int i;
  fdtype ls=fd_init_string(NULL,-1,s), key, value;
  value=lexicon_fetch(pc->grammar->lexicon,ls);
  if (pc->n_inputs+4 >= pc->max_n_inputs) grow_inputs(pc);
  if (FD_EMPTY_CHOICEP(value))
    value=lexicon_fetch(pc->grammar->lexicon,punctuation_symbol);
  if (FD_PAIRP(value)) value=FD_CAR(value);
  if (!((FD_VECTORP(value)) || (FD_PACKETP(value))))
    value=lexicon_fetch(pc->grammar->lexicon,sstrange_word);
  if (FD_VECTORP(value)) {
    i=0; while (i < pc->grammar->n_arcs) {
      fdtype wt=FD_VECTOR_REF(value,i);
      if (FD_FIXNUMP(wt))
	pc->input[pc->n_inputs].weights[i++]=FD_FIX2INT(wt);
      else if (FD_TRUEP(wt))
	pc->input[pc->n_inputs].weights[i++]=0;
      else pc->input[pc->n_inputs].weights[i++]=255;}}
  else {
    memcpy(pc->input[pc->n_inputs].weights,
	   FD_PACKET_DATA(value),
	   pc->grammar->n_arcs);}
  pc->input[pc->n_inputs].spelling=s;
  pc->input[pc->n_inputs].bufptr=bufptr;
  pc->input[pc->n_inputs].lstr=ls;
  pc->input[pc->n_inputs].compounds=FD_EMPTY_CHOICE;  
  pc->input[pc->n_inputs].next=pc->n_inputs+1;
  pc->n_inputs++;
}


/* Queue operations */

/* print_state: (static)
    Arguments: an operation identifer (a string), a state, and a FILE
    Returns: nothing
  Prints out a representation of the transition into the state.
*/
static void print_state(fd_parse_context pc,char *op,fd_parse_state sr,FILE *f)
{
  struct FD_PARSER_STATE *s=&(pc->states[sr]);
  u8_message("%q ==> \n%s d=%d i=%d(%q) t=%q n=%q\n",
	     FD_VECTOR_REF(pc->grammar->arc_names,s->arc),
	     op,s->distance,s->input,s->word,
	     s->node->terminal,
	     s->node->name); 
}

/* check_queue_integrity: (static)
     Arguments: none
     Return value: none
   Checks that the queue of states doesn't have any cycles,
  signalling an excpection if it does.
*/ 
static void check_queue_integrity(fd_parse_context pc)
{
  return;
#if 0
  struct FD_PARSER_STATE *s; unsigned int i=0; s=pc->queue;
  while ((i < pc->n_states*2) && (s)) {i++; s=s->qnext;};
  if (s)
    printf("Circularity\n");
#endif
}

/* queue_push: (static)
    Arguments: a state
    Returns: nothing
  Adds the state to the current queue.
*/
static void queue_push(fd_parse_context pc,fd_parse_state new)
{
#if TRACING
  if (tracing_tagger) print_state(pc,"Pushing",new,stderr);
#endif
  if (pc->queue == -1) pc->queue=new;
  else {
    fd_parse_state *last=&(pc->queue), next=pc->queue, prev=-1;
    struct FD_PARSER_STATE *elt=&((pc->states)[next]), *s=&((pc->states)[new]);
    int i=0, distance=s->distance;
    while ((next >= 0) && (distance > elt->distance)) {
      last=&(elt->qnext); prev=next; next=elt->qnext;
      elt=&((pc->states)[next]); i++;}
    if (new == next) fprintf(stderr,"Whoops");
    s->qnext=next; if (next >= 0) elt->qprev=new;
    s->qprev=prev; *last=new;}
#if DEBUGGING
  check_queue_integrity(pc);
#endif
}

/* queue_reorder: (static)
    Arguments: a state
    Returns: nothing
  Reorders the queue to move a given state further up
*/

static void queue_reorder(fd_parse_context pc,fd_parse_state changed)
{
  if (pc->queue == changed)  return;
  else if (changed < 0) return; /* Should never be reached */
  else {
    struct FD_PARSER_STATE *s=&(pc->states[changed]);
    (pc->states[s->qprev]).qnext=s->qnext;
    if (s->qnext >= 0)
      (pc->states[s->qnext]).qprev=s->qprev;}
  queue_push(pc,changed);
}


/* Expanding the queue */

/* add_state: (static)
    Arguments: a node, a distance, an input index, an origin state, and an arc
    Returns: nothing
  Adds a state to the state space for a particular node and input, 
   at a particular distance, and coming via some arc from some origin state.
*/
static void add_state
  (fd_parse_context pc, struct FD_OFSM_NODE *n,int distance,int in,
   fd_parse_state origin,int arc,fdtype word)
{
  fd_parse_state kref=(pc->cache[in])[n->index];
  if (kref < 0) {
    struct FD_PARSER_STATE *new;
    if (pc->n_states >= pc->max_n_states)
      grow_table((void **)(&pc->states),&pc->max_n_states,
		 FD_INITIAL_N_STATES,sizeof(struct FD_PARSER_STATE));
    new=&(pc->states[pc->n_states]);
    new->node=n; new->distance=distance; new->input=in;
    new->previous=origin; new->arc=arc; new->word=word;
    new->self=pc->n_states; new->qnext=-1; new->qprev=-1;
    (pc->cache[in])[n->index]=pc->n_states;
    pc->n_states++;
    queue_push(pc,new->self);}
  else {
    struct FD_PARSER_STATE *known=&(pc->states[kref]);
    if (distance < known->distance) {
      known->previous=origin; known->arc=arc;
      known->distance=distance; known->word=word;
      queue_reorder(pc,kref);}}
}

static int quote_stringp(u8_string s)
{
  if ((*s<0x80) && (isalnum(*s))) return 0;
  else {
    int c=u8_sgetc(&s);
    while (c>=0)
      if (u8_isalnum(c)) return 0;
      else if ((c=='"') || (c=='\'') || (c=='`') ||
	       (c==0xAB) || (c==0xBB) ||
	       ((c>=0x2018) && (c<=0x201F)) ||
	       (c==0x2039) || (c==0x203A))
	return 1;
      else return 0;}
}

static void expand_state_on_word
  (fd_parse_context pc,fd_parse_state sr,struct FD_WORD *wd)
{
  struct FD_PARSER_STATE *s=&(pc->states[sr]);
  struct FD_OFSM_NODE *n=s->node;
  unsigned int i=1, in=s->input, dist=s->distance;
  while (i < pc->grammar->n_arcs)
    if ((wd->weights[i] != 255) && (n->arcs[i].n_entries > 0)) {
      int j=0, limit=n->arcs[i].n_entries;
      while (j < limit) {
	add_state
	  (pc,n->arcs[i].entries[j].target,
	   dist+n->arcs[i].entries[j].measure+wd->weights[i],
	   wd->next,sr,i,wd->lstr);
	j++;}
      i++;}
    else i++;
}

static int get_weight(fdtype weights,int i)
{
  if (FD_VECTORP(weights)) {
    fdtype v=FD_VECTOR_REF(weights,i);
    if (FD_FIXNUMP(v)) return FD_FIX2INT(v);
    else return -1;}
  else if (FD_PACKETP(weights)) {
    int v=FD_PACKET_REF(weights,i);
    if (v==255) return -1; else return v;}
  else return -1;
}

static void expand_state_on_compound
  (fd_parse_context pc,fd_parse_state sr,fdtype compound)
{
  struct FD_PARSER_STATE *s=&(pc->states[sr]);
  struct FD_OFSM_NODE *n=s->node;
  unsigned int i=1, in=s->input, dist=s->distance;
  fdtype weights=FD_CDR(compound);
  while (i < pc->grammar->n_arcs) {
    int weight=get_weight(weights,i);
    if ((weight>=0) && (n->arcs[i].n_entries > 0)) {
      int j=0, limit=n->arcs[i].n_entries, len=fd_seq_length(FD_CAR(compound));
      while (j < limit) {
	add_state
	  (pc,n->arcs[i].entries[j].target,
	   dist+n->arcs[i].entries[j].measure+weight,
	   in+len,sr,i,FD_CAR(compound));
	j++;}
      i++;}
    else i++;}
}

/* expand_state: (static)
    Arguments: a state
    Returns: nothing
  Adds new states based on the arcs going out from a state
*/
static void expand_state(fd_parse_context pc,fd_parse_state sr)
{
  struct FD_PARSER_STATE *s=&(pc->states[sr]);
  struct FD_OFSM_NODE *n=s->node;
  unsigned int i=1, in=s->input, dist=s->distance;
#if TRACING
  if (tracing_tagger) print_state(pc,"Expanding",sr,stderr);
#endif
  if (in < pc->n_inputs) {
    expand_state_on_word(pc,sr,((struct FD_WORD *)&(pc->input[in])));
    if (!(FD_EMPTY_CHOICEP(pc->input[in].compounds))) {
      FD_DO_CHOICES(compound,pc->input[in].compounds) {
	expand_state_on_compound(pc,sr,compound);}}
    /* Try skipping this input */
    if (quote_stringp(pc->input[in].spelling))
      add_state(pc,n,dist,in+1,sr,pc->grammar->punctuation_tag,
		pc->input[in].lstr);
    else add_state(pc,n,dist+5,in+1,sr,pc->grammar->anything_tag,
		   pc->input[in].lstr);}
  /* Do the epsilon expansion */
  if (n->arcs[0].n_entries > 0) {
    int j=0, limit=n->arcs[0].n_entries;
    while (j < limit) {
      add_state(pc,n->arcs[0].entries[j].target,
		dist+n->arcs[0].entries[j].measure,
		in,sr,0,FD_EMPTY_CHOICE); j++;}}
}

/* The search engine */

/* queue_extend: (static)
     Arguments: none
     Returns: a state
   Gets and expands the top state in the queue, returning
    NULL if the state is a terminal state.
*/
static fd_parse_state queue_extend(fd_parse_context pc)
{
  fd_parse_state tref=pc->queue; 
  struct FD_PARSER_STATE *top;
  if (tref >= 0) {
    top=&(pc->states[tref]); pc->last=tref; pc->queue=top->qnext;}
  else return pc->last;
  if ((top->input == pc->n_inputs) && (!(ZEROP(top->node->terminal))))
    return tref;
  else if (top->input > pc->n_inputs) return -1;
  else {expand_state(pc,tref); return -1;}
}

FD_EXPORT
/* fd_run_parser: 
     Arguments: none
     Returns: a state
   Repeatedly extends the queue until a terminal state is reached,
    returning that state.
*/
fd_parse_state fd_run_parser(fd_parse_context pc)
{
  fd_parse_state answer;
  double start=u8_elapsed_time();
  while ((answer=queue_extend(pc)) < 0);
  pc->runtime=pc->runtime+(u8_elapsed_time()-start);
  if ((answer >= 0) &&
      ((pc->states[answer]).input == pc->n_inputs) && 
      (!(ZEROP((pc->states[answer]).node->terminal)))) 
    return answer;
  else {
    int i=0; fprintf(stderr,"Couldn't parse:");
    while (i < pc->n_inputs)
      fprintf(stderr," %s",pc->input[i++].spelling);
    fprintf(stderr,"\n");
    return -1;}
}


/* Display/converting tag results */

static int possessive_suffixp(fdtype word)
{
  if (FD_STRINGP(word)) {
    u8_string s=FD_STRDATA(word); int len=FD_STRLEN(word);
    if ((len>1) && (s[len-1]=='\'')) return 1;
    else if ((len>2) && (s[len-1]=='s') && (s[len-2]=='\'')) return 1;
    else return 0;}
  else if (FD_PAIRP(word)) {
    fdtype last=word, scan=FD_CDR(word);
    while (FD_PAIRP(scan)) {last=scan; scan=FD_CDR(scan);}
    if (FD_STRINGP(FD_CAR(last)))
      return possessive_suffixp(FD_CAR(last));
    else return 0;}
}

static fdtype possessive_root(fdtype word)
{
  if (FD_STRINGP(word)) {
    u8_string s=FD_STRDATA(word); int len=FD_STRLEN(word);
    if ((len>1) && (s[len-1]=='\''))
      return fd_extract_string(NULL,s,s+(len-1));
    else if ((len>2) && (s[len-1]=='s') && (s[len-2]=='\''))
      return fd_extract_string(NULL,s,s+(len-2));
    else return fd_incref(word);}
  else if (FD_PAIRP(word))
    if (FD_PAIRP(FD_CDR(word)))
      return fd_init_pair(NULL,fd_incref(FD_CAR(word)),
			  possessive_root(FD_CDR(word)));
    else return fd_init_pair(NULL,possessive_root(FD_CAR(word)),FD_EMPTY_LIST);
  else return fd_incref(word);
}

static fdtype get_root(struct FD_GRAMMAR *g,fdtype base,int arcid)
{
  fdtype normalized, result=FD_EMPTY_CHOICE;
  if (g->name_tags[arcid])
    if (possessive_suffixp(base))
      return possessive_root(base);
    else return fd_incref(base);
  else if (FD_STRINGP(base)) {
    u8_string scan=FD_STRDATA(base);
    int firstc=u8_sgetc(&scan);
    if (u8_isupper(firstc)) 
      normalized=lower_string(FD_STRDATA(base));
    else normalized=lower_compound(base);}
  else normalized=fd_incref(base);
  if (g->verb_tags[arcid]) 
    result=get_verb_root(g,normalized);
  else if (g->noun_tags[arcid])
    result=get_noun_root(g,normalized);
  else if (arcid==g->possessive_tag)
    result=possessive_root(normalized);
  if ((FD_EMPTY_CHOICEP(result)) || (FD_VOIDP(result)))
    return normalized;
  else {
    fd_decref(normalized);
    return result;}
}
FD_EXPORT fdtype fd_get_root(struct FD_GRAMMAR *g,fdtype base,int arcid)
{
  return get_root(g,base,arcid);
}

static fdtype word2string(fdtype word)
{
  if (FD_PAIRP(word)) {
    struct U8_OUTPUT out; int i=0; U8_INIT_OUTPUT(&out,32);
    {FD_DOLIST(elt,word) {
      u8_string s=FD_STRDATA(elt); int firstc=u8_sgetc(&s);
      if ((i>0) && (u8_isalnum(firstc))) u8_putc(&out,' '); 
      u8_puts(&out,FD_STRDATA(elt)); i++;}}
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
  else return fd_incref(word);
}

static fdtype make_word_entry(fdtype word,fdtype tag,fdtype root,int distance,fdtype source,int start,int end)
{
  if ((FD_VOIDP(source)) && (start<0))
    return fd_make_vector(4,word,fd_incref(tag),root,
			  FD_USHORT2DTYPE(distance));
  else return fd_make_vector(7,word,fd_incref(tag),root,
			     FD_USHORT2DTYPE(distance),
			     ((FD_VOIDP(source)) ? (FD_FALSE) : (source)),
			     FD_INT2DTYPE(start),FD_INT2DTYPE(end));
}

FD_EXPORT
fdtype fd_gather_tags(fd_parse_context pc,fd_parse_state s)
{
  fdtype answer=FD_EMPTY_LIST, sentence=FD_EMPTY_LIST;
  fdtype arc_names=pc->grammar->arc_names;
  unsigned char *head_tags=pc->grammar->head_tags;
  unsigned char *mod_tags=pc->grammar->mod_tags;
  u8_byte *bufptr=pc->end;
  int glom_phrases=pc->flags&FD_TAGGER_GLOM_PHRASES;
  while (s >= 0) {
    fdtype source=FD_VOID;
    int text_start=-1, text_end=-1;
    struct FD_PARSER_STATE *state=&(pc->states[s]);
    if (state->arc == 0) s=state->previous;
    else if (pc->grammar->head_tags[state->arc]) {
      fdtype word=word2string(state->word);
      fdtype root=get_root(pc->grammar,word,state->arc);
      fdtype rootstring=word2string(root);
      fdtype tag=FD_VECTOR_REF(arc_names,state->arc);
      fdtype glom=FD_VOID, glom_root=FD_VOID, word_entry=FD_VOID;
      fd_parse_state scan=state->previous;
      int glom_caps=capitalizedp(word);
      if (glom_phrases)
	while (scan>=0) {
	  struct FD_PARSER_STATE *nextstate=&(pc->states[scan]);
	  if ((mod_tags[nextstate->arc]) &&
	      ((glom_caps) || (!(capitalizedp(nextstate->word))))) {
	    if (FD_VOIDP(glom)) {
	      glom=fd_make_list(2,fd_incref(nextstate->word),fd_incref(word));
	      glom_root=fd_make_list(2,fd_incref(nextstate->word),rootstring);}
	    else {
	      glom=fd_init_pair(NULL,fd_incref(nextstate->word),glom);
	      glom_root=
		fd_init_pair(NULL,fd_incref(nextstate->word),glom_root);}
	    scan=nextstate->previous;}
	  else if (nextstate->arc==0) scan=nextstate->previous;
	  else break;}
      fd_decref(root);
      if ((pc->flags&FD_TAGGER_INCLUDE_SOURCE) ||
	  (pc->flags&FD_TAGGER_INCLUDE_TEXTRANGE)) {
	struct FD_PARSER_STATE *pstate=&(pc->states[scan]);
	u8_byte *start=pc->input[pstate->input].bufptr;
	if (start==NULL) {}
	else if (bufptr==NULL) {
	  source=fdtype_string(start);
	  text_start=start-pc->buf;
	  text_end=text_start+u8_strlen(start);
	  bufptr=start;}
	else {
	  source=fd_extract_string(NULL,start,bufptr);
	  text_end=bufptr-pc->buf;
	  text_start=start-pc->buf;
	  bufptr=start;}}
      if (FD_VOIDP(glom))
	word_entry=make_word_entry(word,fd_incref(tag),rootstring,
				   state->distance,source,text_start,text_end);
      else {
	word_entry=make_word_entry(glom,fd_incref(tag),glom_root,
				   state->distance,source,text_start,text_end);
	fd_decref(word);}
      sentence=fd_init_pair(NULL,word_entry,sentence);
      s=scan;}
    else {
      fdtype word=word2string(state->word), word_entry=FD_VOID;
      fdtype root=get_root(pc->grammar,word,state->arc);
      fdtype rootstring=word2string(root);
      fdtype tag=FD_VECTOR_REF(pc->grammar->arc_names,state->arc);
      if ((pc->flags&FD_TAGGER_INCLUDE_SOURCE) ||
	  (pc->flags&FD_TAGGER_INCLUDE_TEXTRANGE)) {
	struct FD_PARSER_STATE *pstate=&(pc->states[state->previous]);
	u8_byte *start=pc->input[pstate->input].bufptr;
	if (start==NULL) {}
	else if (bufptr==NULL) {
	  source=fdtype_string(start);
	  text_start=start-pc->buf;
	  text_end=text_start+u8_strlen(start);
	  bufptr=start;}
	else {
	  source=fd_extract_string(NULL,start,bufptr);
	  text_end=bufptr-pc->buf;
	  text_start=start-pc->buf;
	  bufptr=start;}}
      word_entry=
	make_word_entry(word,fd_incref(tag),rootstring,
			state->distance,source,text_start,text_end);
      fd_decref(root);
      if (state->arc==pc->grammar->sentence_end_tag)
	if (FD_EMPTY_LISTP(sentence))
	  sentence=fd_init_pair(NULL,word_entry,sentence);
	else {
	  answer=fd_init_pair(NULL,sentence,answer);
	  sentence=fd_init_pair(NULL,word_entry,FD_EMPTY_LIST);}
      else sentence=fd_init_pair(NULL,word_entry,sentence);
      s=state->previous;}}
  if (FD_EMPTY_LISTP(sentence)) return answer;
  else return fd_init_pair(NULL,sentence,answer);
}


/* Top-level text->fdtype functions */

FD_EXPORT
/* This breaks a string up into sentences (based on some pretty
   simple heuristics) and parses each sentence independently.  It
   also uses the same parse context structure over and over again. */
fdtype fd_analyze_text
 (struct FD_PARSE_CONTEXT *pcxt,u8_string text,
  fdtype (*fn)(fd_parse_context,fd_parse_state,void *),
  void *data)
{
  int skip_markup=(pcxt->flags&FD_TAGGER_SKIP_MARKUP);
  fdtype head=FD_EMPTY_LIST, *tail=&head;
  int free_pcxt=0, n_sentences=0;
  if (pcxt==NULL) {
    struct FD_GRAMMAR *grammar=get_default_grammar();
    if (grammar==NULL)
      return fd_err(NoGrammar,"tagtext_prim",NULL,FD_VOID);
    pcxt=u8_malloc(sizeof(struct FD_PARSE_CONTEXT));
    fd_init_parse_context(pcxt,grammar); 
    free_pcxt=1;}
  fd_parser_set_text(pcxt,text);
  if (pcxt->flags&FD_TAGGER_SPLIT_SENTENCES) {
    u8_byte *sentence=pcxt->start, *sentence_end; int n_calls=0;
    while ((sentence) && (*sentence) &&
	   (sentence_end=find_sentence_end(sentence))) {
      double start_time=u8_elapsed_time();
      fd_parse_state final;
      fdtype retval;
      lexer(pcxt,sentence,sentence_end);
      if (pcxt->n_inputs==0) {
	if (sentence_end) 
	  sentence=skip_whitespace(sentence_end);
	else sentence=sentence_end;
	continue;}
      identify_compounds(pcxt);
      add_state(pcxt,&(pcxt->grammar->nodes[0]),0,0,-1,0,FD_VOID);
      final=fd_run_parser(pcxt);
      pcxt->runtime=pcxt->runtime+(u8_elapsed_time()-start_time);
      retval=fn(pcxt,final,data);
      if (FD_ABORTP(retval)) {
	if (free_pcxt) fd_free_parse_context(pcxt);
	return retval;}
      else {
	n_calls++;
	if (sentence_end) 
	  sentence=skip_whitespace(sentence_end);
	else sentence=sentence_end;}}
    return FD_INT2DTYPE(n_calls);}
  else {
    double start_time=u8_elapsed_time();
    fd_parse_state final;
    fdtype retval;
    u8_string start=pcxt->start;
    lexer(pcxt,pcxt->start,NULL);
    identify_compounds(pcxt);
    add_state(pcxt,&(pcxt->grammar->nodes[0]),0,0,-1,0,FD_VOID);
    final=fd_run_parser(pcxt);
    pcxt->runtime=pcxt->runtime+(u8_elapsed_time()-start_time);
    retval=fn(pcxt,final,data);
    if (FD_ABORTP(retval)) {
      if (free_pcxt) fd_free_parse_context(pcxt);
      return retval;}
    else return FD_INT2DTYPE(1);}
}

/* Sentence tagging */

static fdtype tag_text_helper
  (fd_parse_context pcxt,fd_parse_state s,void *tsdata)
{
  if (s<0)
    return FD_VOID;
  else {
    fdtype *resultptr=(fdtype *)tsdata, result=*resultptr;
    fdtype tags=fd_gather_tags(pcxt,s);
    if (FD_EMPTY_LISTP(result)) *resultptr=tags;
    else {
      fdtype last=result, scan=FD_CDR(result);
      while (FD_PAIRP(scan)) {last=scan; scan=FD_CDR(scan);}
      FD_CDR(last)=tags;}
    return FD_VOID;}
}

FD_EXPORT
/* This breaks a string up into sentences (based on some pretty
   simple heuristics) and parses each sentence independently.  It
   also uses the same parse context structure over and over again. */
fdtype fd_tag_text(struct FD_PARSE_CONTEXT *pcxt,u8_string text)
{
  int free_pcxt=0;
  fdtype result=FD_EMPTY_LIST, retval=FD_VOID;
  if (pcxt==NULL) {
    fd_grammar grammar=get_default_grammar();
    if (grammar==NULL)
      return fd_err(NoGrammar,"tagtext_prim",NULL,FD_VOID);
    free_pcxt=1; pcxt=u8_malloc(sizeof(struct FD_PARSE_CONTEXT));
    fd_init_parse_context(pcxt,grammar);}
  retval=fd_analyze_text(pcxt,text,tag_text_helper,&result);
  if (FD_ABORTP(retval)) {
    if (free_pcxt) {
      fd_free_parse_context(pcxt); u8_free(pcxt);}
    return retval;}
  return result;
}


/* Interpreting parser flags */

static fdtype xml_symbol, plaintext_symbol, glom_symbol, noglom_symbol;
static fdtype allcaps_symbol, whole_symbol, timing_symbol, source_symbol, textpos_symbol;

static interpret_parse_flags(fdtype arg)
{
  int flags=FD_TAGGER_DEFAULT_FLAGS;
  if (fd_overlapp(arg,xml_symbol)) 
    flags=flags|FD_TAGGER_SKIP_MARKUP;
  if (fd_overlapp(arg,plaintext_symbol))
    flags=flags&(~FD_TAGGER_SKIP_MARKUP);
  if (fd_overlapp(arg,glom_symbol))
    flags=flags|FD_TAGGER_GLOM_PHRASES;    
  if (fd_overlapp(arg,allcaps_symbol))
    flags=flags|FD_TAGGER_ALLCAPS;    
  if (fd_overlapp(arg,whole_symbol))
    flags=flags&(~FD_TAGGER_SPLIT_SENTENCES);    
  if (fd_overlapp(arg,whole_symbol))
    flags=flags|FD_TAGGER_VERBOSE_TIMER;    
  if (fd_overlapp(arg,source_symbol))
    flags=flags|FD_TAGGER_INCLUDE_SOURCE;    
  if (fd_overlapp(arg,textpos_symbol))
    flags=flags|FD_TAGGER_INCLUDE_TEXTRANGE;    
  return flags;
}


/* FDScript NLP primitives */

static fdtype tagtext_prim(fdtype input,fdtype flags)
{
  fdtype result=FD_EMPTY_LIST, retval=FD_VOID;
  struct FD_PARSE_CONTEXT parse_context;
  struct FD_GRAMMAR *grammar=get_default_grammar();
  if (grammar==NULL)
    return fd_err(NoGrammar,"tagtext_prim",NULL,FD_VOID);
  /* First do argument checking. */
  if (FD_STRINGP(input)) {}
  else if (FD_PAIRP(input)) {
    FD_DOLIST(elt,input)
      if (!(FD_STRINGP(elt)))
	return fd_type_error(_("text input"),"tagtext_prim",elt);}
  else if (FD_CHOICEP(input)) {
    FD_DO_CHOICES(elt,input)
      if (FD_PAIRP(elt)) {
	FD_DOLIST(sub_elt,elt)
	  if (!(FD_STRINGP(sub_elt)))
	    return fd_type_error(_("text input"),"tagtext_prim",sub_elt);}
      else if (!(FD_STRINGP(elt)))
	return fd_type_error(_("text input"),"tagtext_prim",elt);}
  /* We know the argument is good, so we initialize the parse context. */
  fd_init_parse_context(&parse_context,grammar); 
  /* Now we set the flags from the argument. */
  if (!(FD_VOIDP(flags)))
    parse_context.flags=interpret_parse_flags(flags);
  if (FD_STRINGP(input))
    retval=fd_analyze_text(&parse_context,FD_STRDATA(input),
			   tag_text_helper,&result);
  else if (FD_PAIRP(input)) {
    FD_DOLIST(elt,input) {
      retval=fd_analyze_text(&parse_context,FD_STRDATA(elt),
			     tag_text_helper,&result);
      if (FD_ABORTP(retval)) break;}}
  else if (FD_CHOICEP(input)) {
    FD_DO_CHOICES(each,input) {
      if (FD_STRINGP(each))
	retval=fd_analyze_text
	  (&parse_context,FD_STRDATA(each),tag_text_helper,&result);
      else if (FD_PAIRP(each)) {
	FD_DOLIST(elt,each) {
	  retval=fd_analyze_text
	    (&parse_context,FD_STRDATA(elt),tag_text_helper,&result);
	  if (FD_ABORTP(retval)) break;}}
      if (FD_ABORTP(retval)) break;}}
  fd_free_parse_context(&parse_context);
  if (FD_ABORTP(retval)) return retval;
  else return result;
}

static fdtype tagtextx_prim(fdtype input,fdtype flags)
{
  fdtype result=FD_EMPTY_LIST, retval=FD_VOID;
  struct FD_PARSE_CONTEXT parse_context;
  struct FD_GRAMMAR *grammar=get_default_grammar();
  if (grammar==NULL)
    return fd_err(NoGrammar,"tagtext_prim",NULL,FD_VOID);
  /* First do argument checking. */
  if (FD_STRINGP(input)) {}
  else if (FD_PAIRP(input)) {
    FD_DOLIST(elt,input)
      if (!(FD_STRINGP(elt)))
	return fd_type_error(_("text input"),"tagtext_prim",elt);}
  else if (FD_CHOICEP(input)) {
    FD_DO_CHOICES(elt,input)
      if (FD_PAIRP(elt)) {
	FD_DOLIST(sub_elt,elt)
	  if (!(FD_STRINGP(sub_elt)))
	    return fd_type_error(_("text input"),"tagtext_prim",sub_elt);}
      else if (!(FD_STRINGP(elt)))
	return fd_type_error(_("text input"),"tagtext_prim",elt);}
  /* We know the argument is good, so we initialize the parse context. */
  fd_init_parse_context(&parse_context,grammar); 
  /* Now we set the flags from the argument. */
  if (!(FD_VOIDP(flags)))
    parse_context.flags=interpret_parse_flags(flags);
  if (FD_STRINGP(input))
    retval=fd_analyze_text(&parse_context,FD_STRDATA(input),tag_text_helper,&result);
  else if (FD_PAIRP(input)) {
    FD_DOLIST(elt,input) {
      retval=fd_analyze_text(&parse_context,FD_STRDATA(elt),tag_text_helper,&result);
      if (FD_ABORTP(retval)) break;}}
  else if (FD_CHOICEP(input)) {
    FD_DO_CHOICES(each,input) {
      if (FD_STRINGP(each))
	retval=fd_analyze_text(&parse_context,FD_STRDATA(each),tag_text_helper,&result);
      else if (FD_PAIRP(each)) {
	FD_DOLIST(elt,each) {
	  retval=fd_analyze_text(&parse_context,FD_STRDATA(elt),tag_text_helper,&result);
	  if (FD_ABORTP(retval)) break;}}
      if (FD_ABORTP(retval)) break;}}
  if (FD_ABORTP(retval)) {
    fd_free_parse_context(&parse_context);
    return retval;}
  else result=fd_make_vector
	 (5,FD_INT2DTYPE(parse_context.n_calls),
	  FD_INT2DTYPE(parse_context.n_inputs),
	  FD_INT2DTYPE(parse_context.n_states),
	  fd_init_double(NULL,parse_context.runtime),
	  result);
  fd_free_parse_context(&parse_context);
  return result;
}


/* Lexicon access */

static fdtype lexweight_prim(fdtype string,fdtype tag,fdtype value)
{
  fd_grammar g=fd_default_grammar();
  int i=0, lim=g->n_arcs;
  fdtype arcs=g->arc_names, weights=lexicon_fetch(g->lexicon,string);
  if (FD_EMPTY_CHOICEP(weights)) return FD_EMPTY_CHOICE;
  else if (FD_VOIDP(tag)) {
    fdtype results=FD_EMPTY_CHOICE;
    while (i<lim) {
      int weight=get_weight(weights,i);
      if (weight<0) i++;
      else {
	fdtype pair=fd_init_pair(NULL,fd_incref(FD_VECTOR_REF(arcs,i)),
				  fd_incref(FD_INT2DTYPE(weight)));
	FD_ADD_TO_CHOICE(results,pair); i++;}}
    return results;}
  else {
    while (i<lim)
      if (FD_EQ(tag,FD_VECTOR_REF(arcs,i)))
	if (FD_VOIDP(value)) {
	  int weight=get_weight(weights,i);
	  if (weight==255) return FD_FALSE;
	  else return FD_INT2DTYPE(weight);}
	else {
	  int weight=get_weight(weights,i);
	  if (FD_VECTORP(weights)) {
	    FD_VECTOR_SET(weights,i,fd_incref(value));}
	  else if (FD_PACKETP(weights))
	    if (FD_FALSEP(value)) {
	      FD_PACKET_DATA(weights)[i]=255;}
	    else if ((FD_FIXNUMP(value)) &&
		     (FD_FIX2INT(value)>=0) &&
		     (FD_FIX2INT(value)<128))
	      FD_PACKET_DATA(weights)[i]=FD_FIX2INT(value);
	  if (weight==255) return FD_FALSE;
	  else return FD_INT2DTYPE(weight);}
      else i++;
    return FD_EMPTY_CHOICE;}
}


/* Initialization procedures */

static void init_parser_symbols(void);
static fdtype lisp_report_stats(void);
static fdtype lisp_set_word_limit(fdtype x);

static fdtype configsrc=FD_EMPTY_CHOICE;

static fd_index openindexsource(u8_string base,u8_string component)
{
  int filebase=(strchr(base,'@')==NULL);
  u8_string indexid=
    ((filebase) ? (u8_mkstring("%s/%s",base,component)) :
     (u8_mkstring("%s@%s",component,base)));
  fd_index ix=fd_open_index(indexid);
  u8_free(indexid);
  return ix;
}

FD_EXPORT
struct FD_GRAMMAR *fd_open_grammar(u8_string spec)
{
  fdtype nouns, verbs, names, heads, mods, arc_names;
  struct FD_GRAMMAR *g=u8_malloc(sizeof(struct FD_GRAMMAR));
  g->id=u8_strdup(spec);
  g->lexicon=openindexsource(lexdata_source,"lexicon");
  if (g->lexicon==NULL) 
    return NULL;
  g->verb_roots=openindexsource(lexdata_source,"verb-roots");
  g->noun_roots=openindexsource(lexdata_source,"noun-roots");
  if (strchr(lexdata_source,'@')==NULL) {
    g->grammar=fd_index_get(g->lexicon,grammar_symbol);
    init_ofsm_data(g,g->grammar);
    arc_names=g->arc_names;}
  names=fd_index_get(g->lexicon,names_symbol);
  nouns=fd_index_get(g->lexicon,nouns_symbol);
  verbs=fd_index_get(g->lexicon,verbs_symbol);
  heads=fd_index_get(g->lexicon,heads_symbol);
  mods=fd_index_get(g->lexicon,mods_symbol);
  {
    int len=g->n_arcs;
    unsigned char *taginfo=u8_malloc(sizeof(unsigned char)*len);
    fdtype *data=FD_VECTOR_DATA(g->arc_names);
    int i=0; while (i<len) 
      if (fd_overlapp(data[i],names)) taginfo[i++]=1; else taginfo[i++]=0;
    g->name_tags=taginfo;
    fd_decref(names);}
  {
    int len=g->n_arcs;
    unsigned char *taginfo=u8_malloc(sizeof(unsigned char)*len);
    fdtype *data=FD_VECTOR_DATA(g->arc_names);
    int i=0; while (i<len) 
      if (fd_overlapp(data[i],nouns)) taginfo[i++]=1; else taginfo[i++]=0;
    g->noun_tags=taginfo;
    fd_decref(nouns);}
  {
    int len=g->n_arcs;
    unsigned char *taginfo=u8_malloc(sizeof(unsigned char)*len);
    fdtype *data=FD_VECTOR_DATA(g->arc_names);
    int i=0; while (i<len) 
      if (fd_overlapp(data[i],heads)) taginfo[i++]=1; else taginfo[i++]=0;
    g->head_tags=taginfo;
    fd_decref(heads);}
  {
    int len=g->n_arcs;
    unsigned char *taginfo=u8_malloc(sizeof(unsigned char)*len);
    fdtype *data=FD_VECTOR_DATA(g->arc_names);
    int i=0; while (i<len) 
      if (fd_overlapp(data[i],mods)) taginfo[i++]=1; else taginfo[i++]=0;
    g->mod_tags=taginfo;
    fd_decref(mods);}
  {
    int len=g->n_arcs;
    unsigned char *taginfo=u8_malloc(sizeof(unsigned char)*len);
    fdtype *data=FD_VECTOR_DATA(g->arc_names);
    int i=0; while (i<len) 
      if (fd_overlapp(data[i],verbs)) taginfo[i++]=1; else taginfo[i++]=0;
    g->verb_tags=taginfo;
    fd_decref(verbs);}
  return g;
}

static fdtype lisp_trace_tagger(fdtype flag)
{
  if (FD_FALSEP(flag)) tracing_tagger=0;
  else tracing_tagger=1;
  return FD_VOID;
}

/* fd_set_word_limit:
     Arguments: an int
     Return value: none
  Sets the parser's word limit.  -1 turns limitations off.
*/
FD_EXPORT void fd_set_word_limit(int l)
{
  word_limit=l;
}

static fdtype lisp_set_word_limit(fdtype x)
{
  int olimit=word_limit;
  if (FD_FIXNUMP(x)) word_limit=FD_FIX2INT(x);
  else word_limit=-1;
  return FD_INT2DTYPE(olimit);
}

FD_EXPORT
/* fd_trace_tagger:
      Arguments: none
      Returns: nothing
  Turns on tracing in the tagger */
void fd_trace_tagger()
{
  tracing_tagger=1;
}

static fdtype lisp_get_stats()
{
  return fd_make_vector(4,
			FD_INT2DTYPE(total_states),
			FD_INT2DTYPE(total_inputs),
			FD_INT2DTYPE(total_sentences),
			fd_init_double(NULL,total_parse_time));
}

static fdtype lisp_report_stats()
{
  if (total_parse_time < 120.0)
    u8_message("Processed %d words, %d sentences, %d states in %f seconds\n",
	       total_inputs,total_sentences,total_states,total_parse_time);
  else u8_message
	 ("Processed %d words, %d sentences, %d states in %f minutes\n",
	  total_inputs,total_sentences,total_states,total_parse_time/60.0);
  if (total_parse_time > 0.0) 
    u8_message
      ("Averaged %f words per minute, max of %d words, %d states.",
       (((double) total_inputs)/(total_parse_time/60.0)),
       max_inputs,max_states);
  return FD_TRUE;
}

/* Initialization */

/* Initializes symbols used in the parser. */
static void init_parser_symbols()
{
  ing_word=fd_intern("%ING-WORD");
  ly_word=fd_intern("%LY-WORD");
  ed_word=fd_intern("%ED-WORD");
  punctuation_symbol=fd_intern("%PUNCTUATION");
  sstrange_word=fd_intern("%STRANGE-WORD");
  sproper_name=fd_intern("%PROPER-NAME");
  sdashed_word=fd_intern("%DASHED-WORD");
  sdashed_sword=fd_intern("%DASHED-SWORD");
  stime_ref=fd_intern("%TIME-WORD");
  sscore=fd_intern("%SCORE");
  snumber=fd_intern("%NUMBER");
  sdollars=fd_intern("%DOLLARS");
  spossessive=fd_intern("%POSSESSIVE");
  sproper_possessive=fd_intern("%PROPER-POSSESSIVE");

  lexget_symbol=fd_intern("LEXGET");
  verb_root_symbol=fd_intern("VERB-ROOT");
  noun_root_symbol=fd_intern("NOUN-ROOT");
  names_symbol=fd_intern("%NAMES");
  nouns_symbol=fd_intern("%NOUNS");
  verbs_symbol=fd_intern("%VERBS");
  grammar_symbol=fd_intern("%GRAMMAR");
  heads_symbol=fd_intern("%HEADS");
  mods_symbol=fd_intern("%MODS");

  noun_symbol=fd_intern("NOUN");
  verb_symbol=fd_intern("VERB");
  name_symbol=fd_intern("NAME");
  compound_symbol=fd_intern("COMPOUND");

  xml_symbol=fd_intern("XML");
  plaintext_symbol=fd_intern("PLAINTEXT");
  glom_symbol=fd_intern("GLOM");
  noglom_symbol=fd_intern("NOGLOM");
  allcaps_symbol=fd_intern("ALLCAPS");
  whole_symbol=fd_intern("WHOLE");
  timing_symbol=fd_intern("TIMING");
  source_symbol=fd_intern("SOURCE");
  textpos_symbol=fd_intern("TEXTPOS");
			  
  sentence_end_symbol=fd_intern("SENTENCE-END");
  parse_failed_symbol=fd_intern("NOPARSE");
}

/* fd_init_tagger_c: 
      Arguments: none
      Returns: nothing
*/
FD_EXPORT
void fd_init_tagger_c()
{
  fdtype menv=fd_new_module("TEXTTOOLS",(FD_MODULE_SAFE));
  init_parser_symbols();

#if FD_THREADS_ENABLED
  u8_init_mutex(&default_grammar_lock);
  u8_init_mutex(&parser_stats_lock);
#endif

  /* This are ndprims because the flags arguments may be a set. */
  fd_idefn(menv,fd_make_ndprim(fd_make_cprim2("TAGTEXT",tagtext_prim,1)));
  fd_idefn(menv,fd_make_ndprim(fd_make_cprim2("TAGTEXT*",tagtextx_prim,1)));

  fd_idefn(menv,fd_make_cprim3("LEXWEIGHT",lexweight_prim,1));

  fd_idefn(menv,fd_make_cprim0("NLP-STATS",lisp_get_stats,0));
  fd_idefn(menv,fd_make_cprim0("REPORT-NLP-STATS",lisp_report_stats,0));
  fd_idefn(menv,fd_make_cprim1("SET-WORD-LIMIT!",lisp_set_word_limit,1));
  fd_idefn(menv,fd_make_cprim1("TRACE-TAGGER!",lisp_trace_tagger,1));
  
  fd_register_config("LEXDATA",config_get_lexdata,config_set_lexdata,NULL);
}

/* The CVS log for this file
   $Log: tagger.c,v $
   Revision 1.55  2006/02/07 03:14:03  haase
   Fix buffer growing bug with punctuation

   Revision 1.54  2006/01/26 19:02:32  haase
   Initialize contents of pointer arg to u8_parse_entity

   Revision 1.53  2006/01/25 18:18:08  haase
   Simplified some parsing code by using u8_parse_entity

   Revision 1.52  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.51  2005/11/06 01:11:50  haase
   Fixed bug in proper name glomming with lowercase name elements

   Revision 1.50  2005/09/11 19:43:58  haase
   Fixed glomming to include epsilon transitions

   Revision 1.49  2005/08/21 01:56:04  haase
   Signal no grammar error

   Revision 1.48  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.47  2005/07/18 19:46:02  haase
   Made tagger able to use POS data stored in packets as well as vectors

   Revision 1.46  2005/07/16 19:00:08  haase
   Extended the sentence segmentation algorithm and made segmenter smarter about strings with no words

   Revision 1.45  2005/07/09 02:31:40  haase
   Fixed capitalization bumping and added LEXWEIGHT primitive

   Revision 1.44  2005/07/08 20:19:26  haase
   Various XML parsing fixes

   Revision 1.43  2005/07/07 18:21:13  haase
   Fixes to tagxtract

   Revision 1.42  2005/07/06 20:28:08  haase
   Fixed weight bumping bug for capitalized lexicon entries

   Revision 1.41  2005/06/30 14:36:40  haase
   Fixed bug in sentence-end handling in the tag extractor

   Revision 1.40  2005/06/30 02:58:00  haase
   Fixes to pseudo morphology

   Revision 1.39  2005/06/27 17:03:28  haase
   Fixed unlocked access to total_inputs statistic

   Revision 1.38  2005/06/25 16:25:47  haase
   Added threading primitives

   Revision 1.37  2005/06/24 16:44:47  haase
   Fixed VOID root bug

   Revision 1.36  2005/06/23 15:11:33  haase
   Kludged glomming to be case consistent as a proxy for more complex glomming rules

   Revision 1.35  2005/06/22 22:51:25  haase
   Fixed initialization gc bug

   Revision 1.34  2005/06/21 23:50:53  haase
   Various parser fixes

   Revision 1.33  2005/06/21 17:52:15  haase
   Reorganized tagger to use parse context flags and its own string buffer.

   Revision 1.32  2005/06/21 16:01:23  haase
   Added flags argument to tagging primitives

   Revision 1.31  2005/06/21 15:28:29  haase
   Better parse context cleanup

   Revision 1.30  2005/06/20 15:49:39  haase
   Improved tagger's handling of possessives

   Revision 1.29  2005/06/19 02:51:14  haase
   Added ->bits field to parse contexts and fixed handling of compound glomming

   Revision 1.28  2005/06/19 02:38:07  haase
   Added initial pass at phrase glomming

   Revision 1.27  2005/06/18 02:58:00  haase
   Fixes to tagxtract

   Revision 1.26  2005/06/17 18:14:21  haase
   Removed dead code and fixed parse stats reporting and access

   Revision 1.25  2005/06/17 16:37:20  haase
   Only try downcasing for starts and post puncts

   Revision 1.24  2005/06/16 22:51:20  haase
   Improved built-in morphology routines for English

   Revision 1.23  2005/06/16 02:36:43  haase
   Added tag extraction functions

   Revision 1.22  2005/06/15 15:00:35  haase
   Fixed to handle new fragment conventions

   Revision 1.21  2005/06/15 02:00:09  haase
   Added name tags to tagger

   Revision 1.20  2005/06/15 01:19:45  haase
   Added log entries to texttools files

*/
