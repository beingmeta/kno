/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* ofsm.c
   Copyright (C) 2001-2016 beingmeta, inc.
   This is the main OFSM (optimizing finite state machine) engine.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/dtypestream.h"
#include "framerd/sequences.h"
#include "framerd/numbers.h"
#include "framerd/support.h"

#include "tagger.h"

#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>
#include <libu8/u8ctype.h>
#include <libu8/u8stdio.h>

#include <ctype.h>

#include <time.h>
#include <stdlib.h>
#include <string.h>
#if defined(WIN32)
#include <windows.h>
#endif

#define ZEROP(x) ((x)==(FD_SHORT2DTYPE(0)))

static fd_exception TooManyWords=_("Too many words");
static fd_exception ParseFailed=_("Parser failed");
static fd_exception LateConfiguration=_("LEXDATA has already been configured");
static fd_exception NoGrammar=_("No grammar identified");

fd_ptr_type fd_tagger_type;


/* Miscellaneous defines */

#define DEBUGGING 1
#define TRACING 1

#ifndef PATH_MAX
#define PATH_MAX 1023
#endif

#define next_input(pc,i) ((pc->input)[i].next)
#define prev_input(pc,i) ((pc->input)[i].previous)

#define isquote(c) \
   ((c == '"') || (c == '\'') || (c == '`'))

static int capwindow=5;

static int capitalizedp(fdtype x)
{
  if (FD_STRINGP(x)) {
    u8_string s=FD_STRDATA(x);
    int i=0, c=u8_sgetc(&s);
    while ((i<capwindow) && (c>0))
      if (u8_isupper(c)) return 1;
      else {c=u8_sgetc(&s); i++;}
    return 0;}
  else if ((FD_VECTORP(x)) && (FD_VECTOR_LENGTH(x)>0))
    return capitalizedp(FD_VECTOR_REF(x,0));
  else if (FD_PAIRP(x))
    return capitalizedp(FD_CAR(x));
  else return 0;
}

static int string_capitalizedp(u8_string s)
{
  int i=0, c=u8_sgetc(&s);
  while ((i<capwindow) && (c>0))
    if (u8_isupper(c)) return 1;
    else {c=u8_sgetc(&s); i++;}
  return 0;
}


/* Global declarations */

static int word_limit=-1;
static int trace_tagger=0;
static int trace_load_grammar=0;

#if FD_THREADS_ENABLED
static u8_mutex parser_stats_lock;
#endif

static int max_states=0, total_states=0;
static int max_inputs=0;
static int total_inputs=0, total_sentences=0;
static double total_parse_time=0.0;

/* Declarations for lexicon access */

static fdtype noun_symbol, verb_symbol, name_symbol, compound_symbol;
static fdtype lexget_symbol, verb_root_symbol, noun_root_symbol;
static fdtype nouns_symbol, verbs_symbol, names_symbol, grammar_symbol;
static fdtype heads_symbol, mods_symbol, prefix_symbol;

/* These simple syntactic categories are used when words aren't
   in the lexicon. */
static fdtype sstrange_word, sproper_name, sdashed_word, sdashed_sword;
static fdtype ed_word, ing_word, s_word, ly_word, punctuation_symbol, stime_ref;
static fdtype sscore, snumber, sdollars, spossessive, sproper_possessive;
static fdtype sentence_end_symbol, sxproper_name, sxproper_possessive;

static fdtype parse_failed_symbol;

static int quote_mark_tag=-1, noise_tag=-1, prefix_tag=1;

/* Default grammar */

static u8_string lexdata_source=NULL;
static struct FD_GRAMMAR *default_grammar=NULL;

static u8_mutex default_grammar_lock;

static struct FD_GRAMMAR *get_default_grammar()
{
  fd_lock_mutex(&default_grammar_lock);
  if (default_grammar) {
    fd_unlock_mutex(&default_grammar_lock);
    return default_grammar;}
  else if (lexdata_source==NULL) {
    fd_unlock_mutex(&default_grammar_lock);
    return NULL;}
  else {
    default_grammar=fd_open_grammar(lexdata_source);
    fd_unlock_mutex(&default_grammar_lock);
    return default_grammar;}
}

FD_EXPORT
struct FD_GRAMMAR *fd_default_grammar()
{
  return get_default_grammar();
}

static fdtype config_get_lexdata(fdtype var,void MAYBE_UNUSED *data)
{
  if (lexdata_source)
    return fdtype_string(lexdata_source);
  else return FD_EMPTY_CHOICE;
}
static int config_set_lexdata(fdtype var,fdtype val,void MAYBE_UNUSED *data)
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

static fdtype config_get_lexicon(fdtype var,void MAYBE_UNUSED *data)
{
  struct FD_GRAMMAR *g=fd_default_grammar();
  if (g==NULL) return FD_FALSE;
  else {
    fdtype lp=fd_index2lisp(g->lexicon);
    return lp;}
}
static int config_set_lexicon(fdtype var,fdtype val,void MAYBE_UNUSED *data)
{
  struct FD_GRAMMAR *g=fd_default_grammar();
  if (g==NULL) {
    fd_seterr("No LEXDATA to modify lexicon","config_set_lexicon",NULL,FD_VOID);
    return -1;}
  else if ((FD_PRIM_TYPEP(val,fd_index_type))||
           (FD_PRIM_TYPEP(val,fd_raw_index_type))) {
    fd_index ix=fd_lisp2index(val);
    if (ix==NULL) {
      fd_seterr(fd_TypeError,"config_set_lexicon",
                u8_strdup("index ref"),fd_incref(val));
      return -1;}
    else if (ix==g->lexicon) return 0;
    else {
      fdtype cur=(fdtype)(g->lexicon);
      g->lexicon=ix; fd_incref(val); fd_decref(cur);
      return 1;}}
  else if (FD_STRINGP(val)) {
    fd_index ix=fd_open_index(FD_STRDATA(val));
    if (ix==NULL) {
      fd_seterr(fd_TypeError,"config_set_lexicon",
                u8_strdup("index ref"),fd_incref(val));
      return -1;}
    else if (ix==g->lexicon) return 0;
    else {
      fdtype cur=(fdtype)(g->lexicon);
      g->lexicon=ix; fd_incref(val); fd_decref(cur);
      return 1;}}
  else {
    fd_seterr(fd_TypeError,"config_set_lexicon",
              u8_strdup("index ref"),fd_incref(val));
    return -1;}
}


/* Parse Contexts */

FD_EXPORT
fd_parse_context fd_init_parse_context(fd_parse_context pc,struct FD_GRAMMAR *grammar)
{
  unsigned int i=0;
  memset(pc,0,sizeof(struct FD_PARSE_CONTEXT));
  pc->grammar=grammar; pc->flags=FD_TAGGER_DEFAULT_FLAGS;
  /* Initialize inter-parse fields. */
  pc->n_calls=0; pc->cumulative_inputs=0;
  pc->cumulative_states=0; pc->cumulative_runtime=0.0;
  pc->custom_lexicon=NULL;
  /* Initialize parsing data */
  pc->input=u8_malloc(sizeof(struct FD_WORD)*FD_INITIAL_N_INPUTS);
  pc->n_inputs=0; pc->max_n_inputs=FD_INITIAL_N_INPUTS;
  pc->states=u8_malloc(sizeof(struct FD_PARSER_STATE)*FD_INITIAL_N_STATES);
  pc->n_states=0; pc->max_n_states=FD_INITIAL_N_STATES;
  pc->queue=-1; pc->last=-1; pc->runtime=0.0;
  pc->cache=u8_malloc(sizeof(fd_parse_state *)*FD_INITIAL_N_INPUTS);
  while (i < FD_INITIAL_N_INPUTS) {
    fd_parse_state *vec=u8_malloc(sizeof(fd_parse_state)*pc->grammar->n_nodes);
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
  fd_lock_mutex(&parser_stats_lock);
  total_states=total_states+pcxt->n_states;
  total_inputs=total_inputs+pcxt->n_inputs;
  total_parse_time=total_parse_time+pcxt->runtime;
  total_sentences++;
  if (pcxt->n_states > max_states) max_states=pcxt->n_states;
  if (pcxt->n_inputs > max_inputs) max_inputs=pcxt->n_inputs;
  fd_unlock_mutex(&parser_stats_lock);
  /* Report timing info if requested. */
  if (pcxt->flags&FD_TAGGER_VERBOSE_TIMER)
    u8_log(LOG_DEBUG,"DoneParse","Parsed %d inputs, exploring %d states in %f seconds",
           pcxt->n_inputs,pcxt->n_states,pcxt->runtime);
  /* Update stats for this parse context. */
  pcxt->cumulative_inputs=pcxt->cumulative_inputs+pcxt->n_inputs;
  pcxt->cumulative_states=pcxt->cumulative_states+pcxt->n_states;
  pcxt->cumulative_runtime=pcxt->cumulative_runtime+pcxt->runtime;
  /* Free input data from the last parse. */
  i=0; while (i < pcxt->n_inputs) {
    fd_decref(pcxt->input[i].lstr);
    pcxt->input[i].lstr=FD_VOID;
    fd_decref(pcxt->input[i].alternates);
    pcxt->input[i].alternates=FD_EMPTY_CHOICE;
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
  fd_lock_mutex(&parser_stats_lock);
  total_states=total_states+pcxt->n_states;
  total_inputs=total_inputs+pcxt->n_inputs;
  total_parse_time=total_parse_time+pcxt->runtime;
  total_sentences++;
  if (pcxt->n_states > max_states) max_states=pcxt->n_states;
  if (pcxt->n_inputs > max_inputs) max_inputs=pcxt->n_inputs;
  fd_unlock_mutex(&parser_stats_lock);
  /* Report timing info if requested. */
  if (pcxt->flags&FD_TAGGER_VERBOSE_TIMER)
    u8_log(LOG_DEBUG,"DoneParse",
           "Parsed %d inputs, exploring %d states in %f seconds",
           pcxt->n_inputs,pcxt->n_states,pcxt->runtime);
  /* Free memory structures for inputs */
  i=0; while (i < pcxt->n_inputs) {
    /* Freed by fd_decref */
    /* free(pcxt->input[i].spelling); */
    fd_decref(pcxt->input[i].lstr);
    fd_decref(pcxt->input[i].alternates);
    i++;}
  /* Free internal state variables */
  free(pcxt->input); free(pcxt->states);
  pcxt->input=NULL; pcxt->states=NULL;
  u8_free(pcxt->buf); pcxt->start=NULL; pcxt->end=NULL;
  if (pcxt->custom_lexicon) {
    fd_decref((fdtype)pcxt->custom_lexicon);
    pcxt->custom_lexicon=NULL;}
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
    fd_parse_state *vec=u8_alloc_n(pc->grammar->n_nodes,fd_parse_state);
    unsigned int j=0; while (j < pc->grammar->n_nodes) vec[j++]=-1;
    pc->cache[i++]=vec;}
}


/* OFSM operations */

static void init_ofsm_data(struct FD_GRAMMAR *g,fdtype vector)
{
  fdtype nnodes; unsigned int i=0;
  fdtype arcnames=g->arc_names=fd_incref(FD_VECTOR_REF(vector,0));
  g->n_arcs=FD_VECTOR_LENGTH(g->arc_names);
  nnodes=fd_incref(FD_VECTOR_REF(vector,1));
  g->n_nodes=fd_getint(nnodes);
  /* Get some builtin tags */
  {
    fdtype anything_symbol=fd_intern("ANYTHING");
    fdtype punctuation_symbol=fd_intern("PUNCTUATION");
    fdtype possessive_symbol=fd_intern("POSSESSIVE");
    fdtype quote_mark_symbol=fd_intern("QUOTE-MARK");
    fdtype noise_symbol=fd_intern("NOISE");
    int j=0, lim=g->n_arcs;
    while (j<lim)
      if (FD_EQ(FD_VECTOR_REF(arcnames,j),anything_symbol))
        g->anything_tag=j++;
      else if (FD_EQ(FD_VECTOR_REF(arcnames,j),punctuation_symbol))
        g->punctuation_tag=j++;
      else if (FD_EQ(FD_VECTOR_REF(arcnames,j),possessive_symbol))
        g->possessive_tag=j++;
      else if (FD_EQ(FD_VECTOR_REF(arcnames,j),sentence_end_symbol))
        g->sentence_end_tag=j++;
    /* These should really be on the grammar as a noise[] vector. */
      else if (FD_EQ(FD_VECTOR_REF(arcnames,j),quote_mark_symbol))
        quote_mark_tag=j++;
      else if (FD_EQ(FD_VECTOR_REF(arcnames,j),noise_symbol))
        noise_tag=j++;
      else j++;}
  while (i < g->n_nodes) {
    fdtype spec=fd_incref(FD_VECTOR_REF(vector,i+2)); unsigned int j;
    g->nodes[i].name=FD_VECTOR_REF(spec,0);
    g->nodes[i].index=i;
    /* ASSERT(i==fd_getint(FD_VECTOR_REF(spec,1))); */
    g->nodes[i].terminal=FD_VECTOR_REF(spec,2);
    if (trace_load_grammar)
      u8_message("Read %q (#%d) terminal=%q\n",
                 g->nodes[i].name,g->nodes[i].index,
                 g->nodes[i].terminal);
    j=0; while (j < g->n_arcs) {
      fdtype arcs=FD_VECTOR_REF(spec,3+j);
      g->nodes[i].arcs[j].n_entries=0;
      if (FD_EMPTY_LISTP(arcs)) j++;
      else {
        int size=fd_getint(FD_CAR(arcs)); fdtype ptr=FD_CDR(arcs);
        if (trace_load_grammar)
          u8_message("  on %q, %d expansions:\n",
                     FD_VECTOR_REF(arcnames,j),size);
        g->nodes[i].arcs[j].n_entries=size;
        if (size)
          g->nodes[i].arcs[j].entries=calloc(size,sizeof(struct FD_OFSM_ARC));
        else g->nodes[i].arcs[j].entries=NULL;
        {int k=0; while (k < size) {
            fdtype head=FD_CAR(ptr); ptr=FD_CDR(ptr);
            int target=fd_getint(FD_CDR(head));
            int measure=fd_getint(FD_CAR(head));
            struct FD_OFSM_NODE *node=&(g->nodes[target]);
            g->nodes[i].arcs[j].entries[k].measure=measure;
            g->nodes[i].arcs[j].entries[k].target=&(g->nodes[target]);
            if (trace_load_grammar) {
              if (FD_NULLP(node->name))
                u8_fprintf(stderr,"     to #%d, measure=%d\n",target,measure);
              else u8_fprintf(stderr,"     to %q (#%d), measure=%d\n",
                              node->name,target,measure);}
            k++;}}
        j++;}}
      i++;}
}


/* Preprocessing text */

#define isnamepart(x) ((isalnum(x) && (isupper(x))) || (x == '&'))
#define issep(c) (!(isalnum(c)  || \
                    (c == '-')  || (c == '_') || \
                    (c == '\'') || (c == '`') || \
                    (c == '.')  || (c == ',') || \
                    (c == '$')  || (c == ':') || \
                    (c == '&')  || (c == '%')))

/* possessivep: (static)
    Arguments: a character string
    Returns: 1 or 0 if string looks like a possessive
*/
static int possessivep(const u8_byte *s)
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
  if (FD_VECTORP(compound))  {
    fdtype *elts=FD_VECTOR_DATA(compound);
    int needs_lower=0, i=0, n=FD_VECTOR_LENGTH(compound);
    while (i<n)
      if (FD_STRINGP(elts[i])) {
        u8_string sdata=FD_STRDATA(elts[i]);
        int c=u8_sgetc(&sdata);
        if (u8_isupper(c)) {needs_lower=1; break;}
        else i++;}
      else {
        u8_log(LOG_WARN,"Bad Compound phrase","element is not a string",
               compound);
        return fd_incref(compound);}
    if (needs_lower) {
      fdtype result=fd_init_vector(NULL,n,NULL);
      fdtype *newelts=FD_VECTOR_ELTS(result);
      i=0; while (i<n) {
        fdtype elt=elts[i], new_elt=FD_VOID;
        if (FD_STRINGP(elt)) {
          u8_string sdata=FD_STRDATA(elt);
          int c=u8_sgetc(&sdata);
          if (u8_isupper(c)) new_elt=lower_string(sdata);
          else new_elt=fd_incref(elt);}
        else new_elt=fd_incref(elt);
        newelts[i]=new_elt;
        i++;}
      return result;}
    else return fd_incref(compound);}
  else if (FD_PAIRP(compound)) {
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
          if (u8_isupper(c)) new_elt=lower_string(sdata);
          else new_elt=fd_incref(word);}
        else new_elt=fd_incref(word);
        newpair=fd_conspair(new_elt,FD_EMPTY_LIST);
        *tail=newpair; tail=&(FD_CDR(newpair));}
      return head;}
    else return fd_incref(compound);}
  else return fd_incref(compound);
}

/* lexicon_fetch: (static)
    Arguments: a fdtype string
    Returns: Fetches the vector stored in the lexicon for the string.
*/
FD_INLINE_FCN fdtype lexicon_fetch(fd_index ix,fdtype key)
{
  return fd_index_get(ix,key);
}

FD_INLINE_FCN fdtype lexicon_fetch_lower(fd_index ix,u8_string string)
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

static fdtype convert_custom(struct FD_GRAMMAR *g,fdtype custom)
{
  unsigned char *data=u8_malloc(g->n_arcs);
  int i=0, lim=g->n_arcs;
  while (i<lim) {
    fdtype arc_name=FD_VECTOR_REF(g->arc_names,i);
    fdtype val=fd_get(custom,arc_name,FD_VOID);
    if (FD_TRUEP(val)) data[i]=0;
    else if ((FD_FIXNUMP(val)) && (FD_FIX2INT(val)>=0) && (FD_FIX2INT(val)<128))
      data[i]=FD_FIX2INT(val);
    else data[i]=255;
    i++;}
  fd_decref(custom);
  return fd_init_packet(NULL,g->n_arcs,data);
}

static fdtype get_lexinfo(fd_parse_context pcxt,fdtype key)
{
  fdtype custom=(((pcxt)&&(pcxt->custom_lexicon)) ?
                 (fd_hashtable_get(pcxt->custom_lexicon,key,FD_VOID)) :
                 (FD_VOID));
  if (FD_VOIDP(custom)) {
    if (pcxt)
      return lexicon_fetch(pcxt->grammar->lexicon,key);
    else {
      struct FD_GRAMMAR *grammar=get_default_grammar();
      return lexicon_fetch(grammar->lexicon,key);}}
  else if (FD_PACKETP(custom)) return custom;
  else if (FD_FALSEP(custom)) return custom;
  else {
    if (pcxt)
      return convert_custom(pcxt->grammar,custom);
    else {
      struct FD_GRAMMAR *grammar=get_default_grammar();
      return convert_custom(grammar,custom);}}
}

static fdtype get_noun_root(fd_parse_context pcxt,fdtype key)
{
  if (pcxt->custom_lexicon) {
    struct FD_PAIR tmp_pair;
    fdtype tmp_key=(fdtype)(&tmp_pair), custom_root;
    FD_INIT_STATIC_CONS(&tmp_pair,fd_pair_type);
    tmp_pair.car=noun_root_symbol; tmp_pair.cdr=key;
    custom_root=fd_hashtable_get(pcxt->custom_lexicon,tmp_key,FD_VOID);
    if (FD_VOIDP(custom_root))
      return fd_index_get(pcxt->grammar->noun_roots,key);
    else return custom_root;}
  else return fd_index_get(pcxt->grammar->noun_roots,key);
}

static fdtype get_verb_root(fd_parse_context pcxt,fdtype key)
{
  if (pcxt->custom_lexicon) {
    struct FD_PAIR tmp_pair;
    fdtype tmp_key=(fdtype)(&tmp_pair), custom_root;
    FD_INIT_STATIC_CONS(&tmp_pair,fd_pair_type);
    tmp_pair.car=verb_root_symbol; tmp_pair.cdr=key;
    custom_root=fd_hashtable_get(pcxt->custom_lexicon,tmp_key,FD_VOID);
    if (FD_VOIDP(custom_root))
      return fd_index_get(pcxt->grammar->verb_roots,key);
    else return custom_root;}
  else return fd_index_get(pcxt->grammar->verb_roots,key);
}

static int possessive_namep(fd_parse_context pc,u8_string string)
{
  struct U8_OUTPUT ls; u8_string scan=string; int c;
  fdtype key, data;
  U8_INIT_OUTPUT(&ls,32);
  while ((c=u8_sgetc(&scan))>=0) {
    if (c=='\'') break;
    u8_putc(&ls,u8_tolower(c));}
  key=fd_init_string(NULL,ls.u8_outptr-ls.u8_outbuf,ls.u8_outbuf);
  data=get_lexinfo(pc,key);
  if (FD_VOIDP(data)) {
    fd_decref(key); return 1;}
  else {
    fd_decref(key); fd_decref(data);
    return 0;}
}


/* Sentences into words */

typedef struct MARKUP_TAG {
  const u8_string string; int len;} MARKUP_TAG;

struct MARKUP_TAG whitespace_markup[]={
  {"br",2},
  {"hr",2},
  {"p",1},
  {"/p",2},
  {"div",3},
  {"/div",4},
  {"h1",2},
  {"/h1",3},
  {"h2",2},
  {"/h3",3},
  {"th",2},
  {"/th",3},
  {"td",2},
  {"/td",3},
  {NULL,-1}};

static int add_input(fd_parse_context pc,u8_string s,const u8_byte *bufp);
static void add_punct(fd_parse_context pc,u8_string s,const u8_byte *bufp);

static int is_whitespace_markup(u8_string s)
{
  struct MARKUP_TAG *scan=whitespace_markup;
  while ((scan->string) && (scan->len>=0))
    if (strncasecmp(s,scan->string,scan->len)==0) return 1;
    else scan++;
  return 0;
}

static u8_string process_word(fd_parse_context pc,u8_string input)
{
  struct U8_OUTPUT word_stream;  int xml=(pc->flags&FD_TAGGER_SKIP_MARKUP);
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
    else if ((ch=='<') && (xml)) {
      int wordbreak=is_whitespace_markup(input);
      while ((ch>=0) && (ch != '>')) ch=u8_sgetc(&input);
      if (wordbreak) { /* This will end the word */
        if ((word_stream.u8_outptr-word_stream.u8_outbuf) < 40)
          add_input(pc,word_stream.u8_outbuf,start);
        else free(word_stream.u8_outbuf);
        return tmp;}
      else ch=u8_sgetc(&input);}
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

static u8_string skip_markup(u8_string initial_input)
{
  u8_string input=initial_input;
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
      else if (strncasecmp(input,"script",6)==0) {
        u8_byte *tag_end=strchr(input+6,'>');
        if ((tag_end) && (tag_end[-1]=='/'))
          input=tag_end+1;
        else if ((tag_end) && (strcasestr(input,"src=")) &&
                 (strcasestr(input,"src=")<((char *)tag_end)))
          input=tag_end+1;
        else {
          u8_byte *script_end=strchr(input,'<');
          while ((script_end) && (strncasecmp(script_end,"</script>",9)!=0))
            script_end=strchr(script_end+1,'<');
          if (script_end==NULL) {ch=-1; input=script_end;}
          else input=script_end+9;}}
      else {
        while ((ch>=0) && (ch!='>')) {
          tmp=input; ch=u8_sgetc(&input);}
        input=tmp;}
    else if (ch=='>') return input;
    else return initial_input;
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
  int ch=u8_sgetc(&scan), skip_markup=(pc->flags&FD_TAGGER_SKIP_MARKUP);
  if (pc->n_inputs>0) fd_reset_parse_context(pc);
  if (end==NULL) {end=start+strlen(start);}
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

static int count_chars(u8_string start,u8_string end)
{
  u8_string scan=start;
  int c=u8_sgetc(&scan), n_chars=1;
  while ((c>=0) && (scan<end)) {
    c=u8_sgetc(&scan); n_chars++;}
  return n_chars;
}

static int get_char_pos(fd_parse_context pc,u8_string bufp)
{
  int n=pc->n_inputs-1;
  while (n>=0)
    if ((pc->input[n].char_pos)>0) {
      /* u8_message("Basing cpos for %d on %d",pc->n_inputs,n); */
      return (pc->input[n].char_pos)+
        count_chars((pc->input[n].bufptr),bufp);}
    else n--;
  if (bufp<pc->start) return 0;
  if (bufp>pc->end) return 0;
  else return count_chars(pc->buf,bufp);
}

#if 0
static void add_alt(fd_parse_context pc,int i,fdtype alt)
{
  fdtype phrase;
  if (FD_VECTORP(alt)) {
    phrase=alt; fd_incref(alt);}
  else if (FD_PAIRP(alt)) {
    phrase=alt; fd_incref(alt);}
  else if (FD_STRINGP(alt)) {
    fd_incref(alt);
    phrase=fd_conspair(alt,FD_EMPTY_LIST);}
  FD_ADD_TO_CHOICE(pc->inputs[i].alternates,phrase);
}

static void preparsed(fd_parse_context pc,fdtype words)
{
  if (pc->n_inputs>0) fd_reset_parse_context(pc);
  {
    FD_DOLIST(wd,words) {
      int at=pc->n_inputs, i=0;
      if (FD_PAIRP(alt)) {
        FD_DOLIST(alt,wd) {
          if (i==0) {
            add_input(pc,FD_STRDATA(alt),FD_STRLEN(alt)); i++;}
          else {
            add_alt(pc,at,FD_STRDATA(alt),FD_STRLEN(alt),1);}}}
      else {
        FD_DO_CHOICES(alt,wd) {
          if (i==0) {
            add_input(pc,FD_STRDATA(alt),FD_STRLEN(alt)); i++;}
          else {
            add_alt(pc,at,FD_STRDATA(alt),FD_STRLEN(alt),1);}}}}}
}
#endif


/* Identifying compounds */

static int check_compound(fdtype compound,fd_parse_context pc,int start,
                          int lim,int trylower)
{
  int i=0, len=FD_VECTOR_LENGTH(compound);
  while (i<len) {
    fdtype word=FD_VECTOR_REF(compound,i);
    if ((trylower) ? (strcasecmp(pc->input[start+i].spelling,FD_STRDATA(word))==0) :
        (strcmp(pc->input[start+i].spelling,FD_STRDATA(word))==0))
      i++;
    else return 0;}
  return 1;
}

static fdtype probe_compound
  (fd_parse_context pc,int start,int end,int lim,int lower)
{
  if (end>lim) return FD_EMPTY_CHOICE;
  else if (start+1>=lim)
    return FD_EMPTY_CHOICE;
  else if (pc->input[start].weights[prefix_tag]!=0)
    return FD_EMPTY_CHOICE;
  else {
    fdtype prefix_key=
      fd_conspair(((lower) ? (lower_string(pc->input[start].spelling)) :
                   (fd_incref(pc->input[start].lstr))),
                  ((lower) ? (lower_string(pc->input[start+1].spelling)) :
                   (fd_incref(pc->input[start+1].lstr))));
    fdtype compounds=get_lexinfo(pc,prefix_key);
    if (FD_EMPTY_CHOICEP(compounds)) return compounds;
    else {
      fdtype results=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(compound,compounds)
        if (!(FD_VECTORP(compound))) {}
        else if ((FD_VECTOR_LENGTH(compound))>(lim-start)) {}
        else if (check_compound(compound,pc,start,lim,lower)) {
          fdtype lexinfo=get_lexinfo(pc,compound);
          fdtype lexpair=fd_conspair(fd_incref(compound),lexinfo);
          FD_ADD_TO_CHOICE(results,lexpair);}
        else {}
      fd_decref(compounds); fd_decref(prefix_key);
      return results;}}
}

static void bump_weights_for_capitalization(fd_parse_context,int,int);

static void identify_alternates(fd_parse_context pc)
{
  int oddcaps=(pc->flags&FD_TAGGER_ODDCAPS);
  int allcaps=(pc->flags&FD_TAGGER_ALLCAPS);
  int at_startp=1, i=0, lim=pc->n_inputs-1;
  while (i < lim) {
    fdtype alternates=FD_EMPTY_CHOICE, tmp;
    u8_string scan=pc->input[i].spelling;
    int fc=u8_sgetc(&scan), c2=u8_sgetc(&scan);
    if (strchr(".;!?:\"'`.",fc)) {
      at_startp=1; i++; continue;}
    tmp=probe_compound(pc,i,i+1,pc->n_inputs,0);
    FD_ADD_TO_CHOICE(alternates,tmp);
    if ((u8_isupper(fc)) && (at_startp||oddcaps||u8_isupper(c2))) {
      fdtype lowered=lower_string(pc->input[i].spelling);
      fdtype lexdata=get_lexinfo(pc,lowered);
      if (FD_EMPTY_CHOICEP(lexdata)) {fd_decref(lowered);}
      else {
        fdtype entry=fd_conspair(fd_make_list(1,lowered),lexdata);
        if (((at_startp) || (pc->input[i].cap)) && (!(allcaps)))
          bump_weights_for_capitalization(pc,i,1);
        FD_ADD_TO_CHOICE(alternates,entry);}
      tmp=probe_compound(pc,i,i+1,pc->n_inputs,1);
      FD_ADD_TO_CHOICE(alternates,tmp);}
    FD_ADD_TO_CHOICE(pc->input[i].alternates,alternates);
    at_startp=0;
    i++;}
}


/* Simple text conversion */

FD_EXPORT
void fd_parser_set_text(struct FD_PARSE_CONTEXT *pcxt,u8_string in)
{
  if (pcxt->n_inputs>0) fd_reset_parse_context(pcxt);
  pcxt->n_calls++;
  pcxt->buf=pcxt->start=u8_strdup(in);
  pcxt->end=pcxt->buf+u8_strlen(pcxt->buf);
}


/* Strings into sentences */

struct MARKUP_TAG sentence_break_markup[]={
  {"br",2},
  {"hr",2},
  {"td",2},
  {"/td",3},
  {"th",2},
  {"/th",3},
  {NULL,-1}};

static int tagendp(u8_string s)
{
  int c=u8_sgetc(&s);
  if (c=='>') return 1;
  else if (c=='/') return 1;
  else return u8_isspace(c);
}

FD_FASTOP int markup_is_sentence_breakp(u8_string s)
{
  struct MARKUP_TAG *scan=sentence_break_markup;
  while ((scan->string) && (scan->len>=0))
    if (strncasecmp(s,scan->string,scan->len)==0)
      return tagendp(s+scan->len);
    else scan++;
  return 0;
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
  int in_upper=atupper(string);
  if (string == NULL) return NULL;
  else if (*string == '\0') return NULL;
  else if (*string == '<')
    if (strncmp(string,"<!--",4)==0) {
      u8_string next=strstr(string,"-->");
      if (next) string=next+3; else return NULL;}
    else {
      /* Skip initial markup */
      int c=u8_sgetc(&string);
      while ((c>0) && (c!='>')) c=u8_sgetc(&string);}
  /* Advance a character, use u8_sgetc to handle UTF-8 correctly. */
  else u8_sgetc(&string);
  /* Go till you get to a possible terminator */
  while (*string)
    if (*string=='<')
      if ((string[1]=='!') && (string[2]=='-') && (string[3]=='-')) {
        u8_string next=strstr(string,"-->");
        if (next) string=next+3; else return NULL;}
      else if (string[1]=='/')
        if (((string[2]=='P') || (string[2]=='p')) && (tagendp(string+2)))
          return string;
        else if (((string[2]=='H') || (string[2]=='h')) &&
                 (isdigit(string[3])) && (tagendp(string+3)))
          return string;
        else if ((strncasecmp(string,"</div",5)==0) && (tagendp(string+5)))
          return string;
        else if ((strncasecmp(string,"</dd",4)==0)  && (tagendp(string+4)))
          return string;
        else if (markup_is_sentence_breakp(string+2))
          return string;
        else while ((*string) && (*string != '>')) u8_sgetc(&string);
      else if (((string[1]=='P') || (string[1]=='p')) && (tagendp(string+2)))
        return string;
      else if (((string[1]=='H') || (string[1]=='h')) &&
               (isdigit(string[2])) && (tagendp(string+3)))
        return string;
      else if ((strncasecmp(string,"<div",4)==0) && (tagendp(string+4)))
        return string;
      else if ((strncasecmp(string,"<dd",3)==0)  && (tagendp(string+3)))
        return string;
      else if (strncasecmp(string,"<script",7)==0) {
        u8_byte *tag_end=strchr(string+7,'>');
        if ((tag_end) && (tag_end[-1]=='/'))
          string=tag_end+1;
        else if ((tag_end) && (strcasestr(string,"src=")) &&
                 (strcasestr(string,"src=")<((char *)tag_end)))
          string=tag_end+1;
        else {
          u8_byte *script_end=strchr(string,'<');
          while ((script_end) && (strncasecmp(script_end,"</script>",9)!=0))
            script_end=strchr(script_end+1,'<');
          if (script_end==NULL) {string=script_end;}
          else string=script_end+9;}}
      else if (markup_is_sentence_breakp(string+1))
        return string;
      else while ((*string) && (*string != '>')) u8_sgetc(&string);
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
    else {
      int c=u8_sgetc(&string);
      in_upper=u8_isupper(c);}
  return string;
}


/* Lexicon access */

static fdtype get_lexweights(fd_parse_context pc,u8_string spelling,
                             fdtype ls,fdtype *via);

/* add_input: (static)
    Arguments: a parse context and a word string
    Returns: Adds the string to the end of the current sentence.
*/
static int add_input(fd_parse_context pc,u8_string spelling,
                     const u8_byte *bufp)
{
  u8_string s=strdup(spelling), scan=spelling;
  int first_char=u8_sgetc(&scan);
  int capitalized=string_capitalizedp(spelling), capitalized_in_lexicon=0;
  int i, slen, ends_in_s=0;
  int oddcaps=(pc->flags&FD_TAGGER_ODDCAPS);
  struct FD_GRAMMAR *g=pc->grammar; fd_index lex=g->lexicon;
  fdtype ls=fd_lispstring(s), value=get_lexweights(pc,spelling,ls,NULL);
  if ((word_limit > 0) && (((int)pc->n_inputs) >= word_limit))
    return fd_reterr(TooManyWords,"add_input",NULL,FD_VOID);
  if (pc->n_inputs+4 >= pc->max_n_inputs) grow_inputs(pc);
  slen=FD_STRLEN(ls);
  if ((s[slen-1]=='s') || (s[slen-1]=='S')) ends_in_s=1;
  /* Initialize this word's weights */
  i=0; while (i < FD_MAX_ARCS) pc->input[pc->n_inputs].weights[i++]=255;
  if (FD_PAIRP(value)) value=FD_CAR(value);
  if ((!((FD_VECTORP(value)) || (FD_PACKETP(value)))) ||
      ((FD_VECTORP(value)) && (FD_VECTOR_LENGTH(value)<pc->grammar->n_arcs)) ||
      ((FD_PACKETP(value)) && (FD_PACKET_LENGTH(value)<pc->grammar->n_arcs)))
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
  if ((pc->flags&FD_TAGGER_INCLUDE_SOURCE) ||
      (pc->flags&FD_TAGGER_INCLUDE_TEXTRANGE)) {
    pc->input[pc->n_inputs].char_pos=get_char_pos(pc,bufp);}
  else {
    pc->input[pc->n_inputs].char_pos=0;}
  pc->input[pc->n_inputs].lstr=ls;
  pc->input[pc->n_inputs].alternates=FD_EMPTY_CHOICE;
  pc->input[pc->n_inputs].cap=capitalized_in_lexicon;
  pc->input[pc->n_inputs].next=pc->n_inputs+1;
#if 0
  /* If the term is capitalized but not recognized as a known
     capitalized word, then it might be anything, so take all
     the impossible (w=255) options and make them a little
     possible (w=4).  This may have been more helpful before there
     were alt arcs. */
  if ((capitalized) && (!(capitalized_in_lexicon)) &&
      (pc->n_inputs>0)) {
    int i=0; while (i < pc->grammar->n_arcs)
      if ((1) && /* ?? use retrieved info */
          (pc->input[pc->n_inputs].weights[i]==255)) {
        pc->input[pc->n_inputs].weights[i]=4; i++;}
      else i++;}
#endif
  if (!(u8_isalnum(first_char)))
    pc->input[pc->n_inputs].weights[pc->grammar->punctuation_tag]=1;
  pc->n_inputs++;
  return 1;
}

static fdtype get_lexweights(fd_parse_context pc,u8_string spelling,
                             fdtype ls,fdtype *via)
{
  struct FD_GRAMMAR *g=(pc)?(pc->grammar):(fd_default_grammar());
  fd_index lex=g->lexicon; u8_string scan=spelling;
  int first_char=u8_sgetc(&scan);
  int capitalized=string_capitalizedp(spelling), capitalized_in_lexicon=0;
  int i, slen, ends_in_s=0;
  int oddcaps=(pc)&&(pc->flags&FD_TAGGER_ODDCAPS);
  fdtype value=get_lexinfo(pc,ls);
  u8_string s=FD_STRDATA(ls);
  slen=FD_STRLEN(ls);
  if ((s[slen-1]=='s') || (s[slen-1]=='S')) ends_in_s=1;
  if (!((FD_VECTORP(value)) || (FD_PACKETP(value)))) {
    fd_decref(value); value=FD_EMPTY_CHOICE;}
  if (!(FD_EMPTY_CHOICEP(value))) capitalized_in_lexicon=1;
  /* Here are the default rules for getting POS data */
  else if (possessivep(spelling))
    if (capitalized)
      if (capitalized_in_lexicon) {
        value=lexicon_fetch(lex,sproper_possessive);
        if (via) *via=sproper_possessive;}
      else if (!(oddcaps)) {
        value=lexicon_fetch(lex,sproper_possessive);
        if (via) *via=sproper_possessive;}
      else if (possessive_namep(pc,spelling)) {
        value=lexicon_fetch(lex,sproper_possessive);
        if (via) *via=sproper_possessive;
        capitalized_in_lexicon=1;}
      else {
        value=lexicon_fetch(lex,sxproper_possessive);
        if (via) *via=sxproper_possessive;}
    else {
      value=lexicon_fetch(lex,spossessive);
      if (via) *via=spossessive;}
  else if (u8_isdigit(first_char))
    if (strchr(spelling,'-')) {
      value=lexicon_fetch(lex,sscore);
      if (via) *via=sscore;}
    else {
      value=lexicon_fetch(lex,snumber);
      if (via) *via=snumber;}
  else if ((first_char == '$') && (u8_isdigit(u8_sgetc(&spelling)))) {
    value=lexicon_fetch(lex,sdollars);
    if (via) *via=sdollars;}
  else if (capitalized)
    if (oddcaps) {
      fdtype lowered=lower_string(s);
      fdtype lexdata=get_lexinfo(pc,lowered);
      if (FD_EMPTY_CHOICEP(lexdata)) {
        value=lexicon_fetch(lex,sproper_name);
        if (via) *via=sproper_name;}
      else {
        value=lexicon_fetch(lex,sxproper_name);
        if (via) *via=sxproper_name;}
      fd_decref(lowered); fd_decref(lexdata);}
    else {
      value=lexicon_fetch(lex,sproper_name);
      if (via) *via=sproper_name;}
  else if ((slen>2) && (strcmp(s+(slen-2),"ed")==0)) {
    value=lexicon_fetch(lex,ed_word);
    if (via) *via=ed_word;}
  else if ((slen>2) && (strcmp(s+(slen-2),"ly")==0)) {
    value=lexicon_fetch(lex,ly_word);
    if (via) *via=ly_word;}
  else if ((slen>3) && (strcmp(s+(slen-3),"ing")==0)) {
    value=lexicon_fetch(lex,ing_word);
    if (via) *via=ing_word;}
  else if (ends_in_s)
    if (strchr(spelling,'-')) {
      value=lexicon_fetch(lex,sdashed_word);
      if (via) *via=sdashed_word;}
    else {
      value=lexicon_fetch(lex,sdashed_sword);
      if (via) *via=sdashed_word;}
  else if (strchr(spelling,'-')) {
    value=lexicon_fetch(lex,sdashed_sword);
    if (via) *via=sdashed_word;}
  else {
    value=lexicon_fetch(lex,sstrange_word);
    if (via) *via=sstrange_word;}
  return value;
}

static void bump_weights_for_capitalization(fd_parse_context pc,int word,int by)
{
  /* If it was capitalized in the lexicon, don't bump it for
     capitalization. */
  if (pc->input[word].cap) return;
  /* Otherwise, bump each of the weights by 1 to bias towards
     a proper name assumption. */
  int i=0; while (i < pc->grammar->n_arcs) {
    unsigned char *weights=pc->input[word].weights;
    if (weights[i]==255) i++;
    else {weights[i]=weights[i]+by; i++;}}
}


/* add_punct: (static)
   Arguments: a parse context and a punctuation string
   Returns: Adds the string to the end of the current sentence.
*/
static void add_punct(fd_parse_context pc,u8_string spelling,
                      const u8_byte *bufptr)
{
  u8_string s=strdup(spelling); int i;
  fdtype ls=fd_lispstring(s), value;
  value=get_lexinfo(pc,ls);
  if (pc->n_inputs+4 >= pc->max_n_inputs) grow_inputs(pc);
  if (FD_EMPTY_CHOICEP(value))
    value=get_lexinfo(pc,punctuation_symbol);
  if (FD_PAIRP(value)) value=FD_CAR(value);
  if (!((FD_VECTORP(value)) || (FD_PACKETP(value))))
    value=get_lexinfo(pc,sstrange_word);
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
#if 0
  {
    double start=u8_elapsed_time();
    int cpval=get_char_pos(pc,bufptr);
    pc->input[pc->n_inputs].char_pos=cpval;
    u8_message("punct cpos=%d@%d computed in %f",
               cpval,pc->n_inputs,u8_elapsed_time()-start);}
#else
  if ((pc->flags&FD_TAGGER_INCLUDE_SOURCE) ||
      (pc->flags&FD_TAGGER_INCLUDE_TEXTRANGE)) {
    pc->input[pc->n_inputs].char_pos=get_char_pos(pc,bufptr);}
  else {
    pc->input[pc->n_inputs].char_pos=0;}
#endif
  pc->input[pc->n_inputs].lstr=ls;
  pc->input[pc->n_inputs].alternates=FD_EMPTY_CHOICE;
  pc->input[pc->n_inputs].next=pc->n_inputs+1;
  pc->n_inputs++;
}


/* Queue operations */

static int get_fanout(fd_parse_context pc,struct FD_OFSM_NODE *node);

/* log_state: (static)
    Arguments: an operation identifer (a string), a state, and a FILE
    Returns: nothing
  Prints out a representation of the transition into the state.
*/
static void log_state(fd_parse_context pc,char *op,fd_parse_state sr)
{
  struct FD_WORD *words=pc->input;
  struct FD_PARSER_STATE *states=pc->states;
  struct FD_PARSER_STATE *s=&(states[sr]);
  struct FD_OFSM_NODE *node=s->node;
  struct FD_PARSER_STATE *prev=(s->previous<0)?(NULL):(&(states[s->previous]));
  struct FD_OFSM_NODE *pnode=(prev)?(prev->node):(NULL);
  fdtype arc_names=pc->grammar->arc_names;

  u8_message("%s d=%d %q [s%d.%d@%d] %q(%q) input='%s'(#%d) fanout=%d %s",
             op,s->distance,s->node->name,s->self,node->index,s->input,
             FD_VECTOR_REF(pc->grammar->arc_names,s->arc),s->word,
             words[s->input].spelling,s->input,
             get_fanout(pc,node),
             (node->terminal==FD_FIXZERO)?
             ("non-terminal"):
             ("terminal"));
}

static void show_word_tags(struct U8_OUTPUT *,fdtype arc_names,
                           const unsigned char *weights,int n_arcs);
static void show_lisp_tags(struct U8_OUTPUT *,fdtype arc_names,
                           fdtype weights,int n_arcs);

/* log_state: (static)
    Arguments: an operation identifer (a string), a state, and a FILE
    Returns: nothing
  Prints out a representation of the transition into the state.
*/
static void log_word(fd_parse_context pc,struct FD_WORD *word,int i)
{
  struct U8_OUTPUT out; u8_byte buf[256];
  fdtype arc_names=pc->grammar->arc_names;
  int arc_i=0, n_arcs=pc->grammar->n_arcs;
  U8_INIT_FIXED_OUTPUT(&out,256,buf);
  u8_printf(&out,"'%s' (%d)",word->spelling,i);
  show_word_tags(&out,arc_names,word->weights,n_arcs);
  FD_DO_CHOICES(alternate,word->alternates) {
    if (FD_PAIRP(alternate)) {
      fdtype words=FD_CAR(alternate), weights=FD_CDR(alternate);
      u8_printf(&out,"\n  alternate:");
      if (FD_STRINGP(words)) u8_printf(&out," '%s'",FD_STRDATA(words));
      else if (FD_PAIRP(words)) {
        int started=0;
        FD_DOLIST(word,words) {
          if (FD_STRINGP(word)) {
            if (started) u8_putc(&out,' ');
            else { u8_puts(&out," '"); started=0;}
            u8_printf(&out,"%s",FD_STRDATA(word));}}
        u8_putc(&out,'\'');}
      else {}
      u8_printf(&out,"\n    weights:");
      if (FD_PACKETP(weights))
        show_word_tags(&out,arc_names,FD_PACKET_DATA(weights),n_arcs);
      else show_lisp_tags(&out,arc_names,weights,n_arcs);}}
  u8_message(out.u8_outbuf);
}

static void show_word_tags(struct U8_OUTPUT *out,fdtype arc_names,
                           const unsigned char *weights,int n_arcs)
{
  off_t basepos=out->u8_outptr-out->u8_outbuf;
  int arc_i=0; while (arc_i<n_arcs) {
    int wt=weights[arc_i];
    if (wt!=255) {
      off_t hpos=out->u8_outptr-out->u8_outbuf;
      if (hpos>(basepos+50)) {
        u8_printf(out,"\n\t\t");
        basepos=out->u8_outptr-out->u8_outbuf;}
      u8_printf(out," %q=%d",FD_VECTOR_REF(arc_names,arc_i),wt);}
    arc_i++;}
}

static void show_lisp_tags(struct U8_OUTPUT *out,fdtype arc_names,
                           fdtype weights,int n_arcs)
{
  int len=FD_VECTOR_LENGTH(weights); 
  off_t basepos=out->u8_outptr-out->u8_outbuf;
  int arc_i=0; while ((arc_i<len)&&(arc_i<n_arcs)) {
    fdtype weight=FD_VECTOR_REF(weights,arc_i);
    int wt=((FD_FALSEP(weight))?(255):
            (FD_FIXNUMP(weight))?(FD_FIX2INT(weight)):
            (0));
    if (wt!=255) {
      off_t hpos=out->u8_outptr-out->u8_outbuf;
      if (hpos>(basepos+40)) {
        u8_printf(out,"\n\t\t    ");
        basepos=out->u8_outptr-out->u8_outbuf;}
      u8_printf(out," %q=%d",FD_VECTOR_REF(arc_names,arc_i),wt);}
    arc_i++;}
}

static int get_fanout(struct FD_PARSE_CONTEXT *pc,struct FD_OFSM_NODE *node)
{
  int sum=0; int i=0, n_arcs=pc->grammar->n_arcs;
  struct FD_OFSM_ENTRY *arcs=node->arcs;
  while (i<n_arcs) {
    sum=sum+arcs[i++].n_entries;}
  return sum;
}

/* check_queue_integrity: (static)
     Arguments: none
     Return value: none
   Checks that the queue of states doesn't have any cycles,
  signalling an excpection if it does.
*/
static MAYBE_UNUSED void check_queue_integrity(fd_parse_context pc)
{
  fd_parse_state sref=pc->queue;
  struct FD_PARSER_STATE *states=pc->states;
  struct FD_PARSER_STATE *s=(sref>=0)?&(states[sref]):
    (struct FD_PARSER_STATE *)NULL; 
  unsigned int i=0;
  while ((i < pc->n_states*2) && (s)) {
    fd_parse_state n=s->qnext;
    if (n>=0) s=&states[n]; else s=NULL;
    i++;;}
  if (s) {
    u8_log(LOGCRIT,"QueueIntegrity","Circularity in queue!");
    return;}
  sref=pc->queue; s=(sref>=0)?&states[sref]:(NULL); 
  while (s) {
    fd_parse_state nref=s->qnext;
    struct FD_PARSER_STATE *n=(nref<0)?
      ((struct FD_PARSER_STATE *)NULL):
      &(states[nref]);
    if (n==NULL) break;
    if (s->distance>n->distance) {
      u8_log(LOGCRIT,"QueueIntegrity","Queue integrity failure:");
      log_state(pc,"Before",s->self);
      log_state(pc,"After",n->self);}
    s=n;}
}

/* queue_push: (static)
    Arguments: a state
    Returns: nothing
  Adds the state to the current queue as ordered by distance.
*/
static void queue_push(fd_parse_context pc,fd_parse_state newstate)
{
 if (pc->queue == -1) pc->queue=newstate;
  else {
    fd_parse_state *last=&(pc->queue), next=pc->queue, prev=-1;
    struct FD_PARSER_STATE *insert=&((pc->states)[newstate]);
    struct FD_PARSER_STATE *scan=&((pc->states)[next]);
    int i=0, distance=insert->distance;
    while ((scan) && (distance > scan->distance)) {
      last=&(scan->qnext); prev=next; next=scan->qnext;
      if (next>=0) scan=&((pc->states)[next]);
      else scan=NULL;
      i++;}
    if (newstate == next) fprintf(stderr,"Whoops");
    if (trace_tagger) {
      char namebuf[64];
      sprintf(namebuf,">>> Inserted @%d",i);
      log_state(pc,namebuf,newstate);}
    insert->qprev=prev; insert->qnext=next;
    if (scan) scan->qprev=newstate;
    *last=newstate;
#if DEBUGGING
  check_queue_integrity(pc);
#endif
  }
}

/* queue_reorder: (static)
    Arguments: a state
    Returns: nothing
  Reorders the queue to move a given state further up
*/

static void queue_reorder(fd_parse_context pc,fd_parse_state changed)
{
  if (pc->queue == changed) return;
  else if (changed < 0) return; /* Should never be reached */
  else {
    struct FD_PARSER_STATE *s=&(pc->states[changed]);
    fd_parse_state prev=s->qprev, next=s->qnext;
#if TRACING
    if (trace_tagger)
      log_state(pc,"Reordering",changed);
#endif
    /* Unhook the state from it's current location
       before pushing it back onto the queue */
    struct FD_PARSER_STATE *prevstate=(prev>=0)?(&(pc->states[prev])):(NULL);
    struct FD_PARSER_STATE *nextstate=(next>=0)?(&(pc->states[next])):(NULL);
    if (prevstate==NULL) {
      pc->queue=next;}
    else prevstate->qnext=next;
    if (nextstate) nextstate->qprev=prev;}
  queue_push(pc,changed);
}


/* Expanding the queue */

static fd_parse_state get_state(fd_parse_context pc,
                                struct FD_OFSM_NODE *n,int in,
                                fdtype word,fd_parse_state **alt)
{
  fd_parse_state knownref=(pc->cache[in])[n->index];
  while (knownref>=0) {
    struct FD_PARSER_STATE *known=&(pc->states[knownref]);
    if (FD_EQUAL(word,known->word)) {
      *alt=NULL;
      return knownref;}
    else if (known->alt>=0) {
      knownref=known->alt;
      known=&(pc->states[knownref]);}
    else {
      *alt=&(known->alt);
      return -1;}}
  *alt=&(pc->cache[in])[n->index];
  return -1;
}

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
  fd_parse_state *push;
  fd_parse_state knownref=get_state(pc,n,in,word,&push);
  if (knownref < 0) {
    struct FD_PARSER_STATE *new;
    if (pc->n_states >= pc->max_n_states)
      grow_table((void **)(&pc->states),&pc->max_n_states,
                 FD_INITIAL_N_STATES,sizeof(struct FD_PARSER_STATE));
    new=&(pc->states[pc->n_states]);
    new->node=n; new->distance=distance; new->input=in;
    new->previous=origin; new->arc=arc; new->word=word;
    new->self=pc->n_states; new->qnext=-1; new->qprev=-1;
    new->alt=-1;
    *push=pc->n_states;
    pc->n_states++;
    queue_push(pc,new->self);}
  else {
    struct FD_PARSER_STATE *known=&(pc->states[knownref]);
    if (distance < known->distance) {
      known->previous=origin; known->arc=arc;
      known->distance=distance; known->word=word;
#if TRACING
      if (trace_tagger) log_state(pc,"Reordering",knownref);
#endif
      queue_reorder(pc,knownref);}
    else {
#if TRACING
      if (trace_tagger)
        log_state(pc,"Redundant",knownref);
#endif
    }
  }
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
      else return 0;
    return 0;}
}

static void expand_state_on_word
  (fd_parse_context pc,fd_parse_state sr,struct FD_WORD *wd)
{
  struct FD_PARSER_STATE *s=&(pc->states[sr]);
  struct FD_OFSM_NODE *n=s->node;
  unsigned int i=1, dist=s->distance;
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

static void expand_state_on_alternate
  (fd_parse_context pc,fd_parse_state sr,fdtype alternate)
{
  struct FD_PARSER_STATE *s=&(pc->states[sr]);
  struct FD_OFSM_NODE *n=s->node;
  unsigned int i=1, in=s->input, dist=s->distance;
  fdtype weights=FD_CDR(alternate);
  while (i < pc->grammar->n_arcs) {
    int weight=get_weight(weights,i);
    if ((weight>=0) && (n->arcs[i].n_entries > 0)) {
      int j=0, limit=n->arcs[i].n_entries, len=fd_seq_length(FD_CAR(alternate));
      while (j < limit) {
        add_state
          (pc,n->arcs[i].entries[j].target,
           dist+n->arcs[i].entries[j].measure+weight,
           in+len,sr,i,FD_CAR(alternate));
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
  unsigned int in=s->input, dist=s->distance;
#if TRACING
  if (trace_tagger) log_state(pc,"*** Expanding",sr);
#endif
  if (in < pc->n_inputs) {
    expand_state_on_word(pc,sr,((struct FD_WORD *)&(pc->input[in])));
    if (!(FD_EMPTY_CHOICEP(pc->input[in].alternates))) {
      FD_DO_CHOICES(alternate,pc->input[in].alternates) {
        expand_state_on_alternate(pc,sr,alternate);}}
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
    if (trace_tagger) log_state(pc,"<<< POPPED",tref);
    top=&(pc->states[tref]); pc->last=tref;
    pc->queue=top->qnext;}
  else return pc->last;
  if ((top->input == pc->n_inputs) && (!(ZEROP(top->node->terminal)))) {
    if ((pc->flags&FD_TAGGER_EXPAND_FINAL_STATES))
      expand_state(pc,tref);
    return tref;}
  /*  else if (top->input > pc->n_inputs) return -1; */
  else {
    expand_state(pc,tref);
    return queue_extend(pc);}
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
  if (answer >= 0) {
    return answer;}
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
  else if ((FD_VECTORP(word)) && (FD_VECTOR_LENGTH(word)>0)) {
    int n=FD_VECTOR_LENGTH(word)-1;
    fdtype last=FD_VECTOR_REF(word,n);
    return possessive_suffixp(last);}
  else if (FD_PAIRP(word)) {
    fdtype last=word, scan=FD_CDR(word);
    while (FD_PAIRP(scan)) {last=scan; scan=FD_CDR(scan);}
    if (FD_STRINGP(FD_CAR(last)))
      return possessive_suffixp(FD_CAR(last));
    else return 0;}
  else return 0;
}

static fdtype possessive_root(fdtype word)
{
  if (FD_STRINGP(word)) {
    u8_string s=FD_STRDATA(word); int len=FD_STRLEN(word);
    if ((len>1) && (s[len-1]=='\''))
      return fd_substring(s,s+(len-1));
    else if ((len>2) && (s[len-1]=='s') && (s[len-2]=='\''))
      return fd_substring(s,s+(len-2));
    else return fd_incref(word);}
  else if ((FD_VECTORP(word)) && (FD_VECTOR_LENGTH(word)>0)) {
    int i=0, n=FD_VECTOR_LENGTH(word)-1;
    fdtype result=fd_init_vector(NULL,n,NULL);
    fdtype *newelts=FD_VECTOR_ELTS(result);
    while (i<n) {
      newelts[i]=fd_incref(FD_VECTOR_REF(word,i)); i++;}
    newelts[i]=possessive_root(FD_VECTOR_REF(word,i));
    return result;}
  else if (FD_PAIRP(word))
    if (FD_PAIRP(FD_CDR(word)))
      return fd_conspair(fd_incref(FD_CAR(word)),
                         possessive_root(FD_CDR(word)));
    else return fd_conspair(possessive_root(FD_CAR(word)),FD_EMPTY_LIST);
  else return fd_incref(word);
}

static fdtype get_root(struct FD_PARSE_CONTEXT *pcxt,fdtype base,int arcid,int cap)
{
  struct FD_GRAMMAR *g=pcxt->grammar;
  fdtype normalized, result=FD_EMPTY_CHOICE;
  if (g->name_tags[arcid])
    if (possessive_suffixp(base))
      return possessive_root(base);
    else return fd_incref(base);
  else if (cap)
    normalized=fd_incref(base);
  else if (FD_STRINGP(base)) {
    u8_string scan=FD_STRDATA(base);
    int firstc=u8_sgetc(&scan);
    if (u8_isupper(firstc))
      normalized=lower_string(FD_STRDATA(base));
    else normalized=fd_incref(base);}
  else if (((FD_PAIRP(base)) && (FD_STRINGP(FD_CAR(base)))) ||
           ((FD_VECTORP(base)) && (FD_VECTOR_LENGTH(base)>0) &&
            (FD_STRINGP(FD_VECTOR_REF(base,0))))) {
    u8_string scan=FD_STRDATA(base);
    int firstc=u8_sgetc(&scan);
    if (u8_isupper(firstc))
      normalized=lower_compound(base);
    else normalized=fd_incref(base);}
  else normalized=fd_incref(base);
  if (g->verb_tags[arcid])
    result=get_verb_root(pcxt,normalized);
  else if (g->noun_tags[arcid])
    result=get_noun_root(pcxt,normalized);
  else if (arcid==g->possessive_tag)
    result=possessive_root(normalized);
  if ((FD_EMPTY_CHOICEP(result)) || (FD_VOIDP(result)))
    return normalized;
  else {
    fd_decref(normalized);
    return result;}
}
FD_EXPORT fdtype fd_get_root(struct FD_PARSE_CONTEXT *pcxt,
                             fdtype base,int arcid)
{
  return get_root(pcxt,base,arcid,0);
}

static fdtype word2string(fdtype word)
{
  if ((FD_VECTORP(word)) && (FD_VECTOR_LENGTH(word)>0)) {
    struct U8_OUTPUT out;
    fdtype *elts=FD_VECTOR_DATA(word);
    int i=0, n=FD_VECTOR_LENGTH(word);
    U8_INIT_OUTPUT(&out,32);
    while (i<n)
      if (FD_STRINGP(elts[i])) {
        u8_string s=FD_STRDATA(elts[i]); int firstc=u8_sgetc(&s);
        if ((i>0) && (u8_isalnum(firstc))) u8_putc(&out,' ');
        u8_puts(&out,FD_STRDATA(elts[i])); i++;}
      else {
        u8_log(LOG_WARN,"Bad Compound phrase","element is not a string",
               word);
        if (i>0)
          u8_printf(&out," %q",elts[i]);
        else u8_printf(&out,"%q",elts[i]);
        i++;}
    return fd_stream2string(&out);}
  else if (FD_PAIRP(word)) {
    struct U8_OUTPUT out; int i=0; U8_INIT_OUTPUT(&out,32);
    {FD_DOLIST(elt,word) {
      u8_string s=FD_STRDATA(elt); int firstc=u8_sgetc(&s);
      if ((i>0) && (u8_isalnum(firstc))) u8_putc(&out,' ');
      u8_puts(&out,FD_STRDATA(elt)); i++;}}
    return fd_stream2string(&out);}
  else return fd_incref(word);
}

static fdtype make_word_entry(fdtype word,fdtype tag,
                              fdtype root,int distance,
                              fdtype source,int start,int end)
{
  if ((FD_VOIDP(source)) && (start<0))
    return fd_make_nvector(4,word,fd_incref(tag),root,
                           FD_USHORT2DTYPE(distance));
  else return fd_make_nvector(7,word,fd_incref(tag),root,
                              FD_USHORT2DTYPE(distance),
                              ((FD_VOIDP(source)) ? (FD_FALSE) : (source)),
                              FD_INT(start),FD_INT(end));
}

static u8_string find_end(u8_string start,u8_string lim)
{
  u8_string scan=start, end=start; int c;
  if (lim) while ((scan<lim) && ((c=u8_sgetc(&scan))>0))
             if (u8_isspace(c)) {}
             else if (c=='<') {
               while (c=='<')
                 while ((scan<lim) && (c>0) && (c!='>'))
                   c=u8_sgetc(&scan);}
             else end=scan;
  else while ((c=u8_sgetc(&scan))>0)
         if (u8_isspace(c)) {}
         else if (c=='<') {
           while (c=='<')
             while ((c>0) && (c!='>'))
               c=u8_sgetc(&scan);}
         else end=scan;
  return end;
}

static fdtype fix_weights(fdtype words)
{
  int base=0;
  FD_DOLIST(word,words) {
    fdtype distance=FD_VECTOR_REF(word,3);
    if (FD_FIXNUMP(distance)) {
      int d=FD_FIX2INT(distance);
      int w=d-base; base=d;
      FD_VECTOR_SET(word,3,FD_INT2DTYPE(w));}}
  return words;
}

FD_EXPORT
fdtype fd_gather_tags(fd_parse_context pc,fd_parse_state s)
{
  fdtype answer=FD_EMPTY_LIST, sentence=FD_EMPTY_LIST;
  fdtype arc_names=pc->grammar->arc_names;
  unsigned char *mod_tags=pc->grammar->mod_tags;
  const u8_byte *bufptr=pc->end;
  int glom_phrases=pc->flags&FD_TAGGER_GLOM_PHRASES;
  while (s >= 0) {
    fdtype source=FD_VOID;
    int char_start=-1, char_end=-1;
    struct FD_PARSER_STATE *state=&(pc->states[s]);
    if (state->arc == 0) s=state->previous;
    else if (pc->grammar->head_tags[state->arc]) {
      fdtype word=word2string(state->word);
      fdtype root=get_root(pc,word,state->arc,pc->input[state->input].cap);
      fdtype rootstring=word2string(root);
      fdtype tag=FD_VECTOR_REF(arc_names,state->arc);
      fdtype glom=FD_VOID, glom_root=FD_VOID, word_entry=FD_VOID, last_word=FD_VOID;
      fd_parse_state scan=state->previous;
      int glom_caps=capitalizedp(word);
      if (glom_phrases)
        while (scan>=0) {
          struct FD_PARSER_STATE *nextstate=&(pc->states[scan]);
          if ((mod_tags[nextstate->arc]) &&
              ((glom_caps) || (!(capitalizedp(nextstate->word))))) {
            if (FD_VOIDP(glom))
              if ((nextstate->arc==quote_mark_tag) ||
                  (nextstate->arc==noise_tag)) {
                glom=fd_make_list(1,fd_incref(word));
                glom_root=fd_make_list(1,rootstring);}
              else {
                glom=fd_make_list
                  (2,fd_incref(nextstate->word),fd_incref(word));
                glom_root=fd_make_list
                  (2,fd_incref(nextstate->word),rootstring);}
            else if ((nextstate->arc==quote_mark_tag) ||
                     (nextstate->arc==noise_tag))
              {}
            else {
              glom=fd_conspair(fd_incref(nextstate->word),glom);
              glom_root=
                fd_conspair(fd_incref(nextstate->word),glom_root);}
            scan=nextstate->previous;}
          else if (nextstate->arc==0) scan=nextstate->previous;
          else break;}
      fd_decref(root);
      if ((pc->flags&FD_TAGGER_INCLUDE_SOURCE) ||
          (pc->flags&FD_TAGGER_INCLUDE_TEXTRANGE)) {
        struct FD_PARSER_STATE *pstate=&(pc->states[scan]);
        const u8_byte *start, *end;
        if (scan<0) {
          start=pc->input[0].bufptr;
          end=find_end(pc->input[0].bufptr,bufptr);
          if (bufptr)
            source=fd_substring(start,bufptr);
          else source=fdtype_string(start);
          char_start=pc->input[0].char_pos;
          char_end=char_start+count_chars(start,end);}
        else {
          start=pc->input[pstate->input].bufptr;
          end=find_end(pc->input[pstate->input].bufptr,bufptr);
          char_start=pc->input[pstate->input].char_pos;
          if (bufptr==NULL) {
            source=fdtype_string(start);}
          else {
            source=fd_substring(start,bufptr);}
          char_end=char_start+count_chars(start,end);
          bufptr=start;}}
      if (FD_VOIDP(glom))
        word_entry=make_word_entry
          (word,fd_incref(tag),rootstring,
           state->distance,source,char_start,char_end);
      else {
        word_entry=make_word_entry
          (glom,fd_incref(tag),glom_root,
           state->distance,source,char_start,char_end);
        fd_decref(word);}
      sentence=fd_conspair(word_entry,sentence);
      s=scan;}
    else {
      fdtype word=word2string(state->word), word_entry=FD_VOID;
      fdtype root=get_root(pc,word,state->arc,pc->input[state->input].cap);
      fdtype rootstring=word2string(root);
      fdtype tag=FD_VECTOR_REF(pc->grammar->arc_names,state->arc);
      if ((pc->flags&FD_TAGGER_INCLUDE_SOURCE) ||
          (pc->flags&FD_TAGGER_INCLUDE_TEXTRANGE)) {
        struct FD_PARSER_STATE *pstate=&(pc->states[state->previous]);
        const u8_byte *start=pc->input[pstate->input].bufptr, *end=NULL;
        if (start==NULL) {}
        else {
          if (bufptr==NULL) {
            source=fdtype_string(start);}
          else {
            source=fd_substring(start,bufptr);}
          char_start=pc->input[pstate->input].char_pos;
          end=find_end(start,bufptr);
          char_end=char_start+count_chars(start,end);
          bufptr=start;}}
      word_entry=
        make_word_entry(word,fd_incref(tag),rootstring,state->distance,
                        source,char_start,char_end);
      fd_decref(root);
      if (state->arc==pc->grammar->sentence_end_tag)
        if (FD_EMPTY_LISTP(sentence))
          sentence=fd_conspair(word_entry,sentence);
        else {
          answer=fd_conspair(sentence,fix_weights(answer));
          sentence=fd_conspair(word_entry,FD_EMPTY_LIST);}
      else sentence=fd_conspair(word_entry,sentence);
      s=state->previous;}}
  if (FD_EMPTY_LISTP(sentence)) return answer;
  else return fd_conspair(fix_weights(sentence),answer);
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
  fdtype cxt=FD_VOID;
  if (pcxt==NULL) {
    struct FD_GRAMMAR *grammar=get_default_grammar();
    if (grammar==NULL)
      return fd_err(NoGrammar,"fd_analyze_text",NULL,FD_VOID);
    pcxt=u8_alloc(struct FD_PARSE_CONTEXT);
    fd_init_parse_context(pcxt,grammar);
    cxt=(fdtype)pcxt;}
  fd_parser_set_text(pcxt,text);
  if (pcxt->flags&FD_TAGGER_SPLIT_SENTENCES) {
    double full_start=u8_elapsed_time();
    double lextime=0.0, comptime=0.0, parsetime=0.0, proctime=0.0;
    const u8_byte *sentence=pcxt->start, *sentence_end; int n_calls=0;
    while ((sentence) && (*sentence) &&
           (sentence_end=find_sentence_end(sentence))) {
      double start_time=u8_elapsed_time();
      double lexdone, compdone, parsedone, procdone;
      fd_parse_state final;
      fdtype retval;
      lexer(pcxt,sentence,sentence_end);
      lexdone=u8_elapsed_time();
      if (pcxt->n_inputs==0) {
        if (sentence_end)
          sentence=skip_whitespace(sentence_end);
        else sentence=sentence_end;
        continue;}
      identify_alternates(pcxt);
      compdone=u8_elapsed_time();
      if (trace_tagger) {
        int input_i=0, n_inputs=pcxt->n_inputs;
        struct FD_WORD *inputs=pcxt->input;
        u8_message("Lexed %d inputs from %s",n_inputs,text);
        while (input_i<n_inputs) {
          log_word(pcxt,&inputs[input_i],input_i);
          input_i++;}}
      add_state(pcxt,&(pcxt->grammar->nodes[0]),0,0,-1,0,FD_VOID);
      final=fd_run_parser(pcxt);
      parsedone=u8_elapsed_time();
      pcxt->runtime=pcxt->runtime+(u8_elapsed_time()-start_time);
      retval=fn(pcxt,final,data);
      procdone=u8_elapsed_time();
      if (FD_ABORTP(retval)) {
        fd_decref(cxt);
        return retval;}
      else {
        n_calls++;
        if (sentence_end)
          sentence=skip_whitespace(sentence_end);
        else sentence=sentence_end;}
      lextime=lextime+(lexdone-start_time);
      comptime=comptime+(compdone-lexdone);
      parsetime=parsetime+(parsedone-compdone);
      proctime=proctime+(procdone-parsedone);}
    if (pcxt->flags&FD_TAGGER_VERBOSE_TIMER)
      u8_log(LOG_INFO,"DoneSentences",
             "Parsed %d sentences in %f seconds; %f/%f/%f/%f lex/comp/parse/proc",
             n_calls,u8_elapsed_time()-full_start,
             lextime,comptime,parsetime,proctime);
    fd_decref(cxt);
    return FD_INT(n_calls);}
  else {
    double start_time=u8_elapsed_time();
    fd_parse_state final;
    fdtype retval;
    lexer(pcxt,pcxt->start,NULL);
    identify_alternates(pcxt);
    add_state(pcxt,&(pcxt->grammar->nodes[0]),0,0,-1,0,FD_VOID);
    final=fd_run_parser(pcxt);
    pcxt->runtime=pcxt->runtime+(u8_elapsed_time()-start_time);
    retval=fn(pcxt,final,data);
    fd_decref(cxt);
    return FD_INT(1);}
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
  fdtype result=FD_EMPTY_LIST, retval=FD_VOID, cxt;
  if (pcxt==NULL) {
    fd_grammar grammar=get_default_grammar();
    if (grammar==NULL)
      return fd_err(NoGrammar,"fd_tag_text",NULL,FD_VOID);
    pcxt=u8_alloc(struct FD_PARSE_CONTEXT);
    fd_init_parse_context(pcxt,grammar);
    cxt=(fdtype)pcxt;}
  else {
    cxt=(fdtype)pcxt; fd_incref(cxt);}
  retval=fd_analyze_text(pcxt,text,tag_text_helper,&result);
  fd_decref(cxt);
  if (FD_ABORTP(retval)) {
    fd_decref(result);
    return retval;}
  else return result;
}


/* Interpreting parser flags */

static fdtype xml_symbol, plaintext_symbol, glom_symbol, noglom_symbol;
static fdtype oddcaps_symbol, allcaps_symbol, whole_symbol, timing_symbol, source_symbol, textpos_symbol;

static int interpret_parse_flags(fdtype arg)
{
  int flags=FD_TAGGER_DEFAULT_FLAGS;
  if (fd_testopt(arg,xml_symbol,FD_VOID))
    flags=flags|FD_TAGGER_SKIP_MARKUP;
  if (fd_testopt(arg,plaintext_symbol,FD_VOID))
    flags=flags&(~FD_TAGGER_SKIP_MARKUP);
  if (fd_testopt(arg,glom_symbol,FD_VOID))
    flags=flags|FD_TAGGER_GLOM_PHRASES;
  if (fd_testopt(arg,oddcaps_symbol,FD_VOID))
    flags=flags|FD_TAGGER_ODDCAPS;
  if (fd_testopt(arg,allcaps_symbol,FD_VOID))
    flags=flags|FD_TAGGER_ALLCAPS;
  if (fd_testopt(arg,whole_symbol,FD_VOID))
    flags=flags&(~FD_TAGGER_SPLIT_SENTENCES);
  if (fd_testopt(arg,timing_symbol,FD_VOID))
    flags=flags|FD_TAGGER_VERBOSE_TIMER;
  if (fd_testopt(arg,source_symbol,FD_VOID))
    flags=flags|FD_TAGGER_INCLUDE_SOURCE;
  if (fd_testopt(arg,textpos_symbol,FD_VOID))
    flags=flags|FD_TAGGER_INCLUDE_TEXTRANGE;
  return flags;
}


/* FDScript NLP primitives */

static fdtype tagtext_prim(fdtype input,fdtype flags,fdtype custom)
{
  struct FD_PARSE_CONTEXT parse_context;
  struct FD_GRAMMAR *grammar=get_default_grammar();
  fdtype result=FD_EMPTY_LIST, retval=FD_VOID, cxt=(fdtype)(&parse_context);
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
  FD_INIT_STACK_CONS(&parse_context,fd_tagger_type);
  /* Now we set the custom table if provided */
  if ((FD_FALSEP(custom)) || (FD_VOIDP(custom)) ||
      (FD_EMPTY_CHOICEP(custom))) {}
  else if (FD_HASHTABLEP(custom)) {
    fd_incref(custom);
    parse_context.custom_lexicon=(struct FD_HASHTABLE *)custom;}
  else {
    fd_free_parse_context(&parse_context);
    return fd_type_error(_("hashtable"),"tagtext_prim",custom);}
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
  if (FD_ABORTP(retval)) {
    fd_seterr(ParseFailed,"",NULL,FD_VOID);
    return retval;}
  else return result;
}

static fdtype tagtextx_prim(fdtype input,fdtype flags,fdtype custom)
{
  struct FD_PARSE_CONTEXT parse_context;
  struct FD_GRAMMAR *grammar=get_default_grammar();
  fdtype result=FD_EMPTY_LIST, retval=FD_VOID, cxt=(fdtype)(&parse_context);
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
  FD_INIT_STACK_CONS(cxt,fd_tagger_type);
  /* Now we set the custom table if provided */
  if ((FD_FALSEP(custom))||(FD_VOIDP(custom))||
      (FD_EMPTY_CHOICEP(custom))) {}
  else if (FD_HASHTABLEP(custom)) {
    fd_incref(custom);
    parse_context.custom_lexicon=(struct FD_HASHTABLE *)custom;}
  else {
    fd_free_parse_context(&parse_context);
    return fd_type_error(_("hashtable"),"tagtext_prim",custom);}
  /* Now we set the flags from the argument. */
  if (!(FD_VOIDP(flags)))
    parse_context.flags=interpret_parse_flags(flags);
  if (FD_STRINGP(input))
    retval=fd_analyze_text
      (&parse_context,FD_STRDATA(input),tag_text_helper,&result);
  else if (FD_PAIRP(input)) {
    FD_DOLIST(elt,input) {
      retval=fd_analyze_text
        (&parse_context,FD_STRDATA(elt),tag_text_helper,&result);
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
  if (FD_ABORTP(retval)) {
    fd_free_parse_context(&parse_context);
    fd_seterr(ParseFailed,"",NULL,FD_VOID);
    return retval;}
  else result=fd_make_nvector
         (5,FD_INT(parse_context.n_calls),
          FD_INT(parse_context.n_inputs),
          FD_INT(parse_context.n_states),
          fd_init_double(NULL,parse_context.runtime),
          result);
  fd_free_parse_context(&parse_context);
  return result;
}


/* Tagger object operations */

static fdtype tagger_start(fdtype text,fdtype cxt)
{
  struct FD_PARSE_CONTEXT *pcxt;
  if ((FD_VOIDP(cxt))||(FD_FALSEP(cxt))) {
    struct FD_GRAMMAR *grammar=get_default_grammar();
    pcxt=u8_alloc(struct FD_PARSE_CONTEXT);
    fd_init_parse_context(pcxt,grammar);
    pcxt->flags|=FD_TAGGER_EXPAND_FINAL_STATES;
    FD_INIT_CONS(pcxt,fd_tagger_type);
    cxt=(fdtype) pcxt;}
  else if (FD_PRIM_TYPEP(cxt,fd_tagger_type)) {
    pcxt=(struct FD_PARSE_CONTEXT *)cxt;
    fd_reset_parse_context(pcxt);
    fd_incref(cxt);}
  else return fd_type_error("tagger","tagger_start",cxt);
  if (FD_STRINGP(text)) {
    fd_parser_set_text(pcxt,FD_STRDATA(text));}
  return (fdtype) pcxt;
}

static fdtype tagger_next(fdtype cxt)
{
  struct FD_PARSE_CONTEXT *pcxt=(struct FD_PARSE_CONTEXT *)cxt;
  double start_time=u8_elapsed_time();
  fd_parse_state final;
  fdtype result=FD_EMPTY_LIST, tags=FD_VOID;
  if (pcxt->n_inputs==0) {
    lexer(pcxt,pcxt->start,NULL);
    identify_alternates(pcxt);
    add_state(pcxt,&(pcxt->grammar->nodes[0]),0,0,-1,0,FD_VOID);}
  final=fd_run_parser(pcxt);
  pcxt->runtime=pcxt->runtime+(u8_elapsed_time()-start_time);
  return fd_gather_tags(pcxt,final);
}

static fdtype tagger_top(fdtype cxt)
{
  struct FD_PARSE_CONTEXT *pcxt=(struct FD_PARSE_CONTEXT *)cxt;
  double start_time=u8_elapsed_time();
  fd_parse_state final; struct FD_PARSER_STATE *f;
  fdtype result=FD_EMPTY_LIST, tags=FD_VOID;
  if (pcxt->n_inputs==0) {
    lexer(pcxt,pcxt->start,NULL);
    identify_alternates(pcxt);
    add_state(pcxt,&(pcxt->grammar->nodes[0]),0,0,-1,0,FD_VOID);}
  final=fd_run_parser(pcxt);
  pcxt->runtime=pcxt->runtime+(u8_elapsed_time()-start_time);
  if (final<0) return FD_EMPTY_CHOICE;
  else {
    struct FD_PARSER_STATE *f=&(pcxt->states[final]);
    fdtype results=FD_EMPTY_CHOICE;
    fd_parse_state next=f->qnext;
    int d=f->distance;
    while ((f)&&(f->distance==d)&&
           (f->input==pcxt->n_inputs)&&
           (!(ZEROP(f->node->terminal)))) {
      fdtype tagging=fd_gather_tags(pcxt,final);
      FD_ADD_TO_CHOICE(results,tagging);
      if ((pcxt->flags&FD_TAGGER_EXPAND_FINAL_STATES))
        expand_state(pcxt,final);
      pcxt->queue=next;
      if (next>=0)
        f=&(pcxt->states[next]);
      else f=NULL;
      final=next;}
    return results;}
}


/* Lexicon access */

static fdtype lexweight_prim(fdtype string,fdtype tag,fdtype value)
{
  fd_grammar g=fd_default_grammar();
  int i=0, lim=g->n_arcs;
  fdtype arcs=g->arc_names,
    weights=get_lexweights(NULL,FD_STRDATA(string),string,NULL);
  lexicon_fetch(g->lexicon,string);
  if (FD_EMPTY_CHOICEP(weights)) return FD_EMPTY_CHOICE;
  else if (FD_VOIDP(tag)) {
    fdtype results=FD_EMPTY_CHOICE;
    while (i<lim) {
      int weight=get_weight(weights,i);
      if (weight<0) i++;
      else {
        fdtype pair=fd_conspair(fd_incref(FD_VECTOR_REF(arcs,i)),
                                fd_incref(FD_INT(weight)));
        FD_ADD_TO_CHOICE(results,pair); i++;}}
    return results;}
  else {
    while (i<lim)
      if (FD_EQ(tag,FD_VECTOR_REF(arcs,i)))
        if (FD_VOIDP(value)) {
          int weight=get_weight(weights,i);
          if (weight==255) return FD_FALSE;
          else return FD_INT(weight);}
        else {
          int weight=get_weight(weights,i);
          if (FD_VECTORP(weights)) {
            FD_VECTOR_SET(weights,i,fd_incref(value));}
          else if (FD_PACKETP(weights)) {
            unsigned char *bytes=(unsigned char *)FD_PACKET_DATA(weights);
            if (FD_FALSEP(value)) {
              bytes[i]=255;}
            else if ((FD_FIXNUMP(value)) &&
                     (FD_FIX2INT(value)>=0) &&
                     (FD_FIX2INT(value)<128))
              bytes[i]=FD_FIX2INT(value);}
          else {}
          if (weight==255) return FD_FALSE;
          else return FD_INT(weight);}
      else i++;
    return FD_EMPTY_CHOICE;}
}

static fdtype lexinfo_prim(fdtype string,fdtype tag)
{
  fd_grammar g=fd_default_grammar();
  int i=0, lim=g->n_arcs;
  fdtype arcs=g->arc_names, via=FD_EMPTY_CHOICE,
    weights=get_lexweights(NULL,FD_STRDATA(string),string,&via);
  lexicon_fetch(g->lexicon,string);
  if (FD_EMPTY_CHOICEP(weights)) return via;
  else if (FD_VOIDP(tag)) {
    fdtype results=FD_EMPTY_CHOICE;
    while (i<lim) {
      int weight=get_weight(weights,i);
      if (weight<0) i++;
      else {
        fdtype pair=fd_conspair(fd_incref(FD_VECTOR_REF(arcs,i)),
                                fd_incref(FD_INT(weight)));
        FD_ADD_TO_CHOICE(results,pair); i++;}}
    FD_ADD_TO_CHOICE(results,via);
    return results;}
  else {
    while (i<lim)
      if (FD_EQ(tag,FD_VECTOR_REF(arcs,i))) {
        int weight=get_weight(weights,i);
        if (weight==255) return FD_FALSE;
        else {
          fdtype result=via;
          FD_ADD_TO_CHOICE(result,FD_INT(weight));
          return result;}}
      else i++;
    return FD_EMPTY_CHOICE;}
}

static fdtype lextags_prim(fdtype tag)
{
  fd_grammar g=fd_default_grammar();
  if (g==NULL)
    return FD_ERROR_VALUE;
  else if (FD_VOIDP(tag))
    return fd_incref(g->arc_names);
  else {
    int i=fd_position(tag,g->arc_names,0,-1);
    if (i<0) return FD_EMPTY_CHOICE;
    else return FD_INT(i);}
}

static fdtype lexwordp(fdtype string)
{
  fd_grammar g=fd_default_grammar();
  if (g==NULL)
    return FD_ERROR_VALUE;
  else {
    fdtype v=fd_index_get(g->lexicon,string);
    if (FD_ABORTP(v)) return v;
    else if (FD_EMPTY_CHOICEP(v)) return FD_FALSE;
    else if (FD_FALSEP(v)) return FD_FALSE;
    else {
      fd_decref(v);
      return FD_TRUE;}}
}

static fdtype lexprefixp(fdtype string)
{
  fd_grammar g=fd_default_grammar();
  if (g==NULL)
    return FD_ERROR_VALUE;
  else {
    fdtype v=fd_index_get(g->lexicon,string);
    if (FD_ABORTP(v)) return v;
    else if (FD_EMPTY_CHOICEP(v)) return FD_FALSE;
    else if (FD_FALSEP(v)) return FD_FALSE;
    else if (FD_VECTORP(v)) {
      fdtype flag=FD_VECTOR_REF(v,1), result=FD_FALSE;
      if ((FD_FIXNUMP(flag)) && ((FD_FIX2INT(flag))>=0))
        result=FD_TRUE;
      fd_decref(v);
      return result;}
    else if (FD_PACKETP(v)) {
      int val=FD_PACKET_REF(v,1), retval=0;
      if (val<128) retval=1;
      fd_decref(v);
      if (retval) return FD_TRUE; else return FD_FALSE;}
    else return FD_FALSE;}
}

static fdtype lexicon_prefetch(fdtype keys)
{
  fd_grammar g=fd_default_grammar();
  if (g==NULL) {
    fd_seterr(NoGrammar,"lexicon_prefetch",NULL,FD_VOID);
    return FD_ERROR_VALUE;}
  else if (FD_VOIDP(keys)) {
    fdtype allkeys=fd_index_keys(g->lexicon);
    fd_index_prefetch(g->lexicon,allkeys);
    fd_decref(allkeys);
    allkeys=fd_index_keys(g->noun_roots);
    fd_index_prefetch(g->noun_roots,allkeys);
    fd_decref(allkeys);
    allkeys=fd_index_keys(g->verb_roots);
    fd_index_prefetch(g->noun_roots,allkeys);
    fd_decref(allkeys);
    return FD_VOID;}
  else {
    fd_index_prefetch(g->lexicon,keys);
    fd_index_prefetch(g->noun_roots,keys);    
    fd_index_prefetch(g->verb_roots,keys);
    return FD_VOID;}
}


/* Initialization procedures */

static void init_parser_symbols(void);
static fdtype lisp_report_stats(void);
static fdtype lisp_set_word_limit(fdtype x);

static fd_index openindexsource(u8_string base,u8_string component)
{
  int filebase=(strchr(base,'@')==NULL);
  u8_string indexid=
    ((filebase) ? (u8_mkstring("%s/%s",base,component)) :
     (u8_mkstring("%s@%s",component,base)));
  fd_index ix=fd_open_index_x(indexid,1);
  u8_free(indexid);
  return ix;
}

FD_EXPORT
struct FD_GRAMMAR *fd_open_grammar(u8_string spec)
{
  fdtype nouns, verbs, names, heads, mods, arc_names;
  struct FD_GRAMMAR *g=u8_alloc(struct FD_GRAMMAR);
  memset(g,0,sizeof(struct FD_GRAMMAR));
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
  else arc_names=g->arc_names=fd_index_get(g->lexicon,fd_intern("%ARCS"));
  names=fd_index_get(g->lexicon,names_symbol);
  nouns=fd_index_get(g->lexicon,nouns_symbol);
  verbs=fd_index_get(g->lexicon,verbs_symbol);
  heads=fd_index_get(g->lexicon,heads_symbol);
  mods=fd_index_get(g->lexicon,mods_symbol);
  g->common_arcs=fd_index_get(g->lexicon,fd_intern("%COMMON"));
  {
    int len=g->n_arcs;
    unsigned char *taginfo=u8_alloc_n(len,unsigned char);
    fdtype *data=FD_VECTOR_DATA(arc_names);
    int i=0; while (i<len)
      if (fd_overlapp(data[i],names)) taginfo[i++]=1; else taginfo[i++]=0;
    g->name_tags=taginfo;
    fd_decref(names);}
  {
    int len=g->n_arcs;
    unsigned char *taginfo=u8_alloc_n(len,unsigned char);
    fdtype *data=FD_VECTOR_DATA(arc_names);
    int i=0; while (i<len)
      if (fd_overlapp(data[i],nouns)) taginfo[i++]=1; else taginfo[i++]=0;
    g->noun_tags=taginfo;
    fd_decref(nouns);}
  {
    int len=g->n_arcs;
    unsigned char *taginfo=u8_alloc_n(len,unsigned char);
    fdtype *data=FD_VECTOR_DATA(arc_names);
    int i=0; while (i<len)
      if (fd_overlapp(data[i],heads)) taginfo[i++]=1; else taginfo[i++]=0;
    g->head_tags=taginfo;
    fd_decref(heads);}
  {
    int len=g->n_arcs;
    unsigned char *taginfo=u8_alloc_n(len,unsigned char);
    fdtype *data=FD_VECTOR_DATA(arc_names);
    int i=0; while (i<len)
      if (fd_overlapp(data[i],mods)) taginfo[i++]=1; else taginfo[i++]=0;
    g->mod_tags=taginfo;
    fd_decref(mods);}
  {
    int len=g->n_arcs;
    unsigned char *taginfo=u8_alloc_n(len,unsigned char);
    fdtype *data=FD_VECTOR_DATA(arc_names);
    int i=0; while (i<len)
      if (fd_overlapp(data[i],verbs)) taginfo[i++]=1; else taginfo[i++]=0;
    g->verb_tags=taginfo;
    fd_decref(verbs);}
  return g;
}

static fdtype lisp_trace_tagger(fdtype flag)
{
  if (FD_FALSEP(flag)) trace_tagger=0;
  else trace_tagger=1;
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
  return FD_INT(olimit);
}

FD_EXPORT
/* fd_trace_tagger:
      Arguments: none
      Returns: nothing
  Turns on tracing in the tagger */
void fd_trace_tagger()
{
  trace_tagger=1;
}

static fdtype lisp_get_stats()
{
  return fd_make_nvector(4,
                         FD_INT(total_states),
                         FD_INT(total_inputs),
                         FD_INT(total_sentences),
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
  s_word=fd_intern("%S-WORD");   /* Not currently used */
  punctuation_symbol=fd_intern("%PUNCTUATION");
  sstrange_word=fd_intern("%STRANGE-WORD");
  sproper_name=fd_intern("%PROPER-NAME");
  sxproper_name=fd_intern("%XPROPER-NAME");
  sdashed_word=fd_intern("%DASHED-WORD");
  sdashed_sword=fd_intern("%DASHED-SWORD");
  stime_ref=fd_intern("%TIME-WORD");
  sscore=fd_intern("%SCORE");
  snumber=fd_intern("%NUMBER");
  sdollars=fd_intern("%DOLLARS");
  spossessive=fd_intern("%POSSESSIVE");
  sproper_possessive=fd_intern("%PROPER-POSSESSIVE");
  sxproper_possessive=fd_intern("%XPROPER-POSSESSIVE");

  lexget_symbol=fd_intern("LEXGET");
  verb_root_symbol=fd_intern("VERB-ROOT");
  noun_root_symbol=fd_intern("NOUN-ROOT");
  names_symbol=fd_intern("%NAMES");
  nouns_symbol=fd_intern("%NOUNS");
  verbs_symbol=fd_intern("%VERBS");
  grammar_symbol=fd_intern("%GRAMMAR");
  heads_symbol=fd_intern("%HEADS");
  mods_symbol=fd_intern("%MODS");

  prefix_symbol=fd_intern("PREFIX");
  noun_symbol=fd_intern("NOUN");
  verb_symbol=fd_intern("VERB");
  name_symbol=fd_intern("NAME");
  compound_symbol=fd_intern("COMPOUND");

  xml_symbol=fd_intern("XML");
  plaintext_symbol=fd_intern("PLAINTEXT");
  glom_symbol=fd_intern("GLOM");
  noglom_symbol=fd_intern("NOGLOM");
  oddcaps_symbol=fd_intern("ODDCAPS");
  allcaps_symbol=fd_intern("ALLCAPS");
  whole_symbol=fd_intern("WHOLE");
  timing_symbol=fd_intern("TIMING");
  source_symbol=fd_intern("SOURCE");
  textpos_symbol=fd_intern("TEXTPOS");

  sentence_end_symbol=fd_intern("SENTENCE-END");
  parse_failed_symbol=fd_intern("NOPARSE");
}

/* fd_tagger_type handlers */

static void recycle_tagger(struct FD_CONS *c)
{
  struct FD_PARSE_CONTEXT *pcxt=(struct FD_PARSE_CONTEXT *)c;
  fd_free_parse_context(pcxt);
  u8_free(pcxt);
}

static int unparse_tagger(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_PARSE_CONTEXT *pcxt=(struct FD_PARSE_CONTEXT *)x;
  u8_printf(out,"#<TAGGER 0x%llx>",x);
  return 1;
}

/* fd_init_ofsm_c:
      Arguments: none
      Returns: nothing
*/
FD_EXPORT
void fd_init_ofsm_c()
{
  fdtype menv=fd_new_module("TAGGER",(FD_MODULE_SAFE));
  init_parser_symbols();

  u8_register_source_file(_FILEINFO);

  fd_tagger_type=fd_register_cons_type("tagger");

  fd_unparsers[fd_tagger_type]=unparse_tagger;
  fd_recyclers[fd_tagger_type]=recycle_tagger;

#if FD_THREADS_ENABLED
  fd_init_mutex(&default_grammar_lock);
  fd_init_mutex(&parser_stats_lock);
#endif

  /* This are ndprims because the flags arguments may be a choice. */
  fd_idefn(menv,fd_make_ndprim(fd_make_cprim3("TAGTEXT",tagtext_prim,1)));
  fd_idefn(menv,fd_make_ndprim(fd_make_cprim3("TAGTEXT*",tagtextx_prim,1)));

  fd_idefn(menv,fd_make_cprim2x("TAGGER/START",tagger_start,1,
                                fd_string_type,FD_VOID,-1,FD_VOID));
  fd_idefn(menv,fd_make_cprim1x("TAGGER/NEXT",tagger_next,1,
                                fd_tagger_type,FD_VOID));
  fd_idefn(menv,fd_make_cprim1x("TAGGER/TOP",tagger_top,1,
                                fd_tagger_type,FD_VOID));

  fd_idefn(menv,fd_make_cprim3("LEXWEIGHT",lexweight_prim,1));
  fd_defalias(menv,"LEXWEIGHTS","LEXWEIGHT");
  fd_idefn(menv,fd_make_cprim2("LEXINFO",lexinfo_prim,1));
  fd_idefn(menv,fd_make_cprim1("LEXTAGS",lextags_prim,0));

  fd_idefn(menv,fd_make_cprim0("NLP-STATS",lisp_get_stats,0));
  fd_idefn(menv,fd_make_cprim0("REPORT-NLP-STATS",lisp_report_stats,0));
  fd_idefn(menv,fd_make_cprim1("SET-WORD-LIMIT!",lisp_set_word_limit,1));
  fd_idefn(menv,fd_make_cprim1("TRACE-TAGGER!",lisp_trace_tagger,1));

  fd_idefn(menv,fd_make_ndprim
           (fd_make_cprim1("LEXICON-PREFETCH!",lexicon_prefetch,0)));
  fd_idefn(menv,fd_make_cprim1("LEXWORD?",lexwordp,1));
  fd_idefn(menv,fd_make_cprim1("LEXPREFIX?",lexprefixp,1));

  fd_register_config("LEXDATA",
                     "The location (file/server) for the tagger lexicon",
                     config_get_lexdata,config_set_lexdata,NULL);
  fd_register_config("LEXICON",
                     "The FramerD index (or reference) to use as a lexicon",
                     config_get_lexicon,config_set_lexicon,NULL);
  fd_register_config("TAGGER:TRACE",
                     "Whether to trace the tagging OFSM",
                     fd_boolconfig_get,fd_boolconfig_set,&trace_tagger);
  fd_register_config("TAGGER:TRACELOAD",
                     "Whether to trace the tagging OFSM",
                     fd_boolconfig_get,fd_boolconfig_set,&trace_load_grammar);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
