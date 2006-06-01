/* C Mode */

#include "eframerd/lisp.h"
#include "eframerd/eval.h"
#include "eframerd/fddb.h"
#include "eframerd/pools.h"
#include "eframerd/indices.h"
#include "eframerd/dtypestream.h"

#include "eframerd/chopper.h"

#include <libu8/u8.h>
#include <libu8/ctype.h>
#include <libu8/filefns.h>
#include <libu8/u8io.h>

#include <time.h>
#include <stdlib.h>
#include <string.h>
#if defined(WIN32)
#include <windows.h>
#endif

#define ZEROP(x) ((FD_FIXLISP(x)) == 0)
#define isspacep(c) ((c<80) ? (isspace(c)) : (u8_isspace(c)))

static fd_exception TooManyWords=_("Too many words");
static fd_exception InternalParseError=_("Internal parse error");
static fd_exception LateConfiguration=_("LEXDATA has already been configured");

static int chopper_initialized=0;
static u8_string lexdata_source=NULL;
static fd_mutex initialize_chopper_lock;
static void initialize_chopper(void);


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


/* Global declarations */

static int word_limit=-1;
static int tracing_tagger=0;

static struct NODE nodes[MAX_NODES];
static unsigned int n_nodes=0, n_arcs=0;

#if FD_THREADS_ENABLED
static fd_mutex parser_stats_lock;
static fd_mutex inverses_table_lock;
#endif

static int max_states=0, total_states=0;
static int max_inputs=0, max_phrases=0;
static int total_phrases=0,total_inputs=0, total_sentences=0;
static time_t start_time;
static fd_lisp arc_names;

/* Tags of note. */
static int ing_verb_tag, inf_verb_tag, conjunction_tag, glue_tag;
static int anything_tag, passive_verb_tag, pronoun_tag, noun_tag;
static int verb_as_adjective_tag, complement_adjective_tag;
static int punctuation_tag, comma_tag, sentence_end_tag, and_tag, or_tag;

/* Declarations for hookup */

static unsigned char head_tags[MAX_ARCS];
static unsigned char noun_tags[MAX_ARCS];
static unsigned char verb_tags[MAX_ARCS];
static unsigned char mod_tags[MAX_ARCS];
static unsigned char thing_tags[MAX_ARCS];
static unsigned char action_tags[MAX_ARCS];
static unsigned char prefix_tags[MAX_ARCS];
static unsigned char suffix_tags[MAX_ARCS];
static unsigned char prep_tags[MAX_ARCS];
static unsigned char name_tags[MAX_ARCS];
static unsigned char dangling_tags[MAX_ARCS];

static fd_lisp root_slotid, a_slotid, m_slotid, phrase_slotid;
static fd_lisp pos_slotid, pronoun_symbol, preposition_symbol, of_symbol;
static fd_lisp subject_slotid, object_slotid, by_symbol;
static fd_lisp close_subject_slotid, close_object_slotid, next_act_slotid, 
  subject_of_slotid, close_subject_of_slotid,
  last_act_slotid, next_phrase_slotid, previous_phrase_slotid;

static fd_lisp possible_subject_slotid, likely_subject_slotid;
static fd_lisp possible_object_slotid, tight_object_slotid;
static fd_lisp verb_arg, possible_argument_slotid;
static fd_lisp gfs, ggs, gfo, ggo, tg, tf;

/* Declarations for lexicon access */

static fd_hashtable lexicon_cache=NULL;
static fd_hashtable vroots_cache=NULL;
static fd_hashtable nroots_cache=NULL;
static fd_hashtable vlinks_cache=NULL;

static fd_lisp noun_symbol, verb_symbol, name_symbol;
static fd_lisp lexget_symbol, verb_root_symbol, noun_root_symbol;
static fd_lisp verb_links_symbol;

/* These simple syntactic categories are used when words aren't
   in the lexicon. */
static fd_lisp sstrange_word, sproper_name, sdashed_adjective, stime_ref;
static fd_lisp ed_word, ing_word, ly_word, punctuation_symbol;
static fd_lisp sscore, snumber, sdollars, spossessive, sproper_possessive;

static fd_index verb_roots, noun_roots, lexicon, vlinks;

/* Because we use fd_find_frames, which takes a FD_LISP pointer to an index */
static fd_lisp brico_index;

static fd_lisp name_assocs, place_assocs, number_assocs;
static fd_lisp source_slotid, wordnet_source;

/* Declarations for frame generation */

static int total_frames=0;
static fd_lisp a_slotid, words_slotid, obj_name_slotid, possessive_symbol;
static int place_name_tag, proper_name_tag, proper_adjective_tag;
static int partial_place_name_tag, number_tag, count_adjective_tag;


/* Utility functions */

static fd_lisp nreverse_list(fd_lisp lst)
{
  fd_lisp prev=FD_EMPTY_LIST;
  while (FD_PAIRP(lst)) {
    fd_lisp nxt=FD_CDR(lst);
    struct FD_PAIR *p=(fd_pair)lst;
    p->cdr=prev; lst=nxt; prev=lst;}
  return prev;
}

static u8_string u8_slice(u8_string start,u8_string end)
{
  if (end == NULL) return strdup(start);
  else {
    u8_string copy=u8_malloc((end-start)+1);
    strcpy(copy,start);
    return copy;}
}

static struct {fd_lisp slot, inverse;} inverses[256]; static int n_inverses=0;

static fd_lisp make_slot(fd_lisp x)
{
  if (FD_STRINGP(x))
    return fd_parse(FD_STRDATA(x));
  else if (FD_PAIRP(x)) {
    struct U8_OUTPUT ss; fd_lisp scan=x, answer;
    U8_INIT_OUTPUT(&ss,64);
    while (FD_PAIRP(scan)) {
      fd_lisp car=FD_CAR(scan);
      if (FD_STRINGP(car)) {
	u8_string data=FD_STRDATA(car); int c;
	while ((c=u8_sgetc(&data))>=0) u8_putc(&ss,c);}
      else fd_unparse(&ss,car);
      scan=FD_CDR(scan);
      if (FD_PAIRP(scan)) u8_putc(&ss,'-');}
    answer=fd_parse(ss.bytes); u8_free(ss.bytes);
    return answer;}
  else return fd_incref(x); /* What could this be? */
}

static fd_lisp inverse_slotid(fd_lisp slot)
{
  fd_lisp result; int i=0, lim=n_inverses; while (i < lim)
    if (FD_EQ(inverses[i].slot,slot))
      return inverses[i].inverse; else i++;
  {fd_lock_mutex(&inverses_table_lock); {
    while (i < n_inverses) 
      if (FD_EQ(inverses[i].slot,slot)) {
	result=inverses[i].inverse; break;}
      else i++;
    if (i == n_inverses) {
      char *long_name=u8_malloc(strlen(FD_SYMBOL_NAME(slot))+5);
      strcpy(long_name,FD_SYMBOL_NAME(slot));
      strcat(long_name,"-INV");
      inverses[i].slot=slot;
      result=inverses[i].inverse=fd_intern(long_name);
      n_inverses++;}}
  fd_unlock_mutex(&inverses_table_lock);}
  return result;
}


/* Parse Contexts */

static int init_parse_context(parse_context pc)
{
  unsigned int i=0;
  if (chopper_initialized == 0) {
    fd_lock_mutex(&initialize_chopper_lock); {
      if (chopper_initialized == 0) initialize_chopper();}
    fd_unlock_mutex(&initialize_chopper_lock);}
  pc->input=malloc(sizeof(struct WORD)*INITIAL_N_INPUTS);
  pc->n_inputs=0; pc->max_n_inputs=INITIAL_N_INPUTS;
  pc->states=malloc(sizeof(struct STATE)*INITIAL_N_STATES);
  pc->n_states=0; pc->max_n_states=INITIAL_N_STATES;
  pc->queue=-1; pc->last=-1;
  pc->parse_pool=NULL;
  pc->phrases=malloc(sizeof(struct PHRASE)*INITIAL_N_INPUTS);
  pc->n_phrases=0; pc->max_n_phrases=INITIAL_N_INPUTS;
  pc->cache=malloc(sizeof(state_ref *)*INITIAL_N_INPUTS);
  while (i < INITIAL_N_INPUTS) {
    state_ref *vec=malloc(sizeof(state_ref)*n_nodes);
    unsigned int j=0; while (j < n_nodes) vec[j++]=-1;
    pc->cache[i++]=vec;}
}

static void free_parse_context(parse_context pc)
{
  int i=0;

  /* Update parser stats */
  fd_lock_mutex(&parser_stats_lock);
  total_states=total_states+pc->n_states;
  total_inputs=total_inputs+pc->n_inputs;
  total_phrases=total_phrases+pc->n_phrases;
  total_sentences++;
  if (pc->n_states > max_states) max_states=pc->n_states;
  if (pc->n_inputs > max_inputs) max_inputs=pc->n_inputs;
  if (pc->n_phrases > max_phrases) max_phrases=pc->n_phrases;
  fd_unlock_mutex(&parser_stats_lock);

  /* Free phrase roots and links */
  i=0; while (i < pc->n_phrases) {
    free(pc->phrases[i].links);
    free(pc->phrases[i].extras);
    fd_decref(pc->phrases[i].root); i++;}
  free(pc->phrases);

  /* Free other memory structures */
  i=0; while (i < pc->n_inputs) {
    /* Freed by fd_decref */
    /* free(pc->input[i].spelling); */
    fd_decref(pc->input[i].lstr);
    fd_decref(pc->input[i].compounds);
    i++;}
  free(pc->input); free(pc->states); 
  i=0; while (i < pc->max_n_inputs) free(pc->cache[i++]);
  free(pc->cache);
}

static void grow_table
  (void **table,unsigned int *max_size,
   unsigned int delta,unsigned int elt_size)
{
  void *new=realloc(*table,elt_size*(*max_size+delta));
  if (new == NULL) exit(1);
  *table=new; *max_size=*max_size+delta;
}

static void grow_inputs(parse_context pc)
{
  int i=pc->max_n_inputs;
  grow_table((void **)(&(pc->input)),&(pc->max_n_inputs),
	     INITIAL_N_INPUTS,sizeof(struct WORD));
  grow_table((void **)(&(pc->phrases)),&(pc->max_n_phrases),
	     INITIAL_N_INPUTS,sizeof(struct PHRASE));
  pc->cache=
    realloc(pc->cache,sizeof(state_ref *)*pc->max_n_inputs);
  while (i < pc->max_n_inputs) {
    state_ref *vec=u8_malloc(sizeof(state_ref)*n_nodes);
    unsigned int j=0; while (j < n_nodes) vec[j++]=-1;
    pc->cache[i++]=vec;}
}


/* OFSM operations */

static fd_lisp read_file_as_vector(char *file)
{
  fd_dtype_stream s=fd_dtsopen(file,FD_DTSTREAM_READ);
  fd_lisp item=fd_read_dtype((fd_byte_input)s,NULL);
  fd_lisp *elts=u8_malloc(sizeof(fd_lisp)*256), vec;
  int i=0, max_elts=256, not_done=1;
  while (!(FD_EODP(item))) {
    if (i>=max_elts) {
      int new_size=max_elts+256;
      elts=u8_realloc(elts,sizeof(fd_lisp)*max_elts);
      max_elts=new_size;}
    elts[i++]=item;
    item=fd_read_dtype((fd_byte_input)s,NULL);}
  vec=fd_init_vector(NULL,i,elts);
  return vec;
}

static void init_ofsm_data(fd_lisp vector)
{
  fd_lisp nnodes; unsigned int i=0;
  arc_names=fd_incref(FD_VECTOR_REF(vector,0));
  n_arcs=FD_VECTOR_LENGTH(arc_names);
  nnodes=fd_incref(FD_VECTOR_REF(vector,1));
  n_nodes=FD_FIXLISP(nnodes);
  while (i < n_nodes) {
    fd_lisp spec=fd_incref(FD_VECTOR_REF(vector,i+2)); unsigned int j;
    nodes[i].name=FD_VECTOR_REF(spec,0); 
    nodes[i].index=i;
    /* ASSERT(i==FD_FIXLISP(FD_VECTOR_REF(spec,1))); */
    nodes[i].terminal=FD_VECTOR_REF(spec,2); 
    j=0; while (j < n_arcs) {
      fd_lisp arcs=FD_VECTOR_REF(spec,3+j); nodes[i].arcs[j].n_entries=0;
      if (FD_EMPTY_LISTP(arcs)) j++;
      else {
	int size=FD_FIXLISP(FD_CAR(arcs)); fd_lisp ptr=FD_CDR(arcs);
	nodes[i].arcs[j].n_entries=size;
	if (size)
	  nodes[i].arcs[j].entries=calloc(size,sizeof(struct ARC));
	else nodes[i].arcs[j].entries=NULL;
	{int k=0; while (k < size) {
	  fd_lisp head=FD_CAR(ptr); ptr=FD_CDR(ptr);
	  nodes[i].arcs[j].entries[k].measure=FD_FIXLISP(FD_CAR(head));
	  nodes[i].arcs[j].entries[k].target=
	    &(nodes[FD_FIXLISP(FD_CDR(head))]); k++;}}
	j++;}}
      i++;}
}

static void init_tag_class(fd_lisp expr,unsigned char *tag_class)
{
  unsigned int i=0; while (i < MAX_ARCS) tag_class[i++]=0;
  {FD_DOLIST(val,expr) {
    i=0; while (i < n_arcs)
      if (fd_lisp_equal(val,FD_VECTOR_REF(arc_names,i))) {
	tag_class[i]=1; break;} else i++;}
  if (i >= n_arcs) {exit(1);}}
}

static int get_tag_index(u8_string tag)
{
  fd_lisp symbol=fd_intern(tag);
  int i=0; while (i < n_arcs)
    if (FD_EQ((FD_VECTOR_REF(arc_names,i)),symbol)) return i;
    else i++;
  return -1;
}

static int get_string_tag_index(u8_string tag)
{
  fd_lisp symbol=fd_lisp_string(tag);
  int i=0; while (i < n_arcs)
    if (FD_EQUAL((FD_VECTOR_REF(arc_names,i)),symbol)) {
      fd_decref(symbol); return i;}
    else i++;
  fd_decref(symbol);
  return -1;
}

/* Reads the tables used in hooking up phrases to one another. */
static void init_hookup_data(fd_lisp vector)
{
  unsigned int i=0;
  init_tag_class(FD_VECTOR_REF(vector,0),head_tags);
  init_tag_class(FD_VECTOR_REF(vector,1),thing_tags);
  init_tag_class(FD_VECTOR_REF(vector,2),action_tags);
  init_tag_class(FD_VECTOR_REF(vector,3),prefix_tags);
  init_tag_class(FD_VECTOR_REF(vector,4),suffix_tags);
  init_tag_class(FD_VECTOR_REF(vector,5),prep_tags);
  init_tag_class(FD_VECTOR_REF(vector,6),name_tags);
  init_tag_class(FD_VECTOR_REF(vector,7),dangling_tags);
  ing_verb_tag=get_tag_index("ING-VERB");
  inf_verb_tag=get_tag_index("INFINITIVAL-VERB");
  glue_tag=get_tag_index("GLUE");
  conjunction_tag=get_tag_index("CONJUNCTION");
  passive_verb_tag=get_tag_index("PASSIVE-VERB");
  verb_as_adjective_tag=get_tag_index("VERB-AS-ADJECTIVE");
  complement_adjective_tag=get_tag_index("COMPLEMENT-ADJECTIVE");
  pronoun_tag=get_tag_index("PRONOUN");
  anything_tag=get_tag_index("ANYTHING");
  punctuation_tag=get_tag_index("PUNCTUATION");
  sentence_end_tag=get_tag_index("SENTENCE-END");
  comma_tag=get_string_tag_index(",");
  and_tag=get_string_tag_index("and");
  or_tag=get_string_tag_index("or");
}


/* Preprocessing text */

static char abbrevs[]=
  "Corp.Calif.Mass.Ariz.Wash.Mich.Kans.Colo.Neva.Penn.Okla.Sept.Gov.Sen.Rep.Dr.Lt.Col.Gen.Mr.Mrs.Miss.Ms.Co.Inc.Ltd.Jan.Feb.Mar.Apr.Jun.Jul.Aug.Sep.Sept.Oct.Nov.Dec.";

static fd_hashset months, days, closed, company_cues;
static char *months_init[]={
  "Jan","January","Jan.","Feb","February","Feb.",
  "Mar","March","Mar.","April","Apr","Apr.",
  "May","Jun","June","Jun.","Jul","July","Jul.",
  "Aug","August","Aug.",
  "Sep","Sept","September","Sep.","Sept.",
  "Oct","Oct.","October","Nov","Nov.","November",
  "Dec","Dec.","December",NULL};
static char *days_init[]={
  "Monday","Tuesday","Wednesday","Thursday","Friday",
  "Saturday","Sunday",
  "Monday's","Tuesday's","Wednesday's","Thursday's",
  "Friday's","Saturday's","Sunday's",
  "Sun.","Mon.","Tues.","Wed.","Thurs.","Fri.","Sat.",
  NULL};
static char *closed_init[]={
  "When","The","A","An","Of","If","For","From","According",
  "To","By","With","In","At","While","However","Without",
  NULL};
static char *company_cues_init[]={
  "Co.","Corp.","Inc.","&","Ltd.","AG",NULL};

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
  return fd_hashset_get(hs,(fd_lisp)(&stringdata));
}

/* Takes the word which strings string and checks if it is in
   the set words. */
static int check_word(char *string,fd_hashset words)
{
  char buf[32]; int i=0; char *scan=string;
  while ((*scan) && (!(u8_isspace(*scan))) && (i < 32))
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
static int possessivep(char *s)
{
  char a=s[0], b=s[1];
  if ((a == '\0') || (b == '\0')) return 0;
  while (*s) {a=b; b=*s++;}
  if (((a == '\'') && ((b == 's') || (b == 'S'))) ||
      (((a == 's') || (a == 'S')) && (b == '\'')))
    return 1;
  else return 0;
}


/* Getting root forms */

static fd_lisp lower_string(u8_string string)
{
  struct U8_OUTPUT ls; u8_string scan=string; int c;
  U8_INIT_OUTPUT(&ls,32);
  while ((c=u8_sgetc(&scan))>=0) {
    u8_putc(&ls,u8_tolower(c));}
  return fd_init_string(NULL,ls.point-ls.bytes,ls.bytes);
}

/* lexicon_fetch: (static)
    Arguments: a fd_lisp string
    Returns: Fetches the vector stored in the lexicon for the string.
*/
static fd_lisp lexicon_fetch(fd_lisp key)
{
  return fd_index_get(lexicon,key);
}

static fd_lisp lexicon_fetch_lower(u8_string string)
{
  struct U8_OUTPUT out; int ch; u8_string in=string; fd_lisp key, value;
  U8_INIT_OUTPUT(&out,128);
  while ((ch=u8_sgetc(&in))>=0)
    if (u8_isupper(ch)) u8_putc(&out,u8_tolower(ch));
    else u8_putc(&out,ch);
  key=fd_init_string(NULL,out.point-out.bytes,out.bytes);
  value=lexicon_fetch(key); fd_decref(key);
  return value;
}

static fd_lisp get_noun_root(fd_lisp key) 
{
  fd_lisp answer=fd_index_get(noun_roots,key);
  if (FD_EMPTY_CHOICEP(answer)) return answer;
  else if (FD_STRINGP(answer)) return answer;
  else {
    fd_lisp result=FD_VOID;
    FD_DO_CHOICES(elt,answer)
      if (FD_VOIDP(answer)) result=elt;
      else if ((FD_STRINGP(elt)) && ((strchr(FD_STRDATA(elt),' ')) == NULL))
	result=elt;
    fd_incref(result); fd_decref(answer);
    return result;}
}

static fd_lisp get_verb_root(fd_lisp key) 
{
  fd_lisp answer=fd_index_get(verb_roots,key);
  if (FD_EMPTY_CHOICEP(answer)) return answer;
  else if (FD_STRINGP(answer)) return answer;
  else {
    fd_lisp result=FD_VOID;
    FD_DO_CHOICES(elt,answer)
      if (FD_VOIDP(answer)) result=elt;
      else if ((FD_STRINGP(elt)) && ((strchr(FD_STRDATA(elt),' ')) == NULL))
	result=elt;
    fd_incref(result); fd_decref(answer);
    return result;}
}

static fd_lisp root_form(fd_lisp word,unsigned int tag)
{
  fd_lisp normalized, root=FD_EMPTY_CHOICE;
  if (FD_STRINGP(word)) {
    u8_string string=fd_strdata(word);
    int first_char=u8_sgetc(&string);
    if (u8_isupper(first_char))
      normalized=lower_string(fd_strdata(word));
    else normalized=fd_incref(word);}
  else normalized=fd_incref(word);
  if (action_tags[tag]) root=get_verb_root(normalized);
  else if (thing_tags[tag]) root=get_noun_root(normalized);
  else if (tag == verb_as_adjective_tag) root=get_verb_root(normalized);
  if (!(FD_STRINGP(root))) return normalized;
  else {fd_decref(normalized); return root;}
}


/* Sentences into words */

static int add_input(parse_context pc,u8_string s);
static void add_punct(parse_context pc,u8_string s);

static u8_string process_word(parse_context pc,u8_string input)
{
  struct U8_OUTPUT word_stream; 
  u8_string tmp=input; int ch=u8_sgetc(&input);
  U8_INIT_OUTPUT(&word_stream,16);
  while (ch>=0) 
    if (u8_isalnum(ch)) { /* Keep going */
      u8_putc(&word_stream,ch); tmp=input; ch=u8_sgetc(&input);}
    else if (u8_isspace(ch)) { /* Unambiguous word terminator */
      if ((word_stream.point-word_stream.bytes) < 40)
	add_input(pc,word_stream.bytes);
      else free(word_stream.bytes);
      return tmp;}
    else { /* Ambiguous word terminator (punctuation) */
      int next_char=u8_sgetc(&input);
      if (u8_isalnum(next_char)) {
	/* If the next token is alpha numeric, keep it as one word */
	u8_putc(&word_stream,ch); u8_putc(&word_stream,next_char);
	tmp=input; ch=u8_sgetc(&input);}
      else { /* Otherwise, record the word you just saw and return
		a pointer to the start of the terminator */
	if ((word_stream.point-word_stream.bytes) < 40)
	  add_input(pc,word_stream.bytes);
	else free(word_stream.bytes);
	return tmp;}}
  if ((word_stream.point-word_stream.bytes) < 40)
    add_input(pc,word_stream.bytes);
  else free(word_stream.bytes);
  return tmp;
}

static int ispunct_sorta(int ch)
{
  return
    ((ch>0) && (!(u8_isalnum(ch) || (u8_isspace(ch)))));
}

static u8_string process_punct(parse_context pc,u8_string input)
{
  struct U8_OUTPUT word_stream; 
  int ch=u8_sgetc(&input); u8_string tmp=input; 
  U8_INIT_OUTPUT(&word_stream,16);
  while ((ch>0) && (ispunct_sorta(ch))) {
    tmp=input; u8_putc(&word_stream,ch); ch=u8_sgetc(&input);}
  add_punct(pc,word_stream.bytes);
  /* if (word_stream.size > 5)  
     else free(word_stream.ptr); */
  return tmp;
}

static u8_string skip_whitespace(u8_string input)
{
  u8_string tmp=input; int ch=u8_sgetc(&input);
  while (u8_isspace(ch)) {
    tmp=input; ch=u8_sgetc(&input);}
  return tmp;
}

static u8_string skip_markup(u8_string input)
{
  u8_string tmp=input; int ch=u8_sgetc(&input);
  if (ch=='<') while ((ch>=0) && (ch!='>')) {
    tmp=input; ch=u8_sgetc(&input);}
  return input;
}

static u8_string skip_wordbreak(u8_string input)
{
  u8_string point=input;
  while (1)
    if (*point=='<') point=skip_markup(point);
    else if (u8_isspace(*point)) point=skip_whitespace(point);
    else return point;
}

static void breakup_sentence(parse_context pc,u8_string original)
{
  u8_string scan=original; int ch=u8_sgetc(&scan);
  scan=original; while (ch>=0) {
    u8_string tmp;
    if (u8_isspace(ch)) scan=skip_wordbreak(scan);
    else if (ch=='<') scan=skip_wordbreak(scan);    
    else if (u8_isalnum(ch)) scan=process_word(pc,scan);
    else scan=process_punct(pc,scan);
    tmp=scan; ch=u8_sgetc(&scan); scan=tmp;}
}


/* Identifying compounds */

static void identify_compounds(parse_context pc)
{
  int start=1, i=0; while (i < pc->n_inputs) {
    fd_lisp lower=FD_EMPTY_CHOICE, lowerw=FD_EMPTY_CHOICE;
    fd_lisp dbl=FD_EMPTY_CHOICE, triple=FD_EMPTY_CHOICE;
    fd_lisp dblw=FD_EMPTY_CHOICE, triplew=FD_EMPTY_CHOICE;
    int space=pc->n_inputs-i, fc=u8_string_ref(pc->input[i].spelling);
    if ((start) && (u8_isupper(fc))) {
      fd_lisp str=lower_string(pc->input[i].spelling);
      lower=fd_make_list(1,str); lowerw=lexicon_fetch(str);}
    if ((fc == '"') || (fc == '\'')) start=1; else start=0;
    if (space >= 2) {
      dbl=fd_make_list
	(2,fd_incref(pc->input[i].lstr),fd_incref(pc->input[i+1].lstr));
      dblw=lexicon_fetch(dbl);}
    if (space >= 3) {
      triple=fd_make_list
	(3,fd_incref(pc->input[i].lstr),fd_incref(pc->input[i+1].lstr),
	 fd_incref(pc->input[i+2].lstr));
      triplew=lexicon_fetch(triple);}
    if (!(FD_EMPTY_CHOICEP(lowerw))) {
      fd_lisp entry=fd_init_pair(NULL,lower,fd_incref(lowerw));
      FD_ADD_TO_CHOICE(pc->input[i].compounds,entry);}
    if (!(FD_EMPTY_CHOICEP(dblw))) {
      fd_lisp entry=fd_init_pair(NULL,dbl,fd_incref(dblw));
      FD_ADD_TO_CHOICE(pc->input[i].compounds,entry);}
    else fd_decref(dbl);
    if (!(FD_EMPTY_CHOICEP(triplew))) {
      fd_lisp entry=fd_init_pair(NULL,triple,fd_incref(triplew));
      FD_ADD_TO_CHOICE(pc->input[i].compounds,entry);}
    else fd_decref(triple);
    i++;}
}


/* Strings into sentences */

static char *find_sentence_end(u8_string string)
{
  u8_string start=string;
  if (string == NULL) return NULL;
  else if (*string == '\0') return NULL;
  /* Go till you get to a possible terminator */
  while ((*string) && (strchr("?!;:.\n<",*string) == NULL)) string++;
  /* If you're at the end, you're there. */
  if (*string == '\0') return string;
  /* If you're at a newline and the end of the string,
     you've found it. */
  else if ((*string == '\n') && (string[1] == '\0'))
    return string;
  /* Otherwise, at a newline, check to see if it is really a
     blank line, which we will take as a sentence end. */
  else if (*string == '\n') {
    u8_string next=utf8_next(string);
    int c=utf8_point(next);
    /* If the next character is not a space, the newline isn't
       a sentence end, so keep looking. */
    if (!(u8_isspace(c)))
      return find_sentence_end(utf8_next(string));
    /* if the next character was a space, then skip ahead over
       whitespace.  If you find either another newline or the
       end of the string, there's an entirely blank line, so we
       take it as a sentence end.. */
    else {
      u8_string last=next;
      while (u8_isspace(c)) {
	last=next; c=u8_sgetc(&next);
	if (c == '\n') return last;}
      if ((c == '\n') || (c <= 0)) return next;
      else return find_sentence_end(next);}}
  /* if the next character is the end of the string, that's the end. */
  else if (string[1] == '\0') return utf8_next(string);
  /* if the next character starts certain markup, start there. */
  else if (*string == '<')
    if (strncmp(string,"<BODY>",6) == 0) return string;
    else if (strncmp(string,"<LI>",4) == 0)  return string;
    else if (strncmp(string,"</UL",4) == 0)  return string;
    else return find_sentence_end(utf8_next(string));
  /* if the next character is a quote, take the period seriously. */
  else {
    int nextc=utf8_point(utf8_next(string));
    if ((nextc == '\'') || (nextc == '"')) {
      u8_string last=string;
      while (isquote(nextc)) {
	last=string; nextc=u8_sgetc(&string);}
      return last;}
    /* if the next character isn't a space, ignore the embedded
       punctuation */
    else if (!(u8_isspace(nextc)))
      return find_sentence_end(utf8_next(string));
    /* If the character isn't a period, take it as the break */
    else if (*string != '.') return utf8_next(string);
    /* Now we try and guess whether or not it is an abbreviation.
       This is probably more complicated than it should be. */
    else {
      u8_string prev=string-1, last; int c;
      /* Now go backwards one utf8 char */
      while ((prev>start) && (*prev < 0xc0) && (*prev > 0x80)) prev--;
      if (u8_isupper(utf8_point(prev)))
	return find_sentence_end(utf8_next(string));
      /* Now keep going backwards over non-whitespace. */
      while ((prev>=start) && (!(u8_isspace(c=utf8_point(prev))))) {
	/* If the word has embedded periods, it is probably an
	   abbreviation, so don't take it as a sentence end. */
	if (c == '.') return find_sentence_end(utf8_next(string));
	last=prev; prev--;
	while ((prev>start) && (*prev < 0xc0) && (*prev > 0x80))
	  prev--;}
      /* If the word before the period (the last word in the putative
	 sentence) is capitalized, take it as an abbreviation if it is
	 either in abbrevs */
      if (!(u8_isupper(utf8_point(prev))))
	/* The previous word isn't capitalized, so we'e at sentence end. */
	return utf8_next(string);
      /* The previous word is capitalized, so check for do more
	 checking to see if it might be an abbreviation */
      else {
	fd_lisp key=fd_extract_string(NULL,prev,string);
	u8_string result=strstr(abbrevs,FD_STRDATA(key));
	if (result) {
	  fd_decref(key); return find_sentence_end(utf8_next(string));}
	else if (chopper_initialized) {
	  fd_lisp v=lexicon_fetch(key);
	  fd_decref(key); 
	  if (FD_EMPTY_CHOICEP(v)) return utf8_next(string);
	  else return find_sentence_end(utf8_next(string));}
	else {
	  fd_decref(key); return find_sentence_end(utf8_next(string));}}}}
}


/* Lexicon access */

static int normal_capitalization=1;

/* add_input: (static)
    Arguments: a parse context and a word string
    Returns: Adds the string to the end of the current sentence.
*/
static int add_input(parse_context pc,u8_string spelling)
{
  u8_string s=strdup(spelling);
  fd_lisp ls=fd_init_string(NULL,-1,s), key, value; 
  int first_char=u8_sgetc(&spelling);
  int capitalized=0, capitalized_in_lexicon=0, i;
  if ((word_limit > 0) && (((int)pc->n_inputs) >= word_limit))
    return fd_reterr(TooManyWords,"add_input",NULL,FD_VOID);
  if (pc->n_inputs+4 >= pc->max_n_inputs) grow_inputs(pc);
  /* Initialize this word's weights */
  i=0; while (i < MAX_ARCS) pc->input[pc->n_inputs].weights[i++]=255;
  if ((u8_isupper(first_char)) && (normal_capitalization))
    capitalized=1;
  if (possessivep(spelling))
    if (capitalized) {
      value=lexicon_fetch(sproper_possessive); 
      capitalized_in_lexicon=1;}
    else value=lexicon_fetch(spossessive);
  else if (isdigit(first_char))
    if (strchr(spelling,'-')) value=lexicon_fetch(sscore);
    else value=lexicon_fetch(snumber);
  else if ((first_char == '$') && (u8_isdigit(u8_sgetc(&spelling))))
    value=lexicon_fetch(sdollars);
  else {capitalized_in_lexicon=capitalized; value=lexicon_fetch(ls);}
  if (FD_EMPTY_CHOICEP(value)) {
    capitalized_in_lexicon=0; value=lexicon_fetch_lower(s);}
  /* Ignore capitalization if you don't recognize the word */
  if (FD_EMPTY_CHOICEP(value)) capitalized=0;
  if (!(FD_EMPTY_CHOICEP(value))) {}
  else if (isupper(first_char)) {
    value=lexicon_fetch(sproper_name);
    capitalized_in_lexicon=1;}
  else if (strchr(spelling,'-'))
    value=lexicon_fetch(sdashed_adjective);
  else { /* Morphology cheats */
    int l=strlen(s);
    if ((l>2) && (strcmp(s+(l-2),"ed")==0))
      value=lexicon_fetch(ed_word);
    else if ((l>2) && (strcmp(s+(l-2),"ly")==0))
      value=lexicon_fetch(ly_word);
    else if ((l>3) && (strcmp(s+(l-3),"ing")==0))
      value=lexicon_fetch(ing_word);
    else value=lexicon_fetch(sstrange_word);}
  if (FD_PAIRP(value)) value=FD_CAR(value);
  if (!(FD_VECTORP(value))) value=lexicon_fetch(sstrange_word);
  i=0; while (i < n_arcs) {
    fd_lisp wt=FD_VECTOR_REF(value,i);
    if (FD_FIXNUMP(wt))
      pc->input[pc->n_inputs].weights[i++]=FD_FIXLISP(wt);
    else if (FD_TRUEP(wt))
      pc->input[pc->n_inputs].weights[i++]=0;
    else pc->input[pc->n_inputs].weights[i++]=255;}
  pc->input[pc->n_inputs].spelling=s;
  pc->input[pc->n_inputs].lstr=ls;
  pc->input[pc->n_inputs].compounds=FD_EMPTY_CHOICE;  
  pc->input[pc->n_inputs].next=pc->n_inputs+1;
  if ((capitalized) && (!(capitalized_in_lexicon)) &&
      (pc->n_inputs>0)) {
    int i=0; while (i < n_arcs)
      if ((name_tags[i]) &&
	  (pc->input[pc->n_inputs].weights[i]==255)) {
	pc->input[pc->n_inputs].weights[i]=2; i++;}
      else i++;}
  if (!(u8_isalnum(first_char)))
    pc->input[pc->n_inputs].weights[punctuation_tag]=1;
  pc->n_inputs++; total_inputs++;
  return 1;
}

/* add_punct: (static)
    Arguments: a parse context and a punctuation string
    Returns: Adds the string to the end of the current sentence.
*/
static void add_punct(parse_context pc,u8_string spelling)
{
  u8_string s=strdup(spelling); int i;
  fd_lisp ls=fd_init_string(NULL,-1,s), key, value;
  value=lexicon_fetch(ls);
  if (FD_EMPTY_CHOICEP(value))
    value=lexicon_fetch(punctuation_symbol);
  if (FD_PAIRP(value)) value=FD_CAR(value);
  if (!(FD_VECTORP(value))) value=lexicon_fetch(sstrange_word);
  i=0; while (i < n_arcs) {
    fd_lisp wt=FD_VECTOR_REF(value,i);
    if (FD_FIXNUMP(wt))
      pc->input[pc->n_inputs].weights[i++]=FD_FIXLISP(wt);
    else if (FD_TRUEP(wt))
      pc->input[pc->n_inputs].weights[i++]=0;
    else pc->input[pc->n_inputs].weights[i++]=255;}
  pc->input[pc->n_inputs].spelling=s;
  pc->input[pc->n_inputs].lstr=ls;
  pc->input[pc->n_inputs].compounds=FD_EMPTY_CHOICE;  
  pc->input[pc->n_inputs].next=pc->n_inputs+1;
  pc->n_inputs++; total_inputs++;
}


/* Queue operations */

/* print_state: (static)
    Arguments: an operation identifer (a string), a state, and a FILE
    Returns: nothing
  Prints out a representation of the transition into the state.
*/
static void print_state(parse_context pc,char *op,state_ref sr,FILE *f)
{
  struct STATE *s=&(pc->states[sr]);
  u8_message("%q ==> \n%s d=%d i=%d(%q) t=%q n=%q\n",
	     FD_VECTOR_REF(arc_names,s->arc),
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
static void check_queue_integrity(parse_context pc)
{
  return;
#if 0
  struct STATE *s; unsigned int i=0; s=pc->queue;
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
static void queue_push(parse_context pc,state_ref new)
{
#if TRACING
  if (tracing_tagger) print_state(pc,"Pushing",new,stderr);
#endif
  if (pc->queue == -1) pc->queue=new;
  else {
    state_ref *last=&(pc->queue), next=pc->queue, prev=-1;
    struct STATE *elt=&((pc->states)[next]), *s=&((pc->states)[new]);
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

static void queue_reorder(parse_context pc,state_ref changed)
{
  if (pc->queue == changed)  return;
  else if (changed < 0) return; /* Should never be reached */
  else {
    struct STATE *s=&(pc->states[changed]);
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
  (parse_context pc, struct NODE *n,int distance,int in,
   state_ref origin,int arc,fd_lisp word)
{
  state_ref kref=(pc->cache[in])[n->index];
  if (kref < 0) {
    struct STATE *new;
    if (pc->n_states >= pc->max_n_states)
      grow_table((void **)(&pc->states),&pc->max_n_states,
		 INITIAL_N_STATES,sizeof(struct STATE));
    new=&(pc->states[pc->n_states]);
    new->node=n; new->distance=distance; new->input=in;
    new->previous=origin; new->arc=arc; new->word=word;
    new->self=pc->n_states; new->qnext=-1; new->qprev=-1;
    (pc->cache[in])[n->index]=pc->n_states;
    pc->n_states++;
    queue_push(pc,new->self);}
  else {
    struct STATE *known=&(pc->states[kref]);
    if (distance < known->distance) {
      known->previous=origin; known->arc=arc;
      known->distance=distance; known->word=word;
      queue_reorder(pc,kref);}}
}

static void expand_state_on_word
  (parse_context pc,state_ref sr,struct WORD *wd)
{
  struct STATE *s=&(pc->states[sr]); struct NODE *n=s->node;
  unsigned int i=1, in=s->input, dist=s->distance;
  while (i < n_arcs)
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

static void expand_state_on_compound
  (parse_context pc,state_ref sr,fd_lisp compound)
{
  struct STATE *s=&(pc->states[sr]); struct NODE *n=s->node;
  unsigned int i=1, in=s->input, dist=s->distance;
  fd_lisp weights=FD_CDR(compound);
  while (i < n_arcs) {
    fd_lisp weight=FD_VECTOR_REF(weights,i);
    if ((FD_FIXNUMP(weight)) && (n->arcs[i].n_entries > 0)) {
      int j=0, limit=n->arcs[i].n_entries, len=fd_seq_length(FD_CAR(compound));
      while (j < limit) {
	add_state
	  (pc,n->arcs[i].entries[j].target,
	   dist+n->arcs[i].entries[j].measure+FD_FIXLISP(weight),
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
static void expand_state(parse_context pc,state_ref sr)
{
  struct STATE *s=&(pc->states[sr]);
  struct NODE *n=s->node; unsigned int i=1, in=s->input, dist=s->distance;
#if TRACING
  if (tracing_tagger) print_state(pc,"Expanding",sr,stderr);
#endif
  if (in < pc->n_inputs) {
    expand_state_on_word(pc,sr,((struct WORD *)&(pc->input[in])));
    if (!(FD_EMPTY_CHOICEP(pc->input[in].compounds))) {
      FD_DO_CHOICES(compound,pc->input[in].compounds) {
	expand_state_on_compound(pc,sr,compound);}}
    /* Try skipping this input */
    add_state(pc,n,dist+5,in+1,sr,anything_tag,pc->input[in].lstr);}
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
static state_ref queue_extend(parse_context pc)
{
  state_ref tref=pc->queue; 
  struct STATE *top;
  if (tref >= 0) {
    top=&(pc->states[tref]); pc->last=tref; pc->queue=top->qnext;}
  else return pc->last;
  if ((top->input == pc->n_inputs) && (!(ZEROP(top->node->terminal))))
    return tref;
  else if (top->input > pc->n_inputs) return -1;
  else {expand_state(pc,tref); return -1;}
}

/* queue_run: (static)
     Arguments: none
     Returns: a state
   Repeatedly extends the queue until a terminal state is reached,
    returning that state.
*/
static state_ref queue_run(parse_context pc)
{
  state_ref answer; 
  while ((answer=queue_extend(pc)) < 0);
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

/* Tags the input based on the parse that led to a state */
static void tag_input(parse_context pc,state_ref s)
{
  int last=-1; int i=0;
  while (i < pc->n_inputs) {
    pc->input[i].tag=0;
    pc->input[i].w=0;
    pc->input[i].d=0;
    pc->input[i].lword=FD_VOID;
    i++;}
  while (s >= 0) {
    struct STATE *state=&(pc->states[s]);
    if (state->arc == 0) s=state->previous;
    else {
      if (last >= 0) {
	pc->input[state->input-1].next=last;
	pc->input[last].previous=state->input-1;}
      else pc->input[state->input].next=pc->n_inputs;
      pc->input[state->input-1].tag=state->arc;
      pc->input[state->input-1].d=state->distance;
      if (state->previous>0)
	pc->input[state->input-1].w=
	  state->distance-(pc->states[state->previous].distance);
      else pc->input[state->input-1].w=state->distance;
      pc->input[state->input-1].lword=state->word;
      pc->input[state->input-1].previous=-1;
      last=state->input-1; s=state->previous;}}
}


/* Constructing phrases */

static void add_extra(parse_context pc,phrase_ref p,int i)
{
  struct PHRASE *phrase=&(pc->phrases[p]);
  if (phrase->n_extras >= phrase->max_n_extras)
    grow_table((void **)(&(phrase->extras)),&(phrase->max_n_extras),
	       INITIAL_N_EXTRAS,sizeof(int));
  phrase->extras[phrase->n_extras++]=i; 
}

static void construct_phrases(parse_context pc,state_ref s)
{
  int j, k, last_k;
  phrase_ref last_action=-1; fd_lisp dangling_prep=FD_EMPTY_CHOICE;
  tag_input(pc,s); 
  j=0; while (pc->input[j].tag == 0) j++;
  while (j < pc->n_inputs)
    if ((dangling_tags[pc->input[j].tag]) && (last_action >= 0)) {
      /* This handles dangling adverbs and time references */
      add_extra(pc,last_action,j); j=next_input(pc,j);}
    else if ((pc->input[j].tag == conjunction_tag) &&
	     (pc->n_phrases > 0) &&
	     (!(FD_EMPTY_CHOICEP(pc->phrases[pc->n_phrases-1].prep)))) {
      /* This handles cojoined prepositions
	 (e.g in the morning and afternon) */
      dangling_prep=pc->phrases[pc->n_phrases-1].prep; j=next_input(pc,j);}
    else if (head_tags[pc->input[j].tag]) {
      /* This handles all other heads */
      pc->phrases[pc->n_phrases].head=j;
      pc->phrases[pc->n_phrases].tag=pc->input[j].tag;
      pc->phrases[pc->n_phrases].root=root_form(pc->input[j].lword,pc->input[j].tag);
      pc->phrases[pc->n_phrases].subject_of=-1;
      pc->phrases[pc->n_phrases].last_action=last_action;
      /* Set the value last_action for subsequent phrases */
      if (action_tags[pc->input[j].tag]) last_action=pc->n_phrases;
      /* This hooks up conjoined prepositions (see above) */
      if (thing_tags[pc->input[j].tag]) 
	pc->phrases[pc->n_phrases].prep=dangling_prep;
      else pc->phrases[pc->n_phrases].prep=FD_EMPTY_CHOICE;
      dangling_prep=FD_EMPTY_CHOICE;
      /* Collect prefix of phrase */
      last_k=j; k=prev_input(pc,j);
      while ((k >= 0) && (prefix_tags[pc->input[k].tag])) {
	if (prep_tags[pc->input[k].tag]) 
	  pc->phrases[pc->n_phrases].prep=make_slot(pc->input[k].lword);
	last_k=k; k=pc->input[k].previous;}
      pc->phrases[pc->n_phrases].start=last_k;
      /* Collect postfix of phrase */
      last_k=j; k=next_input(pc,j);
      while ((k < pc->n_inputs) && (suffix_tags[pc->input[k].tag])) {
	last_k=k; k=next_input(pc,k);}
      pc->phrases[pc->n_phrases].end=last_k;
      /* Initialize links and other fields */
      pc->phrases[pc->n_phrases].n_links=0;
      pc->phrases[pc->n_phrases].max_n_links=INITIAL_N_LINKS;
      pc->phrases[pc->n_phrases].links=
	malloc(sizeof(struct PHRASE_LINK)*INITIAL_N_LINKS);
      pc->phrases[pc->n_phrases].n_extras=0;
      pc->phrases[pc->n_phrases].max_n_extras=INITIAL_N_EXTRAS;
      pc->phrases[pc->n_phrases].extras=
	malloc(sizeof(int)*INITIAL_N_EXTRAS);
      pc->phrases[pc->n_phrases].frame=FD_EMPTY_CHOICE;
      /* Initialize subject fields */
      if (action_tags[pc->phrases[pc->n_phrases].tag]) {
	int k=pc->n_phrases-1;
	while ((k >= 0) && (!(action_tags[pc->phrases[k].tag])))
	  if ((thing_tags[pc->phrases[k].tag]) &&
	      (FD_EMPTY_CHOICEP(pc->phrases[k].prep))) {
	    pc->phrases[k].subject_of=pc->n_phrases; k--;}
	  else k--;}
      pc->n_phrases++; j=next_input(pc,j);}
    else j=next_input(pc,j);
}


/* Linking phrases */

static int link_phrases /* static */
  (parse_context pc,phrase_ref left_phrase,fd_lisp link,phrase_ref right_phrase)
{
  struct PHRASE *left, *right; int i=0;
  if ((left_phrase < 0) || (right_phrase < 0)) return;
  if ((left_phrase > ((int)pc->n_phrases)) || 
      (right_phrase > ((int)pc->n_phrases)))
    return fd_reterr(InternalParseError,"link_phrases","phrase out of bounds",
		     FD_VOID);
  left=&(pc->phrases[left_phrase]);
  right=&(pc->phrases[right_phrase]);
  while (i < left->n_links)
    if ((FD_EQ(left->links[i].tag,link)) &&
	(left->links[i].ptr == right_phrase))
      return;
    else i++;
  if (left->n_links >= left->max_n_links)
    grow_table((void **)(&(left->links)),&(left->max_n_links),
	       INITIAL_N_LINKS,sizeof(struct PHRASE_LINK));
  left->links[left->n_links].tag=link; FD_CHECK_PTR(link);
  left->links[left->n_links++].ptr=right_phrase;
  if (right->n_links >= right->max_n_links)
    grow_table((void **)(&(right->links)),&(right->max_n_links),
	       INITIAL_N_LINKS,sizeof(struct PHRASE_LINK));
  right->links[right->n_links].tag=inverse_slotid(link); 
  FD_CHECK_PTR(right->links[right->n_links].tag);
  right->links[right->n_links++].ptr=left_phrase;
  return 0;
}

#define action_phrasep(x) (action_tags[(x)->tag])
#define passive_phrasep(x) \
  ((((x)->tag) == passive_verb_tag) || (((x)->tag) == complement_adjective_tag))
#define thing_phrasep(x) (thing_tags[(x)->tag])

static fd_lisp get_links(struct PHRASE *p)
{
  return fd_index_get(vlinks,p->root);
}

static void hookup_phrases(parse_context pc)
{
  int i=0, j;
  while (i < pc->n_phrases) {
    struct PHRASE *p=&(pc->phrases[i]); fd_lisp links=get_links(p);
    int gives_inf_subj=0, gives_inf_obj=0, gives_ing_subj=0, gives_ing_obj=0;
    int takes_ing=0, takes_inf=0;
    int is_action=action_phrasep(p), is_passive=passive_phrasep(p);
    int close_noun=-1, one_object=0, seen_second_action=0;
    /* This figures out how to propogate subject/object args to verb arguments */
    if (is_action)
      if (FD_EMPTY_CHOICEP(links)) {takes_inf=1; takes_ing=1;}
      else  {
	FD_DO_CHOICES(each,links) 
	  if (FD_EQ(each,gfs)) {gives_inf_subj=1; takes_inf=1;}
	  else if (FD_EQ(each,gfo)) {gives_inf_obj=1; takes_inf=1;}
	  else if (FD_EQ(each,ggs)) {gives_ing_subj=1; takes_ing=1;}
	  else if (FD_EQ(each,ggo)) {gives_ing_obj=1; takes_ing=1;}
	  else if (FD_EQ(each,tg)) takes_ing=1;
	  else if (FD_EQ(each,tf)) takes_inf=1;
	fd_decref(links);}
    else fd_decref(links);
    /* This hooks up last_action chains and also makes glue connections */
    if ((is_action) && (p->last_action >= 0)) {
      int word_ptr=pc->phrases[p->last_action].head;
      while (word_ptr < p->head) {
	if (pc->input[word_ptr].tag == glue_tag) 
	  link_phrases(pc,p->last_action,make_slot(pc->input[word_ptr].lword),i);
	word_ptr=next_input(pc,word_ptr);}
      link_phrases(pc,p->last_action,next_act_slotid,i);}
    /* This makes guesses about subject relations, assuming that
       prepositional phrases can't be subjects */
    j=i-1; if (is_action)
      while (j >= 0) {
	struct PHRASE *earlier=&(pc->phrases[j]);
	if ((thing_phrasep(earlier)) && (FD_EMPTY_CHOICEP(earlier->prep))) {
	  if (close_noun < 0) close_noun=j;
	  if (is_passive == 0)
	    link_phrases(pc,i,possible_subject_slotid,j);}
	j--;}
    /* This handles the objects of passives */
    if ((is_passive) && (close_noun>=0)) {
      link_phrases(pc,i,tight_object_slotid,close_noun);
      link_phrases(pc,i,possible_argument_slotid,close_noun);
      link_phrases(pc,i,possible_object_slotid,close_noun);
      if (p->tag == complement_adjective_tag) {
	add_extra(pc,close_noun,p->head);}}
    /* This hooks up all the later phrases as appropriate, including
       handling prepositions, verb arguments, and passive subjects */
    j=i+1; while (j < pc->n_phrases) {
      struct PHRASE *later=&(pc->phrases[j]);
      if (action_phrasep(later)) seen_second_action=1;
      if ((is_passive) && (FD_EQ(later->prep,by_symbol))) {
	link_phrases(pc,i,possible_subject_slotid,j);
	link_phrases(pc,i,likely_subject_slotid,j);}
      if (FD_EQ(later->prep,of_symbol)) {
	if ((FD_STRINGP(p->root)) && ((strcmp(FD_STRDATA(p->root),"accuse")) == 0))
	  link_phrases(pc,i,of_symbol,j);
	if (i == j-1) link_phrases(pc,i,of_symbol,j);}
      else if (((is_action) || (!(seen_second_action))) &&
	       (!(FD_EMPTY_CHOICEP(later->prep)))) {
	link_phrases(pc,i,later->prep,j);
	link_phrases(pc,i,possible_argument_slotid,j);}
      else if ((is_action) && (!(seen_second_action))) {
	link_phrases(pc,i,possible_argument_slotid,j);
	link_phrases(pc,i,possible_object_slotid,j);
	if (one_object == 0) {
	  link_phrases(pc,i,tight_object_slotid,j); one_object=1;}}
      else {}
      /* This handles verbs which take verbs as arguments and
	 passing on their arguments appropriately */
      if (((takes_ing) && (later->tag == ing_verb_tag)) ||
	  ((takes_inf) && (later->tag == inf_verb_tag))) {
	link_phrases(pc,i,verb_arg,j); /* verb arg */
	if ((close_noun >= 0) && (one_object == 0) && (is_passive == 0))
	  link_phrases(pc,j,likely_subject_slotid,close_noun);}
      j++;}
    if ((close_noun >= 0) && (is_passive == 0))
      link_phrases(pc,i,likely_subject_slotid,close_noun);
    i++;}
}


/* Display/converting tag results */

static fd_lisp get_root(fd_lisp base,int arcid)
{
  if (FD_STRINGP(base)) {
    fd_lisp normalized, result;
    u8_string scan=FD_STRDATA(base);
    int firstc=u8_sgetc(&scan);
    if (name_tags[arcid]) return fd_incref(base);
    else if (u8_isupper(firstc)) 
      normalized=lower_string(FD_STRDATA(base));
    else normalized=fd_incref(base);
    if (action_tags[arcid])
      result=get_verb_root(normalized);
    else if (thing_tags[arcid])
      result=get_noun_root(normalized);
    else if (arcid == verb_as_adjective_tag)
      result=get_verb_root(normalized);
    else return normalized;
    if (FD_EMPTY_CHOICEP(result)) return normalized;
    else {
      fd_decref(normalized);
      return result;}}
  else if (!(FD_PAIRP(base)))
    return fd_type_error(_("word"),"get_root",base);
  else if (thing_tags[arcid]) {
    int len=fd_seq_length(base);
    fd_lisp last_word=fd_seq_elt(base,len-1);
    fd_lisp root=get_root(last_word,arcid);
    if (FD_LISP_EQUAL(last_word,root)) {
      fd_decref(last_word); fd_decref(root);
      return fd_incref(base);}
    else {
      fd_lisp lst=fd_init_pair(NULL,root,FD_EMPTY_LIST);
      int i=fd_seq_length(base)-2;
      while (i>=0) {
	fd_lisp elt=fd_seq_elt(base,i);
	lst=fd_init_pair(NULL,elt,lst); i--;}
      fd_decref(last_word);
      return lst;}}
  else if (action_tags[arcid]) {
    fd_lisp first_word=fd_seq_elt(base,0);
    fd_lisp root=get_root(first_word,arcid);
    if (FD_LISP_EQUAL(first_word,root)) {
      fd_decref(first_word); fd_decref(root);
      return fd_incref(base);}
    else {
      fd_lisp rest=FD_CDR(base);
      fd_decref(first_word);
      return fd_init_pair(NULL,root,fd_incref(rest));}}
  else return fd_incref(base);
}

static fd_lisp gather_tags(parse_context pc,state_ref s)
{
  fd_lisp answer=FD_EMPTY_LIST;
  while (s >= 0) {
    struct STATE *state=&(pc->states[s]);
    if (state->arc == 0) s=state->previous;
    else {
      /* fd_lisp word=fd_incref(pc->input[state->input-1].lstr); */
      fd_lisp word=fd_incref(state->word);
      fd_lisp tag=FD_VECTOR_REF(arc_names,state->arc);
      fd_lisp word_entry=
	fd_make_vector(4,word,fd_incref(tag),
		       get_root(word,state->arc),
		       FD_LISPUSHORT(state->distance));
      answer=fd_init_pair(NULL,word_entry,answer);
      s=state->previous;}}
  return answer;
}


/* Display/converting phrase results */

#define phrase_root(pc,p)  (fd_incref(pc->phrases[p].root))

static fd_lisp gather_phrases(parse_context pc)
{
  fd_lisp result=fd_init_vector(NULL,pc->n_phrases,NULL);
  int i=pc->n_phrases-1; 
  while (i >= 0) {
    int j=pc->phrases[i].end, jmin=pc->phrases[i].start;
    int extra=pc->phrases[i].n_extras-1;
    fd_lisp phrase=FD_EMPTY_LIST;
    while (extra >= 0) {
      fd_lisp word=fd_init_vector(NULL,2,NULL);
      int wd=pc->phrases[i].extras[extra];
      fd_lisp spelling=fd_incref(pc->input[wd].lword);
      FD_VECTOR_SET(word,0,spelling);
      FD_VECTOR_SET(word,1,fd_incref(FD_VECTOR_REF(arc_names,pc->input[wd].tag)));
      phrase=fd_init_pair(NULL,word,phrase);
      extra--;}
    while (j >= jmin) {
      fd_lisp word=pc->input[j].lword, root=root_form(word,pc->input[j].tag);
      fd_lisp tagged_word=fd_init_vector(NULL,5,NULL);
      FD_VECTOR_SET(tagged_word,0,fd_incref(word));
      FD_VECTOR_SET(tagged_word,1,fd_incref(FD_VECTOR_REF(arc_names,pc->input[j].tag)));
      FD_VECTOR_SET(tagged_word,2,root);
      FD_VECTOR_SET(tagged_word,3,FD_LISPFIX(pc->input[j].w));
      FD_VECTOR_SET(tagged_word,4,FD_LISPFIX(pc->input[j].d));
      phrase=fd_init_pair(NULL,tagged_word,phrase);
      j=prev_input(pc,j);}
    FD_VECTOR_SET(result,i,phrase); i--;}
  return result;
}


/* keytuples functions */

static fd_hashset keytuple_stops;

static fd_lisp extract_tuples(parse_context pc,int heads,int phrases,int rels);

static fd_lisp add_keytuple_stop(fd_lisp string)
{
  fd_hashset_add(keytuple_stops,string);
  return FD_VOID;
}

FD_EXPORT
fd_lisp fd_get_keytuples_x(char *original,int heads,int phrases,int rels)
{
  fd_lisp results=FD_EMPTY_CHOICE;
  char *copy=strdup(original), *text=copy, *end; text=skip_whitespace(text);
  while ((text) && (end=find_sentence_end(text))) {
    state_ref final; char sbreak=*end; 
    struct PARSE_CONTEXT parse_context;
    init_parse_context(&parse_context); *end='\0';
    breakup_sentence(&parse_context,text); identify_compounds(&parse_context);
    add_state(&parse_context,&(nodes[0]),0,0,-1,0,FD_VOID);
    final=queue_run(&parse_context); 
    if (final >= 0) {
      fd_lisp tuples;
      construct_phrases(&parse_context,final);
      hookup_phrases(&parse_context);
      tuples=extract_tuples(&parse_context,heads,phrases,rels);
      FD_ADD_TO_CHOICE(results,tuples);}
    free_parse_context(&parse_context);
    if (sbreak) {
      *end=sbreak; text=skip_whitespace(utf8_next(end));}
    else text=end;}
  free(copy);
  return results;
}

FD_EXPORT
fd_lisp fd_get_keytuples(char *original)
{
  return fd_get_keytuples_x(original,1,1,1);
}

static int inversep(fd_lisp slot)
{
  int i=0; while (i < n_inverses)
    if (FD_EQ(slot,inverses[i].inverse))
      return 1;
    else i++;
  return 0;
}

static fd_lisp extract_tuples
  (parse_context pc,int heads,int phrases,int rels)
{
  fd_lisp tuples=FD_EMPTY_CHOICE; int i=pc->n_phrases-1, j, jlimit, jskip;
  while (i >= 0) {
    fd_lisp root=phrase_root(pc,i);
    if ((heads) || (FD_PAIRP(root))) { /* compounds are always interesting */
      fd_lisp phrase_entry=fd_make_list
	(2,fd_incref(FD_VECTOR_REF(arc_names,pc->phrases[i].tag)),root);
      FD_ADD_TO_CHOICE(tuples,phrase_entry);}
    else fd_decref(root);
    if (rels) {
      int j=0; while (j < pc->phrases[i].n_links) {
	struct PHRASE_LINK *pl=&(pc->phrases[i].links[j]);
	if (inversep(pl->tag)) j++;
	else if (pl->ptr < 0) j++;
	else {
	  fd_lisp link_tuple=
	    fd_make_list(3,fd_incref(pc->phrases[i].links[j].tag),
			 phrase_root(pc,i),
			 phrase_root(pc,pc->phrases[i].links[j].ptr));
	  FD_ADD_TO_CHOICE(tuples,link_tuple); j++;}}}
    j=pc->phrases[i].start;
    jskip=pc->phrases[i].head; jlimit=pc->phrases[i].end;
    if (phrases)
      while (j <= jlimit)
	if (j == jskip) j=next_input(pc,j);
	else if ((pc->input[j].tag == punctuation_tag) ||
		 (pc->input[j].tag == comma_tag) ||
		 (pc->input[j].tag == and_tag) ||
		 (pc->input[j].tag == or_tag) ||
		 (pc->input[j].tag == conjunction_tag) ||
		 (pc->input[j].tag == sentence_end_tag))
	  j=next_input(pc,j);
	else {
	  fd_lisp mod_tuple=FD_EMPTY_CHOICE;
	  if (FD_PAIRP(pc->input[j].lword)) {
	    /* compounds which are heads are interesting, so always
	       note the compound together with part of speech in the
	       keytuples, even if heads=0 */
	    fd_lisp compound_tuple=
	      fd_make_list
	      (2,fd_incref(FD_VECTOR_REF(arc_names,pc->input[j].tag)),
	       root_form(pc->input[j].lword,pc->input[j].tag));
	    FD_ADD_TO_CHOICE(tuples,compound_tuple);}
	  mod_tuple=fd_make_list
	    (2,root_form(pc->input[j].lword,pc->input[j].tag),
	     phrase_root(pc,i));
	  FD_ADD_TO_CHOICE(tuples,mod_tuple); j=next_input(pc,j);}
    if (phrases) {
      /* Now add all the extras as mods */
      int k=0, klimit=pc->phrases[i].n_extras;
      while (k < klimit) {
	fd_lisp mod_tuple=fd_make_list
	  (2,root_form(pc->input[pc->phrases[i].extras[k]].lword,
		       pc->input[pc->phrases[i].extras[k]].tag),
	   phrase_root(pc,i));
	FD_ADD_TO_CHOICE(tuples,mod_tuple); k++;}}
    i--;}
  return tuples;
}

static fd_lisp keytuples_cproc(fd_lisp text)
{
  return fd_get_keytuples(fd_strdata(text));
}


/* Top-level text->fd_lisp functions */

FD_EXPORT
fd_lisp fd_tag_sentence(char *text)
{
  state_ref final; fd_lisp results=FD_EMPTY_LIST;
  struct PARSE_CONTEXT parse_context;
  init_parse_context(&parse_context); 
  breakup_sentence(&parse_context,text); identify_compounds(&parse_context);
  add_state(&parse_context,&(nodes[0]),0,0,-1,0,FD_VOID);
  final=queue_run(&parse_context);
  if (final >= 0) results=gather_tags(&parse_context,final);
  free_parse_context(&parse_context);
  return results;
}

FD_EXPORT
void fd_tag_sentences(char *text,void (*handle)(fd_lisp parse))
{
  char *end; text=skip_whitespace(text);
  while ((text) && (end=find_sentence_end(text))) {
    fd_lisp parse; char sbreak=*end; *end='\0';
    parse=fd_tag_sentence(text); 
    handle(parse); fd_decref(parse); *end=sbreak; 
    if (sbreak) text=skip_whitespace(utf8_next(end));
    else text=end;}
}

FD_EXPORT
fd_lisp fd_phrase_sentence(char *text)
{
  state_ref final; fd_lisp results=FD_EMPTY_LIST;
  struct PARSE_CONTEXT parse_context;
  init_parse_context(&parse_context); 
  breakup_sentence(&parse_context,text); identify_compounds(&parse_context);
  add_state(&parse_context,&(nodes[0]),0,0,-1,0,FD_VOID);
  final=queue_run(&parse_context);
  if (final >= 0) {
    construct_phrases(&parse_context,final);
    hookup_phrases(&parse_context);
    results=gather_phrases(&parse_context);}
  free_parse_context(&parse_context);
  return results;
}

FD_EXPORT
void fd_phrase_sentences(char *text,void (*handle)(fd_lisp parse))
{
  char *end; text=skip_whitespace(text);
  while ((text) && (end=find_sentence_end(text))) {
    char sbreak=*end; *end='\0';
    {fd_lisp parse=fd_phrase_sentence(text); 
    handle(parse); fd_decref(parse); *end=sbreak; 
    if (sbreak) text=skip_whitespace(utf8_next(end));
    else text=end;}}
}

FD_EXPORT
fd_lisp fd_phrase_sentence2(char *text)
{
  state_ref final; fd_lisp results=FD_EMPTY_LIST;
  struct PARSE_CONTEXT parse_context;
  init_parse_context(&parse_context); 
  breakup_sentence(&parse_context,text); identify_compounds(&parse_context);
  add_state(&parse_context,&(nodes[0]),0,0,-1,0,FD_VOID);
  final=queue_run(&parse_context);
  if (final >= 0) {
    construct_phrases(&parse_context,final);
    hookup_phrases(&parse_context);
    results=gather_phrases(&parse_context);
    results=fd_make_pair
      (fd_make_list(2,FD_LISPUSHORT(parse_context.states[final].distance),
		    fd_lisp_string(text)),
       results);}
  free_parse_context(&parse_context);
  return results;
}


/* FDScript NLP primitives */

static fd_lisp accumulating_sentences, sentences_tail;

static void accumulate_sentence(fd_lisp s)
{
  if (FD_EMPTY_LISTP(accumulating_sentences))
    sentences_tail=accumulating_sentences=
      fd_init_pair(NULL,fd_incref(s),FD_EMPTY_LIST);
  else {
    fd_lisp new=fd_init_pair(NULL,fd_incref(s),FD_EMPTY_LIST);
    struct FD_PAIR *p=(fd_pair)sentences_tail;
    p->cdr=new; sentences_tail=new;}
}

static fd_lisp parse_cproc(fd_lisp string)
{
  fd_lisp answer;
  accumulating_sentences=FD_EMPTY_LIST;
  if (!(FD_STRINGP(string))) 
    return fd_type_error(_("string"),"parse_cproc",string);
  fd_tag_sentences(FD_STRDATA(string),accumulate_sentence);
  answer=accumulating_sentences; accumulating_sentences=FD_EMPTY_LIST;
  return answer;
}

static fd_lisp phrase_cproc(fd_lisp string)
{
  fd_lisp answer;
  accumulating_sentences=FD_EMPTY_LIST;
  if (!(FD_STRINGP(string))) 
    return fd_type_error(_("string"),"phrase_cproc",string);
  fd_phrase_sentences(FD_STRDATA(string),accumulate_sentence);
  answer=accumulating_sentences; accumulating_sentences=FD_EMPTY_LIST;
  return answer;
}


/* Initialization procedures */

static void init_parser_symbols(void);
static fd_lisp lisp_report_stats(void);
static fd_lisp lisp_set_word_limit(fd_lisp x);

static fd_lisp lisp_get_total_inputs()
{
  return FD_LISPFIX(total_inputs);
}

static fd_lisp configsrc=FD_EMPTY_CHOICE;

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

static fd_lisp config_get_lexdata(fd_lisp var)
{
  if (lexdata_source) 
    return fd_lisp_string(lexdata_source);
  else return FD_EMPTY_CHOICE;
}
static int config_set_lexdata(fd_lisp var,fd_lisp val)
{
  if (lexicon) {
    fd_seterr(LateConfiguration,"config_set_lexdata",
	      u8_strdup(lexdata_source),FD_VOID);
    return -1;}
  else if (FD_STRINGP(val)) {
    if (lexdata_source) u8_free(lexdata_source);
    lexdata_source=u8_strdup(FD_STRDATA(val));
    return 1;}
  else return fd_reterr(fd_TypeError,"config_set_lexdata",
			u8_strdup(_("string")),fd_incref(val));
}

static void initialize_chopper()
{
  if (chopper_initialized) return;
  else {
    lexicon=openindexsource(lexdata_source,"lexicon");
    verb_roots=openindexsource(lexdata_source,"verb-roots");
    noun_roots=openindexsource(lexdata_source,"noun-roots");
    vlinks=openindexsource(lexdata_source,"verb-links");
    if (strchr(lexdata_source,'@')==NULL) {
      u8_string grammar_file=
	u8_mkstring("%s/%s",lexdata_source,"grammar");
      fd_lisp grammar=read_file_as_vector(grammar_file);
      init_ofsm_data(grammar);
      fd_decref(grammar); u8_free(grammar_file);}
    if (strchr(lexdata_source,'@')==NULL) {
      u8_string hookup_file=
	u8_mkstring("%s/%s",lexdata_source,"hookup");
      fd_lisp hookup=read_file_as_vector(hookup_file);
      init_hookup_data(hookup);
      fd_decref(hookup); u8_free(hookup_file);}
    init_parser_symbols();
    start_time=time(NULL);
    chopper_initialized=1;}
}

static fd_lisp lisp_trace_tagger(fd_lisp flag)
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

static fd_lisp lisp_set_word_limit(fd_lisp x)
{
  int olimit=word_limit;
  if (FD_FIXNUMP(x)) word_limit=FD_FIXLISP(x);
  else word_limit=-1;
  return FD_LISPFIX(olimit);
}

FD_EXPORT
/* fd_strange_capitalization:
      Arguments: none
      Returns: nothing
  Switches off a heuristic in the parser which assumes that
uppercase non-initial words are proper names. */
void fd_strange_capitalization()
{
  normal_capitalization=0;
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

static fd_lisp lisp_strange_capitalization()
{
  fd_strange_capitalization();
  return FD_TRUE;}

static void parser_report_stats(FILE *output)
{
  time_t time_now=time(NULL); 
  double secs=difftime(time_now,start_time);
  if (secs < 120.0)
    u8_message("Processed %d words, %d sentences, %d states in %f seconds\n",
	       total_inputs,total_sentences,total_states,secs);
  else u8_message("Processed %d words, %d sentences, %d states in %f minutes\n",
		  total_inputs,total_sentences,total_states,secs/60.0);
  if (secs == 0.0)
    u8_message("Of course, that's just a problem with the clock\n");
  else u8_message("Averaging %f words per minute\n",
		  (((double) total_inputs)/(secs/60.0)));
  u8_message("The biggest input was %d words; ",max_inputs);
  u8_message("the biggest state space was %d states\n",max_states);
}

FD_EXPORT struct PARSER_STATS fd_parser_stats()
{
  time_t time_now=time(NULL); 
  double secs=difftime(time_now,start_time);
  struct PARSER_STATS answer; 
  answer.total_inputs=total_inputs;
  answer.total_states=total_states;
  answer.total_sentences=total_sentences;
  answer.total_frames=total_frames;
  answer.max_states=max_states;
  answer.max_inputs=max_inputs;
  answer.time=secs;
  if (secs == 0.0) answer.wpm=0;
  else answer.wpm=(((double) total_inputs)/((secs)/60.0));
  return answer;
}

static fd_lisp lisp_report_stats()
{
  parser_report_stats(stdout);
  return FD_TRUE;
}

/* Initialization */

static void init_word_builtins()
{
  char **scan;
  months=(fd_hashset)fd_make_hashset();
  scan=months_init; while (*scan) {
    fd_hashset_add(months,fd_lisp_string(*scan)); scan++;}
  days=(fd_hashset)fd_make_hashset();
  scan=days_init; while (*scan) {
    fd_hashset_add(days,fd_lisp_string(*scan)); scan++;}
  closed=(fd_hashset)fd_make_hashset();
  scan=closed_init; while (*scan) {
    fd_hashset_add(closed,fd_lisp_string(*scan)); scan++;}
  company_cues=(fd_hashset)fd_make_hashset();
  scan=company_cues_init; while (*scan) {
    fd_hashset_add(company_cues,fd_lisp_string(*scan)); scan++;}
}


/* Initializes symbols used in the parser. */
static void init_parser_symbols()
{
  ing_word=fd_intern("%ING-WORD");
  ly_word=fd_intern("%LY-WORD");
  ed_word=fd_intern("%ED-WORD");
  punctuation_symbol=fd_intern("%PUNCTUATION");
  sstrange_word=fd_intern("%STRANGE-WORD");
  sproper_name=fd_intern("%PROPER-NAME");
  sdashed_adjective=fd_intern("%DASHED-ADJECTIVE");
  stime_ref=fd_intern("%TIME-WORD");
  sscore=fd_intern("%SCORE");
  snumber=fd_intern("%NUMBER");
  sdollars=fd_intern("%DOLLARS");
  spossessive=fd_intern("%POSSESSIVE");
  sproper_possessive=fd_intern("%PROPER-POSSESSIVE");
  subject_slotid=fd_intern("SUBJECT");
  subject_of_slotid=inverse_slotid(fd_intern("SUBJECT"));
  close_subject_slotid=fd_intern("CSUBJECT");
  close_subject_of_slotid=inverse_slotid(fd_intern("CSUBJECT"));
  object_slotid=fd_intern("OBJECT");
  close_object_slotid=fd_intern("COBJECT");
  next_act_slotid=fd_intern("NEXT-ACT");
  last_act_slotid=fd_intern("LAST-ACT");
  phrase_slotid=fd_intern("PHRASE");
  pos_slotid=fd_intern("PART-OF-SPEECH");
  pronoun_symbol=fd_intern("PRONOUNS");
  preposition_symbol=fd_intern("PREPOSITION");
  possible_argument_slotid=fd_intern("POSSIBLE-ARGUMENT");
  possible_subject_slotid=fd_intern("POSSIBLE-SUBJECT");
  likely_subject_slotid=fd_intern("LIKELY-SUBJECT");
  possible_object_slotid=fd_intern("POSSIBLE-OBJECT");
  tight_object_slotid=fd_intern("TIGHT-OBJECT");
  verb_arg=fd_intern("VERB-ARGUMENT");
  gfs=fd_intern("GIVE-INF-SUBJECT");
  ggs=fd_intern("GIVE-ING-SUBJECT");
  gfo=fd_intern("GIVE-INF-OBJECT");
  ggo=fd_intern("GIVE-ING-OBJECT");
  tg=fd_intern("TAKE-ING");
  tf=fd_intern("TAKE-INF");

  lexget_symbol=fd_intern("LEXGET");
  verb_root_symbol=fd_intern("VERB-ROOT");
  noun_root_symbol=fd_intern("NOUN-ROOT");
  verb_links_symbol=fd_intern("VERB-LINKS");

  {unsigned int i;
   fd_lisp place_name=fd_intern("PLACE-NAME");
   fd_lisp partial_place_name=fd_intern("PARTIAL-PLACE-NAME");
   fd_lisp number=fd_intern("NUMBER");
   fd_lisp count_adjective=fd_intern("COUNT-ADJECTIVE");
   fd_lisp proper_name=fd_intern("PROPER-NAME");
   fd_lisp proper_adjective=fd_intern("PROPER-NAME-AS-ADJECTIVE");
   i=0; while (i < n_arcs)
     if (FD_EQ((FD_VECTOR_REF(arc_names,i)),place_name)) {
       place_name_tag=i; break;}
     else i++;
   i=0; while (i < n_arcs)
     if (FD_EQ((FD_VECTOR_REF(arc_names,i)),number)) {
       number_tag=i; break;}
     else i++;
   i=0; while (i < n_arcs)
     if (FD_EQ((FD_VECTOR_REF(arc_names,i)),count_adjective)) {
       count_adjective_tag=i; break;}
     else i++;
   i=0; while (i < n_arcs)
     if (FD_EQ((FD_VECTOR_REF(arc_names,i)),partial_place_name)) {
       partial_place_name_tag=i; break;}
     else i++;
   i=0; while (i < n_arcs)
     if (FD_EQ((FD_VECTOR_REF(arc_names,i)),proper_name)) {
       proper_name_tag=i; break;}
     else i++;
   i=0; while (i < n_arcs)
     if (FD_EQ((FD_VECTOR_REF(arc_names,i)),proper_adjective)) {
       proper_adjective_tag=i; break;}
     else i++;}
  next_phrase_slotid=fd_intern("NEXT-PHRASE");
  previous_phrase_slotid=fd_intern("PREVIOUS-PHRASE");
}

/* fd_init_chopper_c: 
      Arguments: none
      Returns: nothing
  Reads grammar and lexicon information from a particular directory.
If the directory is not provided, DEFAULT_LEXDATA_SOURCE (defined in mlparse.h)
is used instead.  It loads the OFSM grammar, opens the index for the lexicon,
reads phrase hookup categories, and opens indices for noun and verb roots.
*/
FD_EXPORT
void fd_initialize_chopper()
{
  fd_lisp menv=fd_new_module("CHOPPER",0);
  keytuple_stops=(fd_hashset)fd_make_hashset();
  init_word_builtins();

#if FD_THREADS_ENABLED
  fd_init_mutex(&initialize_chopper_lock);
  fd_init_mutex(&parser_stats_lock);
  fd_init_mutex(&inverses_table_lock);
#endif

  fd_idefn(menv,fd_make_cprim1("TAGTEXT",parse_cproc,1));
  fd_idefn(menv,fd_make_cprim1("NLPHRASE",phrase_cproc,1));
  fd_idefn(menv,fd_make_cprim1("ADD-KEYTUPLE-STOP!",add_keytuple_stop,1));
  fd_idefn(menv,fd_make_cprim1("KEYTUPLES",keytuples_cproc,1));
  fd_idefn(menv,fd_make_cprim0("TOTAL-INPUTS",lisp_get_total_inputs,0));
  fd_idefn(menv,fd_make_cprim0
	  ("STRANGE-CAPITALIZATION!",lisp_strange_capitalization,0));
  fd_idefn(menv,fd_make_cprim0("NLP-STATS",lisp_report_stats,0));
  fd_idefn(menv,fd_make_cprim1("SET-WORD-LIMIT!",lisp_set_word_limit,1));
  fd_idefn(menv,fd_make_cprim1("TRACE-TAGGER!",lisp_trace_tagger,1));
  
  fd_finish_module(menv);

  fd_register_config("LEXDATA",config_get_lexdata,config_set_lexdata,NULL);

}
