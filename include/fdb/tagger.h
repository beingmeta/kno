/* C Mode */

/* Declarations for beingmeta tagger */

/* These are wired in */
#define FD_MAX_ARCS 128
#define FD_MAX_NODES 256

#define FD_INITIAL_N_INPUTS 64
#define FD_INITIAL_N_STATES 16384

/* Parse context bit flags */

#define FD_TAGGER_SKIP_MARKUP 2
#define FD_TAGGER_GLOM_PHRASES 4
#define FD_TAGGER_ALLCAPS 8
#define FD_TAGGER_SPLIT_SENTENCES 16
#define FD_TAGGER_VERBOSE_TIMER 32
#define FD_TAGGER_INCLUDE_SOURCE 64

#define FD_TAGGER_DEFAULT_FLAGS (FD_TAGGER_SKIP_MARKUP|FD_TAGGER_SPLIT_SENTENCES)

struct FD_WORD {
  u8_string spelling; u8_byte *bufptr;
  fdtype lstr, compounds;
  unsigned char weights[FD_MAX_ARCS];
  short cap, tag, d, w; int previous, next;};

typedef struct FD_OFSM_ARC {
  unsigned char measure; struct FD_OFSM_NODE *target;} *fd_arc;
struct FD_OFSM_ENTRY {
  unsigned short n_entries; fd_arc entries;};
struct FD_OFSM_NODE {
  fdtype name; struct FD_OFSM_ENTRY arcs[FD_MAX_ARCS]; 
  fdtype terminal; int index;};

typedef int fd_parse_state;
struct FD_PARSER_STATE {
  struct FD_OFSM_NODE *node;
  fd_parse_state self, previous, qnext, qprev;
  unsigned short distance, input;
  unsigned char arc; fdtype word;};

struct FD_PARSER_STATS {
  unsigned int total_inputs, total_states, total_sentences;
  unsigned int max_states, max_inputs;
  unsigned int total_frames;
  double time; double wpm;};

typedef struct FD_GRAMMAR {
  u8_string id;
  int n_nodes, n_arcs; 
  fdtype grammar, arc_names;
  fd_index lexicon, noun_roots, verb_roots;
  int anything_tag, punctuation_tag, possessive_tag, sentence_end_tag;
  unsigned char *noun_tags, *verb_tags, *name_tags, *head_tags, *mod_tags;
  struct FD_OFSM_NODE nodes[FD_MAX_NODES];
  fdtype *arcs[FD_MAX_ARCS];} *fd_grammar;

typedef struct FD_PARSE_CONTEXT {
  struct FD_GRAMMAR *grammar;
  int flags, cumulative_inputs, cumulative_states, n_calls;
  double runtime, cumulative_runtime;
  U8_OUTPUT text; u8_string start, end;
  struct FD_WORD *input;
  struct FD_PARSER_STATE *states;
  fd_parse_state queue, last;
  fd_parse_state **cache;
  int n_states, max_n_states; 
  int n_inputs, max_n_inputs;} *fd_parse_context;

FD_EXPORT struct FD_GRAMMAR *fd_open_grammar(u8_string spec);
FD_EXPORT struct FD_GRAMMAR *fd_default_grammar(void);

FD_EXPORT struct FD_PARSE_CONTEXT *fd_init_parse_context
  (fd_parse_context,struct FD_GRAMMAR *);
FD_EXPORT void fd_free_parse_context(fd_parse_context pcxt);
FD_EXPORT void fd_reset_parse_context(fd_parse_context pcxt);

FD_EXPORT void fd_parser_set_text
  (struct FD_PARSE_CONTEXT *pcxt,u8_string in);
FD_EXPORT fd_parse_state fd_run_parser(fd_parse_context pc);
FD_EXPORT fdtype fd_get_root(struct FD_GRAMMAR *,fdtype,int);
FD_EXPORT fdtype fd_gather_tags(fd_parse_context,fd_parse_state);
FD_EXPORT fdtype fd_tag_text(struct FD_PARSE_CONTEXT *,u8_string);

FD_EXPORT fdtype fd_analyze_text
 (struct FD_PARSE_CONTEXT *pcxt,u8_string text,
  fdtype (*fn)(fd_parse_context,fd_parse_state,void *),
  void *data);

