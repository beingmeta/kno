/* C Mode */

/* Declarations for the Media Lab NLP parser */

/* These are wired in */
#define MAX_ARCS 128
#define MAX_NODES 128

#define INITIAL_N_INPUTS 64
#define INITIAL_N_STATES 16384
#define INITIAL_N_LINKS 64
#define INITIAL_N_EXTRAS 32

#ifdef WIN32
#define DEFAULT_LEXDATA "\\MLPARSE\\DICTIONARY"
#define DEFAULT_RHIZOME_SOURCE "\\MLPARSE\\USER\\rhiz"
#define DEFAULT_WORDNET_SOURCE "\\MLPARSE\\DATA\\wn15"
#define DEFAULT_PARSE_SOURCE "\\MLPARSE\\USER\\parse"
#else
#define DEFAULT_LEXDATA "/usr/local/lib/musoft/dictionary"
#define DEFAULT_RHIZOME_SOURCE "/mas/mu/data/rhizome"
#define DEFAULT_WORDNET_SOURCE "/mas/mu/data/wordnet"
#define DEFAULT_PARSE_SOURCE "/mas/mu/data/parse"
#endif
KNO_EXPORT void kno_initialize_nlp(void);
KNO_EXPORT void kno_set_lexicon_source(char *directory);
KNO_EXPORT void kno_set_rhizome_source(char *source);

struct WORD {
  char *spelling; lispval lstr, compounds;
  unsigned char weights[MAX_ARCS];
  lispval lword; int tag, d, w; int previous, next;};

typedef struct ARC {
  unsigned char measure; struct NODE *target;} *arc;
struct ARC_ENTRY {
  unsigned short n_entries; arc entries;};
struct NODE {
  lispval name; struct ARC_ENTRY arcs[MAX_ARCS]; 
  lispval terminal; int index;};

typedef int state_ref;
struct STATE {
  struct NODE *node;
  state_ref self, previous, qnext, qprev;
  unsigned short distance, input;
  unsigned char arc; lispval word;};

typedef int phrase_ref;
struct PHRASE_LINK {
  lispval tag; phrase_ref ptr;};
struct PHRASE {
  int start, end, head, tag; lispval root, prep, frame; 
  int n_links, max_n_links, n_extras, max_n_extras; 
  phrase_ref last_action, subject_of;
  unsigned int *extras;
  struct PHRASE_LINK *links;};

struct PARSER_STATS {
  unsigned int total_inputs, total_states, total_sentences;
  unsigned int max_states, max_inputs;
  unsigned int total_frames;
  double time; double wpm;};

typedef struct PARSE_CONTEXT {
  struct WORD *input;
  struct STATE *states; state_ref queue, last;
  state_ref **cache;
  int n_states, max_n_states;
  int n_inputs, max_n_inputs;
  /* Phrase extraction */
  struct PHRASE *phrases;
  int n_phrases, max_n_phrases;
  /* Frame generation */
  kno_pool parse_pool;} *parse_context;

KNO_EXPORT void kno_initialize_chopper() KNO_LIBINIT_FN;

