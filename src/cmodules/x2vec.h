#define X2VEC_MAX_STRING 100
#define X2VEC_MAX_BLOCK_LENGTH 1000
#define X2VEC_MAX_CODE_LENGTH 40
#define X2VEC_MAX_OPTS 8

typedef float real; // Precision of float numbers

typedef fd_int x2vec_word_ref;

struct FD_X2VEC_WORD {
  u8_string word;
  fd_long count;
  unsigned char codelen;
  unsigned char *code;
  x2vec_word_ref *x2vec_opts;
  int *nodeid;
};

struct FD_X2VEC_CONTEXT {
  FD_CONS_HEADER; 
  lispval x2vec_opts; u8_string x2vec_label; 
  int x2vec_loglevel, x2vec_logfreq;
  char x2vec_opt_sep;
  struct FD_X2VEC_WORD *x2vec_vocab;
  x2vec_word_ref x2vec_vocab_max_size, x2vec_vocab_alloc_size, x2vec_vocab_size;
  fd_int *x2vec_vocab_hash;
  ssize_t x2vec_vocab_hash_size, x2vec_vocab_hash_max;
  fd_int x2vec_min_count, x2vec_hash_reduce, x2vec_vocab_locked;
  fd_int x2vec_hidden_size;
  int *x2vec_unigrams;
  ssize_t x2vec_unigrams_size;
  real *x2vec_exp_table, x2vec_exp_max;
  fd_int x2vec_exp_slots;
  int x2vec_bag_of_words:1, x2vec_hisoftmax:1;
  fd_int x2vec_window, x2vec_negsamp;
  fd_long x2vec_train_words, x2vec_word_count_actual;
  fd_int x2vec_n_clusters, x2vec_n_cluster_rounds;
  fd_int *x2vec_word2cluster;
  real *x2vec_centers;
  real x2vec_alpha, x2vec_starting_alpha, x2vec_subsample;
  real *x2vec_syn0, *x2vec_syn1, *x2vec_syn1neg;};

struct FD_X2VEC_STATE {
  struct FD_X2VEC_CONTEXT *x2vec_x2vcxt;
  int x2vec_thread_i, x2vec_n_threads; 
  double x2vec_start; int *x2vec_stop;
  lispval x2vec_input, x2vec_opts;};

FD_EXPORT int fd_init_x2vec_c(void) FD_LIBINIT_FN;

FD_EXPORT fd_ptr_type fd_x2vec_type;

FD_EXPORT struct FD_X2VEC_CONTEXT *fd_init_x2vec(struct FD_X2VEC_CONTEXT *x2v,lispval opts);
FD_EXPORT struct FD_X2VEC_CONTEXT *fd_x2vec_start(lispval opts,lispval train,lispval vocab);
FD_EXPORT struct FD_X2VEC_CONTEXT *fd_x2vec_read(u8_string filename,lispval opts);
FD_EXPORT lispval fd_x2vec_init(struct FD_X2VEC_CONTEXT *x2vcxt,lispval init);
FD_EXPORT int fd_x2vec_import_vocab(struct FD_X2VEC_CONTEXT *x2vcxt,lispval data);

FD_EXPORT int *fd_x2vec_classify(struct FD_X2VEC_CONTEXT *x2v,
                                 int n_clusters,int n_rounds,
                                 real **save_centers);

FD_EXPORT float *_fd_x2vec_get(struct FD_X2VEC_CONTEXT *x2vcxt,u8_string word);
FD_EXPORT int _fd_x2vec_hash(struct FD_X2VEC_CONTEXT *x2vcxt,u8_string word);
FD_EXPORT int _fd_x2vec_getword(struct FD_X2VEC_CONTEXT *x2vcxt,u8_string word);

#if FD_INLINE_X2VEC
FD_FASTOP int fd_x2vec_hash(struct FD_X2VEC_CONTEXT *x2vcxt,u8_string word) 
{
  unsigned long long a, hash = 0;
  size_t vocab_hash_size = x2vcxt->x2vec_vocab_hash_size;
  for (a = 0; a < strlen(word); a++) hash = hash * 257 + word[a];
  hash = hash % vocab_hash_size;
  return hash;
}
// Returns position of a word in the vocabulary; if the word is not found, returns -1
FD_FASTOP x2vec_word_ref fd_x2vec_vocabid
    (struct FD_X2VEC_CONTEXT *x2vcxt,u8_string word) 
{
  struct FD_X2VEC_WORD *vocab = x2vcxt->x2vec_vocab;
  int *vocab_hash = x2vcxt->x2vec_vocab_hash;
  size_t vocab_hash_size = x2vcxt->x2vec_vocab_hash_size;
  unsigned int hash = fd_x2vec_hash(x2vcxt,word);
  while (1) {
    if (vocab_hash[hash] == -1) return -1;
    if (!strcmp(word, vocab[vocab_hash[hash]].word))
      return vocab_hash[hash];
    hash = (hash + 1) % vocab_hash_size;}
  return -1;
}
FD_FASTOP float *fd_x2vec_get(struct FD_X2VEC_CONTEXT *x2vcxt,u8_string word)
{
  int wd = fd_x2vec_vocabid(x2vcxt,word);
  float *syn0 = x2vcxt->x2vec_syn0;
  if (wd<0) return NULL;
  else return &(syn0[wd*(x2vcxt->x2vec_hidden_size)]);
}
#else
#define fd_x2vec_hash _fd_x2vec_hash
#define fd_x2vec_vocabid _fd_x2vec_vocabid
#define fd_x2vec_get _fd_x2vec_get
#endif
   

/* Defaults */

#define FD_X2VEC_HIDDEN_SIZE 200
#define FD_X2VEC_WINDOW 5
/* This is the number used to toss out vocabulary entries while the vocabulary is growing */
#define FD_X2VEC_HASH_REDUCE 1
/* This is the number used to toss out vocabulary entries before sorting/finalizing */
#define FD_X2VEC_MIN_COUNT 5
#define FD_X2VEC_N_CLUSTERS 64
#define FD_X2VEC_N_CLUSTER_ROUNDS 10
#define FD_X2VEC_UNIGRAMS_SIZE 1e8
#define FD_X2VEC_VOCAB_SIZE 5000
#define FD_X2VEC_VOCAB_MAX_SIZE 200000
#define FD_X2VEC_INIT_ALPHA 0.025
#define FD_X2VEC_SUBSAMPLE 0.0
#define FD_X2VEC_NEGSAMP 0
#define FD_X2VEC_HISOFT 1
#define FD_X2VEC_CBOW 0
#define FD_X2VEC_HASH_SIZE 30000000
#define FD_X2VEC_HASH_MAX  200000000
#define FD_X2VEC_EXP_SLOTS 1000
#define FD_X2VEC_EXP_MAX 6
#define FD_X2VEC_LOGLEVEL LOG_NOTICE
#define FD_X2VEC_LOGFREQ 0
