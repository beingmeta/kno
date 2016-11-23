#define MAX_STRING 100
#define EXP_TABLE_SIZE 1000
#define MAX_EXP 6
#define MAX_BLOCK_LENGTH 1000
#define MAX_CODE_LENGTH 40

typedef float real; // Precision of float numbers

struct FD_X2VEC_WORD {
  long long count;
  int *point;
  u8_string word;
  char *code, codelen;
};

struct FD_X2VEC_CONTEXT {
  FD_CONS_HEADER; fdtype opts;
  struct FD_X2VEC_WORD *vocab;
  ssize_t vocab_max_size, vocab_size;
  int layer1_size;
  int *vocab_hash;
  ssize_t vocab_hash_size;
  int *table;
  ssize_t table_size;
  int bag_of_words, window, min_count, min_reduce;
  int hisoftmax, negative_sampling;
  long long train_words, word_count_actual, n_classes;
  int *classes;
  real alpha, starting_alpha, subsample;
  real *syn0, *syn1, *syn1neg, *expTable;
  clock_t start;};

struct FD_X2VEC_STATE {
  struct FD_X2VEC_CONTEXT *x2vcxt;
  unsigned long long random_value;
  fdtype input, opts;};

FD_EXPORT int fd_init_x2vec(void) FD_LIBINIT_FN;

FD_EXPORT fd_ptr_type fd_x2vec_type;
