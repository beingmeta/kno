//  Copyright 2013 Google Inc. All Rights Reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/x2vec.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <pthread.h>

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

static void *safe_malloc(size_t n_bytes,u8_context context)
{
  void *result = u8_malloc(n_bytes);
  if (result == NULL) {
    u8_byte *buf=u8_malloc(64);
    sprintf(buf,"%llu",(unsigned long long)n_bytes);
    u8_raise(fd_MallocFailed,context,buf);
    return NULL;}
  else return result;
}

static void *safe_calloc(size_t n_elts,size_t elt_size,u8_context context)
{
  void *result = calloc(n_elts,elt_size);
  if (result == NULL) {
    u8_byte *buf=u8_malloc(64); 
    sprintf(buf,"%llux{%llu}",
	    (unsigned long long) n_elts,
	    (unsigned long long) elt_size);
    u8_raise(fd_MallocFailed,context,buf);
    return NULL;}
  else return result;
}

fd_ptr_type fd_x2vec_type;

static fdtype layer1_size, window, min_reduce, min_count, n_classes;
static fdtype table_size, vocab_size, vocab_hash_size, alpha, subsample;
static fdtype negative_sampling, hisoftmax, mode, bag_of_words, skipgram;
static fdtype num_threads;


static int default_vocab_hash_size = 30000000;  // Maximum 30 * 0.7 = 2 1M words in the vocabulary

static void init_expTable(struct FD_X2VEC_CONTEXT *x2vcxt)
{
  if (x2vcxt->expTable) return;
  else {
    real *expTable = x2vcxt->expTable=expTable = (real *)
      safe_malloc((EXP_TABLE_SIZE + 1) * sizeof(real),
		  "init_expTable");
    // Precompute the exp() table
    int i=0; for (i = 0; i < EXP_TABLE_SIZE; i++) {
      expTable[i] = exp((i / (real)EXP_TABLE_SIZE * 2 - 1) * MAX_EXP);
      expTable[i] = expTable[i] / (expTable[i] + 1);}}
}

static void init_unigrams(struct FD_X2VEC_CONTEXT *x2vcxt) 
{
  int a, i;
  long long train_words_pow = 0;
  real d1, power = 0.75;
  struct FD_X2VEC_WORD *vocab=x2vcxt->vocab;
  long long vocab_size=x2vcxt->vocab_size;
  int *table=x2vcxt->table;
  long long table_size=x2vcxt->table_size;

  if (vocab==NULL) {
    vocab = x2vcxt->vocab =
      safe_calloc(x2vcxt->vocab_max_size,sizeof(struct FD_X2VEC_WORD),
		  "x2vec/init_unigrams/vocab");}

  if (table==NULL) {
    table = x2vcxt->table = (int *)malloc(table_size * sizeof(int));
    if (table == NULL) {
      u8_raise(fd_MallocFailed,"initUnigramTable",NULL);}}

  for (a = 0; a < vocab_size; a++) {
    train_words_pow += pow(vocab[a].count, power);}
  i = 0;
  d1 = pow(vocab[i].count, power) / (real)train_words_pow;
  for (a = 0; a < table_size; a++) {
    table[a] = i;
    if (a / (real)table_size > d1) {
      i++;
      d1 += pow(vocab[i].count, power) / (real)train_words_pow;
    }
    if (i >= vocab_size) i = vocab_size - 1;
  }
}

// Returns hash value of a word
static int word_hash(struct FD_X2VEC_CONTEXT *x2vcxt,u8_string word) 
{
  unsigned long long a, hash = 0;
  size_t vocab_hash_size = x2vcxt->vocab_hash_size;
  for (a = 0; a < strlen(word); a++) hash = hash * 257 + word[a];
  hash = hash % vocab_hash_size;
  return hash;
}

// Returns position of a word in the vocabulary; if the word is not found, returns -1
static int get_vocab_id(struct FD_X2VEC_CONTEXT *x2vcxt,u8_string word) 
{
  struct FD_X2VEC_WORD *vocab = x2vcxt->vocab;
  int *vocab_hash = x2vcxt->vocab_hash;
  size_t vocab_hash_size = x2vcxt->vocab_hash_size;
  unsigned int hash = word_hash(x2vcxt,word);
  while (1) {
    if (vocab_hash[hash] == -1) return -1;
    if (!strcmp(word, vocab[vocab_hash[hash]].word)) return vocab_hash[hash];
    hash = (hash + 1) % vocab_hash_size;}
  return -1;
}

// Adds a word to the vocabulary
static int vocab_add(struct FD_X2VEC_CONTEXT *x2vcxt,u8_string word)
{
  unsigned int hash, length = strlen(word) + 1;
  struct FD_X2VEC_WORD *vocab = x2vcxt->vocab;
  size_t vocab_size = x2vcxt->vocab_size, vocab_max_size = x2vcxt->vocab_max_size;
  int *vocab_hash = x2vcxt->vocab_hash;
  size_t vocab_hash_size = x2vcxt->vocab_hash_size;
  // Reallocate memory if needed
  if (vocab_size + 2 >= vocab_max_size) {
    size_t new_size = vocab_max_size+1000;
    struct FD_X2VEC_WORD *new_vocab = (struct FD_X2VEC_WORD *)
      realloc(vocab, new_size * sizeof(struct FD_X2VEC_WORD));
    if (new_vocab==NULL) {
      u8_raise(fd_MallocFailed,"x2vec/add_word_to_vocab",u8_strdup(word));}
    x2vcxt->vocab_max_size = vocab_max_size = new_size;
    x2vcxt->vocab = new_vocab;}
  if (length > MAX_STRING) length = MAX_STRING;
  vocab[vocab_size].word = u8_strdup(word);
  vocab[vocab_size].count = 0;
  vocab_size++;
  hash = word_hash(x2vcxt,word);
  while (vocab_hash[hash] != -1) hash = (hash + 1) % vocab_hash_size;
  vocab_hash[hash] = vocab_size - 1;
  return vocab_size - 1;
}

// Used later for sorting by word counts
static int vocab_compare(const void *a, const void *b)
{
  return ((struct FD_X2VEC_WORD *)b)->count -
    ((struct FD_X2VEC_WORD *)a)->count;
}

static void destroy_vocab(struct FD_X2VEC_CONTEXT *x2vcxt)
{
  struct FD_X2VEC_WORD *vocab = x2vcxt->vocab;
  size_t vocab_size = x2vcxt->vocab_size,
    vocab_max_size = x2vcxt->vocab_max_size;
  int a;

  for (a = 0; a < vocab_size; a++) {
    if (vocab[a].word != NULL) {
      free((char *)vocab[a].word);}
    if (vocab[a].code != NULL) {
      free(vocab[a].code);}
    if (vocab[a].point != NULL) {
      free(vocab[a].point);}}
  free((char *)vocab[vocab_size].word);
  free(vocab);

  x2vcxt->vocab=NULL;
  x2vcxt->vocab_size=0;
  x2vcxt->vocab_max_size=-1;
}

static void sort_vocab(struct FD_X2VEC_CONTEXT *x2vcxt);
static void reduce_vocab(struct FD_X2VEC_CONTEXT *x2vcxt);

static void import_vocab(struct FD_X2VEC_CONTEXT *x2vcxt,fdtype data)
{
  long long a, i = 0;
  char c;
  char word[MAX_STRING];
  struct FD_X2VEC_WORD *vocab = x2vcxt->vocab;
  int *vocab_hash = x2vcxt->vocab_hash;
  size_t vocab_hash_size = x2vcxt->vocab_hash_size;
  for (a = 0; a < vocab_hash_size; a++) vocab_hash[a] = -1;
  if (FD_CHOICEP(data)) {
    FD_DO_CHOICES(entry,data) {
      if (FD_PAIRP(entry)) {
	fdtype word=FD_CAR(entry), num=FD_CDR(entry);
	long a = vocab_add(x2vcxt,FD_STRDATA(word));
	vocab[a].count = FD_INT(num);}
      else {
	u8_log(LOG_WARN,"Bad Vocab import","Couldn't use %q",entry);}}}
  sort_vocab(x2vcxt);
}

static void init_vocab(struct FD_X2VEC_CONTEXT *x2vcxt,fdtype traindata)
{
  char word[MAX_STRING];
  long long a, i;
  struct FD_X2VEC_WORD *vocab = x2vcxt->vocab;
  int *vocab_hash = x2vcxt->vocab_hash;
  size_t vocab_hash_size = x2vcxt->vocab_hash_size, vocab_size=0;
  for (a = 0; a < vocab_hash_size; a++) vocab_hash[a] = -1;
  vocab_add(x2vcxt,(char *)"</s>");
  if (FD_VECTORP(traindata)) {
    struct FD_VECTOR *vec =
      FD_GET_CONS(traindata,fd_vector_type,struct FD_VECTOR *);
    fdtype *data = vec->data; int lim=vec->length;
    int ref=0; while (ref<lim) {
      fdtype word=data[ref];
      if (FD_STRINGP(word)) {
	u8_string text = FD_STRDATA(word);
	i = get_vocab_id(x2vcxt,text);
	if (i == -1) {
	  a = vocab_add(x2vcxt,u8_strdup(text));
	  vocab[a].count = 1;}
	else {
	  vocab[i].count++;}}
      else u8_log(LOG_WARN,"BadTrainingInput",
		  "Bad training data element %q",word);
      ref++;}}
  if (x2vcxt->vocab_size > vocab_hash_size * 0.7) 
    reduce_vocab(x2vcxt);
  sort_vocab(x2vcxt);
}

// Sorts the vocabulary by frequency using word counts
static void sort_vocab(struct FD_X2VEC_CONTEXT *x2vcxt)
{
  struct FD_X2VEC_WORD *vocab = x2vcxt->vocab, *new_vocab=NULL;
  size_t vocab_size = x2vcxt->vocab_size, vocab_max_size = x2vcxt->vocab_max_size;
  int *vocab_hash = x2vcxt->vocab_hash;
  size_t vocab_hash_size = x2vcxt->vocab_hash_size;
  long long train_words = 0;
  int a, size;
  unsigned int hash;
  int min_count= x2vcxt->min_count;
  // Sort the vocabulary and keep </s> at the first position
  qsort(&vocab[1], vocab_size - 1, sizeof(struct FD_X2VEC_WORD), vocab_compare);
  for (a = 0; a < vocab_hash_size; a++) vocab_hash[a] = -1;
  size = vocab_size;
  train_words = 0;
  for (a = 1; a < size; a++) { // Skip </s>
    // Words occuring less than min_count times will be discarded from the vocab
    if (vocab[a].count < min_count) {
      vocab_size--;
      free((char *)vocab[a].word);
      vocab[a].word = NULL;
    } else {
      // Hash will be re-computed, as after the sorting it is not actual
      hash=word_hash(x2vcxt,vocab[a].word);
      while (vocab_hash[hash] != -1) hash = (hash + 1) % vocab_hash_size;
      vocab_hash[hash] = a;
      train_words += vocab[a].count;
    }
  }
  new_vocab = (struct FD_X2VEC_WORD *)
    realloc(vocab, (vocab_size + 1) * sizeof(struct FD_X2VEC_WORD));
  if (new_vocab==NULL) {
    u8_raise(fd_MallocFailed,"x2vec/sort_vocab",NULL);}

  // Allocate memory for the binary tree construction
  for (a = 0; a < vocab_size; a++) {
    vocab[a].code = (char *)calloc(MAX_CODE_LENGTH, sizeof(char));
    vocab[a].point = (int *)calloc(MAX_CODE_LENGTH, sizeof(int));}

  if (new_vocab) {
    x2vcxt->vocab=new_vocab;
    x2vcxt->vocab_size=vocab_size;
    x2vcxt->vocab_max_size=vocab_size+1;}
  x2vcxt->train_words=train_words;
}

// Reduces the vocabulary by removing infrequent tokens
static void reduce_vocab(struct FD_X2VEC_CONTEXT *x2vcxt)
{
  int a, b = 0;
  unsigned int *vocab_hash = x2vcxt->vocab_hash, hash;
  struct FD_X2VEC_WORD *vocab = x2vcxt->vocab;
  size_t vocab_size = x2vcxt->vocab_size;
  size_t vocab_hash_size = x2vcxt->vocab_hash_size;
  int min_reduce = x2vcxt->min_reduce;

  for (a = 0; a < vocab_size; a++) {
    if (vocab[a].count > min_reduce) {
      vocab[b].count = vocab[a].count;
      vocab[b].word = vocab[a].word;
      b++;}
    else {
      free((char *)vocab[a].word);}}
  x2vcxt->vocab_size = vocab_size = b;
  for (a = 0; a < vocab_hash_size; a++) vocab_hash[a] = -1;
  for (a = 0; a < vocab_size; a++) {
    // Hash will be re-computed, as it is not accurate any longer
    hash = word_hash(x2vcxt,vocab[a].word);
    while (vocab_hash[hash] != -1) hash = (hash + 1) % vocab_hash_size;
    vocab_hash[hash] = a;}
  x2vcxt->min_reduce++;
}

// Create binary Huffman tree using the word counts
// Frequent words will have short uniqe binary codes
static void vocab_create_binary_tree(struct FD_X2VEC_CONTEXT *x2vcxt)
{
  long long a, b, i, min1i, min2i, pos1, pos2, point[MAX_CODE_LENGTH];
  size_t vocab_size = x2vcxt->vocab_size;
  char code[MAX_CODE_LENGTH];
  long long *count = (long long *)calloc(vocab_size * 2 + 1, sizeof(long long));
  long long *binary = (long long *)calloc(vocab_size * 2 + 1, sizeof(long long));
  long long *parent_node = (long long *)calloc(vocab_size * 2 + 1, sizeof(long long));
  struct FD_X2VEC_WORD *vocab = x2vcxt->vocab;
  for (a = 0; a < vocab_size; a++) count[a] = vocab[a].count;
  for (a = vocab_size; a < vocab_size * 2; a++) count[a] = 1e15;
  pos1 = vocab_size - 1;
  pos2 = vocab_size;
  // Following algorithm constructs the Huffman tree by adding one node at a time
  for (a = 0; a < vocab_size - 1; a++) {
    // First, find two smallest nodes 'min1, min2'
    if (pos1 >= 0) {
      if (count[pos1] < count[pos2]) {
        min1i = pos1;
        pos1--;} 
      else {
	min1i = pos2;
        pos2++;}}
    else {
      min1i = pos2;
      pos2++;}
    if (pos1 >= 0) {
      if (count[pos1] < count[pos2]) {
        min2i = pos1;
        pos1--;}
      else {
	min2i = pos2;
	pos2++;}}
    else {
      min2i = pos2;
      pos2++;}
    count[vocab_size + a] = count[min1i] + count[min2i];
    parent_node[min1i] = vocab_size + a;
    parent_node[min2i] = vocab_size + a;
    binary[min2i] = 1;}
  // Now assign binary code to each vocabulary word
  for (a = 0; a < vocab_size; a++) {
    b = a;
    i = 0;
    while (1) {
      code[i] = binary[b];
      point[i] = b;
      i++;
      b = parent_node[b];
      if (b == vocab_size * 2 - 2) break;}
    vocab[a].codelen = i;
    vocab[a].point[0] = vocab_size - 2;
    for (b = 0; b < i; b++) {
      vocab[a].code[i - b - 1] = code[b];
      vocab[a].point[i - b] = point[b] - vocab_size;}}
  free(count);
  free(binary);
  free(parent_node);
}

void init_nn(struct FD_X2VEC_CONTEXT *x2vcxt)
{
  long long a, b;
  size_t vocab_size = x2vcxt->vocab_size;
  int layer1_size = x2vcxt->layer1_size;
  real *syn0, *syn1, *syn1neg;
  a = posix_memalign((void **)&(x2vcxt->syn0), 128,
		     (long long)vocab_size * layer1_size * sizeof(real));
  if ((x2vcxt->syn0) == NULL) {
    u8_raise(fd_MallocFailed,"x2vec/init_nn",NULL);}
  else syn0=x2vcxt->syn0;

  if (x2vcxt->hisoftmax) {
    a = posix_memalign((void **)&(x2vcxt->syn1), 128,
		       (long long)vocab_size * layer1_size * sizeof(real));
    if ((x2vcxt->syn1) == NULL) {
      u8_raise(fd_MallocFailed,"x2vec/init_nn",NULL);}
    else syn1=x2vcxt->syn1;

    for (b = 0; b < layer1_size; b++) for (a = 0; a < vocab_size; a++)
     syn1[a * layer1_size + b] = 0;}

  if (x2vcxt->negative_sampling>0) {
    a = posix_memalign((void **)&(x2vcxt->syn1neg), 128,
		       (long long)vocab_size * layer1_size * sizeof(real));
    if ((x2vcxt->syn1neg) == NULL) {
      u8_raise(fd_MallocFailed,"x2vec/init_nn",NULL);}
    else syn1neg=x2vcxt->syn1neg;

    for (b = 0; b < layer1_size; b++) for (a = 0; a < vocab_size; a++)
     syn1neg[a * layer1_size + b] = 0;}

  for (b = 0; b < layer1_size; b++) for (a = 0; a < vocab_size; a++)
   syn0[a * layer1_size + b] = (rand() / (real)RAND_MAX - 0.5) / layer1_size;

  vocab_create_binary_tree(x2vcxt);
}

static void destroy_nn(struct FD_X2VEC_CONTEXT *x2vcxt)
{
  if (x2vcxt->syn0 != NULL) {
    free(x2vcxt->syn0);}
  if (x2vcxt->syn1 != NULL) {
    free(x2vcxt->syn1);}
  if (x2vcxt->syn1neg != NULL) {
    free(x2vcxt->syn1neg);}
}

void *training_threadproc(void *state)
{
  struct FD_X2VEC_STATE *x2vs=(struct FD_X2VEC_STATE *)state;
  struct FD_X2VEC_CONTEXT *x2vcxt=x2vs->x2vcxt;
  struct FD_X2VEC_WORD *vocab = x2vcxt->vocab;
  size_t vocab_size = x2vcxt->vocab_size;
  real *syn0 = x2vcxt->syn0, *syn1 = x2vcxt->syn1, *syn1neg = x2vcxt->syn1neg;
  int *table = x2vcxt->table;
  size_t table_size = x2vcxt->table_size;
  int negative = x2vcxt->negative_sampling;
  fdtype input=x2vs->input;
  unsigned long long random_value=x2vs->random_value;
  long long a, b, d, word, last_word, block_length = 0, block_position = 0;
  long long word_count = 0, last_word_count = 0, sen[MAX_BLOCK_LENGTH + 1];
  long long l1, l2, c, target, label;
  int layer1_size = x2vcxt->layer1_size, window = x2vcxt->window;
  struct FD_VECTOR *vec=FD_GET_CONS(input,fd_vector_type,struct FD_VECTOR *);
  int vec_len = vec->length, vec_pos=random_value%vec_len;
  fdtype *vec_data = vec->data;
  real f, g;
  clock_t now;
  real *neu1 = (real *)safe_calloc(layer1_size, sizeof(real),
				   "x2vcxt/TrainModelThread/neu1");
  real *neu1e = (real *)safe_calloc(layer1_size, sizeof(real),
				    "x2vcxt/TrainModelThread/neu1");
  real *expTable = x2vcxt->expTable;

  clock_t start = clock();
  long long train_words = x2vcxt->train_words,
    word_count_actual = x2vcxt->word_count_actual;
  real starting_alpha = x2vcxt->starting_alpha, alpha = x2vcxt->alpha;
  real subsample_size = x2vcxt->subsample * x2vcxt->train_words;
  while (vec_pos<vec_len) {
    if (word_count - last_word_count > 10000) {
      word_count_actual += word_count - last_word_count;
      last_word_count = word_count;
      now=clock();
      u8_log(LOG_INFO,"X2VecTrainingProgress",
	     /* "%cAlpha: %f  Progress: %.2f%%  Words/thread/sec: %.2fk  ", 13, */
	     "%cAlpha: %f  Progress: %.2f%%  Words/thread/sec: %.2fk  ",
	     alpha, word_count_actual / (real)(train_words + 1) * 100,
	     (word_count_actual /
	      ((real)(now - start + 1) / (real)CLOCKS_PER_SEC * 1000)));
      alpha = starting_alpha * (1 - word_count_actual / (real)(train_words + 1));
      if (alpha < starting_alpha * 0.0001) alpha = starting_alpha * 0.0001;}
    if (block_length == 0) {
      while (vec_pos<vec_len) {
	fdtype input = vec_data[vec_pos++];
	if (FD_STRINGP(input)) 
	  word = get_vocab_id(x2vcxt,FD_STRDATA(input));
	else continue;
	if (word < 0) continue;
        word_count++;
        if (word == 0) break;
        // The subsampling randomly discards frequent words while
        // keeping the frequency ordering the same
	if (subsample_size > 0) {
          real ran = (sqrt(vocab[word].count / subsample_size) + 1) 
	    * subsample_size / vocab[word].count;
          random_value = random_value * (unsigned long long)25214903917 + 11;
          if (ran < (random_value & 0xFFFF) / (real)65536) continue;}
        sen[block_length] = word;
        block_length++;
        if (block_length >= MAX_BLOCK_LENGTH) break;}
      block_position = 0;}
    if ((vec_pos >= vec_len) &&
	(word_count > x2vcxt->train_words / x2vcxt->num_threads))
      break;
    word = sen[block_position];
    if (word == -1) continue;
    for (c = 0; c < layer1_size; c++) { neu1[c] = 0; }
    for (c = 0; c < layer1_size; c++) { neu1e[c] = 0; }
    random_value = random_value * (unsigned long long)25214903917 + 11;
    b = random_value % x2vcxt->window;
    if (x2vcxt->bag_of_words) {  //train the cbow architecture
      // in -> hidden
      for (a = b; a < window * 2 + 1 - b; a++) {
	if (a != window) {
	  c = block_position - window + a;
	  if (c < 0) continue;
	  if (c >= block_length) continue;
	  last_word = sen[c];
	  if (last_word == -1) continue;
	  for (c = 0; c < layer1_size; c++) {
	    neu1[c] += syn0[c + last_word * layer1_size];}}}
      if (x2vcxt->hisoftmax) {
	for (d = 0; d < vocab[word].codelen; d++) {
	  f = 0;
	  l2 = vocab[word].point[d] * layer1_size;
	  // Propagate hidden -> output
	  for (c = 0; c < layer1_size; c++) {
	    f += neu1[c] * syn1[c + l2];}
	  if (f <= -MAX_EXP) continue;
	  else if (f >= MAX_EXP) continue;
	  else {
	    f = expTable[(int)((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2))];}
	  // 'g' is the gradient multiplied by the learning rate
	  g = (1 - vocab[word].code[d] - f) * alpha;
	  // Propagate errors output -> hidden
	  for (c = 0; c < layer1_size; c++) {
	    neu1e[c] += g * syn1[c + l2];}
	  // Learn weights hidden -> output
	  for (c = 0; c < layer1_size; c++) {
	    syn1[c + l2] += g * neu1[c];}}}
      // NEGATIVE SAMPLING
      if (negative > 0) {
	for (d = 0; d < negative + 1; d++) {
	  if (d == 0) {
	    target = word;
	    label = 1;}
	  else {
	    random_value = random_value * (unsigned long long)25214903917 + 11;
	    target = table[(random_value >> 16) % table_size];
	    if (target == 0) target = random_value % (vocab_size - 1) + 1;
	    if (target == word) continue;
	    label = 0;}
	  l2 = target * layer1_size;
	  f = 0;
	  for (c = 0; c < layer1_size; c++) {
	    f += neu1[c] * syn1neg[c + l2];}
	  if (f > MAX_EXP) g = (label - 1) * alpha;
	  else if (f < -MAX_EXP) g = (label - 0) * alpha;
	  else g = (label - expTable[(int)((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2))]) * alpha;
	  for (c = 0; c < layer1_size; c++) {
	    neu1e[c] += g * syn1neg[c + l2];}
	  for (c = 0; c < layer1_size; c++) {
	    syn1neg[c + l2] += g * neu1[c];}}}
      // hidden -> in
      for (a = b; a < window * 2 + 1 - b; a++) {
	if (a != window) {
	  c = block_position - window + a;
	  if (c < 0) continue;
	  if (c >= block_length) continue;
	  last_word = sen[c];
	  if (last_word == -1) continue;
	  for (c = 0; c < layer1_size; c++) {
	    syn0[c + last_word * layer1_size] += neu1e[c];}}}
      /* (cbow) */
    }
    else { //train skip-gram
      for (a = b; a < window * 2 + 1 - b; a++) {
	if (a != window) {
	  c = block_position - window + a;
	  if (c < 0) continue;
	  if (c >= block_length) continue;
	  last_word = sen[c];
	  if (last_word == -1) continue;
	  l1 = last_word * layer1_size;
	  for (c = 0; c < layer1_size; c++) neu1e[c] = 0;
	  // HIERARCHICAL SOFTMAX
	  if (x2vcxt->hisoftmax) {
	    for (d = 0; d < vocab[word].codelen; d++) {
	      f = 0;
	      l2 = vocab[word].point[d] * layer1_size;
	      // Propagate hidden -> output
	      for (c = 0; c < layer1_size; c++) f += syn0[c + l1] * syn1[c + l2];
	      if (f <= -MAX_EXP) continue;
	      else if (f >= MAX_EXP) continue;
	      else f = expTable[(int)((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2))];
	      // 'g' is the gradient multiplied by the learning rate
	      g = (1 - vocab[word].code[d] - f) * alpha;
	      // Propagate errors output -> hidden
	      for (c = 0; c < layer1_size; c++) {
		neu1e[c] += g * syn1[c + l2];}
	      // Learn weights hidden -> output
	      for (c = 0; c < layer1_size; c++) {
		syn1[c + l2] += g * syn0[c + l1];}}}
	  // NEGATIVE SAMPLING
	  if (negative > 0) {
	    for (d = 0; d < negative + 1; d++) {
	      if (d == 0) {
		target = word;
		label = 1;}
	      else {
		random_value = random_value * (unsigned long long)25214903917 + 11;
		target = table[(random_value >> 16) % table_size];
		if (target == 0) target = random_value % (vocab_size - 1) + 1;
		if (target == word) continue;
		label = 0;}
	      l2 = target * layer1_size;
	      f = 0;
	      for (c = 0; c < layer1_size; c++) {
		f += syn0[c + l1] * syn1neg[c + l2];}
	      if (f > MAX_EXP) g = (label - 1) * alpha;
	      else if (f < -MAX_EXP) g = (label - 0) * alpha;
	      else g = (label - expTable[(int)((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2))]) * alpha;
	      for (c = 0; c < layer1_size; c++) {
		neu1e[c] += g * syn1neg[c + l2];}
	      for (c = 0; c < layer1_size; c++) {
		syn1neg[c + l2] += g * syn0[c + l1];}}}
	  // Learn weights input -> hidden
	  for (c = 0; c < layer1_size; c++) {
	    syn0[c + l1] += neu1e[c];}}}
      /* train_skipgram (!(bow)) */
    }
    block_position++;
    if (block_position >= block_length) {
      block_length = 0;
      continue;}
    /* while (vec_pos < vec_len) */}
  free(neu1);
  free(neu1e);
  pthread_exit(NULL);
}

static void compute_classes(struct FD_X2VEC_CONTEXT *x2vcxt);

static void train_model(struct FD_X2VEC_CONTEXT *x2vcxt,
			fdtype training,
			fdtype vocab)
{
  long a, b, c, d;
  int num_threads = x2vcxt->num_threads;
  pthread_t *pt = (pthread_t *)malloc(num_threads * sizeof(pthread_t));
  struct FD_X2VEC_STATE *thread_states =
    safe_calloc(num_threads,sizeof(struct FD_X2VEC_STATE),
		"x2vcxt/TrainModel/threadstates");
  real starting_alpha = x2vcxt->starting_alpha = x2vcxt->alpha;
  int negative = x2vcxt->negative_sampling;
  clock_t start;

  init_expTable(x2vcxt);

  if (FD_VOIDP(vocab)) {
    init_vocab(x2vcxt,training);}
  else import_vocab(x2vcxt,vocab);

  init_nn(x2vcxt);

  if (negative > 0) init_unigrams(x2vcxt);

  start = clock();

  /* Start all the threads */
  for (a = 0; a < num_threads; a++) {
    struct FD_X2VEC_STATE *tstate=&(thread_states[a]);
    tstate->x2vcxt=x2vcxt; tstate->input=training;
    tstate->random_value=a;
    pthread_create(&pt[a], NULL, training_threadproc, (void *) tstate);}
  /* Wait for all the threads to finish */
  for (a = 0; a < num_threads; a++) {
    pthread_join(pt[a], NULL);}

  if (x2vcxt->n_classes) compute_classes(x2vcxt);
}

static void compute_classes(struct FD_X2VEC_CONTEXT *x2vcxt)
{
  // Run K-means on the word vectors
  int n_classes = x2vcxt->n_classes, iter = 10, closeid;
  size_t vocab_size = x2vcxt->vocab_size;
  int layer1_size = x2vcxt->layer1_size;
  real *syn0 = x2vcxt->syn0;
  int *centcn = (int *)
    safe_malloc(n_classes * sizeof(int),
		"x2vec/TrainModel/getClasses/centcn");
  int *classes = (int *)
    safe_calloc(x2vcxt->vocab_size, sizeof(int),
		"x2vec/TrainModel/getClasses/cl");
  real closev, x;
  real *cent = (real *)
    safe_calloc(n_classes * layer1_size, sizeof(real),
		"x2vec/TrainModel/getClasses/cent");
  long a, b, c, d;

  for (a = 0; a < vocab_size; a++) classes[a] = a % n_classes;
  for (a = 0; a < iter; a++) {
    for (b = 0; b < n_classes * layer1_size; b++) cent[b] = 0;
    for (b = 0; b < n_classes; b++) centcn[b] = 1;
    for (c = 0; c < vocab_size; c++) {
      for (d = 0; d < layer1_size; d++) {
	cent[layer1_size * classes[c] + d] += syn0[c * layer1_size + d];
	centcn[classes[c]]++;}}
    for (b = 0; b < n_classes; b++) {
      closev = 0;
      for (c = 0; c < layer1_size; c++) {
	cent[layer1_size * b + c] /= centcn[b];
	closev += cent[layer1_size * b + c] * cent[layer1_size * b + c];}
      closev = sqrt(closev);
      for (c = 0; c < layer1_size; c++) {
	cent[layer1_size * b + c] /= closev;}}
    for (c = 0; c < vocab_size; c++) {
      closev = -10;
      closeid = 0;
      for (d = 0; d < n_classes; d++) {
	x = 0;
	for (b = 0; b < layer1_size; b++) {
	  x += cent[layer1_size * d + b] * syn0[c * layer1_size + b];}
	if (x > closev) {
	  closev = x;
	  closeid = d;}}
      classes[c] = closeid;}}

  x2vcxt->classes = classes;
  free(centcn);
  free(cent);
}

/* Creating an X2VEC context */

static long long getintopt(fdtype opts,fdtype sym,
			   long long dflt)
{
  fdtype v=fd_getopt(opts,sym,FD_VOID);
  if (FD_FIXNUMP(v))
    return FD_FIX2INT(v);
  else if ((FD_VOIDP(v))||(FD_FALSEP(v)))
    return dflt;
  else if (FD_BIGINTP(v)) {
    long long intv=fd_bigint_to_long_long((struct FD_BIGINT *)v);
    fd_decref(v);
    return intv;}
  else {
    u8_log(LOG_WARN,"Bad integer option value",
	   "The option %q was %q, which isn't an integer");
    return dflt;}
}

static real getrealopt(fdtype opts,fdtype sym,real dflt)
{
  fdtype v = fd_getopt(opts,sym,FD_VOID);
  if ((FD_VOIDP(v))||(FD_FALSEP(v)))
    return dflt;
  else if (FD_FLONUMP(v)) {
    double realval=FD_FLONUM(v);
    fd_decref(v);
    return (real) realval;}
  else return dflt;
}


FD_EXPORT struct FD_X2VEC_CONTEXT *fd_start_index2vec(fdtype opts)
{
  struct FD_X2VEC_CONTEXT *x2v = u8_alloc(struct FD_X2VEC_CONTEXT);
  FD_INIT_FRESH_CONS(x2v,fd_x2vec_type);
  x2v->layer1_size=getintopt(opts,layer1_size,100);
  x2v->window=getintopt(opts,window,5);
  x2v->num_threads=getintopt(opts,num_threads,2);
  x2v->min_reduce=getintopt(opts,min_reduce,1);
  x2v->min_count=getintopt(opts,min_count,5);
  x2v->n_classes=getintopt(opts,n_classes,5);
  x2v->table_size=getintopt(opts,table_size,1e8);
  x2v->vocab_max_size=getintopt(opts,vocab_size,5000);
  x2v->vocab_hash_size=getintopt(opts,vocab_hash_size,20000);
  x2v->alpha=getrealopt(opts,alpha,0.025);
  x2v->subsample=getrealopt(opts,subsample,0.0);
  x2v->negative_sampling=getrealopt(opts,negative_sampling,0.0);
  x2v->hisoftmax=getrealopt(opts,hisoftmax,0.0);
  if (fd_testopt(opts,mode,bag_of_words))
    x2v->bag_of_words=1;
  else if (fd_testopt(opts,mode,skipgram))
    x2v->bag_of_words=0;
  else x2v->bag_of_words=1;

  if (x2v->vocab_max_size) {
    x2v->vocab = u8_alloc_n(x2v->vocab_max_size,struct FD_X2VEC_WORD);}
  if (x2v->vocab_hash_size) {
    x2v->vocab_hash = u8_alloc_n(x2v->vocab_hash_size,int);}

  return x2v;
}

/* Initialization */

FD_EXPORT int fd_init_x2vec(void) FD_LIBINIT_FN;
static long long int x2vec_initialized=0;

#define DEFAULT_FLAGS (FD_SHORT2DTYPE(FD_MONGODB_DEFAULTS))

static int init_x2vec_symbols(void);

FD_EXPORT int fd_init_x2vec()
{
  fdtype module;
  if (x2vec_initialized) return 0;
  x2vec_initialized=u8_millitime();

  fd_x2vec_type=fd_register_cons_type("X2VEC");

  init_x2vec_symbols();

  module=fd_new_module("X2VEC",(FD_MODULE_SAFE));



  u8_register_source_file(_FILEINFO);

  return 1;
}

static int init_x2vec_symbols()
{
  layer1_size=fd_intern("LAYER1_SIZE");
  window=fd_intern("WINDOW");
  num_threads=fd_intern("NUM_THREADS");
  min_reduce=fd_intern("MIN_REDUCE");
  min_count=fd_intern("MIN_COUNT");
  n_classes=fd_intern("N_CLASSES");
  table_size=fd_intern("TABLE_SIZE");
  vocab_size=fd_intern("VOCAB_SIZE");
  vocab_hash_size=fd_intern("VOCAB_HASH_SIZE");
  alpha=fd_intern("ALPHA");
  subsample=fd_intern("SUBSAMPLE");
  negative_sampling=fd_intern("NEGATIVE_SAMPLING");
  hisoftmax=fd_intern("HISOFTMAX");
  mode=fd_intern("MODE");
  bag_of_words=FD_EMPTY_CHOICE;
  FD_ADD_TO_CHOICE(bag_of_words,fd_intern("BAG_OF_WORDS"));
  FD_ADD_TO_CHOICE(bag_of_words,fd_intern("BAGOFWORDS"));
  FD_ADD_TO_CHOICE(bag_of_words,fd_intern("BOW"));
  skipgram=FD_EMPTY_CHOICE;
  FD_ADD_TO_CHOICE(skipgram,fd_intern("SKIPGRAM"));
}
