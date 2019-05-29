//  Copyright 2013 Google Inc. All Rights Reserved.
//  Copyright 2016 beingmeta, inc
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
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "x2vec.h"

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

static int default_num_threads=2;

fd_ptr_type fd_x2vec_type;

static fdtype layer1_size, window, min_reduce, min_count, n_classes;
static fdtype table_size, vocab_size, vocab_hash_size, alpha, subsample;
static fdtype negative_sampling, hisoftmax, mode, bag_of_words, skipgram;
static fdtype vocab, vecs, training;
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

FD_EXPORT fdtype fd_init_vecs(struct FD_X2VEC_CONTEXT *x2vcxt,fdtype init);

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
    if (!strcmp(word, vocab[vocab_hash[hash]].word))
      return vocab_hash[hash];
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
  if (vocab_hash==NULL) {
    x2vcxt->vocab_hash=vocab_hash=u8_alloc_n(vocab_hash_size,int);
    for (a = 0; a < vocab_hash_size; a++) vocab_hash[a] = -1;}
  if (FD_CHOICEP(data)) {
    FD_DO_CHOICES(entry,data) {
      if (FD_PAIRP(entry)) {
	fdtype word=FD_CAR(entry), num=FD_CDR(entry);
	long a = vocab_add(x2vcxt,FD_STRDATA(word));
	vocab[a].count = FD_INT(num);}
      else {
	u8_log(LOG_WARN,"Bad Vocab import","Couldn't use %q",entry);}}}
  else if (FD_VECTORP(data)) {
    struct FD_VECTOR *vec=(struct FD_VECTOR *)data;
    fdtype *elts=vec->fd_vecelts;
    int i=0, len=vec->fd_veclen; while (i<len) {
      fdtype elt=elts[i];
      if (FD_STRINGP(elt)) {
	u8_string text = FD_STRDATA(elt);
	i = get_vocab_id(x2vcxt,text);
	if (i == -1) {
	  a = vocab_add(x2vcxt,u8_strdup(text));
	  vocab[a].count = 1;}
	else {
	  vocab[i].count++;}}
      else {
	u8_log(LOGWARN,"BadImportItem", "Bad text input item %q",elt);}
      i++;}}
  else if (FD_TABLEP(data)) {
    fdtype keys=fd_getkeys(data);
    FD_DO_CHOICES(key,keys) {
      if (FD_STRINGP(key)) {
	fdtype value=fd_get(data,key,FD_VOID);
	if ((FD_FIXNUMP(value))||(FD_BIGINTP(value))) {
	  u8_string text=FD_STRDATA(key);
	  int a=vocab_add(x2vcxt,u8_strdup(text));
	  if (FD_FIXNUMP(value))
	    vocab[a].count=FD_FIX2INT(value);
	  else {
	    vocab[a].count= fd_bigint_to_long_long((fd_bigint)value);
	    fd_decref(value);}}
	else {
	  u8_log(LOGWARN,"BadWordCount",
		 "Invalid word count value for %q: %q",key,value);
	  fd_decref(value);}}
      else {}}
    fd_decref(keys);}
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
    fdtype *data = vec->fd_vecelts; int lim=vec->fd_veclen;
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
  if (x2vcxt->syn0==NULL) {
    a = posix_memalign((void **)&(x2vcxt->syn0), 128,
		       (long long)vocab_size * layer1_size * sizeof(real));
    if ((x2vcxt->syn0) == NULL) {
      u8_raise(fd_MallocFailed,"x2vec/init_nn",NULL);}
    else syn0=x2vcxt->syn0;}
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
  fdtype opts=x2vs->opts;
  int num_threads=getintopt(opts,num_threads,default_num_threads);
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
  int vec_len = vec->fd_veclen, vec_pos=random_value%vec_len;
  fdtype *vec_data = vec->fd_vecelts;
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
	(word_count > x2vcxt->train_words / num_threads))
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
  fd_decref(opts);
  pthread_exit(NULL);
}

static void compute_classes(struct FD_X2VEC_CONTEXT *x2vcxt);

static void train_model(struct FD_X2VEC_CONTEXT *x2vcxt,
			fdtype training,
			fdtype vocab,
			fdtype opts)
{
  long a, b, c, d;
  pthread_t *pt = (pthread_t *)malloc(num_threads * sizeof(pthread_t));
  fdtype xopts=(FD_VOIDP(opts))?(fd_incref(x2vcxt->opts)):
    (fd_make_pair(opts,x2vcxt->opts));
  int num_threads = getintopt(xopts,num_threads,default_num_threads);
  struct FD_X2VEC_STATE *thread_states =
    safe_calloc(num_threads,sizeof(struct FD_X2VEC_STATE),
		"x2vcxt/TrainModel/threadstates");
  real starting_alpha = x2vcxt->starting_alpha = x2vcxt->alpha;
  int negative = x2vcxt->negative_sampling;
  clock_t start;

  init_expTable(x2vcxt);

  if (FD_VOIDP(vocab)) {
    if (x2vcxt->vocab==NULL)
      init_vocab(x2vcxt,training);}
  else import_vocab(x2vcxt,vocab);

  init_nn(x2vcxt);

  if (negative > 0) init_unigrams(x2vcxt);

  start = clock();

  /* Start all the threads */
  for (a = 0; a < num_threads; a++) {
    struct FD_X2VEC_STATE *tstate=&(thread_states[a]);
    tstate->x2vcxt=x2vcxt; tstate->input=training;
    tstate->opts=xopts; fd_incref(xopts);
    tstate->random_value=a;
    pthread_create(&pt[a], NULL, training_threadproc, (void *) tstate);}
  fd_decref(xopts);
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

/* Set weights */

static struct FD_X2VEC_WORD *init_vocab_size(struct FD_X2VEC_CONTEXT *x2v,size_t vocab_size)
{
  size_t vocab_max_size=1000*((vocab_size/1000)+2);
  struct FD_X2VEC_WORD *vocab=x2v->vocab=u8_alloc_n(vocab_max_size,struct FD_X2VEC_WORD);
  x2v->vocab_max_size=vocab_max_size;
  x2v->vocab_size=vocab_size;
  memset(vocab,0,vocab_max_size*sizeof(struct FD_X2VEC_WORD));
  return vocab;
}

static void init_vocab_word(struct FD_X2VEC_WORD *word,u8_string spelling,int count)
{
  word->word=u8_strdup(spelling);
  word->count=count;
}

FD_EXPORT fdtype fd_init_vecs(struct FD_X2VEC_CONTEXT *x2vcxt,fdtype init)
{
  size_t layer1_size=x2vcxt->layer1_size;
  int init_vocab=x2vcxt->vocab==NULL;
  if (FD_TABLEP(init)) {
    int i=0; fdtype keys=fd_getkeys(init);
    size_t vocab_size=(init_vocab)?(FD_CHOICE_SIZE(keys)):(x2vcxt->vocab_size);
    size_t syn0_size=vocab_size*layer1_size;
    real *syn0=
      (posix_memalign((void **)&(x2vcxt->syn0), 
		      128, syn0_size * sizeof(real)),
       x2vcxt->syn0);
    struct FD_X2VEC_WORD *vocab=(init_vocab)?
      (init_vocab_size(x2vcxt,vocab_size)):
      (x2vcxt->vocab);
    {FD_DO_CHOICES(key,keys) {
	if (FD_STRINGP(key)) {
	  fdtype v=fd_get(init,key,FD_VOID);
	  if (FD_VOIDP(v)) {}
	  else if ((FD_PRIM_TYPEP(v,fd_numeric_vector_type))&&
		   ((FD_NUMVEC_TYPE(v)==fd_float_elt)||
		    (FD_NUMVEC_TYPE(v)==fd_double_elt))) {
	    int n=FD_NUMVEC_LENGTH(v);
	    float *write=&syn0[i*layer1_size];
	    if (FD_NUMVEC_TYPE(v)==fd_float_elt) {
	      fd_float *f=FD_NUMVEC_FLOATS(v);
	      int i=0; while (i<n) {write[i]=f[i]; i++;}}
	    else {
	      fd_double *d=FD_NUMVEC_DOUBLES(v);
	      int i=0; while (i<n) {write[i]=(float)d[i]; i++;}}
	    if (init_vocab) init_vocab_word(&vocab[i],FD_STRDATA(key),1);}
	  else if (FD_PRIM_TYPEP(v,fd_vector_type)) {
	    int n=FD_VECTOR_LENGTH(v);
	    fdtype *elts=FD_VECTOR_ELTS(v);
	    float *f=&syn0[i*layer1_size];
	    int i=0; while (i<n) {
	      fdtype elt=elts[i];
	      if (FD_FLONUMP(elt)) {
		f[i]=FD_FLONUM(elt);}
	      else {
		u8_log(LOGWARN,"BadX2VecValue","%q is not a flonum",elt);}
	      i++;}
	    if (init_vocab)
	      init_vocab_word(&vocab[i],FD_STRDATA(key),1);}
	  else {
	    u8_log(LOGWARN,"BadX2VecValue",
		   "%q is not a vector or float vector",v);}
	  fd_decref(v);
	  i++;}
	else {}}}
    fd_decref(keys);
    return FD_VOID;}
  else return FD_VOID;
}

/* Creating an X2VEC context */

FD_EXPORT struct FD_X2VEC_CONTEXT *fd_start_x2vec(fdtype opts)
{
  struct FD_X2VEC_CONTEXT *x2v = u8_alloc(struct FD_X2VEC_CONTEXT);
  fdtype vocab_init=fd_getopt(opts,vocab,FD_VOID);
  fdtype vecs_init=fd_getopt(opts,vecs,FD_VOID);
  fdtype training_data=fd_getopt(opts,training,FD_VOID);
  FD_INIT_FRESH_CONS(x2v,fd_x2vec_type);
  x2v->opts=opts; fd_incref(opts);
  x2v->layer1_size=getintopt(opts,layer1_size,100);
  x2v->window=getintopt(opts,window,5);
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
  else x2v->bag_of_words=0;

  if (x2v->vocab_max_size) {
    x2v->vocab = u8_alloc_n(x2v->vocab_max_size,struct FD_X2VEC_WORD);}
  if (x2v->vocab_hash_size) {
    long long a;
    size_t vocab_hash_size = x2v->vocab_hash_size;
    int *vocab_hash = x2v->vocab_hash = u8_alloc_n(x2v->vocab_hash_size,int);
    for (a = 0; a < vocab_hash_size; a++) vocab_hash[a] = -1;}

  if (!(FD_VOIDP(vecs_init))) {
    if (!(FD_VOIDP(vocab_init))) import_vocab(x2v,vocab_init);
    fd_init_vecs(x2v,vecs_init);}
  else if (!(FD_VOIDP(vocab_init))) {
    if (!(FD_VOIDP(training))) {
      train_model(x2v,training,vocab_init,opts);}
    else import_vocab(x2v,vocab_init);}
  else if (!(FD_VOIDP(training)))
    train_model(x2v,training,vocab_init,opts);
  else {}

  fd_decref(vecs_init);
  fd_decref(vocab_init);
  fd_decref(training);

  return x2v;
}

static fdtype start_x2vec_prim(fdtype opts)
{
  struct FD_X2VEC_CONTEXT *x2v=fd_start_x2vec(opts);
  return (fdtype) x2v;
}

/* Getting stuff out to Scheme */

static fdtype x2vec_inputs_prim(fdtype arg)
{
  struct FD_X2VEC_CONTEXT *x2v = (struct FD_X2VEC_CONTEXT *) arg;
  int i=0, n=x2v->vocab_size;
  struct FD_X2VEC_WORD *words=x2v->vocab;
  fdtype result=fd_make_vector(n,NULL);
  while (i<n) {
    fdtype s=fd_make_string(NULL,-1,words[i].word);
    FD_VECTOR_SET(result,i,s);}
  return result;
}

static fdtype x2vec_counts_prim(fdtype arg)
{
  struct FD_X2VEC_CONTEXT *x2v = (struct FD_X2VEC_CONTEXT *) arg;
  int i=0, n=x2v->vocab_size;
  struct FD_X2VEC_WORD *words=x2v->vocab;
  fdtype result=fd_make_hashtable(NULL,n);
  fd_hashtable h=(fd_hashtable) result;
  while (i<n) {
    fdtype s=fd_make_string(NULL,-1,words[i].word);
    fdtype v=FD_INT2DTYPE(words[i].count);
    fd_hashtable_op(h,fd_table_store_noref,s,v);
    i++;}
  return result;
}

static fdtype x2vec_get_prim(fdtype arg,fdtype term)
{
  struct FD_X2VEC_CONTEXT *x2v = (struct FD_X2VEC_CONTEXT *) arg;
  size_t vocab_size=x2v->vocab_size;
  size_t layer1_size=x2v->layer1_size;
  struct FD_X2VEC_WORD *vocab=x2v->vocab;
  size_t weights_len=(size_t)(vocab_size * layer1_size);
  real *syn0=x2v->syn0;
  if (syn0==NULL)
    return FD_EMPTY_CHOICE;
  else if (FD_VOIDP(term)) {
    struct FD_HASHTABLE *ht=(fd_hashtable)
      fd_make_hashtable(NULL,vocab_size+vocab_size/2);
    int i=0; while (i<vocab_size) {
      u8_string word=vocab[i].word;
      fdtype key=fd_make_string(NULL,-1,word);
      fdtype vec=fd_make_float_vector
	(layer1_size,&syn0[i*layer1_size]);
      fd_hashtable_store(ht,key,vec);
      fd_decref(key); fd_decref(vec);
      i++;}
    return (fdtype)ht;}
  else if (FD_STRINGP(term)) {
    int i=get_vocab_id(x2v,FD_STRDATA(term));
    if (i<0) return FD_EMPTY_CHOICE;
    else return fd_make_float_vector(layer1_size,&(syn0[i*layer1_size]));}
  else return fd_type_error(_("string"),"x2vec_get_prim",term);
}

/* Handlers */

static void recycle_x2vec(struct FD_CONS *c)
{
  struct FD_X2VEC_CONTEXT *x2v=(struct FD_X2VEC_CONTEXT *)c;
  struct FD_X2VEC_WORD *vocab=x2v->vocab;
  int *vocab_hash=x2v->vocab_hash;
  int *table=x2v->table;
  int *classes=x2v->classes;
  real *syn0=x2v->syn0, *syn1=x2v->syn1;
  real *syn1neg=x2v->syn1neg, *expTable=x2v->expTable;

  if (vocab) {
    int i=0, n=x2v->vocab_size; while (i<n) {
      u8_free(vocab[i].word); i++;}
    u8_free(vocab); x2v->vocab=NULL;}
  if (vocab_hash) {
    u8_free(vocab_hash);
    x2v->vocab_hash=NULL;}
  if (table) {
    u8_free(table);
    x2v->table=NULL;}
  if (classes) {
    u8_free(classes);
    x2v->classes=NULL;}
  if (syn0) {
    u8_free(syn0);
    x2v->syn0=NULL;}
  if (syn1) {
    u8_free(syn1);
    x2v->syn1=NULL;}
  if (syn1neg) {
    u8_free(syn1neg);
    x2v->syn1neg=NULL;}
  if (expTable) {
    u8_free(expTable);
    x2v->expTable=NULL;}

  u8_free(x2v);
}

static int unparse_x2vec(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_X2VEC_CONTEXT *x2v=(struct FD_X2VEC_CONTEXT *)x;
  u8_printf(out,"#<X2VEC #!%llx>",(unsigned long long)x2v);
  return 1;
}

/* Initialization */

FD_EXPORT int fd_init_x2vec(void) FD_LIBINIT_FN;
static long long int x2vec_initialized=0;

#define DEFAULT_FLAGS (FD_SHORT2DTYPE(FD_MONGODB_DEFAULTS))

static void init_x2vec_symbols(void);

FD_EXPORT int fd_init_x2vec()
{
  fdtype module;
  if (x2vec_initialized) return 0;
  x2vec_initialized=u8_millitime();

  fd_x2vec_type=fd_register_cons_type("X2VEC");
  fd_recyclers[fd_x2vec_type]=recycle_x2vec;
  fd_unparsers[fd_x2vec_type]=unparse_x2vec;

  init_x2vec_symbols();

  module=fd_new_module("X2VEC",(FD_MODULE_SAFE));

  fd_idefn(module,fd_make_cprim1("X2VEC/START",start_x2vec_prim,1));
  fd_idefn(module,fd_make_cprim1x("X2VEC/INPUTS",x2vec_inputs_prim,1,
				  fd_x2vec_type,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("X2VEC/COUNTS",x2vec_counts_prim,1,
				  fd_x2vec_type,FD_VOID));
  fd_idefn(module,fd_make_cprim2x("X2VEC/GET",x2vec_get_prim,1,
				  fd_x2vec_type,FD_VOID,
				  fd_string_type,FD_VOID));

  u8_register_source_file(_FILEINFO);

  return 1;
}

static void init_x2vec_symbols()
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
  vocab=fd_intern("VOCAB");
  vecs=fd_intern("VECS");
  training=fd_intern("TRAINGING");
}

