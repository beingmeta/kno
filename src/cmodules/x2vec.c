//  Copyright 2013 Google Inc. All Rights Reserved.
//  Copyright 2016-2019 beingmeta, inc
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

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/eval.h"
#include "kno/numbers.h"
#include "kno/sequences.h"
#include "kno/texttools.h"

#define KNO_INLINE_X2VEC 1

#include "x2vec.h"

typedef struct KNO_X2VEC_CONTEXT *x2vec_context;
typedef struct KNO_X2VEC_WORD *x2vec_word;

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <limits.h>
#include <pthread.h>

#if HAVE_MALLOC_H
#include <malloc.h>
#endif

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#ifndef U8STR
#define U8STR(x) ((u8_string)x)
#endif
#ifndef U8S0
#define U8S0() ((u8_string)"")
#endif
#ifndef U8ALT
#define U8ALT(s,d) ((s)?((u8_string)(s)):((u8_string)(d)))
#endif
#ifndef U8IF
#define U8IF(s,d) ((s)?((u8_string)(d)):(U8_SO()))
#endif

#define QUOTE_LABEL(label)                      \
  ((label)?(U8STR("'")):(U8S0())),              \
    ((label)?(label):(U8S0())),                 \
    ((label)?(U8STR("' ")):(U8S0()))

#define FREE_LOC(loc)                           \
  if (loc!=NULL) { u8_free(loc); loc=NULL;}


static u8_condition InvalidOpSeparator = _("Invalid opt separator");
static int default_num_threads=2;

kno_ptr_type kno_x2vec_type;

static lispval hidden_size, window_symbol, hash_reduce_symbol, min_count_symbol;
static lispval n_clusters_symbol, n_cluster_rounds_symbol;
static lispval unigrams_size_symbol, vocab_size_symbol, alpha_symbol;
static lispval exp_max_symbol, exp_slots_symbol;
static lispval hash_size_symbol,  subsample_symbol, optsep_symbol;
static lispval label_symbol, loglevel_symbol, logfreq_symbol;
static lispval negsamp_symbol, hisoftmax_symbol, hash_max_symbol;
static lispval mode_symbol, bag_of_words_symbol, skipgram_symbol;
static lispval num_threads_symbol;

static int default_hidden_size=KNO_X2VEC_HIDDEN_SIZE;
static int default_window=KNO_X2VEC_WINDOW;
/* This is the number used to toss out vocabulary entries while the vocabulary is growing */
static int default_hash_reduce=KNO_X2VEC_HASH_REDUCE;
/* This is the number used to toss out vocabulary entries before sorting/finalizing */
static int default_min_count=KNO_X2VEC_MIN_COUNT;
static int default_n_clusters=KNO_X2VEC_N_CLUSTERS;
static int default_n_cluster_rounds=KNO_X2VEC_N_CLUSTER_ROUNDS;
static int default_unigrams_size=KNO_X2VEC_UNIGRAMS_SIZE;
static int default_vocab_size=KNO_X2VEC_VOCAB_SIZE;
static float default_init_alpha=KNO_X2VEC_INIT_ALPHA;
static float default_subsample=KNO_X2VEC_SUBSAMPLE;
static int default_negsamp=KNO_X2VEC_NEGSAMP;
static int default_hisoft=KNO_X2VEC_HISOFT;
static int default_cbow=KNO_X2VEC_CBOW;
static int default_loglevel=KNO_X2VEC_LOGLEVEL;
static int default_logfreq=KNO_X2VEC_LOGFREQ;
static size_t default_hash_size=KNO_X2VEC_HASH_SIZE;
static size_t default_hash_max=KNO_X2VEC_HASH_MAX;
static size_t default_exp_slots=KNO_X2VEC_EXP_SLOTS;
static size_t default_exp_max=KNO_X2VEC_EXP_MAX;

static int show_progress=0;

#define X2V_DEFAULT_VOCAB_SIZE 32767

/* Prototypes */

static x2vec_word init_vocab_tables(x2vec_context x2v,ssize_t);
static void init_exp_table(x2vec_context x2vcxt);
static void init_unigrams(x2vec_context x2vcxt);
static void grow_vocab_hash(x2vec_context x2v);
static int *init_vocab_hash(int n);

static void sort_vocab(x2vec_context x2vcxt);
static void reduce_vocab(x2vec_context x2vcxt);
static int vocab_compare(const void *a, const void *b);

static long long read_vectors(x2vec_context x2v,FILE *f);
static float *get_float_vec(x2vec_context ,lispval,int *,int *);

static void hash_overflow(x2vec_context x2v);

static real block_train(x2vec_context x2v,
                        long long *block,
                        long long block_length,
                        real alpha,int *stop,
                        real *hidden, real *errv);

/* Export static defs */

KNO_EXPORT int _kno_x2vec_vocabid(x2vec_context x2vcxt,u8_string word)
{
  return kno_x2vec_vocabid(x2vcxt,word);
}

KNO_EXPORT int _kno_x2vec_hash(x2vec_context x2vcxt,u8_string word)
{
  return kno_x2vec_hash(x2vcxt,word);
}

KNO_EXPORT float *_kno_x2vec_get(x2vec_context x2vcxt,u8_string word)
{
  return kno_x2vec_get(x2vcxt,word);
}

/* Utility macros */

#define dbl(x) ((double)x)

#define israndom(n) ((u8_random(n))==0)
#define next_random(loc) loc=                                           \
    ((unsigned long long) loc) * (unsigned long long)25214903917 + 11

/* Utility functions */

static void *safe_malloc(size_t n_bytes,u8_context context)
{
  void *result = u8_malloc(n_bytes);
  if (result == NULL) {
    u8_byte *buf=u8_malloc(64);
    sprintf(buf,"%llu",KNO_LONGVAL(n_bytes));
    u8_raise(kno_MallocFailed,context,buf);
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
    u8_raise(kno_MallocFailed,context,buf);
    return NULL;}
  else return result;
}

static long long getintopt(lispval opts,lispval sym,
			   long long dflt)
{
  lispval v=kno_getopt(opts,sym,KNO_VOID);
  if (KNO_FIXNUMP(v))
    return KNO_FIX2INT(v);
  else if ((KNO_VOIDP(v))||(KNO_FALSEP(v)))
    return dflt;
  else if (KNO_BIGINTP(v)) {
    long long intv=kno_bigint_to_long_long((struct KNO_BIGINT *)v);
    kno_decref(v);
    return intv;}
  else {
    u8_log(LOG_WARN,"Bad integer option value",
	   "The option %q was %q, which isn't an integer");
    return dflt;}
}

static real getrealopt(lispval opts,lispval sym,real dflt)
{
  lispval v = kno_getopt(opts,sym,KNO_VOID);
  if ((KNO_VOIDP(v))||(KNO_FALSEP(v)))
    return dflt;
  else if (KNO_FLONUMP(v)) {
    double realval=KNO_FLONUM(v);
    kno_decref(v);
    return (real) realval;}
  else return dflt;
}

static long long getboolopt(lispval opts,lispval sym,int dflt)
{
  lispval v=kno_getopt(opts,sym,KNO_VOID);
  if (KNO_VOIDP(v)) 
    return dflt;
  else if (KNO_FIXNUMP(v)) {
    int fv=KNO_FIX2INT(v);
    if (fv>0) return 1; else return 0;}
  else if (KNO_FALSEP(v))
    return 0;
  else if (KNO_TRUEP(v))
    return 1;
  else {
    int bv=-1;
    if (KNO_STRINGP(v))
      bv=kno_boolstring(KNO_CSTRING(v),dflt);
    else if (KNO_SYMBOLP(v))
      bv=kno_boolstring(KNO_SYMBOL_NAME(v),dflt);
    else {}
    if (bv<0) {
      u8_log(LOG_WARN,"Bad boolean option value",
             "The option %q was %q, which isn't an integer");
      return dflt;}
    else return bv;}
}

/* Helper functions */

static float getexp(real f,const real *exp_table,real exp_max,int exp_slots)
{
  if (f <= -exp_max) return 0;
  else if (f >= exp_max) return 1;
  else {
    int exp_i=(int)((f+ exp_max) * (exp_slots / exp_max / 2) );
    return exp_table[exp_i];}
}

/* Vector functions */

static float cosim_float(int n,float *x,float *y,int normalized)
{
  int i=0; float x_sum=0, y_sum=0, xy_sum=0;
  i=0; while (i<n) {
    x_sum=x_sum+(x[i]*x[i]); 
    y_sum=y_sum+(y[i]*y[i]); 
    if (normalized) xy_sum=xy_sum+(x[i]*y[i]); 
    i++;}
  if (normalized)
    return xy_sum;
  else {
    double x_len=sqrt(x_sum), y_len=sqrt(y_sum);
    i=0; while (i<n) {
      float x_norm=x[i]/x_len, y_norm=y[i]/y_len;
      xy_sum=xy_sum+x_norm*y_norm;
      i++;}
    return xy_sum;}
}

static lispval normalized_floatvec(int n,float *vec)
{
  lispval result=kno_make_float_vector(n,vec);
  float *elts=KNO_NUMVEC_FLOATS(result);
  float len=0; int i=0; while (i<n) {
    len=len+elts[i]*elts[i]; i++;}
  if (len==0) {
    kno_decref(result);
    return KNO_EMPTY_CHOICE;}
  else len=sqrt(len);
  i=0; while (i<n) {elts[i]=elts[i]/len; i++;}
  return result;
}


/* Vocabulary functions */

// Adds a word to the vocabulary

static u8_string next_opt(u8_string word,char sep);

static int vocab_add(x2vec_context x2v,
                     u8_string word,
                     char isopt,
                     x2vec_word *vocabp)
{
  unsigned int hash = strlen(word) + 1;
  x2vec_word vocab = x2v->x2vec_vocab, vocab_entry;
  x2vec_word_ref word_ref=-1;
  size_t vocab_size = x2v->x2vec_vocab_size;
  size_t vocab_alloc_size = x2v->x2vec_vocab_alloc_size;
  int *vocab_hash = x2v->x2vec_vocab_hash;
  size_t vocab_hash_size = x2v->x2vec_vocab_hash_size;
  char sep=(isopt)?(0):(x2v->x2vec_opt_sep);
  int n_opts=0; 
  if (sep) {
    u8_string scan_opts=next_opt(word,sep);
    while (scan_opts) { 
      n_opts++; scan_opts=next_opt(scan_opts,sep);}}
  // Reallocate memory if needed
  if (vocab_size + n_opts + 2 >= vocab_alloc_size) {
    size_t new_size = vocab_alloc_size+16384;
    x2vec_word new_vocab = (x2vec_word)
      realloc(vocab, new_size * sizeof(struct KNO_X2VEC_WORD));
    u8_log(x2v->x2vec_loglevel+1,"X2Vec/vocab_add",
           "Growing vocab from %lld to %lld entries",
           vocab_alloc_size,new_size);
    if (new_vocab==NULL) {
      u8_raise(kno_MallocFailed,"x2vec/add_word_to_vocab",u8_strdup(word));}
    x2v->x2vec_vocab_alloc_size = vocab_alloc_size = new_size;
    x2v->x2vec_vocab = vocab = new_vocab;
    if (vocabp) *vocabp=new_vocab;}
  vocab[vocab_size].word = u8_strdup(word);
  vocab[vocab_size].count = 0;
  vocab_size++; x2v->x2vec_vocab_size=vocab_size;
  if (x2v->x2vec_vocab_size > (x2v->x2vec_vocab_hash_size * 0.7))
    hash_overflow(x2v);
  hash = kno_x2vec_hash(x2v,word);
  while (vocab_hash[hash] != -1) hash = (hash + 1) % vocab_hash_size;
  word_ref = vocab_size - 1;
  vocab_hash[hash] = word_ref;
  vocab_entry=&(vocab[word_ref]);
  if (n_opts) {
    u8_byte base[X2VEC_MAX_STRING];
    u8_string scanner=next_opt(word,sep), last=word;
    size_t base_len=scanner-word;
    u8_byte *base_root=base+base_len;
    x2vec_word_ref *opts=vocab_entry->x2vec_opts=
      u8_alloc_n(n_opts+2,x2vec_word_ref);
    x2vec_word_ref opt_ref;
    strncpy(base,word,base_len); base[scanner-word]='\0';
    opt_ref=vocab_add(x2v,base,1,vocabp);
    *opts++=opt_ref;
    last=scanner; scanner=next_opt(scanner,sep);
    while (1) {
      int len=(scanner==NULL)?(strlen(last)):(scanner-last);
      strncpy(base_root,last,len); base[base_len+len+1]='\0';
      opt_ref=vocab_add(x2v,base,1,vocabp);
      *opts++=opt_ref;
      if (scanner==NULL) break;
      else {
        last=scanner; 
        scanner=next_opt(scanner+1,sep);}}}
  return word_ref;
}

/* Reference (and add if needed) a vocab entry */
static int vocab_ref(x2vec_context x2vcxt,
                     u8_string word,
                     x2vec_word *vocabp)
{
  x2vec_word vocab = x2vcxt->x2vec_vocab;
  int *vocab_hash = x2vcxt->x2vec_vocab_hash;
  if (vocab_hash) {
    size_t vocab_hash_size = x2vcxt->x2vec_vocab_hash_size;
    unsigned int hash=kno_x2vec_hash(x2vcxt,word);
    while (1) {
      if (vocab_hash[hash] == -1) break;
      if (!strcmp(word, vocab[vocab_hash[hash]].word)) {
        int vocab_ref=vocab_hash[hash];
        return vocab_ref;}
      hash = (hash + 1) % vocab_hash_size;}}
  return vocab_add(x2vcxt,word,0,vocabp);
}

static u8_string next_opt(u8_string word,char sep)
{
  if (sep) {
    if (word[0]==sep)
      return strchr(word+1,sep);
    else return strchr(word,sep);}
  else return NULL;
}

/* Initializing vocabularies */

// Adds a word to the vocabulary at a location
static int vocab_init(x2vec_context x2vcxt,u8_string word,int i)
{
  unsigned int hash;
  x2vec_word vocab = x2vcxt->x2vec_vocab;
  int *vocab_hash = x2vcxt->x2vec_vocab_hash;
  size_t vocab_hash_size = x2vcxt->x2vec_vocab_hash_size;
  vocab[i].word = u8_strdup(word);
  vocab[i].count = 0;
  hash = kno_x2vec_hash(x2vcxt,word);
  while (vocab_hash[hash] != -1) hash = (hash + 1) % vocab_hash_size;
  vocab_hash[hash] = i;
  return i;
}

static long long import_vocab(x2vec_context x2vcxt,lispval data)
{
  if (x2vcxt->x2vec_vocab_locked) {
    u8_seterr(_("X2Vec/Vocab/Locked"),"import_vocab",NULL);
    return -1;}
  else {
    int fresh=(x2vcxt->x2vec_vocab_size==0);
    x2vec_word vocab = init_vocab_tables(x2vcxt,-1);
    if (KNO_CHOICEP(data)) {
      KNO_DO_CHOICES(entry,data) {
        if (KNO_PAIRP(entry)) {
          lispval word=KNO_CAR(entry), num=KNO_CDR(entry);
          long wd = ((fresh)?(vocab_add(x2vcxt,KNO_CSTRING(word),0,&vocab)):
                     (vocab_ref(x2vcxt,KNO_CSTRING(word),&vocab)));
          if (fresh)
            vocab[wd].count = KNO_INT(num);
          else vocab[wd].count += KNO_INT(num);}
        else if (KNO_STRINGP(entry)) {
          long wd = ((fresh)?(vocab_add(x2vcxt,KNO_CSTRING(entry),0,&vocab)):
                     (vocab_ref(x2vcxt,KNO_CSTRING(entry),&vocab)));
          if (fresh) vocab[wd].count = 1;
          else vocab[wd].count++;}
        else {
          u8_log(LOG_WARN,"Bad Vocab import","Couldn't use %q",entry);}}}
    else if (KNO_VECTORP(data)) {
      struct KNO_VECTOR *vec=(struct KNO_VECTOR *)data;
      lispval *elts=vec->vec_elts;
      int i=0, len=vec->vec_length; while (i<len) {
        lispval elt=elts[i];
        if (KNO_STRINGP(elt)) {
          u8_string text = KNO_CSTRING(elt);
          int wd = vocab_ref(x2vcxt,text,NULL);
          vocab[wd].count++;}
        else {
          u8_log(LOG_WARN,"BadImportItem", "Bad text input item %q",elt);}
        i++;}}
    else if (KNO_TABLEP(data)) {
      lispval keys=kno_getkeys(data);
      KNO_DO_CHOICES(key,keys) {
        if (KNO_STRINGP(key)) {
          lispval value=kno_get(data,key,KNO_VOID);
          if ((KNO_FIXNUMP(value))||(KNO_BIGINTP(value))) {
            u8_string text=KNO_CSTRING(key);
            int wd=vocab_ref(x2vcxt,u8_strdup(text),&vocab);
            long long count=KNO_FIXNUMP(value)? (KNO_FIX2INT(value)) :
              (kno_bigint_to_long_long((kno_bigint)value));
            if (fresh) vocab[wd].count = count;
            else vocab[wd].count += count;
            kno_decref(value);}
          else {
            u8_log(LOG_WARN,"BadWordCount",
                   "Invalid word count value for %q: %q",key,value);
            kno_decref(value);}}
        else {}}
      kno_decref(keys);}
    return x2vcxt->x2vec_vocab_size;}
}

static long long init_vocab(x2vec_context x2v,lispval traindata)
{
  if (x2v->x2vec_vocab_locked) {
    u8_seterr(_("X2Vec/Vocab/Locked"),"init_vocab",NULL);
    return -1;}
  else {
    long long hash_i; int fresh = (x2v->x2vec_vocab_size==0);
    x2vec_word vocab = init_vocab_tables(x2v,-1);
    int *vocab_hash = x2v->x2vec_vocab_hash;
    size_t vocab_hash_size = x2v->x2vec_vocab_hash_size;
    u8_log(x2v->x2vec_loglevel,"X2Vec/Vocab/Init",
           "Initializing vocabulary from %d words of training data\n\t%q",
           KNO_VECTOR_LENGTH(traindata),
           (lispval)x2v);
    for (hash_i = 0; hash_i < vocab_hash_size; hash_i++) 
      vocab_hash[hash_i] = -1;
    if (fresh)
      vocab_add(x2v,(char *)"</s>",0,&vocab);
    else vocab_ref(x2v,(char *)"</s>",&vocab);
    if (KNO_VECTORP(traindata)) {
      struct KNO_VECTOR *vec =
        KNO_GET_CONS(traindata,kno_vector_type,struct KNO_VECTOR *);
      lispval *data = vec->vec_elts; int lim=vec->vec_length;
      int ref=0; while (ref<lim) {
        lispval word=data[ref];
        if (KNO_STRINGP(word)) {
          u8_string text = KNO_CSTRING(word);
          long wd = vocab_ref(x2v,text,&vocab);
          vocab[wd].count++;}
        else u8_log(LOG_WARN,"BadTrainingInput",
                    "Bad training data element %q @%d",word,ref);
        if (x2v->x2vec_vocab_size > (x2v->x2vec_vocab_hash_size * 0.7)) 
          hash_overflow(x2v);
        ref++;}}
    u8_log(x2v->x2vec_loglevel,"X2Vec/Vocab/Init",
           "Selected %lld words from %d words of training data\n\t%q",
           x2v->x2vec_vocab_size,KNO_VECTOR_LENGTH(traindata),
           (lispval)x2v);
    return x2v->x2vec_vocab_size;}
}

// Sorts the vocabulary by frequency using word counts
static void sort_vocab(x2vec_context x2v)
{
  x2vec_word vocab = x2v->x2vec_vocab, new_vocab=NULL;
  size_t vocab_size = x2v->x2vec_vocab_size, size=vocab_size;
  int *vocab_hash = x2v->x2vec_vocab_hash;
  size_t vocab_hash_size = x2v->x2vec_vocab_hash_size;
  long long train_words = 0;
  int a;
  unsigned int hash;
  int min_count= x2v->x2vec_min_count;
  double start = u8_elapsed_time();
  // Sort the vocabulary and keep </s> at the first position
  qsort(&vocab[1], vocab_size - 1, sizeof(struct KNO_X2VEC_WORD), vocab_compare);
  /* Reset the hash table */
  for (a = 0; a < vocab_hash_size; a++) vocab_hash[a] = -1;
  train_words = 0;
  for (a = 1; a < size; a++) { // Skip </s>
    // Words occuring less than min_count times will be discarded from the vocab
    if (vocab[a].count < min_count) break;
    else {
      // Hash will be re-computed, as after the sorting it is not actual
      hash=kno_x2vec_hash(x2v,vocab[a].word);
      while (vocab_hash[hash] != -1) hash = (hash + 1) % vocab_hash_size;
      vocab_hash[hash] = a;
      train_words += vocab[a].count;}}
  vocab_size=a;
  /* Everything else is below min_count */
  while (a<size) {  
    free((char *)vocab[a].word);
    vocab[a].word = NULL;
    a++;}
  u8_log(x2v->x2vec_loglevel,"X2Vec/Vocab/Sort",
         "Removing %lld terms with less than %d occurrences from\n\t%q",
         size-vocab_size,min_count,(lispval)x2v);
  
  /* We realloc to just keep the entries we are using */
  new_vocab = (x2vec_word)
    realloc(vocab, (vocab_size + 1) * sizeof(struct KNO_X2VEC_WORD));
  if (new_vocab==NULL) {
    u8_raise(kno_MallocFailed,"x2vec/sort_vocab",NULL);}

  // Allocate memory for the binary tree construction
  for (a = 0; a < vocab_size; a++) {
    new_vocab[a].nodeid = (int *)
      calloc(X2VEC_MAX_CODE_LENGTH, sizeof(int));
    new_vocab[a].code = (unsigned char *)
      calloc(X2VEC_MAX_CODE_LENGTH, sizeof(unsigned char));}

  if (new_vocab) {
    x2v->x2vec_vocab=new_vocab;
    x2v->x2vec_vocab_size=vocab_size;
    x2v->x2vec_vocab_alloc_size=vocab_size+1;}
  x2v->x2vec_train_words=train_words;

  u8_log(x2v->x2vec_loglevel+1,"X2Vec/Vocab/Sort/Done",
         "Finished sorting %d vocab entries in %02fs",
         vocab_size,u8_elapsed_time()-start);
}

// Reduces the vocabulary by removing infrequent tokens
static void reduce_vocab(x2vec_context x2v)
{
  int a, b = 0;
  unsigned int *vocab_hash = x2v->x2vec_vocab_hash, hash;
  x2vec_word vocab = x2v->x2vec_vocab;
  size_t vocab_size = x2v->x2vec_vocab_size;
  size_t original_vocab_size = vocab_size;
  size_t vocab_hash_size = x2v->x2vec_vocab_hash_size;
  int hash_reduce = x2v->x2vec_hash_reduce;

  u8_log(x2v->x2vec_loglevel,"X2Vec/Vocab/Reduce",
         "Reducing %s%s%svocabulary from %lld terms\n\t%q",
         QUOTE_LABEL(x2v->x2vec_label),vocab_size,(lispval)x2v);

  for (a = 0; a < vocab_size; a++) {
    if (vocab[a].count > hash_reduce) {
      vocab[b].count = vocab[a].count;
      vocab[b].word = vocab[a].word;
      b++;}
    else {
      if (vocab[a].word)
        free((char *)vocab[a].word);}}
  x2v->x2vec_vocab_size = vocab_size = b;
  for (a = 0; a < vocab_hash_size; a++) vocab_hash[a] = -1;
  for (a = 0; a < vocab_size; a++) {
    // Hash will be re-computed, as it is not accurate any longer
    hash = kno_x2vec_hash(x2v,vocab[a].word);
    while (vocab_hash[hash] != -1) hash = (hash + 1) % vocab_hash_size;
    vocab_hash[hash] = a;}

  u8_log(x2v->x2vec_loglevel,"X2Vec/Vocab/Reduced",
         "Reduced %s%s%svocabulary to %lld terms from %lld terms\n\t%q",
         QUOTE_LABEL(x2v->x2vec_label),vocab_size,original_vocab_size,
         (lispval)x2v);

  /* Increment the threshold in case we're called again */
  x2v->x2vec_hash_reduce++;
}

// Used later for sorting by word counts
static int vocab_compare(const void *a, const void *b)
{
  return ((x2vec_word)b)->count -
    ((x2vec_word)a)->count;
}

// Create binary Huffman tree using the word counts
// Frequent words will have short uniqe binary codes
static void vocab_create_binary_tree(x2vec_context x2vcxt)
{
  double start = u8_elapsed_time();
  size_t vocab_size = x2vcxt->x2vec_vocab_size;
  long long min1i, min2i, pos1, pos2;
  long long vocab_i, node_i, node_max=(vocab_size*2);
  long long point[X2VEC_MAX_CODE_LENGTH];
  unsigned char code[X2VEC_MAX_CODE_LENGTH];
  long long *count = (long long *)
    calloc(vocab_size * 2 + 1, sizeof(long long));
  long long *binary = (long long *)
    calloc(vocab_size * 2 + 1, sizeof(long long));
  long long *parent_node = (long long *)
    calloc(vocab_size * 2 + 1, sizeof(long long));
  x2vec_word vocab = x2vcxt->x2vec_vocab;
  for (node_i = 0; node_i < vocab_size; node_i++)
    count[node_i] = vocab[node_i].count;
  // This ensures that all uninitialized nodes will be larger than
  // initialized nodes
  for (node_i = vocab_size; node_i < node_max; node_i++)
    count[node_i] = 1e15;
  pos1 = vocab_size - 1;
  pos2 = vocab_size;
  // Following algorithm constructs the Huffman tree by adding one
  // node at a time.  A single array is used to store both the vocab
  // nodes and the tree nodes. The vocab nodes are sorted high to low
  // and the tree nodes are sorted low to high. pos1 scans the vocab
  // nodes (it never increases) and pos2 scans the tree nodes (it
  // never decreases).
  // 
  // Both pos1 and pos2 always point to the lowest count nodes in
  // their range. They are moved whenever a node becomes the child of
  // a new node in the tree.
  //
  // The algorithm finds the two lowest nodes from both ranges.
  for (node_i = 0; node_i < vocab_size - 1; node_i++) {
    // Figure out the two smallest nodes to combine
    if (pos1 >= 0) {
      if (count[pos1] < count[pos2]) 
        min1i = pos1--;
      else min1i = pos2++;}
    else min1i = pos2++;
    // We've got the smaller (or equal), now we get the next one
    if (pos1 >= 0) {
      if (count[pos1] < count[pos2]) 
        min2i = pos1--;
      else min2i = pos2++;}
    else min2i = pos2++;

    // Now we make a new node (at vocab_size + a)
    count[vocab_size + node_i] = count[min1i] + count[min2i];
    parent_node[min1i] = vocab_size + node_i;
    parent_node[min2i] = vocab_size + node_i;
    // The larger node is marked with binary 1
    binary[min2i] = 1;}
  // Now assign a binary code to each vocabulary word based on the
  // tree
  for (vocab_i = 0; vocab_i < vocab_size; vocab_i++) {
    long long node_scan=vocab_i; int code_pos=0, code_len=0;
    while (1) {
      code[code_pos] = binary[node_scan];
      point[code_pos] = node_scan;
      node_scan = parent_node[node_scan];
      code_pos++;
      // If we've reached the end of the node space, break
      if (node_scan == node_max - 2) break;}
    code_len = code_pos;
    // Assign the code and point information to the word, in opposite
    // order to how they're saved to the code[] and point[] arrays
    vocab[vocab_i].codelen = code_pos;
    // Assign the first nodeid to be larger than all the others
    vocab[vocab_i].nodeid[0] = vocab_size - 2;
    for (code_pos = 0; code_pos < code_len; code_pos++) {
      vocab[vocab_i].code[code_len - code_pos - 1] = 
        code[code_pos];
      vocab[vocab_i].nodeid[code_len - code_pos] = 
        point[code_pos] - vocab_size;}}
  free(count);
  free(binary);
  free(parent_node);
  u8_log(x2vcxt->x2vec_loglevel+1,"X2Vec/Vocab/Huffman/Done",
         "Finished creating a Huffman tree for %d vocab entries in %02fs",
         vocab_size,u8_elapsed_time()-start);
}

static long long finish_vocab(x2vec_context x2v)
{
  if (x2v->x2vec_vocab_locked) 
    return x2v->x2vec_vocab_size;
  u8_log(x2v->x2vec_loglevel,"X2Vec/Vocab/Lock",
         "Finishing/Locking vocabulary for %q",(lispval)x2v);
  if (x2v->x2vec_vocab_size > x2v->x2vec_vocab_hash_size * 0.7) {
    u8_log(x2v->x2vec_loglevel,"X2Vec/Vocab/Lock",
           "Reducing vocabulary for hash size: %q",(lispval)x2v);
    while (x2v->x2vec_vocab_size > x2v->x2vec_vocab_hash_size * 0.7)
      reduce_vocab(x2v);}
  sort_vocab(x2v);
  if (x2v->x2vec_negsamp>0) init_unigrams(x2v);
  vocab_create_binary_tree(x2v);
  x2v->x2vec_vocab_locked=1;
  return x2v->x2vec_vocab_size;
}


/* Initializing the "neural" network */

static void init_nn(x2vec_context x2v)
{
  long long a, b; int memalign_rv=0;
  size_t vocab_size = x2v->x2vec_vocab_size;
  int hidden_size = x2v->x2vec_hidden_size;
  real *syn0=NULL, *syn1=NULL, *syn1neg=NULL;
  if (x2v->x2vec_syn0==NULL) {
    memalign_rv =
      posix_memalign((void **)&(x2v->x2vec_syn0), 128,
                     (long long)vocab_size * hidden_size * sizeof(real));
    if (memalign_rv==EINVAL) {
      u8_raise(_("Bad posix_memalign argument"),"x2vec/init_nn/syn0",NULL);}
    else if (memalign_rv==EINVAL) {
      u8_raise(_("Not enough memory"),"x2vec/init_nn/syn0",NULL);}
    else if ((x2v->x2vec_syn0) == NULL) {
      u8_raise(kno_MallocFailed,"x2vec/init_nn/syn0",NULL);}
    else syn0=x2v->x2vec_syn0;}
  else syn0=x2v->x2vec_syn0;

  if (x2v->x2vec_hisoftmax) {
    memalign_rv = posix_memalign((void **)&(x2v->x2vec_syn1), 128,
                                 (long long)vocab_size * hidden_size * sizeof(real));
    if (memalign_rv==EINVAL) {
      u8_raise(_("Bad posix_memalign argument"),"x2vec/init_nn/syn1",NULL);}
    else if (memalign_rv==EINVAL) {
      u8_raise(_("Not enough memory"),"x2vec/init_nn/syn1",NULL);}
    else if ((x2v->x2vec_syn1) == NULL) {
      u8_raise(kno_MallocFailed,"x2vec/init_nn/syn1",NULL);}
    else syn1=x2v->x2vec_syn1;

    for (b = 0; b < hidden_size; b++) {
      for (a = 0; a < vocab_size; a++) {
        syn1[a * hidden_size + b] = 0;}}}

  if (x2v->x2vec_negsamp>0) {
    a = posix_memalign((void **)&(x2v->x2vec_syn1neg), 128,
		       (long long)vocab_size * hidden_size * sizeof(real));
    if (memalign_rv==EINVAL) {
      u8_raise(_("Bad posix_memalign argument"),"x2vec/init_nn/syn1neg",NULL);}
    else if (memalign_rv==EINVAL) {
      u8_raise(_("Not enough memory"),"x2vec/init_nn/syn1neg",NULL);}
    else if ((x2v->x2vec_syn1neg) == NULL) {
      u8_raise(kno_MallocFailed,"x2vec/init_nn/syn1neg",NULL);}
    else syn1neg=x2v->x2vec_syn1neg;

    for (b = 0; b < hidden_size; b++) {
      for (a = 0; a < vocab_size; a++) {
        syn1neg[a * hidden_size + b] = 0;}}}
  
  for (b = 0; b < hidden_size; b++) {
    for (a = 0; a < vocab_size; a++) {
      syn0[a * hidden_size + b] = (rand() / (real)RAND_MAX - 0.5) / hidden_size;}}
  
  finish_vocab(x2v);
}

/* Training the network */

void *training_threadproc(void *state)
{
  struct KNO_X2VEC_STATE *x2vs=(struct KNO_X2VEC_STATE *)state;
  x2vec_context x2v=x2vs->x2vec_x2vcxt;
  x2vec_word vocab = x2v->x2vec_vocab;
  double start=x2vs->x2vec_start; 
  lispval opts=x2vs->x2vec_opts;
  int thread_i=x2vs->x2vec_thread_i, n_threads=x2vs->x2vec_n_threads;
  size_t vocab_size = x2v->x2vec_vocab_size;
  real *syn0 = x2v->x2vec_syn0, *syn1 = x2v->x2vec_syn1;
  real *syn1neg = x2v->x2vec_syn1neg;
  int *unigrams = x2v->x2vec_unigrams;
  size_t unigrams_size = x2v->x2vec_unigrams_size;
  int negsamp = x2v->x2vec_negsamp;
  lispval input=x2vs->x2vec_input;
  unsigned long long random_value=thread_i;
  long long a, b, d, word, last_word, block_length = 0, block_position = 0;
  long long word_count = 0, last_word_count = 0, block[X2VEC_MAX_BLOCK_LENGTH + 1];
  long long l1, l2, c, target, label;
  int hidden_size = x2v->x2vec_hidden_size, window = x2v->x2vec_window;
  struct KNO_VECTOR *vec=KNO_GET_CONS(input,kno_vector_type,struct KNO_VECTOR *);
  int vec_len = vec->vec_length, vec_pos=(vec_len/n_threads)*thread_i;
  lispval *vec_data = vec->vec_elts;
  real *hidden = (real *)
    safe_calloc(hidden_size, sizeof(real),"x2v/TrainModelThread/hidden");
  real *errv = (real *)
    safe_calloc(hidden_size, sizeof(real),"x2v/TrainModelThread/errv");
  const real *exp_table = x2v->x2vec_exp_table, exp_max=x2v->x2vec_exp_max;
  const int exp_slots = x2v->x2vec_exp_slots;
  int loglevel = x2v->x2vec_loglevel, logfreq = x2v->x2vec_logfreq;
  int *stopval = x2vs->x2vec_stop;

  long long train_words = x2v->x2vec_train_words;
  real starting_alpha = x2v->x2vec_starting_alpha, alpha = x2v->x2vec_alpha;
  real subsample_size = x2v->x2vec_subsample * x2v->x2vec_train_words;
  while (vec_pos<vec_len) {
    if ((word_count - last_word_count) > 10000) {
      int word_count_actual=x2v->x2vec_word_count_actual+
        (word_count - last_word_count);
      x2v->x2vec_word_count_actual += (word_count - last_word_count);
      last_word_count = word_count;
      if ((show_progress>0)&&(u8_random(show_progress))) {
        fprintf(stderr,
                "%cAlpha: %f  Progress: %.2f%%  Words/sec: %.2fk  Words/thread/sec: %.2fk  ",
                13,alpha,word_count_actual / (real)(train_words + 1) * 100,
                (word_count_actual / (u8_elapsed_time()-(start))),
                (word_count / (u8_elapsed_time()-(start))));}
      if ((logfreq>0)&&(u8_random(logfreq)==0)) {
        u8_log(loglevel+1,"X2Vec/Training/progress",
               /* "%cAlpha: %f  Progress: %.2f%%  Words/thread/sec: %.2fk  ", 13, */
               "Alpha: %f  Progress: %.2f%%  Words/sec: %.2fk  Words/thread/sec: %.2fk  ",
               alpha, word_count_actual / (real)(train_words + 1) * 100,
               (word_count_actual / (u8_elapsed_time()-(start))),
               (word_count / (u8_elapsed_time()-(start))));}
      alpha = starting_alpha * 
        (1 - word_count_actual / (real)(train_words + 1));
      if (alpha < starting_alpha * 0.0001)
        alpha = starting_alpha * 0.0001;}
    if (block_length == 0) {
      while (vec_pos<vec_len) {
	lispval input = vec_data[vec_pos++];
	if (KNO_STRINGP(input)) 
	  word = kno_x2vec_vocabid(x2v,KNO_CSTRING(input));
	else continue;
	if (word < 0) continue;
        word_count++;
        if (word == 0) break;
        // The subsampling randomly discards frequent words while
        // keeping the frequency ordering the same
	if (subsample_size > 0) {
          real ran = (sqrt(vocab[word].count / subsample_size) + 1) 
	    * subsample_size / vocab[word].count;
          next_random(random_value);
          if (ran < (random_value & 0xFFFF) / (real)65536) 
            continue;} /* if (subsample_size > 0) */
        block[block_length] = word;
        block_length++;
        if (block_length >= X2VEC_MAX_BLOCK_LENGTH) 
          break;} /* (vec_pos<vec_len) */
      block_position = 0;}
    if (((stopval)&&(*stopval))||
        (word_count > (x2v->x2vec_train_words / n_threads))) {
      if (stopval) *stopval=1;
      break;}
    word = block[block_position];
    if (word == -1) continue;
    for (c = 0; c < hidden_size; c++) { 
      hidden[c] = 0; }
    for (c = 0; c < hidden_size; c++) { 
      errv[c] = 0; }

    next_random(random_value);
    b = random_value % x2v->x2vec_window;
    if (x2v->x2vec_bag_of_words) {  //train the cbow architecture
      // in -> hidden
      for (a = b; a < window * 2 + 1 - b; a++) {
        /* When a==window, we're looking at the word above */
	if (a != window) {
	  c = block_position - window + a;
	  if (c < 0) continue;
	  if (c >= block_length) continue;
	  last_word = block[c];
	  if (last_word == -1) continue;
	  for (c = 0; c < hidden_size; c++) {
            /* The hidden values accumulate weights from every word
               in the context */
	    hidden[c] += syn0[c + last_word * hidden_size];}}}
      if (x2v->x2vec_hisoftmax) {
	for (d = 0; d < vocab[word].codelen; d++) {
          double sum=0, output=0, delta=0;
	  l2 = vocab[word].nodeid[d] * hidden_size;
	  // Propagate hidden -> output
	  for (c = 0; c < hidden_size; c++) {
	    sum += hidden[c] * syn1[c + l2];}
	  if ((sum <= -exp_max)||(sum >= exp_max)) continue;
	  else output=getexp(sum,exp_table,exp_max,exp_slots);
	  // 'delta' is the gradient multiplied by the learning rate
	  delta = ((1 - vocab[word].code[d]) - output) * alpha;
	  // Propagate errors output -> hidden
	  for (c = 0; c < hidden_size; c++) {
	    errv[c] += delta * syn1[c + l2];}
	  // Learn weights hidden -> output
	  for (c = 0; c < hidden_size; c++) {
	    syn1[c + l2] += delta * hidden[c];}}}
      // NEGATIVE SAMPLING
      if (negsamp > 0) {
        real sum=0, output=0, delta=0;
	for (d = 0; d < negsamp + 1; d++) {
	  if (d == 0) {
	    target = word;
	    label = 1;}
	  else {
	    next_random(random_value);
	    target = unigrams[(random_value >> 16) % unigrams_size];
	    if (target == 0) target = random_value % (vocab_size - 1) + 1;
	    if (target == word) continue;
	    label = 0;}
	  l2 = target * hidden_size;
	  for (c = 0; c < hidden_size; c++) {
	    sum += hidden[c] * syn1neg[c + l2];}
          output = getexp(sum,exp_table,exp_max,exp_slots);
          delta = (label - output) * alpha;
	  for (c = 0; c < hidden_size; c++) {
	    errv[c] += delta * syn1neg[c + l2];}
	  for (c = 0; c < hidden_size; c++) {
	    syn1neg[c + l2] += delta * hidden[c];}}}
      // hidden -> in
      for (a = b; a < window * 2 + 1 - b; a++) {
	if (a != window) {
	  c = block_position - window + a;
	  if (c < 0) continue;
	  if (c >= block_length) continue;
	  last_word = block[c];
	  if (last_word == -1) continue;
	  for (c = 0; c < hidden_size; c++) {
	    syn0[c + last_word * hidden_size] += errv[c];}}}
      /* (cbow) */
    }
    else { //train skip-gram
      for (a = b; a < window * 2 + 1 - b; a++) {
        long long hidden_i=0;
	if (a != window) {
	  c = block_position - window + a;
	  if (c < 0) continue;
	  if (c >= block_length) continue;
	  last_word = block[c];
	  if (last_word == -1) continue;
	  l1 = last_word * hidden_size;
	  for (c = 0; c < hidden_size; c++) errv[c] = 0;
	  // HIERARCHICAL SOFTMAX
	  if (x2v->x2vec_hisoftmax) {
	    for (d = 0; d < vocab[word].codelen; d++) {
              real sum=0, output=0, delta=0;
	      l2 = vocab[word].nodeid[d] * hidden_size;
	      // Propagate hidden -> output
	      for (c = 0; c < hidden_size; c++) {
                sum += syn0[c + l1] * syn1[c + l2];}
	      if ((sum <= -exp_max)||(sum >= exp_max)) continue;
	      else output=getexp(sum,exp_table,exp_max,exp_slots);
              // 'delta' is the gradient multiplied by the learning rate
              delta = ((1 - vocab[word].code[d]) - output) * alpha;
	      // Propagate errors output -> hidden
	      for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
		errv[hidden_i] += delta * syn1[l2 + hidden_i];}
	      // Learn weights hidden -> output
	      for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
		syn1[l2 + hidden_i] += delta * syn0[l1 + hidden_i];}}}
          
	  // NEGATIVE SAMPLING
	  if (negsamp > 0) {
	    for (d = 0; d < negsamp + 1; d++) {
              real sum=0, output=0, delta=0;
	      if (d == 0) {
		target = word;
		label = 1;}
	      else {
		next_random(random_value);
		target = unigrams[(random_value >> 16) % unigrams_size];
		if (target == 0) target = random_value % (vocab_size - 1) + 1;
		if (target == word) continue;
		label = 0;}
	      l2 = target * hidden_size;
	      for (c = 0; c < hidden_size; c++) {
		sum += syn0[c + l1] * syn1neg[c + l2];}
              output = getexp(sum,exp_table,exp_max,exp_slots);
              delta = alpha * (label - output);
	      for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
		errv[hidden_i] += delta * syn1neg[l2 + hidden_i];}
	      for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
		syn1neg[l2 + hidden_i] += delta * syn0[l1 + hidden_i];}}}
	  // Learn weights input -> hidden
	  for (c = 0; c < hidden_size; c++) {
	    syn0[c + l1] += errv[c];}}}
      /* train_skipgram (!(bow)) */}
    block_position++;
    if (block_position >= block_length) {
      block_length = 0;
      continue;}
    /* while (vec_pos < vec_len) */}
  free(hidden);
  free(errv);
  kno_decref(opts);
  pthread_exit(NULL);
}

static real block_train(x2vec_context x2v,
                        long long *block,long long block_length,
                        real alpha,int *stop,
                        real *use_hidden, real *use_errv)
{
  unsigned long long random_value = u8_random(UINT_MAX) * u8_random(UINT_MAX);
  long long block_position = 0, word; /* last_block_position=-1, */
  const x2vec_word vocab = x2v->x2vec_vocab;
  int vocab_size = x2v->x2vec_vocab_size;
  int *unigrams = x2v->x2vec_unigrams;
  int unigrams_size = x2v->x2vec_unigrams_size;
  long long window = x2v->x2vec_window;
  int hidden_size = x2v->x2vec_hidden_size;
  real *syn0 = x2v->x2vec_syn0, *syn1 = x2v->x2vec_syn1;
  real *syn1neg = x2v->x2vec_syn1neg;
  const real *exp_table = x2v->x2vec_exp_table, exp_max=x2v->x2vec_exp_max;
  const int exp_slots = x2v->x2vec_exp_slots;
  real *hidden=(use_hidden != NULL) ? (use_hidden) :
    (real *)safe_calloc(hidden_size, sizeof(real),"x2v/block_train/hidden");
  real *errv=(use_errv != NULL) ? (use_errv) :
    (real *)safe_calloc(hidden_size, sizeof(real),"x2v/block_train/errv");

  while (1) {
    if ((stop)&&(*stop)) break;
    /* last_block_position= */

    word = block[block_position];
    if (word == -1) continue;

    memset(hidden,0,sizeof(real)*hidden_size);
    memset(errv,0,sizeof(real)*hidden_size);
    next_random(random_value);

    if (x2v->x2vec_bag_of_words) {  //train the cbow architecture
      // in -> hidden
      long long window_margin=random_value % x2v->x2vec_window;
      long long window_off=window_margin;
      long long window_lim= ( window * 2 ) + 1 - window_margin;
      long long block_off=0, hidden_i=0, last_word=-1;
      for (window_off = window_margin;
           window_off < window_lim;
           window_off++) {
        /* When window_off==window, we're looking at the word above itself */
	if (window_off != window) {
	  block_off = block_position - window + window_off;
	  if ((block_off < 0)||(block_off >= block_length)) continue;
	  last_word = block[block_off];
	  if (last_word == -1) continue;
	  for ( hidden_i = 0; hidden_i < hidden_size; hidden_i++ ) {
            /* The hidden values accumulate weights from every word in
               the context, in this case the word at block_off */
	    hidden[hidden_i] += syn0[hidden_i + ( last_word * hidden_size )];}}}
      
      if (x2v->x2vec_hisoftmax) {
        int bit;
        /* In this scheme, every word is represented by a set of
           inputs corresonding to it's path through the binary tree.
           Because of the nature of the tree, the number of
           nodes/inputs (and the weight ranges to be adjusted) is
           smaller for more common words, leading to a lot less
           adjusting to do.
        */
	for (bit = 0; bit < vocab[word].codelen; bit++) {
          long long output_weight_block =
            vocab[word].nodeid[bit] * hidden_size;
	  real sum = 0, output = 0, delta = 0;
	  // Propagate hidden -> output
	  for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
	    sum += hidden[hidden_i] * syn1[output_weight_block + hidden_i];}
          output=getexp(sum,exp_table,exp_max,exp_slots);
	  // 'delta' is the gradient multiplied by the learning rate
	  delta = ((1 - vocab[word].code[bit]) - output) * alpha;
	  // Propagate errors output -> hidden
	  for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
	    errv[hidden_i] += delta * syn1[output_weight_block + hidden_i];}
	  // Learn weights hidden -> output
	  for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
	    syn1[output_weight_block + hidden_i] += delta * hidden[hidden_i];}}}
      // NEGATIVE SAMPLING
      if (x2v->x2vec_negsamp > 0) {
        long long neg_i, negsamp = x2v->x2vec_negsamp; 
        int target, label;
	for (neg_i = 0; neg_i < negsamp + 1; neg_i++) {
          long long negsamp_output_weights; 
          real sum=0, output=0, delta=0;
	  if (neg_i == 0) {
	    target = word;
	    label = 1;}
	  else {
	    next_random(random_value);
	    target = unigrams[(random_value >> 16) % unigrams_size];
	    if (target == 0) target = random_value % (vocab_size - 1) + 1;
	    if (target == word) continue;
	    label = 0;}
          negsamp_output_weights = target * hidden_size;
	  for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
	    sum += hidden[hidden_i] * syn1neg[negsamp_output_weights + hidden_i];}
          output=getexp(sum,exp_table,exp_max,exp_slots);
          delta = (label - output) * alpha;
	  for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
	    errv[hidden_i] += delta * syn1neg[negsamp_output_weights + hidden_i];}
	  for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
	    syn1neg[negsamp_output_weights + hidden_i] += delta * hidden[hidden_i];}}}
      // hidden -> in
      for (window_off = window_margin;
           window_off < window_lim; 
           window_off++) {
	if (window_off != window) {
	  block_off = block_position - window + window_off;
	  if (block_off < 0) continue;
	  if (block_off >= block_length) continue;
	  last_word = block[block_off];
	  if (last_word == -1) continue;
	  for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
	    syn0[hidden_i + (last_word * hidden_size)] += errv[hidden_i];}}}
      /* (cbow) */}
    else { //train skip-gram
      long long window_margin=random_value % x2v->x2vec_window;
      long long window_off=window_margin;
      long long window_lim= ( window * 2 ) + 1 - window_margin;
      long long block_off=0, hidden_i=0, last_word=-1;
      for (window_off = window_margin;
           window_off < window_lim;
           window_off++) {
	if (window_off != window) {
          long long layer1_weights; 
	  block_off = block_position - window + window_off;
	  if ( (block_off < 0) || (block_off >= block_length) ) continue;
	  last_word = block[block_off];
	  if (last_word == -1) continue;
	  layer1_weights = last_word * hidden_size;
          memset(errv,0,sizeof(real)*hidden_size);
	  // HIERARCHICAL SOFTMAX
	  if (x2v->x2vec_hisoftmax) {
            int bit;
	    for (bit = 0; bit < vocab[word].codelen; bit++) {
              long long layer2_weights = vocab[word].nodeid[bit] * hidden_size;
              real sum=0, output=0, delta=0;
	      // Propagate hidden -> output
	      for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
                sum += syn0[layer1_weights + hidden_i] * syn1[layer2_weights + hidden_i];}
	      if ((sum <= -exp_max)||(sum >= exp_max)) continue;
	      else output=getexp(sum,exp_table,exp_max,exp_slots);
	      // 'delta' is the gradient multiplied by the learning rate
	      delta = ((1 - vocab[word].code[bit]) - output) * alpha;
	      // Propagate errors output -> hidden
	      for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
		errv[hidden_i] += delta * syn1[layer2_weights + hidden_i];}
	      // Learn weights hidden -> output
	      for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
		syn1[layer2_weights + hidden_i] += delta * syn0[layer1_weights + hidden_i];}}}
	  // NEGATIVE SAMPLING
	  if (x2v->x2vec_negsamp > 0) {
            int neg_i=0, negsamp = x2v->x2vec_negsamp; 
            long long target, label, layer2_weights;
	    for (neg_i = 0; neg_i < negsamp + 1; neg_i++) {
              real sum=0, output=0, delta=0;
	      if (neg_i == 0) {
		target = word; label = 1;}
	      else {
		next_random(random_value);
		target = unigrams[(random_value >> 16) % unigrams_size];
		if (target == 0) target = random_value % (vocab_size - 1) + 1;
		if (target == word) continue;
		label = 0;}
	      layer2_weights = target * hidden_size;
	      for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
		sum += syn0[layer1_weights + hidden_i] * syn1neg[layer2_weights + hidden_i];}
              output=getexp(sum,exp_table,exp_max,exp_slots);
              delta = (label - output) * alpha;
	      for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
		errv[hidden_i] += delta * syn1neg[layer2_weights + hidden_i];}
	      for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
		syn1neg[hidden_i + layer2_weights] += delta * syn0[layer1_weights + hidden_i];}}}
	  // Learn weights input -> hidden
	  for (hidden_i = 0; hidden_i < hidden_size; hidden_i++) {
	    syn0[layer1_weights + hidden_i] += errv[hidden_i];}}}
      /* train_skipgram (!(bow)) */}
    block_position++;
    if (block_position >= block_length) 
      break;}
  
  if (use_hidden==NULL) u8_free(hidden);
  if (use_errv==NULL) u8_free(errv);

  return alpha;
}

void *modular_training_threadproc(void *state)
{
  struct KNO_X2VEC_STATE *x2vs=(struct KNO_X2VEC_STATE *)state;
  x2vec_context x2v=x2vs->x2vec_x2vcxt;
  x2vec_word vocab = x2v->x2vec_vocab;
  double start=x2vs->x2vec_start; 
  lispval opts=x2vs->x2vec_opts;
  int thread_i=x2vs->x2vec_thread_i, n_threads=x2vs->x2vec_n_threads;
  lispval input=x2vs->x2vec_input;
  unsigned long long random_value=thread_i;
  long long word;
  long long word_count = 0, last_word_count = 0, block[X2VEC_MAX_BLOCK_LENGTH + 1];
  int hidden_size = x2v->x2vec_hidden_size;
  struct KNO_VECTOR *vec=KNO_GET_CONS(input,kno_vector_type,struct KNO_VECTOR *);
  int vec_len = vec->vec_length, vec_pos=(vec_len/n_threads)*thread_i;
  lispval *vec_data = vec->vec_elts;
  real *hidden = (real *)
    safe_calloc(hidden_size, sizeof(real),"x2v/TrainModelThread/hidden");
  real *errv = (real *)
    safe_calloc(hidden_size, sizeof(real),"x2v/TrainModelThread/errv");
  int loglevel = x2v->x2vec_loglevel, logfreq = x2v->x2vec_logfreq;
  int *stopval = x2vs->x2vec_stop;

  long long train_words = x2v->x2vec_train_words;
  real starting_alpha = x2v->x2vec_starting_alpha, alpha = x2v->x2vec_alpha;
  real subsample_size = x2v->x2vec_subsample * x2v->x2vec_train_words;
  while (vec_pos<vec_len) {
    long long block_length = 0;
    if ((word_count - last_word_count) > 10000) {
      int word_count_actual=x2v->x2vec_word_count_actual+
        (word_count - last_word_count);
      x2v->x2vec_word_count_actual += (word_count - last_word_count);
      last_word_count = word_count;
      if ((show_progress>0)&&(u8_random(show_progress))) {
        fprintf(stderr,
                "%cAlpha: %f  Progress: %.2f%%  Words/sec: %.2fk  Words/thread/sec: %.2fk  ",
                13,alpha,word_count_actual / (real)(train_words + 1) * 100,
                (word_count_actual / (u8_elapsed_time()-(start))),
                (word_count / (u8_elapsed_time()-(start))));}
      if ((logfreq>0)&&(u8_random(logfreq)==0)) {
        u8_log(loglevel+1,"X2Vec/Training/progress",
               /* "%cAlpha: %f  Progress: %.2f%%  Words/thread/sec: %.2fk  ", 13, */
               "Alpha: %f  Progress: %.2f%%  Words/sec: %.2fk  Words/thread/sec: %.2fk  ",
               alpha, word_count_actual / (real)(train_words + 1) * 100,
               (word_count_actual / (u8_elapsed_time()-(start))),
               (word_count / (u8_elapsed_time()-(start))));}
      alpha = starting_alpha * 
        (1 - word_count_actual / (real)(train_words + 1));
      if (alpha < starting_alpha * 0.0001)
        alpha = starting_alpha * 0.0001;}
    while (vec_pos<vec_len) {
      lispval input = vec_data[vec_pos++];
      if (KNO_STRINGP(input)) 
        word = kno_x2vec_vocabid(x2v,KNO_CSTRING(input));
      else continue;
      if (word < 0) continue;
      word_count++;
      if (word == 0) break;
      // The subsampling randomly discards frequent words while
      // keeping the frequency ordering the same
      if (subsample_size > 0) {
        real ran = (sqrt(vocab[word].count / subsample_size) + 1) 
          * subsample_size / vocab[word].count;
        next_random(random_value);
        if (ran < (random_value & 0xFFFF) / (real)65536) 
          continue;} /* if (subsample_size > 0) */
      block[block_length] = word;
      block_length++;
      if (block_length >= X2VEC_MAX_BLOCK_LENGTH) 
        break;} /* (vec_pos<vec_len) */
    if (((stopval)&&(*stopval))||
        (word_count > (x2v->x2vec_train_words / n_threads))) {
      if (stopval) *stopval=1;
      break;}
    alpha=block_train(x2v,block,block_length,alpha,stopval,hidden,errv);
    /* while (vec_pos < vec_len) */}
  free(hidden);
  free(errv);
  kno_decref(opts);
  pthread_exit(NULL);
}

static void train_model(x2vec_context x2v,
			lispval training,
			lispval vocab,
			lispval opts)
{
  long a;
  lispval xopts=(KNO_VOIDP(opts))?(kno_incref(x2v->x2vec_opts)):
    (kno_make_pair(opts,x2v->x2vec_opts));
  int n_threads = getintopt(xopts,num_threads_symbol,default_num_threads);
  pthread_t *pt = (pthread_t *)malloc(n_threads * sizeof(pthread_t));
  struct KNO_X2VEC_STATE *thread_states =
    safe_calloc(n_threads,sizeof(struct KNO_X2VEC_STATE),
		"x2v/TrainModel/threadstates");
  double start, now;
  int stop=0;
  x2v->x2vec_starting_alpha = x2v->x2vec_alpha;

  init_exp_table(x2v);

  if (KNO_VOIDP(vocab)) {
    if (x2v->x2vec_vocab==NULL) {
      init_vocab(x2v,training);}
    else u8_log(x2v->x2vec_loglevel,"X2Vec/Vocab/Init",
                "Using existing vocabulary of %lld words",
                x2v->x2vec_vocab_size);}
  else {
    import_vocab(x2v,vocab);
    u8_log(x2v->x2vec_loglevel,"X2Vec/Vocab/Init",
           "Initialized %lld words from provided info",
           x2v->x2vec_vocab_size);}
  
  init_nn(x2v);

  start = u8_elapsed_time();

  u8_log(x2v->x2vec_loglevel,"X2Vec/Train",
         "Training %s%s%swith %d threads over %d tokens and %d terms\n\t%q",
         QUOTE_LABEL(x2v->x2vec_label),n_threads,
         KNO_VECTOR_LENGTH(training), x2v->x2vec_vocab_size,
         (lispval)x2v);

  /* Start all the threads */
  for (a = 0; a < n_threads; a++) {
    struct KNO_X2VEC_STATE *tstate=&(thread_states[a]);
    tstate->x2vec_x2vcxt=x2v; tstate->x2vec_input=training;
    tstate->x2vec_opts=xopts; kno_incref(xopts);
    tstate->x2vec_thread_i=a; tstate->x2vec_n_threads=n_threads;
    tstate->x2vec_start=start; tstate->x2vec_stop=&stop;
    pthread_create(&pt[a], NULL, training_threadproc, (void *) tstate);}
  kno_decref(xopts);
  /* Wait for all of the threads to finish */
  for (a = 0; a < n_threads; a++) {
    pthread_join(pt[a], NULL);}

  now=u8_elapsed_time();

  u8_log(x2v->x2vec_loglevel,"X2Vec/Train/Done",
         "Finished training %s%s%sin %.2fs over %d inputs and %d terms using %d threads\n\t%q",
         QUOTE_LABEL(x2v->x2vec_label),now-start,
         KNO_VECTOR_LENGTH(training),x2v->x2vec_vocab_size,n_threads,
         (lispval)x2v);

  if ((x2v->x2vec_n_clusters)&&(x2v->x2vec_word2cluster==NULL)) {
    lispval nc=kno_getopt(x2v->x2vec_opts,n_clusters_symbol,KNO_VOID);
    if (KNO_FIXNUMP(nc))
      x2v->x2vec_word2cluster=
        kno_x2vec_classify(x2v,x2v->x2vec_n_clusters,x2v->x2vec_n_cluster_rounds,
                          &(x2v->x2vec_centers));
    else kno_decref(nc);}
}

static void modular_train_model(x2vec_context x2v,
                                lispval training,
                                lispval vocab,
                                lispval opts)
{
  long a;
  lispval xopts=(KNO_VOIDP(opts))?(kno_incref(x2v->x2vec_opts)):
    (kno_make_pair(opts,x2v->x2vec_opts));
  int n_threads = getintopt(xopts,num_threads_symbol,default_num_threads);
  pthread_t *pt = (pthread_t *)malloc(n_threads * sizeof(pthread_t));
  struct KNO_X2VEC_STATE *thread_states =
    safe_calloc(n_threads,sizeof(struct KNO_X2VEC_STATE),
		"x2v/TrainModel/threadstates");
  double start, now;
  int stop=0;
  x2v->x2vec_starting_alpha = x2v->x2vec_alpha;

  init_exp_table(x2v);

  if (KNO_VOIDP(vocab)) {
    if (x2v->x2vec_vocab==NULL) {
      init_vocab(x2v,training);}
    else u8_log(x2v->x2vec_loglevel,"X2Vec/Vocab/Init",
                "Using existing vocabulary of %lld words",
                x2v->x2vec_vocab_size);}
  else {
    import_vocab(x2v,vocab);
    u8_log(x2v->x2vec_loglevel,"X2Vec/Vocab/Init",
           "Initialized %lld words from provided info",
           x2v->x2vec_vocab_size);}
  
  init_nn(x2v);

  start = u8_elapsed_time();

  u8_log(x2v->x2vec_loglevel,"X2Vec/Train",
         "Training %s%s%swith %d threads over %d tokens and %d terms\n\t%q",
         QUOTE_LABEL(x2v->x2vec_label),n_threads,
         KNO_VECTOR_LENGTH(training), x2v->x2vec_vocab_size,
         (lispval)x2v);

  /* Start all the threads */
  for (a = 0; a < n_threads; a++) {
    struct KNO_X2VEC_STATE *tstate=&(thread_states[a]);
    tstate->x2vec_x2vcxt=x2v; tstate->x2vec_input=training;
    tstate->x2vec_opts=xopts; kno_incref(xopts);
    tstate->x2vec_thread_i=a; tstate->x2vec_n_threads=n_threads;
    tstate->x2vec_start=start; tstate->x2vec_stop=&stop;
    pthread_create(&pt[a], NULL, modular_training_threadproc, (void *) tstate);}
  kno_decref(xopts);
  /* Wait for all of the threads to finish */
  for (a = 0; a < n_threads; a++) {
    pthread_join(pt[a], NULL);}

  now=u8_elapsed_time();

  u8_log(x2v->x2vec_loglevel,"X2Vec/Train/Done",
         "Finished training %s%s%sin %.2fs over %d inputs and %d terms using %d threads\n\t%q",
         QUOTE_LABEL(x2v->x2vec_label),now-start,
         KNO_VECTOR_LENGTH(training),x2v->x2vec_vocab_size,n_threads,
         (lispval)x2v);

  if ((x2v->x2vec_n_clusters)&&(x2v->x2vec_word2cluster==NULL)) {
    lispval nc=kno_getopt(x2v->x2vec_opts,n_clusters_symbol,KNO_VOID);
    if (KNO_FIXNUMP(nc))
      x2v->x2vec_word2cluster=
        kno_x2vec_classify(x2v,x2v->x2vec_n_clusters,x2v->x2vec_n_cluster_rounds,
                          &(x2v->x2vec_centers));
    else kno_decref(nc);}
}

/* Computing classes from vectors */

KNO_EXPORT int *kno_x2vec_classify(x2vec_context x2v,
                                 int n_clusters,int n_rounds,
                                 real **save_centers)
{
  // Run K-means on the word vectors
  size_t vocab_size = x2v->x2vec_vocab_size;
  int hidden_size = x2v->x2vec_hidden_size;
  const real *syn0 = x2v->x2vec_syn0;
  int *class_counts = (int *)
    safe_malloc(n_clusters * sizeof(int),
		"x2vec/TrainModel/getClasses/class_counts");
  int *classes = (int *)
    safe_calloc(x2v->x2vec_vocab_size, sizeof(int),
		"x2vec/TrainModel/getClasses/cl");
  real *centers = (real *)
    safe_calloc(n_clusters * hidden_size, sizeof(real),
		"x2vec/TrainModel/getClasses/centers");
  long a, b, c, d;

  u8_log(x2v->x2vec_loglevel,"X2Vec/Classify",
         "Computing %d clusters over the result vectors",
         x2v->x2vec_n_clusters);

  /* Assign all the terms to a class arbitrarily */
  for (a = 0; a < vocab_size; a++) classes[a] = a % n_clusters;
  /* Iterate multiple times */
  for (a = 0; a < n_rounds; a++) {
    /* Initialize the centers */
    for (b = 0; b < n_clusters * hidden_size; b++) centers[b] = 0;
    /* Initialize the counts */
    for (b = 0; b < n_clusters; b++) class_counts[b] = 1;
    /* Add together all the vectors by class */
    for (c = 0; c < vocab_size; c++) {
      for (d = 0; d < hidden_size; d++) {
	centers[hidden_size * classes[c] + d] += syn0[c * hidden_size + d];
	class_counts[classes[c]]++;}}
    /* Compute the actual centers (divide by count) and normalize them */
    for (b = 0; b < n_clusters; b++) {
      real self_product = 0, len;
      for (c = 0; c < hidden_size; c++) {
	centers[hidden_size * b + c] /= class_counts[b];
	self_product += centers[hidden_size * b + c] * centers[hidden_size * b + c];}
      len = sqrt(self_product);
      for (c = 0; c < hidden_size; c++) {
	centers[hidden_size * b + c] /= len;}}
    /* Reassign classes based on the closest vector */
    for (c = 0; c < vocab_size; c++) {
      int closeid=-1; real best=0, sim=0;
      for (d = 0; d < n_clusters; d++) {
	sim = 0;
	for (b = 0; b < hidden_size; b++) {
	  sim += centers[hidden_size * d + b] * syn0[c * hidden_size + b];}
        if ((sim > best)||(closeid < 0)) {
	  best = sim;
	  closeid = d;}}
      classes[c] = closeid;}}

  free(class_counts);
  if (save_centers) 
    *save_centers=centers;
  else free(centers);

  return classes;
}

/* init functions */

static void init_exp_table(x2vec_context x2vcxt)
{
  if (x2vcxt->x2vec_exp_table) return;
  else {
    int exp_slots = x2vcxt->x2vec_exp_slots;
    int exp_max = x2vcxt->x2vec_exp_max;
    real *exp_table = x2vcxt->x2vec_exp_table=exp_table = (real *)
      safe_malloc((exp_slots + 1) * sizeof(real),"init_exp_table");
    // Precompute the exp() table
    int i=0; for (i = 0; i < exp_slots; i++) {
      float rough_exp = 
        exp(( i / (real)exp_slots * 2 - 1 ) * exp_max);
      exp_table[i] = rough_exp / (rough_exp + 1);}}
}

static void init_unigrams(x2vec_context x2vcxt) 
{
  int a, i;
  long long train_words_pow = 0;
  real d1, power = 0.75;
  x2vec_word vocab=x2vcxt->x2vec_vocab;
  long long vocab_size=x2vcxt->x2vec_vocab_size;
  int *unigrams=x2vcxt->x2vec_unigrams;
  long long unigrams_size=x2vcxt->x2vec_unigrams_size;

  if (unigrams==NULL) {
    unigrams = x2vcxt->x2vec_unigrams = (int *)malloc(unigrams_size * sizeof(int));
    if (unigrams == NULL) {
      u8_raise(kno_MallocFailed,"init_unigrams",NULL);}}

  for (a = 0; a < vocab_size; a++) {
    train_words_pow += pow(vocab[a].count, power);}
  i = 0;
  d1 = pow(vocab[i].count, power) / (real)train_words_pow;
  for (a = 0; a < unigrams_size; a++) {
    unigrams[a] = i;
    if (a / (real)unigrams_size > d1) {
      i++;
      d1 += pow(vocab[i].count, power) / (real)train_words_pow;
    }
    if (i >= vocab_size) i = vocab_size - 1;
  }
}

/* Initializing the vocabulary */

static x2vec_word init_vocab_tables
(x2vec_context x2v,ssize_t init_vocab_size)
{
  x2vec_word vocab;
  size_t vocab_hash_size=x2v->x2vec_vocab_hash_size;
  size_t vocab_alloc_size=x2v->x2vec_vocab_alloc_size;
  if (vocab_alloc_size==0) 
    vocab_alloc_size=x2v->x2vec_vocab_alloc_size=
      (init_vocab_size>0)?(init_vocab_size):(X2V_DEFAULT_VOCAB_SIZE);
  if (vocab_hash_size==0) 
    vocab_hash_size=x2v->x2vec_vocab_hash_size=2*vocab_alloc_size;
  if (x2v->x2vec_vocab_hash==NULL) {
    long long vocab_i;
    int *vocab_hash = x2v->x2vec_vocab_hash = 
      u8_alloc_n(x2v->x2vec_vocab_hash_size,int);
    for (vocab_i = 0; vocab_i < vocab_hash_size; vocab_i++) 
      vocab_hash[vocab_i] = -1;}
  if (x2v->x2vec_vocab) return x2v->x2vec_vocab;
  vocab = x2v->x2vec_vocab = u8_alloc_n(vocab_alloc_size,struct KNO_X2VEC_WORD);
  memset(x2v->x2vec_vocab,0,vocab_alloc_size*sizeof(struct KNO_X2VEC_WORD));
  return vocab;
}

static int *init_vocab_hash(int n)
{
  int *hash=u8_alloc_n(n,int);
  int i=0; while (i<n) hash[i++]=-1;
  return hash;
}

static void init_vocab_word
(x2vec_word word,u8_string spelling,int count)
{
  word->word=u8_strdup(spelling);
  word->count=count;
}

static void hash_overflow(x2vec_context x2v)
{
  if (x2v->x2vec_vocab_size <= (x2v->x2vec_vocab_hash_size * 0.7)) return;
  if (x2v->x2vec_vocab_hash_max>0) {
    grow_vocab_hash(x2v);
    if (x2v->x2vec_vocab_size > (x2v->x2vec_vocab_hash_size * 0.7)) {
      u8_log(LOG_CRIT,"X2Vec/Vocab/Reduce",
             "Couldn't grow hashtable for %s%s%s, reducing vocabulary for\n\t%q",
             QUOTE_LABEL(x2v->x2vec_label),(lispval)x2v);}}
  if (x2v->x2vec_vocab_size > (x2v->x2vec_vocab_hash_size * 0.7)) {
    while (x2v->x2vec_vocab_size > (x2v->x2vec_vocab_hash_size * 0.7))
      reduce_vocab(x2v);}
}

static void grow_vocab_hash(x2vec_context x2v)
{
  size_t vocab_hash_size=x2v->x2vec_vocab_hash_size;
  size_t new_size=vocab_hash_size*2; 
  int *new_hashtable=realloc(x2v->x2vec_vocab_hash,new_size*sizeof(int)); 
  if (new_hashtable) {
    off_t hash_i=0, vocab_i=0;
    size_t vocab_size=x2v->x2vec_vocab_size;
    x2vec_word vocab=x2v->x2vec_vocab;
    x2v->x2vec_vocab_hash=new_hashtable;
    x2v->x2vec_vocab_hash_size=new_size;
    for (hash_i = 0; hash_i < new_size; hash_i++) 
      new_hashtable[hash_i] = -1;
    for (vocab_i = 0; vocab_i < vocab_size; vocab_i++) {
      struct KNO_X2VEC_WORD v=vocab[vocab_i];
      int hash = kno_x2vec_hash(x2v,v.word);
      while (new_hashtable[hash] != -1)
        hash = (hash + 1) % vocab_hash_size;
      new_hashtable[hash]=vocab_i;}}
  else {
    u8_log(LOG_WARN,kno_MallocFailed,
           "Couldn't access memory to grow vocabulary hash table");
    x2v->x2vec_vocab_hash_max=0;}
}

/* Initializing the network from saved data */

KNO_EXPORT lispval kno_x2vec_init(x2vec_context x2vcxt,lispval init)
{
  size_t hidden_size=x2vcxt->x2vec_hidden_size; int memalign_rv;
  int init_vocab=x2vcxt->x2vec_vocab==NULL;
  if (KNO_TABLEP(init)) {
    int i=0; lispval keys=kno_getkeys(init);
    size_t vocab_size=(init_vocab)?(KNO_CHOICE_SIZE(keys)):
      (x2vcxt->x2vec_vocab_size);
    size_t syn0_size=vocab_size*hidden_size;
    memalign_rv=posix_memalign((void **)&(x2vcxt->x2vec_syn0), 
                               128, syn0_size * sizeof(real));
    if (memalign_rv==EINVAL) {
      u8_raise(_("Bad posix_memalign argument"),"x2vec/kno_init_vecs",NULL);}
    else if (memalign_rv==EINVAL) {
      u8_raise(_("Not enough memory"),"x2vec/kno_init_vecs",NULL);}
    else if ((x2vcxt->x2vec_syn1neg) == NULL) {
      u8_raise(kno_MallocFailed,"x2vec/kno_init_vecs",NULL);}
    else {
      real *syn0=x2vcxt->x2vec_syn0;
      x2vec_word vocab=(init_vocab)?
        (init_vocab_tables(x2vcxt,vocab_size)):
        (x2vcxt->x2vec_vocab);
      KNO_DO_CHOICES(key,keys) {
	if (KNO_STRINGP(key)) {
	  lispval v=kno_get(init,key,KNO_VOID);
	  if (KNO_VOIDP(v)) {}
	  else if ((KNO_PRIM_TYPEP(v,kno_numeric_vector_type))&&
		   ((KNO_NUMVEC_TYPE(v)==kno_float_elt)||
		    (KNO_NUMVEC_TYPE(v)==kno_double_elt))) {
	    int n=KNO_NUMVEC_LENGTH(v);
	    float *write=&syn0[i*hidden_size];
	    if (KNO_NUMVEC_TYPE(v)==kno_float_elt) {
	      kno_float *f=KNO_NUMVEC_FLOATS(v);
	      int i=0; while (i<n) {write[i]=f[i]; i++;}}
	    else {
	      kno_double *d=KNO_NUMVEC_DOUBLES(v);
	      int i=0; while (i<n) {write[i]=(float)d[i]; i++;}}
	    if (init_vocab) init_vocab_word(&vocab[i],KNO_CSTRING(key),1);}
	  else if (KNO_PRIM_TYPEP(v,kno_vector_type)) {
	    int n=KNO_VECTOR_LENGTH(v);
	    lispval *elts=KNO_VECTOR_ELTS(v);
	    float *f=&syn0[i*hidden_size];
	    int i=0; while (i<n) {
	      lispval elt=elts[i];
	      if (KNO_FLONUMP(elt)) {
		f[i]=KNO_FLONUM(elt);}
	      else {
		u8_log(LOGWARN,"BadX2VecValue","%q is not a flonum",elt);}
	      i++;}
	    if (init_vocab)
	      init_vocab_word(&vocab[i],KNO_CSTRING(key),1);}
	  else {
	    u8_log(LOGWARN,"BadX2VecValue",
		   "%q is not a vector or float vector",v);}
	  kno_decref(v);
	  i++;}
	else {}}}
    kno_decref(keys);
    return KNO_VOID;}
  else return KNO_VOID;
}

KNO_EXPORT int kno_x2vec_import_vocab(x2vec_context x2vcxt,lispval data)
{
  if (x2vcxt->x2vec_vocab_locked) {
    kno_seterr(_("X2Vec/Vocab/Init/error"),"kno_init_vocab",
              "Vocabulary locked for %q",(lispval)x2vcxt);
    return -1;}
  else {
    import_vocab(x2vcxt,data);
    return 0;}
}

/* Creating an X2VEC context */

KNO_EXPORT x2vec_context kno_init_x2vec(x2vec_context x2v,lispval opts)
{
  lispval label=kno_getopt(opts,label_symbol,KNO_VOID);
  lispval optsep=kno_getopt(opts,optsep_symbol,KNO_VOID);
  size_t vocab_alloc_size;

  if (x2v==NULL) {
    x2v=u8_alloc(struct KNO_X2VEC_CONTEXT);
    KNO_INIT_FRESH_CONS(x2v,kno_x2vec_type);}
  else {
    KNO_INIT_STATIC_CONS(x2v,kno_x2vec_type);}
  x2v->x2vec_opts=opts; kno_incref(opts);

  x2v->x2vec_vocab_alloc_size = vocab_alloc_size = 
    getintopt(opts,vocab_size_symbol,default_vocab_size);

  if (vocab_alloc_size==default_vocab_size)
    x2v->x2vec_vocab_hash_size=
      getintopt(opts,hash_size_symbol,default_hash_size);
  else x2v->x2vec_vocab_hash_size=
         getintopt(opts,hash_size_symbol,vocab_alloc_size*2);
  x2v->x2vec_vocab_hash_max=
    getintopt(opts,hash_max_symbol,default_hash_max);
  x2v->x2vec_unigrams_size=
    getintopt(opts,unigrams_size_symbol,default_unigrams_size);
  /* Not using yet, so vocab_max_size == vocab_size */
  x2v->x2vec_vocab_max_size = getintopt(opts,vocab_size_symbol,-1);

  x2v->x2vec_hidden_size=getintopt(opts,hidden_size,default_hidden_size);
  x2v->x2vec_window=getintopt(opts,window_symbol,default_window);
  x2v->x2vec_hash_reduce=getintopt(opts,hash_reduce_symbol,default_hash_reduce);
  x2v->x2vec_min_count=getintopt(opts,min_count_symbol,default_min_count);
  x2v->x2vec_n_clusters=getintopt(opts,n_clusters_symbol,default_n_clusters); 
  x2v->x2vec_n_cluster_rounds=
    getintopt(opts,n_cluster_rounds_symbol,default_n_cluster_rounds);
  
  x2v->x2vec_exp_slots=
    getrealopt(opts,exp_slots_symbol,default_exp_slots);
  x2v->x2vec_exp_max=getrealopt(opts,exp_max_symbol,default_exp_max);

  x2v->x2vec_alpha=getrealopt(opts,alpha_symbol,default_init_alpha);
  x2v->x2vec_subsample=getrealopt(opts,subsample_symbol,default_subsample);
  x2v->x2vec_negsamp=getintopt(opts,negsamp_symbol,default_negsamp);
  x2v->x2vec_hisoftmax=getboolopt(opts,hisoftmax_symbol,default_hisoft);
  x2v->x2vec_loglevel=getintopt(opts,loglevel_symbol,default_loglevel);
  x2v->x2vec_logfreq=getintopt(opts,logfreq_symbol,default_logfreq);

  if (kno_testopt(opts,mode_symbol,bag_of_words_symbol))
    x2v->x2vec_bag_of_words=1;
  else if (kno_testopt(opts,mode_symbol,skipgram_symbol))
    x2v->x2vec_bag_of_words=0;
  else x2v->x2vec_bag_of_words=default_cbow;

  if (KNO_STRINGP(label))
    x2v->x2vec_label=u8_strdup(KNO_CSTRING(label));
  else if (KNO_SYMBOLP(label))
    x2v->x2vec_label=u8_strdup(KNO_SYMBOL_NAME(label));
  else if ((KNO_PAIRP(label))||(KNO_VECTORP(label))||(KNO_NUMBERP(label)))
    x2v->x2vec_label=kno_lisp2string(label);
  else {}

  if (KNO_STRINGP(optsep)) {
    if (KNO_STRLEN(optsep)==1)
      x2v->x2vec_opt_sep=KNO_CSTRING(optsep)[0];
    else {
      u8_log(LOGWARN,InvalidOpSeparator,
             "Ignoring multi-character opt separator %q",optsep);}}
  else if (KNO_CHARACTERP(optsep)) {
    int code=KNO_CHAR2CODE(optsep);
    if (code<0x80) {
      x2v->x2vec_opt_sep=KNO_CSTRING(optsep)[0];}
    else {
      u8_log(LOGWARN,InvalidOpSeparator,
             "Ignoring non-ascii opt separator %q",optsep);}}
  else if (KNO_FIXNUMP(optsep)) {
    int code=KNO_FIX2INT(optsep);
    if (code<0x80) {
      x2v->x2vec_opt_sep=KNO_CSTRING(optsep)[0];}
    else {
      u8_log(LOGWARN,InvalidOpSeparator,
             "Ignoring non-ascii opt separator %q",optsep);}}
  else if (KNO_VOIDP(optsep)) {}
  else {
    u8_log(LOGWARN,InvalidOpSeparator,
           "Ignoring invalid word opt separator %q",optsep);}

  kno_decref(label);
  kno_decref(optsep);

  return x2v;
}

KNO_EXPORT x2vec_context kno_x2vec_start
(lispval opts,lispval training_data,lispval vocab_init)
{
  x2vec_context x2v = kno_init_x2vec(NULL,opts);

  if ( (KNO_VOIDP(training_data)) && (KNO_VOIDP(vocab_init)) ) 
    return x2v;
  else if (KNO_VECTORP(training_data)) {kno_incref(training_data);}
  else if (KNO_STRINGP(training_data)) {
    lispval wordvec=kno_words2vector(KNO_CSTRING(training_data),0);
    training_data=wordvec;}
  else {
    kno_seterr(kno_TypeError,"kno_start_x2vec","training data",KNO_VOID);
    return NULL;}
  train_model(x2v,training_data,vocab_init,opts);

  kno_decref(training_data);

  return x2v;
}

KNO_EXPORT x2vec_context kno_x2vec_modular_start
(lispval opts,lispval training_data,lispval vocab_init)
{
  x2vec_context x2v = kno_init_x2vec(NULL,opts);

  if ( (KNO_VOIDP(training_data)) && (KNO_VOIDP(vocab_init)) ) 
    return x2v;
  else if (KNO_VECTORP(training_data)) {kno_incref(training_data);}
  else if (KNO_STRINGP(training_data)) {
    lispval wordvec=kno_words2vector(KNO_CSTRING(training_data),0);
    training_data=wordvec;}
  else {
    kno_seterr(kno_TypeError,"kno_start_x2vec","training data",KNO_VOID);
    return NULL;}
  modular_train_model(x2v,training_data,vocab_init,opts);

  kno_decref(training_data);

  return x2v;
}

static lispval x2vec_start_prim(lispval opts,lispval data,lispval vocab)
{

  x2vec_context x2v=kno_x2vec_start(opts,data,vocab);
  if (x2v==NULL)
    return KNO_ERROR_VALUE;
  else return (lispval) x2v;
}

static lispval x2vec_modular_start_prim(lispval opts,lispval data,lispval vocab)
{
  x2vec_context x2v=kno_x2vec_modular_start(opts,data,vocab);
  if (x2v==NULL)
    return KNO_ERROR_VALUE;
  else return (lispval) x2v;
}

static lispval x2vec_train_prim(lispval x2v_arg,lispval input,lispval alpha_arg)
{
  x2vec_context x2v=(x2vec_context ) x2v_arg;
  double alpha = ((KNO_VOIDP(alpha_arg))?(x2v->x2vec_alpha):
                  (KNO_FLONUM(alpha_arg)));
  double result=0;
  int vec_i=0, vec_length=KNO_VECTOR_LENGTH(input), block_length=0;
  lispval *vec_elts = KNO_VECTOR_ELTS(input);
  long long *block = u8_alloc_n(vec_length,long long);
  while ( vec_i<vec_length ) {
    lispval elt=vec_elts[vec_i++];
    if (!(KNO_STRINGP(elt))) continue;
    long long word = vocab_ref(x2v,KNO_CSTRING(elt),NULL);
    if (word>=0) block[block_length++]=word;}

  result = block_train(x2v,block,block_length,alpha,NULL,NULL,NULL);

  u8_free(block);

  return kno_make_flonum( result );
}

static lispval x2vec_add_vocab_prim(lispval x2varg,lispval vocab)
{
  x2vec_context x2v=(x2vec_context )x2varg;
  long long retval=import_vocab(x2v,vocab);
  if (retval<0) 
    return KNO_ERROR_VALUE;
  else return KNO_INT2DTYPE(retval);
}

/* Getting stuff out to Scheme */

static lispval x2vec_inputs_prim(lispval arg)
{
  x2vec_context x2v = (x2vec_context ) arg;
  int i=0, n=x2v->x2vec_vocab_size;
  x2vec_word words=x2v->x2vec_vocab;
  lispval result=kno_make_vector(n,NULL);
  while (i<n) {
    lispval s=kno_make_string(NULL,-1,words[i].word);
    KNO_VECTOR_SET(result,i,s);
    i++;}
  return result;
}

static lispval x2vec_counts_prim(lispval arg,lispval word)
{
  x2vec_context x2v = (x2vec_context ) arg;
  int i=0, n=x2v->x2vec_vocab_size;
  x2vec_word words=x2v->x2vec_vocab;
  if (KNO_VOIDP(word)) {
    lispval result=kno_make_hashtable(NULL,n);
    kno_hashtable h=(kno_hashtable) result;
    while (i<n) {
      lispval s=kno_make_string(NULL,-1,words[i].word);
      lispval v=KNO_INT2DTYPE(words[i].count);
      kno_hashtable_op(h,kno_table_store_noref,s,v);
      i++;}
    return result;}
  else if (KNO_STRINGP(word)) {
    u8_string s=KNO_CSTRING(word);
    int id=kno_x2vec_vocabid(x2v,s);
    if (id<0) return KNO_EMPTY_CHOICE;
    else {
      x2vec_word vocab=x2v->x2vec_vocab;
      int count=vocab[id].count;
      return KNO_INT2DTYPE(count);}}
  else return kno_type_error("string","x2vec_counts_prim",word);
}

static lispval x2vec_get_prim(lispval arg,lispval term)
{
  x2vec_context x2v = (x2vec_context ) arg;
  size_t hidden_size=x2v->x2vec_hidden_size;
  real *syn0=x2v->x2vec_syn0;
  if (syn0==NULL)
    return KNO_EMPTY_CHOICE;
  else if (KNO_STRINGP(term)) {
    int i=kno_x2vec_vocabid(x2v,KNO_CSTRING(term));
    if (i<0) return KNO_EMPTY_CHOICE;
    else {
      return normalized_floatvec(hidden_size,&(syn0[i*hidden_size]));}}
  else return kno_type_error(_("string"),"x2vec_get_prim",term);
}

static lispval x2vec_vectors_prim(lispval arg)
{
  x2vec_context x2v = (x2vec_context ) arg;
  size_t vocab_size=x2v->x2vec_vocab_size;
  size_t hidden_size=x2v->x2vec_hidden_size;
  x2vec_word vocab=x2v->x2vec_vocab;
  real *syn0=x2v->x2vec_syn0;
  if (syn0==NULL)
    return KNO_EMPTY_CHOICE;
  else {
    struct KNO_HASHTABLE *ht=(kno_hashtable)
      kno_make_hashtable(NULL,vocab_size+vocab_size/2);
    int i=0; 
    while (i<vocab_size) {
      u8_string word=vocab[i].word;
      lispval key=kno_make_string(NULL,-1,word);
      lispval result=normalized_floatvec(hidden_size,&(syn0[i*hidden_size]));
      kno_hashtable_store(ht,key,result);
      kno_decref(key); kno_decref(result);
      i++;}
    return (lispval)ht;}
}

static lispval x2vec_getclassvec_prim(lispval arg,lispval n_clusters,lispval n_rounds)
{
  x2vec_context x2v = (x2vec_context ) arg;
  int n=(KNO_VOIDP(n_clusters))?(x2v->x2vec_n_clusters):(KNO_FIX2INT(n_clusters));
  int rounds=(KNO_VOIDP(n_rounds))?(x2v->x2vec_n_cluster_rounds):(KNO_FIX2INT(n_rounds));
  int n_words=x2v->x2vec_vocab_size;
  if ((x2v->x2vec_word2cluster)&&
      ((KNO_VOIDP(n_clusters))||
       ((KNO_FIX2INT(n_clusters))==x2v->x2vec_n_clusters))&&
      ((KNO_VOIDP(n_rounds))||
       ((KNO_FIX2INT(n_rounds))==x2v->x2vec_n_cluster_rounds))) {
    int *word2cluster=x2v->x2vec_word2cluster;
    lispval result=kno_make_int_vector(n_words,word2cluster);
    return result;}
  else {
    int *word2cluster=kno_x2vec_classify(x2v,n,rounds,&(x2v->x2vec_centers));
    lispval result=kno_make_int_vector(n_words,word2cluster);
    if (n==x2v->x2vec_n_clusters) 
      x2v->x2vec_word2cluster=word2cluster;
    else u8_free(word2cluster);
    return result;}
}

static lispval x2vec_getclusters_prim(lispval arg,lispval n_clusters,lispval n_rounds)
{
  x2vec_context x2v = (x2vec_context ) arg;
  int class_i=0, vocab_i=0, *word2cluster=NULL;
  int n=(KNO_VOIDP(n_clusters))?(x2v->x2vec_n_clusters):(KNO_FIX2INT(n_clusters));
  int rounds=(KNO_VOIDP(n_rounds))?(x2v->x2vec_n_cluster_rounds):(KNO_FIX2INT(n_rounds));
  struct KNO_HASHSET **sets=u8_alloc_n(n,struct KNO_HASHSET *);
  x2vec_word vocab=x2v->x2vec_vocab;
  ssize_t vocab_size=x2v->x2vec_vocab_size;
  lispval result=KNO_VOID;
  if ((n==x2v->x2vec_n_clusters)&&(rounds=x2v->x2vec_n_cluster_rounds)) {
    if (x2v->x2vec_word2cluster) 
      word2cluster=x2v->x2vec_word2cluster;
    else 
      word2cluster=x2v->x2vec_word2cluster=
        kno_x2vec_classify(x2v,x2v->x2vec_n_clusters,x2v->x2vec_n_cluster_rounds,
                          &(x2v->x2vec_centers));}
  else word2cluster=kno_x2vec_classify(x2v,n,rounds,NULL);
  class_i=0; while (class_i<n) {
    struct KNO_HASHSET *hs=u8_alloc(struct KNO_HASHSET);
    kno_init_hashset(hs,(2*(vocab_size/n))+17,0);
    sets[class_i++]=hs;}
  vocab_i=0; while (vocab_i<vocab_size) {
    int cluster_id=word2cluster[vocab_i]; 
    if (cluster_id<n) {
      lispval string=lispval_string(vocab[vocab_i].word);
      kno_hashset_add_raw(sets[cluster_id],string); 
      kno_decref(string);}
    else {
      u8_log(LOGWARN,"X2Vec/Cluster","Cluster ID out of range");}
    vocab_i++;}
  if (word2cluster!=x2v->x2vec_word2cluster)
    u8_free(word2cluster);
  result=kno_make_vector(n,(lispval *)sets);
  u8_free(sets);
  return result;
}

static lispval x2vec_getcenters_prim(lispval arg,lispval n_clusters,lispval n_rounds)
{
  x2vec_context x2v = (x2vec_context ) arg;
  int class_i=0, *word2cluster=NULL, vec_size=x2v->x2vec_hidden_size;
  int n=(KNO_VOIDP(n_clusters))?(x2v->x2vec_n_clusters):(KNO_FIX2INT(n_clusters));
  int rounds=(KNO_VOIDP(n_rounds))?(x2v->x2vec_n_cluster_rounds):(KNO_FIX2INT(n_rounds));
  lispval result=KNO_VOID;
  real *centers;
  if ((n==x2v->x2vec_n_clusters)&&(rounds=x2v->x2vec_n_cluster_rounds)) {
    if (x2v->x2vec_word2cluster) {
      word2cluster=x2v->x2vec_word2cluster;
      centers=x2v->x2vec_centers;}
    else {
      word2cluster=x2v->x2vec_word2cluster=
        kno_x2vec_classify(x2v,x2v->x2vec_n_clusters,x2v->x2vec_n_cluster_rounds,&centers);
      x2v->x2vec_centers=centers;}}
  else word2cluster=kno_x2vec_classify(x2v,n,rounds,&centers);
  result=kno_make_vector(n,NULL);
  class_i=0; while (class_i<n) {
    lispval center=kno_make_float_vector(vec_size,centers+class_i);
    KNO_VECTOR_SET(result,class_i,center);
    class_i++;}
  if (centers!=x2v->x2vec_centers)
    u8_free(centers);
  if (word2cluster!=x2v->x2vec_word2cluster)
    u8_free(word2cluster);
  return result;
}

static lispval x2vec_cosim_prim(lispval term1,lispval term2,lispval x2varg)
{
  x2vec_context x2v=(KNO_VOIDP(x2varg))?(NULL):
    ((x2vec_context )x2varg);
  int len1=0, len2=0, free1=0, free2=0;
  float *vec1=get_float_vec(x2v,term1,&len1,&free1);
  float *vec2=(vec1!=NULL)?(get_float_vec(x2v,term2,&len2,&free2)):(NULL);
  if ((vec1)&&(vec2)&&(len1==len2)) {
    if ((free1)||(free2)) {
      float r=cosim_float(len1,vec1,vec2,0);
      if (free1) u8_free(vec1);
      if (free2) u8_free(vec2);
      return kno_make_flonum(r);}
    else return kno_make_flonum(cosim_float(len1,vec1,vec2,0));}
  else {
    if ((vec1)&&(vec2)) {
      u8_byte buf[100];
      kno_seterr(_("vector lengths differ"),"x2vec_cosim_prim",
                u8_sprintf(buf,100,"%d != %d",len1,len2),
                KNO_VOID);}
    if (free1) u8_free(vec1);
    if (free2) u8_free(vec2);
    return KNO_ERROR_VALUE;}
}

static float *get_float_vec(x2vec_context x2v,
                            lispval arg,int *lenp,int *freep)
{
  if ((KNO_NUMERIC_VECTORP(arg))&&
      (KNO_NUMVEC_TYPE(arg)==kno_float_elt)) {
    *lenp=KNO_NUMVEC_LENGTH(arg);
    return KNO_NUMVEC_FLOATS(arg);}
  else if ((KNO_NUMERIC_VECTORP(arg))&&
           (KNO_NUMVEC_TYPE(arg)==kno_double_elt)) {
    int i=0, n=KNO_NUMVEC_LENGTH(arg);
    kno_double *elts=KNO_NUMVEC_DOUBLES(arg);
    real *result=u8_alloc_n(n,float);
    while (i<n) {result[i]=(float)elts[i]; i++;}
    *lenp=n; *freep=1;
    return result;}
  else if (KNO_VECTORP(arg)) {
    int i=0, n=KNO_VECTOR_LENGTH(arg);
    lispval *elts=KNO_VECTOR_ELTS(arg);
    real *result=u8_alloc_n(n,float);
    while (i<n) {
      lispval elt=*elts;
      if (KNO_FLONUMP(elt)) {
        kno_double d=KNO_FLONUM(elt);
        result[i]=(float)d;}
      else if (KNO_NUMBERP(elt)) {
        kno_double d=kno_todouble(elt);
        result[i]=(float)d;}
      else if (KNO_TRUEP(elt)) {
        result[i]=(float)1;}
      else if (KNO_FALSEP(elt)) {
        result[i]=(float)0;}
      else {
        kno_incref(arg);
        kno_seterr(kno_TypeError,"get_float_vec","numeric element",arg);
        u8_free(result);
        return NULL;}
      i++; elts++;}
    *lenp=n; *freep=1;
    return result;}
  else if (KNO_STRINGP(arg)) {
    if (x2v) {
      float *v=kno_x2vec_get(x2v,KNO_CSTRING(arg));
      *lenp=x2v->x2vec_hidden_size;
      return v;}
    else {
      kno_seterr(_("Not a vector"),"get_float_vec",KNO_CSTRING(arg),KNO_VOID);
      return NULL;}}
  else if (KNO_FIXNUMP(arg)) {
    int id=KNO_FIX2INT(arg);
    if ((x2v)&&(id>=0)&&(id<x2v->x2vec_vocab_size)) {
      float *v=&((x2v->x2vec_syn0)[id*(x2v->x2vec_hidden_size)]);
      *lenp=x2v->x2vec_hidden_size;
      return v;}
    else if (x2v) {
      kno_seterr(_("Invalid vocab ID"),"get_float_vec",NULL,arg);
      return NULL;}
    else {
      kno_seterr(_("No X2VEC available"),"get_float_vec",
                KNO_CSTRING(arg),KNO_VOID);
      return NULL;}}
  else if (x2v) {
    kno_incref(arg); 
    kno_seterr(_("not a string, id, or vector"),"get_float_vec",NULL,arg);
    return NULL;}
  else {
    kno_seterr(_("not a vector"),"get_float_vec",NULL,arg); kno_incref(arg);
    return NULL;}
}

/* Reading a vector file */

KNO_EXPORT x2vec_context kno_x2vec_read(u8_string filename,lispval opts)
{
  FILE *f = u8_fopen(filename,"rb");
  if (f==NULL) {
    u8_seterr(kno_FileNotFound,"kno_x2vec_read",u8_strdup(filename));
    return NULL;}
  else {
    x2vec_context x2v = kno_init_x2vec(NULL,opts);
    if (x2v->x2vec_label==NULL) x2v->x2vec_label=u8_strdup(filename);
    if (read_vectors(x2v,f)<0) {
      lispval dtypeval=(lispval)x2v;
      fclose(f);
      kno_decref(dtypeval);
      return NULL;}
    else return x2v;}
}

static lispval x2vec_read_prim(lispval filename,lispval opts)
{
  u8_string path=KNO_CSTRING(filename);
  if (u8_file_existsp(path)) {
    x2vec_context x2v=kno_x2vec_read(path,opts);
    if (x2v==NULL)
      return KNO_ERROR_VALUE;
    else return (lispval) x2v;}
  else return kno_err(kno_FileNotFound,"x2vec_read_prim",NULL,filename);
}

static long long read_vectors(x2vec_context x2v,FILE *f)
{
  long i=0, j=0, n_words, n_dimensions, vocab_hash_size;
  x2vec_word vocab;
  int *vocab_hash; float *vecs;
  if (fscanf(f,"%ld",&n_words)<1) {
    kno_seterr(_("Bad vector data file"),"read_vectors",NULL,KNO_VOID);
    return -1;}
  else if (fscanf(f,"%ld",&n_dimensions)<1) {
    kno_seterr(_("Bad vector data file"),"read_vectors",NULL,KNO_VOID);
    return -1;}
  else {
    u8_log(x2v->x2vec_loglevel,"x2vec/ReadVectors",
           "Reading vector data for %d words x %d dimensions",
           n_words,n_dimensions);
    x2v->x2vec_vocab=vocab=u8_alloc_n(n_words,struct KNO_X2VEC_WORD);
    x2v->x2vec_vocab_size=n_words; x2v->x2vec_vocab_alloc_size=n_dimensions;
    x2v->x2vec_vocab_hash_size=vocab_hash_size=kno_get_hashtable_size(2*n_words);
    x2v->x2vec_vocab_hash=vocab_hash=init_vocab_hash(vocab_hash_size);
    x2v->x2vec_syn0=vecs=u8_alloc_n(n_dimensions*n_words,float);
    x2v->x2vec_hidden_size=n_dimensions;
    for ( i = 0; i < n_words; i++ ) {
      char buf[64], ch; /* float len=0; */
      float *vec=vecs+(i*n_dimensions);
      if (fscanf(f,"%s%c",buf,&ch)<1) {
        kno_seterr(_("Bad vector data file"),"read_vectors",NULL,KNO_VOID);
        return -1;}
      else {
        vocab_init(x2v,buf,i);
        if ((fread(vec+j, sizeof(float), n_dimensions, f))!=n_dimensions) {
          kno_seterr(_("Bad vector data file"),"read_vectors",NULL,KNO_VOID);
          return -1;}
        /*
          for ( j = 0; j < n_dimensions; j++ )
          if ((fread(vec+j, sizeof(float), 1, f))!=1) {
          kno_seterr(_("Bad vector data file"),"read_vectors",NULL,KNO_VOID);
          return -1;}
          for ( j = 0; j< n_dimensions; j++ ) 
          len = len + ( vec[j] * vec[j] );
          len=sqrt(len);
          for ( j = 0; j< n_dimensions; j++ ) 
          vec[j] = vec[j] / len;
        */}}
    return n_words*n_dimensions;}
}

/* Handlers */

static void recycle_x2vec(struct KNO_RAW_CONS *c)
{
  x2vec_context x2v=(x2vec_context )c;
  x2vec_word vocab=x2v->x2vec_vocab;

  if (vocab) {
    int i=0, n=x2v->x2vec_vocab_size; while (i<n) {
      u8_free(vocab[i].word); 
      FREE_LOC(vocab[i].nodeid);
      i++;}
    FREE_LOC(x2v->x2vec_vocab);}
  FREE_LOC(x2v->x2vec_vocab_hash);
  FREE_LOC(x2v->x2vec_unigrams);
  FREE_LOC(x2v->x2vec_word2cluster);
  FREE_LOC(x2v->x2vec_syn0);
  FREE_LOC(x2v->x2vec_syn1);
  FREE_LOC(x2v->x2vec_syn1neg);
  FREE_LOC(x2v->x2vec_exp_table);
  FREE_LOC(x2v->x2vec_label);

  u8_free(x2v);
}

static int unparse_x2vec(struct U8_OUTPUT *out,lispval x)
{
  x2vec_context x2v=(x2vec_context )x;
  u8_printf(out,"#<X2VEC %s%s%s(%s%lld terms) x (%d outputs) #!%llx>",
            QUOTE_LABEL(x2v->x2vec_label),
            ((x2v->x2vec_vocab_locked)?(U8S0()):(U8STR("~"))),
            x2v->x2vec_vocab_size,
            x2v->x2vec_hidden_size,
            KNO_LONGVAL(x2v));
  return 1;
}

/* Initialization */

KNO_EXPORT int kno_init_x2vec_c(void) KNO_LIBINIT_FN;
static long long int x2vec_initialized=0;

static void init_x2vec_symbols(void);

KNO_EXPORT int kno_init_x2vec_c()
{
  lispval module;
  if (x2vec_initialized) return 0;
  x2vec_initialized=u8_millitime();

  kno_init_texttools();

  kno_x2vec_type=kno_register_cons_type("X2VEC");
  kno_recyclers[kno_x2vec_type]=recycle_x2vec;
  kno_unparsers[kno_x2vec_type]=unparse_x2vec;

  init_x2vec_symbols();

  module=kno_new_cmodule("x2vec",(KNO_MODULE_SAFE),kno_init_x2vec_c);

  kno_idefn(module,kno_make_cprim3("X2VEC/START",x2vec_start_prim,1));
  kno_idefn(module,kno_make_cprim3("X2VEC/MSTART",x2vec_modular_start_prim,1));
  kno_idefn(module,kno_make_cprim2("X2VEC/READ",x2vec_read_prim,1));
  kno_idefn(module,kno_make_cprim2x("X2VEC/VOCAB!",x2vec_add_vocab_prim,2,
                                  kno_x2vec_type,-1,-1,KNO_VOID));
  kno_idefn(module,kno_make_cprim3x
           ("X2VEC/TRAIN",x2vec_train_prim,2,
            kno_x2vec_type,-1,kno_vector_type,KNO_VOID,
            kno_flonum_type,KNO_VOID));

  kno_idefn(module,kno_make_cprim1x
           ("X2VEC/INPUTS",x2vec_inputs_prim,1,kno_x2vec_type,KNO_VOID));
  kno_idefn(module,kno_make_cprim2x
           ("X2VEC/COUNTS",x2vec_counts_prim,1,kno_x2vec_type,KNO_VOID,kno_string_type,KNO_VOID));
  kno_idefn(module,kno_make_cprim1x("X2VEC/VECTORS",x2vec_vectors_prim,1,kno_x2vec_type,KNO_VOID));
  kno_idefn(module,kno_make_cprim2x
           ("X2VEC/GET",x2vec_get_prim,2,
            kno_x2vec_type,KNO_VOID,kno_string_type,KNO_VOID));
  kno_idefn(module,kno_make_cprim3x
           ("X2VEC/COSIM",x2vec_cosim_prim,2,
            -1,KNO_VOID,-1,KNO_VOID,kno_x2vec_type,KNO_VOID));
  kno_idefn(module,kno_make_cprim3x
           ("X2VEC/CLASSVEC",x2vec_getclassvec_prim,1,
            kno_x2vec_type,KNO_VOID,kno_fixnum_type,KNO_VOID,kno_fixnum_type,KNO_VOID));
  kno_idefn(module,kno_make_cprim3x
           ("X2VEC/CLUSTERS",x2vec_getclusters_prim,1,
            kno_x2vec_type,KNO_VOID,kno_fixnum_type,KNO_VOID,kno_fixnum_type,KNO_VOID));
  kno_idefn(module,kno_make_cprim3x
           ("X2VEC/CENTERS",x2vec_getcenters_prim,1,
            kno_x2vec_type,KNO_VOID,kno_fixnum_type,KNO_VOID,kno_fixnum_type,KNO_VOID));
  kno_defalias(module,"X2VEC/DISTANCE","X2VEC/COSIM");
  kno_defalias(module,"COSIM","X2VEC/COSIM");
  
  kno_register_config("X2VEC:MONITOR",
                     "How often to update the progress display during training",
                     kno_boolconfig_get,kno_boolconfig_set,&show_progress);
  kno_register_config("X2VEC:VECSIZE",
                     "How many elements in the item vectors (the hidden layer size)",
                     kno_intconfig_get,kno_intconfig_set,&default_hidden_size);
  kno_register_config("X2VEC:WINDOW",
                     "The context window for the training algorithm",
                     kno_intconfig_get,kno_intconfig_set,&default_window);
  kno_register_config("X2VEC:MINCOUNT",
                     "The minimum frequency for terms to be used in training",
                     kno_intconfig_get,kno_intconfig_set,&default_min_count);
  kno_register_config("X2VEC:HASHMAX",
                     "The maximum size for growing the input hashtable",
                     kno_intconfig_get,kno_intconfig_set,&default_hash_max);
  kno_register_config("X2VEC:HASHSIZE",
                     "Initial size for the vocabulary hash table",
                     kno_intconfig_get,kno_intconfig_set,&default_vocab_size);
  kno_register_config("X2VEC:HASHREDUCE",
                     "Threshold for reducing the vocabulary when the hashtable overflows",
                     kno_intconfig_get,kno_intconfig_set,&default_hash_reduce);
  kno_register_config("X2VEC:TABLESIZE",
                     "The maximum size for growing the input hahstable",
                     kno_intconfig_get,kno_intconfig_set,&default_unigrams_size);
  kno_register_config("X2VEC:CBOW",
                     "Whether to use CBOW (Continuous Bag Of Words) for training",
                     kno_boolconfig_get,kno_boolconfig_set,&default_cbow);
  kno_register_config("X2VEC:VOCABSIZE",
                     "The initial size for the vocabulary table",
                     kno_intconfig_get,kno_intconfig_set,&default_vocab_size);
  kno_register_config("X2VEC:NCLASSES",
                     "How many classes to generate by default",
                     kno_intconfig_get,kno_intconfig_set,&default_n_clusters);
  
  kno_register_config("X2VEC:ALPHA","The default initial training rate",
                     kno_dblconfig_get,kno_dblconfig_set,&default_init_alpha);
  kno_register_config("X2VEC:SUBSAMPLE",
                     "The threshold for subsampling during training",
                     kno_dblconfig_get,kno_dblconfig_set,&default_subsample);
  kno_register_config("X2VEC:NEGATIVE",
                     "The number of negative samples to use during training",
                     kno_dblconfig_get,kno_dblconfig_set,&default_subsample);

  kno_register_config("X2VEC:LOGFREQ",
                     "How often to log trainining (roughly every n*10000 words)",
                     kno_intconfig_get,kno_intconfig_set,&default_logfreq);

  kno_register_config("X2VEC:EXPMAX",
                     "The maximum/minimum value for estimated exponents",
                     kno_dblconfig_get,kno_dblconfig_set,
                     &default_exp_max);
  kno_register_config("X2VEC:EXP-PRECISION",
                     "The maximum/minimum value for estimated exponents",
                     kno_intconfig_get,kno_intconfig_set,
                     &default_exp_slots);


  u8_register_source_file(_FILEINFO);

  return 1;
}

static void init_x2vec_symbols()
{
  label_symbol=kno_intern("label");
  hidden_size=kno_intern("vecsize");
  window_symbol=kno_intern("window");
  num_threads_symbol=kno_intern("nthreads");
  hash_reduce_symbol=kno_intern("hashreduce");
  min_count_symbol=kno_intern("min-count");
  n_clusters_symbol=kno_intern("n-classes");
  n_cluster_rounds_symbol=kno_intern("n-rounds");
  unigrams_size_symbol=kno_intern("unigrams-size");
  vocab_size_symbol=kno_intern("vocab-size");
  hash_size_symbol=kno_intern("hashsize");
  alpha_symbol=kno_intern("alpha");
  subsample_symbol=kno_intern("subsample");
  negsamp_symbol=kno_intern("negsamp");
  hisoftmax_symbol=kno_intern("hisoftmax");
  mode_symbol=kno_intern("mode");
  bag_of_words_symbol=KNO_EMPTY_CHOICE;
  KNO_ADD_TO_CHOICE(bag_of_words_symbol,kno_intern("bag_of_words"));
  KNO_ADD_TO_CHOICE(bag_of_words_symbol,kno_intern("bag-of-words"));
  KNO_ADD_TO_CHOICE(bag_of_words_symbol,kno_intern("bagofwords"));
  KNO_ADD_TO_CHOICE(bag_of_words_symbol,kno_intern("bow"));
  skipgram_symbol=KNO_EMPTY_CHOICE;
  KNO_ADD_TO_CHOICE(skipgram_symbol,kno_intern("skipgram"));
  hash_max_symbol=kno_intern("hashmax");
  exp_max_symbol=kno_intern("expmax");
  exp_slots_symbol=kno_intern("expslots");
  loglevel_symbol=kno_intern("loglevel");
  logfreq_symbol=kno_intern("logfreq");
  optsep_symbol=kno_intern("optsep");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
