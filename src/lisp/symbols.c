/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"

#include "libu8/u8memlist.h"

int kno_flipcase_fix = 1;

lispval *kno_symbol_names;
int kno_n_symbols = 0, kno_max_symbols = 0, kno_initial_symbols = 4096;
struct KNO_SYMBOL_TABLE kno_symbol_table;

lispval FDSYM_ADD, FDSYM_ADJUNCT, FDSYM_ALL, FDSYM_ALWAYS;
lispval FDSYM_BLOCKSIZE, FDSYM_BUFSIZE;
lispval FDSYM_CACHELEVEL, FDSYM_CACHESIZE;
lispval FDSYM_CONS, FDSYM_CONTENT, FDSYM_CREATE;
lispval FDSYM_DEFAULT, FDSYM_DOT, FDSYM_DROP;
lispval FDSYM_ENCODING, FDSYM_EQUALS, FDSYM_ERROR;
lispval FDSYM_FILE, FDSYM_FILENAME;
lispval FDSYM_FLAGS, FDSYM_FORMAT, FDSYM_FRONT, FDSYM_INPUT;
lispval FDSYM_ISADJUNCT, FDSYM_KEYSLOT;
lispval FDSYM_LABEL, FDSYM_LAZY, FDSYM_LENGTH, FDSYM_LOGLEVEL;
lispval FDSYM_MAIN, FDSYM_MERGE, FDSYM_METADATA;
lispval FDSYM_MINUS, FDSYM_MODULE;
lispval FDSYM_NAME, FDSYM_NO, FDSYM_NONE, FDSYM_NOT;
lispval FDSYM_OPT, FDSYM_OPTS, FDSYM_OUTPUT;
lispval FDSYM_PCTID, FDSYM_PLUS, FDSYM_PREFIX, FDSYM_PROPS;
lispval FDSYM_QMARK, FDSYM_QUOTE, FDSYM_READONLY, FDSYM_SEP;
lispval FDSYM_SET, FDSYM_SIZE, FDSYM_SORT, FDSYM_SORTED;
lispval FDSYM_SOURCE, FDSYM_STAR, FDSYM_STORE, FDSYM_STRING, FDSYM_SUFFIX;
lispval FDSYM_TAG, FDSYM_TEST, FDSYM_TEXT, FDSYM_TYPE;
lispval FDSYM_VERSION, FDSYM_VOID;

u8_rwlock kno_symbol_lock;
static u8_memlist old_symbol_data = NULL;

#define MYSTERIOUS_MULTIPLIER 2654435769U
#define MYSTERIOUS_MODULUS 256001281

KNO_FASTOP unsigned int mult_hash_bytes(const unsigned char *start,int len)
{
  unsigned int h = 0;
  unsigned *istart = (unsigned int *)start, asint = 0;
  const unsigned int *scan = istart, *limit = istart+len/4;
  const unsigned char *tail = start+(len/4)*4;
  while (scan<limit) h = ((127*h)+*scan++)%MYSTERIOUS_MODULUS;
  switch (len%4) {
  case 0: asint = 1; break;
  case 1: asint = tail[0]; break;
  case 2: asint = tail[0]|(tail[1]<<8); break;
  case 3: asint = tail[0]|(tail[1]<<8)|(tail[2]<<16); break;}
  h = ((127*h)+asint)%MYSTERIOUS_MODULUS;
  return h;
}

static void init_builtin_symbols(void);

static void init_symbol_tables()
{
  u8_write_lock(&kno_symbol_lock);
  if (kno_max_symbols) {
    u8_rw_unlock(&kno_symbol_lock); return;}
  else {
    int new_max = ((kno_max_symbols) ? (kno_max_symbols) : (kno_initial_symbols));
    int new_size = kno_get_hashtable_size(new_max*2);
    struct KNO_SYMBOL_ENTRY **new_entries = u8_alloc_n(new_size,kno_symbol_entry);
    lispval *new_symbol_names = u8_alloc_n(new_max,lispval);
    int i = 0, lim = new_size; while (i < lim) new_entries[i++]=NULL;
    i = 0; lim = new_max; while (i < lim) new_symbol_names[i++]=VOID;
    kno_symbol_table.table_size = new_size;
    kno_symbol_table.kno_symbol_entries = new_entries;
    kno_symbol_names = new_symbol_names; kno_max_symbols = new_max;
    u8_rw_unlock(&kno_symbol_lock);
    init_builtin_symbols();
  }
}

static void init_builtin_symbols()
{
  /* Make these low-numberd symbols to have them early in sorts */
  kno_intern("%id"); kno_intern("name"); kno_intern("id"); kno_intern("_id");
  kno_intern("label");

  /* Here they are in alphabetical order */
  FDSYM_ADD = kno_intern("add");
  FDSYM_ADJUNCT = kno_intern("adjunct");
  FDSYM_ALL = kno_intern("all");
  FDSYM_ALWAYS = kno_intern("always");
  FDSYM_BLOCKSIZE = kno_intern("blocksize");
  FDSYM_BUFSIZE = kno_intern("bufsize");
  FDSYM_CACHELEVEL = kno_intern("cachelevel");
  FDSYM_CACHESIZE = kno_intern("cachesize");
  FDSYM_CONS = kno_intern("cons");
  FDSYM_CONTENT = kno_intern("content");
  FDSYM_CREATE = kno_intern("create");
  FDSYM_CREATE = kno_intern("file");
  FDSYM_DEFAULT = kno_intern("default");
  FDSYM_DOT = kno_intern(".");
  FDSYM_DROP = kno_intern("drop");
  FDSYM_ENCODING = kno_intern("encoding");
  FDSYM_EQUALS = kno_intern("=");
  FDSYM_ERROR = kno_intern("error");
  FDSYM_FILE = kno_intern("file");
  FDSYM_FILENAME = kno_intern("filename");
  FDSYM_FLAGS = kno_intern("flags");
  FDSYM_FORMAT = kno_intern("format");
  FDSYM_FRONT = kno_intern("front");
  FDSYM_INPUT = kno_intern("input");
  FDSYM_ISADJUNCT = kno_intern("isadjunct");
  FDSYM_KEYSLOT = kno_intern("keyslot");
  FDSYM_LABEL = kno_intern("label");
  FDSYM_LAZY = kno_intern("lazy");
  FDSYM_LENGTH = kno_intern("length");
  FDSYM_LOGLEVEL = kno_intern("loglevel");
  FDSYM_MAIN = kno_intern("main");
  FDSYM_MERGE = kno_intern("merge");
  FDSYM_METADATA = kno_intern("metadata");
  FDSYM_MINUS = kno_intern("-");
  FDSYM_MODULE = kno_intern("module");
  FDSYM_NAME = kno_intern("name");
  FDSYM_NO = kno_intern("no");
  FDSYM_NONE = kno_intern("none");
  FDSYM_NOT = kno_intern("not");
  FDSYM_OPT = kno_intern("opt");
  FDSYM_OPTS = kno_intern("opts");
  FDSYM_OUTPUT = kno_intern("output");
  FDSYM_PCTID = kno_intern("%id");
  FDSYM_PLUS = kno_intern("+");
  FDSYM_PREFIX = kno_intern("prefix");
  FDSYM_PROPS = kno_intern("props");
  FDSYM_QMARK = kno_intern("?");
  FDSYM_QUOTE = kno_intern("quote");
  FDSYM_READONLY = kno_intern("readonly");
  FDSYM_SEP = kno_intern("sep");
  FDSYM_SET = kno_intern("set");
  FDSYM_SIZE = kno_intern("size");
  FDSYM_SORT = kno_intern("sort");
  FDSYM_SORTED = kno_intern("sorted");
  FDSYM_SOURCE = kno_intern("source");
  FDSYM_STAR = kno_intern("*");
  FDSYM_STORE = kno_intern("store");
  FDSYM_STRING = kno_intern("string");
  FDSYM_SUFFIX = kno_intern("suffix");
  FDSYM_TAG = kno_intern("tag");
  FDSYM_TEST = kno_intern("test");
  FDSYM_TEXT = kno_intern("text");
  FDSYM_TYPE = kno_intern("type");
  FDSYM_VERSION = kno_intern("version");
  FDSYM_VOID = kno_intern("void");
}

static void grow_symbol_tables()
{
  int new_max = kno_max_symbols*2;
  int new_size = kno_get_hashtable_size(new_max*2);
  struct KNO_SYMBOL_ENTRY **old_entries = kno_symbol_table.kno_symbol_entries;
  struct KNO_SYMBOL_ENTRY **new_entries = u8_alloc_n(new_size,kno_symbol_entry);
  lispval *new_symbol_names = u8_alloc_n(new_max,lispval);
  {
    int i = 0, lim = kno_symbol_table.table_size;
    while (i < new_size) new_entries[i++]=NULL;
    i = 0; while (i < lim)
      if (old_entries[i] == NULL) i++;
      else {
        struct KNO_SYMBOL_ENTRY *entry = old_entries[i];
        int probe=
          mult_hash_bytes(entry->sym_pname.str_bytes,
                          entry->sym_pname.str_bytelen)%new_size;
        while (PRED_TRUE(new_entries[probe]!=NULL))
          if (probe >= new_size)
            probe = 0;
          else probe = (probe+1)%new_size;
        new_entries[probe]=entry;
        i++;}
    old_symbol_data = u8_cons_list(old_entries,old_symbol_data,0);
    kno_symbol_table.kno_symbol_entries = new_entries;
    kno_symbol_table.table_size = new_size;}
  {
    int i = 0, lim = kno_n_symbols; lispval *old_symbol_names;
    while (i < lim) {new_symbol_names[i]=kno_symbol_names[i]; i++;}
    old_symbol_names = kno_symbol_names;
    kno_symbol_names = new_symbol_names;
    old_symbol_data = u8_cons_list(old_symbol_names,old_symbol_data,0);}
  kno_max_symbols = new_max;
}

lispval probe_symbol(u8_string bytes,int len)
{
  struct KNO_SYMBOL_ENTRY **entries = kno_symbol_table.kno_symbol_entries;
  int probe, size = kno_symbol_table.table_size;
  if ((kno_n_symbols == 0)||(kno_max_symbols == 0)) return VOID;
  probe = mult_hash_bytes(bytes,len)%size;
  while (entries[probe]) {
    if (len == entries[probe]->sym_pname.str_bytelen)
      if (strncmp(bytes,entries[probe]->sym_pname.str_bytes,len) == 0)
        break;
    probe++;
    if (probe >= size) probe = 0;}
  if (entries[probe]) {
    return KNO_ID2SYMBOL(entries[probe]->symid);}
  else return VOID;
}

lispval kno_make_symbol(u8_string bytes,int len)
{
  if (kno_max_symbols == 0) init_symbol_tables();
  if (len<0) len=strlen(bytes);
  lispval sym=probe_symbol(bytes,len);
  if (SYMBOLP(sym))
    return sym;
  else {
    u8_write_lock(&kno_symbol_lock);
    struct KNO_SYMBOL_ENTRY **entries = kno_symbol_table.kno_symbol_entries;
    int size = kno_symbol_table.table_size;
    int hash = mult_hash_bytes(bytes,len);
    int probe = hash%size;
    while (PRED_TRUE(entries[probe]!=NULL)) {
      unsigned int bytelen=(entries[probe])->sym_pname.str_bytelen;
      if (PRED_TRUE(len == bytelen)) {
        const unsigned char *pname=(entries[probe])->sym_pname.str_bytes;
        if (PRED_TRUE(strncmp(bytes,pname,len) == 0))
          break;}
      probe++; if (probe>=size) probe = 0;}
    if (entries[probe]) {
      int id = entries[probe]->symid;
      u8_rw_unlock(&kno_symbol_lock);
      return KNO_ID2SYMBOL(id);}
    else {
      if (kno_n_symbols >= kno_max_symbols) {
        grow_symbol_tables();
        u8_rw_unlock(&kno_symbol_lock);
        return kno_make_symbol(bytes,len);}
      else {
        int id = kno_n_symbols++;
        u8_string pname = u8_strdup(bytes);
        entries[probe]=u8_alloc(struct KNO_SYMBOL_ENTRY);
        entries[probe]->symid = id;
        kno_init_string(&(entries[probe]->sym_pname),len,pname);
        kno_symbol_names[id]=LISP_CONS(&(entries[probe]->sym_pname));
        u8_rw_unlock(&kno_symbol_lock);
        return KNO_ID2SYMBOL(id);}}}
}

KNO_EXPORT lispval kno_probe_symbol(u8_string bytes,int len)
{
  if (len<0) len=strlen(bytes);
  u8_read_lock(&kno_symbol_lock);
  lispval sym = probe_symbol(bytes,len);
  u8_rw_unlock(&kno_symbol_lock);
  return sym;
}

KNO_EXPORT lispval kno_intern(u8_string string)
{
  return kno_make_symbol(string,strlen(string));
}

KNO_EXPORT lispval kno_getsym(u8_string string)
{
  const u8_byte *scan = string;
  U8_STATIC_OUTPUT(name,64);
  int c = u8_sgetc(&scan);
  while (c>=0) {
    u8_putc(nameout,u8_tolower(c));
    c = u8_sgetc(&scan);}
  lispval result = kno_make_symbol(name.u8_outbuf,name.u8_write-name.u8_outbuf);
  u8_close((u8_stream)nameout);
  return result;
}

KNO_EXPORT lispval kno_fixcase_symbol(u8_string bytes,int len)
{
  int string_case = 0;
  u8_string scan = bytes, limit = bytes+len;
  while (scan < limit) {
    int c = u8_sgetc(&scan);
    if (u8_islower(c)) {
      if (string_case>0)
        return kno_make_symbol(bytes,len);
      else if (string_case == 0)
        string_case = -1;}
    else if (u8_isupper(c)) {
      if (string_case<0)
        return kno_make_symbol(bytes,len);
      else if (string_case == 0)
        string_case = 1;}
    else NO_ELSE;}
  if (string_case > 0)
    return kno_getsym(bytes);
  else return kno_make_symbol(bytes,len);
}

KNO_EXPORT lispval kno_all_symbols()
{
  lispval results = EMPTY;
  int i = 0, n = kno_n_symbols;
  while (i < n) {
    lispval sym = KNO_ID2SYMBOL(i);
    CHOICE_ADD(results,sym);
    i++;}
  return results;
}

/* Initialization */

static int check_symbol(lispval x)
{
  int id = KNO_SYMBOL2ID(x);
  return (id<kno_n_symbols);
}

void kno_init_symbols_c()
{
  kno_immediate_checkfns[kno_symbol_type]=check_symbol;
  u8_register_source_file(_FILEINFO);
}

#if 0
KNO_EXPORT
static int n_refs = 0, n_probes = 0;
void kno_list_symbol_table()
{
  int i = 0, size = kno_symbol_table.size;
  struct KNO_SYMBOL_ENTRY **entries = kno_symbol_table.entries;
  fprintf(stderr,"%d symbols, %d max symbols\n",
          kno_n_symbols,kno_max_symbols);
  fprintf(stderr,"%d refs, %d probes\n",n_refs,n_probes);
  while (i < size)
    if (entries[i]) {
      u8_string string = entries[i]->name.str_bytes;
      int len = entries[i]->name.str_bytelen, chain = 0;
      int hash = mult_hash_bytes(string,len);
      int probe = hash%size;
      fprintf(stderr,"%s\t%d\t%d\t%d",
              string,len,hash,probe);
      while (entries[probe]) {
        if (strcmp(entries[probe]->name.str_bytes,string)==0) break;
        else fprintf(stderr,"\t%s",entries[probe]->name.str_bytes);
        chain++;
        if (probe == (size-1)) probe = 0; else probe++;}
      fprintf(stderr,"\t%d\n",chain);
      i++;}
    else i++;
}
#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
