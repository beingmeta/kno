/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
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

lispval KNOSYM_ADD, KNOSYM_ADJUNCT, KNOSYM_ALL, KNOSYM_ALWAYS, KNOSYM_ARG;
lispval KNOSYM_ATMARK;
lispval KNOSYM_BLOCKSIZE, KNOSYM_BUFSIZE;
lispval KNOSYM_CACHELEVEL, KNOSYM_CACHESIZE;
lispval KNOSYM_CONS, KNOSYM_CONTENT, KNOSYM_CREATE;
lispval KNOSYM_DEFAULT, KNOSYM_DRIVER, KNOSYM_DOT, KNOSYM_DROP, KNOSYM_DTYPE;
lispval KNOSYM_ENCODING, KNOSYM_EQUALS, KNOSYM_ERROR;
lispval KNOSYM_FILE, KNOSYM_FILENAME;
lispval KNOSYM_FLAGS, KNOSYM_FORMAT, KNOSYM_FRONT;
lispval KNOSYM_GT, KNOSYM_GTE;
lispval KNOSYM_HASHMARK, KNOSYM_HISTREF, KNOSYM_HISTORY_TAG;
lispval KNOSYM_ID, KNOSYM_INDEX, KNOSYM_INPUT, KNOSYM_ISADJUNCT;
lispval KNOSYM_KEY, KNOSYM_KEYSLOT, KNOSYM_KEYVAL;
lispval KNOSYM_LABEL, KNOSYM_LAZY, KNOSYM_LENGTH, KNOSYM_LOGLEVEL;
lispval KNOSYM_LT, KNOSYM_LTE;
lispval KNOSYM_MAIN, KNOSYM_MERGE, KNOSYM_METADATA;
lispval KNOSYM_MINUS, KNOSYM_MODSRC, KNOSYM_MODULE, KNOSYM_MODULEID;
lispval KNOSYM_NAME, KNOSYM_NO, KNOSYM_NONE, KNOSYM_NOT;
lispval KNOSYM_OPT, KNOSYM_OPTIONAL, KNOSYM_OPTS, KNOSYM_OUTPUT;
lispval KNOSYM_PACKET, KNOSYM_PCTID, KNOSYM_PLUS, KNOSYM_POOL;
lispval KNOSYM_PREFIX, KNOSYM_PROPS;
lispval KNOSYM_QMARK, KNOSYM_QONST, KNOSYM_QUOTE, KNOSYM_READONLY, KNOSYM_SEP;
lispval KNOSYM_SET, KNOSYM_SIZE, KNOSYM_SORT, KNOSYM_SORTED;
lispval KNOSYM_SOURCE, KNOSYM_STAR, KNOSYM_STORE, KNOSYM_STRING, KNOSYM_SUFFIX;
lispval KNOSYM_TAG, KNOSYM_TEST, KNOSYM_TEXT, KNOSYM_TYPE;
lispval KNOSYM_UTF8;
lispval KNOSYM_VALUE, KNOSYM_VERSION, KNOSYM_VOID;
lispval KNOSYM_XREFS, KNOSYM_XTYPE, KNOSYM_XXREFS;
lispval KNOSYM_ZIPSOURCE;

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
  KNOSYM_ADD = kno_intern("add");
  KNOSYM_ADJUNCT = kno_intern("adjunct");
  KNOSYM_ALL = kno_intern("all");
  KNOSYM_ALWAYS = kno_intern("always");
  KNOSYM_ARG = kno_intern("arg");
  KNOSYM_ATMARK = kno_intern("@");
  KNOSYM_BLOCKSIZE = kno_intern("blocksize");
  KNOSYM_BUFSIZE = kno_intern("bufsize");
  KNOSYM_CACHELEVEL = kno_intern("cachelevel");
  KNOSYM_CACHESIZE = kno_intern("cachesize");
  KNOSYM_CONS = kno_intern("cons");
  KNOSYM_CONTENT = kno_intern("content");
  KNOSYM_CREATE = kno_intern("create");
  KNOSYM_CREATE = kno_intern("file");
  KNOSYM_DEFAULT = kno_intern("default");
  KNOSYM_DRIVER = kno_intern("driver");
  KNOSYM_DOT = kno_intern(".");
  KNOSYM_DROP = kno_intern("drop");
  KNOSYM_DTYPE = kno_intern("dtype");
  KNOSYM_ENCODING = kno_intern("encoding");
  KNOSYM_EQUALS = kno_intern("=");
  KNOSYM_ERROR = kno_intern("error");
  KNOSYM_FILE = kno_intern("file");
  KNOSYM_FILENAME = kno_intern("filename");
  KNOSYM_FLAGS = kno_intern("flags");
  KNOSYM_FORMAT = kno_intern("format");
  KNOSYM_FRONT = kno_intern("front");
  KNOSYM_GT = kno_intern(">");
  KNOSYM_GTE = kno_intern(">=");
  KNOSYM_HASHMARK = kno_intern("%history");
  KNOSYM_HISTREF = kno_intern("%histref");
  KNOSYM_HISTORY_TAG = kno_intern("%history");
  KNOSYM_ID = kno_intern("id");
  KNOSYM_INDEX = kno_intern("index");
  KNOSYM_INPUT = kno_intern("input");
  KNOSYM_ISADJUNCT = kno_intern("isadjunct");
  KNOSYM_KEY = kno_intern("key");
  KNOSYM_KEYSLOT = kno_intern("keyslot");
  KNOSYM_KEYVAL = kno_intern("keyval");
  KNOSYM_LABEL = kno_intern("label");
  KNOSYM_LAZY = kno_intern("lazy");
  KNOSYM_LENGTH = kno_intern("length");
  KNOSYM_LOGLEVEL = kno_intern("loglevel");
  KNOSYM_LT = kno_intern("<");
  KNOSYM_LTE = kno_intern("<=");
  KNOSYM_MAIN = kno_intern("main");
  KNOSYM_MERGE = kno_intern("merge");
  KNOSYM_METADATA = kno_intern("metadata");
  KNOSYM_MINUS = kno_intern("-");
  KNOSYM_MODSRC = kno_intern("%modsrc");
  KNOSYM_MODULE = kno_intern("module");
  KNOSYM_MODULEID = kno_intern("%moduleid");
  KNOSYM_NAME = kno_intern("name");
  KNOSYM_NO = kno_intern("no");
  KNOSYM_NONE = kno_intern("none");
  KNOSYM_NOT = kno_intern("not");
  KNOSYM_OPT = kno_intern("opt");
  KNOSYM_OPTIONAL = kno_intern("optional");
  KNOSYM_OPTS = kno_intern("opts");
  KNOSYM_OUTPUT = kno_intern("output");
  KNOSYM_PACKET = kno_intern("packet");
  KNOSYM_POOL = kno_intern("pool");
  KNOSYM_PCTID = kno_intern("%id");
  KNOSYM_PLUS = kno_intern("+");
  KNOSYM_PREFIX = kno_intern("prefix");
  KNOSYM_PROPS = kno_intern("props");
  KNOSYM_QMARK = kno_intern("?");
  KNOSYM_QONST = kno_intern("qonst");
  KNOSYM_QUOTE = kno_intern("quote");
  KNOSYM_READONLY = kno_intern("readonly");
  KNOSYM_SEP = kno_intern("sep");
  KNOSYM_SET = kno_intern("set");
  KNOSYM_SIZE = kno_intern("size");
  KNOSYM_SORT = kno_intern("sort");
  KNOSYM_SORTED = kno_intern("sorted");
  KNOSYM_SOURCE = kno_intern("source");
  KNOSYM_STAR = kno_intern("*");
  KNOSYM_STORE = kno_intern("store");
  KNOSYM_STRING = kno_intern("string");
  KNOSYM_SUFFIX = kno_intern("suffix");
  KNOSYM_TAG = kno_intern("tag");
  KNOSYM_TEST = kno_intern("test");
  KNOSYM_TEXT = kno_intern("text");
  KNOSYM_TYPE = kno_intern("type");
  KNOSYM_UTF8 = kno_intern("utf8");
  KNOSYM_VALUE = kno_intern("value");
  KNOSYM_VERSION = kno_intern("version");
  KNOSYM_VOID = kno_intern("void");
  KNOSYM_XREFS = kno_intern("xrefs");
  KNOSYM_XTYPE = kno_intern("xtype");
  KNOSYM_XXREFS = kno_intern("%xrefs");
  KNOSYM_ZIPSOURCE = kno_intern("zipsource");
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
               while (USUALLY(new_entries[probe]!=NULL))
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
    while (USUALLY(entries[probe]!=NULL)) {
      unsigned int bytelen=(entries[probe])->sym_pname.str_bytelen;
      if (USUALLY(len == bytelen)) {
        const unsigned char *pname=(entries[probe])->sym_pname.str_bytes;
        if (USUALLY(strncmp(bytes,pname,len) == 0))
          break;}
      probe++;
      if (probe>=size) probe = 0;}
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
	struct KNO_SYMBOL_ENTRY *entry = u8_alloc(struct KNO_SYMBOL_ENTRY);
        entry->symid = id;
        kno_init_string(&(entry->sym_pname),len,pname);
        kno_symbol_names[id]=LISP_CONS(&(entry->sym_pname));
	entries[probe]=entry;
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

