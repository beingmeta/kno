/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"

#include "libu8/u8memlist.h"

lispval *fd_symbol_names;
int fd_n_symbols = 0, fd_max_symbols = 0, fd_initial_symbols = 4096;
struct FD_SYMBOL_TABLE fd_symbol_table;

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
lispval FDSYM_PLUS, FDSYM_PREFIX, FDSYM_PROPS;
lispval FDSYM_QMARK, FDSYM_QUOTE, FDSYM_READONLY, FDSYM_SEP;
lispval FDSYM_SET, FDSYM_SIZE, FDSYM_SORT, FDSYM_SORTED;
lispval FDSYM_SOURCE, FDSYM_STAR, FDSYM_STORE, FDSYM_STRING, FDSYM_SUFFIX;
lispval FDSYM_TAG, FDSYM_TEST, FDSYM_TEXT, FDSYM_TYPE;
lispval FDSYM_VERSION, FDSYM_VOID;

u8_rwlock fd_symbol_lock;
static u8_memlist old_symbol_data = NULL;

#define MYSTERIOUS_MULTIPLIER 2654435769U
#define MYSTERIOUS_MODULUS 256001281

FD_FASTOP unsigned int mult_hash_bytes(const unsigned char *start,int len)
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
  u8_write_lock(&fd_symbol_lock);
  if (fd_max_symbols) {
    u8_rw_unlock(&fd_symbol_lock); return;}
  else {
    int new_max = ((fd_max_symbols) ? (fd_max_symbols) : (fd_initial_symbols));
    int new_size = fd_get_hashtable_size(new_max*2);
    struct FD_SYMBOL_ENTRY **new_entries = u8_alloc_n(new_size,fd_symbol_entry);
    lispval *new_symbol_names = u8_alloc_n(new_max,lispval);
    int i = 0, lim = new_size; while (i < lim) new_entries[i++]=NULL;
    i = 0; lim = new_max; while (i < lim) new_symbol_names[i++]=VOID;
    fd_symbol_table.table_size = new_size;
    fd_symbol_table.fd_symbol_entries = new_entries;
    fd_symbol_names = new_symbol_names; fd_max_symbols = new_max;
    u8_rw_unlock(&fd_symbol_lock);
    init_builtin_symbols();
  }
}

static void init_builtin_symbols()
{
  FDSYM_ADD = fd_intern("ADD");
  FDSYM_ADJUNCT = fd_intern("ADJUNCT");
  FDSYM_ALL = fd_intern("ALL");
  FDSYM_ALWAYS = fd_intern("ALWAYS");
  FDSYM_BLOCKSIZE = fd_intern("BLOCKSIZE");
  FDSYM_BUFSIZE = fd_intern("BUFSIZE");
  FDSYM_CACHELEVEL = fd_intern("CACHELEVEL");
  FDSYM_CACHESIZE = fd_intern("CACHESIZE");
  FDSYM_CONS = fd_intern("CONS");
  FDSYM_CONTENT = fd_intern("CONTENT");
  FDSYM_CREATE = fd_intern("CREATE");
  FDSYM_CREATE = fd_intern("FILE");
  FDSYM_DEFAULT = fd_intern("DEFAULT");
  FDSYM_DOT = fd_intern(".");
  FDSYM_DROP = fd_intern("DROP");
  FDSYM_ENCODING = fd_intern("ENCODING");
  FDSYM_EQUALS = fd_intern("=");
  FDSYM_ERROR = fd_intern("ERROR");
  FDSYM_FILE = fd_intern("FILE");
  FDSYM_FILENAME = fd_intern("FILENAME");
  FDSYM_FLAGS = fd_intern("FLAGS");
  FDSYM_FORMAT = fd_intern("FORMAT");
  FDSYM_FRONT = fd_intern("FRONT");
  FDSYM_INPUT = fd_intern("INPUT");
  FDSYM_ISADJUNCT = fd_intern("ISADJUNCT");
  FDSYM_KEYSLOT = fd_intern("KEYSLOT");
  FDSYM_LABEL = fd_intern("LABEL");
  FDSYM_LAZY = fd_intern("LAZY");
  FDSYM_LENGTH = fd_intern("LENGTH");
  FDSYM_LOGLEVEL = fd_intern("LOGLEVEL");
  FDSYM_MAIN = fd_intern("MAIN");
  FDSYM_MERGE = fd_intern("MERGE");
  FDSYM_METADATA = fd_intern("METADATA");
  FDSYM_MINUS = fd_intern("-");
  FDSYM_MODULE = fd_intern("MODULE");
  FDSYM_NAME = fd_intern("NAME");
  FDSYM_NO = fd_intern("NO");
  FDSYM_NONE = fd_intern("NONE");
  FDSYM_NOT = fd_intern("NOT");
  FDSYM_OPT = fd_intern("OPT");
  FDSYM_OPTS = fd_intern("OPTS");
  FDSYM_OUTPUT = fd_intern("OUTPUT");
  FDSYM_PLUS = fd_intern("+");
  FDSYM_PREFIX = fd_intern("PREFIX");
  FDSYM_PROPS = fd_intern("PROPS");
  FDSYM_QMARK = fd_intern("?");
  FDSYM_QUOTE = fd_intern("QUOTE");
  FDSYM_READONLY = fd_intern("READONLY");
  FDSYM_SEP = fd_intern("SEP");
  FDSYM_SET = fd_intern("SET");
  FDSYM_SIZE = fd_intern("SIZE");
  FDSYM_SORT = fd_intern("SORT");
  FDSYM_SORTED = fd_intern("SORTED");
  FDSYM_SOURCE = fd_intern("SOURCE");
  FDSYM_STAR = fd_intern("*");
  FDSYM_STORE = fd_intern("STORE");
  FDSYM_STRING = fd_intern("STRING");
  FDSYM_SUFFIX = fd_intern("SUFFIX");
  FDSYM_TAG = fd_intern("TAG");
  FDSYM_TEST = fd_intern("TEST");
  FDSYM_TEXT = fd_intern("TEXT");
  FDSYM_TYPE = fd_intern("TYPE");
  FDSYM_VERSION = fd_intern("VERSION");
  FDSYM_VOID = fd_intern("VOID");
}

static void grow_symbol_tables()
{
  int new_max = fd_max_symbols*2;
  int new_size = fd_get_hashtable_size(new_max*2);
  struct FD_SYMBOL_ENTRY **old_entries = fd_symbol_table.fd_symbol_entries;
  struct FD_SYMBOL_ENTRY **new_entries = u8_alloc_n(new_size,fd_symbol_entry);
  lispval *new_symbol_names = u8_alloc_n(new_max,lispval);
  {
    int i = 0, lim = fd_symbol_table.table_size;
    while (i < new_size) new_entries[i++]=NULL;
    i = 0; while (i < lim)
      if (old_entries[i] == NULL) i++;
      else {
        struct FD_SYMBOL_ENTRY *entry = old_entries[i];
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
    fd_symbol_table.fd_symbol_entries = new_entries;
    fd_symbol_table.table_size = new_size;}
  {
    int i = 0, lim = fd_n_symbols; lispval *old_symbol_names;
    while (i < lim) {new_symbol_names[i]=fd_symbol_names[i]; i++;}
    old_symbol_names = fd_symbol_names;
    fd_symbol_names = new_symbol_names;
    old_symbol_data = u8_cons_list(old_symbol_names,old_symbol_data,0);}
  fd_max_symbols = new_max;
}

lispval probe_symbol(u8_string bytes,int len)
{
  struct FD_SYMBOL_ENTRY **entries = fd_symbol_table.fd_symbol_entries;
  int probe, size = fd_symbol_table.table_size;
  if ((fd_n_symbols == 0)||(fd_max_symbols == 0)) return VOID;
  probe = mult_hash_bytes(bytes,len)%size;
  while (entries[probe]) {
    if (len == entries[probe]->sym_pname.str_bytelen)
      if (strncmp(bytes,entries[probe]->sym_pname.str_bytes,len) == 0)
        break;
    probe++;
    if (probe >= size) probe = 0;}
  if (entries[probe]) {
    return FD_ID2SYMBOL(entries[probe]->symid);}
  else return VOID;
}

lispval fd_make_symbol(u8_string bytes,int len)
{
  if (fd_max_symbols == 0) init_symbol_tables();
  if (len<0) len=strlen(bytes);
  lispval sym=probe_symbol(bytes,len);
  if (SYMBOLP(sym))
    return sym;
  else {
    u8_write_lock(&fd_symbol_lock);
    struct FD_SYMBOL_ENTRY **entries = fd_symbol_table.fd_symbol_entries;
    int size = fd_symbol_table.table_size;
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
      u8_rw_unlock(&fd_symbol_lock);
      return FD_ID2SYMBOL(id);}
    else {
      if (fd_n_symbols >= fd_max_symbols) {
        grow_symbol_tables();
        u8_rw_unlock(&fd_symbol_lock);
        return fd_make_symbol(bytes,len);}
      else {
        int id = fd_n_symbols++;
        u8_string pname = u8_strdup(bytes);
        entries[probe]=u8_alloc(struct FD_SYMBOL_ENTRY);
        entries[probe]->symid = id;
        fd_init_string(&(entries[probe]->sym_pname),len,pname);
        fd_symbol_names[id]=LISP_CONS(&(entries[probe]->sym_pname));
        u8_rw_unlock(&fd_symbol_lock);
        return FD_ID2SYMBOL(id);}}}
}

lispval fd_probe_symbol(u8_string bytes,int len)
{
  if (len<0) len=strlen(bytes);
  u8_read_lock(&fd_symbol_lock);
  lispval sym = probe_symbol(bytes,len);
  u8_rw_unlock(&fd_symbol_lock);
  return sym;
}

FD_EXPORT lispval fd_intern(u8_string string)
{
  return fd_make_symbol(string,strlen(string));
}

FD_EXPORT lispval fd_symbolize(u8_string string)
{
  const u8_byte *scan = string;
  U8_STATIC_OUTPUT(name,64);
  int c = u8_sgetc(&scan);
  while (c>=0) {
    u8_putc(nameout,u8_toupper(c));
    c = u8_sgetc(&scan);}
  lispval result = fd_make_symbol(name.u8_outbuf,name.u8_write-name.u8_outbuf);
  u8_close((u8_stream)nameout);
  return result;
}

FD_EXPORT lispval fd_all_symbols()
{
  lispval results = EMPTY;
  int i = 0, n = fd_n_symbols;
  while (i < n) {
    lispval sym = FD_ID2SYMBOL(i);
    CHOICE_ADD(results,sym);
    i++;}
  return results;
}

/* Initialization */

static int check_symbol(lispval x)
{
  int id = FD_SYMBOL2ID(x);
  return (id<fd_n_symbols);
}

void fd_init_symbols_c()
{
  fd_immediate_checkfns[fd_symbol_type]=check_symbol;
  u8_register_source_file(_FILEINFO);
}

#if 0
FD_EXPORT
static int n_refs = 0, n_probes = 0;
void fd_list_symbol_table()
{
  int i = 0, size = fd_symbol_table.size;
  struct FD_SYMBOL_ENTRY **entries = fd_symbol_table.entries;
  fprintf(stderr,"%d symbols, %d max symbols\n",
          fd_n_symbols,fd_max_symbols);
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
