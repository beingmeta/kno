/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"

fdtype *fd_symbol_names;
int fd_n_symbols = 0, fd_max_symbols = 0, fd_initial_symbols = 1024;
struct FD_SYMBOL_TABLE fd_symbol_table;

fdtype FDSYM_TYPE, FDSYM_SIZE, FDSYM_LABEL, FDSYM_NAME,
  FDSYM_BUFSIZE, FDSYM_BLOCKSIZE, FDSYM_CACHESIZE,
  FDSYM_MERGE, FDSYM_SORT, FDSYM_SORTED, FDSYM_LAZY, FDSYM_VERSION,
  FDSYM_QUOTE, FDSYM_PLUS, FDSYM_STAR, FDSYM_OPT,
  FDSYM_DOT, FDSYM_MINUS, FDSYM_EQUALS, FDSYM_QMARK,
  FDSYM_PREFIX, FDSYM_SUFFIX, FDSYM_SEP, FDSYM_TAG,
  FDSYM_TEXT, FDSYM_CONTENT, FDSYM_LENGTH,
  FDSYM_STRING, FDSYM_CONS;

u8_mutex fd_symbol_lock;

#define MYSTERIOUS_MULTIPLIER 2654435769U
#define MYSTERIOUS_MODULUS 256001281

FD_FASTOP unsigned int mult_hash_string(const unsigned char *start,int len)
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
  u8_lock_mutex(&fd_symbol_lock);
  if (fd_max_symbols) {
    u8_unlock_mutex(&fd_symbol_lock); return;}
  else {
    int new_max = ((fd_max_symbols) ? (fd_max_symbols) : (fd_initial_symbols));
    int new_size = fd_get_hashtable_size(new_max*2);
    struct FD_SYMBOL_ENTRY **new_entries = u8_zalloc_n(new_size,fd_symbol_entry);
    fdtype *new_symbol_names = u8_alloc_n(new_max,fdtype);
    int i = 0, lim = new_size; while (i < lim) new_entries[i++]=NULL;
    i = 0; lim = new_max; while (i < lim) new_symbol_names[i++]=FD_VOID;
    fd_symbol_table.table_size = new_size; 
    fd_symbol_table.fd_symbol_entries = new_entries;
    fd_symbol_names = new_symbol_names; fd_max_symbols = new_max;
    u8_unlock_mutex(&fd_symbol_lock);
    init_builtin_symbols();
  }
}

static void init_builtin_symbols()
{
  FDSYM_TYPE = fd_intern("TYPE");
  FDSYM_SIZE = fd_intern("SIZE");
  FDSYM_LABEL = fd_intern("LABEL");
  FDSYM_NAME = fd_intern("NAME");
  FDSYM_BUFSIZE = fd_intern("BUFSIZE");
  FDSYM_BLOCKSIZE = fd_intern("BLOCKSIZE");
  FDSYM_CACHESIZE = fd_intern("CACHESIZE");
  FDSYM_MERGE = fd_intern("MERGE");
  FDSYM_SORTED = fd_intern("SORTED");
  FDSYM_SORT = fd_intern("SORT");
  FDSYM_LAZY = fd_intern("LAZY");
  FDSYM_VERSION = fd_intern("VERSION");
  FDSYM_QUOTE = fd_intern("QUOTE");
  FDSYM_OPT = fd_intern("OPT");
  FDSYM_TEXT = fd_intern("TEXT");
  FDSYM_CONTENT = fd_intern("CONTENT");
  FDSYM_LENGTH = fd_intern("LENGTH");
  FDSYM_STAR = fd_intern("*");
  FDSYM_PLUS = fd_intern("+");
  FDSYM_MINUS = fd_intern("-");
  FDSYM_EQUALS = fd_intern("=");
  FDSYM_DOT = fd_intern(".");
  FDSYM_QMARK = fd_intern("?");
  FDSYM_PREFIX = fd_intern("PREFIX");
  FDSYM_SUFFIX = fd_intern("SUFFIX");
  FDSYM_SEP = fd_intern("SEP");
  FDSYM_TAG = fd_intern("TAG");
  FDSYM_CONS = fd_intern("CONS");
  FDSYM_STRING = fd_intern("STRING");
}

static void grow_symbol_tables()
{
  int new_max = fd_max_symbols*2;
  int new_size = fd_get_hashtable_size(new_max*2);
  struct FD_SYMBOL_ENTRY **old_entries = fd_symbol_table.fd_symbol_entries;
  struct FD_SYMBOL_ENTRY **new_entries = u8_alloc_n(new_size,fd_symbol_entry);
  fdtype *new_symbol_names = u8_zalloc_n(new_max,fdtype);
  {
    int i = 0, lim = fd_symbol_table.table_size;
    while (i < new_size) new_entries[i++]=NULL;
    i = 0; while (i < lim)
      if (old_entries[i] == NULL) i++;
      else {
        struct FD_SYMBOL_ENTRY *entry = old_entries[i];
        int probe=
          mult_hash_string(entry->fd_pname.fd_bytes,
                           entry->fd_pname.fd_bytelen)%new_size;
        while (FD_EXPECT_TRUE(new_entries[probe]!=NULL))
          if (probe >= new_size) probe = 0; else probe = (probe+1)%new_size;
        new_entries[probe]=entry;
        i++;}
    u8_free(old_entries);
    fd_symbol_table.fd_symbol_entries = new_entries; 
    fd_symbol_table.table_size = new_size;}
  {
    int i = 0, lim = fd_n_symbols; fdtype *old_symbol_names;
    while (i < lim) {new_symbol_names[i]=fd_symbol_names[i]; i++;}
    old_symbol_names = fd_symbol_names; fd_symbol_names = new_symbol_names;
    u8_free(old_symbol_names);}
  fd_max_symbols = new_max;
}

fdtype fd_make_symbol(u8_string bytes,int len)
{
  struct FD_SYMBOL_ENTRY **entries;
  int hash, probe, size;
  u8_lock_mutex(&fd_symbol_lock);
  if (fd_max_symbols == 0) {
    u8_unlock_mutex(&fd_symbol_lock);
    init_symbol_tables();
    u8_lock_mutex(&fd_symbol_lock);}
  entries = fd_symbol_table.fd_symbol_entries; 
  size = fd_symbol_table.table_size;
  if (len<0) len = strlen(bytes);
  hash = mult_hash_string(bytes,len);
  probe = hash%size;
  while (FD_EXPECT_TRUE(entries[probe]!=NULL)) {
    if (FD_EXPECT_TRUE(len == (entries[probe])->fd_pname.fd_bytelen))
      if (FD_EXPECT_TRUE(strncmp(bytes,(entries[probe])->fd_pname.fd_bytes,len) == 0))
        break;
    probe++; if (probe>=size) probe = 0;}
  if (entries[probe]) {
    int id = entries[probe]->fd_symid;
    u8_unlock_mutex(&fd_symbol_lock);
    return FD_ID2SYMBOL(id);}
  else {
    if (fd_n_symbols >= fd_max_symbols) {
      grow_symbol_tables();
      u8_unlock_mutex(&fd_symbol_lock);
      return fd_make_symbol(bytes,len);}
    else {
      int id = fd_n_symbols++;
      u8_string pname = u8_strdup(bytes);
      entries[probe]=u8_alloc(struct FD_SYMBOL_ENTRY);
      entries[probe]->fd_symid = id;
      fd_init_string(&(entries[probe]->fd_pname),len,pname);
      fd_symbol_names[id]=FDTYPE_CONS(&(entries[probe]->fd_pname));
      u8_unlock_mutex(&fd_symbol_lock);
      return FD_ID2SYMBOL(id);}}
}

fdtype fd_probe_symbol(u8_string bytes,int len)
{
  struct FD_SYMBOL_ENTRY **entries = fd_symbol_table.fd_symbol_entries;
  int probe, size = fd_symbol_table.table_size;
  if (fd_max_symbols == 0) return FD_VOID;
  u8_lock_mutex(&fd_symbol_lock);
  if (len < 0) len = strlen(bytes);
  probe = mult_hash_string(bytes,len)%size;
  while (entries[probe]) {
    if (len == entries[probe]->fd_pname.fd_bytelen)
      if (strncmp(bytes,entries[probe]->fd_pname.fd_bytes,len) == 0) break;
    if (probe >= size) probe = 0;
    else probe++;}
  if (entries[probe]) {
    u8_unlock_mutex(&fd_symbol_lock);
    return FD_ID2SYMBOL(entries[probe]->fd_symid);}
  else {
    u8_unlock_mutex(&fd_symbol_lock);
    return FD_VOID;}
}

FD_EXPORT fdtype fd_intern(u8_string string)
{
  return fd_make_symbol(string,strlen(string));
}

FD_EXPORT fdtype fd_symbolize(u8_string string)
{
  fdtype result;
  struct U8_OUTPUT out; unsigned char buf[64];
  const u8_byte *scan = string;
  int c = u8_sgetc(&scan);
  U8_INIT_OUTPUT_BUF(&out,64,buf);
  while (c>=0) {
    u8_putc(&out,u8_toupper(c));
    c = u8_sgetc(&scan);}
  result = fd_make_symbol(out.u8_outbuf,out.u8_write-out.u8_outbuf);
  u8_close((u8_stream)&out);
  return result;
}

FD_EXPORT fdtype fd_all_symbols()
{
  fdtype results = FD_EMPTY_CHOICE;
  int i = 0, n = fd_n_symbols;
  while (i < n) {
    fdtype sym = FD_ID2SYMBOL(i);
    FD_ADD_TO_CHOICE(results,sym);
    i++;}
  return results;
}

/* Initialization */

static int check_symbol(fdtype x)
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
      u8_string string = entries[i]->name.fd_bytes;
      int len = entries[i]->name.fd_bytelen, chain = 0;
      int hash = mult_hash_string(string,len);
      int probe = hash%size;
      fprintf(stderr,"%s\t%d\t%d\t%d",
              string,len,hash,probe);
      while (entries[probe]) {
        if (strcmp(entries[probe]->name.fd_bytes,string)==0) break;
        else fprintf(stderr,"\t%s",entries[probe]->name.fd_bytes);
        chain++;
        if (probe == (size-1)) probe = 0; else probe++;}
      fprintf(stderr,"\t%d\n",chain);
      i++;}
    else i++;
}
#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/