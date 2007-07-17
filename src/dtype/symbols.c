/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"

typedef struct FD_SYMBOL_ENTRY {
  struct FD_STRING name; int serial;} FD_SYMBOL_ENTRY;
typedef struct FD_SYMBOL_ENTRY *fd_symbol_entry;
struct FD_SYMBOL_TABLE {
  int size; struct FD_SYMBOL_ENTRY **entries;} fd_symbol_table;
fdtype *fd_symbol_names;
int fd_n_symbols=0, fd_max_symbols=0, fd_initial_symbols=1024;

#if FD_THREADS_ENABLED
u8_mutex fd_symbol_lock;
#endif

static int n_refs=0, n_probes=0;

#define MYSTERIOUS_MULTIPLIER 2654435769U
#define MYSTERIOUS_MODULUS 256001281

FD_FASTOP unsigned int mult_hash_string(unsigned char *start,int len)
{
  unsigned int h=0;
  unsigned *istart=(unsigned int *)start, asint;
  const unsigned int *scan=istart, *limit=istart+len/4;
  unsigned char *tail=start+(len/4)*4;
  while (scan<limit) h=((127*h)+*scan++)%MYSTERIOUS_MODULUS;
  switch (len%4) {
  case 0: asint=1; break;
  case 1: asint=tail[0]; break;
  case 2: asint=tail[0]|(tail[1]<<8); break;
  case 3: asint=tail[0]|(tail[1]<<8)|(tail[2]<<16); break;}
  h=((127*h)+asint)%MYSTERIOUS_MODULUS;
  return h;
}

static void init_symbol_tables()
{
  fd_lock_mutex(&fd_symbol_lock);
  if (fd_max_symbols) {
    fd_unlock_mutex(&fd_symbol_lock); return;}
  else {
    int new_max=((fd_max_symbols) ? (fd_max_symbols) : (fd_initial_symbols));
    int new_size=fd_get_hashtable_size(new_max*2);
    struct FD_SYMBOL_ENTRY **new_entries=u8_alloc_n(new_size,fd_symbol_entry);
    fdtype *new_symbol_names=u8_alloc_n(new_max,fdtype);
    int i=0, lim=new_size; while (i < lim) new_entries[i++]=NULL;
    i=0; lim=new_max; while (i < lim) new_symbol_names[i++]=FD_VOID;
    fd_symbol_table.size=new_size; fd_symbol_table.entries=new_entries;
    fd_symbol_names=new_symbol_names; fd_max_symbols=new_max;
    fd_unlock_mutex(&fd_symbol_lock);
  }
}

static void grow_symbol_tables()
{
  int new_max=fd_max_symbols*2;
  int new_size=fd_get_hashtable_size(new_max*2);
  struct FD_SYMBOL_ENTRY **old_entries=fd_symbol_table.entries;
  struct FD_SYMBOL_ENTRY **new_entries=u8_alloc_n(new_size,fd_symbol_entry);
  fdtype *new_symbol_names=u8_alloc_n(new_max,fdtype);
  {
    int i=0, lim=fd_symbol_table.size;
    while (i < new_size) new_entries[i++]=NULL;
    i=0; while (i < lim)
      if (old_entries[i] == NULL) i++;
      else {
	struct FD_SYMBOL_ENTRY *entry=old_entries[i];
	int probe=
	  mult_hash_string(entry->name.bytes,entry->name.length)%new_size;
	while (FD_EXPECT_TRUE(new_entries[probe]!=NULL))
	  if (probe >= new_size) probe=0; else probe=(probe+1)%new_size;
	new_entries[probe]=entry;
	i++;}
    u8_free(old_entries);
    fd_symbol_table.entries=new_entries; fd_symbol_table.size=new_size;}
  {
    int i=0, lim=fd_n_symbols; fdtype *old_symbol_names;
    while (i < lim) {new_symbol_names[i]=fd_symbol_names[i]; i++;}
    old_symbol_names=fd_symbol_names; fd_symbol_names=new_symbol_names;
    u8_free(old_symbol_names);}
  fd_max_symbols=new_max;
}

fdtype fd_make_symbol(u8_string bytes,int len)
{
  struct FD_SYMBOL_ENTRY **entries;
  int hash, probe, next_probe, size;
  fd_lock_mutex(&fd_symbol_lock);
  if (fd_max_symbols == 0) {
    fd_unlock_mutex(&fd_symbol_lock);
    init_symbol_tables();
    fd_lock_mutex(&fd_symbol_lock);}
  entries=fd_symbol_table.entries; size=fd_symbol_table.size;
  if (len<0) len=strlen(bytes);
  hash=mult_hash_string(bytes,len);
  probe=hash%size;
  while (FD_EXPECT_TRUE(entries[probe]!=NULL)) {
    if (FD_EXPECT_TRUE(len == (entries[probe])->name.length))
      if (FD_EXPECT_TRUE(strncmp(bytes,(entries[probe])->name.bytes,len) == 0))
	break;
    probe++; if (probe>=size) probe=0;}
  if (entries[probe]) {
    int id=entries[probe]->serial;
    fd_unlock_mutex(&fd_symbol_lock);
    return FD_ID2SYMBOL(id);}
  else {
    if (fd_n_symbols >= fd_max_symbols) {
      grow_symbol_tables();
      fd_unlock_mutex(&fd_symbol_lock);
      return fd_make_symbol(bytes,len);}
    else {
      int id=fd_n_symbols++; fdtype symbol;
      entries[probe]=u8_alloc(struct FD_SYMBOL_ENTRY);
      symbol=entries[probe]->serial=id;
      fd_init_string(&(entries[probe]->name),len,u8_strdup(bytes));
      fd_symbol_names[id]=FDTYPE_CONS(&(entries[probe]->name));
      fd_unlock_mutex(&fd_symbol_lock);
      return FD_ID2SYMBOL(id);}}
}

fdtype fd_probe_symbol(u8_string bytes,int len)
{
  struct FD_SYMBOL_ENTRY **entries=fd_symbol_table.entries;
  int probe, size=fd_symbol_table.size;
  if (fd_max_symbols == 0) return FD_VOID;
  fd_lock_mutex(&fd_symbol_lock);
  if (len < 0) len=strlen(bytes);
  probe=mult_hash_string(bytes,len)%size;
  while (entries[probe]) {
    if (len == entries[probe]->name.length)
      if (strncmp(bytes,entries[probe]->name.bytes,len) == 0) break;
    if (probe >= size) probe=0;
    else probe++;}
  if (entries[probe]) {
    fd_unlock_mutex(&fd_symbol_lock);
    return FD_ID2SYMBOL(entries[probe]->serial);}
  else {
    fd_unlock_mutex(&fd_symbol_lock);
    return FD_VOID;}
}

FD_EXPORT fdtype fd_intern(u8_string string)
{
  return fd_make_symbol(string,strlen(string));
}

FD_EXPORT fdtype fd_all_symbols()
{
  fdtype results=FD_EMPTY_CHOICE;
  int i=0, n=fd_n_symbols;
  while (i < n) {
    fdtype sym=FD_ID2SYMBOL(i);
    FD_ADD_TO_CHOICE(results,sym);
    i++;}
  return results;
}

/* Initialization */

static int check_symbol(fdtype x)
{
  int id=FD_SYMBOL2ID(x);
  return (id<fd_n_symbols);
}

void fd_init_symbols_c()
{
  fd_immediate_checkfns[fd_symbol_type-0x82]=check_symbol;
  fd_register_source_file(versionid);
}

#if 0
FD_EXPORT 
void fd_list_symbol_table()
{
  int i=0, size=fd_symbol_table.size;
  struct FD_SYMBOL_ENTRY **entries=fd_symbol_table.entries;
  fprintf(stderr,"%d symbols, %d max symbols\n",
	  fd_n_symbols,fd_max_symbols);
  fprintf(stderr,"%d refs, %d probes\n",n_refs,n_probes);
  while (i < size) 
    if (entries[i]) {
      u8_string string=entries[i]->name.bytes;
      int len=entries[i]->name.length, chain=0;
      int hash=mult_hash_string(string,len);
      int probe=hash%size;
      fprintf(stderr,"%s\t%d\t%d\t%d",
	      string,len,hash,probe);
      while (entries[probe]) {
	if (strcmp(entries[probe]->name.bytes,string)==0) break;
	else fprintf(stderr,"\t%s",entries[probe]->name.bytes);
	chain++;
	if (probe == (size-1)) probe=0; else probe++;}
      fprintf(stderr,"\t%d\n",chain);
      i++;}
    else i++;
}
#endif


/* The CVS log for this file
   $Log: symbols.c,v $
   Revision 1.20  2006/01/31 13:47:24  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.19  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.18  2006/01/09 01:25:07  haase
   Added const decls

   Revision 1.17  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.16  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.15  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.14  2005/04/02 16:06:46  haase
   Made module declarations effective current environments by doing a rplacd for the default environments

   Revision 1.13  2005/04/01 15:11:11  haase
   Adapted the string hash algorithm and added some branch expectations

   Revision 1.12  2005/03/29 01:51:24  haase
   Added U8_MUTEX_DECL and used it

   Revision 1.11  2005/03/18 21:15:18  haase
   Configuration fixes around constructor attributes and __thread declarations

   Revision 1.10  2005/02/22 21:22:16  haase
   Added better FD_CHECK_PTR function

   Revision 1.9  2005/02/19 16:25:44  haase
   Added fd_all_symbols

   Revision 1.8  2005/02/14 02:07:47  haase
   Fixed bug in fd_probe_symbol

   Revision 1.7  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/

#if 0
static unsigned int hash_mult(unsigned int x,unsigned int y)
{
  if (x == 1) return y;
  else if (y == 1) return x;
  if ((x == 0) || (y == 0)) return 0;
#if (SIZEOF_LONG_LONG == 8)
  else {
    unsigned long long result=x*y;
    return (result%(MYSTERIOUS_MODULUS));}
#else
  else {
    unsigned int a=(x>>16), b=(x&0xFFFF); 
    unsigned int c=(y>>16), d=(y&0xFFFF); 
    unsigned int bd=b*d, ad=a*d, bc=b*c, ac=a*c;
    unsigned int hi=ac, lo=(bd&0xFFFF), tmp, carry, i;
    tmp=(bd>>16)+(ad&0xFFFF)+(bc&0xFFFF);
    lo=lo+((tmp&0xFFFF)<<16); carry=(tmp>>16);
    hi=hi+carry+(ad>>16)+(bc>>16);
    i=0; while (i++ < 4) {
      hi=((hi<<8)|(lo>>24))%(MYSTERIOUS_MODULUS); lo=lo<<8;}
    return hi;
  }
#endif
}

static unsigned int hash_combine(unsigned int x,unsigned int y)
{
  if ((x == 0) && (y == 0)) return MYSTERIOUS_MODULUS+2;
  else if ((x == 0) || (y == 0))
    return x+y;
  else return hash_mult(x,y);
}

FD_FASTOP unsigned int mult_hash_string(unsigned char *start,int len)
{
  unsigned int prod=1, asint;
  unsigned char *ptr=start, *limit=ptr+len;
  /* Compute a starting place */
  while (ptr < limit) prod=prod+*ptr++;
  /* Now do a multiplication */
  ptr=start; limit=ptr+((len%4) ? (4*(len/4)) : (len));
  while (ptr < limit) {
    asint=(ptr[0]<<24)|(ptr[1]<<16)|(ptr[2]<<8)|(ptr[3]);
    prod=hash_combine(prod,asint); ptr=ptr+4;}
  switch (len%4) {
  case 0: asint=1; break;
  case 1: asint=ptr[0]; break;
  case 2: asint=ptr[0]|(ptr[1]<<8); break;
  case 3: asint=ptr[0]|(ptr[1]<<8)|(ptr[2]<<16); break;}
  return hash_combine(prod,asint);
}
#endif
