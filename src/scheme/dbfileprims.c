/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2015 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/fddb.h"
#include "framerd/eval.h"
#include "framerd/dbfile.h"
#include "framerd/sequences.h"

static fdtype baseoids_symbol;

static fdtype make_file_pool
  (fdtype fname,fdtype base,fdtype capacity,fdtype opt1,fdtype opt2)
{
  fdtype metadata; unsigned int load;
  int retval;
  if (FD_FIXNUMP(opt1)) {
    load=FD_FIX2INT(opt1); metadata=opt2;}
  else {load=0; metadata=opt1;}
  if (FD_VOIDP(metadata)) {}
  else if (!(FD_SLOTMAPP(metadata)))
    return fd_type_error(_("slotmap"),"make_file_pool",metadata);
  retval=fd_make_file_pool(FD_STRDATA(fname),FD_FILE_POOL_MAGIC_NUMBER,
                           FD_OID_ADDR(base),fd_getint(capacity),
                           load,metadata);
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_TRUE;
}

static fdtype make_zpool
  (fdtype fname,fdtype base,fdtype capacity,fdtype opt1,fdtype opt2)
{
  fdtype metadata; unsigned int load;
  int retval;
  if (FD_FIXNUMP(opt1)) {
    load=FD_FIX2INT(opt1); metadata=opt2;}
  else {load=0; metadata=opt1;}
  if (FD_VOIDP(metadata)) {}
  else if (!(FD_SLOTMAPP(metadata)))
    return fd_type_error(_("slotmap"),"make_file_pool",metadata);
  retval=
    fd_make_file_pool(FD_STRDATA(fname),FD_ZPOOL_MAGIC_NUMBER,
                      FD_OID_ADDR(base),fd_getint(capacity),
                      load,metadata);
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_TRUE;
}

static fdtype label_file_pool(fdtype fname,fdtype label)
{
  int retval=-1;
  fd_dtype_stream stream
    =fd_dtsopen(FD_STRING_DATA(fname),FD_DTSTREAM_MODIFY);
  if (stream) {
    fd_off_t endpos=fd_endpos(stream);
    if (endpos>0) {
      int bytes=fd_dtswrite_dtype(stream,label);
      if (bytes>0) {
        fd_setpos(stream,20);
        if (fd_write_4bytes(((fd_byte_output)stream),(unsigned int)endpos)>=0) {
          retval=1; fd_dtsclose(stream,1);}}}}
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_TRUE;
}

static fdtype make_oidpool(int n,fdtype *args)
{
  FD_OID base; int retval, flags=0, load, cap; u8_string filename, label;
  fdtype fname=args[0], base_arg=args[1], capacity=args[2];
  fdtype label_arg=FD_VOID, flags_arg=FD_VOID, schemas=FD_VOID;
  fdtype metadata=FD_VOID, load_arg=FD_INT(0);
  if (n>3) load_arg=args[3];
  if (n>4) flags_arg=args[4];
  if (n>5) schemas=args[5];
  if (n>6) metadata=args[6];
  if (n>7) label_arg=args[7];

  if (!(FD_OIDP(base_arg)))
    return fd_type_error(_("OID"),"make_oidpool",base_arg);
  else base=FD_OID_ADDR(base_arg);

  if (!(FD_STRINGP(fname)))
    return fd_type_error(_("fixnum"),"make_oidpool",capacity);
  else filename=FD_STRDATA(fname);

  if (FD_STRINGP(label_arg)) label=FD_STRDATA(label_arg);
  else if (FD_FALSEP(label_arg)) label=NULL;
  else if (FD_VOIDP(label_arg)) label=NULL;
  else return fd_type_error(_("string"),"make_oidpool",capacity);

  if (!(FD_FIXNUMP(capacity)))
    return fd_type_error(_("fixnum"),"make_oidpool",capacity);
  else cap=FD_FIX2INT(capacity);

  if (!(FD_FIXNUMP(load_arg)))
    return fd_type_error(_("fixnum"),"make_oidpool",load_arg);
  else load=FD_FIX2INT(load_arg);

  if (FD_FALSEP(metadata)) metadata=FD_VOID;

  /* Check that pool alignment is legal */

  {
    FD_OID end=FD_OID_PLUS(base,cap-1);
    unsigned int base_lo=FD_OID_LO(base);
    unsigned int end_lo=FD_OID_LO(end);
    if (((base_lo)/(1024*1024)) == ((end_lo)/(1024*1024))) {}
    else if (((base_lo%(1024*1024))==0) && ((cap%(1024*1024))==0)) {}
    else return fd_err(_("Misaligned pool"),"make_oidpool",NULL,FD_VOID);
  }

  if (FD_VOIDP(schemas)) {}
  else if (FD_FALSEP(schemas))  schemas=FD_VOID;
  else if (FD_VECTORP(schemas)) {}
  else return fd_type_error(_("vector"),"make_oidpool",schemas);

  if (FD_SEQUENCEP(flags_arg)) {
    if (fd_position(fd_intern("B64"),flags_arg,0,-1)>=0)
      flags=flags|FD_B64;
    else if (fd_position(fd_intern("B32"),flags_arg,0,-1)>=0) {}
    else if (fd_position(fd_intern("B40"),flags_arg,0,-1)>=0)
      flags=flags|FD_B40;
    else flags=flags|FD_B40;

    if (fd_position(fd_intern("NOCOMPRESS"),flags_arg,0,-1)>=0) {}
    else if (fd_position(fd_intern("ZLIB"),flags_arg,0,-1)>=0)
      flags=flags|((FD_ZLIB)<<3);
    else if (fd_position(fd_intern("BZ2"),flags_arg,0,-1)>=0)
      flags=flags|((FD_BZ2)<<3);

    if (fd_position(fd_intern("DTYPEV2"),flags_arg,0,-1)>=0)
      flags=flags|FD_OIDPOOL_DTYPEV2;

    if (fd_position(fd_intern("READONLY"),flags_arg,0,-1)>=0)
      flags=flags|FD_OIDPOOL_READONLY;}

  retval=fd_make_oidpool(filename,label,
                         base,cap,load,flags,
                         schemas,metadata,
                         time(NULL),time(NULL),1);

  if (retval<0) return FD_ERROR_VALUE;
  else return FD_VOID;
}

static fdtype make_file_index(fdtype fname,fdtype size,fdtype metadata)
{
  int retval=
    fd_make_file_index(FD_STRDATA(fname),FD_MULT_FILE_INDEX_MAGIC_NUMBER,
                       fd_getint(size),metadata);
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_TRUE;
}

static fdtype make_legacy_file_index(fdtype fname,fdtype size,fdtype metadata)
{
  int retval=
    fd_make_file_index(FD_STRDATA(fname),FD_FILE_INDEX_MAGIC_NUMBER,
                       fd_getint(size),metadata);
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_TRUE;
}

static fdtype make_zindex(fdtype fname,fdtype size,fdtype metadata)
{
  int retval=
    fd_make_file_index(FD_STRDATA(fname),FD_ZINDEX_MAGIC_NUMBER,
                       fd_getint(size),metadata);
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_TRUE;
}

static int get_make_hash_index_flags(fdtype flags_arg)
{
  if (FD_SEQUENCEP(flags_arg)) {
    int flags=0;
    if (fd_position(fd_intern("B64"),flags_arg,0,-1)>=0)
      flags=flags|(FD_B64<<4);
    else if (fd_position(fd_intern("B32"),flags_arg,0,-1)>=0) {}
    else if (fd_position(fd_intern("B40"),flags_arg,0,-1)>=0)
      flags=flags|(FD_B40<<4);
    else flags=flags|(FD_B40<<4);
    if (fd_position(fd_intern("DTYPEV2"),flags_arg,0,-1)>=0)
      flags=flags|FD_HASH_INDEX_DTYPEV2;
    return flags;}
  else if (FD_EQ(flags_arg,fd_intern("DTYPEV2")))
    return FD_HASH_INDEX_DTYPEV2;
  else if (FD_EQ(flags_arg,fd_intern("B40")))
    return (FD_B40<<4);
  else if (FD_EQ(flags_arg,fd_intern("B64")))
    return (FD_B64<<4);
  else return (FD_B40<<4);
}

static fdtype make_hash_index(fdtype fname,fdtype size,fdtype slotids,fdtype baseoids,fdtype metadata,
                              fdtype flags_arg)
{
  int retval=
    fd_make_hash_index(FD_STRDATA(fname),FD_FIX2INT(size),
                       get_make_hash_index_flags(flags_arg),0,
                       slotids,baseoids,metadata,-1,-1);
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_VOID;
}

static fdtype populate_hash_index
  (fdtype ix_arg,fdtype from,fdtype blocksize_arg,fdtype keys)
{
  fd_index ix=fd_indexptr(ix_arg); int blocksize=-1, retval;
  const fdtype *keyvec; fdtype *consed_keyvec=NULL;
  unsigned int n_keys; fdtype keys_choice=FD_VOID;
  if (!(fd_hash_indexp(ix)))
    return fd_type_error(_("hash index"),"populate_hash_index",ix_arg);
  if (FD_FIXNUMP(blocksize_arg)) blocksize=FD_FIX2INT(blocksize_arg);
  if (FD_CHOICEP(keys)) {
    keyvec=FD_CHOICE_DATA(keys); n_keys=FD_CHOICE_SIZE(keys);}
  else if (FD_VECTORP(keys)) {
    keyvec=FD_VECTOR_DATA(keys); n_keys=FD_VECTOR_LENGTH(keys);}
  else if (FD_VOIDP(keys)) {
    if ((FD_INDEXP(from))||(FD_PRIM_TYPEP(from,fd_raw_index_type))) {
      fd_index ix=fd_indexptr(from);
      if (ix->handler->fetchkeys!=NULL) {
        consed_keyvec=ix->handler->fetchkeys(ix,&n_keys);
        keyvec=consed_keyvec;}
      else keys_choice=fd_getkeys(from);}
    else keys_choice=fd_getkeys(from);
    if (!(FD_VOIDP(keys_choice))) {
      if (FD_CHOICEP(keys_choice)) {
        keyvec=FD_CHOICE_DATA(keys_choice);
        n_keys=FD_CHOICE_SIZE(keys_choice);}
      else {
        keyvec=&keys; n_keys=1;}}
    else {keyvec=&keys; n_keys=0;}}
  else {
    keyvec=&keys; n_keys=1;}
  if (n_keys)
    retval=fd_populate_hash_index
      ((struct FD_HASH_INDEX *)ix,from,keyvec,n_keys,blocksize);
  else retval=0;
  fd_decref(keys_choice);
  if (consed_keyvec) {
    int i=0; while (i<n_keys) {
      fd_decref(keyvec[i]); i++;}
    if (consed_keyvec) u8_free(consed_keyvec);}
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_INT(retval);
}

static fdtype hash_index_bucket(fdtype ix_arg,fdtype key,fdtype modulus)
{
  fd_index ix=fd_indexptr(ix_arg); int bucket;
  if (!(fd_hash_indexp(ix)))
    return fd_type_error(_("hash index"),"hash_index_bucket",ix_arg);
  bucket=fd_hash_index_bucket((struct FD_HASH_INDEX *)ix,key,FD_VOIDP(modulus));
  if (FD_FIXNUMP(modulus))
    return FD_INT((bucket%FD_FIX2INT(modulus)));
  else return FD_INT(bucket);
}

static fdtype hash_index_stats(fdtype ix_arg)
{
  fd_index ix=fd_indexptr(ix_arg);
  if ((ix==NULL) || (!(fd_hash_indexp(ix))))
    return fd_type_error(_("hash index"),"hash_index_stats",ix_arg);
  return fd_hash_index_stats((struct FD_HASH_INDEX *)ix);
}

static fdtype hash_index_slotids(fdtype ix_arg)
{
  fd_index ix=fd_indexptr(ix_arg);
  if (!(fd_hash_indexp(ix)))
    return fd_type_error(_("hash index"),"hash_index_slotids",ix_arg);
  else {
    struct FD_HASH_INDEX *hx=(fd_hash_index)ix;
    fdtype *elts=u8_alloc_n(hx->n_slotids,fdtype), *slotids=hx->slotids;
    int i=0, n=hx->n_slotids;
    while (i< n) {elts[i]=slotids[i]; i++;}
    return fd_init_vector(NULL,n,elts);}
}

/* Cache forcing */

/* This forces the cache into memory for a pool or index.
   It's relevant when the cache is MMAPd. */

static unsigned int load_cache(unsigned int *cache,int length)
{
  unsigned int combo=0;
  int i=0; while (i<length) combo=combo^cache[i++];
  return combo;
}

static int load_pool_cache(fd_pool p,void *ignored)
{
  if ((p->handler==NULL) || (p->handler->name==NULL)) return 0;
  else if (strcmp(p->handler->name,"file_pool")==0) {
    struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
    if (fp->offsets) load_cache(fp->offsets,fp->offsets_size);}
  else if (strcmp(p->handler->name,"oidpool")==0) {
    struct FD_OIDPOOL *fp=(struct FD_OIDPOOL *)p;
    if (fp->offsets) load_cache(fp->offsets,fp->offsets_size);}
  return 0;
}

static int load_index_cache(fd_index ix,void *ignored)
{
  if ((ix->handler==NULL) || (ix->handler->name==NULL)) return 0;
  else if (strcmp(ix->handler->name,"file_index")==0) {
    struct FD_FILE_INDEX *fx=(struct FD_FILE_INDEX *)ix;
    if (fx->offsets) load_cache(fx->offsets,fx->n_slots);}
  else if (strcmp(ix->handler->name,"hash_index")==0) {
    struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
    if (hx->offdata) load_cache(hx->offdata,hx->n_buckets*2);}
  return 0;
}

static fdtype load_caches_prim(fdtype arg)
{
  if (FD_VOIDP(arg)) {
    fd_for_pools(load_pool_cache,NULL);
    fd_for_indices(load_index_cache,NULL);}
  else if (FD_PRIM_TYPEP(arg,fd_index_type))
    load_index_cache(fd_indexptr(arg),NULL);
  else if (FD_PRIM_TYPEP(arg,fd_pool_type))
    load_pool_cache(fd_lisp2pool(arg),NULL);
  else {}
  return FD_VOID;
}

/* Hashing functions */

static fdtype lisphashdtype1(fdtype x)
{
  int hash=fd_hash_dtype1(x);
  return FD_INT(hash);
}
static fdtype lisphashdtype2(fdtype x)
{
  int hash=fd_hash_dtype2(x);
  return FD_INT(hash);
}

static fdtype lisphashdtype3(fdtype x)
{
  int hash=fd_hash_dtype3(x);
  return FD_INT(hash);
}

static fdtype lisphashdtyperep(fdtype x)
{
  unsigned int hash=fd_hash_dtype_rep(x);
  return FD_INT(hash);
}

/* Opening unregistered file pools */

static fdtype open_file_pool(fdtype name)
{
  fd_pool p=fd_unregistered_file_pool(FD_STRDATA(name));
  if (p) return (fdtype) p;
  else return FD_ERROR_VALUE;
}

static fdtype file_pool_prefetch(fdtype pool,fdtype oids)
{
  fd_pool p=(fd_pool)pool;
  int retval=fd_pool_prefetch(p,oids);
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_VOID;
}

/* The init function */

static int scheme_filedb_initialized=0;

FD_EXPORT void fd_init_filedb_c()
{
  fdtype filedb_module;

  baseoids_symbol=fd_intern("%BASEOIDS");

  if (scheme_filedb_initialized) return;
  scheme_filedb_initialized=1;
  fd_init_fdscheme();
  fd_init_dbfile();
  filedb_module=fd_new_module("FILEDB",(FD_MODULE_DEFAULT));
  u8_register_source_file(_FILEINFO);

  fd_idefn(filedb_module,
           fd_make_cprim3x("MAKE-ZINDEX",make_zindex,2,
                           fd_string_type,FD_VOID,
                           fd_fixnum_type,FD_VOID,
                           fd_slotmap_type,FD_VOID));
  fd_idefn(filedb_module,
           fd_make_cprim3x("MAKE-FILE-INDEX",make_file_index,2,
                           fd_string_type,FD_VOID,
                           fd_fixnum_type,FD_VOID,
                           fd_slotmap_type,FD_VOID));
  fd_idefn(filedb_module,
           fd_make_cprim3x("MAKE-LEGACY-FILE-INDEX",make_legacy_file_index,2,
                           fd_string_type,FD_VOID,
                           fd_fixnum_type,FD_VOID,
                           fd_slotmap_type,FD_VOID));
  fd_idefn(filedb_module,
           fd_make_cprim5x("MAKE-FILE-POOL",make_file_pool,3,
                           fd_string_type,FD_VOID,
                           fd_oid_type,FD_VOID,
                           fd_fixnum_type,FD_VOID,
                           -1,FD_VOID,-1,FD_VOID));
  fd_idefn(filedb_module,
           fd_make_cprim5x("MAKE-ZPOOL",make_zpool,3,
                           fd_string_type,FD_VOID,
                           fd_oid_type,FD_VOID,
                           fd_fixnum_type,FD_VOID,
                           -1,FD_VOID,-1,FD_VOID));

  fd_idefn(filedb_module,fd_make_cprim1("LOAD-CACHES",load_caches_prim,0));

  fd_idefn(filedb_module,
           fd_make_cprimn("MAKE-OIDPOOL",make_oidpool,3));

  fd_idefn(filedb_module,fd_make_cprim2x("LABEL-POOL!",label_file_pool,2,
                                         fd_string_type,FD_VOID,
                                         fd_string_type,FD_VOID));

  fd_idefn(filedb_module,fd_make_cprim1x("OPEN-FILE-POOL",open_file_pool,1,
                                         fd_string_type,FD_VOID));
  fd_idefn(filedb_module,
           fd_make_ndprim(fd_make_cprim2x("FILE-POOL-PREFETCH!",file_pool_prefetch,2,
                                          fd_raw_pool_type,FD_VOID,-1,FD_VOID)));


  fd_idefn(filedb_module,fd_make_cprim4x("POPULATE-HASH-INDEX",populate_hash_index,2,
                                         -1,FD_VOID,-1,FD_VOID,
                                         fd_fixnum_type,FD_VOID,-1,FD_VOID));
  fd_idefn(filedb_module,fd_make_cprim6x("MAKE-HASH-INDEX",make_hash_index,2,
                                         fd_string_type,FD_VOID,
                                         fd_fixnum_type,FD_VOID,
                                         -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,
                                         -1,FD_FALSE));
  fd_idefn(filedb_module,fd_make_cprim3x("HASH-INDEX-BUCKET",hash_index_bucket,2,
                                         -1,FD_VOID,-1,FD_VOID));
  fd_idefn(filedb_module,
           fd_make_cprim1("HASH-INDEX-SLOTIDS",hash_index_slotids,1));
  fd_idefn(filedb_module,fd_make_cprim1("HASH-INDEX-STATS",hash_index_stats,1));


  fd_idefn(filedb_module,fd_make_cprim1("HASH-DTYPE",lisphashdtype2,1));
  fd_idefn(filedb_module,fd_make_cprim1("HASH-DTYPE2",lisphashdtype2,1));
  fd_idefn(filedb_module,fd_make_cprim1("HASH-DTYPE3",lisphashdtype3,1));
  fd_idefn(filedb_module,fd_make_cprim1("HASH-DTYPE1",lisphashdtype1,1));

  fd_idefn(filedb_module,fd_make_cprim1("HASH-DTYPE-REP",lisphashdtyperep,1));

  fd_finish_module(filedb_module);
  fd_persist_module(filedb_module);
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
