/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/cons.h"

#include <libu8/u8printf.h>
#include <libu8/u8timefns.h>

#include <stdarg.h>
#include <math.h>

fd_exception fd_MallocFailed=_("malloc/realloc failed");
fd_exception fd_StringOverflow=_("allocating humongous string past limit");
fd_exception fd_StackOverflow=_("Scheme stack overflow");
fd_exception fd_TypeError=_("Type error"), fd_RangeError=_("Range error");
fd_exception fd_DoubleGC=_("Freeing already freed CONS");
fd_exception fd_UsingFreedCons=_("Using freed CONS");
fd_exception fd_FreeingNonHeapCons=_("Freeing non-heap cons");
u8_mutex _fd_ptr_locks[FD_N_PTRLOCKS];

fd_exception fd_BadPtr=_("bad dtype pointer");

u8_string fd_type_names[FD_TYPE_MAX];
fd_hashfn fd_hashfns[FD_TYPE_MAX];
fd_checkfn fd_immediate_checkfns[FD_MAX_IMMEDIATE_TYPES+4];

fd_copy_fn fd_copiers[FD_TYPE_MAX];
fd_unparse_fn fd_unparsers[FD_TYPE_MAX];
fd_dtype_fn fd_dtype_writers[FD_TYPE_MAX];
fd_recycle_fn fd_recyclers[FD_TYPE_MAX];
fd_compare_fn fd_comparators[FD_TYPE_MAX];

static u8_mutex constant_registry_lock;
int fd_n_constants = FD_N_BUILTIN_CONSTANTS;

ssize_t fd_max_strlen = -1;

const char *fd_constant_names[256]={
  "#?","#f","#t","{}","()","#eof","#eod","#eox",
  "#bad_dtype","#bad_parse","#oom","#type_error","#range_error",
  "#error","#badptr","#throw","#exception_tag","#unbound",
  "#neverseen","#lockholder","#default",        /* 21 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL, /* 30 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL, /* 100 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL, /* 200 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL, /* 250 */
  NULL,NULL,NULL,NULL,NULL,NULL};

FD_EXPORT
fdtype fd_register_constant(u8_string name)
{
  int i = 0;
  u8_lock_mutex(&constant_registry_lock);
  while (i<fd_n_constants) {
    if (strcasecmp(name,fd_constant_names[i])==0)
      return FD_CONSTANT(i);
    else i++;}
  fdtype constant=FD_CONSTANT(i);
  if (fd_add_hashname(name,constant)<0) {
    u8_seterr("ConstantConflict","fd_register_constant",
              u8_strdup(name));
    return FD_ERROR_VALUE;}
  else {
    fd_constant_names[fd_n_constants++]=name;
    return constant;}
}

static int validate_constant(fdtype x)
{
  int num = (FD_GET_IMMEDIATE(x,fd_constant_type));
  if ((num>=0) && (num<fd_n_constants) &&
      (fd_constant_names[num] != NULL))
    return 1;
  else return 0;
}

FD_EXPORT
/* fd_check_immediate:
     Arguments: a list pointer
     Returns: 1 or 0 (an int)
  Checks an immediate pointer for validity.
*/
int fd_check_immediate(fdtype x)
{
  int type = FD_IMMEDIATE_TYPE(x);
  if (type<fd_next_immediate_type)
    if (fd_immediate_checkfns[type])
      return fd_immediate_checkfns[type](x);
    else return 1;
  else return 0;
}

/* CONS methods for external calls */

FD_EXPORT void _FD_INIT_CONS(void *vptr,fd_ptr_type type)
{
  fd_raw_cons ptr = (fd_raw_cons)vptr;
  FD_INIT_CONS(ptr,type);
}
FD_EXPORT void _FD_INIT_FRESH_CONS(void *vptr,fd_ptr_type type)
{
  fd_raw_cons ptr = (fd_raw_cons)vptr;
  FD_INIT_FRESH_CONS(ptr,type);
}
FD_EXPORT void _FD_INIT_STACK_CONS(void *vptr,fd_ptr_type type)
{
  fd_raw_cons ptr = (fd_raw_cons)vptr;
  FD_INIT_STACK_CONS(ptr,type);
}
FD_EXPORT void _FD_INIT_STATIC_CONS(void *vptr,fd_ptr_type type)
{
  fd_raw_cons ptr = (fd_raw_cons)vptr;
  FD_INIT_STATIC_CONS(ptr,type);
}
FD_EXPORT void _FD_SET_CONS_TYPE(void *vptr,fd_ptr_type type)
{
  fd_raw_cons ptr = (fd_raw_cons)vptr;
  FD_SET_CONS_TYPE(ptr,type);
}
FD_EXPORT void _FD_SET_REFCOUNT(void *vptr,unsigned int count)
{
  fd_raw_cons ptr = (fd_raw_cons)vptr;
  FD_SET_REFCOUNT(ptr,count);
}

FD_EXPORT fdtype _fd_incref_fn(fdtype ptr)
{
  return fd_incref(ptr);
}

FD_EXPORT void _fd_decref_fn(fdtype ptr)
{
  fd_decref(ptr);
}

FD_EXPORT void _fd_decref_elts(unsigned int n,const fdtype *elts)
{
  return fd_decref_elts(n,elts);
}

FD_EXPORT int _fd_incref_elts(unsigned int n,const fdtype *elts)
{
  return fd_incref_elts(n,elts);
}

FD_EXPORT
/* fdtype_equal:
    Arguments: two dtype pointers
    Returns: 1 or 0 (an int)
  Returns 1 if the two objects are equal. */
int fdtype_equal(fdtype x,fdtype y)
{
  if (FD_ATOMICP(x)) return (x == y);
  else if (FD_ATOMICP(y)) return (x == y);
  else if ((FD_CONS_DATA(x)) == (FD_CONS_DATA(y))) return 1;
  else if ((FD_PRECHOICEP(x)) || (FD_PRECHOICEP(y))) {
    int convert_x = FD_PRECHOICEP(x), convert_y = FD_PRECHOICEP(y);
    fdtype cx = ((convert_x) ? (fd_make_simple_choice(x)) : (x));
    fdtype cy = ((convert_y) ? (fd_make_simple_choice(y)) : (y));
    int result = fdtype_equal(cx,cy);
    if (convert_x) fd_decref(cx);
    if (convert_y) fd_decref(cy);
    return result;}
  else if (!(FD_TYPEP(y,FD_PTR_TYPE(x))))
    if ((FD_PACKETP(x))&&(FD_PACKETP(y)))
      if ((FD_PACKET_LENGTH(x)) != (FD_PACKET_LENGTH(y))) return 0;
      else if (memcmp(FD_PACKET_DATA(x),FD_PACKET_DATA(y),FD_PACKET_LENGTH(x))==0)
        return 1;
      else return 0;
    else return 0;
  else if (FD_PAIRP(x))
    if (FDTYPE_EQUAL(FD_CAR(x),FD_CAR(y)))
      return (FDTYPE_EQUAL(FD_CDR(x),FD_CDR(y)));
    else return 0;
  else if (FD_STRINGP(x))
    if ((FD_STRLEN(x)) != (FD_STRLEN(y))) return 0;
    else if (strncmp(FD_STRDATA(x),FD_STRDATA(y),FD_STRLEN(x)) == 0)
      return 1;
    else return 0;
  else if (FD_PACKETP(x))
    if ((FD_PACKET_LENGTH(x)) != (FD_PACKET_LENGTH(y))) return 0;
    else if (memcmp(FD_PACKET_DATA(x),FD_PACKET_DATA(y),FD_PACKET_LENGTH(x))==0)
      return 1;
    else return 0;
  else if (FD_VECTORP(x))
    if ((FD_VECTOR_LENGTH(x)) != (FD_VECTOR_LENGTH(y))) return 0;
    else {
      int i = 0, len = FD_VECTOR_LENGTH(x);
      fdtype *xdata = FD_VECTOR_DATA(x), *ydata = FD_VECTOR_DATA(y);
      while (i < len)
        if (FDTYPE_EQUAL(xdata[i],ydata[i])) i++; else return 0;
      return 1;}
  else if ((FD_NUMBERP(x)) && (FD_NUMBERP(y)))
    if (fd_numcompare(x,y)==0) return 1;
    else return 0;
  else {
    fd_ptr_type ctype = FD_CONS_TYPE(FD_CONS_DATA(x));
    if (fd_comparators[ctype])
      return (fd_comparators[ctype](x,y,1)==0);
    else return 0;}
}

/* Strings */

FD_EXPORT
/* fd_init_string:
    Arguments: A pointer to an FD_STRING struct, a length, and a byte vector.
    Returns: a lisp string
  This returns a lisp string object from a character string.
  If the structure pointer is NULL, one is mallocd.
  If the length is negative, it is computed. */
fdtype fd_init_string(struct FD_STRING *ptr,int slen,u8_string string)
{
  int len = ((slen<0) ? (strlen(string)) : (slen));
  if (ptr == NULL) {
    ptr = u8_alloc(struct FD_STRING);
    FD_INIT_STRUCT(ptr,struct FD_STRING);
    ptr->fd_freebytes = 1;}
  FD_INIT_CONS(ptr,fd_string_type);
  if ((len==0) && (string == NULL)) {
    u8_byte *bytes = u8_malloc(1); *bytes='\0';
    string = (u8_string)bytes;}
  ptr->fd_bytelen = len; ptr->fd_bytes = string;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT
/* fd_extract_string:
    Arguments: A pointer to an FD_STRING struct, and two pointers to ut8-strings
    Returns: a lisp string
  This returns a lisp string object from a region of a character string.
  If the structure pointer is NULL, one is mallocd.
  This copies the region between the pointers into a string and initializes
   a lisp string based on the region. 
   If the second argument is NULL, the end of the first argument is used. */
fdtype fd_extract_string(struct FD_STRING *ptr,u8_string start,u8_string end)
{
  ssize_t length = ((end == NULL) ? (strlen(start)) : (end-start));
  if ((length>=0)&&((fd_max_strlen<0)||(length<fd_max_strlen))) {
    u8_byte *bytes = NULL; int freedata = 1;
    if (ptr == NULL) {
      ptr = u8_malloc(sizeof(struct FD_STRING)+length+1);
      bytes = ((u8_byte *)ptr)+sizeof(struct FD_STRING);
      memcpy(bytes,start,length); bytes[length]='\0';
      freedata = 0;}
    else bytes = (u8_byte *)u8_strndup(start,length+1);
    FD_INIT_CONS(ptr,fd_string_type);
    ptr->fd_bytelen = length; ptr->fd_bytes = bytes; ptr->fd_freebytes = freedata;
    return FDTYPE_CONS(ptr);}
  else return fd_err(fd_StringOverflow,"fd_extract_string",NULL,FD_VOID);
}

FD_EXPORT
/* fd_substring:
    Arguments: two pointers to utf-8 strings
    Returns: a lisp string
  This returns a lisp string object from a region of a character string.
  If the structure pointer is NULL, one is mallocd.
  This copies the region between the pointers into a string and initializes
   a lisp string based on the region. */
fdtype fd_substring(u8_string start,u8_string end)
{
  ssize_t length = ((end == NULL) ? (strlen(start)) : (end-start));
  if ((length>=0)&&((fd_max_strlen<0)||(length<fd_max_strlen))) {
    struct FD_STRING *ptr = u8_malloc(sizeof(struct FD_STRING)+length+1);
    u8_byte *bytes = ((u8_byte *)ptr)+sizeof(struct FD_STRING);
    memcpy(bytes,start,length); bytes[length]='\0';
    FD_INIT_FRESH_CONS(ptr,fd_string_type);
    ptr->fd_bytelen = length; ptr->fd_bytes = bytes; ptr->fd_freebytes = 0;
    return FDTYPE_CONS(ptr);}
  else return fd_err(fd_StringOverflow,"fd_substring",NULL,FD_VOID);
}

FD_EXPORT
/* fd_make_string:
    Arguments: A pointer to an FD_STRING struct, a length, and a pointer to a byte vector
    Returns: a lisp string
  This returns a lisp string object from a string, copying the string
  If the structure pointer is NULL, the lisp string is uniconsed, so that
    the string data is contiguous with the struct. */
fdtype fd_make_string(struct FD_STRING *ptr,int len,u8_string string)
{
  int length = ((len>=0)?(len):(strlen(string)));
  u8_byte *bytes = NULL; int freedata = 1;
  if (ptr == NULL) {
    ptr = u8_malloc(sizeof(struct FD_STRING)+length+1);
    bytes = ((u8_byte *)ptr)+sizeof(struct FD_STRING);
    if (string) memcpy(bytes,string,length);
    else memset(bytes,'?',length);
    bytes[length]='\0';
    freedata = 0;}
  else {
    bytes = u8_malloc(length+1);
    memcpy(bytes,string,length); bytes[length]='\0';}
  FD_INIT_CONS(ptr,fd_string_type);
  ptr->fd_bytelen = length; ptr->fd_bytes = bytes; ptr->fd_freebytes = freedata;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT
/* fd_block_string:
    Arguments: a length, and a pointer to a byte vector
    Returns: a lisp string
  This returns a uniconsed lisp string object from a string,
    copying and freeing the string data
*/
fdtype fd_block_string(int len,u8_string string)
{
  u8_byte *bytes = NULL;
  int length = ((len>=0)?(len):(strlen(string)));
  struct FD_STRING *ptr = u8_malloc(sizeof(struct FD_STRING)+length+1);
  bytes = ((u8_byte *)ptr)+sizeof(struct FD_STRING);
  if (string) memcpy(bytes,string,length);
  else memset(bytes,'?',length);
  bytes[length]='\0';
  FD_INIT_CONS(ptr,fd_string_type);
  ptr->fd_bytelen = length; ptr->fd_bytes = bytes; ptr->fd_freebytes = 0;
  if (string) u8_free(string);
  return FDTYPE_CONS(ptr);
}

FD_EXPORT
/* fd_conv_string:
    Arguments: A pointer to an FD_STRING struct, a length, and a
      pointer to a byte vector
    Returns: a lisp string
  This returns a lisp string object from a string, copying and freeing the string
  If the structure pointer is NULL, the lisp string is uniconsed, so that
    the string data is contiguous with the struct. */
fdtype fd_conv_string(struct FD_STRING *ptr,int len,u8_string string)
{
  int length = ((len>0)?(len):(strlen(string)));
  u8_byte *bytes = NULL; int freedata = 1;
  if (ptr == NULL) {
    ptr = u8_malloc(sizeof(struct FD_STRING)+length+1);
    bytes = ((u8_byte *)ptr)+sizeof(struct FD_STRING);
    memcpy(bytes,string,length); bytes[length]='\0';
    freedata = 0;}
  else {
    bytes = u8_malloc(length+1);
    memcpy(bytes,string,length); bytes[length]='\0';}
  FD_INIT_CONS(ptr,fd_string_type);
  ptr->fd_bytelen = length;
  ptr->fd_bytes = bytes;
  ptr->fd_freebytes = freedata;
  /* Free the string */
  u8_free(string);
  return FDTYPE_CONS(ptr);
}

FD_EXPORT
/* fdtype_string:
    Arguments: a C string (u8_string)
    Returns: a lisp string
  */
fdtype fdtype_string(u8_string string)
{
  return fd_make_string(NULL,-1,string);
}

/* Pairs */

FD_EXPORT fdtype fd_init_pair(struct FD_PAIR *ptr,fdtype car,fdtype cdr)
{
  if (ptr == NULL) ptr = u8_alloc(struct FD_PAIR);
  FD_INIT_CONS(ptr,fd_pair_type);
  ptr->car = car; ptr->cdr = cdr;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_make_pair(fdtype car,fdtype cdr)
{
  return fd_init_pair(NULL,fd_incref(car),fd_incref(cdr));
}

FD_EXPORT fdtype fd_make_list(int len,...)
{
  va_list args; int i = 0;
  fdtype *elts = u8_alloc_n(len,fdtype), result = FD_EMPTY_LIST;
  va_start(args,len);
  while (i<len) elts[i++]=va_arg(args,fdtype);
  va_end(args);
  i = len-1; while (i>=0) {
    result = fd_init_pair(NULL,elts[i],result); i--;}
  u8_free(elts);
  return result;
}

FD_EXPORT int fd_list_length(fdtype l)
{
  int len = 0; fdtype scan = l; while (FD_PAIRP(scan)) {
    len++; scan = FD_CDR(scan);}
  if (FD_EMPTY_LISTP(scan)) return len;
  else return -len;
}

/* Vectors */

FD_EXPORT fdtype fd_init_vector(struct FD_VECTOR *ptr,int len,fdtype *data)
{
  fdtype *elts; int freedata = 1;
  if ((ptr == NULL)&&(data == NULL)) {
    int i = 0;
    ptr = u8_malloc(sizeof(struct FD_VECTOR)+(sizeof(fdtype)*len));
    /* This might be weird on non byte-addressed architectures */
    elts = ((fdtype *)(((unsigned char *)ptr)+sizeof(struct FD_VECTOR)));
    while (i < len) elts[i++]=FD_VOID;
    freedata = 0;}
  else if (ptr == NULL) {
    ptr = u8_alloc(struct FD_VECTOR);
    elts = data;}
  else if (data == NULL) {
      int i = 0; elts = u8_malloc(sizeof(fdtype)*len);
      while (i<len) elts[i]=FD_VOID;
      freedata = 1;}
  else elts = data;
  FD_INIT_CONS(ptr,fd_vector_type);
  ptr->fdvec_length = len; 
  ptr->fdvec_elts = elts; 
  ptr->fdvec_free_elts = freedata;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_make_nvector(int len,...)
{
  va_list args; int i = 0;
  fdtype result, *elts;
  va_start(args,len);
  result = fd_init_vector(NULL,len,NULL);
  elts = FD_VECTOR_ELTS(result);
  while (i<len) elts[i++]=va_arg(args,fdtype);
  va_end(args);
  return result;
}

FD_EXPORT fdtype fd_make_vector(int len,fdtype *data)
{
  int i = 0;
  struct FD_VECTOR *ptr=
    u8_malloc(sizeof(struct FD_VECTOR)+(sizeof(fdtype)*len));
  fdtype *elts = ((fdtype *)(((unsigned char *)ptr)+sizeof(struct FD_VECTOR)));
  FD_INIT_CONS(ptr,fd_vector_type);
  ptr->fdvec_length = len; 
  ptr->fdvec_elts = elts; 
  ptr->fdvec_free_elts = 0;
  if (data) {
    while (i < len) {elts[i]=data[i]; i++;}}
  else {while (i < len) {elts[i]=FD_VOID; i++;}}
  return FDTYPE_CONS(ptr);
}

/* Rails */

FD_EXPORT fdtype fd_init_code(struct FD_VECTOR *ptr,int len,fdtype *data)
{
  fdtype *elts; int i = 0, freedata = 1;
  if ((ptr == NULL)&&(data == NULL)) {
    ptr = u8_malloc(sizeof(struct FD_VECTOR)+(sizeof(fdtype)*len));
    elts = ((fdtype *)(((unsigned char *)ptr)+sizeof(struct FD_VECTOR)));
    freedata = 0;}
  else if (ptr == NULL) {
    ptr = u8_alloc(struct FD_VECTOR);
    elts = data;}
  else if (data == NULL) {
    int i = 0; elts = u8_alloc_n(len,fdtype);
    while (i<len) elts[i]=FD_VOID;
    freedata = 1;}
  else {
    ptr = u8_alloc(struct FD_VECTOR);
    elts = data;}
  FD_INIT_CONS(ptr,fd_code_type);
  if (data == NULL) while (i < len) elts[i++]=FD_VOID;
  ptr->fdvec_length = len; 
  ptr->fdvec_elts = elts; 
  ptr->fdvec_free_elts = freedata;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_make_nrail(int len,...)
{
  va_list args; int i = 0;
  fdtype result = fd_init_code(NULL,len,NULL);
  fdtype *elts = FD_CODE_ELTS(result);
  va_start(args,len);
  while (i<len) elts[i++]=va_arg(args,fdtype);
  va_end(args);
  return result;
}

FD_EXPORT fdtype fd_make_code(int len,fdtype *data)
{
  int i = 0;
  struct FD_VECTOR *ptr = u8_malloc
    (sizeof(struct FD_VECTOR)+(sizeof(fdtype)*len));
  fdtype *elts = ((fdtype *)(((unsigned char *)ptr)+sizeof(struct FD_VECTOR)));
  FD_INIT_CONS(ptr,fd_code_type);
  ptr->fdvec_length = len;
  ptr->fdvec_elts = elts;
  ptr->fdvec_free_elts = 0;
  while (i < len) {elts[i]=data[i]; i++;}
  return FDTYPE_CONS(ptr);
}

/* Packets */

FD_EXPORT fdtype fd_init_packet
  (struct FD_STRING *ptr,int len,const unsigned char *data)
{
  if ((ptr == NULL)&&(data == NULL))
    return fd_make_packet(ptr,len,data);
  if (ptr == NULL) {
    ptr = u8_alloc(struct FD_STRING);
    ptr->fd_freebytes = 1;}
  FD_INIT_CONS(ptr,fd_packet_type);
  if (data == NULL) {
    u8_byte *consed = u8_malloc(len+1);
    memset(consed,0,len+1);
    data = consed;}
  ptr->fd_bytelen = len; ptr->fd_bytes = data;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_make_packet
  (struct FD_STRING *ptr,int len,const unsigned char *data)
{
  u8_byte *bytes = NULL; int freedata = 1;
  if (ptr == NULL) {
    ptr = u8_malloc(sizeof(struct FD_STRING)+len+1);
    bytes = ((u8_byte *)ptr)+sizeof(struct FD_STRING);
    if (data) {
      memcpy(bytes,data,len);
      bytes[len]='\0';}
    else memset(bytes,0,len+1);
    freedata = 0;}
  else if (data == NULL) {
    bytes = u8_malloc(len+1);}
  else bytes = (unsigned char *)data;
  FD_INIT_CONS(ptr,fd_packet_type);
  ptr->fd_bytelen = len; ptr->fd_bytes = bytes; ptr->fd_freebytes = freedata;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_bytes2packet
  (struct FD_STRING *ptr,int len,const unsigned char *data)
{
  u8_byte *bytes = NULL; int freedata = (data!=NULL);
  if (ptr == NULL) {
    ptr = u8_malloc(sizeof(struct FD_STRING)+len+1);
    bytes = ((u8_byte *)ptr)+sizeof(struct FD_STRING);
    if (data) {
      memcpy(bytes,data,len);
      bytes[len]='\0';}
    else memset(bytes,0,len+1);
    freedata = 0;}
  else {
    bytes = u8_malloc(len);
    if (data == NULL) memset(bytes,0,len);
    else memcpy(bytes,data,len);}
  FD_INIT_CONS(ptr,fd_packet_type);
  ptr->fd_bytelen = len; ptr->fd_bytes = bytes; ptr->fd_freebytes = freedata;
  if (freedata) u8_free(data);
  return FDTYPE_CONS(ptr);
}

/* Compounds */

fdtype fd_compound_descriptor_type;

FD_EXPORT fdtype fd_init_compound
  (struct FD_COMPOUND *p,fdtype tag,u8_byte mutable,short n,...)
{
  va_list args; int i = 0; fdtype *write, *limit, initfn = FD_FALSE;
  if (FD_EXPECT_FALSE((n<0)||(n>=256))) {
    /* Consume the arguments, just in case the implementation is a
       little flaky. */
    va_start(args,n);
    while (i<n) {va_arg(args,fdtype); i++;}
    return fd_type_error
      (_("positive byte"),"fd_init_compound",FD_SHORT2DTYPE(n));}
  else if (p == NULL) {
    if (n==0) p = u8_malloc(sizeof(struct FD_COMPOUND));
    else p = u8_malloc(sizeof(struct FD_COMPOUND)+(n-1)*sizeof(fdtype));}
  FD_INIT_CONS(p,fd_compound_type);
  if (mutable) u8_init_mutex(&(p->compound_lock));
  p->compound_typetag = fd_incref(tag); 
  p->compound_ismutable = mutable; 
  p->compound_isopaque = 0;
  p->fd_n_elts = n;
  if (n>0) {
    write = &(p->compound_0); limit = write+n;
    va_start(args,n);
    while (write<limit) {
      fdtype value = va_arg(args,fdtype);
      *write = value; write++;}
    va_end(args);
    if (FD_ABORTP(initfn)) {
      write = &(p->compound_0);
      while (write<limit) {fd_decref(*write); write++;}
      return initfn;}
    else return FDTYPE_CONS(p);}
  else return FDTYPE_CONS(p);
}

FD_EXPORT fdtype fd_init_compound_from_elts
  (struct FD_COMPOUND *p,fdtype tag,u8_byte mutable,short n,fdtype *elts)
{
  fdtype *write, *limit, *read = elts, initfn = FD_FALSE;
  if (FD_EXPECT_FALSE((n<0) || (n>=256)))
    return fd_type_error(_("positive byte"),"fd_init_compound_from_elts",
                         FD_SHORT2DTYPE(n));
  else if (p == NULL) {
    if (n==0) p = u8_malloc(sizeof(struct FD_COMPOUND));
    else p = u8_malloc(sizeof(struct FD_COMPOUND)+(n-1)*sizeof(fdtype));}
  FD_INIT_CONS(p,fd_compound_type);
  if (mutable) u8_init_mutex(&(p->compound_lock));
  p->compound_typetag = fd_incref(tag);
  p->compound_ismutable = mutable;
  p->fd_n_elts = n; p->compound_isopaque = 0;
  if (n>0) {
    write = &(p->compound_0); limit = write+n;
    while (write<limit) {
      *write = *read++; write++;}
    if (FD_ABORTP(initfn)) {
      write = &(p->compound_0);
      while (write<limit) {fd_decref(*write); write++;}
      return initfn;}
    else return FDTYPE_CONS(p);}
  else return FDTYPE_CONS(p);
}

/* Registering new primitive types */

static u8_mutex type_registry_lock;

unsigned int fd_next_cons_type=
  FD_CONS_TYPECODE(FD_BUILTIN_CONS_TYPES);
unsigned int fd_next_immediate_type=
  FD_IMMEDIATE_TYPECODE(FD_BUILTIN_IMMEDIATE_TYPES);

FD_EXPORT int fd_register_cons_type(char *name)
{
  int typecode;
  u8_lock_mutex(&type_registry_lock);
  if (fd_next_cons_type>=FD_MAX_CONS_TYPE) {
    u8_unlock_mutex(&type_registry_lock);
    return -1;}
  typecode = fd_next_cons_type;
  fd_next_cons_type++;
  fd_type_names[typecode]=name;
  u8_unlock_mutex(&type_registry_lock);
  return typecode;
}

FD_EXPORT int fd_register_immediate_type(char *name,fd_checkfn fn)
{
  int typecode;
  u8_lock_mutex(&type_registry_lock);
  if (fd_next_immediate_type>=FD_MAX_IMMEDIATE_TYPE) {
    u8_unlock_mutex(&type_registry_lock);
    return -1;}
  typecode = fd_next_immediate_type;
  fd_immediate_checkfns[typecode]=fn;
  fd_next_immediate_type++;
  fd_type_names[typecode]=name;
  u8_unlock_mutex(&type_registry_lock);
  return typecode;
}

/* Compound type information */

struct FD_COMPOUND_TYPEINFO *fd_compound_entries = NULL;
static u8_mutex compound_registry_lock;

FD_EXPORT
struct FD_COMPOUND_TYPEINFO
*fd_register_compound(fdtype symbol,fdtype *datap,int *corep)
{
  struct FD_COMPOUND_TYPEINFO *scan, *newrec;
  u8_lock_mutex(&compound_registry_lock);
  scan = fd_compound_entries;
  while (scan)
    if (FD_EQ(scan->compound_typetag,symbol)) {
      if (datap) {
        fdtype data = *datap;
        if (FD_VOIDP(scan->fd_compound_metadata)) {
          fd_incref(data); scan->fd_compound_metadata = data;}
        else {
          fdtype data = *datap; fd_decref(data);
          data = scan->fd_compound_metadata; fd_incref(data);
          *datap = data;}}
      if (corep) {
        if (scan->fd_compound_corelen<0) scan->fd_compound_corelen = *corep;
        else *corep = scan->fd_compound_corelen;}
      u8_unlock_mutex(&compound_registry_lock);
      return scan;}
    else scan = scan->fd_compound_nextinfo;
  newrec = u8_alloc(struct FD_COMPOUND_TYPEINFO);
  memset(newrec,0,sizeof(struct FD_COMPOUND_TYPEINFO));
  if (datap) {
    fdtype data = *datap; fd_incref(data); newrec->fd_compound_metadata = data;}
  else newrec->fd_compound_metadata = FD_VOID;
  newrec->fd_compound_corelen = ((corep)?(*corep):(-1));
  newrec->fd_compound_nextinfo = fd_compound_entries; 
  newrec->compound_typetag = symbol;
  newrec->fd_compound_parser = NULL; 
  newrec->fd_compound_dumpfn = NULL; 
  newrec->fd_compound_restorefn = NULL;
  newrec->fd_compund_tablefns = NULL;
  fd_compound_entries = newrec;
  u8_unlock_mutex(&compound_registry_lock);
  return newrec;
}

FD_EXPORT struct FD_COMPOUND_TYPEINFO 
          *fd_declare_compound(fdtype symbol,fdtype data,int core_slots)
{
  struct FD_COMPOUND_TYPEINFO *scan, *newrec;
  u8_lock_mutex(&compound_registry_lock);
  scan = fd_compound_entries;
  while (scan)
    if (FD_EQ(scan->compound_typetag,symbol)) {
      if (!(FD_VOIDP(data))) {
        fdtype old_data = scan->fd_compound_metadata;
        scan->fd_compound_metadata = fd_incref(data);
        fd_decref(old_data);}
      if (core_slots>0) scan->fd_compound_corelen = core_slots;
      u8_unlock_mutex(&compound_registry_lock);
      return scan;}
    else scan = scan->fd_compound_nextinfo;
  newrec = u8_alloc(struct FD_COMPOUND_TYPEINFO);
  memset(newrec,0,sizeof(struct FD_COMPOUND_TYPEINFO));
  newrec->fd_compound_metadata = data;
  newrec->fd_compound_corelen = core_slots;
  newrec->compound_typetag = symbol;
  newrec->fd_compound_nextinfo = fd_compound_entries; 
  newrec->fd_compound_parser = NULL;
  newrec->fd_compound_dumpfn = NULL;
  newrec->fd_compound_restorefn = NULL;
  newrec->fd_compund_tablefns = NULL;
  fd_compound_entries = newrec;
  u8_unlock_mutex(&compound_registry_lock);
  return newrec;
}

FD_EXPORT struct FD_COMPOUND_TYPEINFO *fd_lookup_compound(fdtype symbol)
{
  struct FD_COMPOUND_TYPEINFO *scan = fd_compound_entries;
  while (scan)
    if (FD_EQ(scan->compound_typetag,symbol)) {
      return scan;}
    else scan = scan->fd_compound_nextinfo;
  return NULL;
}

/* Utility functions (for debugging) */

FD_EXPORT fd_cons fd_cons_data(fdtype x)
{
  unsigned long as_int = (x&(~0x3));
  void *as_addr = (void *) as_int;
  return (fd_cons) as_addr;
}

FD_EXPORT struct FD_PAIR *fd_pair_data(fdtype x)
{
  return FD_CONSPTR(fd_pair,x);
}

FD_EXPORT int _fd_find_elt(fdtype x,fdtype *v,int n)
{
  int i = 0; while (i<n)
    if (v[i]==x) return i;
    else i++;
  return -1;
}

int fd_ptr_debug_density = 1;

FD_EXPORT void _fd_bad_pointer(fdtype badx,u8_context cxt)
{
  u8_raise(fd_BadPtr,cxt,NULL);
}

fd_exception get_pointer_exception(fdtype x)
{
  if (FD_OIDP(x)) return _("BadOIDPtr");
  else if (FD_CONSP(x)) return _("BadCONSPtr");
  else if (FD_IMMEDIATEP(x)) {
    int ptype = FD_IMMEDIATE_TYPE(x);
    if (ptype>=fd_next_immediate_type)
      return _("BadImmediateType");
    else switch (ptype) {
      case fd_symbol_type: return _("BadSymbol");
      case fd_constant_type: return _("BadConstant");
      case fd_fcnid_type: return _("BadFCNId");
      case fd_opcode_type: return _("BadOpcode");
      case fd_lexref_type: return _("BadLexref");
      default:
        return _("BadImmediate");}}
  else return fd_BadPtr;
}

FD_EXPORT fdtype fd_badptr_err(fdtype result,u8_context cxt,u8_string details)
{
  fd_seterr( get_pointer_exception(result), cxt,
             u8dup(details), FD_UINT2DTYPE(result) );
  return FD_ERROR_VALUE;
}

/* Testing */

static U8_MAYBE_UNUSED int some_false(fdtype arg)
{
  int some_false = 0;
  FD_DOELTS(elt,arg,count) {
    if (FD_FALSEP(elt)) some_false = 1;}
  return some_false;
}


/* Initialization */

void fd_init_cons_c()
{
  int i;
  i = 0; while (i < FD_N_PTRLOCKS) {
    u8_init_mutex(&_fd_ptr_locks[i]);
    i++;}

  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&constant_registry_lock);
  u8_init_mutex(&compound_registry_lock);
  u8_init_mutex(&type_registry_lock);

  i = 0; while (i < FD_TYPE_MAX) fd_type_names[i++]=NULL;
  i = 0; while (i<FD_TYPE_MAX) fd_hashfns[i++]=NULL;
  i = 0; while (i<FD_MAX_IMMEDIATE_TYPES+4)
         fd_immediate_checkfns[i++]=NULL;

  i = 0; while (i < FD_TYPE_MAX) fd_recyclers[i++]=NULL;
  i = 0; while (i < FD_TYPE_MAX) fd_unparsers[i++]=NULL;
  i = 0; while (i < FD_TYPE_MAX) fd_dtype_writers[i++]=NULL;
  i = 0; while (i < FD_TYPE_MAX) fd_comparators[i++]=NULL;
  i = 0; while (i < FD_TYPE_MAX) fd_copiers[i++]=NULL;

  fd_immediate_checkfns[fd_constant_type]=validate_constant;

  fd_compound_descriptor_type=
    fd_init_compound
    (NULL,FD_VOID,9,
     fd_intern("COMPOUNDTYPE"),FD_INT(9),
     fd_make_nvector(9,FDSYM_TAG,FDSYM_LENGTH,
                     fd_intern("FIELDS"),fd_intern("INITFN"),
                     fd_intern("FREEFN"),fd_intern("COMPAREFN"),
                     fd_intern("STRINGFN"),fd_intern("DUMPFN"),
                     fd_intern("RESTOREFN")),
     FD_FALSE,FD_FALSE,FD_FALSE,FD_FALSE,
     FD_FALSE,FD_FALSE);
  ((fd_compound)fd_compound_descriptor_type)->compound_typetag=
    fd_compound_descriptor_type;
  fd_incref(fd_compound_descriptor_type);

  i=0; while (i<256) {
    if (fd_constant_names[i]) {
      fd_add_hashname(fd_constant_names[i],FD_CONSTANT(i));}
    i++;}
  fd_add_hashname("#true",FD_TRUE);
  fd_add_hashname("#false",FD_FALSE);
  fd_add_hashname("#empty",FD_EMPTY_CHOICE);
  fd_add_hashname("#dflt",FD_DEFAULT_VALUE);
#ifdef M_PI
  fdtype pival=fd_make_flonum(M_PI); FD_MAKE_STATIC(pival);
  fd_add_hashname("#pi",pival);
#endif
#ifdef M_E
  fdtype e_val=fd_make_flonum(M_E); FD_MAKE_STATIC(e_val);
  fd_add_hashname("#e",e_val);
#endif
#ifdef M_LOG2E
  fdtype log2e=fd_make_flonum(M_LOG2E); FD_MAKE_STATIC(log2e);
  fd_add_hashname("#log2e",log2e);
#endif
#ifdef M_LOG10E
  fdtype log10e=fd_make_flonum(M_LOG10E); FD_MAKE_STATIC(log10e);
  fd_add_hashname("#log2e",log10e);
#endif
#ifdef M_LN2
  fdtype natlog2=fd_make_flonum(M_LN2); FD_MAKE_STATIC(natlog2);
  fd_add_hashname("#ln2",natlog2);
#endif
#ifdef M_LN10
  fdtype natlog10=fd_make_flonum(M_LN10); FD_MAKE_STATIC(natlog10);
  fd_add_hashname("#ln10",natlog10);
#endif
  fd_add_hashname("#answer",FD_INT(42));
  fd_add_hashname("#life",FD_INT(42));
  fd_add_hashname("#universe",FD_INT(42));
  fd_add_hashname("#everything",FD_INT(42));
}




/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
