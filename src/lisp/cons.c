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

lispval fd_compound_descriptor_type;

ssize_t fd_max_strlen = -1;
int fd_check_utf8 = 0;

const char *fd_constant_names[256]={
  "#?","#f","#t","{}","()","#eof","#eod","#eox",
  "#bad_dtype","#bad_parse","#oom","#type_error","#range_error",
  "#error","#badptr","#throw","#exception_tag","#unbound",
  "#neverseen","#lockholder","#default","#preoid", /* 22 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL, /* 30 */
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
lispval fd_register_constant(u8_string name)
{
  int i = 0;
  u8_lock_mutex(&constant_registry_lock);
  while (i<fd_n_constants) {
    if (strcasecmp(name,fd_constant_names[i])==0)
      return FD_CONSTANT(i);
    else i++;}
  lispval constant=FD_CONSTANT(i);
  if (fd_add_hashname(name,constant)<0) {
    u8_seterr("ConstantConflict","fd_register_constant",
              u8_strdup(name));
    return FD_ERROR;}
  else {
    fd_constant_names[fd_n_constants++]=name;
    return constant;}
}

static int validate_constant(lispval x)
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
int fd_check_immediate(lispval x)
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

FD_EXPORT lispval _fd_incref_fn(lispval ptr)
{
  return fd_incref(ptr);
}

FD_EXPORT void _fd_decref_fn(lispval ptr)
{
  fd_decref(ptr);
}

FD_EXPORT
lispval *_fd_init_elts(lispval *elts,size_t n,lispval v)
{
  return fd_init_elts(elts,n,v);
}

FD_EXPORT ssize_t _fd_incref_elts(const lispval *elts,size_t n)
{
  return fd_incref_elts(elts,n);
}

FD_EXPORT ssize_t _fd_decref_elts(const lispval *elts,size_t n)
{
  return fd_decref_elts(elts,n);
}
FD_EXPORT ssize_t _fd_free_elts(lispval *elts,size_t n)
{
  return fd_free_elts(elts,n);
}

FD_EXPORT
/* lispval_equal:
    Arguments: two dtype pointers
    Returns: 1 or 0 (an int)
  Returns 1 if the two objects are equal. */
int lispval_equal(lispval x,lispval y)
{
  if (ATOMICP(x)) return (x == y);
  else if (ATOMICP(y)) return (x == y);
  else if ((FD_CONS_DATA(x)) == (FD_CONS_DATA(y))) return 1;
  else if ((PRECHOICEP(x)) || (PRECHOICEP(y))) {
    int convert_x = PRECHOICEP(x), convert_y = PRECHOICEP(y);
    lispval cx = ((convert_x) ? (fd_make_simple_choice(x)) : (x));
    lispval cy = ((convert_y) ? (fd_make_simple_choice(y)) : (y));
    int result = lispval_equal(cx,cy);
    if (convert_x) fd_decref(cx);
    if (convert_y) fd_decref(cy);
    return result;}
  else if ((NUMBERP(x)) && (NUMBERP(y)))
    if (fd_numcompare(x,y)==0) return 1;
    else return 0;
  else if ((PACKETP(x))&&(PACKETP(y))) {
    size_t xlen=FD_PACKET_LENGTH(x), ylen=FD_PACKET_LENGTH(y);
    if (((xlen)) != (ylen)) return 0;
    else if (memcmp(FD_PACKET_DATA(x),FD_PACKET_DATA(y),xlen)==0)
      return 1;
    else return 0;}
  else if (!(TYPEP(y,FD_PTR_TYPE(x))))
    /* At this point, If the types are different, the values are
       different. */
    return 0;
  else if (PAIRP(x))
    if (LISP_EQUAL(FD_CAR(x),FD_CAR(y)))
      return (LISP_EQUAL(FD_CDR(x),FD_CDR(y)));
    else return 0;
  else if (STRINGP(x))
    if ((STRLEN(x)) != (STRLEN(y))) return 0;
    else if (strncmp(CSTRING(x),CSTRING(y),STRLEN(x)) == 0)
      return 1;
    else return 0;
  else if (PACKETP(x))
    if ((FD_PACKET_LENGTH(x)) != (FD_PACKET_LENGTH(y))) return 0;
    else if (memcmp(FD_PACKET_DATA(x),FD_PACKET_DATA(y),FD_PACKET_LENGTH(x))==0)
      return 1;
    else return 0;
  else if (VECTORP(x))
    if ((VEC_LEN(x)) != (VEC_LEN(y))) return 0;
    else {
      int i = 0, len = VEC_LEN(x);
      lispval *xdata = VEC_DATA(x), *ydata = VEC_DATA(y);
      while (i < len)
        if (LISP_EQUAL(xdata[i],ydata[i])) i++; else return 0;
      return 1;}
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
lispval fd_init_string(struct FD_STRING *ptr,int slen,u8_string string)
{
  int len = ((slen<0) ? (strlen(string)) : (slen));
  if (ptr == NULL) {
    ptr = u8_alloc(struct FD_STRING);
    FD_INIT_STRUCT(ptr,struct FD_STRING);
    ptr->str_freebytes = 1;}
  FD_INIT_CONS(ptr,fd_string_type);
  if ((len==0) && (string == NULL)) {
    u8_byte *bytes = u8_malloc(1); *bytes='\0';
    string = (u8_string)bytes;}
  ptr->str_bytelen = len; ptr->str_bytes = string;
  return LISP_CONS(ptr);
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
lispval fd_extract_string(struct FD_STRING *ptr,u8_string start,u8_string end)
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
    ptr->str_bytelen = length;
    ptr->str_bytes = bytes;
    ptr->str_freebytes = freedata;
    if ( (fd_check_utf8) && (!(u8_validp(bytes))) ) {
      FD_SET_CONS_TYPE(ptr,fd_packet_type);
      fd_seterr("InvalidUTF8","fd_extract_string",NULL,LISP_CONS(ptr));
      return FD_ERROR_VALUE;}
    else return LISP_CONS(ptr);}
  else return fd_err(fd_StringOverflow,"fd_extract_string",NULL,VOID);
}

FD_EXPORT
/* fd_substring:
    Arguments: two pointers to utf-8 strings
    Returns: a lisp string
  This returns a lisp string object from a region of a character string.
  If the structure pointer is NULL, one is mallocd.
  This copies the region between the pointers into a string and initializes
   a lisp string based on the region. */
lispval fd_substring(u8_string start,u8_string end)
{
  ssize_t length = ((end == NULL) ? (strlen(start)) : (end-start));
  if ((length>=0)&&((fd_max_strlen<0)||(length<fd_max_strlen))) {
    struct FD_STRING *ptr = u8_malloc(sizeof(struct FD_STRING)+length+1);
    u8_byte *bytes = ((u8_byte *)ptr)+sizeof(struct FD_STRING);
    memcpy(bytes,start,length); bytes[length]='\0';
    FD_INIT_FRESH_CONS(ptr,fd_string_type);
    ptr->str_bytelen = length;
    ptr->str_bytes = bytes;
    ptr->str_freebytes = 0;
    if ( (fd_check_utf8) && (!(u8_validp(bytes))) ) {
      FD_SET_CONS_TYPE(ptr,fd_packet_type);
      fd_seterr("InvalidUTF8","fd_extract_string",NULL,LISP_CONS(ptr));
      return FD_ERROR_VALUE;}
    else return LISP_CONS(ptr);}
  else return fd_err(fd_StringOverflow,"fd_substring",NULL,VOID);
}

FD_EXPORT
/* fd_make_string:
    Arguments: A pointer to an FD_STRING struct, a length, and a pointer to a byte vector
    Returns: a lisp string
  This returns a lisp string object from a string, copying the string
  If the structure pointer is NULL, the lisp string is uniconsed, so that
    the string data is contiguous with the struct. */
lispval fd_make_string(struct FD_STRING *ptr,int len,u8_string string)
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
  ptr->str_bytelen = length;
  ptr->str_bytes = bytes;
  ptr->str_freebytes = freedata;
  if ( (fd_check_utf8) && (!(u8_validp(bytes))) ) {
    FD_SET_CONS_TYPE(ptr,fd_packet_type);
    fd_seterr("InvalidUTF8","fd_extract_string",NULL,LISP_CONS(ptr));
    return FD_ERROR_VALUE;}
  else return LISP_CONS(ptr);
}

FD_EXPORT
/* fd_block_string:
    Arguments: a length, and a pointer to a byte vector
    Returns: a lisp string
  This returns a uniconsed lisp string object from a string,
    copying and freeing the string data
*/
lispval fd_block_string(int len,u8_string string)
{
  u8_byte *bytes = NULL;
  int length = ((len>=0)?(len):(strlen(string)));
  struct FD_STRING *ptr = u8_malloc(sizeof(struct FD_STRING)+length+1);
  bytes = ((u8_byte *)ptr)+sizeof(struct FD_STRING);
  if (string) memcpy(bytes,string,length);
  else memset(bytes,'?',length);
  bytes[length]='\0';
  FD_INIT_CONS(ptr,fd_string_type);
  ptr->str_bytelen = length; ptr->str_bytes = bytes; ptr->str_freebytes = 0;
  if (string) u8_free(string);
  if ( (fd_check_utf8) && (!(u8_validp(bytes))) ) {
    FD_SET_CONS_TYPE(ptr,fd_packet_type);
    fd_seterr("InvalidUTF8","fd_extract_string",NULL,LISP_CONS(ptr));
    return FD_ERROR_VALUE;}
  else return LISP_CONS(ptr);
  return LISP_CONS(ptr);
}

FD_EXPORT
/* fd_conv_string:
    Arguments: A pointer to an FD_STRING struct, a length, and a
      pointer to a byte vector
    Returns: a lisp string
  This returns a lisp string object from a string, copying and freeing the string
  If the structure pointer is NULL, the lisp string is uniconsed, so that
    the string data is contiguous with the struct. */
lispval fd_conv_string(struct FD_STRING *ptr,int len,u8_string string)
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
  ptr->str_bytelen = length;
  ptr->str_bytes = bytes;
  ptr->str_freebytes = freedata;
  /* Free the string */
  u8_free(string);
  if ( (fd_check_utf8) && (!(u8_validp(bytes))) ) {
    FD_SET_CONS_TYPE(ptr,fd_packet_type);
    fd_seterr("InvalidUTF8","fd_extract_string",NULL,LISP_CONS(ptr));
    return FD_ERROR_VALUE;}
  else return LISP_CONS(ptr);
}

FD_EXPORT
/* lispval_string:
    Arguments: a C string (u8_string)
    Returns: a lisp string
  */
lispval lispval_string(u8_string string)
{
  return fd_make_string(NULL,-1,string);
}

/* Pairs */

FD_EXPORT lispval fd_init_pair(struct FD_PAIR *ptr,lispval car,lispval cdr)
{
  if (ptr == NULL) ptr = u8_alloc(struct FD_PAIR);
  FD_INIT_CONS(ptr,fd_pair_type);
  ptr->car = car; ptr->cdr = cdr;
  return LISP_CONS(ptr);
}

FD_EXPORT lispval fd_make_pair(lispval car,lispval cdr)
{
  return fd_init_pair(NULL,fd_incref(car),fd_incref(cdr));
}

FD_EXPORT lispval fd_make_list(int len,...)
{
  va_list args; int i = 0;
  lispval *elts = u8_alloc_n(len,lispval), result = NIL;
  va_start(args,len);
  while (i<len) elts[i++]=va_arg(args,lispval);
  va_end(args);
  i = len-1; while (i>=0) {
    result = fd_init_pair(NULL,elts[i],result); i--;}
  u8_free(elts);
  return result;
}

FD_EXPORT int fd_list_length(lispval l)
{
  int len = 0; lispval scan = l; while (PAIRP(scan)) {
    len++; scan = FD_CDR(scan);}
  if (NILP(scan)) return len;
  else return -len;
}

FD_EXPORT lispval fd_reverse_list(lispval l)
{
  lispval result = FD_EMPTY_LIST, scan=l;
  while (PAIRP(scan)) {
    lispval car  = FD_CAR(scan); fd_incref(car);
    result = fd_init_pair(NULL,car,result);
    scan = FD_CDR(scan);}
  if (NILP(scan))
    return result;
  else {
    fd_seterr("ImproperList","fd_reverse_list",NULL,l);
    fd_decref(result);
    return FD_ERROR_VALUE;}
}

/* Vectors */

FD_EXPORT lispval fd_init_vector(struct FD_VECTOR *ptr,int len,lispval *data)
{
  lispval *elts; int freedata = 1;
  if ((ptr == NULL)&&(data == NULL)) {
    int i = 0;
    ptr = u8_malloc(sizeof(struct FD_VECTOR)+(sizeof(lispval)*len));
    /* This might be weird on non byte-addressed architectures */
    elts = ((lispval *)(((unsigned char *)ptr)+sizeof(struct FD_VECTOR)));
    while (i < len) elts[i++]=VOID;
    freedata = 0;}
  else if (ptr == NULL) {
    ptr = u8_alloc(struct FD_VECTOR);
    elts = data;}
  else if (data == NULL) {
      int i = 0; elts = u8_malloc(sizeof(lispval)*len);
      while (i<len) elts[i]=VOID;
      freedata = 1;}
  else elts = data;
  FD_INIT_CONS(ptr,fd_vector_type);
  ptr->vec_length = len;
  ptr->vec_elts = elts;
  ptr->vec_free_elts = freedata;
  return LISP_CONS(ptr);
}

FD_EXPORT lispval fd_make_nvector(int len,...)
{
  va_list args; int i = 0;
  lispval result, *elts;
  va_start(args,len);
  result = fd_init_vector(NULL,len,NULL);
  elts = FD_VECTOR_ELTS(result);
  while (i<len) elts[i++]=va_arg(args,lispval);
  va_end(args);
  return result;
}

FD_EXPORT lispval fd_make_vector(int len,lispval *data)
{
  int i = 0;
  struct FD_VECTOR *ptr=
    u8_malloc(sizeof(struct FD_VECTOR)+(sizeof(lispval)*len));
  lispval *elts = ((lispval *)(((unsigned char *)ptr)+sizeof(struct FD_VECTOR)));
  FD_INIT_CONS(ptr,fd_vector_type);
  ptr->vec_length = len;
  ptr->vec_elts = elts;
  ptr->vec_free_elts = 0;
  if (data) {
    while (i < len) {elts[i]=data[i]; i++;}}
  else {while (i < len) {elts[i]=VOID; i++;}}
  return LISP_CONS(ptr);
}

/* Rails */

FD_EXPORT lispval fd_init_code(struct FD_VECTOR *ptr,int len,lispval *data)
{
  lispval *elts; int i = 0, freedata = 1;
  if ((ptr == NULL)&&(data == NULL)) {
    ptr = u8_malloc(sizeof(struct FD_VECTOR)+(sizeof(lispval)*len));
    elts = ((lispval *)(((unsigned char *)ptr)+sizeof(struct FD_VECTOR)));
    freedata = 0;}
  else if (ptr == NULL) {
    ptr = u8_alloc(struct FD_VECTOR);
    elts = data;}
  else if (data == NULL) {
    int i = 0; elts = u8_alloc_n(len,lispval);
    while (i<len) elts[i]=VOID;
    freedata = 1;}
  else {
    ptr = u8_alloc(struct FD_VECTOR);
    elts = data;}
  FD_INIT_CONS(ptr,fd_code_type);
  if (data == NULL) while (i < len) elts[i++]=VOID;
  ptr->vec_length = len; 
  ptr->vec_elts = elts; 
  ptr->vec_free_elts = freedata;
  return LISP_CONS(ptr);
}

FD_EXPORT lispval fd_make_nrail(int len,...)
{
  va_list args; int i = 0;
  lispval result = fd_init_code(NULL,len,NULL);
  lispval *elts = FD_CODE_ELTS(result);
  va_start(args,len);
  while (i<len) elts[i++]=va_arg(args,lispval);
  va_end(args);
  return result;
}

FD_EXPORT lispval fd_make_code(int len,lispval *data)
{
  int i = 0;
  struct FD_VECTOR *ptr = u8_malloc
    (sizeof(struct FD_VECTOR)+(sizeof(lispval)*len));
  lispval *elts = ((lispval *)(((unsigned char *)ptr)+sizeof(struct FD_VECTOR)));
  FD_INIT_CONS(ptr,fd_code_type);
  ptr->vec_length = len;
  ptr->vec_elts = elts;
  ptr->vec_free_elts = 0;
  while (i < len) {elts[i]=data[i]; i++;}
  return LISP_CONS(ptr);
}

/* Packets */

FD_EXPORT lispval fd_init_packet
  (struct FD_STRING *ptr,int len,const unsigned char *data)
{
  if ((ptr == NULL)&&(data == NULL))
    return fd_make_packet(ptr,len,data);
  if (ptr == NULL) {
    ptr = u8_alloc(struct FD_STRING);
    ptr->str_freebytes = 1;}
  FD_INIT_CONS(ptr,fd_packet_type);
  if (data == NULL) {
    u8_byte *consed = u8_malloc(len+1);
    memset(consed,0,len+1);
    data = consed;}
  ptr->str_bytelen = len; ptr->str_bytes = data;
  return LISP_CONS(ptr);
}

FD_EXPORT lispval fd_make_packet
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
  ptr->str_bytelen = len; ptr->str_bytes = bytes; ptr->str_freebytes = freedata;
  return LISP_CONS(ptr);
}

FD_EXPORT lispval fd_bytes2packet
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
  ptr->str_bytelen = len; ptr->str_bytes = bytes; ptr->str_freebytes = freedata;
  if (freedata) u8_free(data);
  return LISP_CONS(ptr);
}

/* Compounds */

FD_EXPORT lispval fd_init_compound
  (struct FD_COMPOUND *p,lispval tag,int ismutable,int n,...)
{
  va_list args; int i = 0; lispval *write, *limit, initfn = FD_FALSE;
  if (PRED_FALSE((n<0)||(n>=256))) {
    /* Consume the arguments, just in case the implementation is a
       little flaky. */
    va_start(args,n);
    while (i<n) {va_arg(args,lispval); i++;}
    return fd_type_error
      (_("positive byte"),"fd_init_compound",FD_SHORT2DTYPE(n));}
  else if (p == NULL) {
    if (n==0) p = u8_malloc(sizeof(struct FD_COMPOUND));
    else p = u8_malloc(sizeof(struct FD_COMPOUND)+(n-1)*sizeof(lispval));}
  FD_INIT_CONS(p,fd_compound_type);
  if (ismutable) u8_init_mutex(&(p->compound_lock));
  p->compound_typetag = fd_incref(tag);
  p->compound_ismutable = ismutable;
  p->compound_isopaque = 0;
  p->compound_length = n;
  if (n>0) {
    write = &(p->compound_0); limit = write+n;
    va_start(args,n);
    while (write<limit) {
      lispval value = va_arg(args,lispval);
      *write = value; write++;}
    va_end(args);
    if (FD_ABORTP(initfn)) {
      write = &(p->compound_0);
      while (write<limit) {fd_decref(*write); write++;}
      return initfn;}
    else return LISP_CONS(p);}
  else return LISP_CONS(p);
}

FD_EXPORT lispval fd_init_compound_from_elts
  (struct FD_COMPOUND *p,lispval tag,int ismutable,int n,lispval *elts)
{
  lispval *write, *limit, *read = elts, initfn = FD_FALSE;
  if (PRED_FALSE((n<0) || (n>=256)))
    return fd_type_error(_("positive byte"),"fd_init_compound_from_elts",
                         FD_SHORT2DTYPE(n));
  else if (p == NULL) {
    if (n==0)
      p = u8_malloc(sizeof(struct FD_COMPOUND));
    else p = u8_malloc(sizeof(struct FD_COMPOUND)+(n-1)*sizeof(lispval));}
  FD_INIT_CONS(p,fd_compound_type);
  if (ismutable) u8_init_mutex(&(p->compound_lock));
  p->compound_typetag = fd_incref(tag);
  p->compound_ismutable = ismutable;
  p->compound_length = n;
  p->compound_isopaque = 0;
  if (n>0) {
    write = &(p->compound_0); limit = write+n;
    while (write<limit) {
      *write = *read++; write++;}
    if (FD_ABORTP(initfn)) {
      write = &(p->compound_0);
      while (write<limit) {fd_decref(*write); write++;}
      return initfn;}
    else return LISP_CONS(p);}
  else return LISP_CONS(p);
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
*fd_register_compound(lispval symbol,lispval *datap,int *corep)
{
  struct FD_COMPOUND_TYPEINFO *scan, *newrec;
  u8_lock_mutex(&compound_registry_lock);
  scan = fd_compound_entries;
  while (scan)
    if (FD_EQ(scan->compound_typetag,symbol)) {
      if (datap) {
        lispval data = *datap;
        if (VOIDP(scan->compound_metadata)) {
          scan->compound_metadata = data;
          fd_incref(data);}
        else {
          lispval data = *datap; fd_decref(data);
          data = scan->compound_metadata;
          fd_incref(data);
          *datap = data;}}
      if (corep) {
        if (scan->compound_corelen<0)
          scan->compound_corelen = *corep;
        else *corep = scan->compound_corelen;}
      u8_unlock_mutex(&compound_registry_lock);
      return scan;}
    else scan = scan->compound_nextinfo;
  newrec = u8_alloc(struct FD_COMPOUND_TYPEINFO);
  memset(newrec,0,sizeof(struct FD_COMPOUND_TYPEINFO));
  if (datap) {
    lispval data = *datap;
    fd_incref(data);
    newrec->compound_metadata = data;}
  else newrec->compound_metadata = VOID;
  newrec->compound_corelen = ((corep)?(*corep):(-1));
  newrec->compound_nextinfo = fd_compound_entries;
  newrec->compound_typetag = symbol;
  newrec->compound_parser = NULL;
  newrec->compound_dumpfn = NULL; 
  newrec->compound_restorefn = NULL;
  newrec->compund_tablefns = NULL;
  fd_compound_entries = newrec;
  u8_unlock_mutex(&compound_registry_lock);
  return newrec;
}

FD_EXPORT struct FD_COMPOUND_TYPEINFO 
          *fd_declare_compound(lispval symbol,lispval data,int core_slots)
{
  struct FD_COMPOUND_TYPEINFO *scan, *newrec;
  u8_lock_mutex(&compound_registry_lock);
  scan = fd_compound_entries;
  while (scan)
    if (FD_EQ(scan->compound_typetag,symbol)) {
      if (!(VOIDP(data))) {
        lispval old_data = scan->compound_metadata;
        scan->compound_metadata = fd_incref(data);
        fd_decref(old_data);}
      if (core_slots>0) scan->compound_corelen = core_slots;
      u8_unlock_mutex(&compound_registry_lock);
      return scan;}
    else scan = scan->compound_nextinfo;
  newrec = u8_alloc(struct FD_COMPOUND_TYPEINFO);
  memset(newrec,0,sizeof(struct FD_COMPOUND_TYPEINFO));
  newrec->compound_metadata = data;
  newrec->compound_corelen = core_slots;
  newrec->compound_typetag = symbol;
  newrec->compound_nextinfo = fd_compound_entries; 
  newrec->compound_parser = NULL;
  newrec->compound_dumpfn = NULL;
  newrec->compound_restorefn = NULL;
  newrec->compund_tablefns = NULL;
  fd_compound_entries = newrec;
  u8_unlock_mutex(&compound_registry_lock);
  return newrec;
}

FD_EXPORT struct FD_COMPOUND_TYPEINFO *fd_lookup_compound(lispval symbol)
{
  struct FD_COMPOUND_TYPEINFO *scan = fd_compound_entries;
  while (scan)
    if (FD_EQ(scan->compound_typetag,symbol)) {
      return scan;}
    else scan = scan->compound_nextinfo;
  return NULL;
}

/* Utility functions (for debugging) */

FD_EXPORT fd_cons fd_cons_data(lispval x)
{
  unsigned long as_int = (x&(~0x3));
  void *as_addr = (void *) as_int;
  return (fd_cons) as_addr;
}

FD_EXPORT struct FD_PAIR *fd_pair_data(lispval x)
{
  return FD_CONSPTR(fd_pair,x);
}

FD_EXPORT int _fd_find_elt(lispval x,lispval *v,int n)
{
  int i = 0; while (i<n)
    if (v[i]==x) return i;
    else i++;
  return -1;
}

int fd_ptr_debug_density = 1;

FD_EXPORT void _fd_bad_pointer(lispval badx,u8_context cxt)
{
  u8_raise(fd_BadPtr,cxt,NULL);
}

fd_exception get_pointer_exception(lispval x)
{
  if (OIDP(x)) return _("BadOIDPtr");
  else if (CONSP(x)) return _("BadCONSPtr");
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

FD_EXPORT lispval fd_badptr_err(lispval result,u8_context cxt,u8_string details)
{
  fd_seterr( get_pointer_exception(result), cxt,
             details, FD_UINT2DTYPE(result) );
  return FD_ERROR;
}

/* Testing */

static U8_MAYBE_UNUSED int some_false(lispval arg)
{
  int some_false = 0;
  FD_DOELTS(elt,arg,count) {
    if (FALSEP(elt)) some_false = 1;}
  return some_false;
}


/* Initialization */

static struct FD_FLONUM flonum_consts[8];

static void
init_flonum_constant(u8_string name,double val,int off)
{
  struct FD_FLONUM *cons = &(flonum_consts[off]);
  lispval lptr = fd_init_flonum(cons,val);
  FD_MAKE_STATIC(lptr);
  fd_add_hashname(name,lptr);
}

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
    (NULL,VOID,1,9,
     fd_intern("COMPOUNDTYPE"),0,FD_INT(9),
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
  fd_add_hashname("#empty",EMPTY);
  fd_add_hashname("#dflt",FD_DEFAULT_VALUE);
  int const_off=0;
#ifdef M_PI
  init_flonum_constant("#pi",M_PI,const_off++);
#endif
#ifdef M_E
  init_flonum_constant("#e",M_E,const_off++);
#endif
#ifdef M_LOG2E
  init_flonum_constant("#log2e",M_LOG2E,const_off++);
#endif
#ifdef M_LOG10E
  init_flonum_constant("#log2e",M_LOG10E,const_off++);
#endif
#ifdef M_LN2
  init_flonum_constant("#ln2",M_LN2,const_off++);
#endif
#ifdef M_LN10
  init_flonum_constant("#ln10",M_LN10,const_off++);
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