/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/cons.h"

#include <libu8/u8printf.h>
#include <libu8/u8timefns.h>

#include <stdarg.h>
#include <math.h>

u8_condition kno_MallocFailed=_("malloc/realloc failed");
u8_condition kno_StringOverflow=_("allocating humongous string past limit");
u8_condition kno_StackOverflow=_("Scheme stack overflow");
u8_condition kno_TypeError=_("Type error"), kno_RangeError=_("Range error");
u8_condition kno_DisorderedRange=_("Disordered range");
u8_condition kno_DoubleGC=_("Freeing already freed CONS");
u8_condition kno_UsingFreedCons=_("Using freed CONS");
u8_condition kno_FreeingNonHeapCons=_("Freeing non-heap cons");
u8_mutex _kno_ptr_locks[KNO_N_PTRLOCKS];

u8_condition kno_BadPtr=_("bad lisp pointer");
u8_condition kno_NullPtr=_("NULL lisp pointer");

u8_string kno_type_names[KNO_TYPE_MAX];
kno_hashfn kno_hashfns[KNO_TYPE_MAX];
kno_checkfn kno_immediate_checkfns[KNO_MAX_IMMEDIATE_TYPES+4];

kno_copy_fn kno_copiers[KNO_TYPE_MAX];
kno_unparse_fn kno_unparsers[KNO_TYPE_MAX];
kno_dtype_fn kno_dtype_writers[KNO_TYPE_MAX];
kno_recycle_fn kno_recyclers[KNO_TYPE_MAX];
kno_compare_fn kno_comparators[KNO_TYPE_MAX];

kno_applyfn kno_applyfns[KNO_TYPE_MAX];
/* This is set if the type is a CONS with a FUNCTION header */
short kno_functionp[KNO_TYPE_MAX];

static u8_mutex constant_registry_lock;
int kno_n_constants = KNO_N_BUILTIN_CONSTANTS;

#ifndef KNO_BIGVEC_THRESHOLD
#define KNO_BIGVEC_THRESHOLD 10000
#endif
ssize_t kno_bigvec_threshold = KNO_BIGVEC_THRESHOLD;

ssize_t kno_max_strlen = -1;
int kno_check_utf8 = 0;

const char *kno_constant_names[256]={
  "#void","#f","#t","{}","()","#eof","#eod","#eox",
  "#bad_dtype","#bad_parse","#oom","#type_error","#range_error",
  "#error","#badptr","#throw","#break","#unbound",
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

KNO_EXPORT
lispval kno_register_constant(u8_string name)
{
  int i = 0;
  u8_lock_mutex(&constant_registry_lock);
  while (i<kno_n_constants) {
    if (strcasecmp(name,kno_constant_names[i])==0)
      return KNO_CONSTANT(i);
    else i++;}
  lispval constant=KNO_CONSTANT(i);
  if (kno_add_constname(name,constant)<0) {
    u8_seterr("ConstantConflict","kno_register_constant",
              u8_strdup(name));
    return KNO_ERROR;}
  else {
    kno_constant_names[kno_n_constants++]=name;
    return constant;}
}

static int validate_constant(lispval x)
{
  int num = (KNO_GET_IMMEDIATE(x,kno_constant_type));
  if ((num>=0) && (num<kno_n_constants) &&
      (kno_constant_names[num] != NULL))
    return 1;
  else return 0;
}

KNO_EXPORT
/* kno_check_immediate:
   Arguments: a list pointer
   Returns: 1 or 0 (an int)
   Checks an immediate pointer for validity.
*/
int kno_check_immediate(lispval x)
{
  int type = KNO_IMMEDIATE_TYPE(x);
  if (type<kno_next_immediate_type)
    if (kno_immediate_checkfns[type])
      return kno_immediate_checkfns[type](x);
    else return 1;
  else return 0;
}

/* CONS methods for external calls */

KNO_EXPORT void _KNO_INIT_CONS(void *vptr,kno_lisp_type type)
{
  kno_raw_cons ptr = (kno_raw_cons)vptr;
  KNO_INIT_CONS(ptr,type);
}
KNO_EXPORT void _KNO_INIT_FRESH_CONS(void *vptr,kno_lisp_type type)
{
  kno_raw_cons ptr = (kno_raw_cons)vptr;
  KNO_INIT_FRESH_CONS(ptr,type);
}
KNO_EXPORT void _KNO_INIT_STACK_CONS(void *vptr,kno_lisp_type type)
{
  kno_raw_cons ptr = (kno_raw_cons)vptr;
  KNO_INIT_STACK_CONS(ptr,type);
}
KNO_EXPORT void _KNO_INIT_STATIC_CONS(void *vptr,kno_lisp_type type)
{
  kno_raw_cons ptr = (kno_raw_cons)vptr;
  KNO_INIT_STATIC_CONS(ptr,type);
}
KNO_EXPORT void _KNO_SET_CONS_TYPE(void *vptr,kno_lisp_type type)
{
  kno_raw_cons ptr = (kno_raw_cons)vptr;
  KNO_SET_CONS_TYPE(ptr,type);
}
KNO_EXPORT void _KNO_SET_REFCOUNT(void *vptr,unsigned int count)
{
  kno_raw_cons ptr = (kno_raw_cons)vptr;
  KNO_SET_REFCOUNT(ptr,count);
}

KNO_EXPORT lispval _kno_incref_fn(lispval ptr)
{
  return kno_incref(ptr);
}

KNO_EXPORT void _kno_decref_fn(lispval ptr)
{
  kno_decref(ptr);
}

KNO_EXPORT
lispval *_kno_init_elts(lispval *elts,size_t n,lispval v)
{
  return kno_init_elts(elts,n,v);
}

KNO_EXPORT ssize_t _kno_incref_elts(const lispval *elts,size_t n)
{
  return kno_incref_elts(elts,n);
}

KNO_EXPORT ssize_t _kno_decref_elts(const lispval *elts,size_t n)
{
  return kno_decref_elts(elts,n);
}
KNO_EXPORT ssize_t _kno_free_elts(lispval *elts,size_t n)
{
  return kno_free_elts(elts,n);
}

KNO_EXPORT
/* lispval_equal:
   Arguments: two dtype pointers
   Returns: 1 or 0 (an int)
   Returns 1 if the two objects are equal. */
int lispval_equal(lispval x,lispval y)
{
  if (ATOMICP(x)) return (x == y);
  else if (ATOMICP(y)) return (x == y);
  else if ((KNO_CONS_DATA(x)) == (KNO_CONS_DATA(y))) return 1;
  else if ((PRECHOICEP(x)) || (PRECHOICEP(y))) {
    int convert_x = PRECHOICEP(x), convert_y = PRECHOICEP(y);
    lispval cx = ((convert_x) ? (kno_make_simple_choice(x)) : (x));
    lispval cy = ((convert_y) ? (kno_make_simple_choice(y)) : (y));
    int result = lispval_equal(cx,cy);
    if (convert_x) kno_decref(cx);
    if (convert_y) kno_decref(cy);
    return result;}
  else if ((NUMBERP(x)) && (NUMBERP(y)))
    if (kno_numcompare(x,y)==0)
      return 1;
    else return 0;
  else if ((PACKETP(x))&&(PACKETP(y))) {
    size_t xlen=KNO_PACKET_LENGTH(x), ylen=KNO_PACKET_LENGTH(y);
    if (((xlen)) != (ylen))
      return 0;
    else if (memcmp(KNO_PACKET_DATA(x),KNO_PACKET_DATA(y),xlen)==0)
      return 1;
    else return 0;}
  else if (!(TYPEP(y,KNO_LISP_TYPE(x))))
    /* At this point, If the types are different, the values are
       different. */
    return 0;
  else if (PAIRP(x))
    if (LISP_EQUAL(KNO_CAR(x),KNO_CAR(y)))
      return (LISP_EQUAL(KNO_CDR(x),KNO_CDR(y)));
    else return 0;
  else if (STRINGP(x))
    if ((STRLEN(x)) != (STRLEN(y))) return 0;
    else if (strncmp(CSTRING(x),CSTRING(y),STRLEN(x)) == 0)
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
  else if ( (CHOICEP(x)) && (CHOICEP(y)) ) {
    struct KNO_CHOICE *cx = (kno_choice) x, *cy = (kno_choice) y;
    if (cx->choice_size != cy->choice_size) return 0;
    else {
      int i = 0, len = cx->choice_size;
      const lispval *xdata = KNO_CHOICE_ELTS(x), *ydata = KNO_CHOICE_ELTS(y);
      while (i<len) {
        if (LISP_EQUAL(xdata[i],ydata[i]))
          i++;
        else return 0;}
      return 1;}}
  else if ( (CHOICEP(x)) || (CHOICEP(y)) )
    return 0;
  else {
    kno_lisp_type ctype = KNO_CONS_TYPE(KNO_CONS_DATA(x));
    if (kno_comparators[ctype])
      return (kno_comparators[ctype](x,y,1)==0);
    else return 0;}
}

/* Strings */

KNO_EXPORT
/* kno_init_string:
   Arguments: A pointer to an KNO_STRING struct, a length, and a byte vector.
   Returns: a lisp string
   This returns a lisp string object from a character string.
   If the structure pointer is NULL, one is mallocd.
   If the length is negative, it is computed. */
lispval kno_init_string(struct KNO_STRING *ptr,int slen,u8_string string)
{
  int len = ((slen<0) ? (strlen(string)) : (slen));
  if (ptr == NULL) {
    ptr = u8_alloc(struct KNO_STRING);
    KNO_INIT_STRUCT(ptr,struct KNO_STRING);
    ptr->str_freebytes = 1;}
  KNO_INIT_CONS(ptr,kno_string_type);
  if ((len==0) && (string == NULL)) {
    u8_byte *bytes = u8_malloc(1); *bytes='\0';
    string = (u8_string)bytes;}
  ptr->str_bytelen = len;
  ptr->str_bytes = string;
  return LISP_CONS(ptr);
}

KNO_EXPORT
/* kno_extract_string:
   Arguments: A pointer to an KNO_STRING struct, and two pointers to ut8-strings
   Returns: a lisp string
   This returns a lisp string object from a region of a character string.
   If the structure pointer is NULL, one is mallocd.
   This copies the region between the pointers into a string and initializes
   a lisp string based on the region.
   If the second argument is NULL, the end of the first argument is used. */
lispval kno_extract_string(struct KNO_STRING *ptr,u8_string start,u8_string end)
{
  ssize_t length = ((end == NULL) ? (strlen(start)) : (end-start));
  if ((length>=0)&&((kno_max_strlen<0)||(length<kno_max_strlen))) {
    u8_byte *bytes = NULL; int freedata = 1;
    if (ptr == NULL) {
      ptr = u8_malloc(KNO_STRING_LEN+length+1);
      bytes = ((u8_byte *)ptr)+KNO_STRING_LEN;
      memcpy(bytes,start,length); bytes[length]='\0';
      freedata = 0;}
    else bytes = (u8_byte *)u8_strndup(start,length+1);
    KNO_INIT_CONS(ptr,kno_string_type);
    ptr->str_bytelen = length;
    ptr->str_bytes = bytes;
    ptr->str_freebytes = freedata;
    if ( (kno_check_utf8) && (!(u8_validp(bytes))) ) {
      KNO_SET_CONS_TYPE(ptr,kno_packet_type);
      return kno_err("InvalidUTF8","kno_extract_string",NULL,LISP_CONS(ptr));}
    else return LISP_CONS(ptr);}
  else return kno_err(kno_StringOverflow,"kno_extract_string",NULL,VOID);
}

KNO_EXPORT
/* kno_substring:
   Arguments: two pointers to utf-8 strings
   Returns: a lisp string
   This returns a lisp string object from a region of a character string.
   If the structure pointer is NULL, one is mallocd.
   This copies the region between the pointers into a string and initializes
   a lisp string based on the region. */
lispval kno_substring(u8_string start,u8_string end)
{
  ssize_t length = ((end == NULL) ? (strlen(start)) : (end-start));
  if ((length>=0)&&((kno_max_strlen<0)||(length<kno_max_strlen))) {
    struct KNO_STRING *ptr = u8_malloc(KNO_STRING_LEN+length+1);
    u8_byte *bytes = ((u8_byte *)ptr)+KNO_STRING_LEN;
    memcpy(bytes,start,length); bytes[length]='\0';
    KNO_INIT_FRESH_CONS(ptr,kno_string_type);
    ptr->str_bytelen = length;
    ptr->str_bytes = bytes;
    ptr->str_freebytes = 0;
    if ( (kno_check_utf8) && (!(u8_validp(bytes))) ) {
      KNO_SET_CONS_TYPE(ptr,kno_packet_type);
      return kno_err("InvalidUTF8","kno_extract_string",NULL,LISP_CONS(ptr));}
    else return LISP_CONS(ptr);}
  else return kno_err(kno_StringOverflow,"kno_substring",NULL,VOID);
}

KNO_EXPORT
/* kno_make_string:
   Arguments: A pointer to an KNO_STRING struct, a length, and a pointer to a byte vector
   Returns: a lisp string
   This returns a lisp string object from a string, copying the string
   If the structure pointer is NULL, the lisp string is uniconsed, so that
   the string data is contiguous with the struct. */
lispval kno_make_string(struct KNO_STRING *ptr,int len,u8_string string)
{
  int length = ((len>=0)?(len):(strlen(string)));
  u8_byte *bytes = NULL; int freedata = 1;
  if (ptr == NULL) {
    ptr = u8_malloc(KNO_STRING_LEN+length+1);
    bytes = ((u8_byte *)ptr)+KNO_STRING_LEN;
    if (string) memcpy(bytes,string,length);
    else memset(bytes,'?',length);
    bytes[length]='\0';
    freedata = 0;}
  else {
    bytes = u8_malloc(length+1);
    memcpy(bytes,string,length); bytes[length]='\0';}
  KNO_INIT_CONS(ptr,kno_string_type);
  ptr->str_bytelen = length;
  ptr->str_bytes = bytes;
  ptr->str_freebytes = freedata;
  if ( (kno_check_utf8) && (!(u8_validp(bytes))) ) {
    KNO_SET_CONS_TYPE(ptr,kno_packet_type);
    return kno_err("InvalidUTF8","kno_extract_string",NULL,LISP_CONS(ptr));}
  else return LISP_CONS(ptr);
}

KNO_EXPORT
/* kno_block_string:
   Arguments: a length, and a pointer to a byte vector
   Returns: a lisp string
   This returns a uniconsed lisp string object from a string,
   copying and freeing the string data
*/
lispval kno_block_string(int len,u8_string string)
{
  u8_byte *bytes = NULL;
  int length = ((len>=0)?(len):(strlen(string)));
  struct KNO_STRING *ptr = u8_malloc(KNO_STRING_LEN+length+1);
  bytes = ((u8_byte *)ptr)+KNO_STRING_LEN;
  if (string) memcpy(bytes,string,length);
  else memset(bytes,'?',length);
  bytes[length]='\0';
  KNO_INIT_CONS(ptr,kno_string_type);
  ptr->str_bytelen = length; ptr->str_bytes = bytes; ptr->str_freebytes = 0;
  if (string) u8_free(string);
  if ( (kno_check_utf8) && (!(u8_validp(bytes))) ) {
    KNO_SET_CONS_TYPE(ptr,kno_packet_type);
    return kno_err("InvalidUTF8","kno_extract_string",NULL,LISP_CONS(ptr));}
  else return LISP_CONS(ptr);
  return LISP_CONS(ptr);
}

KNO_EXPORT
/* kno_mkstring:
   Arguments: a C string (u8_string)
   Returns: a lisp string
*/
lispval kno_mkstring(u8_string string)
{
  return kno_make_string(NULL,-1,string);
}

/* Pairs */

KNO_EXPORT lispval kno_init_pair(struct KNO_PAIR *ptr,lispval car,lispval cdr)
{
  if (ptr == NULL) ptr = u8_alloc(struct KNO_PAIR);
  KNO_INIT_CONS(ptr,kno_pair_type);
  ptr->car = car; ptr->cdr = cdr;
  return LISP_CONS(ptr);
}

KNO_EXPORT lispval kno_make_pair(lispval car,lispval cdr)
{
  return kno_init_pair(NULL,kno_incref(car),kno_incref(cdr));
}

KNO_EXPORT lispval kno_make_list(int len,...)
{
  va_list args; int i = 0;
  lispval *elts = u8_alloc_n(len,lispval), result = NIL;
  va_start(args,len);
  while (i<len) elts[i++]=va_arg(args,lispval);
  va_end(args);
  i = len-1; while (i>=0) {
    result = kno_init_pair(NULL,elts[i],result); i--;}
  u8_free(elts);
  return result;
}

KNO_EXPORT int kno_list_length(lispval l)
{
  int len = 0; lispval scan = l; while (PAIRP(scan)) {
    len++; scan = KNO_CDR(scan);}
  if (NILP(scan)) return len;
  else return -len;
}

KNO_EXPORT lispval kno_reverse_list(lispval l)
{
  lispval result = KNO_EMPTY_LIST, scan=l;
  while (PAIRP(scan)) {
    lispval car  = KNO_CAR(scan); kno_incref(car);
    result = kno_init_pair(NULL,car,result);
    scan = KNO_CDR(scan);}
  if (NILP(scan))
    return result;
  else {
    kno_decref(result);
    return kno_err("ImproperList","kno_reverse_list",NULL,l);}
}

/* Vectors */

KNO_EXPORT lispval kno_cons_vector(struct KNO_VECTOR *ptr,
                                   int len,int big_alloc_elts,
                                   lispval *data)
{
  lispval *elts; int free_data = 1; int big_alloc = 0;
  if (len<0)
    return kno_err("NegativeLength","kno_cons_vector",NULL,KNO_INT(len));
  else if ((ptr == NULL)&&(data == NULL)) {
    int i = 0;
    if ( len > kno_bigvec_threshold) {
      ptr = u8_big_alloc(KNO_VECTOR_LEN+(LISPVEC_BYTELEN(len)));
      big_alloc=1;}
    else ptr = u8_malloc(KNO_VECTOR_LEN+(LISPVEC_BYTELEN(len)));
    elts = ((lispval *)(((unsigned char *)ptr)+KNO_VECTOR_LEN));
    while (i < len) elts[i++]=VOID;
    free_data = 0;}
  else if (ptr == NULL) {
    ptr = u8_alloc(struct KNO_VECTOR);
    elts = data;}
  else if (data == NULL) {
    int i = 0; elts = u8_malloc(LISPVEC_BYTELEN(len));
    while (i<len) elts[i]=VOID;
    free_data = 1;}
  else elts = data;
  KNO_INIT_CONS(ptr,kno_vector_type);
  ptr->vec_length = len;
  ptr->vec_elts = elts;
  ptr->vec_free_elts = free_data;
  ptr->vec_bigalloc  = big_alloc;
  ptr->vec_bigalloc_elts  = big_alloc_elts;
  return LISP_CONS(ptr);
}

KNO_EXPORT lispval kno_init_vector(struct KNO_VECTOR *ptr,int len,lispval *data)
{
  return kno_cons_vector(ptr,len,0,data);
}

KNO_EXPORT lispval kno_empty_vector(int len)
{
  return kno_cons_vector(NULL,len,0,NULL);
}

KNO_EXPORT lispval kno_wrap_vector(int len,lispval *data)
{
  return kno_cons_vector(NULL,len,0,data);
}

KNO_EXPORT lispval kno_fill_vector(int len,lispval init_elt)
{
  lispval vec = kno_cons_vector(NULL,len,0,NULL);
  int i = 0; while (i < len) {
    KNO_VECTOR_SET(vec,i,init_elt);
    kno_incref(init_elt);
    i++;}
  return vec;
}

KNO_EXPORT lispval kno_make_nvector(int len,...)
{
  va_list args; int i = 0;
  lispval result, *elts;
  if (len<0)
    return kno_err("NegativeLength","kno_make_nvector",NULL,KNO_INT(len));
  va_start(args,len);
  result = kno_empty_vector(len);
  elts = KNO_VECTOR_ELTS(result);
  while (i<len) elts[i++]=va_arg(args,lispval);
  va_end(args);
  return result;
}

KNO_EXPORT lispval kno_make_vector(int len,lispval *data)
{
  int i = 0;
  if (len<0)
    return kno_err("NegativeLength","kno_make_vector",NULL,KNO_INT(len));
  int use_big_alloc = (len > kno_bigvec_threshold);
  struct KNO_VECTOR *ptr =
    (use_big_alloc) ?
    (u8_big_alloc(KNO_VECTOR_LEN+(LISPVEC_BYTELEN(len)))) :
    (u8_malloc(KNO_VECTOR_LEN+(LISPVEC_BYTELEN(len))));
  lispval *elts = ((lispval *)(((unsigned char *)ptr)+KNO_VECTOR_LEN));
  KNO_INIT_CONS(ptr,kno_vector_type);
  ptr->vec_length = len;
  ptr->vec_elts = elts;
  ptr->vec_free_elts = 0;
  ptr->vec_bigalloc = use_big_alloc;
  if (data) {
    while (i < len) {elts[i]=data[i]; i++;}}
  else {while (i < len) {elts[i]=VOID; i++;}}
  return LISP_CONS(ptr);
}

/* Packets */

KNO_EXPORT lispval kno_init_packet
(struct KNO_STRING *ptr,int len,const unsigned char *data)
{
  if (len<0)
    return kno_err("NegativeLength","kno_init_packet",NULL,KNO_INT(len));
  else if ((ptr == NULL)&&(data == NULL))
    return kno_make_packet(ptr,len,data);
  if (ptr == NULL) {
    ptr = u8_alloc(struct KNO_STRING);
    ptr->str_freebytes = 1;}
  KNO_INIT_CONS(ptr,kno_packet_type);
  if (data == NULL) {
    u8_byte *consed = u8_malloc(len+1);
    memset(consed,0,len+1);
    data = consed;}
  ptr->str_bytelen = len; ptr->str_bytes = data;
  return LISP_CONS(ptr);
}

KNO_EXPORT lispval kno_make_packet
(struct KNO_STRING *ptr,int len,const unsigned char *data)
{
  u8_byte *bytes = NULL; int freedata = 1;
  if (len<0)
    return kno_err("NegativeLength","kno_make_packet",NULL,KNO_INT(len));
  else if (ptr == NULL) {
    ptr = u8_malloc(KNO_STRING_LEN+len+1);
    bytes = ((u8_byte *)ptr)+KNO_STRING_LEN;
    if (data) {
      memcpy(bytes,data,len);
      bytes[len]='\0';}
    else memset(bytes,0,len+1);
    freedata = 0;}
  else if (data == NULL) {
    bytes = u8_malloc(len+1);}
  else bytes = (unsigned char *)data;
  KNO_INIT_CONS(ptr,kno_packet_type);
  ptr->str_bytelen = len; ptr->str_bytes = bytes; ptr->str_freebytes = freedata;
  return LISP_CONS(ptr);
}

KNO_EXPORT lispval kno_bytes2packet
(struct KNO_STRING *use_ptr,int len,const unsigned char *data)
{
  struct KNO_STRING *ptr = NULL;
  u8_byte *bytes = NULL; int freedata = (data!=NULL);
  if (len<0)
    return kno_err("NegativeLength","kno_bytes2packet",NULL,KNO_INT(len));
  else if (use_ptr == NULL) {
    ptr = u8_malloc(KNO_STRING_LEN+len+1);
    bytes = ((u8_byte *)ptr)+KNO_STRING_LEN;
    if (data) {
      memcpy(bytes,data,len);
      bytes[len]='\0';}
    else memset(bytes,0,len+1);
    freedata = 0;}
  else {
    ptr = use_ptr;
    bytes = u8_malloc(len);
    if (data == NULL) memset(bytes,0,len);
    else memcpy(bytes,data,len);}
  KNO_INIT_CONS(ptr,kno_packet_type);
  ptr->str_bytelen = len;
  ptr->str_bytes = bytes;
  ptr->str_freebytes = freedata;
  return LISP_CONS(ptr);
}

/* Registering new primitive types */

static u8_mutex type_registry_lock;

unsigned int kno_next_cons_type=
  KNO_CONS_TYPECODE(KNO_BUILTIN_CONS_TYPES);
unsigned int kno_next_immediate_type=
  KNO_IMMEDIATE_TYPECODE(KNO_BUILTIN_IMMEDIATE_TYPES);

KNO_EXPORT int kno_register_cons_type(char *name)
{
  int typecode;
  u8_lock_mutex(&type_registry_lock);
  if (kno_next_cons_type>=KNO_MAX_CONS_TYPE) {
    u8_unlock_mutex(&type_registry_lock);
    return -1;}
  typecode = kno_next_cons_type;
  kno_next_cons_type++;
  kno_type_names[typecode]=name;
  u8_unlock_mutex(&type_registry_lock);
  return typecode;
}

KNO_EXPORT int kno_register_immediate_type(char *name,kno_checkfn fn)
{
  int typecode;
  u8_lock_mutex(&type_registry_lock);
  if (kno_next_immediate_type>=KNO_MAX_IMMEDIATE_TYPE) {
    u8_unlock_mutex(&type_registry_lock);
    return -1;}
  typecode = kno_next_immediate_type;
  kno_immediate_checkfns[typecode]=fn;
  kno_next_immediate_type++;
  kno_type_names[typecode]=name;
  u8_unlock_mutex(&type_registry_lock);
  return typecode;
}

/* Utility functions (for debugging) */

KNO_EXPORT kno_cons kno_cons_data(lispval x)
{
  unsigned long as_int = (x&(~0x3));
  void *as_addr = (void *) as_int;
  return (kno_cons) as_addr;
}

KNO_EXPORT struct KNO_PAIR *kno_pair_data(lispval x)
{
  return KNO_CONSPTR(kno_pair,x);
}

KNO_EXPORT int _kno_find_elt(lispval x,lispval *v,int n)
{
  int i = 0; while (i<n)
               if (v[i]==x) return i;
               else i++;
  return -1;
}

int kno_ptr_debug_density = 1;

KNO_EXPORT void _kno_bad_pointer(lispval badx,u8_context cxt)
{
  u8_raise(kno_BadPtr,cxt,NULL);
}

u8_condition get_pointer_exception(lispval x)
{
  if (KNO_NULLP(x))
    return _("NullPointer");
  else if (OIDP(x)) return _("BadOIDPtr");
  else if (CONSP(x)) return _("BadCONSPtr");
  else if (KNO_IMMEDIATEP(x)) {
    int ptype = KNO_IMMEDIATE_TYPE(x);
    if (ptype>=kno_next_immediate_type)
      return _("BadImmediateType");
    else switch (ptype) {
      case kno_symbol_type: return _("BadSymbol");
      case kno_constant_type: return _("BadConstant");
      case kno_fcnid_type: return _("BadFcnId");
      case kno_opcode_type: return _("BadOpcode");
      case kno_lexref_type: return _("BadLexref");
      default:
        return _("BadImmediate");}}
  else return kno_BadPtr;
}

KNO_EXPORT lispval kno_badptr_err(lispval result,u8_context cxt,
                                  u8_string details)
{
  if (errno) u8_graberrno(cxt,u8_strdup(details));
  return kno_err( get_pointer_exception(result), cxt,
                  details, KNO_UINT2LISP(result) );
}

/* Testing */

static U8_MAYBE_UNUSED int some_false(lispval arg)
{
  int some_false = 0;
  KNO_DOELTS(elt,arg,count) {
    if (FALSEP(elt)) some_false = 1;}
  return some_false;
}

/* Typeinfo */

static struct KNO_HASHTABLE typeinfo;

KNO_EXPORT struct KNO_TYPEINFO *kno_probe_typeinfo(lispval tag)
{
  lispval v = kno_hashtable_get(&typeinfo,tag,KNO_FALSE);
  if (KNO_TYPEP(v,kno_typeinfo_type))
    return (kno_typeinfo) v;
  else return NULL;
}

KNO_EXPORT struct KNO_TYPEINFO *kno_use_typeinfo(lispval tag)
{
  lispval exists = kno_hashtable_get(&typeinfo,tag,KNO_VOID);
  if (KNO_VOIDP(exists)) {
    struct KNO_TYPEINFO *info = u8_alloc(struct KNO_TYPEINFO);
    KNO_INIT_STATIC_CONS(info,kno_typeinfo_type);
    info->typetag = tag; kno_incref(tag);
    info->type_props = kno_make_slotmap(2,0,NULL);
    info->type_handlers = kno_make_slotmap(2,0,NULL);
    info->type_name = (KNO_SYMBOLP(tag)) ? (KNO_SYMBOL_NAME(tag)) :
      (KNO_STRINGP(tag)) ? (KNO_CSTRING(tag)) : (kno_lisp2string(tag));
    info->type_description = NULL;
    int rv = kno_hashtable_op(&typeinfo,kno_table_init,tag,((lispval)info));
    if (rv > 0)
      return info;
    else {
      kno_decref(info->typetag);
      kno_decref(info->type_props);
      kno_decref(info->type_handlers);
      u8_free(info);
      if (rv < 0)
	return NULL;
      else {
	lispval useval = kno_hashtable_get(&typeinfo,tag,KNO_FALSE);
	if (KNO_TYPEP(useval,kno_typeinfo_type))
	  return (kno_typeinfo) useval;
	else return NULL;}}}
  else return (kno_typeinfo) exists;
}

KNO_EXPORT
int kno_set_unparsefn(lispval tag,kno_type_unparsefn fn)
{
  struct KNO_TYPEINFO *info = kno_use_typeinfo(tag);
  if (info) {
    info->type_unparsefn = fn;
    return 1;}
  else return 0;
}

KNO_EXPORT
int kno_set_freefn(lispval tag,kno_type_freefn fn)
{
  struct KNO_TYPEINFO *info = kno_use_typeinfo(tag);
  if (info) {
    info->type_freefn = fn;
    return 1;}
  else return 0;
}

KNO_EXPORT
int kno_set_parsefn(lispval tag,kno_type_parsefn fn)
{
  struct KNO_TYPEINFO *info = kno_use_typeinfo(tag);
  if (info) {
    info->type_parsefn = fn;
    return 1;}
  else return 0;
}

KNO_EXPORT
int kno_set_dumpfn(lispval tag,kno_type_dumpfn fn)
{
  struct KNO_TYPEINFO *info = kno_use_typeinfo(tag);
  if (info) {
    info->type_dumpfn = fn;
    return 1;}
  else return 0;
}

KNO_EXPORT
int kno_set_restorefn(lispval tag,kno_type_restorefn fn)
{
  struct KNO_TYPEINFO *info = kno_use_typeinfo(tag);
  if (info) {
    info->type_restorefn = fn;
    return 1;}
  else return 0;
}

/* Initialization */

static struct KNO_FLONUM flonum_consts[8];

static void
init_flonum_constant(u8_string name,double val,int off)
{
  struct KNO_FLONUM *cons = &(flonum_consts[off]);
  lispval lptr = kno_init_flonum(cons,val);
  KNO_MAKE_STATIC(lptr);
  kno_add_constname(name,lptr);
}

void kno_init_cons_c()
{
  int i;
  i = 0; while (i < KNO_N_PTRLOCKS) {
    u8_init_mutex(&_kno_ptr_locks[i]);
    i++;}

  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&constant_registry_lock);
  u8_init_mutex(&type_registry_lock);

  kno_init_hashtable(&typeinfo,223,NULL);

  i = 0; while (i < KNO_TYPE_MAX) kno_type_names[i++]=NULL;
  i = 0; while (i<KNO_TYPE_MAX) kno_hashfns[i++]=NULL;
  i = 0; while (i<KNO_MAX_IMMEDIATE_TYPES+4)
           kno_immediate_checkfns[i++]=NULL;

  i = 0; while (i < KNO_TYPE_MAX) kno_recyclers[i++]=NULL;
  i = 0; while (i < KNO_TYPE_MAX) kno_unparsers[i++]=NULL;
  i = 0; while (i < KNO_TYPE_MAX) kno_dtype_writers[i++]=NULL;
  i = 0; while (i < KNO_TYPE_MAX) kno_comparators[i++]=NULL;
  i = 0; while (i < KNO_TYPE_MAX) kno_copiers[i++]=NULL;

  kno_immediate_checkfns[kno_constant_type]=validate_constant;

  i=0; while (i<256) {
    if (kno_constant_names[i]) {
      kno_add_constname(kno_constant_names[i],KNO_CONSTANT(i));}
    i++;}
  kno_add_constname("#true",KNO_TRUE);
  kno_add_constname("#false",KNO_FALSE);
  kno_add_constname("#empty",EMPTY);
  kno_add_constname("#dflt",KNO_DEFAULT_VALUE);
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
  kno_add_constname("#answer",KNO_INT(42));
  kno_add_constname("#life",KNO_INT(42));
  kno_add_constname("#universe",KNO_INT(42));
  kno_add_constname("#everything",KNO_INT(42));

#define ONEK ((unsigned long long)1024)

  kno_add_constname("#kib",KNO_INT(ONEK));
  kno_add_constname("#1kib",KNO_INT(ONEK));
  kno_add_constname("#2kib",KNO_INT(2*ONEK));
  kno_add_constname("#4kib",KNO_INT(4*ONEK));
  kno_add_constname("#8kib",KNO_INT(8*ONEK));
  kno_add_constname("#16kib",KNO_INT(16*ONEK));
  kno_add_constname("#32kib",KNO_INT(32*ONEK));
  kno_add_constname("#64kib",KNO_INT(64*ONEK));
  kno_add_constname("#mib",KNO_INT((ONEK)*(ONEK)));
  kno_add_constname("#1mib",KNO_INT((ONEK)*(ONEK)));
  kno_add_constname("#2mib",KNO_INT((2)*(ONEK)*(ONEK)));
  kno_add_constname("#3mib",KNO_INT((3)*(ONEK)*(ONEK)));
  kno_add_constname("#4mib",KNO_INT((4)*(ONEK)*(ONEK)));
  kno_add_constname("#5mib",KNO_INT((5)*(ONEK)*(ONEK)));
  kno_add_constname("#6mib",KNO_INT((6)*(ONEK)*(ONEK)));
  kno_add_constname("#7mib",KNO_INT((7)*(ONEK)*(ONEK)));
  kno_add_constname("#8mib",KNO_INT((8)*(ONEK)*(ONEK)));
  kno_add_constname("#gib",KNO_INT((1024*1024*1024)));
  kno_add_constname("#1gib",KNO_INT(((ONEK)*(ONEK)*(ONEK))));
  kno_add_constname("#2gib",KNO_INT((2*(ONEK)*(ONEK)*(ONEK))));
  kno_add_constname("#3gib",KNO_INT((3*(ONEK)*(ONEK)*(ONEK))));
  kno_add_constname("#4gib",KNO_INT((4*(ONEK)*(ONEK)*(ONEK))));
  kno_add_constname("#5gib",KNO_INT((5*(ONEK)*(ONEK)*(ONEK))));
  kno_add_constname("#6gib",KNO_INT((6*(ONEK)*(ONEK)*(ONEK))));
  kno_add_constname("#7gib",KNO_INT((7*(ONEK)*(ONEK)*(ONEK))));
  kno_add_constname("#8gib",KNO_INT((8*(ONEK)*(ONEK)*(ONEK))));
  kno_add_constname("#1mib",KNO_INT((1024*1024)));
  kno_add_constname("#2mib",KNO_INT((2*1024*1024)));

  lispval plugh = kno_register_constant("plugh");
  if (plugh != kno_register_constant("plugh"))
    u8_log(LOGERR,"RegisterConstant","Doesn't handle repeated calls");

}

