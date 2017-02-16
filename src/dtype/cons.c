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

/* For sprintf */
#include <stdio.h>

fd_exception fd_MallocFailed=_("malloc/realloc failed");
fd_exception fd_StringOverflow=_("allocating humongous string past limit");
fd_exception fd_StackOverflow=_("Scheme stack overflow");
fd_exception fd_TypeError=_("Type error"), fd_RangeError=_("Range error");
fd_exception fd_BadPtr=_("bad dtype pointer");
fd_exception fd_DoubleGC=_("Freeing already freed CONS");
fd_exception fd_UsingFreedCons=_("Using freed CONS");
fd_exception fd_FreeingNonHeapCons=_("Freeing non-heap cons");
#if FD_THREADS_ENABLED
u8_mutex _fd_ptr_locks[FD_N_PTRLOCKS];
#endif

u8_string fd_type_names[FD_TYPE_MAX];
fd_recycle_fn fd_recyclers[FD_TYPE_MAX];
fd_unparse_fn fd_unparsers[FD_TYPE_MAX];
fd_dtype_fn fd_dtype_writers[FD_TYPE_MAX];
fd_compare_fn fd_comparators[FD_TYPE_MAX];
fd_copy_fn fd_copiers[FD_TYPE_MAX];
fd_hashfn fd_hashfns[FD_TYPE_MAX];
fd_checkfn fd_immediate_checkfns[FD_MAX_IMMEDIATE_TYPES+4];

#if FD_THREADS_ENABLED
static u8_mutex constant_registry_lock;
#endif
int fd_n_constants=FD_N_BUILTIN_CONSTANTS;

ssize_t fd_max_strlen=-1;

const char *fd_constant_names[]={
  "#?","#f","#t","{}","()","#eof","#eod","#eox",
  "#baddtype","badparse","#oom","#typeerror","#rangeerror",
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
  int i=0;
  u8_lock_mutex(&constant_registry_lock);
  while (i<fd_n_constants) {
    if (strcasecmp(name,fd_constant_names[i])==0)
      return FD_CONSTANT(i);
    else i++;}
  fd_constant_names[fd_n_constants++]=name;
  return FD_CONSTANT(i);
}

static int validate_constant(fdtype x)
{
  int num=(FD_GET_IMMEDIATE(x,fd_constant_type));
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
  int type=FD_IMMEDIATE_TYPE(x);
  if (type<fd_next_immediate_type)
    if (fd_immediate_checkfns[type])
      return fd_immediate_checkfns[type](x);
    else return 1;
  else return 0;
}

/* Other methods */

FD_EXPORT
/* fd_recycle_cons:
    Arguments: a pointer to an FD_CONS struct
    Returns: void
 Recycles a cons cell */
void fd_recycle_cons(fd_cons c)
{
  int ctype=FD_CONS_TYPE(c);
  switch (ctype) {
    case fd_rational_type:
    case fd_complex_type:
      if (fd_recyclers[ctype]) {
        fd_recyclers[ctype](c);
        return;}
    case fd_pair_type: {
      /* This is hairy in order to iteratively free up long lists. */
      struct FD_PAIR *p=(struct FD_PAIR *)c;
      fdtype cdr=p->fd_cdr, car=p->fd_car;
      u8_free(p); fd_decref(car);
      if (FD_CONSP(cdr)) {
        if (FD_PAIRP(cdr)) {
          struct FD_PAIR *xcdr=(struct FD_PAIR *)cdr;
          FD_LOCK_PTR(xcdr);
          while (FD_CONS_REFCOUNT(xcdr)==1) {
            car=xcdr->fd_car; cdr=xcdr->fd_cdr;
            u8_free(xcdr);
            FD_UNLOCK_PTR(xcdr);
            fd_decref(car);
            if (FD_PAIRP(cdr)) {
              xcdr=(fd_pair)cdr;
              FD_LOCK_PTR(xcdr);
              continue;}
            else {xcdr=NULL; break;}}
          if (xcdr) FD_UNLOCK_PTR(xcdr);
          fd_decref(cdr);}
        else fd_decref(cdr);}
      break;}
    case fd_string_type: case fd_packet_type: case fd_secret_type: {
      struct FD_STRING *s=(struct FD_STRING *)c;
      if ((s->fd_bytes)&&(s->fd_freedata)) u8_free(s->fd_bytes);
      if (!(FD_STATIC_CONSP(s))) u8_free(s);
      break;}
    case fd_vector_type: case fd_rail_type: {
      struct FD_VECTOR *v=(struct FD_VECTOR *)c;
      int len=v->fd_veclen; fdtype *scan=v->fd_vecelts, *limit=scan+len;
      if (scan) {
        while (scan<limit) {fd_decref(*scan); scan++;}
        if (v->fd_freedata) u8_free(v->fd_vecelts);}
      if (!(FD_STATIC_CONSP(v))) u8_free(v);
      break;}
    case fd_choice_type: {
      struct FD_CHOICE *cv=(struct FD_CHOICE *)c;
      int len=cv->choice_size, atomicp=cv->choice_isatomic;
      const fdtype *scan=FD_XCHOICE_DATA(cv), *limit=scan+len;
      if (scan == NULL) break;
      if (!(atomicp)) while (scan<limit) {fd_decref(*scan); scan++;}
      if (!(FD_STATIC_CONSP(cv))) u8_free(cv);
      break;}
    case fd_qchoice_type: {
      struct FD_QCHOICE *qc=(struct FD_QCHOICE *)c;
      fd_decref(qc->fd_choiceval);
      if (!(FD_STATIC_CONSP(qc))) u8_free(qc);
      break;}
    default: {
      if (fd_recyclers[ctype]) fd_recyclers[ctype](c);}
    }
}

FD_EXPORT
/* Frees a vector of LISP pointers */
void fd_free_vec(fdtype *vec,int n,int free_vec)
{
  int i=0; while (i<n) {
    fdtype elt=vec[i++];
    fd_decref(elt);}
  if (free_vec) u8_free(vec);
}

FD_EXPORT
/* fdtype_equal:
    Arguments: two dtype pointers
    Returns: 1 or 0 (an int)
  Returns 1 if the two objects are equal. */
int fdtype_equal(fdtype x,fdtype y)
{
  if (FD_ATOMICP(x)) return (x==y);
  else if (FD_ATOMICP(y)) return (x==y);
  else if ((FD_CONS_DATA(x)) == (FD_CONS_DATA(y))) return 1;
  else if ((FD_ACHOICEP(x)) || (FD_ACHOICEP(y))) {
    int convert_x=FD_ACHOICEP(x), convert_y=FD_ACHOICEP(y);
    fdtype cx=((convert_x) ? (fd_make_simple_choice(x)) : (x));
    fdtype cy=((convert_y) ? (fd_make_simple_choice(y)) : (y));
    int result=fdtype_equal(cx,cy);
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
      int i=0, len=FD_VECTOR_LENGTH(x);
      fdtype *xdata=FD_VECTOR_DATA(x), *ydata=FD_VECTOR_DATA(y);
      while (i < len)
        if (FDTYPE_EQUAL(xdata[i],ydata[i])) i++; else return 0;
      return 1;}
  else if ((FD_NUMBERP(x)) && (FD_NUMBERP(y)))
    if (fd_numcompare(x,y)==0) return 1;
    else return 0;
  else {
    fd_ptr_type ctype=FD_CONS_TYPE(FD_CONS_DATA(x));
    if (fd_comparators[ctype])
      return (fd_comparators[ctype](x,y,1)==0);
    else return 0;}
}

FD_EXPORT
/* fdtype_compare:
    Arguments: two dtype pointers
    Returns: 1, 0, or -1 (an int)
  Returns a function corresponding to a generic sort of two dtype pointers. */
int fdtype_compare(fdtype x,fdtype y,int quick)
{

  /* This is just defined for this function */
#define DOCOMPARE(x,y) \
  ((quick) ? (FD_QCOMPARE(x,y)) : (FDTYPE_COMPARE(x,y)))

  if (x == y) return 0;
  else if ((quick) && (FD_ATOMICP(x)))
    if (FD_ATOMICP(y))
      if (x>y) return 1; else if (x<y) return -1; else return 0;
    else return -1;
  else if ((quick) && (FD_ATOMICP(y))) return 1;
  else if ((FD_FIXNUMP(x)) && (FD_FIXNUMP(y))) {
    int xval=FD_FIX2INT(x), yval=FD_FIX2INT(y);
    /* The == case is handled by the x==y above. */
    if (xval<yval) return -1; else return 1;}
  else if ((FD_OIDP(x)) && (FD_OIDP(y))) {
    FD_OID xaddr=FD_OID_ADDR(x), yaddr=FD_OID_ADDR(y);
    return FD_OID_COMPARE(xaddr,yaddr);}
  else {
    fd_ptr_type xtype=FD_PTR_TYPE(x);
    fd_ptr_type ytype=FD_PTR_TYPE(y);
    if (FD_NUMBER_TYPEP(xtype))
      if (FD_NUMBER_TYPEP(ytype))
        return fd_numcompare(x,y);
      else return -1;
    else if (FD_NUMBER_TYPEP(ytype))
      return 1;
    else if ((FD_ACHOICEP(x))&&(FD_ACHOICEP(y))) {
      fdtype sx=fd_make_simple_choice(x);
      fdtype sy=fd_make_simple_choice(y);
      int retval=DOCOMPARE(sx,sy);
      fd_decref(sx); fd_decref(sy);
      return retval;}
    else if (FD_ACHOICEP(x)) {
      fdtype sx=fd_make_simple_choice(x);
      int retval=DOCOMPARE(sx,y);
      fd_decref(sx);
      return retval;}
    else if (FD_ACHOICEP(y)) {
      fdtype sy=fd_make_simple_choice(y);
      int retval=DOCOMPARE(x,sy);
      fd_decref(sy);
      return retval;}
    else if (xtype<ytype) return -1;
    else if (xtype>ytype) return 1;
    else if (FD_CONSP(x))
      switch (xtype) {
      case fd_pair_type: {
        int car_cmp=DOCOMPARE(FD_CAR(x),FD_CAR(y));
        if (car_cmp == 0) return (DOCOMPARE(FD_CDR(x),FD_CDR(y)));
        else return car_cmp;}
      case fd_string_type: {
        int xlen=FD_STRLEN(x), ylen=FD_STRLEN(y);
        if (quick) {
          if (xlen>ylen) return 1; else if (xlen<ylen) return -1;}
        return strncmp(FD_STRDATA(x),FD_STRDATA(y),xlen);}
      case fd_packet_type: case fd_secret_type: {
        int xlen=FD_PACKET_LENGTH(x), ylen=FD_PACKET_LENGTH(y);
        if (quick) {
          if (xlen>ylen) return 1; else if (xlen<ylen) return -1;}
        return memcmp(FD_PACKET_DATA(x),FD_PACKET_DATA(y),xlen);}
      case fd_vector_type: case fd_rail_type: {
        int i=0, xlen=FD_VECTOR_LENGTH(x), ylen=FD_VECTOR_LENGTH(y), lim;
        fdtype *xdata=FD_VECTOR_DATA(x), *ydata=FD_VECTOR_DATA(y);
        if (quick) {
          if (xlen>ylen) return 1; else if (xlen<ylen) return -1;}
        if (xlen<ylen) lim=xlen; else lim=ylen;
        while (i < lim) {
          int cmp=DOCOMPARE(xdata[i],ydata[i]);
          if (cmp) return cmp; else i++;}
        if (quick)
          if (xlen<ylen) return -1;
          else if (ylen>xlen) return 1;
          else return 0;
        else return 0;}
      case fd_choice_type: {
        struct FD_CHOICE *xc=fd_consptr(struct FD_CHOICE *,x,fd_choice_type);
        struct FD_CHOICE *yc=fd_consptr(struct FD_CHOICE *,y,fd_choice_type);
        if ((quick) && (xc->choice_size>yc->choice_size)) return 1;
        else if ((quick) && (xc->choice_size<yc->choice_size)) return -1;
        else {
          int xlen=FD_XCHOICE_SIZE(xc), ylen=FD_XCHOICE_SIZE(yc);
          const fdtype *xscan=FD_XCHOICE_DATA(xc);
          const fdtype *yscan=FD_XCHOICE_DATA(yc), *xlim=xscan+xlen;
          if (ylen<xlen) xlim=xscan+ylen;
          while (xscan<xlim) {
            int cmp=DOCOMPARE(*xscan,*yscan);
            if (cmp) return cmp;
            xscan++; yscan++;}
          if (xlen<ylen) return -1;
          else if (xlen>ylen) return 1;
          else return 0;}}
      default: {
        fd_ptr_type ctype=FD_CONS_TYPE(FD_CONS_DATA(x));
        if (fd_comparators[ctype])
          return fd_comparators[ctype](x,y,quick);
        else if (x<y) return -1;
        else if (x>y) return 1;
        else return 0;}}
    else if (x<y) return -1;
    else return 1;}
#undef DOCOMPARE
}

FD_EXPORT
/* fd_deep_copy:
    Arguments: a dtype pointer
    Returns: a dtype pointer
  This returns a copy of its argument, recurring to sub objects. */
fdtype fd_copier(fdtype x,int flags)
{
  int static_copy=U8_BITP(flags,FD_STATIC_COPY);
  if (FD_ATOMICP(x)) return x;
  else {
    fd_ptr_type ctype=FD_CONS_TYPE(FD_CONS_DATA(x));
    switch (ctype) {
    case fd_pair_type: {
      fdtype result=FD_EMPTY_LIST, *tail=&result, scan=x;
      while (FD_TYPEP(scan,fd_pair_type)) {
        struct FD_PAIR *p=FD_CONSPTR(fd_pair,scan);
        struct FD_PAIR *newpair=u8_alloc(struct FD_PAIR);
        fdtype car=p->fd_car;
        FD_INIT_CONS(newpair,fd_pair_type);
        if (static_copy) {FD_MAKE_STATIC(result);}
        if (FD_CONSP(car)) {
          struct FD_CONS *c=(struct FD_CONS *)car;
          if ( U8_BITP(flags,FD_FULL_COPY) || FD_STATIC_CONSP(c) )
            newpair->fd_car=fd_copier(car,flags);
          else {fd_incref(car); newpair->fd_car=car;}}
        else {
          fd_incref(car); newpair->fd_car=car;}
        *tail=(fdtype)newpair;
        tail=&(newpair->fd_cdr);
        scan=p->fd_cdr;}
      if (FD_CONSP(scan))
        *tail=fd_copier(scan,flags);
      else *tail=scan;
      if (static_copy) {FD_MAKE_STATIC(result);}
      return result;}
    case fd_vector_type: case fd_rail_type: {
      struct FD_VECTOR *v=FD_CONSPTR(fd_vector,x);
      fdtype *olddata=v->fd_vecelts; int i=0, len=v->fd_veclen;
      fdtype result=((ctype==fd_vector_type)?
                     (fd_init_vector(NULL,len,NULL)):
                     (fd_init_rail(NULL,len,NULL)));
      fdtype *newdata=FD_VECTOR_ELTS(result);
      while (i<len) {
          fdtype v=olddata[i], newv=v;
          if (FD_CONSP(v)) {
            struct FD_CONS *c=(struct FD_CONS *)newv;
            if ((flags&FD_FULL_COPY)||(FD_STATIC_CONSP(c)))
              newv=fd_copier(newv,flags);
            else fd_incref(newv);}
          newdata[i++]=newv;}
      if (static_copy) {FD_MAKE_STATIC(result);}
      return result;}
    case fd_string_type: {
      struct FD_STRING *s=FD_CONSPTR(fd_string,x);
      fdtype result=fd_make_string(NULL,s->fd_bytelen,s->fd_bytes);
      if (static_copy) {FD_MAKE_STATIC(result);}
      return result;}
    case fd_packet_type: case fd_secret_type: {
      struct FD_STRING *s=FD_CONSPTR(fd_string,x);
      fdtype result;
      if (ctype==fd_secret_type) {
        result=fd_make_packet(NULL,s->fd_bytelen,s->fd_bytes);
        FD_SET_CONS_TYPE(result,fd_secret_type);
        return result;}
      else result=fd_make_packet(NULL,s->fd_bytelen,s->fd_bytes);
      if (static_copy) {FD_MAKE_STATIC(result);}
      return result;}
    case fd_choice_type: {
      int n=FD_CHOICE_SIZE(x);
      int flags=(FD_ATOMIC_CHOICEP(x))?
        (FD_CHOICE_ISATOMIC):
        (FD_CHOICE_ISCONSES);
      struct FD_CHOICE *copy=fd_alloc_choice(n);
      const fdtype *read=FD_CHOICE_DATA(x), *limit=read+n;
      fdtype *write=(fdtype *)&(copy->choice_0);
      fdtype result;
      if (FD_ATOMIC_CHOICEP(x))
        memcpy(write,read,sizeof(fdtype)*n);
      else if (flags&FD_FULL_COPY) while (read<limit) {
          fdtype v=*read++, c=fd_copier(v,flags); *write++=c;}
      else while (read<limit) {
          fdtype v=*read++, newv=v;
          if (FD_CONSP(newv)) {
            struct FD_CONS *c=(struct FD_CONS *)newv;
            if (FD_STATIC_CONSP(c))
              newv=fd_copier(newv,flags);
            else fd_incref(newv);}
          *write++=newv;}
      result=fd_init_choice(copy,n,NULL,flags);
      if (static_copy) {FD_MAKE_STATIC(result);}
      return result;}
    default:
      if (fd_copiers[ctype]) {
        fdtype copy=(fd_copiers[ctype])(x,flags);
        if ((static_copy)&&(copy!=x)) {FD_MAKE_STATIC(copy);}
        return copy;}
      else if (!(FD_MALLOCD_CONSP((fd_cons)x)))
        return fd_err(fd_NoMethod,"fd_copier/static",
                      fd_type_names[ctype],FD_VOID);
      else if ((flags)&(FD_STRICT_COPY))
        return fd_err(fd_NoMethod,"fd_copier",fd_type_names[ctype],x);
      else {fd_incref(x); return x;}}}
}

FD_EXPORT
fdtype fd_deep_copy(fdtype x)
{
  return fd_copier(x,(FD_DEEP_COPY|FD_FULL_COPY));
}

FD_EXPORT
fdtype fd_static_copy(fdtype x)
{
  return fd_copier(x,(FD_DEEP_COPY|FD_FULL_COPY|FD_STATIC_COPY));
}

FD_EXPORT
/* Copies a vector of LISP pointers */
fdtype *fd_copy_vec(fdtype *vec,int n,fdtype *into,int flags)
{
  fdtype *dest=(into==NULL)?(u8_alloc_n(n,fdtype)):(into);
  int i=0; while (i<n) {
    fdtype elt=vec[i];
    if (elt==FD_NULL)
      break;
    else if (!(FD_CONSP(elt)))
      dest[i]=elt;
    else if (U8_BITP(flags,FD_DEEP_COPY)) {
      dest[i]=fd_copier(elt,flags);}
    else
      dest[i]=fd_incref(elt);
    i++;}
  while (i<n) dest[i++]=FD_NULL;
  return dest;
}

FD_EXPORT
/* fd_copy:
    Arguments: a dtype pointer
    Returns: a dtype pointer
  If the argument is a malloc'd cons, this just increfs it.
  If it is a static cons, it does a deep copy. */
fdtype fd_copy(fdtype x)
{
  if (!(FD_CONSP(x))) return x;
  else if (FD_MALLOCD_CONSP(((fd_cons)x)))
    return fd_incref(x);
  else return fd_copier(x,0);
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
  int len=((slen<0) ? (strlen(string)) : (slen));
  if (ptr == NULL) {
    ptr=u8_alloc(struct FD_STRING);
    FD_INIT_STRUCT(ptr,struct FD_STRING);
    ptr->fd_freedata=1;}
  FD_INIT_CONS(ptr,fd_string_type);
  if ((len==0) && (string==NULL)) {
    u8_byte *bytes=u8_malloc(1); *bytes='\0';
    string=(u8_string)bytes;}
  ptr->fd_bytelen=len; ptr->fd_bytes=string;
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
  ssize_t length=((end==NULL) ? (strlen(start)) : (end-start));
  if ((length>=0)&&((fd_max_strlen<0)||(length<fd_max_strlen))) {
    u8_byte *bytes=NULL; int freedata=1;
    if (ptr == NULL) {
      ptr=u8_malloc(sizeof(struct FD_STRING)+length+1);
      bytes=((u8_byte *)ptr)+sizeof(struct FD_STRING);
      memcpy(bytes,start,length); bytes[length]='\0';
      freedata=0;}
    else bytes=(u8_byte *)u8_strndup(start,length+1);
    FD_INIT_CONS(ptr,fd_string_type);
    ptr->fd_bytelen=length; ptr->fd_bytes=bytes; ptr->fd_freedata=freedata;
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
  ssize_t length=((end==NULL) ? (strlen(start)) : (end-start));
  if ((length>=0)&&((fd_max_strlen<0)||(length<fd_max_strlen))) {
    struct FD_STRING *ptr=u8_malloc(sizeof(struct FD_STRING)+length+1);
    u8_byte *bytes=((u8_byte *)ptr)+sizeof(struct FD_STRING);
    memcpy(bytes,start,length); bytes[length]='\0';
    FD_INIT_CONS(ptr,fd_string_type);
    ptr->fd_bytelen=length; ptr->fd_bytes=bytes; ptr->fd_freedata=0;
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
  int length=((len>=0)?(len):(strlen(string)));
  u8_byte *bytes=NULL; int freedata=1;
  if (ptr == NULL) {
    ptr=u8_malloc(sizeof(struct FD_STRING)+length+1);
    bytes=((u8_byte *)ptr)+sizeof(struct FD_STRING);
    if (string) memcpy(bytes,string,length);
    else memset(bytes,'?',length);
    bytes[length]='\0';
    freedata=0;}
  else {
    bytes=u8_malloc(length+1);
    memcpy(bytes,string,length); bytes[length]='\0';}
  FD_INIT_CONS(ptr,fd_string_type);
  ptr->fd_bytelen=length; ptr->fd_bytes=bytes; ptr->fd_freedata=freedata;
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
  u8_byte *bytes=NULL;
  int length=((len>=0)?(len):(strlen(string)));
  struct FD_STRING *ptr=u8_malloc(sizeof(struct FD_STRING)+length+1);
  bytes=((u8_byte *)ptr)+sizeof(struct FD_STRING);
  if (string) memcpy(bytes,string,length);
  else memset(bytes,'?',length);
  bytes[length]='\0';
  FD_INIT_CONS(ptr,fd_string_type);
  ptr->fd_bytelen=length; ptr->fd_bytes=bytes; ptr->fd_freedata=0;
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
  int length=((len>0)?(len):(strlen(string)));
  u8_byte *bytes=NULL; int freedata=1;
  if (ptr == NULL) {
    ptr=u8_mallocz(sizeof(struct FD_STRING)+length+1);
    bytes=((u8_byte *)ptr)+sizeof(struct FD_STRING);
    memcpy(bytes,string,length); bytes[length]='\0';
    freedata=0;}
  else {
    bytes=u8_mallocz(length+1);
    memcpy(bytes,string,length); bytes[length]='\0';}
  FD_INIT_CONS(ptr,fd_string_type);
  ptr->fd_bytelen=length; ptr->fd_bytes=bytes; ptr->fd_freedata=freedata;
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
  if (ptr == NULL) ptr=u8_alloc(struct FD_PAIR);
  FD_INIT_CONS(ptr,fd_pair_type);
  ptr->fd_car=car; ptr->fd_cdr=cdr;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_make_pair(fdtype car,fdtype cdr)
{
  return fd_init_pair(NULL,fd_incref(car),fd_incref(cdr));
}

FD_EXPORT fdtype fd_make_list(int len,...)
{
  va_list args; int i=0;
  fdtype *elts=u8_alloc_n(len,fdtype), result=FD_EMPTY_LIST;
  va_start(args,len);
  while (i<len) elts[i++]=va_arg(args,fdtype);
  va_end(args);
  i=len-1; while (i>=0) {
    result=fd_init_pair(NULL,elts[i],result); i--;}
  u8_free(elts);
  return result;
}

FD_EXPORT int fd_list_length(fdtype l)
{
  int len=0; fdtype scan=l; while (FD_PAIRP(scan)) {
    len++; scan=FD_CDR(scan);}
  if (FD_EMPTY_LISTP(scan)) return len;
  else return -len;
}

/* Vectors */

FD_EXPORT fdtype fd_init_vector(struct FD_VECTOR *ptr,int len,fdtype *data)
{
  fdtype *elts; int freedata=1;
  if ((ptr == NULL)&&(data==NULL)) {
    int i=0;
    ptr=u8_mallocz(sizeof(struct FD_VECTOR)+(sizeof(fdtype)*len));
    /* This might be weird on non byte-addressed architectures */
    elts=((fdtype *)(((unsigned char *)ptr)+sizeof(struct FD_VECTOR)));
    while (i < len) elts[i++]=FD_VOID;
    freedata=0;}
  else if (ptr==NULL) {
    ptr=u8_zalloc(struct FD_VECTOR);
    elts=data;}
  else if (data==NULL) {
      int i=0; elts=u8_mallocz(sizeof(fdtype)*len);
      while (i<len) elts[i]=FD_VOID;
      freedata=1;}
  else elts=data;
  FD_INIT_CONS(ptr,fd_vector_type);
  ptr->fd_veclen=len; ptr->fd_vecelts=elts; ptr->fd_freedata=freedata;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_make_nvector(int len,...)
{
  va_list args; int i=0;
  fdtype result, *elts;
  va_start(args,len);
  result=fd_init_vector(NULL,len,NULL);
  elts=FD_VECTOR_ELTS(result);
  while (i<len) elts[i++]=va_arg(args,fdtype);
  va_end(args);
  return result;
}

FD_EXPORT fdtype fd_make_vector(int len,fdtype *data)
{
  int i=0;
  struct FD_VECTOR *ptr=
    u8_mallocz(sizeof(struct FD_VECTOR)+(sizeof(fdtype)*len));
  fdtype *elts=((fdtype *)(((unsigned char *)ptr)+sizeof(struct FD_VECTOR)));
  FD_INIT_CONS(ptr,fd_vector_type);
  ptr->fd_veclen=len; ptr->fd_vecelts=elts; ptr->fd_freedata=0;
  if (data) {
    while (i < len) {elts[i]=data[i]; i++;}}
  else {while (i < len) {elts[i]=FD_VOID; i++;}}
  return FDTYPE_CONS(ptr);
}

/* Rails */

FD_EXPORT fdtype fd_init_rail(struct FD_VECTOR *ptr,int len,fdtype *data)
{
  fdtype *elts; int i=0, freedata=1;
  if ((ptr == NULL)&&(data==NULL)) {
    ptr=u8_mallocz(sizeof(struct FD_VECTOR)+(sizeof(fdtype)*len));
    elts=((fdtype *)(((unsigned char *)ptr)+sizeof(struct FD_VECTOR)));
    freedata=0;}
  else if (ptr==NULL) {
    ptr=u8_alloc(struct FD_VECTOR);
    elts=data;}
  else if (data==NULL) {
      int i=0; elts=u8_zalloc(sizeof(fdtype)*len);
      while (i<len) elts[i]=FD_VOID;
      freedata=1;}
  else {
    ptr=u8_alloc(struct FD_VECTOR);
    elts=data;}
  FD_INIT_CONS(ptr,fd_rail_type);
  if (data==NULL) while (i < len) elts[i++]=FD_VOID;
  ptr->fd_veclen=len; ptr->fd_vecelts=elts; ptr->fd_freedata=freedata;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_make_nrail(int len,...)
{
  va_list args; int i=0;
  fdtype result=fd_init_rail(NULL,len,NULL);
  fdtype *elts=FD_RAIL_ELTS(result);
  va_start(args,len);
  while (i<len) elts[i++]=va_arg(args,fdtype);
  va_end(args);
  return result;
}

FD_EXPORT fdtype fd_make_rail(int len,fdtype *data)
{
  int i=0;
  struct FD_VECTOR *ptr=u8_mallocz
    (sizeof(struct FD_VECTOR)+(sizeof(fdtype)*len));
  fdtype *elts=((fdtype *)(((unsigned char *)ptr)+sizeof(struct FD_VECTOR)));
  FD_INIT_CONS(ptr,fd_rail_type);
  ptr->fd_veclen=len; ptr->fd_vecelts=elts; ptr->fd_freedata=0;
  while (i < len) {elts[i]=data[i]; i++;}
  return FDTYPE_CONS(ptr);
}

/* Packets */

FD_EXPORT fdtype fd_init_packet
  (struct FD_STRING *ptr,int len,const unsigned char *data)
{
  if ((ptr==NULL)&&(data==NULL))
    return fd_make_packet(ptr,len,data);
  if (ptr == NULL) {
    ptr=u8_alloc(struct FD_STRING);
    ptr->fd_freedata=1;}
  FD_INIT_CONS(ptr,fd_packet_type);
  if (data == NULL) {
    u8_byte *consed=u8_mallocz(len+1);
    memset(consed,0,len+1);
    data=consed;}
  ptr->fd_bytelen=len; ptr->fd_bytes=data;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_make_packet
  (struct FD_STRING *ptr,int len,const unsigned char *data)
{
  u8_byte *bytes=NULL; int freedata=1;
  if (ptr == NULL) {
    ptr=u8_mallocz(sizeof(struct FD_STRING)+len+1);
    bytes=((u8_byte *)ptr)+sizeof(struct FD_STRING);
    if (data) {
      memcpy(bytes,data,len);
      bytes[len]='\0';}
    else memset(bytes,0,len+1);
    freedata=0;}
  else if (data==NULL) {
    bytes=u8_malloc(len+1);}
  else bytes=(unsigned char *)data;
  FD_INIT_CONS(ptr,fd_packet_type);
  ptr->fd_bytelen=len; ptr->fd_bytes=bytes; ptr->fd_freedata=freedata;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_bytes2packet
  (struct FD_STRING *ptr,int len,const unsigned char *data)
{
  u8_byte *bytes=NULL; int freedata=(data!=NULL);
  if (ptr == NULL) {
    ptr=u8_mallocz(sizeof(struct FD_STRING)+len+1);
    bytes=((u8_byte *)ptr)+sizeof(struct FD_STRING);
    if (data) {
      memcpy(bytes,data,len);
      bytes[len]='\0';}
    else memset(bytes,0,len+1);
    freedata=0;}
  else {
    bytes=u8_malloc(len);
    if (data==NULL) memset(bytes,0,len);
    else memcpy(bytes,data,len);}
  FD_INIT_CONS(ptr,fd_packet_type);
  ptr->fd_bytelen=len; ptr->fd_bytes=bytes; ptr->fd_freedata=freedata;
  if (freedata) u8_free(data);
  return FDTYPE_CONS(ptr);
}

/* Compounds */

fdtype fd_compound_descriptor_type;

FD_EXPORT fdtype fd_init_compound
  (struct FD_COMPOUND *p,fdtype tag,u8_byte mutable,short n,...)
{
  va_list args; int i=0; fdtype *write, *limit, initfn=FD_FALSE;
  if (FD_EXPECT_FALSE((n<0)||(n>=256))) {
    /* Consume the arguments, just in case the implementation is a
       little flaky. */
    va_start(args,n);
    while (i<n) {va_arg(args,fdtype); i++;}
    return fd_type_error
      (_("positive byte"),"fd_init_compound",FD_SHORT2DTYPE(n));}
  else if (p==NULL) {
    if (n==0) p=u8_malloc(sizeof(struct FD_COMPOUND));
    else p=u8_malloc(sizeof(struct FD_COMPOUND)+(n-1)*sizeof(fdtype));}
  FD_INIT_CONS(p,fd_compound_type);
  if (mutable) fd_init_mutex(&(p->fd_lock));
  p->compound_typetag=fd_incref(tag); p->compound_ismutable=mutable; p->fd_n_elts=n; p->compound_isopaque=0;
  if (n>0) {
    write=&(p->compound_0); limit=write+n;
    va_start(args,n);
    while (write<limit) {
      fdtype value=va_arg(args,fdtype);
      *write=value; write++;}
    va_end(args);
    if (FD_ABORTP(initfn)) {
      write=&(p->compound_0);
      while (write<limit) {fd_decref(*write); write++;}
      return initfn;}
    else return FDTYPE_CONS(p);}
  else return FDTYPE_CONS(p);
}

FD_EXPORT fdtype fd_init_compound_from_elts
  (struct FD_COMPOUND *p,fdtype tag,u8_byte mutable,short n,fdtype *elts)
{
  fdtype *write, *limit, *read=elts, initfn=FD_FALSE;
  if (FD_EXPECT_FALSE((n<0) || (n>=256)))
    return fd_type_error(_("positive byte"),"fd_init_compound_from_elts",
                         FD_SHORT2DTYPE(n));
  else if (p==NULL) {
    if (n==0) p=u8_malloc(sizeof(struct FD_COMPOUND));
    else p=u8_malloc(sizeof(struct FD_COMPOUND)+(n-1)*sizeof(fdtype));}
  FD_INIT_CONS(p,fd_compound_type);
  if (mutable) fd_init_mutex(&(p->fd_lock));
  p->compound_typetag=fd_incref(tag);
  p->compound_ismutable=mutable;
  p->fd_n_elts=n; p->compound_isopaque=0;
  if (n>0) {
    write=&(p->compound_0); limit=write+n;
    while (write<limit) {
      *write=*read++; write++;}
    if (FD_ABORTP(initfn)) {
      write=&(p->compound_0);
      while (write<limit) {fd_decref(*write); write++;}
      return initfn;}
    else return FDTYPE_CONS(p);}
  else return FDTYPE_CONS(p);
}

static void recycle_compound(struct FD_CONS *c)
{
  struct FD_COMPOUND *compound=(struct FD_COMPOUND *)c;
  int i=0, n=compound->fd_n_elts; fdtype *data=&(compound->compound_0);
  while (i<n) {fd_decref(data[i]); i++;}
  fd_decref(compound->compound_typetag);
  if (compound->compound_ismutable) fd_destroy_mutex(&(compound->fd_lock));
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

static int compare_compounds(fdtype x,fdtype y,int quick)
{
  struct FD_COMPOUND *xc=fd_consptr(struct FD_COMPOUND *,x,fd_compound_type);
  struct FD_COMPOUND *yc=fd_consptr(struct FD_COMPOUND *,y,fd_compound_type);
  int cmp;
  if (xc == yc) return 0;
  else if ((xc->compound_isopaque) || (yc->compound_isopaque))
    if (xc>yc) return 1; else return -1;
  else if ((cmp=(FD_COMPARE(xc->compound_typetag,yc->compound_typetag,quick))))
    return cmp;
  else if (xc->fd_n_elts<yc->fd_n_elts) return -1;
  else if (xc->fd_n_elts>yc->fd_n_elts) return 1;
  else {
    int i=0, len=xc->fd_n_elts;
    fdtype *xdata=&(xc->compound_0), *ydata=&(yc->compound_0);
    while (i<len)
      if ((cmp=(FD_COMPARE(xdata[i],ydata[i],quick)))==0)
        i++;
      else return cmp;
    return 0;}
}

static int dtype_compound(struct FD_BYTE_OUTPUT *out,fdtype x)
{
  struct FD_COMPOUND *xc=fd_consptr(struct FD_COMPOUND *,x,fd_compound_type);
  int n_bytes=1;
  fd_write_byte(out,dt_compound);
  n_bytes=n_bytes+fd_write_dtype(out,xc->compound_typetag);
  if (xc->fd_n_elts==1)
    n_bytes=n_bytes+fd_write_dtype(out,xc->compound_0);
  else {
    int i=0, n=xc->fd_n_elts; fdtype *data=&(xc->compound_0);
    fd_write_byte(out,dt_vector);
    fd_write_4bytes(out,xc->fd_n_elts);
    n_bytes=n_bytes+5;
    while (i<n) {
      int written=fd_write_dtype(out,data[i]);
      if (written<0) return written;
      else n_bytes=n_bytes+written;
      i++;}}
  return n_bytes;
}

static fdtype copy_compound(fdtype x,int flags)
{
  struct FD_COMPOUND *xc=fd_consptr(struct FD_COMPOUND *,x,fd_compound_type);
  if (xc->compound_isopaque) {
    fd_incref(x); return x;}
  else {
    int i=0, n=xc->fd_n_elts;
    struct FD_COMPOUND *nc=u8_malloc(sizeof(FD_COMPOUND)+(n-1)*sizeof(fdtype));
    fdtype *data=&(xc->compound_0), *write=&(nc->compound_0);
    FD_INIT_CONS(nc,fd_compound_type);
    if (xc->compound_ismutable) fd_init_mutex(&(nc->fd_lock));
    nc->compound_ismutable=xc->compound_ismutable; nc->compound_isopaque=1;
    nc->compound_typetag=fd_incref(xc->compound_typetag); nc->fd_n_elts=xc->fd_n_elts;
    if (flags)
      while (i<n) {
        *write=fd_copier(data[i],flags); i++; write++;}
    else while (i<n) {
        *write=fd_incref(data[i]); i++; write++;}
    return FDTYPE_CONS(nc);}
}

/* Exceptions */

FD_EXPORT fdtype fd_init_exception
   (struct FD_EXCEPTION_OBJECT *exo,u8_exception ex)
{
  if (exo==NULL) exo=u8_alloc(struct FD_EXCEPTION_OBJECT);
  FD_INIT_CONS(exo,fd_error_type); exo->fd_u8ex=ex;
  return FDTYPE_CONS(exo);
}

FD_EXPORT fdtype fd_make_exception
  (fd_exception c,u8_context cxt,u8_string details,fdtype content)
{
  struct FD_EXCEPTION_OBJECT *exo=u8_alloc(struct FD_EXCEPTION_OBJECT);
  u8_exception ex; void *xdata; u8_exception_xdata_freefn freefn;
  if (FD_VOIDP(content)) {
    xdata=NULL; freefn=NULL;}
  else {
    xdata=(void *) content;
    freefn=fd_free_exception_xdata;}
  ex=u8_make_exception(c,cxt,details,xdata,freefn);
  FD_INIT_CONS(exo,fd_error_type); exo->fd_u8ex=ex;
  return FDTYPE_CONS(exo);
}

static void recycle_exception(struct FD_CONS *c)
{
  struct FD_EXCEPTION_OBJECT *exo=(struct FD_EXCEPTION_OBJECT *)c;
  if (exo->fd_u8ex) {
    u8_free_exception(exo->fd_u8ex,1);
    exo->fd_u8ex=NULL;}
  if (!(FD_STATIC_CONSP(exo))) u8_free(exo);
}

static int dtype_exception(struct FD_BYTE_OUTPUT *out,fdtype x)
{
  struct FD_EXCEPTION_OBJECT *exo=(struct FD_EXCEPTION_OBJECT *)x;
  if (exo->fd_u8ex==NULL) {
    u8_log(LOG_CRIT,NULL,"Trying to serialize expired exception ");
    fd_write_byte(out,dt_void);
    return 1;}
  else {
    u8_exception ex=exo->fd_u8ex;
    fdtype irritant=fd_exception_xdata(ex);
    int veclen=((FD_VOIDP(irritant)) ? (3) : (4));
    fdtype vector=fd_init_vector(NULL,veclen,NULL);
    int n_bytes;
    FD_VECTOR_SET(vector,0,fd_intern((u8_string)(ex->u8x_cond)));
    if (ex->u8x_context) {
      FD_VECTOR_SET(vector,1,fd_intern((u8_string)(ex->u8x_context)));}
    else {FD_VECTOR_SET(vector,1,FD_FALSE);}
    if (ex->u8x_details) {
      FD_VECTOR_SET(vector,2,fdtype_string(ex->u8x_details));}
    else {FD_VECTOR_SET(vector,2,FD_FALSE);}
    if (!(FD_VOIDP(irritant)))
      FD_VECTOR_SET(vector,3,fd_incref(irritant));
    fd_write_byte(out,dt_exception);
    n_bytes=1+fd_write_dtype(out,vector);
    fd_decref(vector);
    return n_bytes;}
}

static int unparse_exception(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_EXCEPTION_OBJECT *xo=
    fd_consptr(struct FD_EXCEPTION_OBJECT *,x,fd_error_type);
  u8_exception ex=xo->fd_u8ex;
  if (ex==NULL)
    u8_printf(out,"#<!OLDEXCEPTION>");
  else {
    u8_printf(out,"#<!EXCEPTION ");
    fd_sum_exception(out,xo->fd_u8ex);
    u8_printf(out,"!>");}
  return 1;
}

static u8_exception copy_exception_helper(u8_exception ex,int flags)
{
  u8_exception newex; u8_string details=NULL; fdtype irritant;
  if (ex==NULL) return ex;
  if (ex->u8x_details) details=u8_strdup(ex->u8x_details);
  irritant=fd_exception_xdata(ex);
  if (FD_VOIDP(irritant))
    newex=u8_make_exception
      (ex->u8x_cond,ex->u8x_context,details,NULL,NULL);
  else if (flags)
    newex=u8_make_exception
      (ex->u8x_cond,ex->u8x_context,details,
       (void *)fd_copier(irritant,flags),fd_free_exception_xdata);
  else newex=u8_make_exception
         (ex->u8x_cond,ex->u8x_context,details,
          (void *)fd_incref(irritant),fd_free_exception_xdata);
  newex->u8x_prev=copy_exception_helper(ex->u8x_prev,flags);
  return newex;
}

static fdtype copy_exception(fdtype x,int deep)
{
  struct FD_EXCEPTION_OBJECT *xo=
    fd_consptr(struct FD_EXCEPTION_OBJECT *,x,fd_error_type);
  return fd_init_exception(NULL,copy_exception_helper(xo->fd_u8ex,deep));
}


/* Mystery types */

static int unparse_mystery(u8_output out,fdtype x)
{
  struct FD_MYSTERY_DTYPE *d=fd_consptr(struct FD_MYSTERY_DTYPE *,x,fd_mystery_type);
  char buf[128];
  if (d->fd_dtcode&0x80)
    sprintf(buf,_("#<MysteryVector 0x%x/0x%x %d elements>"),
            d->fd_dtpackage,d->fd_dtcode,d->fd_dtlen);
  else sprintf(buf,_("#<MysteryPacket 0x%x/0x%x %d bytes>"),
               d->fd_dtpackage,d->fd_dtcode,d->fd_dtlen);
  u8_puts(out,buf);
  return 1;
}

static void recycle_mystery(struct FD_CONS *c)
{
  struct FD_MYSTERY_DTYPE *myst=(struct FD_MYSTERY_DTYPE *)c;
  if (myst->fd_dtcode&0x80)
    u8_free(myst->fd_mystery_payload.fd_dtelts);
  else u8_free(myst->fd_mystery_payload.fd_dtbytes);
  if (!(FD_STATIC_CONSP(myst))) u8_free(myst);
}

/* Registering new primitive types */

#if FD_THREADS_ENABLED
static u8_mutex type_registry_lock;
#endif

unsigned int fd_next_cons_type=FD_CONS_TYPECODE(FD_BUILTIN_CONS_TYPES);
unsigned int fd_next_immediate_type=FD_IMMEDIATE_TYPECODE(FD_BUILTIN_IMMEDIATE_TYPES);

FD_EXPORT int fd_register_cons_type(char *name)
{
  int typecode;
  fd_lock_mutex(&type_registry_lock);
  if (fd_next_cons_type>=FD_MAX_CONS_TYPE) {
    fd_unlock_mutex(&type_registry_lock);
    return -1;}
  typecode=fd_next_cons_type;
  fd_next_cons_type++;
  fd_type_names[typecode]=name;
  fd_unlock_mutex(&type_registry_lock);
  return typecode;
}

FD_EXPORT int fd_register_immediate_type(char *name,fd_checkfn fn)
{
  int typecode;
  fd_lock_mutex(&type_registry_lock);
  if (fd_next_immediate_type>=FD_MAX_IMMEDIATE_TYPE) {
    fd_unlock_mutex(&type_registry_lock);
    return -1;}
  typecode=fd_next_immediate_type;
  fd_immediate_checkfns[typecode]=fn;
  fd_next_immediate_type++;
  fd_type_names[typecode]=name;
  fd_unlock_mutex(&type_registry_lock);
  return typecode;
}

/* Compound type information */

struct FD_COMPOUND_TYPEINFO *fd_compound_entries=NULL;
#if FD_THREADS_ENABLED
static u8_mutex compound_registry_lock;
#endif

FD_EXPORT struct FD_COMPOUND_TYPEINFO *fd_register_compound(fdtype symbol,fdtype *datap,int *corep)
{
  struct FD_COMPOUND_TYPEINFO *scan, *newrec;
  fd_lock_mutex(&compound_registry_lock);
  scan=fd_compound_entries;
  while (scan)
    if (FD_EQ(scan->compound_typetag,symbol)) {
      if (datap) {
        fdtype data=*datap;
        if (FD_VOIDP(scan->fd_compound_metadata)) {
          fd_incref(data); scan->fd_compound_metadata=data;}
        else {
          fdtype data=*datap; fd_decref(data);
          data=scan->fd_compound_metadata; fd_incref(data);
          *datap=data;}}
      if (corep) {
        if (scan->fd_compound_corelen<0) scan->fd_compound_corelen=*corep;
        else *corep=scan->fd_compound_corelen;}
      fd_unlock_mutex(&compound_registry_lock);
      return scan;}
    else scan=scan->fd_compound_nextinfo;
  newrec=u8_alloc(struct FD_COMPOUND_TYPEINFO);
  memset(newrec,0,sizeof(struct FD_COMPOUND_TYPEINFO));
  if (datap) {
    fdtype data=*datap; fd_incref(data); newrec->fd_compound_metadata=data;}
  else newrec->fd_compound_metadata=FD_VOID;
  newrec->fd_compound_corelen=((corep)?(*corep):(-1));
  newrec->fd_compound_nextinfo=fd_compound_entries; 
  newrec->compound_typetag=symbol;
  newrec->fd_compound_parser=NULL; 
  newrec->fd_compound_dumpfn=NULL; 
  newrec->fd_compound_restorefn=NULL;
  newrec->fd_compund_tablefns=NULL;
  fd_compound_entries=newrec;
  fd_unlock_mutex(&compound_registry_lock);
  return newrec;
}

FD_EXPORT struct FD_COMPOUND_TYPEINFO 
          *fd_declare_compound(fdtype symbol,fdtype data,int core_slots)
{
  struct FD_COMPOUND_TYPEINFO *scan, *newrec;
  fd_lock_mutex(&compound_registry_lock);
  scan=fd_compound_entries;
  while (scan)
    if (FD_EQ(scan->compound_typetag,symbol)) {
      if (!(FD_VOIDP(data))) {
        fdtype old_data=scan->fd_compound_metadata;
        scan->fd_compound_metadata=fd_incref(data);
        fd_decref(old_data);}
      if (core_slots>0) scan->fd_compound_corelen=core_slots;
      fd_unlock_mutex(&compound_registry_lock);
      return scan;}
    else scan=scan->fd_compound_nextinfo;
  newrec=u8_alloc(struct FD_COMPOUND_TYPEINFO);
  memset(newrec,0,sizeof(struct FD_COMPOUND_TYPEINFO));
  newrec->fd_compound_metadata=data;
  newrec->fd_compound_corelen=core_slots;
  newrec->compound_typetag=symbol;
  newrec->fd_compound_nextinfo=fd_compound_entries; 
  newrec->fd_compound_parser=NULL;
  newrec->fd_compound_dumpfn=NULL;
  newrec->fd_compound_restorefn=NULL;
  newrec->fd_compund_tablefns=NULL;
  fd_compound_entries=newrec;
  fd_unlock_mutex(&compound_registry_lock);
  return newrec;
}

FD_EXPORT struct FD_COMPOUND_TYPEINFO *fd_lookup_compound(fdtype symbol)
{
  struct FD_COMPOUND_TYPEINFO *scan=fd_compound_entries;
  while (scan)
    if (FD_EQ(scan->compound_typetag,symbol)) {
      return scan;}
    else scan=scan->fd_compound_nextinfo;
  return NULL;
}

/* Timestamps */

static fdtype timestamp_symbol, timestamp0_symbol;

FD_EXPORT
/* fd_make_timestamp:
    Arguments: a pointer to a U8_XTIME struct and a memory pool
    Returns: a dtype pointer to a timestamp
 */
fdtype fd_make_timestamp(struct U8_XTIME *tm)
{
  struct FD_TIMESTAMP *tstamp=u8_alloc(struct FD_TIMESTAMP);
  memset(tstamp,0,sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(tstamp,fd_timestamp_type);
  if (tm)
    memcpy(&(tstamp->fd_u8xtime),tm,sizeof(struct U8_XTIME));
  else u8_now(&(tstamp->fd_u8xtime));
  return FDTYPE_CONS(tstamp);
}

FD_EXPORT
/* fd_time2timestamp
    Arguments: a pointer to a U8_XTIME struct and a memory pool
    Returns: a dtype pointer to a timestamp
 */
fdtype fd_time2timestamp(time_t moment)
{
  struct U8_XTIME xt;
  u8_init_xtime(&xt,moment,u8_second,0,0,0);
  return fd_make_timestamp(&xt);
}

static int reversible_time=1;

static int unparse_timestamp(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_TIMESTAMP *tm=
    fd_consptr(struct FD_TIMESTAMP *,x,fd_timestamp_type);
  if (reversible_time) {
    u8_puts(out,"#T");
    u8_xtime_to_iso8601(out,&(tm->fd_u8xtime));
    return 1;}
  else {
    u8_printf(out,"#<TIMESTAMP 0x%x \"",(unsigned int)x);
    u8_xtime_to_iso8601(out,&(tm->fd_u8xtime));
    u8_printf(out,"\">");
    return 1;}
}

static fdtype timestamp_parsefn(int n,fdtype *args,fd_compound_typeinfo e)
{
  struct FD_TIMESTAMP *tm=u8_alloc(struct FD_TIMESTAMP);
  u8_string timestring;
  memset(tm,0,sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(tm,fd_timestamp_type);
  if ((n==2) && (FD_STRINGP(args[1])))
    timestring=FD_STRDATA(args[1]);
  else if ((n==3) && (FD_STRINGP(args[2])))
    timestring=FD_STRDATA(args[2]);
  else return fd_err(fd_CantParseRecord,"TIMESTAMP",NULL,FD_VOID);
  u8_iso8601_to_xtime(timestring,&(tm->fd_u8xtime));
  return FDTYPE_CONS(tm);
}

static void recycle_timestamp(struct FD_CONS *c)
{
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}

static fdtype copy_timestamp(fdtype x,int deep)
{
  struct FD_TIMESTAMP *tm=
    fd_consptr(struct FD_TIMESTAMP *,x,fd_timestamp_type);
  struct FD_TIMESTAMP *newtm=u8_alloc(struct FD_TIMESTAMP);
  memset(newtm,0,sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(newtm,fd_timestamp_type);
  memcpy(&(newtm->fd_u8xtime),&(tm->fd_u8xtime),sizeof(struct U8_XTIME));
  return FDTYPE_CONS(newtm);
}

static int compare_timestamps(fdtype x,fdtype y,int quick)
{
  struct FD_TIMESTAMP *xtm=
    fd_consptr(struct FD_TIMESTAMP *,x,fd_timestamp_type);
  struct FD_TIMESTAMP *ytm=
    fd_consptr(struct FD_TIMESTAMP *,y,fd_timestamp_type);
  double diff=u8_xtime_diff(&(xtm->fd_u8xtime),&(ytm->fd_u8xtime));
  if (diff<0.0) return -1;
  else if (diff == 0.0) return 0;
  else return 1;
}

static int dtype_timestamp(struct FD_BYTE_OUTPUT *out,fdtype x)
{
  struct FD_TIMESTAMP *xtm=
    fd_consptr(struct FD_TIMESTAMP *,x,fd_timestamp_type);
  int size=1;
  fd_write_byte(out,dt_compound);
  size=size+fd_write_dtype(out,timestamp_symbol);
  if ((xtm->fd_u8xtime.u8_prec == u8_second) && (xtm->fd_u8xtime.u8_tzoff==0)) {
    fdtype xval=FD_INT(xtm->fd_u8xtime.u8_tick);
    size=size+fd_write_dtype(out,xval);}
  else {
    fdtype vec=fd_init_vector(NULL,4,NULL);
    int tzoff=xtm->fd_u8xtime.u8_tzoff;
    FD_VECTOR_SET(vec,0,FD_INT(xtm->fd_u8xtime.u8_tick));
    FD_VECTOR_SET(vec,1,FD_INT(xtm->fd_u8xtime.u8_nsecs));
    FD_VECTOR_SET(vec,2,FD_INT((int)xtm->fd_u8xtime.u8_prec));
    FD_VECTOR_SET(vec,3,FD_INT(tzoff));
    size=size+fd_write_dtype(out,vec);
    fd_decref(vec);}
  return size;
}

static fdtype timestamp_restore(fdtype tag,fdtype x,fd_compound_typeinfo e)
{
  if (FD_FIXNUMP(x)) {
    struct FD_TIMESTAMP *tm=u8_alloc(struct FD_TIMESTAMP);
    memset(tm,0,sizeof(struct FD_TIMESTAMP));
    FD_INIT_CONS(tm,fd_timestamp_type);
    u8_init_xtime(&(tm->fd_u8xtime),FD_FIX2INT(x),u8_second,0,0,0);
    return FDTYPE_CONS(tm);}
  else if (FD_BIGINTP(x)) {
    struct FD_TIMESTAMP *tm=u8_alloc(struct FD_TIMESTAMP);
    time_t tval=(time_t)(fd_bigint_to_long((fd_bigint)x));
    memset(tm,0,sizeof(struct FD_TIMESTAMP));
    FD_INIT_CONS(tm,fd_timestamp_type);
    u8_init_xtime(&(tm->fd_u8xtime),tval,u8_second,0,0,0);
    return FDTYPE_CONS(tm);}
  else if (FD_VECTORP(x)) {
    struct FD_TIMESTAMP *tm=u8_alloc(struct FD_TIMESTAMP);
    int secs=fd_getint(FD_VECTOR_REF(x,0));
    int nsecs=fd_getint(FD_VECTOR_REF(x,1));
    int iprec=fd_getint(FD_VECTOR_REF(x,2));
    int tzoff=fd_getint(FD_VECTOR_REF(x,3));
    memset(tm,0,sizeof(struct FD_TIMESTAMP));
    FD_INIT_CONS(tm,fd_timestamp_type);
    u8_init_xtime(&(tm->fd_u8xtime),secs,iprec,nsecs,tzoff,0);
    return FDTYPE_CONS(tm);}
  else return fd_err(fd_DTypeError,"bad timestamp compound",NULL,x);
}

/* Utility functions (for debugging) */

FD_EXPORT fd_cons fd_cons_data(fdtype x)
{
  unsigned long as_int=(x&(~0x3));
  void *as_addr=(void *) as_int;
  return (fd_cons) as_addr;
}

FD_EXPORT struct FD_PAIR *fd_pair_data(fdtype x)
{
  return FD_CONSPTR(fd_pair,x);
}

FD_EXPORT int _fd_find_elt(fdtype x,fdtype *v,int n)
{
  int i=0; while (i<n)
    if (v[i]==x) return i;
    else i++;
  return -1;
}

int fd_ptr_debug_density=1;

FD_EXPORT void _fd_bad_pointer(fdtype badx,u8_context cxt)
{
  u8_raise(fd_BadPtr,cxt,NULL);
}


/* UUID Types */

static fdtype uuid_symbol;

FD_EXPORT fdtype fd_cons_uuid
   (struct FD_UUID *ptr,
    struct U8_XTIME *xtime,long long nodeid,short clockid)
{
  if (ptr == NULL) ptr=u8_alloc(struct FD_UUID);
  FD_INIT_CONS(ptr,fd_uuid_type);
  u8_consuuid(xtime,nodeid,clockid,(u8_uuid)&(ptr->fd_uuid16));
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_fresh_uuid(struct FD_UUID *ptr)
{
  if (ptr == NULL) ptr=u8_alloc(struct FD_UUID);
  FD_INIT_CONS(ptr,fd_uuid_type);
  u8_getuuid((u8_uuid)&(ptr->fd_uuid16));
  return FDTYPE_CONS(ptr);
}

static void recycle_uuid(struct FD_CONS *c)
{
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}

static int unparse_uuid(u8_output out,fdtype x)
{
  struct FD_UUID *uuid=fd_consptr(struct FD_UUID *,x,fd_uuid_type);
  char buf[37]; u8_uuidstring((u8_uuid)(&(uuid->fd_uuid16)),buf);
  u8_printf(out,"#U%s",buf);
  return 1;
}

static fdtype copy_uuid(fdtype x,int deep)
{
  struct FD_UUID *uuid=fd_consptr(struct FD_UUID *,x,fd_uuid_type);
  struct FD_UUID *nuuid=u8_alloc(struct FD_UUID);
  FD_INIT_CONS(nuuid,fd_uuid_type);
  memcpy(nuuid->fd_uuid16,uuid->fd_uuid16,16);
  return FDTYPE_CONS(nuuid);
}

static int compare_uuids(fdtype x,fdtype y,int quick)
{
  struct FD_UUID *xuuid=fd_consptr(struct FD_UUID *,x,fd_uuid_type);
  struct FD_UUID *yuuid=fd_consptr(struct FD_UUID *,y,fd_uuid_type);
  return memcmp(xuuid->fd_uuid16,yuuid->fd_uuid16,16);
}

#define MU U8_MAYBE_UNUSED

static int uuid_dtype(struct FD_BYTE_OUTPUT *out,fdtype x)
{
  int size=0;
  struct FD_UUID *uuid=fd_consptr(struct FD_UUID *,x,fd_uuid_type);
  fd_write_byte(out,dt_compound);
  size=size+1+fd_write_dtype(out,uuid_symbol);
  fd_write_byte(out,dt_packet); fd_write_4bytes(out,16); size=size+5;
  fd_write_bytes(out,uuid->fd_uuid16,16); size=size+16;
  return size;
}

static fdtype uuid_dump(fdtype x,fd_compound_typeinfo MU e)
{
  struct FD_UUID *uuid=fd_consptr(struct FD_UUID *,x,fd_uuid_type);
  return fd_make_packet(NULL,16,uuid->fd_uuid16);
}

static fdtype uuid_restore(fdtype MU tag,fdtype x,fd_compound_typeinfo MU e)
{
  if (FD_PACKETP(x)) {
    struct FD_STRING *p=fd_consptr(struct FD_STRING *,x,fd_packet_type);
    if (p->fd_bytelen==16) {
      struct FD_UUID *uuid=u8_alloc(struct FD_UUID);
      FD_INIT_CONS(uuid,fd_uuid_type);
      memcpy(uuid->fd_uuid16,p->fd_bytes,16);
      return FDTYPE_CONS(uuid);}
    else return fd_err("Bad UUID packet","uuid_restore",
                       "UUID packet has wrong length",
                       x);}
  else return fd_err("Bad UUID rep","uuid_restore",
                     "UUID serialization isn't a packet",
                     x);
}

/* Testing */

static int some_false(fdtype arg)
{
  int some_false=0;
  FD_DOELTS(elt,arg,count) {
    if (FD_FALSEP(elt)) some_false=1;}
  return some_false;
}


/* Initialization */

void fd_init_cons_c()
{
  int i;
#if FD_THREADS_ENABLED
  i=0; while (i < FD_N_PTRLOCKS) fd_init_mutex(&_fd_ptr_locks[i++]);
#endif

  u8_register_source_file(_FILEINFO);

#if FD_THREADS_ENABLED
  fd_init_mutex(&constant_registry_lock);
  fd_init_mutex(&compound_registry_lock);
  fd_init_mutex(&type_registry_lock);
#endif

  i=0; while (i < FD_TYPE_MAX) fd_unparsers[i++]=NULL;
  i=0; while (i < FD_TYPE_MAX) fd_type_names[i++]=NULL;
  i=0; while (i < FD_TYPE_MAX) fd_recyclers[i++]=NULL;
  i=0; while (i < FD_TYPE_MAX) fd_dtype_writers[i++]=NULL;
  i=0; while (i < FD_TYPE_MAX) fd_comparators[i++]=NULL;
  i=0; while (i<FD_TYPE_MAX) fd_hashfns[i++]=NULL;
  i=0; while (i<FD_MAX_IMMEDIATE_TYPES+4)
         fd_immediate_checkfns[i++]=NULL;

  fd_immediate_checkfns[fd_constant_type]=validate_constant;

  fd_recyclers[fd_error_type]=recycle_exception;
  fd_copiers[fd_error_type]=copy_exception;
  if (fd_dtype_writers[fd_error_type]==NULL)
    fd_dtype_writers[fd_error_type]=dtype_exception;
  if (fd_unparsers[fd_error_type]==NULL)
    fd_unparsers[fd_error_type]=unparse_exception;

  fd_recyclers[fd_mystery_type]=recycle_mystery;
  fd_recyclers[fd_compound_type]=recycle_compound;

  fd_comparators[fd_compound_type]=compare_compounds;

  fd_copiers[fd_compound_type]=copy_compound;

  if (fd_unparsers[fd_mystery_type]==NULL)
    fd_unparsers[fd_mystery_type]=unparse_mystery;

  fd_dtype_writers[fd_compound_type]=dtype_compound;

  timestamp_symbol=fd_intern("TIMESTAMP");
  timestamp0_symbol=fd_intern("TIMESTAMP0");
  {
    struct FD_COMPOUND_TYPEINFO *e=
      fd_register_compound(timestamp_symbol,NULL,NULL);
    e->fd_compound_parser=timestamp_parsefn;
    e->fd_compound_dumpfn=NULL;
    e->fd_compound_restorefn=timestamp_restore;}
  {
    struct FD_COMPOUND_TYPEINFO *e=
      fd_register_compound(timestamp0_symbol,NULL,NULL);
    e->fd_compound_parser=timestamp_parsefn;
    e->fd_compound_dumpfn=NULL;
    e->fd_compound_restorefn=timestamp_restore;}
  fd_dtype_writers[fd_timestamp_type]=dtype_timestamp;
  fd_unparsers[fd_timestamp_type]=unparse_timestamp;
  fd_copiers[fd_timestamp_type]=copy_timestamp;
  fd_comparators[fd_timestamp_type]=compare_timestamps;
  fd_recyclers[fd_timestamp_type]=recycle_timestamp;

  fd_unparsers[fd_uuid_type]=unparse_uuid;
  fd_dtype_writers[fd_uuid_type]=uuid_dtype;
  fd_recyclers[fd_uuid_type]=recycle_uuid;
  fd_comparators[fd_uuid_type]=compare_uuids;
  fd_copiers[fd_uuid_type]=copy_uuid;
  uuid_symbol=fd_intern("UUID");
  {
    struct FD_COMPOUND_TYPEINFO *e=fd_register_compound(uuid_symbol,NULL,NULL);
    e->fd_compound_dumpfn=uuid_dump;
    e->fd_compound_restorefn=uuid_restore;}

  fd_compound_descriptor_type=
    fd_init_compound
    (NULL,FD_VOID,9,
     fd_intern("COMPOUNDTYPE"),FD_INT(9),
     fd_make_nvector(9,fd_intern("TAG"),fd_intern("LENGTH"),
                     fd_intern("FIELDS"),fd_intern("INITFN"),
                     fd_intern("FREEFN"),fd_intern("COMPAREFN"),
                     fd_intern("STRINGFN"),fd_intern("DUMPFN"),
                     fd_intern("RESTOREFN")),
     FD_FALSE,FD_FALSE,FD_FALSE,FD_FALSE,
     FD_FALSE,FD_FALSE);
  ((fd_compound)fd_compound_descriptor_type)->compound_typetag=fd_compound_descriptor_type;
  fd_incref(fd_compound_descriptor_type);
}




/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
