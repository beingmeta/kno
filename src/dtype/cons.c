/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/cons.h"
#include <stdarg.h>

fd_exception fd_MallocFailed=_("malloc/realloc failed");
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
fd_checkfn fd_immediate_checkfns[128];

FD_EXPORT
/* fd_check_immediate:
     Arguments: a list pointer
     Returns: 1 or 0 (an int)
  Checks an immediate pointer for validity.
*/
int fd_check_immediate(fdtype x)
{
  int type=FD_IMMEDIATE_TYPE_FIELD(x);
  if (FD_IMMEDIATE_TYPECODE(type)<fd_max_immediate_type)
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
void fd_recycle_cons(struct FD_CONS *c)
{
  int ctype=FD_CONS_TYPE(c);
  int mallocd=(FD_MALLOCD_CONSP(c));
  switch (ctype) {
  case fd_rational_type:
  case fd_complex_type: 
    if (fd_recyclers[ctype]) {
      fd_recyclers[ctype](c); return;}
  case fd_pair_type: {
    struct FD_PAIR *p=(struct FD_PAIR *)c;
    fd_decref(p->car); fd_decref(p->cdr);
    if (mallocd) u8_free_x(p,sizeof(struct FD_PAIR));
    break;}
  case fd_string_type: case fd_packet_type: {
    struct FD_STRING *s=(struct FD_STRING *)c;
    if (s->bytes) u8_free_x(s->bytes,s->length);
    if (mallocd) u8_free_x(s,sizeof(struct FD_STRING));
    break;}
  case fd_vector_type: {
    struct FD_VECTOR *v=(struct FD_VECTOR *)c;
    int len=v->length; fdtype *scan=v->data, *limit=scan+len;
    if (scan) {
      while (scan<limit) {fd_decref(*scan); scan++;}
      u8_free_x(v->data,sizeof(fdtype)*len);}
    if (mallocd) u8_free_x(v,sizeof(struct FD_VECTOR));
    break;}
  case fd_choice_type: {
    struct FD_CHOICE *cv=(struct FD_CHOICE *)c;
    int len=(cv->size&(FD_CHOICE_SIZE_MASK)),
      atomicp=((cv->size&(FD_ATOMIC_CHOICE_MASK)));
    const fdtype *scan=FD_XCHOICE_DATA(cv), *limit=scan+len;
    if (scan == NULL) break;
    if (!(atomicp)) while (scan<limit) {fd_decref(*scan); scan++;}
    if (mallocd) u8_free_x(cv,sizeof(struct FD_CHOICE));
    break;}
  case fd_qchoice_type: {
    struct FD_QCHOICE *qc=(struct FD_QCHOICE *)c;
    fd_decref(qc->choice);
    if (mallocd) u8_free_x(qc,sizeof(struct FD_QCHOICE));
    break;}
  default: {
    if (fd_recyclers[ctype]) fd_recyclers[ctype](c);}
  }
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
  else if (!(FD_PRIM_TYPEP(y,FD_PTR_TYPE(x))))
    return 0;
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
	if (quick)
	  if (xlen>ylen) return 1; else if (xlen<ylen) return -1;
	return strncmp(FD_STRDATA(x),FD_STRDATA(y),xlen);}
      case fd_packet_type: {
	int xlen=FD_PACKET_LENGTH(x), ylen=FD_PACKET_LENGTH(y);
	if (quick)
	  if (xlen>ylen) return 1; else if (xlen<ylen) return -1;
	return memcmp(FD_PACKET_DATA(x),FD_PACKET_DATA(y),xlen);}
      case fd_vector_type: {
	int i=0, xlen=FD_VECTOR_LENGTH(x), ylen=FD_VECTOR_LENGTH(y), lim;
	fdtype *xdata=FD_VECTOR_DATA(x), *ydata=FD_VECTOR_DATA(y);
	if (quick)
	  if (xlen>ylen) return 1; else if (xlen<ylen) return -1;
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
	struct FD_CHOICE *xc=FD_GET_CONS(x,fd_choice_type,struct FD_CHOICE *);
	struct FD_CHOICE *yc=FD_GET_CONS(y,fd_choice_type,struct FD_CHOICE *);
	if ((quick) && (xc->size>yc->size)) return 1;
	else if ((quick) && (xc->size<yc->size)) return -1;
	else {
	  int xlen=FD_XCHOICE_SIZE(xc), ylen=FD_XCHOICE_SIZE(yc);
	  int i=0;
	  const fdtype *xscan=FD_XCHOICE_DATA(xc), *yscan=FD_XCHOICE_DATA(yc), *xlim=xscan+xlen;
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
fdtype fd_deep_copy(fdtype x)
{
  if (FD_ATOMICP(x)) return x;
  else {
    fd_ptr_type ctype=FD_CONS_TYPE(FD_CONS_DATA(x));
    switch (ctype) {
    case fd_pair_type: {
      struct FD_PAIR *p=FD_STRIP_CONS(x,ctype,struct FD_PAIR *);
      return fd_make_pair(p->car,p->cdr);}
    case fd_vector_type: {
      struct FD_VECTOR *v=FD_STRIP_CONS(x,ctype,struct FD_VECTOR *);
      fdtype *olddata=v->data;
      fdtype *newdata=u8_malloc(sizeof(fdtype)*(v->length));
      int i=0, len=v->length; while (i<len) {
	newdata[i]=fd_deep_copy(olddata[i]); i++;}
      return fd_init_vector(NULL,v->length,newdata);}
    case fd_string_type: {
      struct FD_STRING *s=FD_STRIP_CONS(x,ctype,struct FD_STRING *);
      return fd_init_string(NULL,s->length,u8_strndup(s->bytes,s->length));}
    case fd_compound_type: {
      struct FD_COMPOUND *c=FD_STRIP_CONS(x,ctype,struct FD_COMPOUND *);
      return fd_init_compound(NULL,fd_incref(c->tag),fd_deep_copy(c->data));}
    default:
      if (fd_copiers[ctype])
	return (fd_copiers[ctype])(x);
      else return fd_err(fd_NoMethod,"fd_deep_copy",fd_type_names[ctype],x);}}
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
  else if (FD_MALLOCD_CONSP(((struct FD_CONS *)x)))
    return fd_incref(x);
  else return fd_deep_copy(x);
}

/* Strings */

FD_EXPORT
/* fd_init_string:
    Arguments: A pointer to an FD_STRING struct, a length, and a byte vector.
    Returns: a lisp string
  This returns a lisp string object from a character string.
  If the structure pointer is NULL, one is mallocd.
  If the length is negative, it is computed. */ 
fdtype fd_init_string
  (struct FD_STRING *ptr,int slen,u8_string string)
{
  int len=((slen<0) ? (strlen(string)) : (slen));
  if (ptr == NULL) ptr=u8_malloc(sizeof(struct FD_STRING));
  FD_INIT_CONS(ptr,fd_string_type);
  ptr->length=len; ptr->bytes=string;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT
/* fd_extract_string:
    Arguments: A pointer to an FD_STRING struct, and two pointers to byte vectors
    Returns: a lisp string
  This returns a lisp string object from a region of a character string.
  If the structure pointer is NULL, one is mallocd.
  This copies the region between the pointers into a string and initializes
   a lisp string based on the region. */ 
fdtype fd_extract_string
  (struct FD_STRING *ptr,u8_byte *start,u8_byte *end)
{
  int len=((end==NULL) ? (strlen(start)) : (end-start));
  if (ptr == NULL) ptr=u8_malloc(sizeof(struct FD_STRING));
  FD_INIT_CONS(ptr,fd_string_type);
  ptr->length=len; ptr->bytes=u8_strndup(start,len+1);
  ptr->bytes[len]='\0';
  return FDTYPE_CONS(ptr);
}

FD_EXPORT
/* fdtype_string:
    Arguments: a C string (u8_string)
    Returns: a lisp string
  */ 
fdtype fdtype_string(u8_string string)
{
  return fd_init_string(NULL,-1,u8_strdup(string));
}


/* Pairs */

FD_EXPORT fdtype fd_init_pair(struct FD_PAIR *ptr,fdtype car,fdtype cdr)
{
  if (ptr == NULL) ptr=u8_malloc(sizeof(struct FD_PAIR));
  FD_INIT_CONS(ptr,fd_pair_type);
  ptr->car=car; ptr->cdr=cdr;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_make_pair(fdtype car,fdtype cdr)
{
  return fd_init_pair(NULL,fd_incref(car),fd_incref(cdr));
}

FD_EXPORT fdtype fd_make_list(int len,...)
{
  va_list args; int i=0;
  fdtype *elts=u8_malloc(sizeof(fdtype)*len), result=FD_EMPTY_LIST;
  va_start(args,len);
  while (i<len) elts[i++]=va_arg(args,fdtype);
  va_end(args);
  i=len-1; while (i>=0) {
    result=fd_init_pair(NULL,elts[i],result); i--;}
  u8_free(elts);
  return result;
}

/* Vectors */

FD_EXPORT fdtype fd_init_vector(struct FD_VECTOR *ptr,int len,fdtype *data)
{
  if (ptr == NULL) ptr=u8_malloc(sizeof(struct FD_VECTOR));
  FD_INIT_CONS(ptr,fd_vector_type);
  if ((data == NULL) && (len)) {
    int i=0; data=u8_malloc(sizeof(fdtype)*len);
    while (i < len) data[i++]=FD_VOID;}
  ptr->length=len; ptr->data=data;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_make_vector(int len,...)
{
  va_list args; int i=0;
  fdtype *elts=u8_malloc(sizeof(fdtype)*len), result=FD_EMPTY_LIST;
  va_start(args,len);
  while (i<len) elts[i++]=va_arg(args,fdtype);
  va_end(args);
  return fd_init_vector(NULL,len,elts);
}

/* Packets */

FD_EXPORT fdtype fd_init_packet
  (struct FD_STRING *ptr,int len,unsigned char *data)
{
  if (ptr == NULL) ptr=u8_malloc(sizeof(struct FD_STRING));
  FD_INIT_CONS(ptr,fd_packet_type);
  if (data == NULL) {
    int i=0; data=u8_malloc(len);
    while (i < len) data[i++]=0;}
  ptr->length=len; ptr->bytes=data;
  return FDTYPE_CONS(ptr);
}

/* Compounds */

FD_EXPORT fdtype fd_init_compound
  (struct FD_COMPOUND *ptr,fdtype tag,fdtype data)
{
  if (ptr == NULL) ptr=u8_malloc(sizeof(struct FD_COMPOUND));
  FD_INIT_CONS(ptr,fd_compound_type);
  ptr->tag=tag; ptr->data=data;
  return FDTYPE_CONS(ptr);
}

static void recycle_compound(struct FD_CONS *c)
{
  struct FD_COMPOUND *compound=(struct FD_COMPOUND *)c;
  fd_decref(compound->tag); fd_decref(compound->data);
  if (FD_MALLOCD_CONSP(c))
    u8_free_x(c,sizeof(struct FD_COMPOUND));
}

static int compare_compounds(fdtype x,fdtype y,int quick)
{
  struct FD_COMPOUND *xc=FD_GET_CONS(x,fd_compound_type,struct FD_COMPOUND *);
  struct FD_COMPOUND *yc=FD_GET_CONS(y,fd_compound_type,struct FD_COMPOUND *);
  int cmp;
  if (xc == yc) return 0;
  else if (cmp=FD_COMPARE(xc->tag,yc->tag,quick)) return cmp;
  else return FD_COMPARE(xc->data,yc->data,quick);
}

static int dtype_compound(struct FD_BYTE_OUTPUT *out,fdtype x)
{
  struct FD_COMPOUND *xc=FD_GET_CONS(x,fd_compound_type,struct FD_COMPOUND *);
  int n_bytes=1;
  fd_write_byte(out,dt_compound);
  n_bytes=n_bytes+fd_write_dtype(out,xc->tag);
  n_bytes=n_bytes+fd_write_dtype(out,xc->data);
  return n_bytes;
}

/* Exceptions */

FD_EXPORT fdtype fd_make_exception
  (fd_exception ex,u8_context cxt,
   u8_string details,fdtype irritant,fdtype backtrace)
{
  struct FD_EXCEPTION_OBJECT *exo=
    u8_malloc(sizeof(struct FD_EXCEPTION_OBJECT));
  FD_INIT_CONS(exo,fd_exception_type);
  exo->data.next=NULL; exo->data.cond=ex; exo->data.cxt=cxt;
  if (details) exo->data.details=u8_strdup(details);
  else exo->data.details=details;
  if (FD_CHECK_PTR(irritant)) 
    exo->data.irritant=fd_incref(irritant);
  else exo->data.irritant=FD_BADPTR;
  exo->backtrace=backtrace;
  return FDTYPE_CONS(exo);
}
FD_EXPORT fdtype fd_err
  (fd_exception ex,u8_context cxt,u8_string details,fdtype irritant)
{
  struct FD_EXCEPTION_OBJECT *exo=
    u8_malloc(sizeof(struct FD_EXCEPTION_OBJECT));
  FD_INIT_CONS(exo,fd_exception_type);
  exo->data.next=NULL; exo->data.cond=ex; exo->data.cxt=cxt;
  if (details) exo->data.details=u8_strdup(details);
  else exo->data.details=details;
  if (FD_CHECK_PTR(irritant)) 
    exo->data.irritant=fd_incref(irritant);
  else exo->data.irritant=FD_BADPTR;
  exo->backtrace=FD_EMPTY_LIST;
  return FDTYPE_CONS(exo);
}
FD_EXPORT fdtype fd_passerr(fdtype err,fdtype context)
{
  if (FD_EXCEPTIONP(err)) {
    struct FD_EXCEPTION_OBJECT *exo=
      FD_GET_CONS(err,fd_exception_type,struct FD_EXCEPTION_OBJECT *);
    exo->backtrace=fd_init_pair(NULL,context,exo->backtrace);
    return err;}
  else if (FD_TROUBLEP(err)) {
    struct FD_EXCEPTION_OBJECT *exo=u8_malloc_type(struct FD_EXCEPTION_OBJECT);
    exo->data.cond=fd_retcode_to_exception(err); exo->data.cxt=NULL;
    exo->data.details=NULL; exo->data.irritant=FD_VOID;
    exo->backtrace=fd_init_pair(NULL,context,FD_EMPTY_LIST);
    return err;}
  else {
    struct FD_EXCEPTION_OBJECT *exo=u8_malloc_type(struct FD_EXCEPTION_OBJECT);
    exo->data.cond=fd_UnknownError; exo->data.cxt=NULL;
    exo->data.details=NULL; exo->data.irritant=FD_VOID;
    exo->backtrace=fd_init_pair(NULL,context,FD_EMPTY_LIST);
    return err;}
}
FD_EXPORT fdtype fd_passerr2(fdtype err,fdtype context1,fdtype context2)
{
  struct FD_EXCEPTION_OBJECT *exo=
    FD_GET_CONS(err,fd_exception_type,struct FD_EXCEPTION_OBJECT *);
  exo->backtrace=
    fd_init_pair(NULL,context2,fd_init_pair(NULL,context1,exo->backtrace));
  return err;
}

FD_EXPORT fdtype fd_type_error
  (u8_string type_name,u8_context cxt,fdtype irritant)
{
  return fd_err(fd_TypeError,cxt,
		u8_mkstring(_("object is not a %m"),type_name),
		irritant);
}

FD_EXPORT void fd_set_type_error(u8_string type_name,fdtype irritant)
{
  struct U8_OUTPUT out; 
  return fd_seterr(fd_TypeError,type_name,NULL,irritant);
}

static void recycle_exception(struct FD_CONS *c)
{
  struct FD_EXCEPTION_OBJECT *exo=(struct FD_EXCEPTION_OBJECT *)c;
  u8_free(exo->data.details);
  fd_decref(exo->data.irritant); fd_decref(exo->backtrace);
  u8_free_x(exo,sizeof(struct FD_EXCEPTION_OBJECT));
}

static int dtype_exception(struct FD_BYTE_OUTPUT *out,fdtype x)
{
  struct FD_EXCEPTION_OBJECT *exo=(struct FD_EXCEPTION_OBJECT *)x;
  fdtype vector=fd_init_vector(NULL,4,NULL);
  int n_bytes;
  FD_VECTOR_SET(vector,0,fdtype_string((u8_string)exo->data.cond));
  if (exo->data.details) {
    FD_VECTOR_SET(vector,1,fdtype_string(exo->data.details));}
  else {FD_VECTOR_SET(vector,1,FD_VOID);}
  FD_VECTOR_SET(vector,2,fd_incref(exo->data.irritant));
  FD_VECTOR_SET(vector,3,fd_incref(exo->backtrace));
  fd_write_byte(out,dt_exception);
  n_bytes=1+fd_write_dtype(out,vector);
  fd_decref(vector);
  return n_bytes;
}

static int unparse_exception(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_EXCEPTION_OBJECT *xo=
    FD_GET_CONS(x,fd_exception_type,struct FD_EXCEPTION_OBJECT *);
  u8_printf(out,"#<!");
  fd_errout(out,&(xo->data));
  u8_printf(out,"!>");
  return 1;
}

static fdtype copy_exception(fdtype x)
{
  struct FD_EXCEPTION_OBJECT *xo=
    FD_GET_CONS(x,fd_exception_type,struct FD_EXCEPTION_OBJECT *);
  return fd_make_exception
    (xo->data.cond,xo->data.cxt,u8_strdup(xo->data.details),
     fd_incref(xo->data.irritant),fd_incref(xo->backtrace));
}

/* Mystery types */

static int unparse_mystery(u8_output out,fdtype x)
{
  struct FD_MYSTERY *d=FD_GET_CONS(x,fd_mystery_type,struct FD_MYSTERY *);
  char buf[128];
  if (d->code&0x80)
    sprintf(buf,_("#<MysteryVector 0x%x/0x%x %d elements>"),
	    d->package,d->code,d->size);
  else sprintf(buf,_("#<MysteryPacket 0x%x/0x%x %d bytes>"),
	       d->package,d->code,d->size);
  u8_puts(out,buf);
  return 1;
}

static void recycle_mystery(struct FD_CONS *c)
{
  struct FD_MYSTERY *myst=(struct FD_MYSTERY *)c;
  if (myst->code&0x80)
    u8_free(myst->payload.vector);
  else u8_free(myst->payload.packet);
  u8_free_x(myst,sizeof(struct FD_MYSTERY));
}

/* Registering new primitive types */

#if FD_THREADS_ENABLED
static u8_mutex type_registry_lock;
#endif

unsigned int fd_max_cons_type=FD_CONS_TYPECODE(FD_BUILTIN_CONS_TYPES);
unsigned int fd_max_immediate_type=FD_IMMEDIATE_TYPECODE(FD_BUILTIN_IMMEDIATE_TYPES);

FD_EXPORT int fd_register_cons_type(char *name)
{
  int typecode;
  u8_lock_mutex(&type_registry_lock);
  if (fd_max_cons_type>=FD_MAX_CONS_TYPE) {
    u8_unlock_mutex(&type_registry_lock);
    return -1;}
  fd_max_cons_type++;
  typecode=fd_max_cons_type;
  fd_type_names[typecode]=name;
  u8_unlock_mutex(&type_registry_lock);
  return typecode;
}

FD_EXPORT int fd_register_immediate_type(char *name,fd_checkfn fn)
{
  int typecode;
  u8_lock_mutex(&type_registry_lock);
  if (fd_max_immediate_type>=FD_MAX_IMMEDIATE_TYPE) {
    u8_unlock_mutex(&type_registry_lock);
    return -1;}
  typecode=fd_max_immediate_type;
  fd_immediate_checkfns[typecode-0x04]=fn;
  fd_max_immediate_type++;
  fd_type_names[typecode]=name;
  u8_unlock_mutex(&type_registry_lock);
  return typecode;
}

/* Compound type information */

struct FD_COMPOUND_ENTRY *fd_compound_entries=NULL;
#if FD_THREADS_ENABLED
static u8_mutex compound_registry_lock;
#endif

FD_EXPORT struct FD_COMPOUND_ENTRY *fd_register_compound(fdtype symbol)
{
  struct FD_COMPOUND_ENTRY *scan, *newrec;
  u8_lock_mutex(&compound_registry_lock);
  scan=fd_compound_entries;
  while (scan)
    if (FD_EQ(scan->tag,symbol)) {
      u8_unlock_mutex(&compound_registry_lock);
      return scan;}
    else scan=scan->next;
  newrec=u8_malloc(sizeof(struct FD_COMPOUND_ENTRY));
  newrec->next=fd_compound_entries; newrec->tag=symbol;
  newrec->parser=NULL; newrec->dump=NULL; newrec->restore=NULL;
  newrec->tablefns=NULL;
  fd_compound_entries=newrec;
  u8_unlock_mutex(&compound_registry_lock);
  return newrec;
}

FD_EXPORT struct FD_COMPOUND_ENTRY *fd_lookup_compound(fdtype symbol)
{
  struct FD_COMPOUND_ENTRY *scan=fd_compound_entries;
  while (scan)
    if (FD_EQ(scan->tag,symbol)) {
      return scan;}
    else scan=scan->next;
  return NULL;
}

/* Timestamps */

static fdtype timestamp_symbol, timestamp0_symbol;

FD_EXPORT
/* fd_make_timestamp:
    Arguments: a pointer to a U8_XTIME struct and a memory pool
    Returns: a dtype pointer to a timestamp
 */
fdtype fd_make_timestamp(struct U8_XTIME *tm,FD_MEMORY_POOL_TYPE *mp)
{
  struct FD_TIMESTAMP *tstamp=u8_pmalloc(mp,sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(tstamp,fd_timestamp_type);
  memcpy(&(tstamp->xtime),tm,sizeof(struct U8_XTIME));
  return FDTYPE_CONS(tstamp);
}

static int unparse_timestamp(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_TIMESTAMP *tm=
    FD_GET_CONS(x,fd_timestamp_type,struct FD_TIMESTAMP *);
  u8_printf(out,"#<TIMESTAMP 0x%x \"",(unsigned int)x);
  u8_xtime_to_iso8601(out,&(tm->xtime));
  u8_printf(out,"\">");
  return 1;
}

static fdtype timestamp_parsefn(FD_MEMORY_POOL_TYPE *pool,int n,fdtype *args)
{
  struct FD_TIMESTAMP *tm=u8_malloc(sizeof(struct FD_TIMESTAMP));
  u8_string timestring;
  FD_INIT_CONS(tm,fd_timestamp_type);
  if ((n==2) && (FD_STRINGP(args[1])))
    timestring=FD_STRDATA(args[1]);
  else if ((n==3) && (FD_STRINGP(args[2])))
    timestring=FD_STRDATA(args[2]);
  else return fd_err(fd_CantParseRecord,"TIMESTAMP",NULL,FD_VOID);
  u8_iso8601_to_xtime(timestring,&(tm->xtime));
  return FDTYPE_CONS(tm);
}

static void recycle_timestamp(struct FD_CONS *c)
{
  u8_free_x(c,sizeof(struct FD_TIMESTAMP));
}

static fdtype copy_timestamp(fdtype x)
{
  struct FD_TIMESTAMP *tm=
    FD_GET_CONS(x,fd_timestamp_type,struct FD_TIMESTAMP *);
  struct FD_TIMESTAMP *newtm=u8_malloc_type(struct FD_TIMESTAMP);
  FD_INIT_CONS(newtm,fd_timestamp_type);
  memcpy(&(newtm->xtime),&(tm->xtime),sizeof(struct U8_XTIME));
  return FDTYPE_CONS(newtm);
}

static int compare_timestamps(fdtype x,fdtype y,int quick)
{
  struct FD_TIMESTAMP *xtm=
    FD_GET_CONS(x,fd_timestamp_type,struct FD_TIMESTAMP *);
  struct FD_TIMESTAMP *ytm=
    FD_GET_CONS(y,fd_timestamp_type,struct FD_TIMESTAMP *);
  double diff=u8_xtime_diff(&(xtm->xtime),&(ytm->xtime));
  if (diff<0.0) return -1;
  else if (diff == 0.0) return 0;
  else return 1;
}

static int dtype_timestamp(struct FD_BYTE_OUTPUT *out,fdtype x)
{
  struct FD_TIMESTAMP *xtm=
    FD_GET_CONS(x,fd_timestamp_type,struct FD_TIMESTAMP *);
  int size=1;
  fd_write_byte(out,dt_compound);
  size=size+fd_write_dtype(out,timestamp_symbol);
  if ((xtm->xtime.precision == u8_second) && (xtm->xtime.tzoff==0)) {
    fdtype xval=FD_INT2DTYPE(xtm->xtime.secs);
    size=size+fd_write_dtype(out,xval);}
  else {
    fdtype vec=fd_init_vector(NULL,4,NULL); int n_bytes;
    FD_VECTOR_SET(vec,0,FD_INT2DTYPE(xtm->xtime.secs));
    FD_VECTOR_SET(vec,1,FD_INT2DTYPE(xtm->xtime.nsecs));
    FD_VECTOR_SET(vec,2,FD_INT2DTYPE((int)xtm->xtime.precision));
    FD_VECTOR_SET(vec,3,FD_INT2DTYPE((int)xtm->xtime.tzoff));
    size=size+fd_write_dtype(out,vec);}
  return size;
}

static fdtype timestamp_restore(FD_MEMORY_POOL_TYPE *p,fdtype tag,fdtype x)
{
  if (FD_FIXNUMP(x)) {
    struct FD_TIMESTAMP *tm=u8_pmalloc_type(p,struct FD_TIMESTAMP);
    FD_INIT_CONS(tm,fd_timestamp_type);
    u8_offtime(&(tm->xtime),FD_FIX2INT(x),0);
    return FDTYPE_CONS(tm);}
  else if (FD_BIGINTP(x)) {
    struct FD_TIMESTAMP *tm=u8_pmalloc_type(p,struct FD_TIMESTAMP);
    time_t tval=(time_t)(fd_bigint_to_long((fd_bigint)x));
    FD_INIT_CONS(tm,fd_timestamp_type);
    u8_offtime(&(tm->xtime),tval,0);
    return FDTYPE_CONS(tm);}
  else if (FD_VECTORP(x)) {
    struct FD_TIMESTAMP *tm=u8_pmalloc_type(p,struct FD_TIMESTAMP);
    int secs=fd_getint(FD_VECTOR_REF(x,0));
    int nsecs=fd_getint(FD_VECTOR_REF(x,1));
    int iprec=fd_getint(FD_VECTOR_REF(x,2));
    int tzoff=fd_getint(FD_VECTOR_REF(x,3));
    FD_INIT_CONS(tm,fd_timestamp_type);
    u8_offtime(&(tm->xtime),secs,tzoff);
    tm->xtime.nsecs=nsecs;
    tm->xtime.precision=iprec;
    return FDTYPE_CONS(tm);}
  else return fd_err(fd_DTypeError,"bad timestamp compound",NULL,x);
}

/* Utility functions (for debugging) */

FD_EXPORT struct FD_CONS *fd_cons_data(fdtype x)
{
  unsigned long as_int=(x&(~0x3));
  void *as_addr=(void *) as_int;
  return (struct FD_CONS *) as_addr;
}

FD_EXPORT struct FD_PAIR *fd_pair_data(fdtype x)
{
  return FD_STRIP_CONS(x,fd_pair_type,struct FD_PAIR *);
}


/* Initialization */

void fd_init_cons_c()
{
  int i;
#if FD_THREADS_ENABLED
  i=0; while (i < FD_N_PTRLOCKS) u8_init_mutex(&_fd_ptr_locks[i++]);
#endif

  fd_register_source_file(versionid);

#if FD_THREADS_ENABLED
  u8_init_mutex(&compound_registry_lock);
  u8_init_mutex(&type_registry_lock);
#endif

  i=0; while (i < FD_TYPE_MAX) fd_unparsers[i++]=NULL;
  i=0; while (i < FD_TYPE_MAX) fd_type_names[i++]=NULL;
  i=0; while (i < FD_TYPE_MAX) fd_recyclers[i++]=NULL;
  i=0; while (i < FD_TYPE_MAX) fd_dtype_writers[i++]=NULL;
  i=0; while (i < FD_TYPE_MAX) fd_comparators[i++]=NULL;
  i=0; while (i<FD_TYPE_MAX) fd_hashfns[i++]=NULL;
  i=0; while (i<64) fd_immediate_checkfns[i++]=NULL;

  fd_recyclers[fd_exception_type]=recycle_exception;
  fd_recyclers[fd_mystery_type]=recycle_mystery;
  fd_recyclers[fd_compound_type]=recycle_compound;

  fd_comparators[fd_compound_type]=compare_compounds;

  fd_copiers[fd_exception_type]=copy_exception;

  if (fd_dtype_writers[fd_exception_type]==NULL)
    fd_dtype_writers[fd_exception_type]=dtype_exception;

  if (fd_unparsers[fd_exception_type]==NULL)
    fd_unparsers[fd_exception_type]=unparse_exception;
  if (fd_unparsers[fd_mystery_type]==NULL)
    fd_unparsers[fd_mystery_type]=unparse_mystery;

  fd_dtype_writers[fd_compound_type]=dtype_compound;

  timestamp_symbol=fd_intern("TIMESTAMP");
  timestamp0_symbol=fd_intern("TIMESTAMP0");

  {
    struct FD_COMPOUND_ENTRY *e=fd_register_compound(timestamp_symbol);
    e->parser=timestamp_parsefn;
    e->dump=NULL;
    e->restore=timestamp_restore;}
    {
    struct FD_COMPOUND_ENTRY *e=fd_register_compound(timestamp0_symbol);
    e->parser=timestamp_parsefn;
    e->dump=NULL;
    e->restore=timestamp_restore;}
  fd_dtype_writers[fd_timestamp_type]=dtype_timestamp;
  fd_unparsers[fd_timestamp_type]=unparse_timestamp;
  fd_copiers[fd_timestamp_type]=copy_timestamp;
  fd_comparators[fd_timestamp_type]=compare_timestamps;
  fd_recyclers[fd_timestamp_type]=recycle_timestamp;

}


/* The CVS log for this file
   $Log: cons.c,v $
   Revision 1.92  2006/01/31 13:47:23  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.91  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.90  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.89  2006/01/07 23:12:46  haase
   Moved framerd object dtype handling into the main fd_read_dtype core, which led to substantial performanc improvements

   Revision 1.88  2006/01/07 18:26:46  haase
   Added pointer checking, both built in and configurable

   Revision 1.87  2006/01/05 19:43:54  haase
   Added GMT property for timestamps

   Revision 1.86  2005/12/28 23:03:29  haase
   Made choices be direct blocks of elements, including various fixes, simplifications, and more detailed documentation.

   Revision 1.85  2005/12/26 20:14:26  haase
   Further fixes to type reorganization

   Revision 1.84  2005/12/26 18:19:44  haase
   Reorganized and documented dtype pointers and conses

   Revision 1.83  2005/12/22 14:38:08  haase
   Add timestamp recycler

   Revision 1.82  2005/12/19 00:41:56  haase
   Free string conses even if they're zero length

   Revision 1.81  2005/12/17 17:36:14  haase
   Fixes to fd_deep_copy bugs with atoms and vectors and made fdtype_equal defer to numeric comparison before going to type specific functions

   Revision 1.80  2005/12/17 15:10:39  haase
   Fixed achoice comparison for fdtype_equal

   Revision 1.79  2005/12/17 05:58:38  haase
   Fixes to packet comparison

   Revision 1.78  2005/12/01 23:28:46  haase
   Fixed bug in vector and packet comparison

   Revision 1.77  2005/10/31 02:05:58  haase
   Fixed bug with choice comparison and made fdtype_equal do a quick comparison

   Revision 1.76  2005/10/10 16:53:49  haase
   Fixes for new mktime/offtime functions

   Revision 1.75  2005/09/18 03:58:04  haase
   Warning-based fixes for 64-bit compilation

   Revision 1.74  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.73  2005/07/13 23:07:45  haase
   Added global adjuncts and LISP access to adjunct declaration

   Revision 1.72  2005/06/27 16:54:30  haase
   Thread function improvements and addition of THREADJOIN primitive

   Revision 1.71  2005/06/19 19:40:04  haase
   Regularized use of FD_CONS_HEADER

   Revision 1.70  2005/06/11 16:34:22  haase
   Fixes to non-quick comparison of vectors and choices

   Revision 1.69  2005/06/04 16:52:06  haase
   Added MORPHRULE

   Revision 1.68  2005/05/30 18:04:43  haase
   Removed legacy numeric plugin layer

   Revision 1.67  2005/05/30 17:49:59  haase
   Distinguished FD_QCOMPARE and FDTYPE_COMPARE

   Revision 1.66  2005/05/27 21:27:19  haase
   Made error functions use FD_CHECK_PTR on irritants

   Revision 1.65  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.64  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.63  2005/04/24 01:56:48  haase
   Fix nasty bug in equals for strings

   Revision 1.62  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.61  2005/04/12 15:22:12  haase
   Fixed bug with dynamic type allocation

   Revision 1.60  2005/04/11 21:29:17  haase
   Made fd_passerr always return an object

   Revision 1.59  2005/04/10 01:10:53  haase
   Made fd_deep_copy return errors when not handled

   Revision 1.58  2005/04/09 01:12:30  haase
   Made unparse_exception return 1, indicating that it has handled the output

   Revision 1.57  2005/04/08 04:47:11  haase
   Made fd_type_error synthesize more useful details and ignore context

   Revision 1.56  2005/04/04 22:21:52  haase
   Improved integration of error facilities

   Revision 1.55  2005/04/03 06:21:32  haase
   Modifications to allow varieties of sloppy parsing

   Revision 1.54  2005/04/02 20:56:50  haase
   Fixes to timestamp parsing

   Revision 1.53  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.52  2005/03/29 01:51:24  haase
   Added U8_MUTEX_DECL and used it

   Revision 1.51  2005/03/28 19:09:39  haase
   Defined fd_make_timestamp to convert a U8_XTIME to a dtype pointer

   Revision 1.50  2005/03/24 17:16:16  haase
   Added DTYPE emitter for exceptions

   Revision 1.49  2005/03/23 03:39:35  haase
   Added DTYPE dumping and restore of timestamps

   Revision 1.48  2005/03/23 01:43:39  haase
   Moved timestamp structure into fdlisp core

   Revision 1.47  2005/03/22 20:24:01  haase
   Added some more conditions and conversion of immediate error return values

   Revision 1.46  2005/03/17 03:59:30  haase
   Fixed empty vector GC bug

   Revision 1.45  2005/03/16 22:22:46  haase
   Added bignums

   Revision 1.44  2005/03/14 05:49:31  haase
   Updated comments and internal documentation

   Revision 1.43  2005/03/11 14:43:54  haase
   Added more time functions

   Revision 1.42  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.41  2005/03/05 19:40:18  haase
   Lisp exception unparsing uses i18n catalogs

   Revision 1.40  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.39  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.38  2005/03/01 19:44:55  haase
   Removed harmless zero malloc call

   Revision 1.37  2005/02/23 22:49:27  haase
   Created generalized compound registry and made compound dtypes and #< reading use it

   Revision 1.36  2005/02/22 21:22:16  haase
   Added better FD_CHECK_PTR function

   Revision 1.35  2005/02/18 00:42:37  haase
   Added type checking for arithmetic operations

   Revision 1.34  2005/02/16 02:34:58  haase
   Various optmizations, etc

   Revision 1.33  2005/02/15 03:03:40  haase
   Updated to use the new libu8

   Revision 1.32  2005/02/14 02:08:38  haase
   Added dtype writer for compounds

   Revision 1.31  2005/02/14 01:30:45  haase
   IRemoved built in packet copier for better code coverage

   Revision 1.30  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
