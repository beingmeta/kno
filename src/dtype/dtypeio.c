/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_INLINE_DTYPEIO 1

#include "fdb/dtype.h"
#include "fdb/dtypeio.h"
#include <errno.h>

int (*fd_dtype_error)
     (struct FD_BYTE_OUTPUT *,fdtype x,u8_string details)=NULL;

fd_exception fd_UnexpectedEOD=_("Unexpected end of data");
fd_exception fd_DTypeError=_("Malformed DTYPE representation");
static fd_exception BadUnReadByte=_("Inconsistent read/unread byte");

#if FD_THREADS_ENABLED
static u8_mutex dtype_unpacker_lock;
#endif

static fdtype _return_errcode(fdtype x)
{
  return x;
}

#if 1
#define return_errcode(x) (_return_errcode(x))
#else
#define return_errcode(x) (x)
#endif

/* Byte output */

static int grow_byte_buffer(struct FD_BYTE_OUTPUT *b,int delta)
{
  unsigned int current_size=b->ptr-b->start;
  unsigned int current_limit=b->end-b->start, new_limit=current_limit;
  unsigned int need_size=current_size+delta;
  unsigned char *new;
  while (new_limit < need_size)
    if (new_limit>=0x40000) new_limit=new_limit+0x40000;
    else new_limit=new_limit*2;
  new=u8_prealloc(b->mpool,b->start,new_limit);
  if (new == NULL) return 0;
  b->start=new; b->ptr=new+current_size;
  b->end=b->start+new_limit;
  return 1;
}
int fd_needs_space(struct FD_BYTE_OUTPUT *b,int delta)
{
  if (b->ptr+delta > b->end)
    return grow_byte_buffer(b,delta);
  else return 1;
}

FD_EXPORT int _fd_write_byte(struct FD_BYTE_OUTPUT *b,unsigned char byte)
{
  if (fd_needs_space(b,1)) {*(b->ptr++)=byte; return 1;}
  else return -1;
}

FD_EXPORT int _fd_write_4bytes(struct FD_BYTE_OUTPUT *b,unsigned int w)
{
  if (fd_needs_space(b,4)==0)
    return -1;
#if FD_WORDS_ARE_ALIGNED
  *(b->ptr++)=(((w>>24)&0xFF));
  *(b->ptr++)=(((w>>16)&0xFF));
  *(b->ptr++)=(((w>>8)&0xFF));
  *(b->ptr++)=(((w>>0)&0xFF));
#else
  *((unsigned int *) (b->ptr))=fd_net_order(word);
  b->ptr=b->ptr+4;
#endif
  return 4;
}

FD_EXPORT int _fd_write_bytes
   (struct FD_BYTE_OUTPUT *b,unsigned char *data,int size)
{
  if (fd_needs_space(b,size)==0) return -1;
  memcpy(b->ptr,data,size); b->ptr=b->ptr+size;
  return size;
}

static int write_hashtable(struct FD_BYTE_OUTPUT *out,struct FD_HASHTABLE *v);
static int write_hashset(struct FD_BYTE_OUTPUT *out,struct FD_HASHSET *v);
static int write_slotmap(struct FD_BYTE_OUTPUT *out,struct FD_SLOTMAP *v);
static int write_schemap(struct FD_BYTE_OUTPUT *out,struct FD_SCHEMAP *v);
static int write_mystery(struct FD_BYTE_OUTPUT *out,struct FD_MYSTERY *v);

#define output_byte(out,b) \
  if (fd_write_byte(out,b)<0) return -1; else {}
#define output_4bytes(out,w) \
  if (fd_write_4bytes(out,w)<0) return -1; else {}
#define output_bytes(out,bytes,n)				\
  if (fd_write_bytes(out,bytes,n)<0) return -1; else {}
static int try_dtype_output(int *len,struct FD_BYTE_OUTPUT *out,fdtype x)
{
  int olen=out->ptr-out->start;
  int dlen=fd_write_dtype(out,x);
  if (dlen<0)
    return -1;
  else if ((olen+dlen) != (out->ptr-out->start))
    u8_warn("trouble","inconsistent dlen");
  *len=*len+dlen;
  return dlen;
}
#define output_dtype(len,out,x) \
  if (try_dtype_output(&len,out,x)<0) return -1; else {}

FD_EXPORT int fd_write_dtype(struct FD_BYTE_OUTPUT *out,fdtype x)
{
  switch (FD_PTR_MANIFEST_TYPE(x)) {
  case fd_oid_ptr_type: { /* output OID */
    FD_OID addr=FD_OID_ADDR(x);
    output_byte(out,dt_oid);
    output_4bytes(out,FD_OID_HI(addr));
    output_4bytes(out,FD_OID_LO(addr));
    return 9;}
  case fd_fixnum_ptr_type: { /* output fixnum */
    int val=FD_FIX2INT(x);
    output_byte(out,dt_fixnum);
    output_4bytes(out,val);
    return 5;}
  case fd_immediate_ptr_type: { /* output constant */
    fd_ptr_type itype=FD_IMMEDIATE_TYPE(x);
    int data=FD_GET_IMMEDIATE(x,itype);
    if (itype == fd_symbol_type) { /* output symbol */
      fdtype name=fd_symbol_names[data];
      struct FD_STRING *s=FD_GET_CONS(name,fd_string_type,struct FD_STRING *);
      {output_byte(out,dt_symbol);}
      {output_4bytes(out,s->length);}
      {output_bytes(out,s->bytes,s->length);}
      return s->length+5;}
    else if (itype == fd_character_type) { /* Output unicode character */
      output_byte(out,dt_character_package);
      if (data<128) {
	output_byte(out,dt_ascii_char);
	output_byte(out,1); output_byte(out,data);
	return 4;}
      else {
	output_byte(out,dt_unicode_char);
	if (data<0x100) {
	  output_byte(out,1);
	  output_byte(out,data);
	  return 4;}
	else if (data<0x10000) {
	  output_byte(out,2);
	  output_byte(out,((data>>8)&0xFF));
	  output_byte(out,(data&0xFF));
	  return 5;}
	else if (data<0x1000000) {
	  output_byte(out,3);
	  output_byte(out,((data>>16)&0xFF));
	  output_byte(out,((data>>8)&0xFF));
	  output_byte(out,(data&0xFF));
	  return 6;}
	else {
	  output_byte(out,3); output_4bytes(out,data);
	  return 7;}}
      return 5;}
    else switch (data) {
    case 0: output_byte(out,dt_void); return 1;
    case 1: output_byte(out,dt_boolean); output_byte(out,0); return 2;
    case 2: output_byte(out,dt_boolean); output_byte(out,1); return 2;
    case 3:
      output_byte(out,dt_framerd_package);
      output_byte(out,dt_small_choice);
      output_byte(out,0);
      return 3;
    case 4: output_byte(out,dt_empty_list); return 1;
    default:
      fd_seterr(_("Invalid constant"),NULL,NULL,x);
      return -1;}
    break;}
  case fd_cons_ptr_type: {/* output cons */
    struct FD_CONS *cons=FD_CONS_DATA(x);
    int ctype=FD_CONS_TYPE(cons);
    switch (ctype) {
    case fd_string_type: {
      struct FD_STRING *s=(struct FD_STRING *) cons;
      output_byte(out,dt_string);
      output_4bytes(out,s->length);
      output_bytes(out,s->bytes,s->length);
      return 5+s->length;}
    case fd_packet_type: {
      struct FD_STRING *s=(struct FD_STRING *) cons;
      output_byte(out,dt_packet);
      output_4bytes(out,s->length);
      output_bytes(out,s->bytes,s->length);
      return 5+s->length;}
    case fd_pair_type: {
      unsigned int len=1;
      struct FD_PAIR *p=(struct FD_PAIR *) cons; 
      output_byte(out,dt_pair);
      output_dtype(len,out,p->car);
      output_dtype(len,out,p->cdr);
      return len;}
    case fd_rational_type:  case fd_complex_type: {
      fdtype car, cdr;
      unsigned int len=1;
      if (ctype == fd_rational_type) {
	_fd_unpack_rational((fdtype)cons,&car,&cdr);
	output_byte(out,dt_rational);}
      else {
	_fd_unpack_complex((fdtype)cons,&car,&cdr);
	output_byte(out,dt_complex);}
      output_dtype(len,out,car);
      output_dtype(len,out,cdr);
      return len;}
    case fd_vector_type: {
      struct FD_VECTOR *v=(struct FD_VECTOR *) cons;
      int i=0, length=v->length, dtype_len=5;
      output_byte(out,dt_vector);
      output_4bytes(out,length);
      while (i < length) {
	output_dtype(dtype_len,out,v->data[i]); i++;}
      return dtype_len;}
    case fd_choice_type: {
      struct FD_CHOICE *v=(struct FD_CHOICE *) cons;
      const fdtype *data=FD_XCHOICE_DATA(v);
      int i=0, len=FD_CHOICE_SIZE(x), dtype_len;
      output_byte(out,dt_framerd_package);
      if (len < 256) {
	dtype_len=3;
	output_byte(out,dt_small_choice);
	output_byte(out,len);}
      else {
	dtype_len=6;
	output_byte(out,dt_choice);
	output_4bytes(out,len);}
      while (i < len) {
	output_dtype(dtype_len,out,data[i]); i++;}
      return dtype_len;}
    case fd_qchoice_type: {
      struct FD_QCHOICE *qv=(struct FD_QCHOICE *) cons;
      if (FD_EMPTY_CHOICEP(qv->choice)) {
	output_byte(out,dt_framerd_package);
	output_byte(out,dt_small_qchoice);
	output_byte(out,0);
	return 3;}
      else {
	struct FD_CHOICE *v=(struct FD_CHOICE *) (qv->choice);
	const fdtype *data=FD_XCHOICE_DATA(v);
	int i=0, len=FD_XCHOICE_SIZE(v), dtype_len;
	
	if (len < 256) {
	dtype_len=3;
	output_byte(out,dt_small_qchoice);
	output_byte(out,len);}
	else {
	  dtype_len=6;
	  output_byte(out,dt_qchoice);
	  output_4bytes(out,len);}
      while (i < len) {
	output_dtype(dtype_len,out,data[i]); i++;}
      return dtype_len;}}
    case fd_hashset_type: 
      return write_hashset(out,(struct FD_HASHSET *) cons);
    case fd_slotmap_type: 
      return write_slotmap(out,(struct FD_SLOTMAP *) cons);
    case fd_schemap_type: 
      return write_schemap(out,(struct FD_SCHEMAP *) cons);
    case fd_hashtable_type: 
      return write_hashtable(out,(struct FD_HASHTABLE *) cons);
    case fd_mystery_type: 
      return write_mystery(out,(struct FD_MYSTERY *) cons);
    default: {
      fd_ptr_type ctype=FD_CONS_TYPE(cons); int dtype_len;
      if ((ctype < FD_TYPE_MAX) && (fd_dtype_writers[ctype]))
	return fd_dtype_writers[ctype](out,x);
      else if ((fd_dtype_error) &&
	       (dtype_len=fd_dtype_error(out,x,"no handler"))) 
	return dtype_len;
      else {
	fd_seterr(fd_NoMethod,_("Can't write DTYPE"),NULL,x);
	return -1;}}
    }}
  }
}

static int write_mystery(struct FD_BYTE_OUTPUT *out,struct FD_MYSTERY *v)
{
  int size=v->size, dtype_size=2;
  int vectorp=(v->code)&0x80;
  output_byte(out,v->package);
  if (size>256) {
    output_byte(out,v->code|0x40);
    output_4bytes(out,size);
    dtype_size=dtype_size+4;}
  else {
    output_byte(out,v->code&(~0x40));
    output_byte(out,size);
    dtype_size=dtype_size+1;}
  if (vectorp) {
    fdtype *elts=v->payload.vector, *limit=elts+size;
    while (elts<limit) {
      output_dtype(dtype_size,out,*elts); elts++;}
    return dtype_size;}
  else {
    output_bytes(out,v->payload.packet,size);
    return dtype_size+size;}
}

static int write_slotmap(struct FD_BYTE_OUTPUT *out,struct FD_SLOTMAP *v)
{
  int dtype_len;
  fd_lock_mutex(&(v->lock));
  {
    struct FD_KEYVAL *keyvals=v->keyvals;
    int i=0, kvsize=FD_XSLOTMAP_SIZE(v), len=kvsize*2;
    output_byte(out,dt_framerd_package);
    if (len < 256) {
      dtype_len=3;
      output_byte(out,dt_small_slotmap);
      output_byte(out,len);}
    else {
      dtype_len=6;
      output_byte(out,dt_slotmap);
      output_4bytes(out,len);}
    while (i < kvsize) {
      output_dtype(dtype_len,out,keyvals[i].key);
      output_dtype(dtype_len,out,keyvals[i].value); i++;}}
  fd_unlock_mutex(&(v->lock));
  return dtype_len;
}

static int write_schemap(struct FD_BYTE_OUTPUT *out,struct FD_SCHEMAP *v)
{
  int dtype_len;
  fd_lock_mutex(&(v->lock));
  {
    fdtype *schema=v->schema, *values=v->values;
    int i=0, schemasize=FD_XSCHEMAP_SIZE(v), len=schemasize*2;
    output_byte(out,dt_framerd_package);
    if (len < 256) {
      dtype_len=3;
      output_byte(out,dt_small_slotmap);
      output_byte(out,len);}
    else {
      dtype_len=6;
      output_byte(out,dt_slotmap);
      output_4bytes(out,len);}
    while (i < schemasize) {
      output_dtype(dtype_len,out,schema[i]);
      output_dtype(dtype_len,out,values[i]); i++;}}
  fd_unlock_mutex(&(v->lock));
  return dtype_len;
}

static int write_hashtable(struct FD_BYTE_OUTPUT *out,struct FD_HASHTABLE *v)
{
  int dtype_len;
  fd_lock_mutex(&(v->lock));
  {
    int size=v->n_keys;
    struct FD_HASHENTRY **scan=v->slots, **limit=scan+v->n_slots;
    output_byte(out,dt_framerd_package);
    if (size < 128) {
      dtype_len=3;
      output_byte(out,dt_small_hashtable);
      output_byte(out,size*2);}
    else {
      dtype_len=6;
      output_byte(out,dt_hashtable);
      output_4bytes(out,size*2);}
    scan=v->slots; limit=scan+v->n_slots;
    while (scan < limit)
      if (*scan) {
	struct FD_HASHENTRY *he=*scan++;
	struct FD_KEYVAL *kscan=&(he->keyval0), *klimit=kscan+he->n_keyvals;
	while (kscan < klimit) {
	  output_dtype(dtype_len,out,kscan->key);
	  output_dtype(dtype_len,out,kscan->value);
	  kscan++;}}
      else scan++;}
  fd_unlock_mutex(&(v->lock));
  return dtype_len;
}

static int write_hashset(struct FD_BYTE_OUTPUT *out,struct FD_HASHSET *v)
{
  int dtype_len;
  fd_lock_mutex(&(v->lock));
  {
    int size=v->n_keys;
    fdtype *scan=v->slots, *limit=scan+v->n_slots;
    output_byte(out,dt_framerd_package);
    if (size < 128) {
      dtype_len=3;
      output_byte(out,dt_small_hashset);
      output_byte(out,size);}
    else {
      dtype_len=6;
      output_byte(out,dt_hashset);
      output_4bytes(out,size);}
    scan=v->slots; limit=scan+v->n_slots;
    while (scan < limit)
      if (*scan) {
	output_dtype(dtype_len,out,*scan); scan++;}
      else scan++;}
  fd_unlock_mutex(&(v->lock));
  return dtype_len;
}

#define newpos(pos,ptr,lim) ((ptr+pos <= lim) ? (pos) : (-1)) 

static int validate_dtype(int pos,unsigned char *ptr,unsigned char *lim)
{
  if (pos < 0) return pos;
  else if (ptr+pos >= lim) return -1;
  else {
    int code=ptr[pos];
    switch (code) {
    case dt_empty_list: case dt_void:
      return pos+1;
    case dt_boolean:
      return newpos(pos+2,ptr,lim);
    case dt_fixnum: case dt_flonum:
      return newpos(pos+5,ptr,lim);
    case dt_oid: return newpos(pos+9,ptr,lim);
    case dt_error: case dt_exception:
      return validate_dtype(newpos(pos+1,ptr,lim),ptr,lim);
    case dt_pair: case dt_compound: case dt_rational: case dt_complex:
      return validate_dtype(validate_dtype(validate_dtype(pos+1,ptr,lim),
					   ptr,lim),
			    ptr,lim);
    case dt_symbol: case dt_packet: case dt_string: case dt_zstring:
      if (ptr+pos+5 >= lim) return -1;
      else {
	int len=fd_get_4bytes(ptr+pos+1);
	return newpos(pos+5+len,ptr,lim);}
    case dt_vector:
      if (ptr+pos+5 >= lim) return -1;
      else {
	int i=0, len=fd_get_4bytes(ptr+pos+1), npos=pos+5;
	while ((i < len) && (npos > 0)) npos=validate_dtype(npos,ptr,lim);
	return npos;}
    default:
      if ((code > 0x40) && (ptr+pos+2 < lim)) {
	int subcode=*(ptr+pos+1), len, npos;
	if (subcode&0x40)
	  if (ptr+pos+6 < lim) {
	    npos=pos+6; len=fd_get_4bytes(ptr+pos+2);}
	  else return -1;
	else if (ptr+pos+3 < lim) {
	  npos=pos+3; len=fd_get_byte(ptr+pos+2);}
	else return -1;
	if (subcode&0x80) {
	  int i=0; while ((i < len) && (npos > 0)) {
	    npos=validate_dtype(npos,ptr,lim); i++;}
	  return npos;}
	else return newpos(npos+len,ptr,lim);}
      else return -1;}}
}
 
static int fd_validate_dtype(struct FD_BYTE_INPUT *in)
{
  return validate_dtype(0,in->ptr,in->end);
}

/* Resurrecting compounds */

/* Byte input */

#define nobytes(in,nbytes) (FD_EXPECT_FALSE(!(fd_needs_bytes(in,nbytes))))
#define havebytes(in,nbytes) (FD_EXPECT_TRUE(fd_needs_bytes(in,nbytes)))

static u8_string tostring(fdtype x)
{
  if (FD_STRINGP(x))
    return u8_strdup(FD_STRDATA(x));
  else if (FD_SYMBOLP(x))
    return u8_strdup(FD_SYMBOL_NAME(x));
  else return fd_dtype2string(x);
}

FD_EXPORT fdtype fd_make_mystery_packet
   (FD_MEMORY_POOL_TYPE *,int,int,unsigned int,unsigned char *);
FD_EXPORT fdtype fd_make_mystery_vector
   (FD_MEMORY_POOL_TYPE *,int,int,unsigned int,fdtype *);
static fdtype read_packaged_dtype
   (int,struct FD_BYTE_INPUT *,FD_MEMORY_POOL_TYPE *);

static fdtype *read_dtypes
  (int n,struct FD_BYTE_INPUT *in,FD_MEMORY_POOL_TYPE *p,fdtype *why_not)
{
  if (n==0) return NULL;
  else {
    fdtype *vec=u8_malloc(sizeof(fdtype)*n);
    int i=0; while (i < n) {
      fdtype v=fd_read_dtype(in,p);
      if (FD_COOLP(v)) vec[i++]=v;
      else {
	int j=0; while (j<i) {fd_decref(vec[j]); j++;}
	u8_free(vec); *why_not=v;
	return NULL;}}
    return vec;}
}

FD_EXPORT fdtype fd_read_dtype
  (struct FD_BYTE_INPUT *in,FD_MEMORY_POOL_TYPE *p)
{
  if (havebytes(in,1)) {
    int code=*(in->ptr++);
    switch (code) {
    case dt_empty_list: return FD_EMPTY_LIST;
    case dt_void: return FD_VOID;
    case dt_boolean:
      if (nobytes(in,1))
	return return_errcode(FD_EOD);
      else if (*(in->ptr++))
	return FD_TRUE;
      else return FD_FALSE;
    case dt_fixnum: 
      if (havebytes(in,4)) {
	int intval=fd_get_4bytes(in->ptr); in->ptr=in->ptr+4;
	return FD_INT2DTYPE(intval);}
      else return return_errcode(FD_EOD);
    case dt_flonum: {
      char bytes[4];
      float *f=(float *)&bytes; double flonum;
      unsigned int *i=(unsigned int *)&bytes, num;
      num=fd_read_4bytes(in);
      *i=num; flonum=*f;
      return _fd_make_double(p,flonum);}
    case dt_oid: {
      FD_OID addr;
      FD_SET_OID_HI(addr,fd_read_4bytes(in));
      FD_SET_OID_LO(addr,fd_read_4bytes(in));
      return fd_make_oid(addr);}
#if 0
    case dt_error: {
      fdtype content=fd_read_dtype(in,p);
      return fd_init_compound
	(u8_pmalloc(p,sizeof(struct FD_COMPOUND)),FD_ERROR_TAG,content);}
    case dt_exception: {
      fdtype content=fd_read_dtype(in,p);
      return fd_init_compound
	(u8_pmalloc(p,sizeof(struct FD_COMPOUND)),FD_EXCEPTION_TAG,content);}
#endif
    case dt_error: case dt_exception: {
      /* Return an exception object if possible (content as expected)
	 and a compound if there are any big surprises */
      fdtype content=fd_read_dtype(in,p);
      u8_string exname="Unknown error", details=NULL;
      fdtype irritant=FD_VOID, exo;
      if (FD_TROUBLEP(content)) return content;
      else if (FD_VECTORP(content)) {
	int len=FD_VECTOR_LENGTH(content);
	if (len>0) exname=tostring(FD_VECTOR_REF(content,0));
	if (len>1) details=tostring(FD_VECTOR_REF(content,1));
	if (len==3) {
	  irritant=FD_VECTOR_REF(content,2); fd_decref(content);}
	else return fd_make_exception
	       (exname,NULL,details,content,FD_EMPTY_LIST);}
      else if (FD_PAIRP(content)) {
	exname=tostring(FD_CAR(content));
	if (FD_PAIRP(FD_CDR(content))) {
	  details=tostring(FD_CADR(content));
	  if ((FD_PAIRP(FD_CDR(FD_CDR(content)))) &&
	      (FD_EMPTY_LISTP(FD_CDR(FD_CDR(FD_CDR(content))))))
	    irritant=fd_incref(FD_CAR(FD_CDR(FD_CDR(content))));
	  else irritant=fd_incref(FD_CDR(FD_CDR(content)));}}
      else if (code == dt_exception)
	return fd_init_compound
	  (u8_pmalloc(p,sizeof(struct FD_COMPOUND)),
	   FD_EXCEPTION_TAG,content);
      else return fd_init_compound
	     (u8_pmalloc(p,sizeof(struct FD_COMPOUND)),FD_ERROR_TAG,content);
      exo=fd_make_exception(exname,NULL,details,irritant,FD_EMPTY_LIST);
      fd_decref(content);
      return exo;}
    case dt_pair: {
      fdtype head=FD_EMPTY_LIST, *tail=&head;
      while (1) {
	fdtype car=fd_read_dtype(in,p);
	if (FD_ABORTP(car)) {
	  fd_decref(head); return car;}
	else {
	  fdtype new_pair=
	    fd_init_pair(u8_pmalloc(p,sizeof(struct FD_PAIR)),
			 car,FD_EMPTY_LIST);
	  int dtcode=fd_read_byte(in);
	  *tail=new_pair; tail=&(FD_CDR(new_pair));
	  if (dtcode != dt_pair) {
	    fdtype cdr;
	    if (fd_unread_byte(in,dtcode)<0) {
	      fd_decref(head);
	      return fd_erreify();}
	    cdr=fd_read_dtype(in,p);
	    if (FD_ABORTP(cdr)) {
	      fd_decref(head); return cdr;}
	    *tail=cdr;
	    return head;}}}}
    case dt_compound: case dt_rational: case dt_complex: {
      fdtype car=fd_read_dtype(in,p), cdr;
      if (FD_TROUBLEP(car)) return car;
      cdr=fd_read_dtype(in,p);
      if (FD_TROUBLEP(cdr)) {fd_decref(car); return cdr;}
      switch (code) {
      case dt_compound: {
	struct FD_COMPOUND_ENTRY *e=fd_lookup_compound(car);
	if ((e) && (e->restore)) {
	  fdtype result=e->restore(p,car,cdr);
	  fd_decref(cdr);
	  return result;}
	else return fd_init_compound
	       (u8_pmalloc(p,sizeof(struct FD_COMPOUND)),car,cdr);}
      case dt_rational:
	return _fd_make_rational(p,car,cdr);
      case dt_complex:
	return _fd_make_complex(p,car,cdr);}}
    case dt_packet: case dt_string:
      if (nobytes(in,4)) return return_errcode(FD_EOD);
      else {
	int len=fd_get_4bytes(in->ptr); in->ptr=in->ptr+4;
	if (nobytes(in,len)) return return_errcode(FD_EOD);
	else {
	  unsigned char *data=u8_pmalloc(p,len+1);
	  memcpy(data,in->ptr,len); data[len]='\0'; in->ptr=in->ptr+len;
	  switch (code) {
	  case dt_string:
	    return fd_init_string(u8_pmalloc(p,sizeof(struct FD_STRING)),
				  len,data);
	  case dt_packet:
	    return fd_init_packet(u8_pmalloc(p,sizeof(struct FD_STRING)),
				  len,data);}}}
    case dt_symbol: case dt_zstring:
      if (nobytes(in,4)) return return_errcode(FD_EOD);
      else {
	int len=fd_get_4bytes(in->ptr); in->ptr=in->ptr+4;
	if (nobytes(in,len)) return return_errcode(FD_EOD);
	else {
	  unsigned char buf[64], *data; fdtype result;
	  if (len >= 64) data=u8_pmalloc(p,len); else data=buf;
	  memcpy(data,in->ptr,len); data[len]='\0'; in->ptr=in->ptr+len;
	  switch (code) {
	  case dt_symbol:
	    result=fd_make_symbol(data,len);
	  case dt_zstring:
	    result=fd_make_symbol(data,len);}
	  if (data != buf) u8_pfree_x(p,data,len);
	  return result;}}
    case dt_vector:
      if (nobytes(in,4)) return return_errcode(FD_EOD);
      else {
	int i=0, len=fd_read_4bytes(in);
	if (FD_EXPECT_FALSE(len == 0))
	  return fd_init_vector
	    (u8_pmalloc(p,sizeof(struct FD_VECTOR)),0,NULL);
	else {
	  fdtype why_not=FD_EOD, *data=read_dtypes(len,in,p,&why_not);
	  if (FD_EXPECT_TRUE((data!=NULL)))
	    return fd_init_vector(u8_pmalloc(p,sizeof(struct FD_VECTOR)),
				  len,data);
	  else return return_errcode(why_not);}}
    case dt_framerd_package: {
      int code, lenlen, len;
      if (nobytes(in,2)) return return_errcode(FD_EOD);
      code=*(in->ptr++); lenlen=((code&0x40) ? 4 : 1); 
      if (nobytes(in,lenlen)) return return_errcode(FD_EOD);
      else if (lenlen==4) len=fd_read_4bytes(in);
      else len=fd_read_byte(in);
      switch (code) {
      case dt_qchoice: case dt_small_qchoice:
	if (len==0)
	  return fd_init_qchoice(u8_pmalloc(p,sizeof(struct FD_QCHOICE)),FD_EMPTY_CHOICE);
      case dt_choice: case dt_small_choice:
	if (len==0) return FD_EMPTY_CHOICE;
	else {
	  fdtype result;
	  struct FD_CHOICE *ch=fd_palloc_choice(p,len);
	  fdtype *write=(fdtype *)FD_XCHOICE_DATA(ch), *limit=write+len;
	  while (write<limit) *write++=fd_read_dtype(in,p);
	  result=fd_init_choice(ch,len,NULL,(FD_CHOICE_DOSORT|FD_CHOICE_REALLOC));
	  if (FD_CHOICEP(result))
	    if ((code==dt_qchoice) || (code==dt_small_qchoice))
	      return fd_init_qchoice(u8_pmalloc(p,sizeof(struct FD_QCHOICE)),result);
	    else return result;
	  else return result;}
      case dt_slotmap: case dt_small_slotmap:
	if (len==0) 
	  return fd_init_slotmap(u8_pmalloc(p,sizeof(struct FD_SLOTMAP)),
				 0,NULL,p);
	else {
	  int n_slots=len/2;
	  struct FD_KEYVAL *keyvals=
	    u8_malloc(n_slots*sizeof(struct FD_KEYVAL));
	  struct FD_KEYVAL *write=keyvals, *limit=keyvals+n_slots;
	  while (write<limit) {
	    write->key=fd_read_dtype(in,p);
	    write->value=fd_read_dtype(in,p);
	    write++;}
	  return fd_init_slotmap(u8_pmalloc(p,sizeof(struct FD_SLOTMAP)),
				 n_slots,keyvals,p);}
      case dt_hashtable: case dt_small_hashtable:
	if (len==0) 
	  return fd_init_hashtable(u8_pmalloc(p,sizeof(struct FD_HASHTABLE)),
				 0,NULL,p);
	else {
	  fdtype result; int n_slots=len/2, n_read=0;
	  struct FD_KEYVAL *keyvals=
	    u8_malloc(n_slots*sizeof(struct FD_KEYVAL));
	  struct FD_KEYVAL *write=keyvals, *scan=keyvals,
	    *limit=keyvals+n_slots;
	  while (n_read<n_slots) {
	    write->key=fd_read_dtype(in,p);
	    write->value=fd_read_dtype(in,p);
	    n_read++;
	    if (FD_EMPTY_CHOICEP(write->value)) {
	      fd_decref(write->key);}
	    else write++;}
	  limit=write;
	  result=fd_init_hashtable(u8_pmalloc(p,sizeof(struct FD_HASHTABLE)),
				   limit-keyvals,keyvals,p);
	  while (scan<limit) {
	    fd_decref(scan->key); fd_decref(scan->value); scan++;}
	  u8_free(keyvals);
	  return result;}
      case dt_hashset: case dt_small_hashset: {
	int i=0; struct FD_HASHSET *h=u8_pmalloc(p,sizeof(struct FD_HASHSET));
	fd_init_hashset(h,len);
	while (i<len) {
	  fdtype v=fd_read_dtype(in,p);
	  fd_hashset_init_add(h,v);
	  i++;}
	return FDTYPE_CONS(h);}
      default: {
	int i=0; fdtype *data=u8_malloc(sizeof(fdtype)*len);
	while (i<len) data[i++]=fd_read_dtype(in,p);
	return fd_make_mystery_vector(p,dt_framerd_package,code,len,data);}}}
    default: 
      if ((code >= 0x40) && (code < 0x80))
	return read_packaged_dtype(code,in,p);
      else return FD_DTYPE_ERROR;}}
  else return return_errcode(FD_EOD);
}

/* Vector and packet unpackers */

struct  FD_DTYPE_PACKAGE {
  fd_packet_unpacker packetfns[64];
  fd_vector_unpacker vectorfns[64];};
struct FD_DTYPE_PACKAGE *dtype_packages[64];

FD_EXPORT int fd_register_vector_unpacker
  (unsigned int package,unsigned int code,fd_vector_unpacker f)
{
  int package_offset=package-0x40, code_offset=((code-0x80)&(~(0x40)));
  int replaced=0;
  if ((package<0x40) || (package>0x80)) return -1;
  else if ((code&0x80)==0) return -2;
  fd_lock_mutex(&dtype_unpacker_lock);
  if (dtype_packages[package_offset]==NULL) {
    struct FD_DTYPE_PACKAGE *pkg=
      dtype_packages[package_offset]=
      u8_malloc(sizeof(struct FD_DTYPE_PACKAGE));
    memset(pkg,0,sizeof(struct FD_DTYPE_PACKAGE));
    replaced=2;}
  else if (dtype_packages[package_offset]->vectorfns[code_offset])
    replaced=1;
  dtype_packages[package_offset]->vectorfns[code_offset]=f;
  fd_unlock_mutex(&dtype_unpacker_lock);
  return replaced;
}
  
FD_EXPORT int fd_register_packet_unpacker
  (unsigned int package,unsigned int code,fd_packet_unpacker f)
{
  int package_offset=package-0x40, code_offset=((code)&(~(0x40)));
  int replaced=0;
  if ((package<0x40) || (package>0x80)) return -1;
  else if ((code&0x80)) return -2;
  fd_lock_mutex(&dtype_unpacker_lock);
  if (dtype_packages[package_offset]==NULL) {
    struct FD_DTYPE_PACKAGE *pkg=
      dtype_packages[package_offset]=
      u8_malloc(sizeof(struct FD_DTYPE_PACKAGE));
    memset(pkg,0,sizeof(struct FD_DTYPE_PACKAGE));
    replaced=2;}
  else if (dtype_packages[package_offset]->packetfns[code_offset])
    replaced=1;
  dtype_packages[package_offset]->packetfns[code_offset]=f;
  fd_unlock_mutex(&dtype_unpacker_lock);
  return replaced;
}

/* Reading packaged dtypes */

static fdtype make_framerd_type
  (FD_MEMORY_POOL_TYPE *p,int code,int len,fdtype *data);
static fdtype make_character_type
  (FD_MEMORY_POOL_TYPE *p,int code,int len,unsigned char *data);

static fdtype read_packaged_dtype
   (int package,struct FD_BYTE_INPUT *in,FD_MEMORY_POOL_TYPE *p)
{
  fdtype result, *vector; unsigned char *packet;
  unsigned int code, lenlen, len, vectorp;
  if (nobytes(in,2)) return return_errcode(FD_EOD);
  code=*(in->ptr++); lenlen=((code&0x40) ? 4 : 1); vectorp=(code&0x80);
  if (nobytes(in,lenlen)) return return_errcode(FD_EOD);
  else if (lenlen==4) len=fd_read_4bytes(in);
  else len=fd_read_byte(in);
  if (vectorp) {
    fdtype why_not;
    vector=read_dtypes(len,in,p,&why_not);
    if ((len>0) && (vector==NULL)) return return_errcode(why_not);}
  else if (nobytes(in,len)) return return_errcode(FD_EOD);
  else {
    packet=u8_pmalloc(p,len);
    memcpy(packet,in->ptr,len); in->ptr=in->ptr+len;}
  switch (package) {
#if 0
  case dt_framerd_package:
    if (vectorp)
      return make_framerd_type(p,code,len,vector);
    else return fd_make_mystery_packet(p,package,code,len,packet);
#endif
  case dt_character_package:
    if (vectorp)
      return fd_make_mystery_vector(p,package,code,len,vector);
    else return make_character_type(p,code,len,packet);
    break;
  default:
    if (vectorp)
      return fd_make_mystery_vector(p,package,code,len,vector);
    else return fd_make_mystery_packet(p,package,code,len,packet);
  }
}

static fdtype make_character_type
  (FD_MEMORY_POOL_TYPE *p,int code,int len,unsigned char *bytes)
{
  switch (code) {
  case dt_ascii_char: {
    int c=bytes[0]; u8_pfree_x(p,bytes,len);
    return FD_CODE2CHAR(c);}
  case dt_unicode_char: {
    int c=0, i=0; while (i<len) {
      c=c<<8|bytes[i]; i++;}
    u8_pfree_x(p,bytes,len);
    return FD_CODE2CHAR(c);}
  case dt_unicode_short_string: case dt_unicode_string: {
    struct U8_OUTPUT os; unsigned char *scan, *limit;
    U8_INIT_OUTPUT(&os,len);
    scan=bytes; limit=bytes+len;
    while (scan < limit) {
      int c=scan[0]<<8|scan[1]; scan=scan+2;
      u8_putc(&os,c);}
    u8_pfree_x(p,bytes,len);
    return fd_init_string(u8_pmalloc(p,sizeof(struct FD_STRING)),
			  os.u8_outptr-os.u8_outbuf,os.u8_outbuf);}
  case dt_unicode_short_symbol: case dt_unicode_symbol: {
    fdtype sym;
    struct U8_OUTPUT os; unsigned char *scan, *limit;
    U8_INIT_OUTPUT(&os,len);
    scan=bytes; limit=bytes+len;
    while (scan < limit) {
      int c=scan[0]<<8|scan[1]; scan=scan+2;
      u8_putc(&os,c);}
    sym=fd_make_symbol(os.u8_outbuf,os.u8_outptr-os.u8_outbuf);
    u8_pfree_x(p,bytes,len);
    u8_pfree_x(p,os.u8_outbuf,os.u8_outptr-os.u8_outbuf);
    return sym;}
  default:
    return fd_make_mystery_packet(p,dt_character_package,code,len,bytes);
  }
}

#if 0
static fdtype make_framerd_type
  (FD_MEMORY_POOL_TYPE *p,int code,int len,fdtype *data)
{
  switch (code) {
  case dt_choice: case dt_small_choice:
    if (len==0) return FD_EMPTY_CHOICE;
    else if (FD_EXPECT_TRUE(len>1)) 
      return fd_init_choice(fd_palloc_choice(p,len),len,data,
			    (FD_CHOICE_FREEDATA|FD_CHOICE_DOSORT|FD_CHOICE_REALLOC));
    else {
      fdtype v=data[0]; u8_pfree_x(p,data,sizeof(fdtype)*len);
      return v;}
  case dt_qchoice: case dt_small_qchoice:
    if (len==0)
      return fd_init_qchoice(u8_pmalloc(p,sizeof(struct FD_QCHOICE)),
			     FD_EMPTY_CHOICE);
    else if (FD_EXPECT_TRUE(len>1)) {
      fdtype choice=fd_init_choice
	(fd_palloc_choice(p,len),len,data,
	 (FD_CHOICE_FREEDATA|FD_CHOICE_DOSORT|FD_CHOICE_REALLOC));
      if (FD_CHOICEP(choice))
	return fd_init_qchoice(u8_pmalloc(p,sizeof(struct FD_QCHOICE)),choice);
      else return choice;}
    else {
      fdtype v=data[0]; u8_pfree_x(p,data,sizeof(fdtype)*len);
      return v;}
  case dt_slotmap: case dt_small_slotmap:
    return fd_init_slotmap(u8_pmalloc(p,sizeof(struct FD_SLOTMAP)),
			   len/2,(struct FD_KEYVAL *)data,p);
  case dt_hashtable: case dt_small_hashtable: {
    fdtype table; int i=0;
    table=fd_init_hashtable
      (u8_pmalloc(p,sizeof(struct FD_HASHTABLE)),
       len/2,(struct FD_KEYVAL *)data,p);
    while (i<len) {fd_decref(data[i]); i++;}
    u8_pfree_x(p,data,sizeof(fdtype)*len);
    return table;}
  case dt_hashset: case dt_small_hashset: {
    struct FD_HASHSET *h=u8_pmalloc(p,sizeof(struct FD_HASHSET));
    fdtype *scan=data, *limit=scan+len;
    fd_init_hashset(h,len*3);
    while (scan<limit) {fd_hashset_init_add(h,*scan); scan++;}
    u8_pfree_x(p,data,sizeof(fdtype)*len);
    return FDTYPE_CONS(h);}
  default:
    return fd_make_mystery_vector(p,dt_framerd_package,code,len,data);
  }
}
#endif

FD_EXPORT fdtype fd_make_mystery_packet
  (FD_MEMORY_POOL_TYPE *p,int package,int typecode,
   unsigned int len,unsigned char *bytes)
{
  struct FD_MYSTERY *myst;
  int package_offset=package-0x40;
  int code_offset=(typecode&0x3F);
  if ((package_offset<0x40) && (dtype_packages[package_offset]) &&
      (dtype_packages[package_offset]->packetfns[code_offset]))
    return (dtype_packages[package_offset]->packetfns[code_offset])(len,bytes);
  myst=u8_pmalloc(p,sizeof(struct FD_MYSTERY));
  FD_INIT_CONS(myst,fd_mystery_type);
  myst->package=package; myst->code=typecode;
  myst->payload.packet=bytes; myst->size=len;
  return FDTYPE_CONS(myst);
}

FD_EXPORT fdtype fd_make_mystery_vector
  (FD_MEMORY_POOL_TYPE *p,int package,int typecode,
   unsigned int len,fdtype *elts)
{
  struct FD_MYSTERY *myst; int i=0;
  int package_offset=package-0x40;
  int code_offset=(typecode&0x3F);
  if ((package_offset<0x40) && (dtype_packages[package_offset]) &&
      (dtype_packages[package_offset]->vectorfns[code_offset]))
    return (dtype_packages[package_offset]->vectorfns[code_offset])(len,elts);
  myst=u8_pmalloc(p,sizeof(struct FD_MYSTERY));
  FD_INIT_CONS(myst,fd_mystery_type);
  myst->package=package; myst->code=typecode;
  myst->payload.vector=elts; myst->size=len;
  return FDTYPE_CONS(myst);
}  

/* Arith stubs */

static fdtype default_make_rational
  (FD_MEMORY_POOL_TYPE *pool,fdtype car,fdtype cdr)
{
  struct FD_PAIR *p=u8_pmalloc(pool,sizeof(struct FD_PAIR));
  FD_INIT_CONS(p,fd_rational_type);
  p->car=car; p->cdr=cdr;
  return FDTYPE_CONS(p);
}

static void default_unpack_rational
  (fdtype x,fdtype *car,fdtype *cdr)
{
  struct FD_PAIR *p=FD_GET_CONS(x,fd_rational_type,struct FD_PAIR *);
  *car=p->car; *cdr=p->cdr;
}

static fdtype default_make_complex
  (FD_MEMORY_POOL_TYPE *pool,fdtype car,fdtype cdr)
{
  struct FD_PAIR *p=u8_pmalloc(pool,sizeof(struct FD_PAIR));
  FD_INIT_CONS(p,fd_complex_type);
  p->car=car; p->cdr=cdr;
  return FDTYPE_CONS(p);
}

static void default_unpack_complex
  (fdtype x,fdtype *car,fdtype *cdr)
{
  struct FD_PAIR *p=FD_GET_CONS(x,fd_complex_type,struct FD_PAIR *);
  *car=p->car; *cdr=p->cdr;
}

static fdtype default_make_double
  (FD_MEMORY_POOL_TYPE *p,double d)
{
  struct FD_DOUBLE *ds=u8_malloc(sizeof(struct FD_DOUBLE));
  FD_INIT_CONS(ds,fd_double_type); ds->flonum=d;
  return FDTYPE_CONS(ds);
}

fdtype
  (*_fd_make_rational)(FD_MEMORY_POOL_TYPE *,fdtype car,fdtype cdr)=
  default_make_rational;
void
  (*_fd_unpack_rational)(fdtype,fdtype *,fdtype *)=default_unpack_rational;
fdtype
  (*_fd_make_complex)(FD_MEMORY_POOL_TYPE *,fdtype car,fdtype cdr)=
  default_make_complex;
void
  (*_fd_unpack_complex)(fdtype,fdtype *,fdtype *)=default_unpack_complex;
fdtype
  (*_fd_make_double)(FD_MEMORY_POOL_TYPE *,double)=
  default_make_double;

/* Exported functions */

FD_EXPORT int _fd_read_byte(struct FD_BYTE_INPUT *stream)
{
  if (fd_needs_bytes(stream,1)) return (*(stream->ptr++));
  else return -1;
}

FD_EXPORT int _fd_unread_byte(struct FD_BYTE_INPUT *stream,int byte)
{
  if (stream->ptr==stream->start) {
    fd_seterr(BadUnReadByte,"_fd_unread_byte",NULL,FD_VOID);
    return -1;}
  else if (stream->ptr[-1]!=byte) {
    fd_seterr(BadUnReadByte,"_fd_unread_byte",NULL,FD_VOID);
    return -1;}
  else {stream->ptr--; return 0;}
}

FD_EXPORT unsigned int _fd_read_4bytes(struct FD_BYTE_INPUT *stream)
{
  if (fd_needs_bytes(stream,4)) {
    unsigned int bytes=fd_get_4bytes(stream->ptr);
    stream->ptr=stream->ptr+4;
    return bytes;}
  else {
    fd_seterr1(fd_UnexpectedEOD);
    return 0;}
}

FD_EXPORT int
  _fd_read_bytes(unsigned char *bytes,struct FD_BYTE_INPUT *stream,int len) 
{
  if (fd_needs_bytes(stream,len)) {
    memcpy(bytes,stream->ptr,len);
    stream->ptr=stream->ptr+len;
    return len;}
  else return -1;
}

/* File initialization */

FD_EXPORT fd_init_dtypeio_c()
{
  fd_register_source_file(versionid);

#if FD_THREADS_ENABLED
  fd_init_mutex(&(dtype_unpacker_lock));
#endif
}


/* The CVS log for this file
   $Log: dtypeio.c,v $
   Revision 1.48  2006/01/31 13:47:23  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.47  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.46  2006/01/16 14:16:32  haase
   Made dtype functions handle empty slotmaps and hashtables better

   Revision 1.45  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.44  2006/01/07 23:12:46  haase
   Moved framerd object dtype handling into the main fd_read_dtype core, which led to substantial performanc improvements

   Revision 1.43  2006/01/07 14:01:13  haase
   Fixed some leaks

   Revision 1.42  2006/01/03 02:48:38  haase
   Added hashset reading/writing

   Revision 1.41  2005/12/28 23:03:29  haase
   Made choices be direct blocks of elements, including various fixes, simplifications, and more detailed documentation.

   Revision 1.40  2005/12/26 18:19:44  haase
   Reorganized and documented dtype pointers and conses

   Revision 1.39  2005/12/20 00:53:45  haase
   Added QCHOICE dtypes

   Revision 1.38  2005/12/17 21:50:24  haase
   Fixed bug in reading character dtypes

   Revision 1.37  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.36  2005/08/02 22:51:54  haase
   Added schemap dtype writing methods

   Revision 1.35  2005/07/13 21:39:31  haase
   XSLOTMAP/XSCHEMAP renaming

   Revision 1.34  2005/06/15 02:29:42  haase
   Made dtype pair reading iterate in the CDR direction and added fd_unread_byte to support it

   Revision 1.33  2005/05/27 13:47:36  haase
   Added better remote exception reading

   Revision 1.32  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.31  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.30  2005/04/01 02:42:40  haase
   Reimplemented module exports to be faster and less kludgy

   Revision 1.29  2005/03/30 15:30:00  haase
   Made calls to new seterr do appropriate strdups

   Revision 1.28  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.27  2005/03/19 02:50:15  haase
   Rearranging core pointer types

   Revision 1.26  2005/03/17 03:59:30  haase
   Fixed empty vector GC bug

   Revision 1.25  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.24  2005/02/23 22:49:27  haase
   Created generalized compound registry and made compound dtypes and #< reading use it

   Revision 1.23  2005/02/16 02:34:58  haase
   Various optmizations, etc

   Revision 1.22  2005/02/15 22:39:04  haase
   More extensions to port handling functions, including GETLINE procedure

   Revision 1.21  2005/02/15 03:03:40  haase
   Updated to use the new libu8

   Revision 1.20  2005/02/14 02:08:20  haase
   Added error return value check to dtype length consistency checking

   Revision 1.19  2005/02/11 03:48:01  haase
   Added branch expectation to DTYPE iostream code

   Revision 1.18  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
