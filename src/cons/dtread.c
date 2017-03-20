/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_BUFIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/dtypeio.h"

#include <zlib.h>
#include <errno.h>

#ifndef FD_DEBUG_DTYPEIO
#define FD_DEBUG_DTYPEIO 0
#endif

#define FD_DEFAULT_ZLEVEL 9

static u8_mutex dtype_unpacker_lock;

fd_exception fd_UnexpectedEOD=_("Unexpected end of data");
fd_exception fd_DTypeError=_("Malformed DTYPE representation");

static fdtype error_symbol;

#define newpos(pos,ptr,lim) ((((ptr)+pos) <= lim) ? (pos) : (-1))

static int validate_dtype(int pos,const unsigned char *ptr,
                          const unsigned char *lim)
{
  if (pos < 0) 
    return pos;
  else if (ptr+pos >= lim) 
    return -1;
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
    case dt_tiny_symbol: case dt_tiny_string:
      if (ptr+pos+1 >= lim) return -1;
      else return newpos(pos+1+ptr[pos+1],ptr,lim);
    case dt_empty_choice:
      return newpos(pos+1,ptr,lim);
    case dt_tiny_choice:
      if (ptr+pos+1 >= lim) return -1;
      else {
        int i=0, len=ptr[pos+1];
        while (i<len) {
          int i=0, len=ptr[pos+1], npos=pos+2;
          while ((i < len) && (npos > 0)) npos=validate_dtype(npos,ptr,lim);
          return npos;}}
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

FD_EXPORT int fd_validate_dtype(struct FD_INBUF *in)
{
  return validate_dtype(0,in->bufread,in->buflim);
}

/* Byte input */

#define nobytes(in,nbytes) (FD_EXPECT_FALSE(!(fd_needs_bytes(in,nbytes))))
#define havebytes(in,nbytes) (FD_EXPECT_TRUE(fd_needs_bytes(in,nbytes)))

static fdtype restore_dtype_exception(fdtype content);
FD_EXPORT fdtype fd_make_mystery_packet(int,int,unsigned int,unsigned char *);
FD_EXPORT fdtype fd_make_mystery_vector(int,int,unsigned int,fdtype *);
static fdtype read_packaged_dtype(int,struct FD_INBUF *);

static fdtype *read_dtypes(int n,struct FD_INBUF *in,
                           fdtype *why_not,fdtype *into)
{
  if (n==0) return NULL;
  else {
    fdtype *vec=((into)?(into):(u8_alloc_n(n,fdtype)));
    int i=0; while (i < n) {
      fdtype v=fd_read_dtype(in);
      if (FD_COOLP(v)) vec[i++]=v;
      else {
        int j=0; while (j<i) {fd_decref(vec[j]); j++;}
        u8_free(vec); *why_not=v;
        return NULL;}}
    return vec;}
}

FD_EXPORT fdtype fd_read_dtype(struct FD_INBUF *in)
{
  if (FD_EXPECT_FALSE(FD_ISWRITING(in)))
    return fdt_iswritebuf(in);
  else if (havebytes(in,1)) {
    int code=*(in->bufread++);
    switch (code) {
    case dt_empty_list: return FD_EMPTY_LIST;
    case dt_void: return FD_VOID;
    case dt_empty_choice: return FD_EMPTY_CHOICE;
    case dt_boolean:
      if (nobytes(in,1))
        return fd_return_errcode(FD_EOD);
      else if (*(in->bufread++))
        return FD_TRUE;
      else return FD_FALSE;
    case dt_fixnum:
      if (havebytes(in,4)) {
        int intval=fd_get_4bytes(in->bufread); in->bufread=in->bufread+4;
        return FD_INT(intval);}
      else return fd_return_errcode(FD_EOD);
    case dt_flonum: {
      char bytes[4];
      float *f=(float *)&bytes; double flonum;
      unsigned int *i=(unsigned int *)&bytes, num;
      num=fd_read_4bytes(in);
      *i=num; flonum=*f;
      return _fd_make_double(flonum);}
    case dt_oid: {
      FD_OID addr=FD_NULL_OID_INIT;
      FD_SET_OID_HI(addr,fd_read_4bytes(in));
      FD_SET_OID_LO(addr,fd_read_4bytes(in));
      return fd_make_oid(addr);}
    case dt_error: {
      fdtype content=fd_read_dtype(in);
      return fd_init_compound(NULL,error_symbol,0,1,content);}
    case dt_exception: {
      fdtype content=fd_read_dtype(in);
      return restore_dtype_exception(content);}
    case dt_pair: {
      fdtype head=FD_EMPTY_LIST, *tail=&head;
      while (1) {
        fdtype car=fd_read_dtype(in);
        if (FD_ABORTP(car)) {
          fd_decref(head); return car;}
        else {
          fdtype new_pair=
            fd_init_pair(u8_alloc(struct FD_PAIR),
                         car,FD_EMPTY_LIST);
          int dtcode=fd_read_byte(in);
          if (dtcode<0) {
            fd_decref(head); fd_decref(new_pair);
            fd_seterr1(fd_UnexpectedEOD);
            return FD_EOD;}
          *tail=new_pair; tail=&(FD_CDR(new_pair));
          if (dtcode != dt_pair) {
            fdtype cdr;
            if (fd_unread_byte(in,dtcode)<0) {
              fd_decref(head);
              return FD_ERROR_VALUE;}
            cdr=fd_read_dtype(in);
            if (FD_ABORTP(cdr)) {
              fd_decref(head); return cdr;}
            *tail=cdr;
            return head;}}}}
    case dt_compound: case dt_rational: case dt_complex: {
      fdtype car=fd_read_dtype(in), cdr;
      if (FD_TROUBLEP(car)) return car;
      cdr=fd_read_dtype(in);
      if (FD_TROUBLEP(cdr)) {fd_decref(car); return cdr;}
      switch (code) {
      case dt_compound: {
        struct FD_COMPOUND_TYPEINFO *e=fd_lookup_compound(car);
        if ((e) && (e->fd_compound_restorefn)) {
          fdtype result=e->fd_compound_restorefn(car,cdr,e);
          fd_decref(cdr);
          return result;}
        else if ((FD_VECTORP(cdr)) && (FD_VECTOR_LENGTH(cdr)<32767)) {
          struct FD_VECTOR *vec=(struct FD_VECTOR *)cdr;
          short n_elts=(short)(vec->fdvec_length);
          fdtype result=
            fd_init_compound_from_elts(NULL,car,0,n_elts,vec->fdvec_elts);
          /* Note that the incref'd values are now stored in the compound,
             so we don't decref them ourselves. */
          u8_free(vec);
          return result;}
        else return fd_init_compound
               (u8_alloc(struct FD_COMPOUND),car,1,1,cdr);}
      case dt_rational:
        return _fd_make_rational(car,cdr);
      case dt_complex:
        return _fd_make_complex(car,cdr);}}
    case dt_packet: case dt_string:
      if (nobytes(in,4)) return fd_return_errcode(FD_EOD);
      else {
        int len=fd_get_4bytes(in->bufread); in->bufread=in->bufread+4;
        if (nobytes(in,len)) return fd_return_errcode(FD_EOD);
        else {
          fdtype result=FD_VOID;
          switch (code) {
          case dt_string:
            result=fd_make_string(NULL,len,in->bufread); break;
          case dt_packet:
            result=fd_make_packet(NULL,len,in->bufread); break;}
          in->bufread=in->bufread+len;
          return result;}}
    case dt_tiny_symbol:
      if (nobytes(in,1)) return fd_return_errcode(FD_EOD);
      else {
        int len=fd_get_byte(in->bufread); in->bufread=in->bufread+1;
        if (nobytes(in,len)) return fd_return_errcode(FD_EOD);
        else {
          u8_byte data[257];
          memcpy(data,in->bufread,len); data[len]='\0';
          in->bufread=in->bufread+len;
          return fd_make_symbol(data,len);}}
    case dt_tiny_string:
      if (nobytes(in,1)) return fd_return_errcode(FD_EOD);
      else {
        int len=fd_get_byte(in->bufread);
        in->bufread=in->bufread+1;
        if (nobytes(in,len)) return fd_return_errcode(FD_EOD);
        else {
          fdtype result= fd_make_string(NULL,len,in->bufread);
          in->bufread=in->bufread+len;
          return result;}}
    case dt_symbol: case dt_zstring:
      if (nobytes(in,4)) return fd_return_errcode(FD_EOD);
      else {
        int len=fd_get_4bytes(in->bufread);
        in->bufread=in->bufread+4;
        if (nobytes(in,len))
          return fd_return_errcode(FD_EOD);
        else {
          unsigned char buf[64], *data;
          if (len >= 64) data=u8_malloc(len+1); else data=buf;
          memcpy(data,in->bufread,len); data[len]='\0';
          in->bufread=in->bufread+len;
          if (data != buf) {
            fdtype result=fd_make_symbol(data,len);
            u8_free(data);
            return result;}
          else return fd_make_symbol(data,len);}}
    case dt_vector:
      if (nobytes(in,4)) return fd_return_errcode(FD_EOD);
      else {
        int len=fd_read_4bytes(in);
        if (FD_EXPECT_FALSE(len == 0))
          return fd_init_vector(NULL,0,NULL);
        else {
          fdtype why_not=FD_EOD, result=fd_init_vector(NULL,len,NULL);
          fdtype *elts=FD_VECTOR_ELTS(result);
          fdtype *data=read_dtypes(len,in,&why_not,elts);
          if (FD_EXPECT_TRUE((data!=NULL)))
            return result;
          else return fd_return_errcode(why_not);}}
    case dt_tiny_choice:
      if (nobytes(in,1)) return fd_return_errcode(FD_EOD);
      else {
        int len=fd_read_byte(in);
        struct FD_CHOICE *ch=fd_alloc_choice(len);
        fdtype *write=(fdtype *)FD_XCHOICE_DATA(ch), *limit=write+len;
        while (write<limit) {
          fdtype v=fd_read_dtype(in);
          if (FD_ABORTP(v)) {
            u8_free(ch);
            return v;}
          *write++=v;}
        return fd_init_choice(ch,len,NULL,(FD_CHOICE_DOSORT|FD_CHOICE_REALLOC));}

    case dt_block:
      if (nobytes(in,4)) return fd_return_errcode(FD_EOD);
      else {
        int nbytes=fd_read_4bytes(in);
        if (nobytes(in,nbytes)) return fd_return_errcode(FD_EOD);
        return fd_read_dtype(in);}

    case dt_framerd_package: {
      int code, lenlen, len;
      if (nobytes(in,2)) return fd_return_errcode(FD_EOD);
      code=*(in->bufread++); lenlen=((code&0x40) ? 4 : 1);
      if (nobytes(in,lenlen)) return fd_return_errcode(FD_EOD);
      else if (lenlen==4) len=fd_read_4bytes(in);
      else len=fd_read_byte(in);
      switch (code) {
      case dt_qchoice: case dt_small_qchoice:
        if (len==0)
          return fd_init_qchoice(u8_alloc(struct FD_QCHOICE),FD_EMPTY_CHOICE);
      case dt_choice: case dt_small_choice:
        if (len==0) return FD_EMPTY_CHOICE;
        else {
          fdtype result;
          struct FD_CHOICE *ch=fd_alloc_choice(len);
          fdtype *write=(fdtype *)FD_XCHOICE_DATA(ch), *limit=write+len;
          while (write<limit) *write++=fd_read_dtype(in);
          result=fd_init_choice(ch,len,NULL,(FD_CHOICE_DOSORT|FD_CHOICE_REALLOC));
          if (FD_CHOICEP(result))
            if ((code==dt_qchoice) || (code==dt_small_qchoice))
              return fd_init_qchoice(u8_alloc(struct FD_QCHOICE),result);
            else return result;
          else return result;}
      case dt_slotmap: case dt_small_slotmap:
        if (len==0)
          return fd_empty_slotmap();
        else {
          int n_slots=len/2;
          struct FD_KEYVAL *keyvals=u8_alloc_n(n_slots,struct FD_KEYVAL);
          struct FD_KEYVAL *write=keyvals, *limit=keyvals+n_slots;
          while (write<limit) {
            write->kv_key=fd_read_dtype(in);
            write->kv_val=fd_read_dtype(in);
            write++;}
          if (n_slots<7) {
            fdtype result=fd_make_slotmap(n_slots,n_slots,keyvals);
            u8_free(keyvals);
            return result;}
          else return fd_init_slotmap(NULL,n_slots,keyvals);}
      case dt_hashtable: case dt_small_hashtable:
        if (len==0)
          return fd_init_hashtable(NULL,0,NULL);
        else {
          int n_keys=len/2, n_read=0;
          fdtype result=fd_init_hashtable(NULL,len/2,NULL);
          struct FD_HASHTABLE *ht=(struct FD_HASHTABLE *)result;
          while (n_read<n_keys) {
            fdtype key=fd_read_dtype(in);
            fdtype value=fd_read_dtype(in);
            if (!(FD_EMPTY_CHOICEP(value)))
              fd_hashtable_op_nolock(ht,fd_table_store_noref,key,value);
            fd_decref(key);
            n_read++;}
          return result;}
      case dt_hashset: case dt_small_hashset: {
        int i=0; struct FD_HASHSET *h=u8_alloc(struct FD_HASHSET);
        fd_init_hashset(h,len,FD_MALLOCD_CONS);
        while (i<len) {
          fdtype v=fd_read_dtype(in);
          fd_hashset_add_raw(h,v);
          i++;}
        return FDTYPE_CONS(h);}
      default: {
        int i=0; fdtype *data=u8_alloc_n(len,fdtype);
        while (i<len) data[i++]=fd_read_dtype(in);
        return fd_make_mystery_vector(dt_framerd_package,code,len,data);}}}
    default:
      if ((code >= 0x40) && (code < 0x80))
        return read_packaged_dtype(code,in);
      else return FD_DTYPE_ERROR;}}
  else return fd_return_errcode(FD_EOD);
}

/* Vector and packet unpackers */

struct  FD_DTYPE_PACKAGE {
  fd_packet_unpacker packetfns[64];
  fd_vector_unpacker vectorfns[64];};
struct FD_DTYPE_PACKAGE *fd_dtype_packages[64];

FD_EXPORT int fd_register_vector_unpacker
  (unsigned int package,unsigned int code,fd_vector_unpacker f)
{
  int package_offset=package-0x40, code_offset=((code-0x80)&(~(0x40)));
  int replaced=0;
  if ((package<0x40) || (package>0x80)) return -1;
  else if ((code&0x80)==0) return -2;
  u8_lock_mutex(&dtype_unpacker_lock);
  if (fd_dtype_packages[package_offset]==NULL) {
    struct FD_DTYPE_PACKAGE *pkg=
      fd_dtype_packages[package_offset]=
      u8_alloc(struct FD_DTYPE_PACKAGE);
    memset(pkg,0,sizeof(struct FD_DTYPE_PACKAGE));
    replaced=2;}
  else if (fd_dtype_packages[package_offset]->vectorfns[code_offset])
    replaced=1;
  fd_dtype_packages[package_offset]->vectorfns[code_offset]=f;
  u8_unlock_mutex(&dtype_unpacker_lock);
  return replaced;
}

FD_EXPORT int fd_register_packet_unpacker
  (unsigned int package,unsigned int code,fd_packet_unpacker f)
{
  int package_offset=package-0x40, code_offset=((code)&(~(0x40)));
  int replaced=0;
  if ((package<0x40) || (package>0x80)) return -1;
  else if ((code&0x80)) return -2;
  u8_lock_mutex(&dtype_unpacker_lock);
  if (fd_dtype_packages[package_offset]==NULL) {
    struct FD_DTYPE_PACKAGE *pkg=
      fd_dtype_packages[package_offset]=
      u8_alloc(struct FD_DTYPE_PACKAGE);
    memset(pkg,0,sizeof(struct FD_DTYPE_PACKAGE));
    replaced=2;}
  else if (fd_dtype_packages[package_offset]->packetfns[code_offset])
    replaced=1;
  fd_dtype_packages[package_offset]->packetfns[code_offset]=f;
  u8_unlock_mutex(&dtype_unpacker_lock);
  return replaced;
}

/* Reading packaged dtypes */

static fdtype make_character_type(int code,int len,unsigned char *data);

static fdtype read_packaged_dtype
   (int package,struct FD_INBUF *in)
{
  fdtype *vector=NULL; unsigned char *packet=NULL;
  unsigned int code, lenlen, len, vectorp;
  if (nobytes(in,2)) return fd_return_errcode(FD_EOD);
  code=*(in->bufread++); lenlen=((code&0x40) ? 4 : 1); vectorp=(code&0x80);
  if (nobytes(in,lenlen)) return fd_return_errcode(FD_EOD);
  else if (lenlen==4) len=fd_read_4bytes(in);
  else len=fd_read_byte(in);
  if (vectorp) {
    fdtype why_not;
    vector=read_dtypes(len,in,&why_not,NULL);
    if ((len>0) && (vector==NULL)) return fd_return_errcode(why_not);}
  else if (nobytes(in,len)) return fd_return_errcode(FD_EOD);
  else {
    packet=u8_malloc(len);
    memcpy(packet,in->bufread,len); in->bufread=in->bufread+len;}
  switch (package) {
#if 0
  case dt_framerd_package:
    if (vectorp)
      return make_framerd_type(p,fd_dtcode,len,fd_dtelts);
    else return fd_make_mystery_packet(p,fd_dtpackage,fd_dtcode,len,fd_dtbytes);
#endif
  case dt_character_package:
    if (vectorp)
      return fd_make_mystery_vector(package,code,len,vector);
    else return make_character_type(code,len,packet);
    break;
  default:
    if (vectorp)
      return fd_make_mystery_vector(package,code,len,vector);
    else return fd_make_mystery_packet(package,code,len,packet);
  }
}

static fdtype make_character_type(int code,int len,unsigned char *bytes)
{
  switch (code) {
  case dt_ascii_char: {
    int c=bytes[0]; u8_free(bytes);
    return FD_CODE2CHAR(c);}
  case dt_unicode_char: {
    int c=0, i=0; while (i<len) {
      c=c<<8|bytes[i]; i++;}
    u8_free(bytes);
    return FD_CODE2CHAR(c);}
  case dt_unicode_short_string: case dt_unicode_string: {
    u8_byte buf[256];
    struct U8_OUTPUT os; unsigned char *scan, *limit; fdtype result;
    U8_INIT_OUTPUT_X(&os,256,buf,0);
    scan=bytes; limit=bytes+len;
    while (scan < limit) {
      int c=scan[0]<<8|scan[1]; scan=scan+2;
      u8_putc(&os,c);}
    u8_free(bytes);
    result=fd_make_string(NULL,os.u8_write-os.u8_outbuf,os.u8_outbuf);
    u8_close_output(&os);
    return result;}
  case dt_secret_packet: case dt_short_secret_packet: {
    fdtype result=fd_make_packet(NULL,len,bytes);
    FD_SET_CONS_TYPE(result,fd_secret_type);
    return result;}
  case dt_unicode_short_symbol: case dt_unicode_symbol: {
    fdtype sym;
    struct U8_OUTPUT os; unsigned char *scan, *limit;
    U8_INIT_OUTPUT(&os,len);
    scan=bytes; limit=bytes+len;
    while (scan < limit) {
      int c=scan[0]<<8|scan[1]; scan=scan+2;
      u8_putc(&os,c);}
    sym=fd_make_symbol(os.u8_outbuf,os.u8_write-os.u8_outbuf);
    u8_free(bytes); u8_free(os.u8_outbuf);
    return sym;}
  default:
    return fd_make_mystery_packet(dt_character_package,code,len,bytes);
  }
}

FD_EXPORT fdtype fd_make_mystery_packet
  (int package,int typecode,unsigned int len,unsigned char *bytes)
{
  struct FD_MYSTERY_DTYPE *myst;
  int pkg_offset=package-0x40;
  int code_offset=(typecode&0x3F);
  if ((pkg_offset<0x40) && (fd_dtype_packages[pkg_offset]) &&
      (fd_dtype_packages[pkg_offset]->packetfns[code_offset]))
    return (fd_dtype_packages[pkg_offset]->packetfns[code_offset])(len,bytes);
  myst=u8_alloc(struct FD_MYSTERY_DTYPE);
  FD_INIT_CONS(myst,fd_mystery_type);
  myst->fd_dtpackage=package; myst->fd_dtcode=typecode;
  myst->fd_mystery_payload.fd_dtbytes=bytes; myst->fd_dtlen=len;
  return FDTYPE_CONS(myst);
}

FD_EXPORT fdtype fd_make_mystery_vector
  (int package,int typecode,unsigned int len,fdtype *elts)
{
  struct FD_MYSTERY_DTYPE *myst;
  int pkg_offset=package-0x40;
  int code_offset=(typecode&0x3F);
  if ((pkg_offset<0x40) && (fd_dtype_packages[pkg_offset]) &&
      (fd_dtype_packages[pkg_offset]->vectorfns[code_offset]))
    return (fd_dtype_packages[pkg_offset]->vectorfns[code_offset])(len,elts);
  myst=u8_alloc(struct FD_MYSTERY_DTYPE);
  FD_INIT_CONS(myst,fd_mystery_type);
  myst->fd_dtpackage=package; myst->fd_dtcode=typecode;
  myst->fd_mystery_payload.fd_dtelts=elts; myst->fd_dtlen=len;
  return FDTYPE_CONS(myst);
}

static fdtype restore_dtype_exception(fdtype content)
{
  /* Return an exception object if possible (content as expected)
     and a compound if there are any big surprises */
  fd_exception exname=_("Poorly Restored Error");
  u8_context context=NULL; u8_string details=NULL;
  fdtype irritant=FD_VOID; int new_format=0;
  if (FD_TROUBLEP(content)) return content;
  else if (FD_VECTORP(content)) {
    int len=FD_VECTOR_LENGTH(content);
    /* One old format was:
         #(ex details irritant backtrace)
         where ex is a string
       The new format is:
         #(ex context details irritant)
         where ex and context are symbols
       We handle both cases
    */
    if (len>0) {
      fdtype elt0=FD_VECTOR_REF(content,0);
      if (FD_SYMBOLP(elt0)) {
        exname=FD_SYMBOL_NAME(elt0); new_format=1;}
      else if (FD_STRINGP(elt0)) { /* Old format */
        exname=FD_SYMBOL_NAME(elt0); new_format=0;}
      else {
        u8_log(LOG_WARN,fd_DTypeError,"Odd exception content: %q",content);
        new_format=-1;}}
    if (new_format<0) {}
    else if (new_format)
      if ((len<3) ||
          (!(FD_SYMBOLP(FD_VECTOR_REF(content,0)))) ||
          (!((FD_FALSEP(FD_VECTOR_REF(content,1))) ||
             (FD_SYMBOLP(FD_VECTOR_REF(content,1))))) ||
          (!((FD_FALSEP(FD_VECTOR_REF(content,2))) ||
             (FD_STRINGP(FD_VECTOR_REF(content,1))))))
        u8_log(LOG_WARN,fd_DTypeError,"Odd exception content: %q",content);
      else {
        exname=(u8_condition)(FD_SYMBOL_NAME(FD_VECTOR_REF(content,0)));
        context=(u8_context)
          ((FD_FALSEP(FD_VECTOR_REF(content,1))) ? (NULL) :
           (FD_SYMBOL_NAME(FD_VECTOR_REF(content,1))));
        details=(u8_string)
          ((FD_FALSEP(FD_VECTOR_REF(content,2))) ? (NULL) :
           (FD_STRDATA(content)));
        if (len>3) irritant=FD_VECTOR_REF(content,3);}
    else { /* Old format */
      if ((len>0) && (FD_SYMBOLP(FD_VECTOR_REF(content,0)))) {
        fdtype sym=fd_intern(FD_STRDATA(FD_VECTOR_REF(content,0)));
        exname=(u8_condition)FD_SYMBOL_NAME(sym);}
      else if ((len>0) && (FD_STRINGP(FD_VECTOR_REF(content,0)))) {
        exname=(u8_condition)FD_SYMBOL_NAME(FD_VECTOR_REF(content,0));}
      if ((len>1) && (FD_STRINGP(FD_VECTOR_REF(content,1))))
        details=FD_STRDATA(FD_VECTOR_REF(content,1));
      if (len>2) irritant=FD_VECTOR_REF(content,2);}
    return fd_make_exception(exname,context,details,irritant);}
  else return fd_make_exception
         (fd_DTypeError,"restore_dtype_exception",NULL,content);
}

/* Arith stubs */

static fdtype default_make_rational(fdtype car,fdtype cdr)
{
  struct FD_PAIR *p=u8_alloc(struct FD_PAIR);
  FD_INIT_CONS(p,fd_rational_type);
  p->car=car; p->cdr=cdr;
  return FDTYPE_CONS(p);
}

static void default_unpack_rational
  (fdtype x,fdtype *car,fdtype *cdr)
{
  struct FD_PAIR *p=fd_consptr(struct FD_PAIR *,x,fd_rational_type);
  *car=p->car; *cdr=p->cdr;
}

static fdtype default_make_complex(fdtype car,fdtype cdr)
{
  struct FD_PAIR *p=u8_alloc(struct FD_PAIR);
  FD_INIT_CONS(p,fd_complex_type);
  p->car=car; p->cdr=cdr;
  return FDTYPE_CONS(p);
}

static void default_unpack_complex
  (fdtype x,fdtype *car,fdtype *cdr)
{
  struct FD_PAIR *p=fd_consptr(struct FD_PAIR *,x,fd_complex_type);
  *car=p->car; *cdr=p->cdr;
}

static fdtype default_make_double(double d)
{
  struct FD_FLONUM *ds=u8_alloc(struct FD_FLONUM);
  FD_INIT_CONS(ds,fd_flonum_type); ds->floval=d;
  return FDTYPE_CONS(ds);
}

fdtype(*_fd_make_rational)(fdtype car,fdtype cdr)=default_make_rational;
void
  (*_fd_unpack_rational)(fdtype,fdtype *,fdtype *)=default_unpack_rational;
fdtype (*_fd_make_complex)(fdtype car,fdtype cdr)=default_make_complex;
void (*_fd_unpack_complex)(fdtype,fdtype *,fdtype *)=default_unpack_complex;
fdtype (*_fd_make_double)(double)=default_make_double;

/* Reading and writing compressed dtypes */

static unsigned char *do_uncompress
  (unsigned char *bytes,size_t n_bytes,ssize_t *dbytes)
{
  int error;
  uLongf x_lim=4*n_bytes, x_bytes=x_lim;
  Bytef *fdata=(Bytef *)bytes, *xdata=u8_malloc(x_bytes);
  while ((error=uncompress(xdata,&x_bytes,fdata,n_bytes)) < Z_OK)
    if (error == Z_MEM_ERROR) {
      u8_free(xdata);
      fd_seterr1("ZLIB Out of Memory");
      return NULL;}
    else if (error == Z_BUF_ERROR) {
      xdata=u8_realloc(xdata,x_lim*2); x_bytes=x_lim=x_lim*2;}
    else if (error == Z_DATA_ERROR) {
      u8_free(xdata);
      fd_seterr1("ZLIB Data error");
      return NULL;}
    else {
      u8_free(xdata);
      fd_seterr1("Bad ZLIB return code");
      return NULL;}
  *dbytes=x_bytes;
  return xdata;
}

/* This reads a non frame value with compression. */
FD_EXPORT fdtype fd_zread_dtype(struct FD_INBUF *in)
{
  fdtype result;
  ssize_t n_bytes=fd_read_zint(in), dbytes;
  unsigned char *bytes=u8_malloc(n_bytes);
  int retval=fd_read_bytes(bytes,in,n_bytes);
  struct FD_INBUF tmp;
  if (retval<n_bytes) {
    u8_free(bytes);
    return FD_ERROR_VALUE;}
  memset(&tmp,0,sizeof(tmp));
  tmp.bufread=tmp.buffer=do_uncompress(bytes,n_bytes,&dbytes);
  tmp.buf_flags=FD_BUFFER_IS_MALLOCD;
  tmp.buflim=tmp.buffer+dbytes;
  result=fd_read_dtype(&tmp);
  u8_free(bytes); u8_free(tmp.buffer);
  return result;
}
/* File initialization */

FD_EXPORT void fd_init_dtread_c()
{
  u8_register_source_file(_FILEINFO);

  error_symbol=fd_intern("%ERROR");

  fd_register_config
    ("USEDTBLOCK",_("Use the DTBLOCK dtype code when appropriate"),
     fd_boolconfig_get,fd_boolconfig_set,&fd_use_dtblock);
  fd_register_config
    ("CHECKDTSIZE",_("whether to check returned and real dtype sizes"),
     fd_boolconfig_get,fd_boolconfig_set,&fd_check_dtsize);

  u8_init_mutex(&(dtype_unpacker_lock));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
