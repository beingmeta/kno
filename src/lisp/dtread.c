/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_BUFIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/compounds.h"
#include "framerd/dtypeio.h"

#include <libu8/u8elapsed.h>

#include <zlib.h>
#include <errno.h>

#ifndef FD_DEBUG_DTYPEIO
#define FD_DEBUG_DTYPEIO 0
#endif

FD_EXPORT lispval fd_restore_exception_dtype(lispval content);

static u8_mutex dtype_unpacker_lock;

u8_condition fd_UnexpectedEOD=_("Unexpected end of data");
u8_condition fd_DTypeError=_("Malformed DTYPE representation");

static lispval error_symbol;

#define newpos(pos,ptr,lim) ((((ptr)+pos) <= lim) ? (pos) : (-1))

static ssize_t validate_dtype(int pos,const unsigned char *ptr,
                          const unsigned char *lim)
{
  if (pos < 0)
    return pos;
  else if (ptr+pos >= lim)
    return -1;
  else {
    int code = ptr[pos];
    switch (code) {
    case dt_empty_list: case dt_void:
    case dt_empty_choice: case dt_default_value:
      return newpos(pos+1,ptr,lim);
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
    case dt_tiny_choice:
      if (ptr+pos+1 >= lim) return -1;
      else {
        int i = 0, len = ptr[pos+1];
        while (i<len) {
          int i = 0, len = ptr[pos+1], npos = pos+2;
          while ((i < len) && (npos > 0))
            npos = validate_dtype(npos,ptr,lim);
          return npos;}}
    case dt_symbol: case dt_packet: case dt_string: case dt_zstring:
      if (ptr+pos+5 >= lim) return -1;
      else {
        int len = fd_get_4bytes(ptr+pos+1);
        return newpos(pos+5+len,ptr,lim);}
    case dt_vector:
      if (ptr+pos+5 >= lim) return -1;
      else {
        int i = 0, len = fd_get_4bytes(ptr+pos+1), npos = pos+5;
        while ((i < len) && (npos > 0))
          npos = validate_dtype(npos,ptr,lim);
        return npos;}
    default:
      if ((code > 0x40) && (ptr+pos+2 < lim)) {
        int subcode = *(ptr+pos+1), len, npos;
        if (subcode&0x40)
          if (ptr+pos+6 < lim) {
            npos = pos+6;
            len = fd_get_4bytes(ptr+pos+2);}
          else return -1;
        else if (ptr+pos+3 < lim) {
          npos = pos+3;
          len = fd_get_byte(ptr+pos+2);}
        else return -1;
        if (subcode&0x80) {
          int i = 0; while ((i < len) && (npos > 0)) {
            npos = validate_dtype(npos,ptr,lim); i++;}
          return npos;}
        else return newpos(npos+len,ptr,lim);}
      else return -1;}}
}

FD_EXPORT ssize_t fd_validate_dtype(struct FD_INBUF *in)
{
  return validate_dtype(0,in->bufread,in->buflim);
}

/* Byte input */

#define nobytes(in,nbytes) (PRED_FALSE(!(fd_request_bytes(in,nbytes))))
#define havebytes(in,nbytes) (PRED_TRUE(fd_request_bytes(in,nbytes)))

FD_EXPORT lispval fd_make_mystery_packet(int,int,unsigned int,unsigned char *);
FD_EXPORT lispval fd_make_mystery_vector(int,int,unsigned int,lispval *);
static lispval read_packaged_dtype(int,struct FD_INBUF *);

static lispval *read_dtypes(int n,struct FD_INBUF *in,
                           lispval *why_not,lispval *into)
{
  if (n==0) return NULL;
  else {
    lispval *vec = ((into)?(into):(u8_alloc_n(n,lispval)));
    int i = 0; while (i < n) {
      lispval v = fd_read_dtype(in);
      if (FD_COOLP(v))
        vec[i++]=v;
      else {
        int j = 0; while (j<i) {fd_decref(vec[j]); j++;}
        u8_free(vec); *why_not = v;
        return NULL;}}
    return vec;}
}

static lispval unexpected_eod()
{
  fd_seterr1(fd_UnexpectedEOD);
  return FD_ERROR;
}

FD_EXPORT lispval fd_read_dtype(struct FD_INBUF *in)
{
  if (PRED_FALSE(FD_ISWRITING(in)))
    return fdt_iswritebuf(in);
  else if (havebytes(in,1)) {
    int code = *(in->bufread++);
    long long len=-1;
    switch (code) {
    case dt_empty_list: return NIL;
    case dt_void: return VOID;
    case dt_empty_choice: return EMPTY;
    case dt_default_value: return FD_DEFAULT_VALUE;
    case dt_boolean:
      if (nobytes(in,1))
        return fd_return_errcode(FD_EOD);
      else if (*(in->bufread++))
        return FD_TRUE;
      else return FD_FALSE;
    case dt_fixnum:
      if (havebytes(in,4)) {
        int intval = fd_get_4bytes(in->bufread);
        in->bufread = in->bufread+4;
        return FD_INT(intval);}
      else return fd_return_errcode(FD_EOD);
    case dt_flonum: {
      char bytes[4];
      double flonum;
      float *f = (float *)&bytes;
      unsigned int *i = (unsigned int *)&bytes, num;
      long long result=fd_read_4bytes(in);
      if (result<0)
        return unexpected_eod();
      else num=(unsigned int)result;
      *i = num; flonum = *f;
      return _fd_make_double(flonum);}
    case dt_oid: {
      long long hival=fd_read_4bytes(in), loval;
      if (PRED_FALSE(hival<0))
        return unexpected_eod();
      else loval=fd_read_4bytes(in);
      if (PRED_FALSE(loval<0))
        return unexpected_eod();
      else {
        FD_OID addr = FD_NULL_OID_INIT;
        FD_SET_OID_HI(addr,hival);
        FD_SET_OID_LO(addr,loval);
        return fd_make_oid(addr);}}
    case dt_error: {
      lispval content = fd_read_dtype(in);
      if (FD_ABORTP(content))
        return content;
      else return fd_init_compound(NULL,error_symbol,0,1,content);}
    case dt_exception: {
      lispval content = fd_read_dtype(in);
      if (FD_ABORTP(content))
        return content;
      else return fd_restore_exception_dtype(content);}
    case dt_pair: {
      lispval head = NIL, *tail = &head;
      while (1) {
        lispval car = fd_read_dtype(in);
        if (FD_ABORTP(car)) {
          fd_decref(head);
          return car;}
        else {
          lispval new_pair=
            fd_init_pair(u8_alloc(struct FD_PAIR),
                         car,NIL);
          int dtcode = fd_read_byte(in);
          if (dtcode<0) {
            fd_decref(head);
            fd_decref(new_pair);
            return unexpected_eod();}
          *tail = new_pair;
          tail = &(FD_CDR(new_pair));
          if (dtcode != dt_pair) {
            lispval cdr;
            if (fd_unread_byte(in,dtcode)<0) {
              fd_decref(head);
              return FD_ERROR;}
            cdr = fd_read_dtype(in);
            if (FD_ABORTP(cdr)) {
              fd_decref(head);
              return cdr;}
            *tail = cdr;
            return head;}}}}
    case dt_compound: case dt_rational: case dt_complex: {
      lispval car = fd_read_dtype(in), cdr;
      if (FD_TROUBLEP(car))
        return car;
      else cdr = fd_read_dtype(in);
      if (FD_TROUBLEP(cdr)) {
        fd_decref(car);
        return cdr;}
      else switch (code) {
      case dt_compound: {
        struct FD_COMPOUND_TYPEINFO *e = fd_lookup_compound(car);
        if ((e) && (e->compound_restorefn)) {
          lispval result = e->compound_restorefn(car,cdr,e);
          fd_decref(cdr);
          return result;}
        else {
          int flags = FD_COMPOUND_USEREF;
          if (e) {
            if (e->compound_isopaque)
              flags |= FD_COMPOUND_OPAQUE;
            if (e->compound_ismutable)
              flags |= FD_COMPOUND_MUTABLE;
            if (e->compound_istable)
              flags |= FD_COMPOUND_TABLE;
            if (e->compound_istable)
              flags |= FD_COMPOUND_SEQUENCE;}
          else if (FD_VECTORP(cdr)) {
            flags |= FD_COMPOUND_SEQUENCE;}
          else NO_ELSE;
          if (FD_VECTORP(cdr)) {
            struct FD_VECTOR *vec = (struct FD_VECTOR *)cdr;
            lispval result = fd_init_compound_from_elts
              (NULL,car,flags,vec->vec_length,vec->vec_elts);
            vec->vec_length = 0; vec->vec_elts = NULL;
            fd_decref(cdr);
            return result;}
          else return fd_init_compound(NULL,car,flags,1,cdr);}}
      case dt_rational:
        return _fd_make_rational(car,cdr);
      case dt_complex:
        return _fd_make_complex(car,cdr);}}
    case dt_packet: case dt_string:
      len=fd_read_4bytes(in);
      if (len<0)
        return unexpected_eod();
      else if (nobytes(in,len))
        return unexpected_eod();
      else {
        lispval result = VOID;
        switch (code) {
        case dt_string:
          result = fd_make_string(NULL,len,in->bufread); break;
        case dt_packet:
          result = fd_make_packet(NULL,len,in->bufread); break;}
        in->bufread = in->bufread+len;
        return result;}
    case dt_tiny_symbol:
      len = fd_read_byte(in);
      if (len<0)
        return unexpected_eod();
      else if (nobytes(in,len))
        return unexpected_eod();
      else {
        u8_byte data[257];
        memcpy(data,in->bufread,len); data[len]='\0';
        in->bufread += len;
        return fd_make_symbol(data,len);}
    case dt_tiny_string:
      len = fd_read_byte(in);
      if (len<0)
        return unexpected_eod();
      else if (nobytes(in,len))
        return unexpected_eod();
      else {
        lispval result = fd_make_string(NULL,len,in->bufread);
        in->bufread += len;
        return result;}
    case dt_symbol: case dt_zstring:
      len = fd_read_4bytes(in);
      if ( (len<0) || (nobytes(in,len)) )
        return fd_return_errcode(FD_EOD);
      else {
        unsigned char data[len+1];
        memcpy(data,in->bufread,len); data[len]='\0';
        in->bufread += len;
        return fd_make_symbol(data,len);}
    case dt_vector:
      len = fd_read_4bytes(in);
      if (len < 0)
        return fd_return_errcode(FD_EOD);
      else if (PRED_FALSE(len == 0))
        return fd_empty_vector(0);
      else {
        lispval why_not = FD_EOD, result = fd_empty_vector(len);
        lispval *elts = FD_VECTOR_ELTS(result);
        lispval *data = read_dtypes(len,in,&why_not,elts);
        if (PRED_TRUE((data!=NULL)))
          return result;
        else return FD_ERROR;}
    case dt_tiny_choice:
      len=fd_read_byte(in);
      if (len<0)
        return unexpected_eod();
      else {
        struct FD_CHOICE *ch = fd_alloc_choice(len);
        lispval *write = (lispval *)FD_XCHOICE_DATA(ch);
        lispval *limit = write+len;
        while (write<limit) {
          lispval v = fd_read_dtype(in);
          if (FD_ABORTP(v)) {
            fd_free_choice(ch);
            return v;}
          *write++=v;}
        return fd_init_choice(ch,len,NULL,(FD_CHOICE_DOSORT|FD_CHOICE_REALLOC));}
    case dt_block:
      len=fd_read_4bytes(in);
      if ( (len<0) || (nobytes(in,len)) )
        return unexpected_eod();
      else return fd_read_dtype(in);
    case dt_framerd_package: {
      int code, lenlen;
      if (nobytes(in,2))
        return unexpected_eod();
      code = *(in->bufread++); lenlen = ((code&0x40) ? 4 : 1);
      if (lenlen==4)
        len = fd_read_4bytes(in);
      else len = fd_read_byte(in);
      if (len < 0)
        return unexpected_eod();
      else switch (code) {
        case dt_qchoice: case dt_small_qchoice:
          if (len==0)
            return fd_init_qchoice(u8_alloc(struct FD_QCHOICE),EMPTY);
        case dt_choice: case dt_small_choice:
          if (len==0)
            return EMPTY;
          else {
            lispval result;
            struct FD_CHOICE *ch = fd_alloc_choice(len);
            lispval *write = (lispval *)FD_XCHOICE_DATA(ch);
            lispval *limit = write+len;
            while (write<limit) {
              lispval v=fd_read_dtype(in);
              if (FD_ABORTP(v)) {
                lispval *elts=(lispval *)FD_XCHOICE_DATA(ch);
                fd_decref_vec(elts,write-elts);
                u8_big_free(ch);
                return v;}
              else *write++=v;}
            result = fd_init_choice(ch,len,NULL,(FD_CHOICE_DOSORT|FD_CHOICE_REALLOC));
            if (CHOICEP(result))
              if ((code == dt_qchoice) || (code == dt_small_qchoice))
                return fd_init_qchoice(u8_alloc(struct FD_QCHOICE),result);
              else return result;
            else return result;}
        case dt_slotmap: case dt_small_slotmap:
          if (len==0)
            return fd_empty_slotmap();
          else {
            int n_slots = len/2;
            struct FD_KEYVAL *keyvals = u8_alloc_n(n_slots,struct FD_KEYVAL);
            struct FD_KEYVAL *write = keyvals, *limit = keyvals+n_slots;
            while (write<limit) {
              lispval key = fd_read_dtype(in), value;
              if (FD_ABORTP(key)) value=key;
              else value = fd_read_dtype(in);
              if (FD_ABORTP(value)) {
                struct FD_KEYVAL *scan=keyvals;
                if (!(FD_ABORTP(key))) fd_decref(key);
                while (scan<write) {
                  fd_decref(scan->kv_key);
                  fd_decref(scan->kv_val);
                  scan++;}
                u8_free(keyvals);
                return value;}
              else {
                write->kv_key = key;
                write->kv_val = value;
                write++;}}
            return fd_init_slotmap(NULL,n_slots,keyvals);}
        case dt_hashtable: case dt_small_hashtable:
          if (len==0)
            return fd_init_hashtable(NULL,0,NULL);
          else {
            int n_keys = len/2, n_read = 0;
            struct FD_KEYVAL *keyvals = u8_big_alloc_n(n_keys,struct FD_KEYVAL);
            while (n_read<n_keys) {
              lispval key = fd_read_dtype(in), value;
              if (FD_ABORTP(key)) value=key;
              else value = fd_read_dtype(in);
              if (FD_ABORTP(value)) {
                if (!(FD_ABORTP(key))) fd_decref(key);
                int j = 0; while (j<n_read) {
                  fd_decref(keyvals[j].kv_key);
                  fd_decref(keyvals[j].kv_val);
                  j++;}
                u8_big_free(keyvals);
                return value;}
              else if ( (EMPTYP(value)) || (EMPTYP(key)) ) {
                fd_decref(key);
                fd_decref(value);}
              else {
                keyvals[n_read].kv_key = key;
                keyvals[n_read].kv_val = value;
                n_read++;}}
            lispval table = fd_initialize_hashtable(NULL,keyvals,n_read);
            u8_big_free(keyvals);
            return table;}
        case dt_hashset: case dt_small_hashset: {
          int i = 0;
          struct FD_HASHSET *h = u8_alloc(struct FD_HASHSET);
          fd_init_hashset(h,len,FD_MALLOCD_CONS);
          while (i<len) {
            lispval v = fd_read_dtype(in);
            if (FD_ABORTP(v)) {
              fd_decref((lispval)h);
              return v;}
            fd_hashset_add_raw(h,v);
            i++;}
          return LISP_CONS(h);}
        default: {
          int i = 0; lispval *data = u8_alloc_n(len,lispval);
          while (i<len) {
            lispval v=fd_read_dtype(in);
            if (FD_ABORTP(v)) {
              fd_decref_vec(data,i);
              u8_free(data);
              return v;}
            else data[i++]=fd_read_dtype(in);}
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
  int package_offset = package-0x40, code_offset = ((code-0x80)&(~(0x40)));
  int replaced = 0;
  if ((package<0x40) || (package>0x80)) return -1;
  else if ((code&0x80)==0) return -2;
  u8_lock_mutex(&dtype_unpacker_lock);
  if (fd_dtype_packages[package_offset]==NULL) {
    struct FD_DTYPE_PACKAGE *pkg=
      fd_dtype_packages[package_offset]=
      u8_alloc(struct FD_DTYPE_PACKAGE);
    memset(pkg,0,sizeof(struct FD_DTYPE_PACKAGE));
    replaced = 2;}
  else if (fd_dtype_packages[package_offset]->vectorfns[code_offset])
    replaced = 1;
  fd_dtype_packages[package_offset]->vectorfns[code_offset]=f;
  u8_unlock_mutex(&dtype_unpacker_lock);
  return replaced;
}

FD_EXPORT int fd_register_packet_unpacker
  (unsigned int package,unsigned int code,fd_packet_unpacker f)
{
  int package_offset = package-0x40, code_offset = ((code)&(~(0x40)));
  int replaced = 0;
  if ((package<0x40) || (package>0x80)) return -1;
  else if ((code&0x80)) return -2;
  u8_lock_mutex(&dtype_unpacker_lock);
  if (fd_dtype_packages[package_offset]==NULL) {
    struct FD_DTYPE_PACKAGE *pkg=
      fd_dtype_packages[package_offset]=
      u8_alloc(struct FD_DTYPE_PACKAGE);
    memset(pkg,0,sizeof(struct FD_DTYPE_PACKAGE));
    replaced = 2;}
  else if (fd_dtype_packages[package_offset]->packetfns[code_offset])
    replaced = 1;
  fd_dtype_packages[package_offset]->packetfns[code_offset]=f;
  u8_unlock_mutex(&dtype_unpacker_lock);
  return replaced;
}

/* Reading packaged dtypes */

static lispval make_character_type(int code,int len,unsigned char *data);

static lispval read_packaged_dtype
   (int package,struct FD_INBUF *in)
{
  lispval *vector = NULL; unsigned char *packet = NULL;
  long long len;
  unsigned int code, lenlen, vectorp;
  if (nobytes(in,2))
    return unexpected_eod();
  code = *(in->bufread++);
  lenlen = ((code&0x40) ? 4 : 1);
  vectorp = (code&0x80);
  if (nobytes(in,lenlen))
    return unexpected_eod();
  else if (lenlen==4)
    len = fd_read_4bytes(in);
  else len = fd_read_byte(in);
  if (len<0)
    return unexpected_eod();
  else if (vectorp) {
    lispval why_not;
    vector = read_dtypes(len,in,&why_not,NULL);
    if ( (len>0) && (vector == NULL))
      return why_not;}
  else if (nobytes(in,len))
    return unexpected_eod();
  else {
    packet = u8_malloc(len);
    memcpy(packet,in->bufread,len);
    in->bufread = in->bufread+len;}
  switch (package) {
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

static lispval make_character_type(int code,int len,unsigned char *bytes)
{
  switch (code) {
  case dt_ascii_char: {
    int c = bytes[0]; u8_free(bytes);
    return FD_CODE2CHAR(c);}
  case dt_unicode_char: {
    int c = 0, i = 0; while (i<len) {
      c = c<<8|bytes[i]; i++;}
    u8_free(bytes);
    return FD_CODE2CHAR(c);}
  case dt_unicode_short_string: case dt_unicode_string: {
    u8_byte buf[256];
    struct U8_OUTPUT os; unsigned char *scan, *limit; lispval result;
    U8_INIT_OUTPUT_X(&os,256,buf,0);
    scan = bytes; limit = bytes+len;
    while (scan < limit) {
      int c = scan[0]<<8|scan[1]; scan = scan+2;
      u8_putc(&os,c);}
    u8_free(bytes);
    result = fd_make_string(NULL,os.u8_write-os.u8_outbuf,os.u8_outbuf);
    u8_close_output(&os);
    return result;}
  case dt_secret_packet: case dt_short_secret_packet: {
    lispval result = fd_make_packet(NULL,len,bytes);
    FD_SET_CONS_TYPE(result,fd_secret_type);
    return result;}
  case dt_unicode_short_symbol: case dt_unicode_symbol: {
    lispval sym;
    struct U8_OUTPUT os; unsigned char *scan, *limit;
    U8_INIT_OUTPUT(&os,len);
    scan = bytes; limit = bytes+len;
    while (scan < limit) {
      int c = scan[0]<<8|scan[1]; scan = scan+2;
      u8_putc(&os,c);}
    sym = fd_make_symbol(os.u8_outbuf,os.u8_write-os.u8_outbuf);
    u8_free(bytes); u8_free(os.u8_outbuf);
    return sym;}
  default:
    return fd_make_mystery_packet(dt_character_package,code,len,bytes);
  }
}

FD_EXPORT lispval fd_make_mystery_packet
  (int package,int typecode,unsigned int len,unsigned char *bytes)
{
  struct FD_MYSTERY_DTYPE *myst;
  int pkg_offset = package-0x40;
  int code_offset = (typecode&0x3F);
  if ((pkg_offset<0x40) && (fd_dtype_packages[pkg_offset]) &&
      (fd_dtype_packages[pkg_offset]->packetfns[code_offset]))
    return (fd_dtype_packages[pkg_offset]->packetfns[code_offset])(len,bytes);
  myst = u8_alloc(struct FD_MYSTERY_DTYPE);
  FD_INIT_CONS(myst,fd_mystery_type);
  myst->myst_dtpackage = package; myst->myst_dtcode = typecode;
  myst->mystery_payload.bytes = bytes; myst->myst_dtsize = len;
  return LISP_CONS(myst);
}

FD_EXPORT lispval fd_make_mystery_vector
  (int package,int typecode,unsigned int len,lispval *elts)
{
  struct FD_MYSTERY_DTYPE *myst;
  int pkg_offset = package-0x40;
  int code_offset = (typecode&0x3F);
  if ((pkg_offset<0x40) && (fd_dtype_packages[pkg_offset]) &&
      (fd_dtype_packages[pkg_offset]->vectorfns[code_offset]))
    return (fd_dtype_packages[pkg_offset]->vectorfns[code_offset])(len,elts);
  myst = u8_alloc(struct FD_MYSTERY_DTYPE);
  FD_INIT_CONS(myst,fd_mystery_type);
  myst->myst_dtpackage = package; myst->myst_dtcode = typecode;
  myst->mystery_payload.elts = elts; myst->myst_dtsize = len;
  return LISP_CONS(myst);
}

/* Arith stubs */

static lispval default_make_rational(lispval car,lispval cdr)
{
  struct FD_PAIR *p = u8_alloc(struct FD_PAIR);
  FD_INIT_CONS(p,fd_rational_type);
  p->car = car; p->cdr = cdr;
  return LISP_CONS(p);
}

static void default_unpack_rational
  (lispval x,lispval *car,lispval *cdr)
{
  struct FD_PAIR *p = fd_consptr(struct FD_PAIR *,x,fd_rational_type);
  *car = p->car; *cdr = p->cdr;
}

static lispval default_make_complex(lispval car,lispval cdr)
{
  struct FD_PAIR *p = u8_alloc(struct FD_PAIR);
  FD_INIT_CONS(p,fd_complex_type);
  p->car = car;
  p->cdr = cdr;
  return LISP_CONS(p);
}

static void default_unpack_complex
  (lispval x,lispval *car,lispval *cdr)
{
  struct FD_PAIR *p = fd_consptr(struct FD_PAIR *,x,fd_complex_type);
  *car = p->car;
  *cdr = p->cdr;
}

static lispval default_make_double(double d)
{
  struct FD_FLONUM *ds = u8_alloc(struct FD_FLONUM);
  FD_INIT_CONS(ds,fd_flonum_type);
  ds->floval = d;
  return LISP_CONS(ds);
}

lispval(*_fd_make_rational)(lispval car,lispval cdr) = default_make_rational;
void
  (*_fd_unpack_rational)(lispval,lispval *,lispval *) = default_unpack_rational;
lispval (*_fd_make_complex)(lispval car,lispval cdr) = default_make_complex;
void (*_fd_unpack_complex)(lispval,lispval *,lispval *) = default_unpack_complex;
lispval (*_fd_make_double)(double) = default_make_double;

/* Reading and writing compressed dtypes */

static unsigned char *do_uncompress
(Bytef *fdata,size_t n_bytes,ssize_t *dbytes,Bytef *init_xbuf,ssize_t buflen)
{
  Bytef *xbuf=init_xbuf;
  int error=0;
  if (init_xbuf==NULL) {
    if (buflen<0) buflen=4*n_bytes;
    xbuf=u8_malloc(buflen);}
  uLongf xbuf_size=buflen;
  while ((error = uncompress(xbuf,&xbuf_size,fdata,n_bytes)) < Z_OK)
    if (error == Z_MEM_ERROR) {
      fd_seterr1("ZLIB Out of Memory");
      if (xbuf!=init_xbuf) u8_free(xbuf);
      return NULL;}
    else if (error == Z_BUF_ERROR) {
      Bytef *newbuf;
      if (xbuf == init_xbuf)
        newbuf = u8_malloc(buflen*2);
      else newbuf = u8_realloc(xbuf,buflen*2);
      if (newbuf == NULL) {
        u8_seterr(fd_MallocFailed,"do_uncompress",NULL);
        if (xbuf == init_xbuf) u8_free(xbuf);
        return NULL;}
      xbuf=newbuf;
      buflen=buflen*2;
      xbuf_size=buflen;}
    else if (error == Z_DATA_ERROR) {
      if (xbuf == init_xbuf) u8_free(xbuf);
      fd_seterr1("ZLIB Data error");
      return NULL;}
    else if (error == Z_STREAM_ERROR) {
      if (xbuf == init_xbuf) u8_free(xbuf);
      fd_seterr1("ZLIB Data error");
      return NULL;}
    else {
      if (xbuf == init_xbuf) u8_free(xbuf);
      fd_seterr1("Bad ZLIB return code");
      return NULL;}
  *dbytes = xbuf_size;
  return xbuf;
}

/* This reads a non frame value with compression. */
FD_EXPORT lispval fd_zread_dtype(struct FD_INBUF *in)
{
  lispval result;
  ssize_t n_bytes = fd_read_zint(in), dbytes;
  unsigned char *bytes = u8_malloc(n_bytes);
  int retval = fd_read_bytes(bytes,in,n_bytes);
  struct FD_INBUF tmp = { 0 };
  if (retval<n_bytes) {
    u8_free(bytes);
    return FD_ERROR;}
  tmp.bufread = tmp.buffer = do_uncompress(bytes,n_bytes,&dbytes,NULL,-1);
  tmp.buf_flags = FD_HEAP_BUFFER;
  tmp.buflim = tmp.buffer+dbytes;
  result = fd_read_dtype(&tmp);
  u8_free(bytes);
  u8_free(tmp.buffer);
  return result;
}
/* File initialization */

FD_EXPORT void fd_init_dtread_c()
{
  u8_register_source_file(_FILEINFO);

  error_symbol = fd_intern("%ERROR");

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
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
